BEGIN;

ALTER TABLE agent_economy.worker_jobs DROP CONSTRAINT worker_jobs_collection_admission;
ALTER TABLE agent_economy.worker_jobs ADD CONSTRAINT worker_jobs_collection_admission CHECK (
    (mode = 'collect'
        AND collection_chain_scope IS NOT NULL
        AND collection_source_id IS NOT NULL
        AND collection_start_height >= 0
        AND collection_end_height >= collection_start_height
        AND collection_end_height - collection_start_height + 1 <= 10000
        AND (
            (
                collection_acquisition_contract = 'alchemy-rpc-block-v1'
                AND collection_chain_scope IN ('ethereum', 'base', 'solana', 'tempo')
                AND collection_source_id = 'alchemy-' || collection_chain_scope
            )
            OR (
                collection_acquisition_contract = 'cdp-x402-bazaar-v1'
                AND collection_chain_scope = 'base'
                AND collection_source_id = 'cdp-x402-bazaar-base'
                AND collection_start_height = 0
                AND collection_end_height = 0
            )
        )
        AND collection_evidence_contract = 'evidence-store-create-read-sha256-v1')
    OR (mode <> 'collect'
        AND collection_chain_scope IS NULL
        AND collection_source_id IS NULL
        AND collection_start_height IS NULL
        AND collection_end_height IS NULL
        AND collection_acquisition_contract IS NULL
        AND collection_evidence_contract IS NULL)
);

CREATE OR REPLACE FUNCTION agent_economy.stage_collection_batch(
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid,
    p_input_sha256 text,
    p_batch_sha256 text,
    p_chain_scope text,
    p_source_id text,
    p_observed_at_unix_ms bigint,
    p_start_height bigint,
    p_end_height bigint,
    p_evidence_json jsonb
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    bound_namespace uuid;
    result uuid;
    existing agent_economy.pending_collection_batches%ROWTYPE;
BEGIN
    bound_namespace := agent_economy.bound_collection_namespace('collect');
    PERFORM 1 FROM agent_economy.worker_jobs job
    WHERE job.namespace_id = bound_namespace AND job.job_id = p_job_id
      AND job.mode = 'collect' AND job.job_kind = 'chain-protocol-range'
      AND job.status = 'leased' AND job.lease_owner = p_lease_owner
      AND job.lease_token = p_lease_token AND job.lease_expires_at > clock_timestamp()
      AND job.input_sha256 = p_input_sha256
      AND job.collection_chain_scope = p_chain_scope
      AND job.collection_source_id = p_source_id
      AND job.collection_start_height = p_start_height
      AND job.collection_end_height = p_end_height
      AND job.collection_acquisition_contract IN ('alchemy-rpc-block-v1', 'cdp-x402-bazaar-v1')
      AND job.collection_evidence_contract = 'evidence-store-create-read-sha256-v1'
    FOR UPDATE;
    IF NOT FOUND THEN RAISE EXCEPTION 'collection admission is not live'; END IF;
    IF p_batch_sha256 !~ '^[0-9a-f]{64}$' OR jsonb_typeof(p_evidence_json) <> 'array'
       OR jsonb_array_length(p_evidence_json) <> p_end_height - p_start_height + 1
       OR EXISTS (
         SELECT 1 FROM jsonb_array_elements(p_evidence_json) evidence(item)
         WHERE jsonb_typeof(item) <> 'object'
            OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 7
            OR NOT (item ?& ARRAY['evidence_id','sha256','storage_uri','storage_generation','media_type','byte_length','height'])
            OR (item->>'evidence_id') <> 'evidence:sha256:' || (item->>'sha256')
            OR (item->>'sha256') !~ '^[0-9a-f]{64}$'
            OR (item->>'storage_uri') !~ ('^evidence/' || p_source_id || '/[0-9]{4}-[0-9]{2}-[0-9]{2}/sha256/' || left((item->>'sha256'),2) || '/' || (item->>'sha256') || '$')
            OR (p_source_id = 'cdp-x402-bazaar-base' AND (item->>'media_type') <> 'application/json')
            OR (p_source_id <> 'cdp-x402-bazaar-base' AND (item->>'media_type') <> 'application/vnd.agent-economy.rpc')
            OR (item->>'height')::bigint NOT BETWEEN p_start_height AND p_end_height
            OR (item->>'byte_length')::bigint NOT BETWEEN 1 AND 4194304
       ) OR (SELECT count(DISTINCT (item->>'height')::bigint)
             FROM jsonb_array_elements(p_evidence_json) evidence(item))
             <> p_end_height - p_start_height + 1
    THEN RAISE EXCEPTION 'invalid pending collection batch'; END IF;

    SELECT * INTO existing FROM agent_economy.pending_collection_batches
    WHERE namespace_id = bound_namespace AND job_id = p_job_id FOR UPDATE;
    IF FOUND THEN
        IF existing.input_sha256 <> p_input_sha256 OR existing.batch_sha256 <> p_batch_sha256
           OR existing.chain_scope <> p_chain_scope OR existing.source_id <> p_source_id
           OR existing.observed_at_unix_ms <> p_observed_at_unix_ms
           OR existing.start_height <> p_start_height OR existing.end_height <> p_end_height
           OR existing.evidence_json <> p_evidence_json
        THEN RAISE EXCEPTION 'changed collection staging replay'; END IF;
        RETURN existing.pending_id;
    END IF;
    INSERT INTO agent_economy.pending_collection_batches
      (namespace_id, job_id, input_sha256, batch_sha256, chain_scope, source_id,
       observed_at_unix_ms, start_height, end_height, evidence_json)
    VALUES (bound_namespace, p_job_id, p_input_sha256, p_batch_sha256, p_chain_scope,
            p_source_id, p_observed_at_unix_ms, p_start_height, p_end_height, p_evidence_json)
    RETURNING pending_id INTO result;
    RETURN result;
END
$$;

CREATE OR REPLACE FUNCTION agent_economy.commit_collection_batch(
    p_namespace_id uuid,
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid,
    p_input_sha256 text,
    p_batch_sha256 text,
    p_chain_scope text,
    p_source_id text,
    p_observed_at_unix_ms bigint,
    p_start_height bigint,
    p_end_height bigint,
    p_evidence_json jsonb,
    p_observations_json jsonb
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    current_height bigint;
    computed_next_height bigint;
    observed_date text;
    existing_receipt agent_economy.collection_range_receipts%ROWTYPE;
BEGIN
    IF p_chain_scope NOT IN ('ethereum', 'base', 'solana', 'tempo')
        OR p_source_id !~ '^[a-z0-9][a-z0-9_.-]{0,62}[a-z0-9]$'
        OR p_observed_at_unix_ms NOT BETWEEN 1 AND 253402300799999
        OR p_start_height < 0
        OR p_end_height < p_start_height
        OR p_end_height = 9223372036854775807
        OR p_end_height - p_start_height + 1 > 10000
        OR p_input_sha256 !~ '^[0-9a-f]{64}$'
        OR p_batch_sha256 !~ '^[0-9a-f]{64}$'
        OR jsonb_typeof(p_evidence_json) <> 'array'
        OR jsonb_array_length(p_evidence_json) NOT BETWEEN 1 AND 10000
        OR jsonb_typeof(p_observations_json) <> 'array'
        OR jsonb_array_length(p_observations_json) > 50000
    THEN
        RAISE EXCEPTION 'invalid collection batch';
    END IF;
    computed_next_height := p_end_height + 1;
    observed_date := to_char(
        to_timestamp(p_observed_at_unix_ms / 1000.0) AT TIME ZONE 'UTC',
        'YYYY-MM-DD'
    );

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
        WHERE jsonb_typeof(item) <> 'object'
            OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 7
            OR NOT (item ?& ARRAY[
                'evidence_id', 'sha256', 'storage_uri', 'storage_generation',
                'media_type', 'byte_length', 'height'
            ])
            OR jsonb_typeof(item -> 'storage_generation') NOT IN ('string', 'null')
            OR (item ->> 'sha256') !~ '^[0-9a-f]{64}$'
            OR (item ->> 'evidence_id') <> 'evidence:sha256:' || (item ->> 'sha256')
            OR (item ->> 'storage_uri') <>
                'evidence/' || p_source_id || '/' || observed_date || '/sha256/' ||
                left(item ->> 'sha256', 2) || '/' || (item ->> 'sha256')
            OR (item ->> 'media_type') NOT IN (
                'application/http',
                'application/json',
                'application/vnd.agent-economy.rpc',
                'application/vnd.oai.openapi+json'
            )
            OR NOT CASE
                WHEN (item ->> 'byte_length') ~ '^[0-9]{1,10}$'
                THEN (item ->> 'byte_length')::bigint BETWEEN 1 AND 4194304
                ELSE false
            END
            OR NOT CASE
                WHEN (item ->> 'height') ~ '^[0-9]{1,19}$'
                THEN (item ->> 'height')::numeric BETWEEN p_start_height AND p_end_height
                ELSE false
            END
    ) OR (
        SELECT count(*)
        FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
    ) <> (
        SELECT count(DISTINCT item ->> 'evidence_id')
        FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
    ) OR (
        SELECT sum((item ->> 'byte_length')::bigint)
        FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
    ) > 16777216
    OR (
        SELECT count(DISTINCT (item ->> 'height')::bigint)
        FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
    ) <> p_end_height - p_start_height + 1
    THEN
        RAISE EXCEPTION 'invalid collection evidence';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_observations_json) AS observation(item)
        WHERE jsonb_typeof(item) <> 'object'
            OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 6
            OR NOT (item ?& ARRAY[
                'observation_id', 'protocol', 'evidence_id',
                'observation_hash', 'parser_version', 'height'
            ])
            OR (item ->> 'observation_id') !~ '^sha256:[0-9a-f]{64}$'
            OR (item ->> 'protocol') NOT IN ('x402', 'mpp')
            OR (item ->> 'observation_hash') !~ '^[0-9a-f]{64}$'
            OR (item ->> 'parser_version') !~ '^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$'
            OR NOT CASE
                WHEN (item ->> 'height') ~ '^[0-9]{1,19}$'
                THEN (item ->> 'height')::numeric BETWEEN p_start_height AND p_end_height
                ELSE false
            END
            OR NOT EXISTS (
                SELECT 1
                FROM jsonb_array_elements(p_evidence_json) AS evidence(evidence_item)
                WHERE evidence_item ->> 'evidence_id' = item ->> 'evidence_id'
                    AND evidence_item ->> 'height' = item ->> 'height'
            )
    ) OR (
        SELECT count(*)
        FROM jsonb_array_elements(p_observations_json) AS observation(item)
    ) <> (
        SELECT count(DISTINCT concat_ws(
            ':', item ->> 'protocol', item ->> 'observation_id'
        ))
        FROM jsonb_array_elements(p_observations_json) AS observation(item)
    )
    THEN
        RAISE EXCEPTION 'invalid collection observation';
    END IF;

    PERFORM 1
    FROM agent_economy.worker_jobs AS job
    JOIN agent_economy.pending_collection_batches AS pending
      ON pending.namespace_id = job.namespace_id AND pending.job_id = job.job_id
    WHERE job.namespace_id = p_namespace_id
        AND job.job_id = p_job_id
        AND job.mode = 'collect'
        AND job.job_kind = 'chain-protocol-range'
        AND job.status = 'succeeded'
        AND job.output_sha256 = p_batch_sha256
        AND job.input_sha256 = p_input_sha256
        AND job.collection_chain_scope = p_chain_scope
        AND job.collection_source_id = p_source_id
        AND job.collection_start_height = p_start_height
        AND job.collection_end_height = p_end_height
        AND job.collection_acquisition_contract IN ('alchemy-rpc-block-v1', 'cdp-x402-bazaar-v1')
        AND job.collection_evidence_contract = 'evidence-store-create-read-sha256-v1'
        AND pending.status = 'verifying'
        AND pending.verifier_owner = p_lease_owner
        AND pending.verifier_token = p_lease_token
        AND pending.verifier_expires_at > clock_timestamp()
        AND pending.input_sha256 = p_input_sha256
        AND pending.batch_sha256 = p_batch_sha256
        AND pending.chain_scope = p_chain_scope
        AND pending.source_id = p_source_id
        AND pending.observed_at_unix_ms = p_observed_at_unix_ms
        AND pending.start_height = p_start_height
        AND pending.end_height = p_end_height
        AND pending.evidence_json = p_evidence_json
    FOR UPDATE OF job, pending;
    IF NOT FOUND THEN
        RETURN false;
    END IF;


    INSERT INTO agent_economy.collection_cursors
        (namespace_id, chain_scope, source_id, next_height)
    VALUES (p_namespace_id, p_chain_scope, p_source_id, p_start_height)
    ON CONFLICT (namespace_id, chain_scope, source_id) DO NOTHING;

    SELECT cursor.next_height
    INTO STRICT current_height
    FROM agent_economy.collection_cursors AS cursor
    WHERE cursor.namespace_id = p_namespace_id
        AND cursor.chain_scope = p_chain_scope
        AND cursor.source_id = p_source_id
    FOR UPDATE;

    IF current_height <> p_start_height AND current_height <> computed_next_height THEN
        RETURN false;
    END IF;

    SELECT *
    INTO existing_receipt
    FROM agent_economy.collection_range_receipts AS receipt
    WHERE receipt.namespace_id = p_namespace_id
        AND receipt.chain_scope = p_chain_scope
        AND receipt.source_id = p_source_id
        AND receipt.start_height = p_start_height
        AND receipt.end_height = p_end_height
    FOR UPDATE;
    IF FOUND AND (
        existing_receipt.input_sha256 <> p_input_sha256
        OR existing_receipt.batch_sha256 <> p_batch_sha256
        OR existing_receipt.evidence_json <> p_evidence_json
        OR existing_receipt.observations_json <> p_observations_json
        OR existing_receipt.evidence_count <> jsonb_array_length(p_evidence_json)
        OR existing_receipt.observation_count <> jsonb_array_length(p_observations_json)
    ) THEN
        RAISE EXCEPTION 'collection replay differs from immutable receipt';
    END IF;

    INSERT INTO agent_economy.evidence_objects
        (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
    SELECT p_namespace_id,
           item ->> 'evidence_id',
           item ->> 'sha256',
           item ->> 'storage_uri',
           item ->> 'media_type',
           (item ->> 'byte_length')::bigint,
           to_timestamp(p_observed_at_unix_ms / 1000.0)
    FROM jsonb_array_elements(p_evidence_json) AS input(item)
    ON CONFLICT (namespace_id, evidence_id) DO NOTHING;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_evidence_json) AS input(item)
        JOIN agent_economy.evidence_objects AS evidence
          ON evidence.namespace_id = p_namespace_id
         AND evidence.evidence_id = item ->> 'evidence_id'
        WHERE evidence.sha256 <> item ->> 'sha256'
           OR evidence.storage_uri <> item ->> 'storage_uri'
           OR evidence.media_type <> item ->> 'media_type'
           OR evidence.byte_length <> (item ->> 'byte_length')::bigint
           OR evidence.observed_at <> to_timestamp(p_observed_at_unix_ms / 1000.0)
    ) THEN
        RAISE EXCEPTION 'contradictory collection evidence';
    END IF;

    INSERT INTO agent_economy.provenance_records
        (namespace_id, provenance_id, source_id, observed_at, parser_version,
         provider, chain_scope, block_reference, evidence_id)
    SELECT p_namespace_id,
           (
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 1, 8) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 9, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 13, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 17, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 21, 12)
           )::uuid,
           p_source_id,
           to_timestamp(p_observed_at_unix_ms / 1000.0),
           item ->> 'parser_version',
           CASE WHEN p_source_id = 'cdp-x402-bazaar-base' THEN 'cdp-bazaar' ELSE 'alchemy-rpc' END,
           p_chain_scope,
           item ->> 'height',
           item ->> 'evidence_id'
    FROM jsonb_array_elements(p_observations_json) AS observation(item)
    ON CONFLICT (namespace_id, provenance_id) DO NOTHING;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_observations_json) AS observation(item)
        JOIN agent_economy.provenance_records AS provenance
          ON provenance.namespace_id = p_namespace_id
         AND provenance.provenance_id = (
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 1, 8) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 9, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 13, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 17, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 21, 12)
         )::uuid
        WHERE provenance.source_id <> p_source_id
           OR provenance.observed_at <> to_timestamp(p_observed_at_unix_ms / 1000.0)
           OR provenance.parser_version <> item ->> 'parser_version'
           OR provenance.provider <> CASE WHEN p_source_id = 'cdp-x402-bazaar-base' THEN 'cdp-bazaar' ELSE 'alchemy-rpc' END
           OR provenance.chain_scope <> p_chain_scope
           OR provenance.block_reference <> item ->> 'height'
           OR provenance.evidence_id <> item ->> 'evidence_id'
    ) THEN
        RAISE EXCEPTION 'contradictory collection provenance';
    END IF;

    INSERT INTO agent_economy.observations
        (namespace_id, chain_scope, source_id, observation_id, observed_at,
         parser_version, protocol, evidence_id, provenance_id, observation_hash)
    SELECT p_namespace_id,
           p_chain_scope,
           p_source_id,
           item ->> 'observation_id',
           to_timestamp(p_observed_at_unix_ms / 1000.0),
           item ->> 'parser_version',
           item ->> 'protocol',
           item ->> 'evidence_id',
           (
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 1, 8) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 9, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 13, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 17, 4) || '-' ||
             substr(md5(p_source_id || ':' || (item ->> 'observation_id')), 21, 12)
           )::uuid,
           item ->> 'observation_hash'
    FROM jsonb_array_elements(p_observations_json) AS observation(item)
    ON CONFLICT (namespace_id, chain_scope, source_id, protocol, observation_id) DO NOTHING;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_observations_json) AS observation(item)
        JOIN agent_economy.observations AS existing
          ON existing.namespace_id = p_namespace_id
         AND existing.chain_scope = p_chain_scope
         AND existing.source_id = p_source_id
         AND existing.protocol = item ->> 'protocol'
         AND existing.observation_id = item ->> 'observation_id'
        WHERE existing.observed_at <> to_timestamp(p_observed_at_unix_ms / 1000.0)
           OR existing.parser_version <> item ->> 'parser_version'
           OR existing.evidence_id <> item ->> 'evidence_id'
           OR existing.observation_hash <> item ->> 'observation_hash'
    ) THEN
        RAISE EXCEPTION 'contradictory collection observation';
    END IF;

    INSERT INTO agent_economy.collection_range_receipts
        (namespace_id, chain_scope, source_id, start_height, end_height,
         input_sha256, batch_sha256, evidence_json, observations_json,
         evidence_count, observation_count)
    VALUES
        (p_namespace_id, p_chain_scope, p_source_id, p_start_height, p_end_height,
         p_input_sha256, p_batch_sha256, p_evidence_json, p_observations_json,
         jsonb_array_length(p_evidence_json),
         jsonb_array_length(p_observations_json))
    ON CONFLICT (namespace_id, chain_scope, source_id, start_height, end_height) DO NOTHING;

    IF current_height = p_start_height THEN
        UPDATE agent_economy.collection_cursors AS cursor
        SET next_height = computed_next_height,
            version = cursor.version + 1,
            updated_at = clock_timestamp()
        WHERE cursor.namespace_id = p_namespace_id
            AND cursor.chain_scope = p_chain_scope
            AND cursor.source_id = p_source_id
            AND cursor.next_height = p_start_height;
        IF NOT FOUND THEN
            RETURN false;
        END IF;
    END IF;
    RETURN true;
END
$$;

REVOKE ALL ON FUNCTION agent_economy.commit_collection_batch(uuid,uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,jsonb) FROM PUBLIC;

COMMIT;
