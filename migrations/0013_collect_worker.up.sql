BEGIN;

ALTER TABLE agent_economy.worker_jobs
    ADD COLUMN collection_chain_scope text,
    ADD COLUMN collection_source_id text,
    ADD COLUMN collection_start_height bigint,
    ADD COLUMN collection_end_height bigint,
    ADD COLUMN collection_acquisition_contract text,
    ADD COLUMN collection_evidence_contract text,
    ADD CONSTRAINT worker_jobs_collection_admission CHECK (
        (mode = 'collect'
            AND collection_chain_scope IN ('ethereum', 'base', 'solana', 'tempo')
            AND collection_source_id = 'alchemy-' || collection_chain_scope
            AND collection_start_height >= 0
            AND collection_end_height >= collection_start_height
            AND collection_end_height - collection_start_height + 1 <= 10000
            AND collection_acquisition_contract = 'alchemy-rpc-block-v1'
            AND collection_evidence_contract = 'evidence-store-create-read-sha256-v1')
        OR (mode <> 'collect'
            AND collection_chain_scope IS NULL
            AND collection_source_id IS NULL
            AND collection_start_height IS NULL
            AND collection_end_height IS NULL
            AND collection_acquisition_contract IS NULL
            AND collection_evidence_contract IS NULL)
    );

CREATE OR REPLACE FUNCTION agent_economy.protect_worker_job_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(
        OLD.namespace_id, OLD.job_id, OLD.mode, OLD.job_kind,
        OLD.idempotency_key, OLD.input_sha256, OLD.max_attempts, OLD.created_at,
        OLD.collection_chain_scope, OLD.collection_source_id,
        OLD.collection_start_height, OLD.collection_end_height,
        OLD.collection_acquisition_contract, OLD.collection_evidence_contract
    ) IS DISTINCT FROM ROW(
        NEW.namespace_id, NEW.job_id, NEW.mode, NEW.job_kind,
        NEW.idempotency_key, NEW.input_sha256, NEW.max_attempts, NEW.created_at,
        NEW.collection_chain_scope, NEW.collection_source_id,
        NEW.collection_start_height, NEW.collection_end_height,
        NEW.collection_acquisition_contract, NEW.collection_evidence_contract
    ) THEN
        RAISE EXCEPTION 'worker job identity is immutable';
    END IF;
    IF OLD.status IN ('succeeded', 'dead_letter', 'cancelled') AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal worker job is immutable';
    END IF;
    RETURN NEW;
END
$$;

CREATE TABLE agent_economy.collection_cursors (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    chain_scope text NOT NULL CHECK (chain_scope IN ('ethereum', 'base', 'solana', 'tempo')),
    source_id text NOT NULL CHECK (source_id <> ''),
    next_height bigint NOT NULL CHECK (next_height >= 0),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, chain_scope, source_id)
);

CREATE TABLE agent_economy.collection_range_receipts (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    chain_scope text NOT NULL CHECK (chain_scope IN ('ethereum', 'base', 'solana', 'tempo')),
    source_id text NOT NULL CHECK (source_id <> ''),
    start_height bigint NOT NULL CHECK (start_height >= 0),
    end_height bigint NOT NULL CHECK (end_height >= start_height),
    input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    batch_sha256 text NOT NULL CHECK (batch_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_json jsonb NOT NULL CHECK (jsonb_typeof(evidence_json) = 'array'),
    observations_json jsonb NOT NULL CHECK (jsonb_typeof(observations_json) = 'array'),
    evidence_count integer NOT NULL CHECK (evidence_count > 0),
    observation_count integer NOT NULL CHECK (observation_count >= 0),
    committed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, chain_scope, source_id, start_height, end_height)
);

CREATE TABLE agent_economy.collection_evidence_attestations (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    lease_token uuid NOT NULL,
    evidence_id text NOT NULL CHECK (evidence_id ~ '^evidence:sha256:[0-9a-f]{64}$'),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    storage_uri text NOT NULL,
    media_type text NOT NULL,
    byte_length bigint NOT NULL CHECK (byte_length BETWEEN 1 AND 4194304),
    height bigint NOT NULL CHECK (height >= 0),
    evidence_contract text NOT NULL,
    attested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, job_id, lease_token, evidence_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id)
);

CREATE FUNCTION agent_economy.attest_collection_evidence(
    p_namespace_id uuid,
    p_job_id uuid,
    p_lease_token uuid,
    p_evidence_id text,
    p_sha256 text,
    p_storage_uri text,
    p_media_type text,
    p_byte_length bigint,
    p_height bigint
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    admission agent_economy.worker_jobs%ROWTYPE;
BEGIN
    SELECT * INTO admission
    FROM agent_economy.worker_jobs AS job
    WHERE job.namespace_id = p_namespace_id
      AND job.job_id = p_job_id
      AND job.mode = 'collect'
      AND job.job_kind = 'chain-protocol-range'
      AND job.status = 'leased'
      AND job.lease_token = p_lease_token
      AND job.lease_expires_at > clock_timestamp()
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    IF p_evidence_id <> 'evidence:sha256:' || p_sha256
       OR p_sha256 !~ '^[0-9a-f]{64}$'
       OR p_storage_uri !~ (
           '^evidence/' || admission.collection_source_id ||
           '/[0-9]{4}-[0-9]{2}-[0-9]{2}/sha256/' || left(p_sha256, 2) || '/' || p_sha256 || '$'
       )
       OR p_media_type <> 'application/vnd.agent-economy.rpc'
       OR p_byte_length NOT BETWEEN 1 AND 4194304
       OR p_height NOT BETWEEN admission.collection_start_height AND admission.collection_end_height
    THEN
        RAISE EXCEPTION 'invalid verified collection evidence';
    END IF;
    INSERT INTO agent_economy.collection_evidence_attestations
        (namespace_id, job_id, lease_token, evidence_id, sha256, storage_uri,
         media_type, byte_length, height, evidence_contract)
    VALUES
        (p_namespace_id, p_job_id, p_lease_token, p_evidence_id, p_sha256, p_storage_uri,
         p_media_type, p_byte_length, p_height, admission.collection_evidence_contract)
    ON CONFLICT (namespace_id, job_id, lease_token, evidence_id) DO UPDATE
    SET sha256 = EXCLUDED.sha256,
        storage_uri = EXCLUDED.storage_uri,
        media_type = EXCLUDED.media_type,
        byte_length = EXCLUDED.byte_length,
        height = EXCLUDED.height,
        evidence_contract = EXCLUDED.evidence_contract
    WHERE agent_economy.collection_evidence_attestations.sha256 = EXCLUDED.sha256
      AND agent_economy.collection_evidence_attestations.storage_uri = EXCLUDED.storage_uri
      AND agent_economy.collection_evidence_attestations.media_type = EXCLUDED.media_type
      AND agent_economy.collection_evidence_attestations.byte_length = EXCLUDED.byte_length
      AND agent_economy.collection_evidence_attestations.height = EXCLUDED.height
      AND agent_economy.collection_evidence_attestations.evidence_contract = EXCLUDED.evidence_contract;
    RETURN FOUND;
END
$$;

CREATE FUNCTION agent_economy.commit_collection_batch(
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
            OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 6
            OR NOT (item ?& ARRAY[
                'evidence_id', 'sha256', 'storage_uri', 'media_type', 'byte_length', 'height'
            ])
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
    WHERE job.namespace_id = p_namespace_id
        AND job.job_id = p_job_id
        AND job.mode = 'collect'
        AND job.job_kind = 'chain-protocol-range'
        AND job.status = 'leased'
        AND job.lease_owner = p_lease_owner
        AND job.lease_token = p_lease_token
        AND job.input_sha256 = p_input_sha256
        AND job.collection_chain_scope = p_chain_scope
        AND job.collection_source_id = p_source_id
        AND job.collection_start_height = p_start_height
        AND job.collection_end_height = p_end_height
        AND job.collection_acquisition_contract = 'alchemy-rpc-block-v1'
        AND job.collection_evidence_contract = 'evidence-store-create-read-sha256-v1'
        AND job.lease_expires_at > clock_timestamp()
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_evidence_json) AS submitted(item)
        WHERE NOT EXISTS (
            SELECT 1
            FROM agent_economy.collection_evidence_attestations AS attestation
            WHERE attestation.namespace_id = p_namespace_id
              AND attestation.job_id = p_job_id
              AND attestation.lease_token = p_lease_token
              AND attestation.evidence_id = item ->> 'evidence_id'
              AND attestation.sha256 = item ->> 'sha256'
              AND attestation.storage_uri = item ->> 'storage_uri'
              AND attestation.media_type = item ->> 'media_type'
              AND attestation.byte_length = (item ->> 'byte_length')::bigint
              AND attestation.height = (item ->> 'height')::bigint
              AND attestation.evidence_contract = 'evidence-store-create-read-sha256-v1'
        )
    ) THEN
        RAISE EXCEPTION 'missing verified collection evidence';
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
           'alchemy-rpc',
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
           OR provenance.provider <> 'alchemy-rpc'
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

REVOKE ALL ON agent_economy.collection_cursors FROM PUBLIC;
REVOKE ALL ON agent_economy.collection_cursors FROM agent_economy_worker;
REVOKE ALL ON agent_economy.collection_range_receipts FROM PUBLIC;
REVOKE ALL ON agent_economy.collection_range_receipts FROM agent_economy_worker;

CREATE FUNCTION agent_economy.claim_collection_job(
    p_namespace_id uuid,
    p_lease_owner text,
    p_lease_seconds bigint
)
RETURNS TABLE (
    job_id uuid,
    mode text,
    job_kind text,
    input_sha256 text,
    attempt_count smallint,
    lease_owner text,
    lease_token uuid,
    collection_chain_scope text,
    collection_source_id text,
    collection_start_height bigint,
    collection_end_height bigint,
    collection_acquisition_contract text,
    collection_evidence_contract text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
BEGIN
    IF p_lease_owner !~ '^[A-Za-z0-9_.:-]{1,128}$'
        OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid collection lease request';
    END IF;

    UPDATE agent_economy.worker_jobs AS expired
    SET status = 'dead_letter', lease_owner = NULL, lease_token = NULL,
        lease_expires_at = NULL, last_error_code = 'lease_expired',
        updated_at = clock_timestamp()
    WHERE expired.namespace_id = p_namespace_id
        AND expired.mode = 'collect'
        AND expired.job_kind = 'chain-protocol-range'
        AND expired.status = 'leased'
        AND expired.lease_expires_at <= clock_timestamp()
        AND expired.attempt_count >= expired.max_attempts;

    RETURN QUERY
    WITH candidate AS (
        SELECT queued.namespace_id, queued.job_id
        FROM agent_economy.worker_jobs AS queued
        WHERE queued.namespace_id = p_namespace_id
            AND queued.mode = 'collect'
            AND queued.job_kind = 'chain-protocol-range'
            AND queued.attempt_count < queued.max_attempts
            AND (
                (queued.status IN ('pending', 'retryable')
                    AND queued.scheduled_for <= clock_timestamp())
                OR (queued.status = 'leased'
                    AND queued.lease_expires_at <= clock_timestamp())
            )
        ORDER BY queued.scheduled_for, queued.created_at, queued.job_id
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    )
    UPDATE agent_economy.worker_jobs AS claimed
    SET status = 'leased', lease_owner = p_lease_owner,
        lease_token = gen_random_uuid(),
        lease_expires_at = clock_timestamp() + (p_lease_seconds * interval '1 second'),
        attempt_count = claimed.attempt_count + 1, last_error_code = NULL,
        updated_at = clock_timestamp()
    FROM candidate
    WHERE claimed.namespace_id = candidate.namespace_id
        AND claimed.job_id = candidate.job_id
    RETURNING claimed.job_id, claimed.mode, claimed.job_kind, claimed.input_sha256,
        claimed.attempt_count, claimed.lease_owner, claimed.lease_token,
        claimed.collection_chain_scope, claimed.collection_source_id,
        claimed.collection_start_height, claimed.collection_end_height,
        claimed.collection_acquisition_contract, claimed.collection_evidence_contract;
END
$$;

CREATE FUNCTION agent_economy.complete_collection_job(
    p_namespace_id uuid,
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid,
    p_output_sha256 text
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    WITH completed AS (
        UPDATE agent_economy.worker_jobs AS job
        SET status = 'succeeded', output_sha256 = p_output_sha256,
            lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
            updated_at = clock_timestamp()
        WHERE p_output_sha256 ~ '^[0-9a-f]{64}$'
            AND job.namespace_id = p_namespace_id AND job.job_id = p_job_id
            AND job.mode = 'collect' AND job.job_kind = 'chain-protocol-range'
            AND job.status = 'leased' AND job.lease_owner = p_lease_owner
            AND job.lease_token = p_lease_token
            AND job.lease_expires_at > clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) = 1 FROM completed
$$;

CREATE FUNCTION agent_economy.fail_collection_job(
    p_namespace_id uuid,
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid,
    p_error_code text,
    p_retryable boolean,
    p_retry_delay_seconds bigint
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    WITH failed AS (
        UPDATE agent_economy.worker_jobs AS job
        SET status = CASE WHEN p_retryable AND job.attempt_count < job.max_attempts
                          THEN 'retryable' ELSE 'dead_letter' END,
            scheduled_for = CASE
                WHEN p_retryable AND job.attempt_count < job.max_attempts
                THEN clock_timestamp()
                    + (p_retry_delay_seconds * job.attempt_count * interval '1 second')
                ELSE job.scheduled_for
            END,
            lease_owner = NULL, lease_token = NULL, lease_expires_at = NULL,
            last_error_code = p_error_code, updated_at = clock_timestamp()
        WHERE p_error_code ~ '^[a-z0-9_]{1,64}$'
            AND p_retry_delay_seconds BETWEEN 0 AND 3600
            AND job.namespace_id = p_namespace_id AND job.job_id = p_job_id
            AND job.mode = 'collect' AND job.job_kind = 'chain-protocol-range'
            AND job.status = 'leased' AND job.lease_owner = p_lease_owner
            AND job.lease_token = p_lease_token
            AND job.lease_expires_at > clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) = 1 FROM failed
$$;

DO $$
DECLARE
    collector_role pg_roles%ROWTYPE;
    verifier_role pg_roles%ROWTYPE;
BEGIN
    BEGIN
        CREATE ROLE agent_economy_collector_runtime LOGIN NOINHERIT;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    BEGIN
        CREATE ROLE agent_economy_evidence_verifier_runtime LOGIN NOINHERIT;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;

    SELECT * INTO STRICT collector_role
    FROM pg_roles
    WHERE rolname = 'agent_economy_collector_runtime';

    IF NOT collector_role.rolcanlogin
        OR collector_role.rolinherit
        OR collector_role.rolsuper
        OR collector_role.rolcreatedb
        OR collector_role.rolcreaterole
        OR collector_role.rolreplication
        OR collector_role.rolbypassrls
    THEN
        RAISE EXCEPTION 'agent_economy_collector_runtime must be an unprivileged LOGIN role';
    END IF;

    IF EXISTS (
        SELECT 1 FROM pg_auth_members AS membership
        WHERE membership.member = collector_role.oid
    ) THEN
        RAISE EXCEPTION 'agent_economy_collector_runtime must not inherit roles';
    END IF;

    IF EXISTS (
        WITH RECURSIVE collector_members(member) AS (
            SELECT membership.member
            FROM pg_auth_members AS membership
            WHERE membership.roleid = collector_role.oid
            UNION
            SELECT membership.member
            FROM pg_auth_members AS membership
            JOIN collector_members ON membership.roleid = collector_members.member
        )
        SELECT 1 FROM collector_members
    ) THEN
        RAISE EXCEPTION 'agent_economy_collector_runtime must not have members';
    END IF;

    SELECT * INTO STRICT verifier_role
    FROM pg_roles
    WHERE rolname = 'agent_economy_evidence_verifier_runtime';
    IF NOT verifier_role.rolcanlogin
        OR verifier_role.rolinherit
        OR verifier_role.rolsuper
        OR verifier_role.rolcreatedb
        OR verifier_role.rolcreaterole
        OR verifier_role.rolreplication
        OR verifier_role.rolbypassrls
    THEN
        RAISE EXCEPTION 'agent_economy_evidence_verifier_runtime must be an unprivileged LOGIN role';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_auth_members AS membership
        WHERE membership.member = verifier_role.oid OR membership.roleid = verifier_role.oid
    ) THEN
        RAISE EXCEPTION 'agent_economy_evidence_verifier_runtime must be isolated';
    END IF;
END
$$;

GRANT USAGE ON SCHEMA agent_economy TO agent_economy_collector_runtime;
GRANT USAGE ON SCHEMA agent_economy TO agent_economy_evidence_verifier_runtime;
REVOKE ALL ON agent_economy.collection_evidence_attestations FROM PUBLIC;
REVOKE ALL ON agent_economy.collection_evidence_attestations FROM agent_economy_worker;
REVOKE ALL ON agent_economy.collection_evidence_attestations FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.attest_collection_evidence(
    uuid, uuid, uuid, text, text, text, text, bigint, bigint
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION agent_economy.attest_collection_evidence(
    uuid, uuid, uuid, text, text, text, text, bigint, bigint
) TO agent_economy_evidence_verifier_runtime;
REVOKE ALL ON FUNCTION agent_economy.claim_collection_job(uuid, text, bigint) FROM PUBLIC;

CREATE OR REPLACE FUNCTION agent_economy.renew_collection_job_lease(
    p_namespace_id uuid,
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid,
    p_lease_seconds bigint
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, agent_economy
AS $$
    UPDATE agent_economy.worker_jobs
    SET lease_expires_at = clock_timestamp() + make_interval(secs => p_lease_seconds::double precision),
        updated_at = clock_timestamp()
    WHERE namespace_id = p_namespace_id
      AND job_id = p_job_id
      AND mode = 'collect'
      AND job_kind = 'chain-protocol-range'
      AND status = 'leased'
      AND lease_owner = p_lease_owner
      AND lease_token = p_lease_token
      AND lease_expires_at > clock_timestamp()
      AND p_lease_seconds BETWEEN 1 AND 3600
    RETURNING true;
$$;

REVOKE ALL ON FUNCTION agent_economy.renew_collection_job_lease(
    uuid, uuid, text, uuid, bigint
) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.complete_collection_job(uuid, uuid, text, uuid, text)
    FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.fail_collection_job(
    uuid, uuid, text, uuid, text, boolean, bigint
) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.commit_collection_batch(
    uuid, uuid, text, uuid, text, text, text, text, bigint, bigint, bigint, jsonb, jsonb
) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.commit_collection_batch(
    uuid, uuid, text, uuid, text, text, text, text, bigint, bigint, bigint, jsonb, jsonb
) FROM agent_economy_worker;
GRANT EXECUTE ON FUNCTION agent_economy.claim_collection_job(uuid, text, bigint)
    TO agent_economy_collector_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.renew_collection_job_lease(uuid, uuid, text, uuid, bigint)
    TO agent_economy_collector_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.complete_collection_job(uuid, uuid, text, uuid, text)
    TO agent_economy_collector_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.fail_collection_job(
    uuid, uuid, text, uuid, text, boolean, bigint
) TO agent_economy_collector_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.commit_collection_batch(
    uuid, uuid, text, uuid, text, text, text, text, bigint, bigint, bigint, jsonb, jsonb
) TO agent_economy_collector_runtime;

COMMIT;
