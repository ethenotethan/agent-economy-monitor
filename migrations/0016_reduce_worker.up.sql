BEGIN;

CREATE TABLE agent_economy.reducer_job_inputs (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version = 1),
    reducer_version text NOT NULL CHECK (reducer_version = 'reducer@1'),
    attribution_version text NOT NULL CHECK (attribution_version = 'attribution@1'),
    chain_scope text NOT NULL CHECK (chain_scope IN ('ethereum', 'base', 'solana', 'tempo')),
    start_height bigint NOT NULL CHECK (start_height >= 0),
    end_height bigint NOT NULL CHECK (end_height >= start_height),
    input_manifest jsonb NOT NULL CHECK (jsonb_typeof(input_manifest) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, job_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id),
    CHECK (end_height - start_height + 1 <= 10000)
);

CREATE TABLE agent_economy.reducer_checkpoints (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    chain_scope text NOT NULL CHECK (chain_scope IN ('ethereum', 'base', 'solana', 'tempo')),
    next_height bigint NOT NULL CHECK (next_height >= 0),
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, chain_scope)
);

CREATE TABLE agent_economy.reduction_range_receipts (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    chain_scope text NOT NULL,
    start_height bigint NOT NULL,
    end_height bigint NOT NULL,
    input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    output_sha256 text NOT NULL CHECK (output_sha256 ~ '^[0-9a-f]{64}$'),
    reducer_version text NOT NULL,
    events_json jsonb NOT NULL CHECK (jsonb_typeof(events_json) = 'array'),
    finality_json jsonb NOT NULL CHECK (jsonb_typeof(finality_json) = 'array'),
    attributions_json jsonb NOT NULL CHECK (jsonb_typeof(attributions_json) = 'array'),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, chain_scope, start_height, end_height)
);

CREATE TABLE agent_economy.reducer_runtime_namespaces (
    login_name name PRIMARY KEY,
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    purpose text NOT NULL CHECK (purpose = 'reduce'),
    UNIQUE (namespace_id, purpose)
);

CREATE FUNCTION agent_economy.validate_reducer_job_input()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE
    job agent_economy.worker_jobs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT job
    FROM agent_economy.worker_jobs
    WHERE namespace_id = NEW.namespace_id AND job_id = NEW.job_id;
    IF job.mode <> 'reduce'
       OR job.job_kind <> 'canonical-observation-range-v1'
       OR job.status <> 'pending'
       OR job.input_sha256 <> encode(sha256(convert_to(NEW.input_manifest::text, 'UTF8')), 'hex')
       OR NEW.input_manifest <> jsonb_build_object(
            'schema_version', NEW.schema_version,
            'reducer_version', NEW.reducer_version,
            'attribution_version', NEW.attribution_version,
            'chain_scope', NEW.chain_scope,
            'start_height', NEW.start_height,
            'end_height', NEW.end_height,
            'observations', NEW.input_manifest -> 'observations',
            'finality', NEW.input_manifest -> 'finality',
            'settlements', NEW.input_manifest -> 'settlements'
       )
       OR jsonb_typeof(NEW.input_manifest -> 'observations') <> 'array'
       OR jsonb_typeof(NEW.input_manifest -> 'finality') <> 'array'
       OR jsonb_typeof(NEW.input_manifest -> 'settlements') <> 'array'
       OR jsonb_array_length(NEW.input_manifest -> 'observations') NOT BETWEEN 1 AND 50000
    THEN
        RAISE EXCEPTION 'invalid reducer job input';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER reducer_job_inputs_validate
BEFORE INSERT ON agent_economy.reducer_job_inputs
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_reducer_job_input();
CREATE TRIGGER reducer_job_inputs_immutable
BEFORE UPDATE OR DELETE ON agent_economy.reducer_job_inputs
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER reducer_job_inputs_truncate_immutable
BEFORE TRUNCATE ON agent_economy.reducer_job_inputs
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER reduction_range_receipts_immutable
BEFORE UPDATE OR DELETE ON agent_economy.reduction_range_receipts
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE FUNCTION agent_economy.bound_reducer_namespace()
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog
AS $$
    SELECT binding.namespace_id
    FROM agent_economy.reducer_runtime_namespaces AS binding
    WHERE binding.login_name = session_user
      AND binding.purpose = 'reduce'
$$;

CREATE FUNCTION agent_economy.claim_reducer_job(
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
    lease_token uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    bound_namespace uuid;
BEGIN
    bound_namespace := agent_economy.bound_reducer_namespace();
    IF bound_namespace IS NULL
       OR p_lease_owner !~ '^[A-Za-z0-9_.:-]{1,128}$'
       OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid reducer lease request';
    END IF;

    UPDATE agent_economy.worker_jobs AS expired
    SET status = 'dead_letter', lease_owner = NULL, lease_token = NULL,
        lease_expires_at = NULL, last_error_code = 'lease_expired',
        updated_at = clock_timestamp()
    WHERE expired.namespace_id = bound_namespace
      AND expired.mode = 'reduce'
      AND expired.job_kind = 'canonical-observation-range-v1'
      AND expired.status = 'leased'
      AND expired.lease_expires_at <= clock_timestamp()
      AND expired.attempt_count >= expired.max_attempts;

    RETURN QUERY
    WITH candidate AS (
        SELECT queued.namespace_id, queued.job_id
        FROM agent_economy.worker_jobs AS queued
        JOIN agent_economy.reducer_job_inputs AS input
          ON input.namespace_id = queued.namespace_id AND input.job_id = queued.job_id
        WHERE queued.namespace_id = bound_namespace
          AND queued.mode = 'reduce'
          AND queued.job_kind = 'canonical-observation-range-v1'
          AND queued.attempt_count < queued.max_attempts
          AND ((queued.status IN ('pending', 'retryable') AND queued.scheduled_for <= clock_timestamp())
            OR (queued.status = 'leased' AND queued.lease_expires_at <= clock_timestamp()))
        ORDER BY queued.scheduled_for, queued.created_at, queued.job_id
        FOR UPDATE OF queued SKIP LOCKED
        LIMIT 1
    )
    UPDATE agent_economy.worker_jobs AS claimed
    SET status = 'leased', lease_owner = p_lease_owner,
        lease_token = gen_random_uuid(),
        lease_expires_at = clock_timestamp() + p_lease_seconds * interval '1 second',
        attempt_count = claimed.attempt_count + 1,
        last_error_code = NULL,
        updated_at = clock_timestamp()
    FROM candidate
    WHERE claimed.namespace_id = candidate.namespace_id
      AND claimed.job_id = candidate.job_id
    RETURNING claimed.job_id, claimed.mode, claimed.job_kind, claimed.input_sha256,
              claimed.attempt_count, claimed.lease_owner, claimed.lease_token;
END
$$;

CREATE FUNCTION agent_economy.load_reducer_job_input(
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid
)
RETURNS bytea
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog
AS $$
    SELECT convert_to(input.input_manifest::text, 'UTF8')
    FROM agent_economy.worker_jobs AS job
    JOIN agent_economy.reducer_job_inputs AS input
      ON input.namespace_id = job.namespace_id AND input.job_id = job.job_id
    WHERE job.namespace_id = agent_economy.bound_reducer_namespace()
      AND job.job_id = p_job_id
      AND job.mode = 'reduce'
      AND job.job_kind = 'canonical-observation-range-v1'
      AND job.status = 'leased'
      AND job.lease_owner = p_lease_owner
      AND job.lease_token = p_lease_token
      AND job.lease_expires_at > clock_timestamp()
$$;

CREATE FUNCTION agent_economy.commit_reduction_batch(
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid,
    p_input_sha256 text,
    p_output_sha256 text,
    p_reducer_version text,
    p_chain_scope text,
    p_start_height bigint,
    p_end_height bigint,
    p_events_json jsonb,
    p_finality_json jsonb,
    p_attributions_json jsonb
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    bound_namespace uuid;
    current_height bigint;
    existing_receipt agent_economy.reduction_range_receipts%ROWTYPE;
BEGIN
    bound_namespace := agent_economy.bound_reducer_namespace();
    IF bound_namespace IS NULL
       OR p_input_sha256 !~ '^[0-9a-f]{64}$'
       OR p_output_sha256 !~ '^[0-9a-f]{64}$'
       OR p_reducer_version <> 'reducer@1'
       OR p_chain_scope NOT IN ('ethereum', 'base', 'solana', 'tempo')
       OR p_start_height < 0 OR p_end_height < p_start_height
       OR jsonb_typeof(p_events_json) <> 'array'
       OR jsonb_typeof(p_finality_json) <> 'array'
       OR jsonb_typeof(p_attributions_json) <> 'array'
       OR jsonb_array_length(p_events_json) NOT BETWEEN 1 AND 50000
    THEN
        RAISE EXCEPTION 'invalid reduction batch';
    END IF;

    PERFORM 1
    FROM agent_economy.worker_jobs AS job
    JOIN agent_economy.reducer_job_inputs AS input
      ON input.namespace_id = job.namespace_id AND input.job_id = job.job_id
    WHERE job.namespace_id = bound_namespace
      AND job.job_id = p_job_id
      AND job.mode = 'reduce'
      AND job.job_kind = 'canonical-observation-range-v1'
      AND job.status = 'leased'
      AND job.lease_owner = p_lease_owner
      AND job.lease_token = p_lease_token
      AND job.lease_expires_at > clock_timestamp()
      AND job.input_sha256 = p_input_sha256
      AND input.reducer_version = p_reducer_version
      AND input.chain_scope = p_chain_scope
      AND input.start_height = p_start_height
      AND input.end_height = p_end_height
    FOR UPDATE OF job;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(p_events_json) AS event(item)
        WHERE jsonb_typeof(item) <> 'object'
           OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 4
           OR (item ->> 'protocol') NOT IN ('x402', 'mpp')
           OR (item ->> 'canonical_event_id') !~
              ('^event:' || (item ->> 'protocol') || ':sha256:[0-9a-f]{64}$')
           OR NOT CASE WHEN (item ->> 'event_at_unix_ms') ~ '^[0-9]{1,15}$'
                       THEN (item ->> 'event_at_unix_ms')::bigint > 0 ELSE false END
           OR jsonb_typeof(item -> 'observation_links') <> 'array'
           OR jsonb_array_length(item -> 'observation_links') = 0
           OR EXISTS (
                SELECT 1
                FROM jsonb_array_elements(item -> 'observation_links') AS link(link_item)
                WHERE jsonb_typeof(link_item) <> 'object'
                   OR (SELECT count(*) FROM jsonb_object_keys(link_item)) <> 3
                   OR (link_item ->> 'source_id') = ''
                   OR (link_item ->> 'observation_id') !~ '^sha256:[0-9a-f]{64}$'
                   OR (link_item ->> 'support_role') NOT IN ('supporting', 'conflicting')
           )
    ) THEN
        RAISE EXCEPTION 'invalid reduced event';
    END IF;

    IF (
        SELECT count(*) FROM jsonb_array_elements(
            (SELECT input_manifest -> 'observations'
             FROM agent_economy.reducer_job_inputs
             WHERE namespace_id = bound_namespace AND job_id = p_job_id)
        ) AS observation(item)
    ) <> (
        SELECT count(DISTINCT link_item ->> 'observation_id')
        FROM jsonb_array_elements(p_events_json) AS event(item)
        CROSS JOIN LATERAL jsonb_array_elements(item -> 'observation_links') AS link(link_item)
    ) OR EXISTS (
        SELECT 1
        FROM jsonb_array_elements(
            (SELECT input_manifest -> 'observations'
             FROM agent_economy.reducer_job_inputs
             WHERE namespace_id = bound_namespace AND job_id = p_job_id)
        ) AS manifest(item)
        WHERE NOT EXISTS (
            SELECT 1
            FROM jsonb_array_elements(p_events_json) AS event(event_item)
            CROSS JOIN LATERAL jsonb_array_elements(event_item -> 'observation_links') AS link(link_item)
            JOIN agent_economy.observations AS observation
              ON observation.namespace_id = bound_namespace
             AND observation.chain_scope = p_chain_scope
             AND observation.source_id = link_item ->> 'source_id'
             AND observation.protocol = event_item ->> 'protocol'
             AND observation.observation_id = link_item ->> 'observation_id'
            JOIN agent_economy.provenance_records AS provenance
              ON provenance.namespace_id = observation.namespace_id
             AND provenance.provenance_id = observation.provenance_id
            WHERE link_item ->> 'source_id' = item ->> 'source_id'
              AND link_item ->> 'observation_id' = item ->> 'observation_id'
              AND provenance.block_reference = item ->> 'height'
              AND observation.observation_hash = encode(
                    sha256(decode(item ->> 'encoded_base64', 'base64')), 'hex')
              AND (item ->> 'height')::bigint BETWEEN p_start_height AND p_end_height
        )
    ) THEN
        RAISE EXCEPTION 'reduction output does not exactly cover immutable observations';
    END IF;

    IF jsonb_array_length(p_finality_json) <> (
        SELECT jsonb_array_length(input_manifest -> 'finality')
        FROM agent_economy.reducer_job_inputs
        WHERE namespace_id = bound_namespace AND job_id = p_job_id
    ) OR jsonb_array_length(p_attributions_json) <> (
        SELECT jsonb_array_length(input_manifest -> 'settlements')
        FROM agent_economy.reducer_job_inputs
        WHERE namespace_id = bound_namespace AND job_id = p_job_id
    ) THEN
     RAISE EXCEPTION 'reduction output does not exactly cover immutable finality and settlements';
 END IF;

 IF EXISTS (
     SELECT 1
     FROM jsonb_array_elements(p_finality_json) AS output(item)
     WHERE jsonb_typeof(item) <> 'object'
        OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 14
        OR (item ->> 'status') NOT IN ('observed', 'confirmed', 'finalized', 'orphaned', 'reverted')
        OR (item ->> 'current_status') NOT IN ('observed', 'confirmed', 'finalized', 'orphaned', 'reverted')
        OR (item ->> 'timeline_state_hash') !~ '^[0-9a-f]{64}$'
        OR jsonb_typeof(item -> 'accepted') <> 'boolean'
        OR jsonb_typeof(item -> 'current') <> 'boolean'
        OR NOT EXISTS (
             SELECT 1
             FROM jsonb_array_elements((
                 SELECT input_manifest -> 'finality'
                 FROM agent_economy.reducer_job_inputs
                 WHERE namespace_id = bound_namespace AND job_id = p_job_id
             )) AS manifest(source)
             WHERE source ->> 'canonical_event_id' = item ->> 'canonical_event_id'
               AND source ->> 'transaction_id' = item ->> 'transaction_id'
               AND source ->> 'source_id' = item ->> 'source_id'
               AND source ->> 'provenance_id' = item ->> 'provenance_id'
               AND source ->> 'evidence_id' = item ->> 'evidence_id'
               AND source ->> 'asserted_at_unix_ms' = item ->> 'asserted_at_unix_ms'
               AND source ->> 'block_hash' = item ->> 'block_hash'
               AND CASE source ->> 'kind'
                   WHEN 'evm' THEN
                       item ->> 'position' = source ->> 'block_number'
                       AND item -> 'basis' = jsonb_build_object(
                           'kind', 'evm',
                           'block_number', source -> 'block_number',
                           'block_hash', source -> 'block_hash',
                           'canonical_block_hash', source -> 'canonical_block_hash',
                           'latest_block', source -> 'latest_block',
                           'finalized_block', source -> 'finalized_block',
                           'execution', source -> 'execution'
                       )
                   WHEN 'solana' THEN
                       item ->> 'position' = source ->> 'slot'
                       AND item -> 'basis' = jsonb_build_object(
                           'kind', 'solana',
                           'slot', source -> 'slot',
                           'block_hash', source -> 'block_hash',
                           'canonical_block_hash', source -> 'canonical_block_hash',
                           'commitment', source -> 'commitment',
                           'execution', source -> 'execution'
                       )
                   ELSE false
               END
        )
 ) OR (
     SELECT count(DISTINCT item ->> 'evidence_id')
     FROM jsonb_array_elements(p_finality_json) AS output(item)
 ) <> jsonb_array_length(p_finality_json) THEN
     RAISE EXCEPTION 'reduction finality output is not bound to immutable finality evidence';
 END IF;

 IF EXISTS (
     SELECT 1
     FROM jsonb_array_elements(p_attributions_json) AS output(item)
     WHERE jsonb_typeof(item) <> 'object'
        OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 19
        OR jsonb_typeof(item -> 'evidence_ids') <> 'array'
        OR jsonb_typeof(item -> 'requirements') <> 'array'
        OR jsonb_typeof(item -> 'candidates') <> 'array'
        OR (item ->> 'input_snapshot_hash') !~ '^[0-9a-f]{64}$'
        OR (item ->> 'state_hash') !~ '^[0-9a-f]{64}$'
        OR item ->> 'state_hash' <> encode(
             sha256(decode(item ->> 'result_encoded_base64', 'base64')), 'hex')
        OR NOT EXISTS (
             SELECT 1
             FROM jsonb_array_elements((
                 SELECT input_manifest -> 'settlements'
                 FROM agent_economy.reducer_job_inputs
                 WHERE namespace_id = bound_namespace AND job_id = p_job_id
             )) AS manifest(source)
             WHERE source -> 'settlement' ->> 'id' = item ->> 'settlement_id'
               AND source ->> 'canonical_event_id' = item ->> 'canonical_event_id'
               AND source ->> 'transaction_id' = item ->> 'transaction_id'
               AND source ->> 'settled_at_unix_ms' = item ->> 'settled_at_unix_ms'
               AND source ->> 'source_id' = item ->> 'source_id'
               AND source ->> 'provenance_id' = item ->> 'provenance_id'
               AND source -> 'settlement' ->> 'protocol' = item ->> 'protocol'
               AND source -> 'settlement' ->> 'asset' = item ->> 'asset'
               AND source -> 'settlement' ->> 'amount_atomic' = item ->> 'amount_atomic'
               AND source -> 'settlement' -> 'evidence_ids' = item -> 'evidence_ids'
               AND item ->> 'engine_version' = (
                   SELECT input.attribution_version
                   FROM agent_economy.reducer_job_inputs AS input
                   WHERE input.namespace_id = bound_namespace AND input.job_id = p_job_id
               )
               AND jsonb_array_length(source -> 'requirements') =
                   jsonb_array_length(item -> 'requirements')
               AND NOT EXISTS (
                   SELECT 1
                   FROM jsonb_array_elements(item -> 'requirements') AS output_requirement(requirement)
                   WHERE jsonb_typeof(requirement) <> 'object'
                      OR (SELECT count(*) FROM jsonb_object_keys(requirement)) <> 5
                      OR NOT EXISTS (
                           SELECT 1
                           FROM jsonb_array_elements(source -> 'requirements') AS manifest_requirement(expected)
                           WHERE expected ->> 'id' = requirement ->> 'requirement_id'
                             AND expected ->> 'payment_option_id' = requirement ->> 'payment_option_id'
                             AND expected ->> 'endpoint_id' = requirement ->> 'endpoint_id'
                             AND expected ->> 'service_id' = requirement ->> 'service_id'
                             AND expected -> 'evidence_ids' = requirement -> 'evidence_ids'
                      )
               )
        )
 ) OR (
     SELECT count(DISTINCT item ->> 'settlement_id')
     FROM jsonb_array_elements(p_attributions_json) AS output(item)
 ) <> jsonb_array_length(p_attributions_json) THEN
     RAISE EXCEPTION 'reduction attribution output is not bound to immutable settlement evidence';
 END IF;

 INSERT INTO agent_economy.reducer_checkpoints
        (namespace_id, chain_scope, next_height)
    VALUES (bound_namespace, p_chain_scope, p_start_height)
    ON CONFLICT (namespace_id, chain_scope) DO NOTHING;
    SELECT checkpoint.next_height INTO STRICT current_height
    FROM agent_economy.reducer_checkpoints AS checkpoint
    WHERE checkpoint.namespace_id = bound_namespace AND checkpoint.chain_scope = p_chain_scope
    FOR UPDATE;
    IF current_height NOT IN (p_start_height, p_end_height + 1) THEN
        RETURN false;
    END IF;

    SELECT * INTO existing_receipt
    FROM agent_economy.reduction_range_receipts AS receipt
    WHERE receipt.namespace_id = bound_namespace
      AND receipt.chain_scope = p_chain_scope
      AND receipt.start_height = p_start_height
      AND receipt.end_height = p_end_height
    FOR UPDATE;
    IF FOUND AND (existing_receipt.input_sha256 <> p_input_sha256
       OR existing_receipt.output_sha256 <> p_output_sha256
       OR existing_receipt.reducer_version <> p_reducer_version
       OR existing_receipt.events_json <> p_events_json
       OR existing_receipt.finality_json <> p_finality_json
       OR existing_receipt.attributions_json <> p_attributions_json) THEN
        RAISE EXCEPTION 'reduction replay differs from immutable receipt';
    END IF;

    INSERT INTO agent_economy.canonical_events
        (namespace_id, protocol, canonical_event_id, chain_scope, event_at,
         reducer_version, canonical_state_hash)
    SELECT bound_namespace, item ->> 'protocol', item ->> 'canonical_event_id',
           p_chain_scope, to_timestamp((item ->> 'event_at_unix_ms')::bigint / 1000.0),
           p_reducer_version, p_output_sha256
    FROM jsonb_array_elements(p_events_json) AS event(item)
    ON CONFLICT (namespace_id, protocol, chain_scope, canonical_event_id) DO NOTHING;

    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_events_json) AS event(item)
        JOIN agent_economy.canonical_events AS existing
          ON existing.namespace_id = bound_namespace
         AND existing.protocol = item ->> 'protocol'
         AND existing.chain_scope = p_chain_scope
         AND existing.canonical_event_id = item ->> 'canonical_event_id'
        WHERE existing.event_at <> to_timestamp((item ->> 'event_at_unix_ms')::bigint / 1000.0)
           OR existing.reducer_version <> p_reducer_version
           OR existing.canonical_state_hash <> p_output_sha256
    ) THEN
        RAISE EXCEPTION 'contradictory canonical event replay';
    END IF;

    INSERT INTO agent_economy.canonical_event_observations
        (namespace_id, protocol, canonical_event_id, chain_scope,
         source_id, observation_id, support_role)
    SELECT bound_namespace, item ->> 'protocol', item ->> 'canonical_event_id', p_chain_scope,
           link_item ->> 'source_id', link_item ->> 'observation_id',
           link_item ->> 'support_role'
    FROM jsonb_array_elements(p_events_json) AS event(item)
    CROSS JOIN LATERAL jsonb_array_elements(item -> 'observation_links') AS link(link_item)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.event_finality_assertions (
        namespace_id, protocol, chain_scope, canonical_event_id,
        assertion_sequence, accepted, asserted_status, current_status,
        asserted_at, source_id, provenance_id, transaction_id,
        basis_kind, position, block_hash, canonical_block_hash,
        latest_position, finalized_position, confirmations_required,
        commitment, execution_outcome, finality_state_hash
    )
    SELECT
        bound_namespace,
        split_part(item ->> 'canonical_event_id', ':', 2),
        p_chain_scope,
        item ->> 'canonical_event_id',
        COALESCE((
            SELECT max(existing.assertion_sequence)
            FROM agent_economy.event_finality_assertions AS existing
            WHERE existing.namespace_id = bound_namespace
              AND existing.protocol = split_part(item ->> 'canonical_event_id', ':', 2)
              AND existing.chain_scope = p_chain_scope
              AND existing.canonical_event_id = item ->> 'canonical_event_id'
        ), 0) + row_number() OVER (
            PARTITION BY item ->> 'canonical_event_id'
            ORDER BY (item ->> 'asserted_at_unix_ms')::bigint, item ->> 'evidence_id'
        )::integer,
        (item ->> 'accepted')::boolean,
        item ->> 'status',
        item ->> 'current_status',
        to_timestamp((item ->> 'asserted_at_unix_ms')::bigint / 1000.0),
        item ->> 'source_id',
        (item ->> 'provenance_id')::uuid,
        item ->> 'transaction_id',
        item -> 'basis' ->> 'kind',
        (item ->> 'position')::bigint,
        item ->> 'block_hash',
        item -> 'basis' ->> 'canonical_block_hash',
        CASE WHEN item -> 'basis' ->> 'kind' = 'evm'
             THEN (item -> 'basis' ->> 'latest_block')::bigint END,
        CASE WHEN item -> 'basis' ->> 'kind' = 'evm'
             THEN (item -> 'basis' ->> 'finalized_block')::bigint END,
        CASE WHEN item -> 'basis' ->> 'kind' = 'evm'
             THEN (SELECT (manifest_item ->> 'confirmations')::bigint
                   FROM jsonb_array_elements((SELECT input_manifest -> 'finality'
                       FROM agent_economy.reducer_job_inputs
                       WHERE namespace_id = bound_namespace AND job_id = p_job_id)) AS manifest(manifest_item)
                   WHERE manifest_item ->> 'evidence_id' = item ->> 'evidence_id'
                   LIMIT 1) END,
        CASE WHEN item -> 'basis' ->> 'kind' = 'solana'
             THEN item -> 'basis' ->> 'commitment' END,
        item -> 'basis' ->> 'execution',
        item ->> 'timeline_state_hash'
    FROM jsonb_array_elements(p_finality_json) AS finality(item)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.settlements (
        namespace_id, chain_scope, settlement_id, protocol,
        canonical_event_id, source_id, asset, amount_atomic,
        settled_at, provenance_id
    )
    SELECT bound_namespace, p_chain_scope, item ->> 'settlement_id',
           item ->> 'protocol', item ->> 'canonical_event_id',
           item ->> 'source_id', item ->> 'asset',
           (item ->> 'amount_atomic')::numeric,
           to_timestamp((item ->> 'settled_at_unix_ms')::bigint / 1000.0),
           (item ->> 'provenance_id')::uuid
    FROM jsonb_array_elements(p_attributions_json) AS attribution(item)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.attribution_runs (
        namespace_id, chain_scope, settlement_id, attribution_version,
        engine_version, encoding_version, match_method,
        explicit_requirement_id, input_snapshot_hash, level,
        settlement_evidence_id
    )
    SELECT bound_namespace, p_chain_scope, item ->> 'settlement_id', 1,
           item ->> 'engine_version', 'aem-attribution-result-v1',
           item ->> 'method', item ->> 'explicit_requirement_id',
           item ->> 'input_snapshot_hash', item ->> 'level',
           item -> 'evidence_ids' ->> 0
    FROM jsonb_array_elements(p_attributions_json) AS attribution(item)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.attribution_run_evidence (
        namespace_id, chain_scope, settlement_id, attribution_version,
        evidence_id, evidence_role
    )
    SELECT bound_namespace, p_chain_scope, item ->> 'settlement_id', 1,
           evidence_id #>> '{}', 'settlement'
    FROM jsonb_array_elements(p_attributions_json) AS attribution(item)
    CROSS JOIN LATERAL jsonb_array_elements(item -> 'evidence_ids') AS evidence(evidence_id)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.attribution_run_requirements (
        namespace_id, chain_scope, settlement_id, attribution_version,
        requirement_id, payment_option_id, endpoint_id, service_id,
        requirement_evidence_id
    )
    SELECT bound_namespace, p_chain_scope, item ->> 'settlement_id', 1,
           requirement ->> 'requirement_id', requirement ->> 'payment_option_id',
           requirement ->> 'endpoint_id', requirement ->> 'service_id',
           requirement -> 'evidence_ids' ->> 0
    FROM jsonb_array_elements(p_attributions_json) AS attribution(item)
    CROSS JOIN LATERAL jsonb_array_elements(item -> 'requirements') AS requirement_row(requirement)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.attribution_candidates (
        namespace_id, chain_scope, settlement_id, attribution_version,
        candidate_id, requirement_id, payment_option_id, endpoint_id,
        service_id, confidence, settlement_evidence_id,
        requirement_evidence_id
    )
    SELECT bound_namespace, p_chain_scope, item ->> 'settlement_id', 1,
           candidate ->> 'attribution_id', candidate ->> 'requirement_id',
           candidate ->> 'payment_option_id', candidate ->> 'endpoint_id',
           candidate ->> 'service_id',
           (candidate ->> 'confidence_bps')::numeric / 10000,
           item -> 'evidence_ids' ->> 0,
           candidate ->> 'requirement_evidence_id'
    FROM jsonb_array_elements(p_attributions_json) AS attribution(item)
    CROSS JOIN LATERAL jsonb_array_elements(item -> 'candidates') AS candidate_row(candidate)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.attribution_candidate_evidence (
        namespace_id, chain_scope, settlement_id, attribution_version,
        candidate_id, evidence_id, evidence_role
    )
    SELECT bound_namespace, p_chain_scope, item ->> 'settlement_id', 1,
           candidate ->> 'attribution_id', evidence_id #>> '{}',
           CASE WHEN evidence_id #>> '{}' = candidate ->> 'requirement_evidence_id'
                THEN 'requirement' ELSE 'settlement' END
    FROM jsonb_array_elements(p_attributions_json) AS attribution(item)
    CROSS JOIN LATERAL jsonb_array_elements(item -> 'candidates') AS candidate_row(candidate)
    CROSS JOIN LATERAL jsonb_array_elements(candidate -> 'evidence_ids') AS evidence(evidence_id)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.attribution_run_seals (
        namespace_id, chain_scope, settlement_id, attribution_version,
        result_encoding, state_hash
    )
    SELECT bound_namespace, p_chain_scope, item ->> 'settlement_id', 1,
           decode(item ->> 'result_encoded_base64', 'base64'), item ->> 'state_hash'
    FROM jsonb_array_elements(p_attributions_json) AS attribution(item)
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.reduction_range_receipts
        (namespace_id, chain_scope, start_height, end_height, input_sha256,
         output_sha256, reducer_version, events_json, finality_json, attributions_json)
    VALUES (bound_namespace, p_chain_scope, p_start_height, p_end_height,
            p_input_sha256, p_output_sha256, p_reducer_version, p_events_json,
            p_finality_json, p_attributions_json)
    ON CONFLICT DO NOTHING;

    IF current_height = p_start_height THEN
        UPDATE agent_economy.reducer_checkpoints AS checkpoint
        SET next_height = p_end_height + 1,
            version = checkpoint.version + 1,
            updated_at = clock_timestamp()
        WHERE checkpoint.namespace_id = bound_namespace
          AND checkpoint.chain_scope = p_chain_scope
          AND checkpoint.next_height = p_start_height;
        IF NOT FOUND THEN RETURN false; END IF;
    END IF;
    RETURN true;
END
$$;

CREATE FUNCTION agent_economy.renew_reducer_job_lease(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid, p_lease_seconds bigint
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT agent_economy.renew_worker_job_lease(
        agent_economy.bound_reducer_namespace(), p_job_id, p_lease_owner,
        p_lease_token, p_lease_seconds
    )
$$;

CREATE FUNCTION agent_economy.complete_reducer_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid, p_output_sha256 text
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT CASE WHEN EXISTS (
        SELECT 1
        FROM agent_economy.worker_jobs AS job
        JOIN agent_economy.reducer_job_inputs AS input
          ON input.namespace_id = job.namespace_id AND input.job_id = job.job_id
        JOIN agent_economy.reduction_range_receipts AS receipt
          ON receipt.namespace_id = input.namespace_id
         AND receipt.chain_scope = input.chain_scope
         AND receipt.start_height = input.start_height
         AND receipt.end_height = input.end_height
         AND receipt.input_sha256 = job.input_sha256
         AND receipt.output_sha256 = p_output_sha256
         AND receipt.reducer_version = input.reducer_version
        JOIN agent_economy.reducer_checkpoints AS checkpoint
          ON checkpoint.namespace_id = input.namespace_id
         AND checkpoint.chain_scope = input.chain_scope
         AND checkpoint.next_height = input.end_height + 1
        WHERE job.namespace_id = agent_economy.bound_reducer_namespace()
          AND job.job_id = p_job_id
          AND job.status = 'leased'
          AND job.lease_owner = p_lease_owner
          AND job.lease_token = p_lease_token
          AND job.lease_expires_at > clock_timestamp()
    ) THEN agent_economy.complete_worker_job(
        agent_economy.bound_reducer_namespace(), p_job_id, p_lease_owner,
        p_lease_token, p_output_sha256
    ) ELSE false END
$$;

CREATE FUNCTION agent_economy.fail_reducer_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid,
    p_error_code text, p_retryable boolean, p_retry_delay_seconds bigint
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    SELECT agent_economy.fail_worker_job(
        agent_economy.bound_reducer_namespace(), p_job_id, p_lease_owner,
        p_lease_token, p_error_code, p_retryable, p_retry_delay_seconds
    )
$$;

DO $$
DECLARE reducer_role pg_roles%ROWTYPE;
BEGIN
    BEGIN
        CREATE ROLE agent_economy_reducer_runtime LOGIN NOINHERIT;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    SELECT * INTO STRICT reducer_role FROM pg_roles
    WHERE rolname = 'agent_economy_reducer_runtime';
    IF NOT reducer_role.rolcanlogin OR reducer_role.rolinherit OR reducer_role.rolsuper
       OR reducer_role.rolcreatedb OR reducer_role.rolcreaterole
       OR reducer_role.rolreplication OR reducer_role.rolbypassrls
    THEN
        RAISE EXCEPTION 'agent_economy_reducer_runtime must be an unprivileged LOGIN NOINHERIT role';
    END IF;
    IF EXISTS (
        SELECT 1 FROM pg_auth_members
        WHERE member = reducer_role.oid OR roleid = reducer_role.oid
    ) THEN
        RAISE EXCEPTION 'agent_economy_reducer_runtime must be isolated';
    END IF;
END
$$;

DO $migration$
BEGIN
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO agent_economy_reducer_runtime',
        current_database()
    );
END
$migration$;

REVOKE ALL ON agent_economy.reducer_job_inputs FROM PUBLIC;
REVOKE ALL ON agent_economy.reducer_job_inputs FROM agent_economy_reducer_runtime;
REVOKE ALL ON agent_economy.reducer_checkpoints FROM PUBLIC;
REVOKE ALL ON agent_economy.reducer_checkpoints FROM agent_economy_reducer_runtime;
REVOKE ALL ON agent_economy.reduction_range_receipts FROM PUBLIC;
REVOKE ALL ON agent_economy.reduction_range_receipts FROM agent_economy_reducer_runtime;
REVOKE ALL ON agent_economy.reducer_runtime_namespaces FROM PUBLIC;
REVOKE ALL ON agent_economy.reducer_runtime_namespaces FROM agent_economy_reducer_runtime;
REVOKE ALL ON agent_economy.observations FROM agent_economy_reducer_runtime;
REVOKE ALL ON agent_economy.canonical_events FROM agent_economy_reducer_runtime;
REVOKE ALL ON agent_economy.canonical_event_observations FROM agent_economy_reducer_runtime;
REVOKE ALL ON FUNCTION agent_economy.validate_reducer_job_input() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.bound_reducer_namespace() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.claim_reducer_job(text, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.load_reducer_job_input(uuid, text, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.commit_reduction_batch(
    uuid, text, uuid, text, text, text, text, bigint, bigint, jsonb, jsonb, jsonb
) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.renew_reducer_job_lease(uuid, text, uuid, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.complete_reducer_job(uuid, text, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.fail_reducer_job(uuid, text, uuid, text, boolean, bigint) FROM PUBLIC;
GRANT USAGE ON SCHEMA agent_economy TO agent_economy_reducer_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.bound_reducer_namespace() TO agent_economy_reducer_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.claim_reducer_job(text, bigint) TO agent_economy_reducer_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.load_reducer_job_input(uuid, text, uuid) TO agent_economy_reducer_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.commit_reduction_batch(
    uuid, text, uuid, text, text, text, text, bigint, bigint, jsonb, jsonb, jsonb
) TO agent_economy_reducer_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.renew_reducer_job_lease(uuid, text, uuid, bigint) TO agent_economy_reducer_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.complete_reducer_job(uuid, text, uuid, text) TO agent_economy_reducer_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.fail_reducer_job(uuid, text, uuid, text, boolean, bigint) TO agent_economy_reducer_runtime;

COMMIT;
