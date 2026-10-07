BEGIN;

CREATE TABLE agent_economy.classifier_job_inputs (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    input_manifest jsonb NOT NULL CHECK (jsonb_typeof(input_manifest) = 'object'),
    expected_output_sha256 text NOT NULL CHECK (expected_output_sha256 ~ '^[0-9a-f]{64}$'),
    expected_input_snapshot_hash text NOT NULL CHECK (expected_input_snapshot_hash ~ '^[0-9a-f]{64}$'),
    expected_label_set_hash text NOT NULL CHECK (expected_label_set_hash ~ '^[0-9a-f]{64}$'),
    expected_features_json jsonb NOT NULL CHECK (jsonb_typeof(expected_features_json) = 'object'),
    expected_claims_json jsonb NOT NULL CHECK (jsonb_typeof(expected_claims_json) = 'array'),
    expected_evidence_ids text[] NOT NULL CHECK (cardinality(expected_evidence_ids) BETWEEN 1 AND 50000),
    expected_result_encoding bytea NOT NULL CHECK (octet_length(expected_result_encoding) > 0),
    expected_state_hash text NOT NULL CHECK (expected_state_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, job_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id)
);

CREATE TABLE agent_economy.classifier_runtime_namespaces (
    login_name name PRIMARY KEY,
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    purpose text NOT NULL CHECK (purpose = 'classify'),
    UNIQUE (namespace_id, purpose)
);

CREATE TABLE agent_economy.classifier_job_receipts (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    derived_output_sha256 text NOT NULL CHECK (derived_output_sha256 ~ '^[0-9a-f]{64}$'),
    canonical_output_sha256 text NOT NULL CHECK (canonical_output_sha256 ~ '^[0-9a-f]{64}$'),
    run_id text NOT NULL,
    run_version integer NOT NULL CHECK (run_version > 0),
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, job_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id),
    FOREIGN KEY (namespace_id, run_id, run_version)
        REFERENCES agent_economy.classification_run_seals (namespace_id, run_id, run_version)
);

CREATE FUNCTION agent_economy.validate_classifier_job_input()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
DECLARE job agent_economy.worker_jobs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT job
    FROM agent_economy.worker_jobs
    WHERE namespace_id = NEW.namespace_id AND job_id = NEW.job_id;
    IF job.mode <> 'classify'
       OR job.job_kind <> 'buyer-behavior-v1'
       OR job.status <> 'pending'
       OR job.input_sha256 <> encode(sha256(convert_to(NEW.input_manifest::text, 'UTF8')), 'hex')
       OR NEW.input_manifest <> jsonb_build_object(
            'schema_version', NEW.input_manifest -> 'schema_version',
            'buyer_handle_id', NEW.input_manifest -> 'buyer_handle_id',
            'run_id', NEW.input_manifest -> 'run_id',
            'run_version', NEW.input_manifest -> 'run_version',
            'classifier_version', NEW.input_manifest -> 'classifier_version',
            'feature_version', NEW.input_manifest -> 'feature_version',
            'window_start_unix_seconds', NEW.input_manifest -> 'window_start_unix_seconds',
            'window_end_unix_seconds', NEW.input_manifest -> 'window_end_unix_seconds',
            'provenance_id', NEW.input_manifest -> 'provenance_id',
            'activities', NEW.input_manifest -> 'activities',
            'labels', NEW.input_manifest -> 'labels'
       )
       OR NEW.input_manifest ->> 'schema_version' <> '1'
       OR jsonb_typeof(NEW.input_manifest -> 'activities') <> 'array'
       OR jsonb_array_length(NEW.input_manifest -> 'activities') NOT BETWEEN 1 AND 50000
       OR jsonb_typeof(NEW.input_manifest -> 'labels') <> 'array'
       OR jsonb_array_length(NEW.input_manifest -> 'labels') NOT BETWEEN 1 AND 1000
    THEN
        RAISE EXCEPTION 'invalid classifier job input';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER classifier_job_inputs_validate
BEFORE INSERT ON agent_economy.classifier_job_inputs
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_classifier_job_input();
CREATE TRIGGER classifier_job_inputs_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classifier_job_inputs
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classifier_job_inputs_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classifier_job_inputs
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classifier_job_receipts_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classifier_job_receipts
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classifier_job_receipts_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classifier_job_receipts
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE FUNCTION agent_economy.bound_classifier_namespace()
RETURNS uuid
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog
AS $$
    SELECT binding.namespace_id
    FROM agent_economy.classifier_runtime_namespaces AS binding
    WHERE binding.login_name = session_user
      AND binding.purpose = 'classify'
$$;

CREATE FUNCTION agent_economy.claim_classifier_job(
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
DECLARE bound_namespace uuid;
BEGIN
    bound_namespace := agent_economy.bound_classifier_namespace();
    IF bound_namespace IS NULL
       OR p_lease_owner !~ '^[A-Za-z0-9_.:-]{1,128}$'
       OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid classifier lease request';
    END IF;

    UPDATE agent_economy.worker_jobs AS expired
    SET status = 'dead_letter', lease_owner = NULL, lease_token = NULL,
        lease_expires_at = NULL, last_error_code = 'lease_expired',
        updated_at = clock_timestamp()
    WHERE expired.namespace_id = bound_namespace
      AND expired.mode = 'classify'
      AND expired.job_kind = 'buyer-behavior-v1'
      AND expired.status = 'leased'
      AND expired.lease_expires_at <= clock_timestamp()
      AND expired.attempt_count >= expired.max_attempts;

    RETURN QUERY
    WITH candidate AS (
        SELECT queued.namespace_id, queued.job_id
        FROM agent_economy.worker_jobs AS queued
        JOIN agent_economy.classifier_job_inputs AS input
          ON input.namespace_id = queued.namespace_id AND input.job_id = queued.job_id
        WHERE queued.namespace_id = bound_namespace
          AND queued.mode = 'classify'
          AND queued.job_kind = 'buyer-behavior-v1'
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
        last_error_code = NULL, updated_at = clock_timestamp()
    FROM candidate
    WHERE claimed.namespace_id = candidate.namespace_id
      AND claimed.job_id = candidate.job_id
    RETURNING claimed.job_id, claimed.mode, claimed.job_kind, claimed.input_sha256,
              claimed.attempt_count, claimed.lease_owner, claimed.lease_token;
END
$$;

CREATE FUNCTION agent_economy.load_classifier_job_input(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid
)
RETURNS bytea
LANGUAGE sql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog
AS $$
    SELECT convert_to(input.input_manifest::text, 'UTF8')
    FROM agent_economy.worker_jobs AS job
    JOIN agent_economy.classifier_job_inputs AS input
      ON input.namespace_id = job.namespace_id AND input.job_id = job.job_id
    WHERE job.namespace_id = agent_economy.bound_classifier_namespace()
      AND job.job_id = p_job_id
      AND job.mode = 'classify'
      AND job.job_kind = 'buyer-behavior-v1'
      AND job.status = 'leased'
      AND job.lease_owner = p_lease_owner
      AND job.lease_token = p_lease_token
      AND job.lease_expires_at > clock_timestamp()
$$;

CREATE FUNCTION agent_economy.commit_classification_batch(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid,
    p_input_sha256 text, p_output_sha256 text,
    p_run_id text, p_run_version integer, p_buyer_handle_id text,
    p_classifier_version text, p_provenance_id uuid,
    p_window_start bigint, p_window_end bigint,
    p_input_snapshot_hash text, p_label_set_hash text,
    p_features_json jsonb, p_labels_json jsonb, p_claims_json jsonb,
    p_evidence_ids text[], p_result_encoding bytea, p_state_hash text
)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    bound_namespace uuid;
    input agent_economy.classifier_job_inputs%ROWTYPE;
    prior_receipt agent_economy.classifier_job_receipts%ROWTYPE;
    canonical_output_sha256 text;
BEGIN
    bound_namespace := agent_economy.bound_classifier_namespace();
    IF bound_namespace IS NULL
       OR p_input_sha256 !~ '^[0-9a-f]{64}$'
       OR p_output_sha256 !~ '^[0-9a-f]{64}$'
       OR p_input_snapshot_hash !~ '^[0-9a-f]{64}$'
       OR p_label_set_hash !~ '^[0-9a-f]{64}$'
       OR p_state_hash !~ '^[0-9a-f]{64}$'
       OR p_window_end <= p_window_start
       OR p_run_version < 1
       OR jsonb_typeof(p_features_json) <> 'object'
       OR jsonb_typeof(p_labels_json) <> 'array'
       OR jsonb_typeof(p_claims_json) <> 'array'
       OR cardinality(p_evidence_ids) NOT BETWEEN 1 AND 50000
       OR p_result_encoding IS NULL OR octet_length(p_result_encoding) = 0
    THEN
        RAISE EXCEPTION 'invalid classification batch';
    END IF;

    SELECT * INTO prior_receipt
    FROM agent_economy.classifier_job_receipts
    WHERE namespace_id = bound_namespace AND job_id = p_job_id;
    IF FOUND THEN
        IF prior_receipt.input_sha256 = p_input_sha256
           AND prior_receipt.derived_output_sha256 = p_output_sha256
           AND prior_receipt.run_id = p_run_id
           AND prior_receipt.run_version = p_run_version
        THEN
            RETURN prior_receipt.canonical_output_sha256;
        END IF;
        RETURN NULL;
    END IF;

    SELECT input_row.* INTO input
    FROM agent_economy.worker_jobs AS job
    JOIN agent_economy.classifier_job_inputs AS input_row
      ON input_row.namespace_id = job.namespace_id AND input_row.job_id = job.job_id
    WHERE job.namespace_id = bound_namespace
      AND job.job_id = p_job_id
      AND job.mode = 'classify'
      AND job.job_kind = 'buyer-behavior-v1'
      AND job.status = 'leased'
      AND job.lease_owner = p_lease_owner
      AND job.lease_token = p_lease_token
      AND job.lease_expires_at > clock_timestamp()
      AND job.input_sha256 = p_input_sha256
    FOR UPDATE OF job;
    IF NOT FOUND THEN RETURN NULL; END IF;

    IF input.expected_output_sha256 <> p_output_sha256
       OR input.expected_input_snapshot_hash <> p_input_snapshot_hash
       OR input.expected_label_set_hash <> p_label_set_hash
       OR input.expected_features_json <> p_features_json
       OR input.input_manifest -> 'labels' <> p_labels_json
       OR input.expected_claims_json <> p_claims_json
       OR input.expected_evidence_ids <> p_evidence_ids
       OR input.expected_result_encoding <> p_result_encoding
       OR input.expected_state_hash <> p_state_hash
       OR input.input_manifest ->> 'run_id' <> p_run_id
       OR (input.input_manifest ->> 'run_version')::integer <> p_run_version
       OR input.input_manifest ->> 'buyer_handle_id' <> p_buyer_handle_id
       OR input.input_manifest ->> 'classifier_version' <> p_classifier_version
       OR input.input_manifest ->> 'provenance_id' <> p_provenance_id::text
       OR (input.input_manifest ->> 'window_start_unix_seconds')::bigint <> p_window_start
       OR (input.input_manifest ->> 'window_end_unix_seconds')::bigint <> p_window_end
       OR (SELECT array_agg(DISTINCT value ORDER BY value)
           FROM jsonb_array_elements_text(to_jsonb(p_evidence_ids))) <>
          (SELECT array_agg(DISTINCT activity ->> 'evidence_id' ORDER BY activity ->> 'evidence_id')
           FROM jsonb_array_elements(input.input_manifest -> 'activities') AS source(activity))
    THEN
        RAISE EXCEPTION 'classification output is not bound to immutable input';
    END IF;

    IF (SELECT count(*) FROM jsonb_object_keys(p_features_json)) <> 8
       OR EXISTS (
            SELECT 1 FROM jsonb_each(p_features_json) AS feature(name, value)
            WHERE name NOT IN ('total_spend_atomic', 'payment_count', 'median_cadence_seconds',
                'x402_count', 'mpp_count', 'unique_counterparties',
                'autonomous_count', 'autonomy_observed_count')
               OR (value <> 'null'::jsonb AND value #>> '{}' !~ '^(0|[1-9][0-9]*)$')
       )
       OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_labels_json) AS label(item)
            WHERE jsonb_typeof(item) <> 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 7
               OR item ->> 'kind' NOT IN ('core', 'extension')
               OR item ->> 'metric' NOT IN ('total_spend_atomic', 'payment_count',
                    'median_cadence_seconds', 'x402_count', 'mpp_count',
                    'unique_counterparties', 'autonomous_count', 'autonomy_observed_count')
               OR item ->> 'threshold' !~ '^(0|[1-9][0-9]*)$'
               OR item ->> 'definition_hash' !~ '^[0-9a-f]{64}$'
               OR (item ->> 'confidence_bps')::integer NOT BETWEEN 0 AND 10000
       )
       OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_claims_json) AS claim(item)
            WHERE jsonb_typeof(item) <> 'object'
               OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 4
               OR item ->> 'status' <> 'inferred'
               OR NOT EXISTS (
                    SELECT 1 FROM jsonb_array_elements(p_labels_json) AS label(label_item)
                    WHERE label_item ->> 'id' = item ->> 'label_id'
                      AND label_item ->> 'version' = item ->> 'label_version'
                      AND label_item ->> 'confidence_bps' = item ->> 'confidence_bps'
               )
       )
    THEN
        RAISE EXCEPTION 'invalid classification output';
    END IF;

    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(input.input_manifest -> 'activities') AS source(activity)
        WHERE NOT EXISTS (
            SELECT 1
            FROM agent_economy.settlements AS settlement
            JOIN agent_economy.attribution_run_evidence AS evidence
              ON evidence.namespace_id = settlement.namespace_id
             AND evidence.chain_scope = settlement.chain_scope
             AND evidence.settlement_id = settlement.settlement_id
             AND evidence.evidence_id = activity ->> 'evidence_id'
            WHERE settlement.namespace_id = bound_namespace
              AND settlement.settlement_id = activity ->> 'settlement_id'
              AND settlement.protocol = activity ->> 'protocol'
              AND settlement.amount_atomic::text = activity ->> 'amount_atomic'
              AND extract(epoch FROM settlement.settled_at)::bigint =
                    (activity ->> 'occurred_at_unix_seconds')::bigint
            UNION ALL
            SELECT 1
            FROM agent_economy.settlements AS settlement
            JOIN agent_economy.event_finality_assertions AS finality
              ON finality.namespace_id = settlement.namespace_id
             AND finality.chain_scope = settlement.chain_scope
             AND finality.protocol = settlement.protocol
             AND finality.canonical_event_id = settlement.canonical_event_id
             AND finality.accepted AND finality.current_status = 'finalized'
            JOIN agent_economy.buyer_finalized_history_evidence AS history
              ON history.namespace_id = settlement.namespace_id
             AND history.buyer_handle_id = settlement.buyer_handle_id
             AND history.chain_scope = settlement.chain_scope
             AND history.transaction_reference = finality.transaction_id
             AND history.evidence_id = activity ->> 'evidence_id'
            WHERE settlement.namespace_id = bound_namespace
              AND settlement.settlement_id = activity ->> 'settlement_id'
              AND settlement.protocol = activity ->> 'protocol'
              AND settlement.amount_atomic::text = activity ->> 'amount_atomic'
              AND extract(epoch FROM settlement.settled_at)::bigint =
                    (activity ->> 'occurred_at_unix_seconds')::bigint
        )
    ) THEN
        RAISE EXCEPTION 'classification activity is not canonical settlement evidence';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM jsonb_array_elements(input.input_manifest -> 'activities') AS source(activity)
        WHERE activity ? 'observation_id'
          AND ((activity ->> 'observation_id') !~ '^sha256:[0-9a-f]{64}$'
            OR NOT EXISTS (
                SELECT 1
                FROM agent_economy.observations AS observation
                JOIN agent_economy.canonical_event_observations AS event_observation
                  ON event_observation.namespace_id = observation.namespace_id
                 AND event_observation.chain_scope = observation.chain_scope
                 AND event_observation.source_id = observation.source_id
                 AND event_observation.protocol = observation.protocol
                 AND event_observation.observation_id = observation.observation_id
                 AND event_observation.support_role = 'supporting'
                JOIN agent_economy.settlements AS settlement
                  ON settlement.namespace_id = event_observation.namespace_id
                 AND settlement.chain_scope = event_observation.chain_scope
                 AND settlement.protocol = event_observation.protocol
                 AND settlement.canonical_event_id = event_observation.canonical_event_id
                JOIN agent_economy.event_finality_assertions AS finality
                  ON finality.namespace_id = settlement.namespace_id
                 AND finality.chain_scope = settlement.chain_scope
                 AND finality.protocol = settlement.protocol
                 AND finality.canonical_event_id = settlement.canonical_event_id
                 AND finality.accepted AND finality.current_status = 'finalized'
                JOIN agent_economy.buyer_finalized_history_evidence AS history
                  ON history.namespace_id = settlement.namespace_id
                 AND history.buyer_handle_id = settlement.buyer_handle_id
                 AND history.chain_scope = settlement.chain_scope
                 AND history.transaction_reference = finality.transaction_id
                 AND history.evidence_id = observation.evidence_id
                WHERE observation.namespace_id = bound_namespace
                  AND observation.source_id = 'alchemy-history'
                  AND observation.observation_id = activity ->> 'observation_id'
                  AND observation.protocol = activity ->> 'protocol'
                  AND observation.evidence_id = activity ->> 'evidence_id'
                  AND settlement.settlement_id = activity ->> 'settlement_id'
                  AND settlement.buyer_handle_id = p_buyer_handle_id
            ))
    ) THEN
        RAISE EXCEPTION 'classification activity is not bound to immutable history observation';
    END IF;

    INSERT INTO agent_economy.classification_label_definitions
        (namespace_id, label_id, label_version, label_kind, rule_definition, definition_hash)
    SELECT bound_namespace, item ->> 'id', (item ->> 'version')::integer,
           item ->> 'kind', jsonb_build_object('metric', item ->> 'metric',
               'threshold', item ->> 'threshold'), item ->> 'definition_hash'
    FROM jsonb_array_elements(p_labels_json) AS label(item)
    ON CONFLICT DO NOTHING;
    IF EXISTS (
        SELECT 1 FROM jsonb_array_elements(p_labels_json) AS label(item)
        JOIN agent_economy.classification_label_definitions AS existing
          ON existing.namespace_id = bound_namespace
         AND existing.label_id = item ->> 'id'
         AND existing.label_version = (item ->> 'version')::integer
        WHERE existing.label_kind <> item ->> 'kind'
           OR existing.rule_definition <> jsonb_build_object('metric', item ->> 'metric',
                'threshold', item ->> 'threshold')
           OR existing.definition_hash <> item ->> 'definition_hash'
    ) THEN RAISE EXCEPTION 'contradictory classification label replay'; END IF;

    INSERT INTO agent_economy.classification_runs
        (namespace_id, run_id, run_version, supersedes_version, buyer_handle_id,
         classifier_version, feature_version, label_set_hash, input_snapshot_hash,
         window_start, window_end)
    VALUES (bound_namespace, p_run_id, p_run_version,
            CASE WHEN p_run_version > 1 THEN p_run_version - 1 END,
            p_buyer_handle_id, p_classifier_version,
            input.input_manifest ->> 'feature_version', p_label_set_hash,
            p_input_snapshot_hash, to_timestamp(p_window_start), to_timestamp(p_window_end));

    INSERT INTO agent_economy.classification_run_label_definitions
        (namespace_id, run_id, run_version, label_id, label_version)
    SELECT bound_namespace, p_run_id, p_run_version, item ->> 'id',
           (item ->> 'version')::integer
    FROM jsonb_array_elements(p_labels_json) AS label(item);

    INSERT INTO agent_economy.classification_run_features
        (namespace_id, run_id, run_version, feature_name, feature_value)
    SELECT bound_namespace, p_run_id, p_run_version, name,
           CASE WHEN value = 'null'::jsonb THEN 'null'::jsonb
                ELSE to_jsonb((value #>> '{}')::numeric) END
    FROM jsonb_each(p_features_json) AS feature(name, value);

    INSERT INTO agent_economy.classification_run_evidence
        (namespace_id, run_id, run_version, evidence_id, evidence_role)
    SELECT bound_namespace, p_run_id, p_run_version, evidence_id, 'supporting'
    FROM unnest(p_evidence_ids) AS evidence(evidence_id);

    INSERT INTO agent_economy.classification_claims
        (namespace_id, claim_id, version, supersedes_version, buyer_handle_id,
         label, method, confidence, evidence_window_start, evidence_window_end,
         valid_from, status, provenance_id)
    SELECT bound_namespace, p_run_id || ':' || (item ->> 'label_id'), p_run_version,
           CASE WHEN p_run_version > 1 THEN p_run_version - 1 END,
           p_buyer_handle_id, item ->> 'label_id', p_classifier_version,
           (item ->> 'confidence_bps')::numeric / 10000,
           to_timestamp(p_window_start), to_timestamp(p_window_end),
           to_timestamp(p_window_end), 'inferred', p_provenance_id
    FROM jsonb_array_elements(p_claims_json) AS claim(item);

    INSERT INTO agent_economy.classification_claim_evidence
        (namespace_id, claim_id, claim_version, evidence_id, evidence_role)
    SELECT bound_namespace, p_run_id || ':' || (claim_item ->> 'label_id'), p_run_version,
           evidence_id, 'supporting'
    FROM jsonb_array_elements(p_claims_json) AS claim(claim_item)
    CROSS JOIN unnest(p_evidence_ids) AS evidence(evidence_id);

    INSERT INTO agent_economy.classification_run_claims
        (namespace_id, run_id, run_version, claim_id, claim_version,
         label_id, label_version, status)
    SELECT bound_namespace, p_run_id, p_run_version,
           p_run_id || ':' || (item ->> 'label_id'), p_run_version,
           item ->> 'label_id', (item ->> 'label_version')::integer, 'inferred'
    FROM jsonb_array_elements(p_claims_json) AS claim(item);

    INSERT INTO agent_economy.classification_run_seals
        (namespace_id, run_id, run_version, result_encoding, state_hash, content_hash)
    VALUES (bound_namespace, p_run_id, p_run_version, p_result_encoding,
            p_state_hash, p_output_sha256)
    RETURNING content_hash INTO canonical_output_sha256;

    INSERT INTO agent_economy.classifier_job_receipts
        (namespace_id, job_id, input_sha256, derived_output_sha256,
         canonical_output_sha256, run_id, run_version)
    VALUES (bound_namespace, p_job_id, p_input_sha256, p_output_sha256,
            canonical_output_sha256, p_run_id, p_run_version);
    RETURN canonical_output_sha256;
END
$$;

CREATE FUNCTION agent_economy.renew_classifier_job_lease(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid, p_lease_seconds bigint
)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT agent_economy.renew_worker_job_lease(
        agent_economy.bound_classifier_namespace(), p_job_id, p_lease_owner,
        p_lease_token, p_lease_seconds)
$$;

CREATE FUNCTION agent_economy.complete_classifier_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid, p_output_sha256 text
)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT CASE WHEN EXISTS (
        SELECT 1 FROM agent_economy.worker_jobs AS job
        JOIN agent_economy.classifier_job_receipts AS receipt
          ON receipt.namespace_id = job.namespace_id AND receipt.job_id = job.job_id
        WHERE job.namespace_id = agent_economy.bound_classifier_namespace()
          AND job.job_id = p_job_id AND job.status = 'leased'
          AND job.lease_owner = p_lease_owner AND job.lease_token = p_lease_token
          AND job.lease_expires_at > clock_timestamp()
          AND receipt.canonical_output_sha256 = p_output_sha256
    ) THEN agent_economy.complete_worker_job(
        agent_economy.bound_classifier_namespace(), p_job_id, p_lease_owner,
        p_lease_token, p_output_sha256) ELSE false END
$$;

CREATE FUNCTION agent_economy.fail_classifier_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid,
    p_error_code text, p_retryable boolean, p_retry_delay_seconds bigint
)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT agent_economy.fail_worker_job(
        agent_economy.bound_classifier_namespace(), p_job_id, p_lease_owner,
        p_lease_token, p_error_code, p_retryable, p_retry_delay_seconds)
$$;

DO $$
DECLARE classifier_role pg_roles%ROWTYPE;
BEGIN
    BEGIN
        CREATE ROLE agent_economy_classifier_runtime LOGIN NOINHERIT;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;
    SELECT * INTO STRICT classifier_role FROM pg_roles
    WHERE rolname = 'agent_economy_classifier_runtime';
    IF NOT classifier_role.rolcanlogin OR classifier_role.rolinherit OR classifier_role.rolsuper
       OR classifier_role.rolcreatedb OR classifier_role.rolcreaterole
       OR classifier_role.rolreplication OR classifier_role.rolbypassrls
    THEN RAISE EXCEPTION 'agent_economy_classifier_runtime must be unprivileged'; END IF;
    IF EXISTS (SELECT 1 FROM pg_auth_members
               WHERE member = classifier_role.oid OR roleid = classifier_role.oid)
    THEN RAISE EXCEPTION 'agent_economy_classifier_runtime must be isolated'; END IF;
END
$$;

DO $migration$
BEGIN
    EXECUTE format('GRANT CONNECT ON DATABASE %I TO agent_economy_classifier_runtime', current_database());
END
$migration$;

REVOKE ALL ON agent_economy.worker_jobs FROM agent_economy_classifier_runtime;
REVOKE ALL ON agent_economy.classifier_job_inputs FROM PUBLIC, agent_economy_classifier_runtime;
REVOKE ALL ON agent_economy.classifier_job_receipts FROM PUBLIC, agent_economy_classifier_runtime;
REVOKE ALL ON agent_economy.classifier_runtime_namespaces FROM PUBLIC, agent_economy_classifier_runtime;
REVOKE ALL ON agent_economy.classification_runs FROM agent_economy_classifier_runtime;
REVOKE ALL ON agent_economy.classification_run_features FROM agent_economy_classifier_runtime;
REVOKE ALL ON agent_economy.classification_claims FROM agent_economy_classifier_runtime;
REVOKE ALL ON FUNCTION agent_economy.validate_classifier_job_input() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.bound_classifier_namespace() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.claim_classifier_job(text, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.load_classifier_job_input(uuid, text, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.commit_classification_batch(uuid, text, uuid, text, text, text, integer, text, text, uuid, bigint, bigint, text, text, jsonb, jsonb, jsonb, text[], bytea, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.renew_classifier_job_lease(uuid, text, uuid, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.complete_classifier_job(uuid, text, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.fail_classifier_job(uuid, text, uuid, text, boolean, bigint) FROM PUBLIC;
GRANT USAGE ON SCHEMA agent_economy TO agent_economy_classifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.bound_classifier_namespace() TO agent_economy_classifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.claim_classifier_job(text, bigint) TO agent_economy_classifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.load_classifier_job_input(uuid, text, uuid) TO agent_economy_classifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.commit_classification_batch(uuid, text, uuid, text, text, text, integer, text, text, uuid, bigint, bigint, text, text, jsonb, jsonb, jsonb, text[], bytea, text) TO agent_economy_classifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.renew_classifier_job_lease(uuid, text, uuid, bigint) TO agent_economy_classifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.complete_classifier_job(uuid, text, uuid, text) TO agent_economy_classifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.fail_classifier_job(uuid, text, uuid, text, boolean, bigint) TO agent_economy_classifier_runtime;

CREATE TABLE agent_economy.enrichment_job_inputs (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    input_manifest jsonb NOT NULL CHECK (jsonb_typeof(input_manifest) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, job_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id)
);

CREATE TABLE agent_economy.pending_enrichment_batches (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    output_sha256 text NOT NULL CHECK (output_sha256 ~ '^[0-9a-f]{64}$'),
    buyer_handle_id text NOT NULL,
    chain_scope text NOT NULL,
    handle_value text NOT NULL,
    observed_date date NOT NULL,
    cursor_version bigint NOT NULL,
    start_cursor text,
    next_cursor text,
    requests_used bigint NOT NULL,
    complete boolean NOT NULL,
    evidence_json jsonb NOT NULL CHECK (jsonb_typeof(evidence_json) = 'array'),
    records_json jsonb NOT NULL CHECK (jsonb_typeof(records_json) = 'array'),
    classification_labels jsonb NOT NULL CHECK (jsonb_typeof(classification_labels) = 'array'),
    status text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'verifying', 'succeeded')),
    verifier_owner text,
    verifier_token uuid,
    verifier_expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, job_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id),
    CHECK ((status = 'verifying') =
           (verifier_owner IS NOT NULL AND verifier_token IS NOT NULL AND verifier_expires_at IS NOT NULL))
);

CREATE TABLE agent_economy.enrichment_job_receipts (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    output_sha256 text NOT NULL CHECK (output_sha256 ~ '^[0-9a-f]{64}$'),
    evidence_json jsonb NOT NULL CHECK (jsonb_typeof(evidence_json) = 'array'),
    records_json jsonb NOT NULL CHECK (jsonb_typeof(records_json) = 'array'),
    protocol_attribution boolean NOT NULL DEFAULT false CHECK (protocol_attribution = false),
    classification_job_id uuid,
    committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, job_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id)
);

CREATE TABLE agent_economy.enrichment_reduction_receipts (
    namespace_id uuid NOT NULL,
    enrichment_job_id uuid NOT NULL,
    verified_records jsonb NOT NULL CHECK (jsonb_typeof(verified_records) = 'array'),
    reduced_activities jsonb NOT NULL CHECK (jsonb_typeof(reduced_activities) = 'array'),
    reduced_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, enrichment_job_id),
    FOREIGN KEY (namespace_id, enrichment_job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id)
);

CREATE TABLE agent_economy.classification_admission_requests (
    namespace_id uuid NOT NULL,
    enrichment_job_id uuid NOT NULL,
    input_manifest jsonb NOT NULL CHECK (jsonb_typeof(input_manifest) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, enrichment_job_id),
    FOREIGN KEY (namespace_id, enrichment_job_id)
        REFERENCES agent_economy.enrichment_reduction_receipts (namespace_id, enrichment_job_id)
);

CREATE TABLE agent_economy.enricher_runtime_namespaces (
    login_name name PRIMARY KEY,
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    purpose text NOT NULL CHECK (purpose = 'enrich'),
    UNIQUE (namespace_id, purpose)
);

CREATE FUNCTION agent_economy.validate_enrichment_job_input()
RETURNS trigger LANGUAGE plpgsql SET search_path = pg_catalog AS $$
DECLARE job agent_economy.worker_jobs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT job FROM agent_economy.worker_jobs
    WHERE namespace_id = NEW.namespace_id AND job_id = NEW.job_id;
    IF job.mode <> 'enrich' OR job.job_kind <> 'buyer-public-history-v1'
       OR job.status <> 'pending'
       OR job.input_sha256 <> encode(sha256(convert_to(NEW.input_manifest::text, 'UTF8')), 'hex')
       OR NEW.input_manifest <> jsonb_build_object(
            'schema_version', NEW.input_manifest -> 'schema_version',
            'namespace_id', NEW.input_manifest -> 'namespace_id',
            'buyer_handle_id', NEW.input_manifest -> 'buyer_handle_id',
            'chain_scope', NEW.input_manifest -> 'chain_scope',
            'handle_value', NEW.input_manifest -> 'handle_value',
            'enrichment_mode', NEW.input_manifest -> 'enrichment_mode',
            'max_pages', NEW.input_manifest -> 'max_pages',
            'request_budget', NEW.input_manifest -> 'request_budget',
            'observed_date', NEW.input_manifest -> 'observed_date',
            'cursor_version', NEW.input_manifest -> 'cursor_version',
            'start_cursor', NEW.input_manifest -> 'start_cursor',
            'classification_labels', NEW.input_manifest -> 'classification_labels')
       OR NEW.input_manifest ->> 'schema_version' <> '1'
       OR NEW.input_manifest ->> 'namespace_id' <> NEW.namespace_id::text
       OR NEW.input_manifest ->> 'chain_scope' NOT IN ('ethereum', 'base', 'solana', 'tempo')
       OR NEW.input_manifest ->> 'enrichment_mode' NOT IN ('automatic', 'manual_deep_scan')
       OR jsonb_typeof(NEW.input_manifest -> 'classification_labels') <> 'array'
       OR jsonb_array_length(NEW.input_manifest -> 'classification_labels') NOT BETWEEN 1 AND 1000
    THEN RAISE EXCEPTION 'invalid enrichment job input'; END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER enrichment_job_inputs_validate
BEFORE INSERT ON agent_economy.enrichment_job_inputs
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_enrichment_job_input();
CREATE TRIGGER enrichment_job_inputs_immutable
BEFORE UPDATE OR DELETE ON agent_economy.enrichment_job_inputs
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER enrichment_job_inputs_truncate_immutable
BEFORE TRUNCATE ON agent_economy.enrichment_job_inputs
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER enrichment_job_receipts_immutable
BEFORE UPDATE OR DELETE ON agent_economy.enrichment_job_receipts
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER enrichment_reduction_receipts_immutable
BEFORE UPDATE OR DELETE ON agent_economy.enrichment_reduction_receipts
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_admission_requests_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_admission_requests
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE FUNCTION agent_economy.bound_enricher_namespace()
RETURNS uuid LANGUAGE sql SECURITY DEFINER STABLE SET search_path = pg_catalog AS $$
    SELECT binding.namespace_id FROM agent_economy.enricher_runtime_namespaces AS binding
    WHERE binding.login_name = session_user AND binding.purpose = 'enrich'
$$;

CREATE FUNCTION agent_economy.claim_enrichment_job(p_lease_owner text, p_lease_seconds bigint)
RETURNS TABLE (job_id uuid, mode text, job_kind text, input_sha256 text,
    attempt_count smallint, lease_owner text, lease_token uuid)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE bound_namespace uuid;
BEGIN
    bound_namespace := agent_economy.bound_enricher_namespace();
    IF bound_namespace IS NULL OR p_lease_owner !~ '^[A-Za-z0-9_.:-]{1,128}$'
       OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN RAISE EXCEPTION 'invalid enricher lease request'; END IF;
    UPDATE agent_economy.worker_jobs AS expired
    SET status = 'dead_letter', lease_owner = NULL, lease_token = NULL,
        lease_expires_at = NULL, last_error_code = 'lease_expired', updated_at = clock_timestamp()
    WHERE expired.namespace_id = bound_namespace AND expired.mode = 'enrich'
      AND expired.job_kind = 'buyer-public-history-v1' AND expired.status = 'leased'
      AND expired.lease_expires_at <= clock_timestamp()
      AND expired.attempt_count >= expired.max_attempts;
    RETURN QUERY
    WITH candidate AS (
        SELECT queued.namespace_id, queued.job_id
        FROM agent_economy.worker_jobs AS queued
        JOIN agent_economy.enrichment_job_inputs AS input
          ON input.namespace_id = queued.namespace_id AND input.job_id = queued.job_id
        WHERE queued.namespace_id = bound_namespace
          AND queued.mode = 'enrich'
          AND queued.job_kind = 'buyer-public-history-v1'
          AND queued.attempt_count < queued.max_attempts
          AND ((queued.status IN ('pending', 'retryable') AND queued.scheduled_for <= clock_timestamp())
            OR (queued.status = 'leased' AND queued.lease_expires_at <= clock_timestamp()))
        ORDER BY queued.scheduled_for, queued.created_at, queued.job_id
        FOR UPDATE OF queued SKIP LOCKED LIMIT 1
    )
    UPDATE agent_economy.worker_jobs AS claimed
    SET status = 'leased', lease_owner = p_lease_owner, lease_token = gen_random_uuid(),
        lease_expires_at = clock_timestamp() + p_lease_seconds * interval '1 second',
        attempt_count = claimed.attempt_count + 1, last_error_code = NULL,
        updated_at = clock_timestamp()
    FROM candidate
    WHERE claimed.namespace_id = candidate.namespace_id AND claimed.job_id = candidate.job_id
    RETURNING claimed.job_id, claimed.mode, claimed.job_kind, claimed.input_sha256,
              claimed.attempt_count, claimed.lease_owner, claimed.lease_token;
END
$$;

CREATE FUNCTION agent_economy.load_enrichment_job_input(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid)
RETURNS bytea LANGUAGE sql SECURITY DEFINER STABLE SET search_path = pg_catalog AS $$
    SELECT convert_to(input.input_manifest::text, 'UTF8')
    FROM agent_economy.worker_jobs AS job
    JOIN agent_economy.enrichment_job_inputs AS input
      ON input.namespace_id = job.namespace_id AND input.job_id = job.job_id
    WHERE job.namespace_id = agent_economy.bound_enricher_namespace()
      AND job.job_id = p_job_id AND job.mode = 'enrich'
      AND job.job_kind = 'buyer-public-history-v1' AND job.status = 'leased'
      AND job.lease_owner = p_lease_owner AND job.lease_token = p_lease_token
      AND job.lease_expires_at > clock_timestamp()
$$;

CREATE FUNCTION agent_economy.stage_enrichment_batch(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid,
    p_input_sha256 text, p_output_sha256 text,
    p_namespace_id uuid, p_buyer_handle_id text, p_chain_scope text,
    p_handle_value text, p_observed_date date, p_cursor_version bigint,
    p_start_cursor text, p_next_cursor text, p_requests_used bigint,
    p_complete boolean, p_evidence_json jsonb, p_records_json jsonb,
    p_classification_labels jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    bound_namespace uuid;
    input agent_economy.enrichment_job_inputs%ROWTYPE;
    prior agent_economy.pending_enrichment_batches%ROWTYPE;
BEGIN
    bound_namespace := agent_economy.bound_enricher_namespace();
    IF bound_namespace IS NULL OR bound_namespace <> p_namespace_id
       OR p_input_sha256 !~ '^[0-9a-f]{64}$' OR p_output_sha256 !~ '^[0-9a-f]{64}$'
       OR p_chain_scope NOT IN ('ethereum', 'base', 'solana', 'tempo')
       OR p_cursor_version < 0 OR p_requests_used NOT BETWEEN 1 AND 1000
       OR jsonb_typeof(p_evidence_json) <> 'array'
       OR jsonb_array_length(p_evidence_json) <> p_requests_used
       OR jsonb_typeof(p_records_json) <> 'array'
       OR jsonb_typeof(p_classification_labels) <> 'array'
    THEN RAISE EXCEPTION 'invalid enrichment batch'; END IF;

    SELECT * INTO prior FROM agent_economy.pending_enrichment_batches
    WHERE namespace_id = bound_namespace AND job_id = p_job_id;
    IF FOUND THEN
        RETURN prior.input_sha256 = p_input_sha256
           AND prior.output_sha256 = p_output_sha256
           AND prior.evidence_json = p_evidence_json
           AND prior.records_json = p_records_json;
    END IF;

    SELECT input_row.* INTO input
    FROM agent_economy.worker_jobs AS job
    JOIN agent_economy.enrichment_job_inputs AS input_row
      ON input_row.namespace_id = job.namespace_id AND input_row.job_id = job.job_id
    WHERE job.namespace_id = bound_namespace AND job.job_id = p_job_id
      AND job.mode = 'enrich' AND job.job_kind = 'buyer-public-history-v1'
      AND job.status = 'leased' AND job.lease_owner = p_lease_owner
      AND job.lease_token = p_lease_token AND job.lease_expires_at > clock_timestamp()
      AND job.input_sha256 = p_input_sha256
    FOR UPDATE OF job;
    IF NOT FOUND THEN RETURN false; END IF;

    IF input.input_manifest ->> 'buyer_handle_id' <> p_buyer_handle_id
       OR input.input_manifest ->> 'chain_scope' <> p_chain_scope
       OR input.input_manifest ->> 'handle_value' <> p_handle_value
       OR (input.input_manifest ->> 'observed_date')::date <> p_observed_date
       OR (input.input_manifest ->> 'cursor_version')::bigint <> p_cursor_version
       OR input.input_manifest ->> 'start_cursor' IS DISTINCT FROM p_start_cursor
       OR input.input_manifest -> 'classification_labels' <> p_classification_labels
       OR p_requests_used > (input.input_manifest ->> 'request_budget')::bigint
       OR p_requests_used > (input.input_manifest ->> 'max_pages')::bigint
       OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
            WHERE jsonb_typeof(item) <> 'object'
               OR item <> jsonb_build_object(
                    'byte_length', item -> 'byte_length',
                    'object_name', item -> 'object_name',
                    'sha256', item -> 'sha256',
                    'storage_generation', item -> 'storage_generation')
               OR item ->> 'object_name' !~ '^evidence/alchemy-history/[0-9]{4}-[0-9]{2}-[0-9]{2}/sha256/[0-9a-f]{2}/[0-9a-f]{64}$'
               OR item ->> 'sha256' !~ '^[0-9a-f]{64}$'
               OR right(item ->> 'object_name', 64) <> item ->> 'sha256'
               OR (item ->> 'byte_length')::bigint <= 0
               OR (jsonb_typeof(item -> 'storage_generation') NOT IN ('string', 'null'))
               OR (jsonb_typeof(item -> 'storage_generation') = 'string'
                   AND item ->> 'storage_generation' = ''))
       OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_records_json) AS record(item)
            WHERE jsonb_typeof(item) <> 'object'
               OR item ->> 'transaction_reference' = ''
               OR item ->> 'block_reference' = ''
               OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_evidence_json) AS evidence(e)
                              WHERE e ->> 'object_name' = item ->> 'evidence_id'))
    THEN RAISE EXCEPTION 'enrichment output is not bound to immutable input'; END IF;

    INSERT INTO agent_economy.pending_enrichment_batches
        (namespace_id, job_id, input_sha256, output_sha256, buyer_handle_id,
         chain_scope, handle_value, observed_date, cursor_version, start_cursor,
         next_cursor, requests_used, complete, evidence_json, records_json,
         classification_labels)
    VALUES (bound_namespace, p_job_id, p_input_sha256, p_output_sha256,
            p_buyer_handle_id, p_chain_scope, p_handle_value, p_observed_date,
            p_cursor_version, p_start_cursor, p_next_cursor, p_requests_used,
            p_complete, p_evidence_json, p_records_json, p_classification_labels);
    RETURN true;
END
$$;

CREATE FUNCTION agent_economy.claim_pending_enrichment_batch(
    p_lease_owner text, p_lease_seconds bigint)
RETURNS SETOF agent_economy.pending_enrichment_batches
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE bound_namespace uuid;
BEGIN
    bound_namespace := agent_economy.bound_collection_namespace('verify-evidence');
    IF bound_namespace IS NULL OR p_lease_owner !~ '^[A-Za-z0-9_.:-]{1,128}$'
       OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN RAISE EXCEPTION 'invalid enrichment verifier lease'; END IF;
    RETURN QUERY
    WITH candidate AS (
        SELECT pending.namespace_id, pending.job_id
        FROM agent_economy.pending_enrichment_batches AS pending
        WHERE pending.namespace_id = bound_namespace
          AND (pending.status = 'pending'
            OR (pending.status = 'verifying' AND pending.verifier_expires_at <= clock_timestamp()))
        ORDER BY pending.created_at, pending.job_id
        FOR UPDATE SKIP LOCKED LIMIT 1
    )
    UPDATE agent_economy.pending_enrichment_batches AS claimed
    SET status = 'verifying', verifier_owner = p_lease_owner,
        verifier_token = gen_random_uuid(),
        verifier_expires_at = clock_timestamp() + p_lease_seconds * interval '1 second'
    FROM candidate
    WHERE claimed.namespace_id = candidate.namespace_id AND claimed.job_id = candidate.job_id
    RETURNING claimed.*;
END
$$;

CREATE FUNCTION agent_economy.commit_enrichment_batch(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid,
    p_input_sha256 text, p_output_sha256 text,
    p_namespace_id uuid, p_buyer_handle_id text, p_chain_scope text,
    p_handle_value text, p_observed_date date, p_cursor_version bigint,
    p_start_cursor text, p_next_cursor text, p_requests_used bigint,
    p_complete boolean, p_evidence_json jsonb, p_evidence_bodies bytea[], p_records_json jsonb,
    p_classification_labels jsonb)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    bound_namespace uuid;
    input agent_economy.enrichment_job_inputs%ROWTYPE;
    prior_receipt agent_economy.enrichment_job_receipts%ROWTYPE;
    classification_manifest jsonb;
    classification_job_id uuid;
    classification_sha text;
    classification_provenance uuid;
    window_start bigint;
    window_end bigint;
    canonical_records jsonb;
    reduced_activities jsonb;
BEGIN
    bound_namespace := agent_economy.bound_collection_namespace('verify-evidence');
    IF bound_namespace IS NULL OR bound_namespace <> p_namespace_id
       OR p_input_sha256 !~ '^[0-9a-f]{64}$' OR p_output_sha256 !~ '^[0-9a-f]{64}$'
       OR p_chain_scope NOT IN ('ethereum', 'base', 'solana', 'tempo')
       OR p_cursor_version < 0 OR p_requests_used NOT BETWEEN 1 AND 1000
       OR jsonb_typeof(p_evidence_json) <> 'array'
       OR jsonb_array_length(p_evidence_json) <> p_requests_used
       OR cardinality(p_evidence_bodies) <> p_requests_used
       OR jsonb_typeof(p_records_json) <> 'array'
       OR jsonb_typeof(p_classification_labels) <> 'array'
    THEN RAISE EXCEPTION 'invalid enrichment batch'; END IF;

    SELECT * INTO prior_receipt FROM agent_economy.enrichment_job_receipts
    WHERE namespace_id = bound_namespace AND job_id = p_job_id;
    IF FOUND THEN
        RETURN prior_receipt.input_sha256 = p_input_sha256
           AND prior_receipt.output_sha256 = p_output_sha256
           AND prior_receipt.evidence_json = p_evidence_json
           AND prior_receipt.records_json = p_records_json;
    END IF;

    SELECT input_row.* INTO input
    FROM agent_economy.pending_enrichment_batches AS pending
    JOIN agent_economy.enrichment_job_inputs AS input_row
      ON input_row.namespace_id = pending.namespace_id AND input_row.job_id = pending.job_id
    WHERE pending.namespace_id = bound_namespace AND pending.job_id = p_job_id
      AND pending.status = 'verifying' AND pending.verifier_owner = p_lease_owner
      AND pending.verifier_token = p_lease_token
      AND pending.verifier_expires_at > clock_timestamp()
      AND pending.input_sha256 = p_input_sha256
      AND pending.output_sha256 = p_output_sha256
      AND pending.buyer_handle_id = p_buyer_handle_id
      AND pending.chain_scope = p_chain_scope AND pending.handle_value = p_handle_value
      AND pending.observed_date = p_observed_date
      AND pending.cursor_version = p_cursor_version
      AND pending.start_cursor IS NOT DISTINCT FROM p_start_cursor
      AND pending.next_cursor IS NOT DISTINCT FROM p_next_cursor
      AND pending.requests_used = p_requests_used AND pending.complete = p_complete
      AND pending.evidence_json = p_evidence_json
      AND pending.records_json = p_records_json
      AND pending.classification_labels = p_classification_labels
    FOR UPDATE OF pending;
    IF NOT FOUND THEN RETURN false; END IF;

    IF input.input_manifest ->> 'buyer_handle_id' <> p_buyer_handle_id
       OR input.input_manifest ->> 'chain_scope' <> p_chain_scope
       OR input.input_manifest ->> 'handle_value' <> p_handle_value
       OR (input.input_manifest ->> 'observed_date')::date <> p_observed_date
       OR (input.input_manifest ->> 'cursor_version')::bigint <> p_cursor_version
       OR input.input_manifest ->> 'start_cursor' IS DISTINCT FROM p_start_cursor
       OR input.input_manifest -> 'classification_labels' <> p_classification_labels
       OR p_requests_used > (input.input_manifest ->> 'request_budget')::bigint
       OR p_requests_used > (input.input_manifest ->> 'max_pages')::bigint
       OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
            WHERE jsonb_typeof(item) <> 'object'
               OR item ->> 'object_name' !~ '^evidence/alchemy-history/[0-9]{4}-[0-9]{2}-[0-9]{2}/sha256/[0-9a-f]{2}/[0-9a-f]{64}$'
               OR item ->> 'sha256' !~ '^[0-9a-f]{64}$'
               OR right(item ->> 'object_name', 64) <> item ->> 'sha256'
               OR (item ->> 'byte_length')::bigint <= 0)
       OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_records_json) AS record(item)
            WHERE jsonb_typeof(item) <> 'object'
               OR item ->> 'transaction_reference' = ''
               OR item ->> 'block_reference' = ''
               OR NOT EXISTS (SELECT 1 FROM jsonb_array_elements(p_evidence_json) AS evidence(e)
                              WHERE e ->> 'object_name' = item ->> 'evidence_id'))
    THEN RAISE EXCEPTION 'enrichment output is not bound to immutable input'; END IF;

    IF EXISTS (
        SELECT 1
        FROM generate_subscripts(p_evidence_bodies, 1) AS page(index)
        JOIN LATERAL jsonb_array_elements(p_evidence_json) WITH ORDINALITY AS evidence(item, ordinal)
          ON evidence.ordinal = page.index
        WHERE encode(sha256(p_evidence_bodies[page.index]), 'hex') <> evidence.item ->> 'sha256'
           OR octet_length(p_evidence_bodies[page.index]) <> (evidence.item ->> 'byte_length')::bigint
           OR right(evidence.item ->> 'object_name', 64) <>
                encode(sha256(p_evidence_bodies[page.index]), 'hex')
    ) THEN
        RAISE EXCEPTION 'enrichment evidence bytes do not match archived custody metadata';
    END IF;

    BEGIN
        WITH pages AS (
            SELECT page.index,
                   convert_from(p_evidence_bodies[page.index], 'UTF8')::jsonb AS document,
                   evidence.item ->> 'object_name' AS evidence_id
            FROM generate_subscripts(p_evidence_bodies, 1) AS page(index)
            JOIN LATERAL jsonb_array_elements(p_evidence_json) WITH ORDINALITY AS evidence(item, ordinal)
              ON evidence.ordinal = page.index
        ), records AS (
            SELECT pages.index AS page_index, transfer.ordinal AS item_index,
                   jsonb_build_object(
                       'block_reference', transfer.item ->> 'blockNum',
                       'evidence_id', pages.evidence_id,
                       'transaction_reference', transfer.item ->> 'hash') AS record
            FROM pages
            CROSS JOIN LATERAL jsonb_array_elements(pages.document -> 'result' -> 'transfers')
                WITH ORDINALITY AS transfer(item, ordinal)
            WHERE p_chain_scope IN ('ethereum', 'base', 'tempo')
            UNION ALL
            SELECT pages.index, signature.ordinal,
                   jsonb_build_object(
                       'block_reference', signature.item ->> 'slot',
                       'evidence_id', pages.evidence_id,
                       'transaction_reference', signature.item ->> 'signature')
            FROM pages
            CROSS JOIN LATERAL jsonb_array_elements(pages.document -> 'result')
                WITH ORDINALITY AS signature(item, ordinal)
            WHERE p_chain_scope = 'solana'
        )
        SELECT COALESCE(jsonb_agg(record ORDER BY page_index, item_index), '[]'::jsonb)
        INTO canonical_records FROM records;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION 'invalid enrichment evidence body';
    END;
    IF canonical_records <> p_records_json THEN
        RAISE EXCEPTION 'enrichment history is not derived from verified evidence bytes';
    END IF;

    INSERT INTO agent_economy.buyer_enrichment_cursors
        (namespace_id, buyer_handle_id, chain_scope, handle_value, cursor, version)
    VALUES (bound_namespace, p_buyer_handle_id, p_chain_scope, p_handle_value,
            p_start_cursor, p_cursor_version)
    ON CONFLICT (namespace_id, buyer_handle_id, chain_scope) DO NOTHING;
    PERFORM 1 FROM agent_economy.buyer_enrichment_cursors AS cursor
    WHERE cursor.namespace_id = bound_namespace AND cursor.buyer_handle_id = p_buyer_handle_id
      AND cursor.chain_scope = p_chain_scope AND cursor.handle_value = p_handle_value
      AND cursor.version = p_cursor_version
      AND cursor.cursor IS NOT DISTINCT FROM p_start_cursor
    FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;

    INSERT INTO agent_economy.evidence_objects
        (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
    SELECT bound_namespace, item ->> 'object_name', item ->> 'sha256',
           item ->> 'object_name', 'application/json', (item ->> 'byte_length')::bigint,
           p_observed_date::timestamptz
    FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
    ON CONFLICT DO NOTHING;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(p_evidence_json) AS evidence(item)
        JOIN agent_economy.evidence_objects AS existing
          ON existing.namespace_id = bound_namespace AND existing.evidence_id = item ->> 'object_name'
        WHERE existing.sha256 <> item ->> 'sha256'
           OR existing.byte_length <> (item ->> 'byte_length')::bigint)
    THEN RAISE EXCEPTION 'contradictory enrichment evidence replay'; END IF;

    INSERT INTO agent_economy.buyer_finalized_history
        (namespace_id, buyer_handle_id, chain_scope, transaction_reference, block_reference)
    SELECT bound_namespace, p_buyer_handle_id, p_chain_scope,
           item ->> 'transaction_reference', item ->> 'block_reference'
    FROM jsonb_array_elements(p_records_json) AS record(item)
    ON CONFLICT DO NOTHING;
    INSERT INTO agent_economy.buyer_finalized_history_evidence
        (namespace_id, buyer_handle_id, chain_scope, transaction_reference, evidence_id)
    SELECT bound_namespace, p_buyer_handle_id, p_chain_scope,
           item ->> 'transaction_reference', item ->> 'evidence_id'
    FROM jsonb_array_elements(p_records_json) AS record(item)
    ON CONFLICT DO NOTHING;

    WITH history_records AS (
        SELECT item,
               encode(sha256(convert_to(
                   'enrichment-provenance-v1' || chr(10) || (item ->> 'evidence_id') ||
                   chr(10) || (item ->> 'transaction_reference'), 'UTF8')), 'hex') AS identity_hash
        FROM jsonb_array_elements(p_records_json) AS record(item)
    )
    INSERT INTO agent_economy.provenance_records
        (namespace_id, provenance_id, source_id, observed_at, parser_version,
         provider, chain_scope, block_reference, transaction_reference, finality, evidence_id)
    SELECT bound_namespace,
           (substring(identity_hash, 1, 8) || '-' || substring(identity_hash, 9, 4) || '-' ||
            substring(identity_hash, 13, 4) || '-' || substring(identity_hash, 17, 4) || '-' ||
            substring(identity_hash, 21, 12))::uuid,
           'alchemy-history', p_observed_date::timestamptz, 'buyer-history@1', 'alchemy',
           p_chain_scope, item ->> 'block_reference', item ->> 'transaction_reference',
           'finalized', item ->> 'evidence_id'
    FROM history_records
    ON CONFLICT DO NOTHING;

    WITH matched_history AS (
        SELECT record.item, settlement.protocol,
               jsonb_build_object(
                   'block_reference', record.item ->> 'block_reference',
                   'buyer_handle_id', p_buyer_handle_id,
                   'chain_scope', p_chain_scope,
                   'evidence_id', record.item ->> 'evidence_id',
                   'protocol', settlement.protocol,
                   'transaction_reference', record.item ->> 'transaction_reference') AS payload
        FROM jsonb_array_elements(p_records_json) AS record(item)
        JOIN agent_economy.event_finality_assertions AS finality
          ON finality.namespace_id = bound_namespace
         AND finality.chain_scope = p_chain_scope
         AND finality.transaction_id = record.item ->> 'transaction_reference'
         AND finality.accepted AND finality.current_status = 'finalized'
        JOIN agent_economy.settlements AS settlement
          ON settlement.namespace_id = finality.namespace_id
         AND settlement.chain_scope = finality.chain_scope
         AND settlement.protocol = finality.protocol
         AND settlement.canonical_event_id = finality.canonical_event_id
         AND settlement.buyer_handle_id = p_buyer_handle_id
    ), observation_rows AS (
        SELECT item, protocol,
               encode(sha256(convert_to('buyer-history-observation-v1' || payload::text,
                                         'UTF8')), 'hex') AS observation_digest,
               encode(sha256(convert_to(
                   'enrichment-provenance-v1' || chr(10) || (item ->> 'evidence_id') ||
                   chr(10) || (item ->> 'transaction_reference'), 'UTF8')), 'hex') AS provenance_digest
        FROM matched_history
    )
    INSERT INTO agent_economy.observations
        (namespace_id, chain_scope, source_id, observation_id, observed_at,
         parser_version, protocol, evidence_id, provenance_id, observation_hash)
    SELECT bound_namespace, p_chain_scope, 'alchemy-history',
           'sha256:' || observation_digest, p_observed_date::timestamptz,
           'buyer-history@1', protocol, item ->> 'evidence_id',
           (substring(provenance_digest, 1, 8) || '-' || substring(provenance_digest, 9, 4) || '-' ||
            substring(provenance_digest, 13, 4) || '-' || substring(provenance_digest, 17, 4) || '-' ||
            substring(provenance_digest, 21, 12))::uuid,
           observation_digest
    FROM observation_rows
    ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.canonical_event_observations
        (namespace_id, protocol, canonical_event_id, chain_scope,
         source_id, observation_id, support_role)
    SELECT bound_namespace, settlement.protocol, settlement.canonical_event_id,
           p_chain_scope, 'alchemy-history', observation.observation_id, 'supporting'
    FROM jsonb_array_elements(p_records_json) AS record(item)
    JOIN agent_economy.event_finality_assertions AS finality
      ON finality.namespace_id = bound_namespace
     AND finality.chain_scope = p_chain_scope
     AND finality.transaction_id = record.item ->> 'transaction_reference'
     AND finality.accepted AND finality.current_status = 'finalized'
    JOIN agent_economy.settlements AS settlement
      ON settlement.namespace_id = finality.namespace_id
     AND settlement.chain_scope = finality.chain_scope
     AND settlement.protocol = finality.protocol
     AND settlement.canonical_event_id = finality.canonical_event_id
     AND settlement.buyer_handle_id = p_buyer_handle_id
    JOIN agent_economy.provenance_records AS provenance
      ON provenance.namespace_id = bound_namespace
     AND provenance.source_id = 'alchemy-history'
     AND provenance.chain_scope = p_chain_scope
     AND provenance.transaction_reference = record.item ->> 'transaction_reference'
     AND provenance.evidence_id = record.item ->> 'evidence_id'
    JOIN agent_economy.observations AS observation
      ON observation.namespace_id = provenance.namespace_id
     AND observation.chain_scope = provenance.chain_scope
     AND observation.source_id = provenance.source_id
     AND observation.protocol = settlement.protocol
     AND observation.provenance_id = provenance.provenance_id
    ON CONFLICT DO NOTHING;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
               'settlement_id', settlement.settlement_id,
               'amount_atomic', settlement.amount_atomic::text,
               'occurred_at_unix_seconds', extract(epoch FROM settlement.settled_at)::bigint,
               'protocol', settlement.protocol,
               'counterparty', COALESCE((SELECT min(candidate.service_id)
                   FROM agent_economy.attribution_candidates AS candidate
                   WHERE candidate.namespace_id = settlement.namespace_id
                     AND candidate.chain_scope = settlement.chain_scope
                     AND candidate.settlement_id = settlement.settlement_id), 'unattributed'),
               'autonomy', 'unknown',
               'evidence_id', record.item ->> 'evidence_id',
               'observation_id', observation.observation_id)
               ORDER BY settlement.settled_at, settlement.settlement_id), '[]'::jsonb)
    INTO reduced_activities
    FROM jsonb_array_elements(p_records_json) AS record(item)
    JOIN agent_economy.provenance_records AS provenance
      ON provenance.namespace_id = bound_namespace
     AND provenance.source_id = 'alchemy-history'
     AND provenance.chain_scope = p_chain_scope
     AND provenance.transaction_reference = record.item ->> 'transaction_reference'
     AND provenance.evidence_id = record.item ->> 'evidence_id'
    JOIN agent_economy.observations AS observation
      ON observation.namespace_id = provenance.namespace_id
     AND observation.chain_scope = provenance.chain_scope
     AND observation.source_id = provenance.source_id
     AND observation.provenance_id = provenance.provenance_id
    JOIN agent_economy.canonical_event_observations AS event_observation
      ON event_observation.namespace_id = observation.namespace_id
     AND event_observation.chain_scope = observation.chain_scope
     AND event_observation.source_id = observation.source_id
     AND event_observation.protocol = observation.protocol
     AND event_observation.observation_id = observation.observation_id
     AND event_observation.support_role = 'supporting'
    JOIN agent_economy.event_finality_assertions AS finality
      ON finality.namespace_id = event_observation.namespace_id
     AND finality.chain_scope = event_observation.chain_scope
     AND finality.protocol = event_observation.protocol
     AND finality.canonical_event_id = event_observation.canonical_event_id
     AND finality.transaction_id = record.item ->> 'transaction_reference'
     AND finality.accepted AND finality.current_status = 'finalized'
    JOIN agent_economy.settlements AS settlement
      ON settlement.namespace_id = finality.namespace_id
     AND settlement.chain_scope = finality.chain_scope
     AND settlement.protocol = finality.protocol
     AND settlement.canonical_event_id = finality.canonical_event_id
     AND settlement.buyer_handle_id = p_buyer_handle_id;

    INSERT INTO agent_economy.enrichment_reduction_receipts
        (namespace_id, enrichment_job_id, verified_records, reduced_activities)
    VALUES (bound_namespace, p_job_id, canonical_records, reduced_activities);

    IF jsonb_array_length(reduced_activities) > 0 THEN
        SELECT extract(epoch FROM min(settlement.settled_at))::bigint,
               GREATEST(extract(epoch FROM max(settlement.settled_at))::bigint + 1,
                        extract(epoch FROM min(settlement.settled_at))::bigint + 1),
               min(settlement.provenance_id::text)::uuid
        INTO window_start, window_end, classification_provenance
        FROM agent_economy.settlements AS settlement
        JOIN jsonb_array_elements(reduced_activities) AS activity(item)
          ON activity.item ->> 'settlement_id' = settlement.settlement_id
        WHERE settlement.namespace_id = bound_namespace;
        classification_manifest := jsonb_build_object(
            'schema_version', 1,
            'buyer_handle_id', p_buyer_handle_id,
            'run_id', 'classification:' || p_buyer_handle_id,
            'run_version', COALESCE((SELECT max(run_version) + 1
                FROM agent_economy.classification_runs
                WHERE namespace_id = bound_namespace AND buyer_handle_id = p_buyer_handle_id), 1),
            'classifier_version', 'buyer-classifier@1',
            'feature_version', 'behavior-features@1',
            'window_start_unix_seconds', window_start,
            'window_end_unix_seconds', window_end,
            'provenance_id', classification_provenance::text,
            'activities', reduced_activities,
            'labels', p_classification_labels);
        INSERT INTO agent_economy.classification_admission_requests
            (namespace_id, enrichment_job_id, input_manifest)
        VALUES (bound_namespace, p_job_id, classification_manifest);
    END IF;

    UPDATE agent_economy.buyer_enrichment_cursors AS cursor
    SET cursor = p_next_cursor, version = cursor.version + 1,
        requests_used_total = cursor.requests_used_total + p_requests_used,
        last_run_budget = (input.input_manifest ->> 'request_budget')::bigint,
        last_run_requests_used = p_requests_used, complete = p_complete,
        updated_at = clock_timestamp()
    WHERE cursor.namespace_id = bound_namespace AND cursor.buyer_handle_id = p_buyer_handle_id
      AND cursor.chain_scope = p_chain_scope AND cursor.version = p_cursor_version;
    IF NOT FOUND THEN RETURN false; END IF;

    INSERT INTO agent_economy.enrichment_job_receipts
        (namespace_id, job_id, input_sha256, output_sha256, evidence_json,
         records_json, protocol_attribution, classification_job_id)
    VALUES (bound_namespace, p_job_id, p_input_sha256, p_output_sha256,
            p_evidence_json, p_records_json, false, classification_job_id);
    UPDATE agent_economy.pending_enrichment_batches
    SET status = 'succeeded', verifier_owner = NULL, verifier_token = NULL,
        verifier_expires_at = NULL
    WHERE namespace_id = bound_namespace AND job_id = p_job_id;
    RETURN true;
END
$$;

CREATE FUNCTION agent_economy.renew_enrichment_job_lease(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid, p_lease_seconds bigint)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT agent_economy.renew_worker_job_lease(agent_economy.bound_enricher_namespace(),
        p_job_id, p_lease_owner, p_lease_token, p_lease_seconds)
$$;
CREATE FUNCTION agent_economy.complete_enrichment_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid, p_output_sha256 text)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT CASE WHEN EXISTS (SELECT 1 FROM agent_economy.pending_enrichment_batches AS pending
        JOIN agent_economy.worker_jobs AS job
          ON job.namespace_id = pending.namespace_id AND job.job_id = pending.job_id
        WHERE pending.namespace_id = agent_economy.bound_enricher_namespace()
          AND pending.job_id = p_job_id AND pending.output_sha256 = p_output_sha256
          AND job.status = 'leased' AND job.lease_owner = p_lease_owner
          AND job.lease_token = p_lease_token AND job.lease_expires_at > clock_timestamp())
    THEN agent_economy.complete_worker_job(agent_economy.bound_enricher_namespace(),
        p_job_id, p_lease_owner, p_lease_token, p_output_sha256) ELSE false END
$$;
CREATE FUNCTION agent_economy.fail_enrichment_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid,
    p_error_code text, p_retryable boolean, p_retry_delay_seconds bigint)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT agent_economy.fail_worker_job(agent_economy.bound_enricher_namespace(),
        p_job_id, p_lease_owner, p_lease_token, p_error_code, p_retryable, p_retry_delay_seconds)
$$;

-- Preserve the existing fail-closed runtime inventory while extending the
-- independent evidence verifier with enrichment claim/promotion authority.
CREATE OR REPLACE FUNCTION agent_economy.bound_collection_namespace(p_purpose text)
RETURNS uuid LANGUAGE plpgsql SECURITY DEFINER STABLE SET search_path = pg_catalog AS $$
DECLARE
    expected_login name;
    role_row pg_roles%ROWTYPE;
    expected_routines text[];
    actual_routines text[];
    result uuid;
BEGIN
    expected_login := CASE p_purpose
        WHEN 'collect' THEN 'agent_economy_collector_runtime'::name
        WHEN 'verify-evidence' THEN 'agent_economy_evidence_verifier_runtime'::name
        ELSE NULL END;
    expected_routines := CASE p_purpose
        WHEN 'collect' THEN ARRAY[
          'agent_economy.bound_collection_namespace(text)',
          'agent_economy.claim_bound_collection_job(text,bigint)',
          'agent_economy.complete_bound_collection_job(uuid,text,uuid,text)',
          'agent_economy.fail_bound_collection_job(uuid,text,uuid,text,boolean,bigint)',
          'agent_economy.renew_bound_collection_job(uuid,text,uuid,bigint)',
          'agent_economy.stage_collection_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb)'
        ]::text[]
        WHEN 'verify-evidence' THEN ARRAY[
          'agent_economy.bound_collection_namespace(text)',
          'agent_economy.claim_pending_collection_batch(text,bigint)',
          'agent_economy.claim_pending_enrichment_batch(text,bigint)',
          'agent_economy.commit_enrichment_batch(uuid,text,uuid,text,text,uuid,text,text,text,date,bigint,text,text,bigint,boolean,jsonb,bytea[],jsonb,jsonb)',
          'agent_economy.fail_pending_collection_batch(uuid,text,uuid,text,boolean)',
          'agent_economy.promote_pending_collection_batch(uuid,text,uuid,jsonb,text)'
        ]::text[]
        ELSE ARRAY[]::text[] END;
    IF expected_login IS NULL OR session_user::name <> expected_login THEN
        RAISE EXCEPTION 'runtime purpose/login mismatch';
    END IF;
    SELECT * INTO STRICT role_row FROM pg_roles WHERE rolname = expected_login;
    IF NOT role_row.rolcanlogin OR role_row.rolinherit OR role_row.rolsuper
       OR role_row.rolcreatedb OR role_row.rolcreaterole OR role_row.rolreplication
       OR role_row.rolbypassrls THEN
        RAISE EXCEPTION 'unsafe collection runtime login';
    END IF;
    IF EXISTS (
        WITH RECURSIVE inbound(roleid, member) AS (
            SELECT m.roleid, m.member FROM pg_auth_members m
            WHERE m.roleid = role_row.oid OR m.member = role_row.oid
            UNION
            SELECT m.roleid, m.member FROM pg_auth_members m
            JOIN inbound i ON m.roleid = i.member OR m.member = i.roleid
        ) SELECT 1 FROM inbound
    ) THEN RAISE EXCEPTION 'collection runtime login must be membership-isolated'; END IF;
    IF EXISTS (SELECT 1 FROM pg_shdepend d
        WHERE d.refclassid = 'pg_authid'::regclass AND d.refobjid = role_row.oid AND d.deptype = 'o')
    THEN RAISE EXCEPTION 'collection runtime login must not own database objects'; END IF;
    IF (SELECT count(*) FROM pg_database d
          CROSS JOIN LATERAL aclexplode(coalesce(d.datacl,acldefault('d',d.datdba))) acl
          WHERE acl.grantee=role_row.oid AND d.datname=current_database()
            AND acl.privilege_type='CONNECT') <> 1
       OR EXISTS (SELECT 1 FROM pg_database d
          CROSS JOIN LATERAL aclexplode(coalesce(d.datacl,acldefault('d',d.datdba))) acl
          WHERE acl.grantee=role_row.oid
            AND NOT (d.datname=current_database() AND acl.privilege_type='CONNECT'))
       OR NOT has_database_privilege(expected_login, current_database(), 'CONNECT')
       OR has_database_privilege(expected_login, current_database(), 'CREATE,TEMPORARY')
       OR NOT has_schema_privilege(expected_login, 'agent_economy', 'USAGE')
       OR has_schema_privilege(expected_login, 'agent_economy', 'CREATE')
       OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='agent_economy' AND c.relkind IN ('r','p','v','m','f')
            AND has_table_privilege(expected_login,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))
       OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='agent_economy' AND c.relkind IN ('r','p','v','m','f')
            AND has_any_column_privilege(expected_login,c.oid,'SELECT,INSERT,UPDATE,REFERENCES'))
       OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='agent_economy' AND CASE WHEN c.relkind='S'
            THEN has_sequence_privilege(expected_login,c.oid,'USAGE,SELECT,UPDATE') ELSE false END)
       OR EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
          WHERE n.nspname='agent_economy' AND t.typrelid=0 AND t.typelem=0
            AND has_type_privilege(expected_login,t.oid,'USAGE'))
    THEN RAISE EXCEPTION 'unsafe collection runtime object authority'; END IF;
    SELECT coalesce(array_agg(p.oid::regprocedure::text ORDER BY p.oid::regprocedure::text),ARRAY[]::text[])
      INTO actual_routines
      FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
      WHERE n.nspname='agent_economy' AND acl.grantee=role_row.oid AND acl.privilege_type='EXECUTE';
    IF actual_routines <> expected_routines THEN
       RAISE EXCEPTION 'unsafe collection runtime routine authority';
    END IF;
    IF EXISTS (SELECT 1 FROM pg_database d
          CROSS JOIN LATERAL aclexplode(coalesce(d.datacl,acldefault('d',d.datdba))) acl
          WHERE d.datname=current_database() AND acl.grantee=0
            AND acl.privilege_type IN ('CONNECT','CREATE','TEMPORARY'))
       OR EXISTS (SELECT 1 FROM pg_namespace n
          CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) acl
          WHERE n.nspname='agent_economy' AND acl.grantee=0 AND acl.privilege_type='CREATE')
       OR EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,c.relowner))) acl
          WHERE n.nspname='agent_economy' AND acl.grantee=0)
       OR EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
          CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
          WHERE n.nspname='agent_economy' AND acl.grantee=0 AND acl.privilege_type='EXECUTE')
       OR EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
          CROSS JOIN LATERAL aclexplode(coalesce(t.typacl,acldefault('T',t.typowner))) acl
          WHERE n.nspname='agent_economy' AND t.typrelid=0 AND t.typelem=0
            AND acl.grantee=0 AND acl.privilege_type='USAGE')
       OR EXISTS (SELECT 1 FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) acl
          WHERE acl.grantee IN (0,role_row.oid))
    THEN RAISE EXCEPTION 'dangerous ambient collection authority'; END IF;
    SELECT mapping.namespace_id INTO STRICT result
    FROM agent_economy.collection_runtime_namespaces mapping
    WHERE mapping.login_name = expected_login AND mapping.purpose = p_purpose;
    RETURN result;
END
$$;

DO $$
DECLARE enricher_role pg_roles%ROWTYPE;
BEGIN
    BEGIN CREATE ROLE agent_economy_enricher_runtime LOGIN NOINHERIT;
    EXCEPTION WHEN duplicate_object THEN NULL; END;
    SELECT * INTO STRICT enricher_role FROM pg_roles
    WHERE rolname = 'agent_economy_enricher_runtime';
    IF NOT enricher_role.rolcanlogin OR enricher_role.rolinherit OR enricher_role.rolsuper
       OR enricher_role.rolcreatedb OR enricher_role.rolcreaterole
       OR enricher_role.rolreplication OR enricher_role.rolbypassrls
    THEN RAISE EXCEPTION 'agent_economy_enricher_runtime must be unprivileged'; END IF;
    IF EXISTS (SELECT 1 FROM pg_auth_members
               WHERE member = enricher_role.oid OR roleid = enricher_role.oid)
    THEN RAISE EXCEPTION 'agent_economy_enricher_runtime must be isolated'; END IF;
END
$$;
DO $migration$
BEGIN EXECUTE format('GRANT CONNECT ON DATABASE %I TO agent_economy_enricher_runtime', current_database()); END
$migration$;
REVOKE ALL ON agent_economy.worker_jobs FROM agent_economy_enricher_runtime;
REVOKE ALL ON agent_economy.enrichment_job_inputs FROM PUBLIC, agent_economy_enricher_runtime;
REVOKE ALL ON agent_economy.pending_enrichment_batches FROM PUBLIC, agent_economy_enricher_runtime, agent_economy_evidence_verifier_runtime;
REVOKE ALL ON agent_economy.enrichment_job_receipts FROM PUBLIC, agent_economy_enricher_runtime;
REVOKE ALL ON agent_economy.enrichment_reduction_receipts FROM PUBLIC, agent_economy_enricher_runtime;
REVOKE ALL ON agent_economy.classification_admission_requests FROM PUBLIC, agent_economy_enricher_runtime;
REVOKE ALL ON agent_economy.enricher_runtime_namespaces FROM PUBLIC, agent_economy_enricher_runtime;
REVOKE ALL ON agent_economy.evidence_objects FROM agent_economy_enricher_runtime;
REVOKE ALL ON agent_economy.provenance_records FROM agent_economy_enricher_runtime;
REVOKE ALL ON agent_economy.buyer_finalized_history FROM agent_economy_enricher_runtime;
REVOKE ALL ON FUNCTION agent_economy.validate_enrichment_job_input() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.bound_enricher_namespace() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.claim_enrichment_job(text, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.load_enrichment_job_input(uuid, text, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.stage_enrichment_batch(uuid, text, uuid, text, text, uuid, text, text, text, date, bigint, text, text, bigint, boolean, jsonb, jsonb, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.claim_pending_enrichment_batch(text, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.commit_enrichment_batch(uuid, text, uuid, text, text, uuid, text, text, text, date, bigint, text, text, bigint, boolean, jsonb, bytea[], jsonb, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.renew_enrichment_job_lease(uuid, text, uuid, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.complete_enrichment_job(uuid, text, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.fail_enrichment_job(uuid, text, uuid, text, boolean, bigint) FROM PUBLIC;
GRANT USAGE ON SCHEMA agent_economy TO agent_economy_enricher_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.bound_enricher_namespace() TO agent_economy_enricher_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.claim_enrichment_job(text, bigint) TO agent_economy_enricher_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.load_enrichment_job_input(uuid, text, uuid) TO agent_economy_enricher_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.stage_enrichment_batch(uuid, text, uuid, text, text, uuid, text, text, text, date, bigint, text, text, bigint, boolean, jsonb, jsonb, jsonb) TO agent_economy_enricher_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.claim_pending_enrichment_batch(text, bigint) TO agent_economy_evidence_verifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.commit_enrichment_batch(uuid, text, uuid, text, text, uuid, text, text, text, date, bigint, text, text, bigint, boolean, jsonb, bytea[], jsonb, jsonb) TO agent_economy_evidence_verifier_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.renew_enrichment_job_lease(uuid, text, uuid, bigint) TO agent_economy_enricher_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.complete_enrichment_job(uuid, text, uuid, text) TO agent_economy_enricher_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.fail_enrichment_job(uuid, text, uuid, text, boolean, bigint) TO agent_economy_enricher_runtime;

COMMIT;
