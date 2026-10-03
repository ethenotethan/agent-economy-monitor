BEGIN;

CREATE TABLE agent_economy.collection_runtime_namespaces (
    login_name name PRIMARY KEY,
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    purpose text NOT NULL CHECK (purpose IN ('collect', 'verify-evidence')),
    UNIQUE (namespace_id, purpose)
);

CREATE TABLE agent_economy.pending_collection_batches (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    pending_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id uuid NOT NULL,
    input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    batch_sha256 text NOT NULL CHECK (batch_sha256 ~ '^[0-9a-f]{64}$'),
    chain_scope text NOT NULL CHECK (chain_scope IN ('ethereum', 'base', 'solana', 'tempo')),
    source_id text NOT NULL CHECK (source_id <> ''),
    observed_at_unix_ms bigint NOT NULL CHECK (observed_at_unix_ms > 0),
    start_height bigint NOT NULL CHECK (start_height >= 0),
    end_height bigint NOT NULL CHECK (end_height >= start_height),
    evidence_json jsonb NOT NULL CHECK (jsonb_typeof(evidence_json) = 'array'),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'verifying', 'succeeded', 'retryable', 'dead_letter')),
    attempt_count smallint NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts smallint NOT NULL DEFAULT 5 CHECK (max_attempts BETWEEN 1 AND 100),
    scheduled_for timestamptz NOT NULL DEFAULT clock_timestamp(),
    verifier_owner text,
    verifier_token uuid,
    verifier_expires_at timestamptz,
    verifier_result_sha256 text CHECK (verifier_result_sha256 IS NULL OR verifier_result_sha256 ~ '^[0-9a-f]{64}$'),
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (namespace_id, job_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id),
    CHECK ((status = 'verifying') =
        (verifier_owner IS NOT NULL AND verifier_token IS NOT NULL AND verifier_expires_at IS NOT NULL))
);

REVOKE ALL ON agent_economy.collection_runtime_namespaces FROM PUBLIC;
REVOKE ALL ON agent_economy.pending_collection_batches FROM PUBLIC;

CREATE FUNCTION agent_economy.bound_collection_namespace(p_purpose text)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    expected_login name;
    role_row pg_roles%ROWTYPE;
    result uuid;
BEGIN
    expected_login := CASE p_purpose
        WHEN 'collect' THEN 'agent_economy_collector_runtime'::name
        WHEN 'verify-evidence' THEN 'agent_economy_evidence_verifier_runtime'::name
        ELSE NULL
    END;
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
    ) THEN
        RAISE EXCEPTION 'collection runtime login must be membership-isolated';
    END IF;
    SELECT mapping.namespace_id INTO STRICT result
    FROM agent_economy.collection_runtime_namespaces mapping
    WHERE mapping.login_name = expected_login AND mapping.purpose = p_purpose;
    RETURN result;
END
$$;

CREATE FUNCTION agent_economy.claim_bound_collection_job(
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
DECLARE
    bound_namespace uuid;
BEGIN
    bound_namespace := agent_economy.bound_collection_namespace('collect');
    RETURN QUERY SELECT * FROM agent_economy.claim_collection_job(
        bound_namespace, p_lease_owner, p_lease_seconds
    );
END
$$;

CREATE FUNCTION agent_economy.renew_bound_collection_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid, p_lease_seconds bigint
)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT agent_economy.renew_collection_job_lease(
        agent_economy.bound_collection_namespace('collect'),
        p_job_id, p_lease_owner, p_lease_token, p_lease_seconds)
$$;

CREATE FUNCTION agent_economy.complete_bound_collection_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid, p_output_sha256 text
)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT agent_economy.complete_collection_job(
        agent_economy.bound_collection_namespace('collect'),
        p_job_id, p_lease_owner, p_lease_token, p_output_sha256)
$$;

CREATE FUNCTION agent_economy.fail_bound_collection_job(
    p_job_id uuid, p_lease_owner text, p_lease_token uuid,
    p_error_code text, p_retryable boolean, p_retry_delay_seconds bigint
)
RETURNS boolean LANGUAGE sql SECURITY DEFINER SET search_path = pg_catalog AS $$
    SELECT agent_economy.fail_collection_job(
        agent_economy.bound_collection_namespace('collect'),
        p_job_id, p_lease_owner, p_lease_token,
        p_error_code, p_retryable, p_retry_delay_seconds)
$$;

CREATE FUNCTION agent_economy.stage_collection_batch(
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
      AND job.collection_acquisition_contract = 'alchemy-rpc-block-v1'
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
            OR (item->>'media_type') <> 'application/vnd.agent-economy.rpc'
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

CREATE FUNCTION agent_economy.claim_pending_collection_batch(
    p_verifier_owner text, p_lease_seconds bigint
)
RETURNS TABLE (
    pending_id uuid, job_id uuid, verifier_token uuid, input_sha256 text,
    batch_sha256 text, chain_scope text, source_id text, observed_at_unix_ms bigint,
    start_height bigint, end_height bigint, evidence_json jsonb
)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE bound_namespace uuid;
BEGIN
    bound_namespace := agent_economy.bound_collection_namespace('verify-evidence');
    IF p_verifier_owner !~ '^[A-Za-z0-9_.:-]{1,128}$' OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN RAISE EXCEPTION 'invalid verifier lease'; END IF;
    RETURN QUERY
    WITH candidate AS (
      SELECT pending.pending_id FROM agent_economy.pending_collection_batches pending
      JOIN agent_economy.worker_jobs job
        ON job.namespace_id = pending.namespace_id AND job.job_id = pending.job_id
      WHERE pending.namespace_id = bound_namespace AND job.status = 'succeeded'
        AND job.output_sha256 = pending.batch_sha256
        AND pending.attempt_count < pending.max_attempts
        AND ((pending.status IN ('pending','retryable') AND pending.scheduled_for <= clock_timestamp())
             OR (pending.status='verifying' AND pending.verifier_expires_at <= clock_timestamp()))
      ORDER BY pending.created_at, pending.pending_id FOR UPDATE OF pending SKIP LOCKED LIMIT 1
    )
    UPDATE agent_economy.pending_collection_batches claimed
    SET status='verifying', verifier_owner=p_verifier_owner, verifier_token=gen_random_uuid(),
        verifier_expires_at=clock_timestamp() + p_lease_seconds * interval '1 second',
        attempt_count=claimed.attempt_count+1, last_error_code=NULL, updated_at=clock_timestamp()
    FROM candidate WHERE claimed.pending_id=candidate.pending_id
    RETURNING claimed.pending_id, claimed.job_id, claimed.verifier_token,
      claimed.input_sha256, claimed.batch_sha256, claimed.chain_scope, claimed.source_id,
      claimed.observed_at_unix_ms, claimed.start_height, claimed.end_height, claimed.evidence_json;
END
$$;

CREATE FUNCTION agent_economy.promote_pending_collection_batch(
    p_pending_id uuid, p_verifier_owner text, p_verifier_token uuid,
    p_observations_json jsonb, p_verifier_result_sha256 text
)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE pending agent_economy.pending_collection_batches%ROWTYPE; promoted boolean;
BEGIN
    SELECT * INTO pending FROM agent_economy.pending_collection_batches batch
    WHERE batch.namespace_id = agent_economy.bound_collection_namespace('verify-evidence')
      AND batch.pending_id = p_pending_id AND batch.status = 'verifying'
      AND batch.verifier_owner = p_verifier_owner AND batch.verifier_token = p_verifier_token
      AND batch.verifier_expires_at > clock_timestamp() FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    IF p_verifier_result_sha256 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'invalid verifier result';
    END IF;
    -- The inner function is owner-only after this migration. Its arguments are the exact
    -- immutable pending row plus observations derived by the storage-reader process.
    SELECT agent_economy.commit_collection_batch(
      pending.namespace_id, pending.job_id, p_verifier_owner, p_verifier_token,
      pending.input_sha256, pending.batch_sha256, pending.chain_scope, pending.source_id,
      pending.observed_at_unix_ms, pending.start_height, pending.end_height,
      pending.evidence_json, p_observations_json) INTO promoted;
    IF promoted THEN
      UPDATE agent_economy.pending_collection_batches SET status='succeeded',
        verifier_owner=NULL, verifier_token=NULL, verifier_expires_at=NULL,
        verifier_result_sha256=p_verifier_result_sha256, updated_at=clock_timestamp()
      WHERE pending_id=p_pending_id;
    END IF;
    RETURN promoted;
END
$$;

CREATE FUNCTION agent_economy.fail_pending_collection_batch(
    p_pending_id uuid, p_verifier_owner text, p_verifier_token uuid,
    p_error_code text, p_retryable boolean
)
RETURNS boolean LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE bound_namespace uuid;
BEGIN
  bound_namespace := agent_economy.bound_collection_namespace('verify-evidence');
  UPDATE agent_economy.pending_collection_batches batch SET
    status=CASE WHEN p_retryable AND batch.attempt_count < batch.max_attempts THEN 'retryable' ELSE 'dead_letter' END,
    scheduled_for=clock_timestamp()+interval '30 seconds', verifier_owner=NULL,
    verifier_token=NULL, verifier_expires_at=NULL, last_error_code=p_error_code,
    updated_at=clock_timestamp()
  WHERE batch.namespace_id=bound_namespace AND batch.pending_id=p_pending_id
    AND batch.status='verifying' AND batch.verifier_owner=p_verifier_owner
    AND batch.verifier_token=p_verifier_token AND batch.verifier_expires_at>clock_timestamp()
    AND p_error_code ~ '^[a-z0-9_]{1,64}$';
  RETURN FOUND;
END
$$;

-- The old assertion and direct promotion capabilities are unreachable to either runtime.
REVOKE ALL ON FUNCTION agent_economy.claim_collection_job(uuid,text,bigint) FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.renew_collection_job_lease(uuid,uuid,text,uuid,bigint) FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.complete_collection_job(uuid,uuid,text,uuid,text) FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.fail_collection_job(uuid,uuid,text,uuid,text,boolean,bigint) FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.commit_collection_batch(uuid,uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,jsonb) FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.attest_collection_evidence(uuid,uuid,uuid,text,text,text,text,bigint,bigint) FROM agent_economy_evidence_verifier_runtime;
DROP FUNCTION agent_economy.attest_collection_evidence(uuid,uuid,uuid,text,text,text,text,bigint,bigint);
DROP TABLE agent_economy.collection_evidence_attestations;

REVOKE ALL ON FUNCTION agent_economy.bound_collection_namespace(text) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.claim_bound_collection_job(text,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.renew_bound_collection_job(uuid,text,uuid,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.complete_bound_collection_job(uuid,text,uuid,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.fail_bound_collection_job(uuid,text,uuid,text,boolean,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.stage_collection_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.claim_pending_collection_batch(text,bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.promote_pending_collection_batch(uuid,text,uuid,jsonb,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.fail_pending_collection_batch(uuid,text,uuid,text,boolean) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION agent_economy.claim_bound_collection_job(text,bigint),
 agent_economy.renew_bound_collection_job(uuid,text,uuid,bigint),
 agent_economy.complete_bound_collection_job(uuid,text,uuid,text),
 agent_economy.fail_bound_collection_job(uuid,text,uuid,text,boolean,bigint),
 agent_economy.stage_collection_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb)
TO agent_economy_collector_runtime;
GRANT EXECUTE ON FUNCTION agent_economy.claim_pending_collection_batch(text,bigint),
 agent_economy.promote_pending_collection_batch(uuid,text,uuid,jsonb,text),
 agent_economy.fail_pending_collection_batch(uuid,text,uuid,text,boolean)
TO agent_economy_evidence_verifier_runtime;

COMMIT;
