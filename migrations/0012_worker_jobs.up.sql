BEGIN;

CREATE TABLE agent_economy.worker_jobs (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    job_id uuid NOT NULL,
    mode text NOT NULL CHECK (mode IN ('collect', 'reduce', 'classify', 'enrich')),
    job_kind text NOT NULL CHECK (job_kind <> ''),
    idempotency_key text NOT NULL CHECK (idempotency_key <> ''),
    input_sha256 text NOT NULL CHECK (input_sha256 ~ '^[0-9a-f]{64}$'),
    output_sha256 text CHECK (output_sha256 ~ '^[0-9a-f]{64}$'),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'leased', 'retryable', 'succeeded', 'dead_letter', 'cancelled')),
    scheduled_for timestamptz NOT NULL DEFAULT now(),
    max_attempts smallint NOT NULL DEFAULT 3 CHECK (max_attempts BETWEEN 1 AND 100),
    attempt_count smallint NOT NULL DEFAULT 0 CHECK (attempt_count BETWEEN 0 AND max_attempts),
    lease_owner text,
    lease_token uuid,
    lease_expires_at timestamptz,
    last_error_code text CHECK (last_error_code IS NULL OR last_error_code ~ '^[a-z0-9_]{1,64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, job_id),
    UNIQUE (namespace_id, idempotency_key),
    CHECK (
        (status = 'leased' AND lease_owner IS NOT NULL AND lease_token IS NOT NULL
            AND lease_expires_at IS NOT NULL)
        OR (status <> 'leased' AND lease_owner IS NULL AND lease_token IS NULL
            AND lease_expires_at IS NULL)
    ),
    CHECK ((status = 'succeeded' AND output_sha256 IS NOT NULL)
        OR (status <> 'succeeded' AND output_sha256 IS NULL))
);

CREATE INDEX worker_jobs_claim_idx
ON agent_economy.worker_jobs (namespace_id, mode, status, scheduled_for, created_at, job_id)
WHERE status IN ('pending', 'retryable', 'leased');

CREATE FUNCTION agent_economy.protect_worker_job_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(
        OLD.namespace_id, OLD.job_id, OLD.mode, OLD.job_kind,
        OLD.idempotency_key, OLD.input_sha256, OLD.max_attempts, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.namespace_id, NEW.job_id, NEW.mode, NEW.job_kind,
        NEW.idempotency_key, NEW.input_sha256, NEW.max_attempts, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'worker job identity is immutable';
    END IF;
    IF OLD.status IN ('succeeded', 'dead_letter', 'cancelled') AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'terminal worker job is immutable';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER worker_jobs_protect_identity
BEFORE UPDATE ON agent_economy.worker_jobs
FOR EACH ROW EXECUTE FUNCTION agent_economy.protect_worker_job_identity();

CREATE FUNCTION agent_economy.claim_worker_job(
    p_namespace_id uuid,
    p_mode text,
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
BEGIN
    IF p_mode NOT IN ('collect', 'reduce', 'classify', 'enrich')
        OR p_lease_owner !~ '^[A-Za-z0-9_.:-]{1,128}$'
        OR p_lease_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid worker lease request';
    END IF;

    UPDATE agent_economy.worker_jobs AS expired
    SET status = 'dead_letter', lease_owner = NULL, lease_token = NULL,
        lease_expires_at = NULL, last_error_code = 'lease_expired',
        updated_at = clock_timestamp()
    WHERE expired.namespace_id = p_namespace_id AND expired.mode = p_mode
        AND expired.status = 'leased'
        AND expired.lease_expires_at <= clock_timestamp()
        AND expired.attempt_count >= expired.max_attempts;

    RETURN QUERY
    WITH candidate AS (
        SELECT queued.namespace_id, queued.job_id
        FROM agent_economy.worker_jobs AS queued
        WHERE queued.namespace_id = p_namespace_id AND queued.mode = p_mode
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
        claimed.attempt_count, claimed.lease_owner, claimed.lease_token;
END
$$;

CREATE FUNCTION agent_economy.renew_worker_job_lease(
    p_namespace_id uuid,
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid,
    p_lease_seconds bigint
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    WITH renewed AS (
        UPDATE agent_economy.worker_jobs AS job
        SET lease_expires_at = clock_timestamp() + (p_lease_seconds * interval '1 second'),
            updated_at = clock_timestamp()
        WHERE p_lease_seconds BETWEEN 1 AND 3600
            AND job.namespace_id = p_namespace_id AND job.job_id = p_job_id
            AND job.status = 'leased' AND job.lease_owner = p_lease_owner
            AND job.lease_token = p_lease_token
            AND job.lease_expires_at > clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) = 1 FROM renewed
$$;

CREATE FUNCTION agent_economy.complete_worker_job(
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
            AND job.status = 'leased' AND job.lease_owner = p_lease_owner
            AND job.lease_token = p_lease_token
            AND job.lease_expires_at > clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) = 1 FROM completed
$$;

CREATE FUNCTION agent_economy.fail_worker_job(
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
            AND job.status = 'leased' AND job.lease_owner = p_lease_owner
            AND job.lease_token = p_lease_token
            AND job.lease_expires_at > clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) = 1 FROM failed
$$;

CREATE FUNCTION agent_economy.cancel_worker_job(
    p_namespace_id uuid,
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid
)
RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
    WITH cancelled AS (
        UPDATE agent_economy.worker_jobs AS job
        SET status = 'cancelled', lease_owner = NULL, lease_token = NULL,
            lease_expires_at = NULL, last_error_code = 'cancelled',
            updated_at = clock_timestamp()
        WHERE job.namespace_id = p_namespace_id AND job.job_id = p_job_id
            AND job.status = 'leased' AND job.lease_owner = p_lease_owner
            AND job.lease_token = p_lease_token
            AND job.lease_expires_at > clock_timestamp()
        RETURNING 1
    )
    SELECT count(*) = 1 FROM cancelled
$$;

DO $$
DECLARE
    worker_role pg_roles%ROWTYPE;
BEGIN
    BEGIN
        CREATE ROLE agent_economy_worker NOLOGIN;
    EXCEPTION WHEN duplicate_object THEN NULL;
    END;

    SELECT * INTO STRICT worker_role
    FROM pg_roles
    WHERE rolname = 'agent_economy_worker';

    IF worker_role.rolcanlogin
        OR worker_role.rolsuper
        OR worker_role.rolcreatedb
        OR worker_role.rolcreaterole
        OR worker_role.rolreplication
        OR worker_role.rolbypassrls
    THEN
        RAISE EXCEPTION 'agent_economy_worker must be an unprivileged NOLOGIN role';
    END IF;

    IF EXISTS (
        SELECT 1
        FROM pg_auth_members AS membership
        WHERE membership.member = worker_role.oid
    ) THEN
        RAISE EXCEPTION 'agent_economy_worker must not inherit roles';
    END IF;
END
$$;

REVOKE ALL ON agent_economy.worker_jobs FROM PUBLIC;
REVOKE ALL ON agent_economy.worker_jobs FROM agent_economy_worker;
REVOKE ALL ON FUNCTION agent_economy.protect_worker_job_identity() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.claim_worker_job(uuid, text, text, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.renew_worker_job_lease(uuid, uuid, text, uuid, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.complete_worker_job(uuid, uuid, text, uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.fail_worker_job(uuid, uuid, text, uuid, text, boolean, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.cancel_worker_job(uuid, uuid, text, uuid) FROM PUBLIC;
GRANT USAGE ON SCHEMA agent_economy TO agent_economy_worker;
GRANT EXECUTE ON FUNCTION agent_economy.claim_worker_job(uuid, text, text, bigint)
    TO agent_economy_worker;
GRANT EXECUTE ON FUNCTION agent_economy.renew_worker_job_lease(uuid, uuid, text, uuid, bigint)
    TO agent_economy_worker;
GRANT EXECUTE ON FUNCTION agent_economy.complete_worker_job(uuid, uuid, text, uuid, text)
    TO agent_economy_worker;
GRANT EXECUTE ON FUNCTION agent_economy.fail_worker_job(uuid, uuid, text, uuid, text, boolean, bigint)
    TO agent_economy_worker;
GRANT EXECUTE ON FUNCTION agent_economy.cancel_worker_job(uuid, uuid, text, uuid)
    TO agent_economy_worker;
GRANT agent_economy_worker TO CURRENT_USER;

COMMIT;
