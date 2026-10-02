BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.worker_jobs) THEN
        RAISE EXCEPTION 'worker job rollback blocked: worker history exists';
    END IF;
END
$$;

DROP FUNCTION agent_economy.cancel_worker_job(uuid, uuid, text, uuid);
DROP FUNCTION agent_economy.fail_worker_job(uuid, uuid, text, uuid, text, boolean, bigint);
DROP FUNCTION agent_economy.complete_worker_job(uuid, uuid, text, uuid, text);
DROP FUNCTION agent_economy.renew_worker_job_lease(uuid, uuid, text, uuid, bigint);
DROP FUNCTION agent_economy.claim_worker_job(uuid, text, text, bigint);
DROP TABLE agent_economy.worker_jobs;
DROP FUNCTION agent_economy.protect_worker_job_identity();

COMMIT;
