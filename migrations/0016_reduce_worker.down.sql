BEGIN;

LOCK TABLE agent_economy.reduction_range_receipts IN ACCESS EXCLUSIVE MODE;
DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.reduction_range_receipts LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.reducer_checkpoints WHERE version > 0 LIMIT 1)
    THEN
        RAISE EXCEPTION 'cannot roll back reducer worker history';
    END IF;
END
$block$;

DROP FUNCTION agent_economy.fail_reducer_job(uuid, text, uuid, text, boolean, bigint);
DROP FUNCTION agent_economy.complete_reducer_job(uuid, text, uuid, text);
DROP FUNCTION agent_economy.renew_reducer_job_lease(uuid, text, uuid, bigint);
DROP FUNCTION agent_economy.commit_reduction_batch(
    uuid, text, uuid, text, text, text, text, bigint, bigint, jsonb, jsonb, jsonb
);
DROP FUNCTION agent_economy.load_reducer_job_input(uuid, text, uuid);
DROP FUNCTION agent_economy.claim_reducer_job(text, bigint);
DROP FUNCTION agent_economy.bound_reducer_namespace();
DROP TRIGGER reducer_job_inputs_validate ON agent_economy.reducer_job_inputs;
DROP FUNCTION agent_economy.validate_reducer_job_input();
DROP TABLE agent_economy.reducer_runtime_namespaces;
DROP TABLE agent_economy.reduction_range_receipts;
DROP TABLE agent_economy.reducer_checkpoints;
DROP TABLE agent_economy.reducer_job_inputs;
DO $migration$
BEGIN
    EXECUTE format(
        'REVOKE CONNECT ON DATABASE %I FROM agent_economy_reducer_runtime',
        current_database()
    );
END
$migration$;
REVOKE USAGE ON SCHEMA agent_economy FROM agent_economy_reducer_runtime;
DROP ROLE agent_economy_reducer_runtime;

COMMIT;
