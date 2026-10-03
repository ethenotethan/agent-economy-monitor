BEGIN;

DO $rollback$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.collection_cursors LIMIT 1) THEN
        RAISE EXCEPTION 'cannot roll back non-empty collection cursor state';
    END IF;
    IF EXISTS (SELECT 1 FROM agent_economy.collection_range_receipts LIMIT 1) THEN
        RAISE EXCEPTION 'cannot roll back non-empty collection receipt state';
    END IF;
END
$rollback$;

REVOKE ALL ON FUNCTION agent_economy.claim_collection_job(uuid, text, bigint)
    FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.renew_collection_job_lease(uuid, uuid, text, uuid, bigint)
    FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.complete_collection_job(uuid, uuid, text, uuid, text)
    FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.fail_collection_job(
    uuid, uuid, text, uuid, text, boolean, bigint
) FROM agent_economy_collector_runtime;
REVOKE ALL ON FUNCTION agent_economy.commit_collection_batch(
    uuid, uuid, text, uuid, text, text, text, text, bigint, bigint, bigint, jsonb, jsonb
) FROM agent_economy_collector_runtime;
DROP FUNCTION agent_economy.fail_collection_job(
    uuid, uuid, text, uuid, text, boolean, bigint
);
DROP FUNCTION agent_economy.complete_collection_job(uuid, uuid, text, uuid, text);
DROP FUNCTION agent_economy.renew_collection_job_lease(uuid, uuid, text, uuid, bigint);
DROP FUNCTION agent_economy.claim_collection_job(uuid, text, bigint);
DROP FUNCTION agent_economy.commit_collection_batch(
    uuid, uuid, text, uuid, text, text, text, text, bigint, bigint, bigint, jsonb, jsonb
);
DROP TABLE agent_economy.collection_range_receipts;
DROP TABLE agent_economy.collection_cursors;
DROP ROLE agent_economy_collector_runtime;

COMMIT;
