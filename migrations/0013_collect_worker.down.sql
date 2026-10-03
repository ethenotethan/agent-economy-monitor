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
REVOKE ALL ON FUNCTION agent_economy.attest_collection_evidence(
    uuid, uuid, uuid, text, text, text, text, bigint, bigint
) FROM agent_economy_evidence_verifier_runtime;
DROP FUNCTION agent_economy.fail_collection_job(
    uuid, uuid, text, uuid, text, boolean, bigint
);
DROP FUNCTION agent_economy.complete_collection_job(uuid, uuid, text, uuid, text);
DROP FUNCTION agent_economy.renew_collection_job_lease(uuid, uuid, text, uuid, bigint);
DROP FUNCTION agent_economy.claim_collection_job(uuid, text, bigint);
DROP FUNCTION IF EXISTS agent_economy.commit_collection_batch(
    uuid, uuid, text, uuid, text, text, text, text, bigint, bigint, bigint, jsonb, jsonb
);
DROP FUNCTION IF EXISTS agent_economy.attest_collection_evidence(
    uuid, uuid, uuid, text, text, text, text, bigint, bigint
);
DROP TABLE IF EXISTS agent_economy.collection_range_receipts;
DROP TABLE IF EXISTS agent_economy.collection_evidence_attestations;
DROP TABLE IF EXISTS agent_economy.collection_cursors;

ALTER TABLE agent_economy.worker_jobs
    DROP COLUMN IF EXISTS collection_evidence_contract,
    DROP COLUMN IF EXISTS collection_acquisition_contract,
    DROP COLUMN IF EXISTS collection_end_height,
    DROP COLUMN IF EXISTS collection_start_height,
    DROP COLUMN IF EXISTS collection_source_id,
    DROP COLUMN IF EXISTS collection_chain_scope;
DROP ROLE agent_economy_evidence_verifier_runtime;
DROP ROLE agent_economy_collector_runtime;

COMMIT;
