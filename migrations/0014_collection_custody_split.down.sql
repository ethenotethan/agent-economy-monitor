BEGIN;
DO $$
BEGIN
    RAISE EXCEPTION 'collection custody split is irreversible; restore from a pre-0014 backup';
END
$$;
DROP FUNCTION IF EXISTS agent_economy.fail_pending_collection_batch(uuid,text,uuid,text,boolean);
DROP FUNCTION IF EXISTS agent_economy.promote_pending_collection_batch(uuid,text,uuid,jsonb,text);
DROP FUNCTION IF EXISTS agent_economy.claim_pending_collection_batch(text,bigint);
DROP FUNCTION IF EXISTS agent_economy.stage_collection_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb);
DROP FUNCTION IF EXISTS agent_economy.fail_bound_collection_job(uuid,text,uuid,text,boolean,bigint);
DROP FUNCTION IF EXISTS agent_economy.complete_bound_collection_job(uuid,text,uuid,text);
DROP FUNCTION IF EXISTS agent_economy.renew_bound_collection_job(uuid,text,uuid,bigint);
DROP FUNCTION IF EXISTS agent_economy.claim_bound_collection_job(text,bigint);
DROP FUNCTION IF EXISTS agent_economy.bound_collection_namespace(text);
DROP TABLE IF EXISTS agent_economy.pending_collection_batches;
DROP TABLE IF EXISTS agent_economy.collection_runtime_namespaces;
COMMIT;
