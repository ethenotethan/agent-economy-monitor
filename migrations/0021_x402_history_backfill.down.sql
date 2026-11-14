BEGIN;

LOCK TABLE agent_economy.x402_settlement_observation_links,
    agent_economy.x402_payment_observations,
    agent_economy.x402_history_ranges,
    agent_economy.x402_history_plans,
    agent_economy.x402_history_sources
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.x402_payment_observations)
       OR EXISTS (SELECT 1 FROM agent_economy.x402_settlement_observation_links)
       OR EXISTS (SELECT 1 FROM agent_economy.x402_history_ranges)
    THEN
        RAISE EXCEPTION 'cannot roll back x402 history with retained observations';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION agent_economy.fail_pending_collection_batch(
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

DROP VIEW agent_economy.x402_history_coverage;
DROP TRIGGER bind_pending_collection_registry ON agent_economy.pending_collection_batches;
DROP FUNCTION agent_economy.bind_pending_collection_registry();
DROP FUNCTION agent_economy.reconcile_x402_history_range(uuid,text,text,bigint,bigint);
DROP FUNCTION agent_economy.plan_x402_history_jobs(uuid,text,text,jsonb);
DROP FUNCTION agent_economy.admit_x402_history_source(uuid,text,text,bigint,timestamptz,bigint,text,text,bigint,bigint,uuid);
DROP TRIGGER x402_settlement_observation_links_immutable
    ON agent_economy.x402_settlement_observation_links;
DROP TRIGGER x402_payment_observations_immutable
    ON agent_economy.x402_payment_observations;
DROP TRIGGER x402_history_ranges_immutable
    ON agent_economy.x402_history_ranges;
DROP TRIGGER x402_history_sources_immutable
    ON agent_economy.x402_history_sources;
DROP TRIGGER x402_history_plans_immutable
    ON agent_economy.x402_history_plans;
DROP TABLE agent_economy.x402_settlement_observation_links;
DROP TABLE agent_economy.x402_payment_observations;
DROP TABLE agent_economy.x402_history_ranges;
DROP TABLE agent_economy.x402_history_plans;
DROP TABLE agent_economy.x402_history_sources;

ALTER TABLE agent_economy.worker_jobs DROP CONSTRAINT worker_jobs_collection_admission;
ALTER TABLE agent_economy.worker_jobs ADD CONSTRAINT worker_jobs_collection_admission CHECK (
    (mode = 'collect'
        AND collection_chain_scope IS NOT NULL
        AND collection_source_id IS NOT NULL
        AND collection_start_height >= 0
        AND collection_end_height >= collection_start_height
        AND collection_end_height - collection_start_height + 1 <= 10000
        AND (
            (collection_acquisition_contract = 'alchemy-rpc-block-v1'
                AND collection_chain_scope IN ('ethereum', 'base', 'solana', 'tempo')
                AND collection_source_id = 'alchemy-' || collection_chain_scope)
            OR (collection_acquisition_contract = 'cdp-x402-bazaar-v1'
                AND collection_chain_scope = 'base'
                AND collection_source_id = 'cdp-x402-bazaar-base'
                AND collection_end_height - collection_start_height BETWEEN 0 AND 399)
        )
        AND collection_evidence_contract = 'evidence-store-create-read-sha256-v1')
    OR (mode <> 'collect'
        AND collection_chain_scope IS NULL
        AND collection_source_id IS NULL
        AND collection_start_height IS NULL
        AND collection_end_height IS NULL
        AND collection_acquisition_contract IS NULL
        AND collection_evidence_contract IS NULL)
);
ALTER TABLE agent_economy.pending_collection_batches
    DROP CONSTRAINT pending_collection_registry_binding,
    DROP COLUMN registry_version,
    DROP COLUMN registry_sha256;
ALTER TABLE agent_economy.worker_jobs
    DROP COLUMN collection_registry_version,
    DROP COLUMN collection_registry_sha256;

COMMIT;
