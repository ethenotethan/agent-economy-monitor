BEGIN;

LOCK TABLE agent_economy.buyer_finalized_history_evidence,
    agent_economy.buyer_finalized_history,
    agent_economy.buyer_enrichment_cursors
IN ACCESS EXCLUSIVE MODE;

DO $rollback$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.buyer_finalized_history_evidence LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.buyer_finalized_history LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.buyer_enrichment_cursors LIMIT 1)
    THEN
        RAISE EXCEPTION 'cannot roll back non-empty buyer enrichment state';
    END IF;
END
$rollback$;

DROP TABLE agent_economy.buyer_finalized_history_evidence;
DROP TABLE agent_economy.buyer_finalized_history;
DROP TABLE agent_economy.buyer_enrichment_cursors;
ALTER TABLE agent_economy.buyer_handles
    DROP CONSTRAINT buyer_handles_enrichment_chain_key;

COMMIT;
