BEGIN;

DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.collection_jobs LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.observations LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.canonical_events LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.canonical_event_observations LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.event_finality_updates LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.settlements LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.feature_windows LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.analytics_daily_metrics LIMIT 1)
    THEN
        RAISE EXCEPTION 'cannot roll back non-empty operational and analytics schema';
    END IF;
END
$block$;

DROP MATERIALIZED VIEW agent_economy.pulse_hourly;
DROP VIEW agent_economy.bigquery_projection_readiness;
DROP VIEW agent_economy.current_event_finality;
DROP TABLE agent_economy.analytics_daily_metrics;
DROP TABLE agent_economy.feature_windows;
DROP TABLE agent_economy.settlements;
DROP TABLE agent_economy.event_finality_updates;
DROP TABLE agent_economy.canonical_event_observations;
DROP TABLE agent_economy.canonical_events;
DROP TABLE agent_economy.observations;
DROP TABLE agent_economy.collection_jobs;
ALTER TABLE agent_economy.buyer_handles DROP CONSTRAINT buyer_handles_chain_key;
ALTER TABLE agent_economy.provenance_records
    DROP CONSTRAINT provenance_records_evidence_source_chain_key;
ALTER TABLE agent_economy.provenance_records DROP CONSTRAINT provenance_records_source_chain_key;

COMMIT;
