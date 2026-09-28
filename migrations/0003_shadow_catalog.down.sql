BEGIN;

DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.catalog_candidates LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.catalog_candidate_aliases LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.catalog_verification_signals LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.catalog_liveness_checks LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.catalog_promotions LIMIT 1)
    THEN
        RAISE EXCEPTION 'cannot roll back non-empty shadow catalog';
    END IF;
END
$block$;

DROP VIEW agent_economy.verified_catalog;
DROP VIEW agent_economy.shadow_catalog;
DROP FUNCTION agent_economy.catalog_candidate_health(uuid, text, timestamptz);
DROP TRIGGER services_verified_require_promotion ON agent_economy.services;
DROP TABLE agent_economy.catalog_promotions;
DROP TABLE agent_economy.catalog_liveness_checks;
DROP TABLE agent_economy.catalog_verification_signals;
DROP TABLE agent_economy.catalog_candidate_aliases;
DROP TABLE agent_economy.catalog_candidates;
DROP FUNCTION agent_economy.guard_catalog_promotion();
DROP FUNCTION agent_economy.guard_catalog_alias_source();
DROP FUNCTION agent_economy.guard_direct_verified_service();
ALTER TABLE agent_economy.provenance_records
    DROP CONSTRAINT provenance_records_observed_at_key;

COMMIT;
