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

DROP VIEW agent_economy.x402_history_coverage;
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
DROP TYPE agent_economy.x402_history_support_role;

COMMIT;
