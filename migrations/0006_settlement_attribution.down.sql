BEGIN;

LOCK TABLE
    agent_economy.attribution_run_seals,
    agent_economy.attribution_candidate_evidence,
    agent_economy.attribution_candidates,
    agent_economy.attribution_run_requirements,
    agent_economy.attribution_run_evidence,
    agent_economy.attribution_runs,
    agent_economy.payment_requirements
IN ACCESS EXCLUSIVE MODE;

DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.attribution_run_seals)
       OR EXISTS (SELECT 1 FROM agent_economy.attribution_candidate_evidence)
       OR EXISTS (SELECT 1 FROM agent_economy.attribution_candidates)
       OR EXISTS (SELECT 1 FROM agent_economy.attribution_run_requirements)
       OR EXISTS (SELECT 1 FROM agent_economy.attribution_run_evidence)
       OR EXISTS (SELECT 1 FROM agent_economy.attribution_runs)
       OR EXISTS (SELECT 1 FROM agent_economy.payment_requirements)
    THEN
        RAISE EXCEPTION 'cannot roll back settlement attribution history';
    END IF;
END
$block$;

DROP TABLE agent_economy.attribution_run_seals;
DROP TABLE agent_economy.attribution_candidate_evidence;
DROP TABLE agent_economy.attribution_candidates;
DROP TABLE agent_economy.attribution_run_requirements;
DROP TABLE agent_economy.attribution_run_evidence;
DROP TABLE agent_economy.attribution_runs;
DROP TABLE agent_economy.payment_requirements;

DROP FUNCTION agent_economy.guard_attribution_child_append();
DROP FUNCTION agent_economy.validate_attribution_run_seal();
DROP FUNCTION agent_economy.lock_attribution_series();
DROP FUNCTION agent_economy.validate_attribution_settlement_evidence();
DROP FUNCTION agent_economy.validate_payment_requirement_hierarchy();

COMMIT;
