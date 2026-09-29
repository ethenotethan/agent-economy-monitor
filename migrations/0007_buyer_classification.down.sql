BEGIN;

LOCK TABLE
    agent_economy.classification_replay_drift,
    agent_economy.classification_run_seals,
    agent_economy.classification_run_claims,
    agent_economy.classification_run_evidence,
    agent_economy.classification_run_features,
    agent_economy.classification_run_label_definitions,
    agent_economy.classification_runs,
    agent_economy.classification_label_definitions
IN ACCESS EXCLUSIVE MODE;

DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.classification_replay_drift)
       OR EXISTS (SELECT 1 FROM agent_economy.classification_run_seals)
       OR EXISTS (SELECT 1 FROM agent_economy.classification_run_claims)
       OR EXISTS (SELECT 1 FROM agent_economy.classification_run_evidence)
       OR EXISTS (SELECT 1 FROM agent_economy.classification_run_features)
       OR EXISTS (SELECT 1 FROM agent_economy.classification_run_label_definitions)
       OR EXISTS (SELECT 1 FROM agent_economy.classification_runs)
       OR EXISTS (SELECT 1 FROM agent_economy.classification_label_definitions)
    THEN
        RAISE EXCEPTION 'cannot roll back buyer classification history';
    END IF;
END
$block$;

DROP TABLE agent_economy.classification_replay_drift;
DROP TABLE agent_economy.classification_run_seals;
DROP TABLE agent_economy.classification_run_claims;
DROP TABLE agent_economy.classification_run_evidence;
DROP TABLE agent_economy.classification_run_features;
DROP TABLE agent_economy.classification_run_label_definitions;
DROP TABLE agent_economy.classification_runs;
DROP TABLE agent_economy.classification_label_definitions;

DROP FUNCTION agent_economy.validate_classification_replay_drift();
DROP FUNCTION agent_economy.validate_classification_run_seal();
DROP FUNCTION agent_economy.guard_classification_child_append();
DROP FUNCTION agent_economy.lock_classification_series();

COMMIT;
