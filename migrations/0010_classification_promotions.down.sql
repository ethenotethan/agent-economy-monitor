BEGIN;

LOCK TABLE agent_economy.classification_run_promotions IN ACCESS EXCLUSIVE MODE;

DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.classification_run_promotions) THEN
        RAISE EXCEPTION 'cannot roll back classification promotion history';
    END IF;
END
$block$;

DROP VIEW agent_economy.current_buyer_classification_runs;
DROP FUNCTION agent_economy.promote_buyer_classification_run(
    uuid, text, text, integer, text, uuid
);
DROP TABLE agent_economy.classification_run_promotions;
DROP FUNCTION agent_economy.validate_classification_run_promotion();

COMMIT;
