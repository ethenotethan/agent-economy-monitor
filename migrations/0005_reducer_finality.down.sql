BEGIN;

LOCK TABLE agent_economy.event_finality_assertions IN ACCESS EXCLUSIVE MODE;
LOCK TABLE agent_economy.event_finality_updates IN ACCESS EXCLUSIVE MODE;

DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.event_finality_assertions LIMIT 1)
       OR EXISTS (
           SELECT 1
           FROM agent_economy.event_finality_updates
           WHERE finality_status = 'reverted'
           LIMIT 1
       )
    THEN
        RAISE EXCEPTION 'cannot roll back reducer finality history';
    END IF;
END
$block$;

DROP VIEW agent_economy.event_finality_conflicts;
DROP TABLE agent_economy.event_finality_assertions;
DROP FUNCTION agent_economy.validate_finality_assertion_provenance();
ALTER TABLE agent_economy.event_finality_updates
    DROP CONSTRAINT event_finality_updates_finality_status_check;
ALTER TABLE agent_economy.event_finality_updates
    ADD CONSTRAINT event_finality_updates_finality_status_check
    CHECK (finality_status IN ('observed', 'confirmed', 'finalized', 'orphaned'));

COMMIT;
