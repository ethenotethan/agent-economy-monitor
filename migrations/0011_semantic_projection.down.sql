BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.projection_publications)
        OR EXISTS (SELECT 1 FROM agent_economy.projection_approvals)
        OR EXISTS (SELECT 1 FROM agent_economy.projection_jobs)
        OR EXISTS (SELECT 1 FROM agent_economy.projection_snapshots)
    THEN
        RAISE EXCEPTION 'projection rollback blocked: projection history exists';
    END IF;
END
$$;

DROP VIEW agent_economy.projection_mirror_pages;
DROP TABLE agent_economy.projection_publications;
DROP TABLE agent_economy.projection_approvals;
DROP TABLE agent_economy.projection_jobs;
DROP TABLE agent_economy.projection_snapshots;
DROP FUNCTION agent_economy.validate_projection_publication();
DROP FUNCTION agent_economy.validate_projection_approval();
DROP FUNCTION agent_economy.protect_projection_job_identity();

COMMIT;