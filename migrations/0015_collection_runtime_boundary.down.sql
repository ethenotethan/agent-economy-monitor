BEGIN;

CREATE OR REPLACE FUNCTION agent_economy.bound_collection_namespace(p_purpose text)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
STABLE
SET search_path = pg_catalog
AS $$
DECLARE
    expected_login name;
    role_row pg_roles%ROWTYPE;
    result uuid;
BEGIN
    expected_login := CASE p_purpose
        WHEN 'collect' THEN 'agent_economy_collector_runtime'::name
        WHEN 'verify-evidence' THEN 'agent_economy_evidence_verifier_runtime'::name
        ELSE NULL
    END;
    IF expected_login IS NULL OR session_user::name <> expected_login THEN
        RAISE EXCEPTION 'runtime purpose/login mismatch';
    END IF;
    SELECT * INTO STRICT role_row FROM pg_roles WHERE rolname = expected_login;
    IF NOT role_row.rolcanlogin OR role_row.rolinherit OR role_row.rolsuper
       OR role_row.rolcreatedb OR role_row.rolcreaterole OR role_row.rolreplication
       OR role_row.rolbypassrls THEN
        RAISE EXCEPTION 'unsafe collection runtime login';
    END IF;
    IF EXISTS (
        WITH RECURSIVE inbound(roleid, member) AS (
            SELECT m.roleid, m.member FROM pg_auth_members m
            WHERE m.roleid = role_row.oid OR m.member = role_row.oid
            UNION
            SELECT m.roleid, m.member FROM pg_auth_members m
            JOIN inbound i ON m.roleid = i.member OR m.member = i.roleid
        ) SELECT 1 FROM inbound
    ) THEN
        RAISE EXCEPTION 'collection runtime login must be membership-isolated';
    END IF;
    SELECT mapping.namespace_id INTO STRICT result
    FROM agent_economy.collection_runtime_namespaces mapping
    WHERE mapping.login_name = expected_login AND mapping.purpose = p_purpose;
    RETURN result;
END
$$;

REVOKE EXECUTE ON FUNCTION agent_economy.bound_collection_namespace(text)
FROM agent_economy_collector_runtime, agent_economy_evidence_verifier_runtime;

DO $migration$
BEGIN
    EXECUTE format(
        'REVOKE CONNECT ON DATABASE %I FROM agent_economy_collector_runtime, agent_economy_evidence_verifier_runtime',
        current_database()
    );
    EXECUTE format('GRANT CONNECT, TEMPORARY ON DATABASE %I TO PUBLIC', current_database());
END
$migration$;

GRANT EXECUTE ON FUNCTION agent_economy.promote_buyer_classification_run(uuid,text,text,integer,text,uuid) TO PUBLIC;
GRANT EXECUTE ON FUNCTION agent_economy.protect_projection_job_identity() TO PUBLIC;
GRANT EXECUTE ON FUNCTION agent_economy.validate_projection_approval() TO PUBLIC;
GRANT EXECUTE ON FUNCTION agent_economy.validate_projection_publication() TO PUBLIC;

COMMIT;
