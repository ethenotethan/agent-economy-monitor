BEGIN;

-- Close ambient routine execution that predates collection custody hardening.
REVOKE ALL ON FUNCTION agent_economy.promote_buyer_classification_run(uuid,text,text,integer,text,uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.protect_projection_job_identity() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.validate_projection_approval() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.validate_projection_publication() FROM PUBLIC;

-- Admission occurs before SET ROLE, so the two real LOGINs retain only direct
-- CONNECT while ambient database admission and temporary-object authority close.
DO $migration$
BEGIN
    EXECUTE format('REVOKE CONNECT, TEMPORARY ON DATABASE %I FROM PUBLIC', current_database());
    EXECUTE format(
        'GRANT CONNECT ON DATABASE %I TO agent_economy_collector_runtime, agent_economy_evidence_verifier_runtime',
        current_database()
    );
END
$migration$;

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
    expected_routines text[];
    actual_routines text[];
    result uuid;
BEGIN
    expected_login := CASE p_purpose
        WHEN 'collect' THEN 'agent_economy_collector_runtime'::name
        WHEN 'verify-evidence' THEN 'agent_economy_evidence_verifier_runtime'::name
        ELSE NULL
    END;
    expected_routines := CASE p_purpose
        WHEN 'collect' THEN ARRAY[
          'agent_economy.bound_collection_namespace(text)',
          'agent_economy.claim_bound_collection_job(text,bigint)',
          'agent_economy.complete_bound_collection_job(uuid,text,uuid,text)',
          'agent_economy.fail_bound_collection_job(uuid,text,uuid,text,boolean,bigint)',
          'agent_economy.renew_bound_collection_job(uuid,text,uuid,bigint)',
          'agent_economy.stage_collection_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb)'
        ]::text[]
        WHEN 'verify-evidence' THEN ARRAY[
          'agent_economy.bound_collection_namespace(text)',
          'agent_economy.claim_pending_collection_batch(text,bigint)',
          'agent_economy.fail_pending_collection_batch(uuid,text,uuid,text,boolean)',
          'agent_economy.promote_pending_collection_batch(uuid,text,uuid,jsonb,text)'
        ]::text[]
        ELSE ARRAY[]::text[]
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
    IF EXISTS (
        SELECT 1 FROM pg_shdepend d
        WHERE d.refclassid = 'pg_authid'::regclass
          AND d.refobjid = role_row.oid
          AND d.deptype = 'o'
    )
    THEN RAISE EXCEPTION 'collection runtime login must not own database objects'; END IF;

    IF (
          SELECT count(*) FROM pg_database d
          CROSS JOIN LATERAL aclexplode(coalesce(d.datacl,acldefault('d',d.datdba))) acl
          WHERE acl.grantee=role_row.oid
            AND d.datname=current_database()
            AND acl.privilege_type='CONNECT'
       ) <> 1
       OR EXISTS (
          SELECT 1 FROM pg_database d
          CROSS JOIN LATERAL aclexplode(coalesce(d.datacl,acldefault('d',d.datdba))) acl
          WHERE acl.grantee=role_row.oid
            AND NOT (d.datname=current_database() AND acl.privilege_type='CONNECT')
       )
       OR NOT has_database_privilege(expected_login, current_database(), 'CONNECT')
       OR has_database_privilege(expected_login, current_database(), 'CREATE,TEMPORARY')
       OR NOT has_schema_privilege(expected_login, 'agent_economy', 'USAGE')
       OR has_schema_privilege(expected_login, 'agent_economy', 'CREATE')
       OR EXISTS (
          SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='agent_economy' AND c.relkind IN ('r','p','v','m','f')
            AND has_table_privilege(expected_login,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')
       )
       OR EXISTS (
          SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='agent_economy' AND c.relkind IN ('r','p','v','m','f')
            AND has_any_column_privilege(expected_login,c.oid,'SELECT,INSERT,UPDATE,REFERENCES')
       )
       OR EXISTS (
          SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          WHERE n.nspname='agent_economy'
            AND CASE WHEN c.relkind='S'
                THEN has_sequence_privilege(expected_login,c.oid,'USAGE,SELECT,UPDATE')
                ELSE false END
       )
       OR EXISTS (
          SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
          WHERE n.nspname='agent_economy' AND t.typrelid=0 AND t.typelem=0
            AND has_type_privilege(expected_login,t.oid,'USAGE')
       )
    THEN RAISE EXCEPTION 'unsafe collection runtime object authority'; END IF;

    SELECT coalesce(array_agg(p.oid::regprocedure::text ORDER BY p.oid::regprocedure::text),ARRAY[]::text[])
      INTO actual_routines
      FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
      CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
      WHERE n.nspname='agent_economy' AND acl.grantee=role_row.oid AND acl.privilege_type='EXECUTE';
    IF actual_routines <> expected_routines THEN
       RAISE EXCEPTION 'unsafe collection runtime routine authority';
    END IF;

    IF EXISTS (
          SELECT 1 FROM pg_database d
          CROSS JOIN LATERAL aclexplode(coalesce(d.datacl,acldefault('d',d.datdba))) acl
          WHERE d.datname=current_database() AND acl.grantee=0
            AND acl.privilege_type IN ('CONNECT','CREATE','TEMPORARY')
       ) OR EXISTS (
          SELECT 1 FROM pg_namespace n
          CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) acl
          WHERE n.nspname='agent_economy' AND acl.grantee=0 AND acl.privilege_type='CREATE'
       ) OR EXISTS (
          SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
          CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,c.relowner))) acl
          WHERE n.nspname='agent_economy' AND acl.grantee=0
       )
       OR EXISTS (
          SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
          CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) acl
          WHERE n.nspname='agent_economy' AND acl.grantee=0 AND acl.privilege_type='EXECUTE'
       )
       OR EXISTS (
          SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid=t.typnamespace
          CROSS JOIN LATERAL aclexplode(coalesce(t.typacl,acldefault('T',t.typowner))) acl
          WHERE n.nspname='agent_economy' AND t.typrelid=0 AND t.typelem=0
            AND acl.grantee=0 AND acl.privilege_type='USAGE'
       )
       OR EXISTS (
          SELECT 1 FROM pg_default_acl d CROSS JOIN LATERAL aclexplode(d.defaclacl) acl
          WHERE acl.grantee IN (0,role_row.oid)
       )
    THEN RAISE EXCEPTION 'dangerous ambient collection authority'; END IF;

    SELECT mapping.namespace_id INTO STRICT result
    FROM agent_economy.collection_runtime_namespaces mapping
    WHERE mapping.login_name = expected_login AND mapping.purpose = p_purpose;
    RETURN result;
END
$$;

REVOKE ALL ON FUNCTION agent_economy.bound_collection_namespace(text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION agent_economy.bound_collection_namespace(text)
TO agent_economy_collector_runtime, agent_economy_evidence_verifier_runtime;

COMMIT;
