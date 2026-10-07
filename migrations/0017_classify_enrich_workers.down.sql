BEGIN;

LOCK TABLE agent_economy.classifier_job_inputs,
           agent_economy.classifier_job_receipts,
           agent_economy.enrichment_job_inputs,
           agent_economy.pending_enrichment_batches,
           agent_economy.enrichment_job_receipts
    IN ACCESS EXCLUSIVE MODE;
DO $block$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.classifier_job_inputs LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.classifier_job_receipts LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.enrichment_job_inputs LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.pending_enrichment_batches LIMIT 1)
       OR EXISTS (SELECT 1 FROM agent_economy.enrichment_job_receipts LIMIT 1)
    THEN
        RAISE EXCEPTION 'cannot roll back classification or enrichment worker history';
    END IF;
END
$block$;

DROP FUNCTION agent_economy.fail_enrichment_job(uuid, text, uuid, text, boolean, bigint);
DROP FUNCTION agent_economy.complete_enrichment_job(uuid, text, uuid, text);
DROP FUNCTION agent_economy.renew_enrichment_job_lease(uuid, text, uuid, bigint);
DROP FUNCTION agent_economy.commit_enrichment_batch(
    uuid, text, uuid, text, text, uuid, text, text, text, date, bigint, text,
    text, bigint, boolean, jsonb, bytea[], jsonb, jsonb
);
DROP FUNCTION agent_economy.claim_pending_enrichment_batch(text, bigint);
DROP FUNCTION agent_economy.stage_enrichment_batch(
    uuid, text, uuid, text, text, uuid, text, text, text, date, bigint, text,
    text, bigint, boolean, jsonb, jsonb, jsonb
);
DROP FUNCTION agent_economy.load_enrichment_job_input(uuid, text, uuid);
DROP FUNCTION agent_economy.claim_enrichment_job(text, bigint);
DROP FUNCTION agent_economy.bound_enricher_namespace();
DROP TRIGGER enrichment_job_receipts_immutable ON agent_economy.enrichment_job_receipts;
DROP TRIGGER enrichment_job_inputs_truncate_immutable ON agent_economy.enrichment_job_inputs;
DROP TRIGGER enrichment_job_inputs_immutable ON agent_economy.enrichment_job_inputs;
DROP TRIGGER enrichment_job_inputs_validate ON agent_economy.enrichment_job_inputs;
DROP FUNCTION agent_economy.validate_enrichment_job_input();
DROP TABLE agent_economy.enricher_runtime_namespaces;
DROP TABLE agent_economy.classification_admission_requests;
DROP TABLE agent_economy.enrichment_reduction_receipts;
DROP TABLE agent_economy.enrichment_job_receipts;
DROP TABLE agent_economy.pending_enrichment_batches;
DROP TABLE agent_economy.enrichment_job_inputs;

DO $restore_collection_boundary$
DECLARE
    definition text;
    next_definition text;
    claim_line text := '          ''agent_economy.claim_pending_enrichment_batch(text,bigint)'',' || chr(10);
    commit_line text := '          ''agent_economy.commit_enrichment_batch(uuid,text,uuid,text,text,uuid,text,text,text,date,bigint,text,text,bigint,boolean,jsonb,bytea[],jsonb,jsonb)'',' || chr(10);
BEGIN
    SELECT pg_get_functiondef('agent_economy.bound_collection_namespace(text)'::regprocedure)
    INTO definition;
    next_definition := replace(definition, claim_line, '');
    IF next_definition = definition THEN
        RAISE EXCEPTION 'cannot restore collection verifier claim inventory';
    END IF;
    definition := next_definition;
    next_definition := replace(definition, commit_line, '');
    IF next_definition = definition THEN
        RAISE EXCEPTION 'cannot restore collection verifier commit inventory';
    END IF;
    EXECUTE next_definition;
END
$restore_collection_boundary$;

DROP FUNCTION agent_economy.fail_classifier_job(uuid, text, uuid, text, boolean, bigint);
DROP FUNCTION agent_economy.complete_classifier_job(uuid, text, uuid, text);
DROP FUNCTION agent_economy.renew_classifier_job_lease(uuid, text, uuid, bigint);
DROP FUNCTION agent_economy.commit_classification_batch(
    uuid, text, uuid, text, text, text, integer, text, text, uuid, bigint,
    bigint, text, text, jsonb, jsonb, jsonb, text[], bytea, text
);
DROP FUNCTION agent_economy.load_classifier_job_input(uuid, text, uuid);
DROP FUNCTION agent_economy.claim_classifier_job(text, bigint);
DROP FUNCTION agent_economy.bound_classifier_namespace();
DROP TRIGGER classifier_job_receipts_truncate_immutable ON agent_economy.classifier_job_receipts;
DROP TRIGGER classifier_job_receipts_immutable ON agent_economy.classifier_job_receipts;
DROP TRIGGER classifier_job_inputs_truncate_immutable ON agent_economy.classifier_job_inputs;
DROP TRIGGER classifier_job_inputs_immutable ON agent_economy.classifier_job_inputs;
DROP TRIGGER classifier_job_inputs_validate ON agent_economy.classifier_job_inputs;
DROP FUNCTION agent_economy.validate_classifier_job_input();
DROP TABLE agent_economy.classifier_runtime_namespaces;
DROP TABLE agent_economy.classifier_job_receipts;
DROP TABLE agent_economy.classifier_job_inputs;

DO $migration$
BEGIN
    EXECUTE format(
        'REVOKE CONNECT ON DATABASE %I FROM agent_economy_classifier_runtime, agent_economy_enricher_runtime',
        current_database()
    );
END
$migration$;
REVOKE USAGE ON SCHEMA agent_economy FROM agent_economy_classifier_runtime;
REVOKE USAGE ON SCHEMA agent_economy FROM agent_economy_enricher_runtime;
DO $roles$
BEGIN
    BEGIN
        DROP ROLE agent_economy_classifier_runtime;
    EXCEPTION WHEN dependent_objects_still_exist THEN
        NULL;
    END;
    BEGIN
        DROP ROLE agent_economy_enricher_runtime;
    EXCEPTION WHEN dependent_objects_still_exist THEN
        NULL;
    END;
END
$roles$;

COMMIT;
