BEGIN;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM agent_economy.catalog_crawl_staging) THEN
        RAISE EXCEPTION 'refusing to discard catalog crawl history';
    END IF;
END
$$;

DO $$
DECLARE
    definition text;
BEGIN
    SELECT pg_get_functiondef(
        'agent_economy.commit_collection_batch(uuid,uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,jsonb)'::regprocedure
    ) INTO STRICT definition;
    IF position('jsonb_array_length(p_observations_json) > 200000' IN definition) = 0
       OR position(') > 268435456' IN definition) = 0
       OR position(
           'AND (job.collection_end_height = p_end_height OR (job.collection_acquisition_contract = ''cdp-x402-bazaar-v1'' AND job.collection_end_height = p_start_height))'
           IN definition
       ) = 0
    THEN
        RAISE EXCEPTION 'expected pagination collection limits are absent';
    END IF;
    definition := replace(
        definition,
        'jsonb_array_length(p_observations_json) > 200000',
        'jsonb_array_length(p_observations_json) > 50000'
    );
    definition := replace(definition, ') > 268435456', ') > 16777216');
    definition := replace(
        definition,
        'AND (job.collection_end_height = p_end_height OR (job.collection_acquisition_contract = ''cdp-x402-bazaar-v1'' AND job.collection_end_height = p_start_height))',
        'AND job.collection_end_height = p_end_height'
    );
    definition := replace(
        definition,
        'OR (SELECT count(*) FROM jsonb_object_keys(item)) <> CASE WHEN p_source_id = ''cdp-x402-bazaar-base'' THEN 7 ELSE 6 END',
        'OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 6'
    );
    definition := replace(
        definition,
        'OR (item ->> ''parser_version'') !~ ''^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$''
            OR (p_source_id = ''cdp-x402-bazaar-base'' AND (
                NOT item ? ''catalog_resource_id''
                OR (item ->> ''catalog_resource_id'') !~ ''^sha256:[0-9a-f]{64}$''
            ))
            OR (p_source_id <> ''cdp-x402-bazaar-base'' AND item ? ''catalog_resource_id'')',
        'OR (item ->> ''parser_version'') !~ ''^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$'''
    );
    EXECUTE definition;
END
$$;

REVOKE ALL ON FUNCTION agent_economy.stage_catalog_crawl_batch(
    uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,bigint,bigint
) FROM agent_economy_collector_runtime;

DO $migration$
DECLARE
    definition text;
    predecessor_routine text := 'agent_economy.stage_collection_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb)';
    pagination_routine text := 'agent_economy.stage_catalog_crawl_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,bigint,bigint)';
BEGIN
    SELECT pg_get_functiondef(
        'agent_economy.bound_collection_namespace(text)'::regprocedure
    ) INTO STRICT definition;
    IF position(quote_literal(predecessor_routine) IN definition) = 0
       OR position(quote_literal(pagination_routine) IN definition) = 0
    THEN
        RAISE EXCEPTION 'expected pagination collector routine allowlist is absent';
    END IF;
    definition := replace(
        definition,
        quote_literal(pagination_routine) || E',\n          ' || quote_literal(predecessor_routine),
        quote_literal(predecessor_routine)
    );
    EXECUTE definition;
END
$migration$;

DROP VIEW agent_economy.catalog_crawl_coverage;
DROP FUNCTION agent_economy.stage_catalog_crawl_batch(
    uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,bigint,bigint
);
DROP TABLE agent_economy.catalog_crawl_staging;

ALTER TABLE agent_economy.worker_jobs DROP CONSTRAINT worker_jobs_collection_admission;
ALTER TABLE agent_economy.worker_jobs ADD CONSTRAINT worker_jobs_collection_admission CHECK (
    (mode = 'collect'
        AND collection_chain_scope IS NOT NULL
        AND collection_source_id IS NOT NULL
        AND collection_start_height >= 0
        AND collection_end_height >= collection_start_height
        AND collection_end_height - collection_start_height + 1 <= 10000
        AND (
            (
                collection_acquisition_contract = 'alchemy-rpc-block-v1'
                AND collection_chain_scope IN ('ethereum', 'base', 'solana', 'tempo')
                AND collection_source_id = 'alchemy-' || collection_chain_scope
            )
            OR (
                collection_acquisition_contract = 'cdp-x402-bazaar-v1'
                AND collection_chain_scope = 'base'
                AND collection_source_id = 'cdp-x402-bazaar-base'
                AND collection_start_height = 0
                AND collection_end_height = 0
            )
        )
        AND collection_evidence_contract = 'evidence-store-create-read-sha256-v1')
    OR (mode <> 'collect'
        AND collection_chain_scope IS NULL
        AND collection_source_id IS NULL
        AND collection_start_height IS NULL
        AND collection_end_height IS NULL
        AND collection_acquisition_contract IS NULL
        AND collection_evidence_contract IS NULL)
);

COMMIT;
