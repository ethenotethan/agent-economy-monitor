BEGIN;

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
                AND collection_end_height - collection_start_height BETWEEN 0 AND 399
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

DO $$
DECLARE
    definition text;
BEGIN
    SELECT pg_get_functiondef(
        'agent_economy.commit_collection_batch(uuid,uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,jsonb)'::regprocedure
    ) INTO STRICT definition;
    IF position('jsonb_array_length(p_observations_json) > 50000' IN definition) = 0
       OR position(') > 16777216' IN definition) = 0
       OR position('AND job.collection_end_height = p_end_height' IN definition) = 0
    THEN
        RAISE EXCEPTION 'expected predecessor collection limits are absent';
    END IF;
    definition := replace(
        definition,
        'jsonb_array_length(p_observations_json) > 50000',
        'jsonb_array_length(p_observations_json) > 200000'
    );
    definition := replace(definition, ') > 16777216', ') > 268435456');
    definition := replace(
        definition,
        'AND job.collection_end_height = p_end_height',
        'AND (job.collection_end_height = p_end_height OR (' ||
        'job.collection_acquisition_contract = ''cdp-x402-bazaar-v1'' AND ' ||
        'job.collection_end_height = p_start_height))'
    );
    definition := replace(
        definition,
        'OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 6',
        'OR (SELECT count(*) FROM jsonb_object_keys(item)) <> CASE WHEN p_source_id = ''cdp-x402-bazaar-base'' THEN 7 ELSE 6 END'
    );
    definition := replace(
        definition,
        'OR (item ->> ''parser_version'') !~ ''^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$''',
        'OR (item ->> ''parser_version'') !~ ''^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$''
            OR (p_source_id = ''cdp-x402-bazaar-base'' AND (
                NOT item ? ''catalog_resource_id''
                OR (item ->> ''catalog_resource_id'') !~ ''^sha256:[0-9a-f]{64}$''
            ))
            OR (p_source_id <> ''cdp-x402-bazaar-base'' AND item ? ''catalog_resource_id'')'
    );
    EXECUTE definition;
END
$$;

CREATE TABLE agent_economy.catalog_crawl_staging (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    job_id uuid NOT NULL,
    source_reported_total bigint NOT NULL CHECK (source_reported_total BETWEEN 0 AND 40000),
    resources_observed bigint NOT NULL CHECK (resources_observed BETWEEN 0 AND 40000),
    pages_archived integer NOT NULL CHECK (pages_archived BETWEEN 1 AND 400),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, job_id),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.worker_jobs (namespace_id, job_id),
    CHECK (resources_observed = source_reported_total)
);

REVOKE ALL ON agent_economy.catalog_crawl_staging FROM PUBLIC;

CREATE FUNCTION agent_economy.stage_catalog_crawl_batch(
    p_job_id uuid,
    p_lease_owner text,
    p_lease_token uuid,
    p_input_sha256 text,
    p_batch_sha256 text,
    p_chain_scope text,
    p_source_id text,
    p_observed_at_unix_ms bigint,
    p_start_height bigint,
    p_end_height bigint,
    p_evidence_json jsonb,
    p_source_reported_total bigint,
    p_resources_observed bigint
)
RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
DECLARE
    bound_namespace uuid;
    staged_id uuid;
    pending agent_economy.pending_collection_batches%ROWTYPE;
    existing agent_economy.catalog_crawl_staging%ROWTYPE;
BEGIN
    bound_namespace := agent_economy.bound_collection_namespace('collect');
    IF p_chain_scope <> 'base'
       OR p_source_id <> 'cdp-x402-bazaar-base'
       OR p_start_height < 0
       OR p_end_height - p_start_height NOT BETWEEN 0 AND 399
       OR p_source_reported_total NOT BETWEEN 0 AND 40000
       OR p_resources_observed <> p_source_reported_total
       OR jsonb_array_length(p_evidence_json) <> p_end_height - p_start_height + 1
    THEN
        RAISE EXCEPTION 'invalid catalog crawl batch';
    END IF;

    PERFORM 1 FROM agent_economy.worker_jobs AS job
    WHERE job.namespace_id = bound_namespace
      AND job.job_id = p_job_id
      AND job.mode = 'collect'
      AND job.job_kind = 'chain-protocol-range'
      AND job.status = 'leased'
      AND job.lease_owner = p_lease_owner
      AND job.lease_token = p_lease_token
      AND job.lease_expires_at > clock_timestamp()
      AND job.input_sha256 = p_input_sha256
      AND job.collection_chain_scope = 'base'
      AND job.collection_source_id = 'cdp-x402-bazaar-base'
      AND job.collection_start_height = p_start_height
      AND job.collection_end_height = p_start_height
      AND job.collection_acquisition_contract = 'cdp-x402-bazaar-v1'
      AND job.collection_evidence_contract = 'evidence-store-create-read-sha256-v1'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'catalog crawl admission is not live';
    END IF;

    IF p_batch_sha256 !~ '^[0-9a-f]{64}$'
       OR jsonb_typeof(p_evidence_json) <> 'array'
       OR EXISTS (
         SELECT 1 FROM jsonb_array_elements(p_evidence_json) evidence(item)
         WHERE jsonb_typeof(item) <> 'object'
            OR (SELECT count(*) FROM jsonb_object_keys(item)) <> 7
            OR NOT (item ?& ARRAY['evidence_id','sha256','storage_uri','storage_generation','media_type','byte_length','height'])
            OR (item->>'evidence_id') <> 'evidence:sha256:' || (item->>'sha256')
            OR (item->>'sha256') !~ '^[0-9a-f]{64}$'
            OR (item->>'storage_uri') !~ ('^evidence/' || p_source_id || '/[0-9]{4}-[0-9]{2}-[0-9]{2}/sha256/' || left((item->>'sha256'),2) || '/' || (item->>'sha256') || '$')
            OR (item->>'media_type') <> 'application/json'
            OR (item->>'height')::bigint NOT BETWEEN p_start_height AND p_end_height
            OR (item->>'byte_length')::bigint NOT BETWEEN 1 AND 4194304
       ) OR (SELECT count(DISTINCT (item->>'height')::bigint)
             FROM jsonb_array_elements(p_evidence_json) evidence(item))
             <> p_end_height - p_start_height + 1
    THEN
        RAISE EXCEPTION 'invalid pending catalog crawl batch';
    END IF;

    SELECT * INTO pending
    FROM agent_economy.pending_collection_batches AS batch
    WHERE batch.namespace_id = bound_namespace AND batch.job_id = p_job_id
    FOR UPDATE;
    IF FOUND THEN
        IF pending.input_sha256 <> p_input_sha256
           OR pending.batch_sha256 <> p_batch_sha256
           OR pending.chain_scope <> p_chain_scope
           OR pending.source_id <> p_source_id
           OR pending.observed_at_unix_ms <> p_observed_at_unix_ms
           OR pending.start_height <> p_start_height
           OR pending.end_height <> p_end_height
           OR pending.evidence_json <> p_evidence_json
        THEN
            RAISE EXCEPTION 'changed collection staging replay';
        END IF;
        staged_id := pending.pending_id;
    ELSE
        INSERT INTO agent_economy.pending_collection_batches
          (namespace_id, job_id, input_sha256, batch_sha256, chain_scope, source_id,
           observed_at_unix_ms, start_height, end_height, evidence_json)
        VALUES (bound_namespace, p_job_id, p_input_sha256, p_batch_sha256, p_chain_scope,
                p_source_id, p_observed_at_unix_ms, p_start_height, p_end_height,
                p_evidence_json)
        RETURNING pending_id INTO staged_id;
    END IF;

    SELECT * INTO existing
    FROM agent_economy.catalog_crawl_staging AS crawl
    WHERE crawl.namespace_id = bound_namespace AND crawl.job_id = p_job_id
    FOR UPDATE;
    IF FOUND THEN
        IF existing.source_reported_total <> p_source_reported_total
           OR existing.resources_observed <> p_resources_observed
           OR existing.pages_archived <> p_end_height - p_start_height + 1
        THEN
            RAISE EXCEPTION 'changed catalog crawl staging replay';
        END IF;
    ELSE
        INSERT INTO agent_economy.catalog_crawl_staging
            (namespace_id, job_id, source_reported_total, resources_observed, pages_archived)
        VALUES
            (bound_namespace, p_job_id, p_source_reported_total,
             p_resources_observed, (p_end_height - p_start_height + 1)::integer);
    END IF;
    RETURN staged_id;
END
$$;

REVOKE ALL ON FUNCTION agent_economy.stage_catalog_crawl_batch(
    uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,bigint,bigint
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION agent_economy.stage_catalog_crawl_batch(
    uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,bigint,bigint
) TO agent_economy_collector_runtime;

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
       OR position(quote_literal(pagination_routine) IN definition) <> 0
    THEN
        RAISE EXCEPTION 'expected predecessor collector routine allowlist is absent';
    END IF;
    definition := replace(
        definition,
        quote_literal(predecessor_routine),
        quote_literal(pagination_routine) || E',\n          ' || quote_literal(predecessor_routine)
    );
    EXECUTE definition;
END
$migration$;

CREATE VIEW agent_economy.catalog_crawl_coverage AS
SELECT
    job.namespace_id,
    job.collection_source_id AS source_id,
    job.job_id,
    COALESCE(crawl.source_reported_total, 0) AS source_reported_total,
    COALESCE(crawl.resources_observed, 0) AS resources_observed,
    COALESCE(crawl.pages_archived, 0) AS pages_archived,
    CASE WHEN pending.status = 'succeeded'
         THEN qualified_resource.verified_qualifying_resources ELSE 0 END
         AS verified_qualifying_resources,
    CASE
        WHEN job.status = 'dead_letter' OR pending.status = 'dead_letter' THEN 'failed'
        WHEN pending.status IS DISTINCT FROM 'succeeded' THEN 'partial'
        WHEN pending.updated_at < clock_timestamp() - interval '30 minutes' THEN 'stale'
        ELSE 'complete'
    END AS coverage_status,
    COALESCE(pending.updated_at, job.updated_at) AS observed_at,
    latest.last_complete_crawl,
    CASE WHEN latest.last_complete_crawl IS NULL THEN NULL
         ELSE GREATEST(
             0,
             EXTRACT(EPOCH FROM clock_timestamp() - latest.last_complete_crawl)::bigint
         )
    END AS crawl_lag_seconds,
    CASE WHEN pending.status = 'succeeded'
         THEN qualified.provenance_ids ELSE ARRAY[]::text[] END AS provenance_ids
FROM agent_economy.worker_jobs AS job
LEFT JOIN agent_economy.catalog_crawl_staging AS crawl
  ON crawl.namespace_id = job.namespace_id AND crawl.job_id = job.job_id
LEFT JOIN agent_economy.pending_collection_batches AS pending
  ON pending.namespace_id = job.namespace_id AND pending.job_id = job.job_id
LEFT JOIN agent_economy.collection_range_receipts AS receipt
  ON receipt.namespace_id = job.namespace_id
 AND receipt.chain_scope = job.collection_chain_scope
 AND receipt.source_id = job.collection_source_id
 AND receipt.start_height = pending.start_height
 AND receipt.end_height = pending.end_height
LEFT JOIN LATERAL (
    SELECT count(DISTINCT item ->> 'catalog_resource_id')::bigint
        AS verified_qualifying_resources
    FROM jsonb_array_elements(COALESCE(receipt.observations_json, '[]'::jsonb)) observation(item)
) AS qualified_resource ON true
LEFT JOIN LATERAL (
    SELECT
        COALESCE(
            array_agg(DISTINCT observation.provenance_id::text
                ORDER BY observation.provenance_id::text),
            ARRAY[]::text[]
        ) AS provenance_ids
    FROM agent_economy.observations AS observation
    WHERE observation.namespace_id = job.namespace_id
      AND observation.source_id = job.collection_source_id
      AND EXISTS (
          SELECT 1
          FROM jsonb_array_elements(COALESCE(pending.evidence_json, '[]'::jsonb)) evidence(item)
          WHERE item ->> 'evidence_id' = observation.evidence_id
      )
) AS qualified ON true
LEFT JOIN LATERAL (
    SELECT max(done.updated_at) AS last_complete_crawl
    FROM agent_economy.pending_collection_batches AS done
    JOIN agent_economy.worker_jobs AS done_job
      ON done_job.namespace_id = done.namespace_id AND done_job.job_id = done.job_id
    WHERE done.namespace_id = job.namespace_id
      AND done.status = 'succeeded'
      AND done_job.collection_source_id = job.collection_source_id
      AND done_job.collection_acquisition_contract = 'cdp-x402-bazaar-v1'
) AS latest ON true
WHERE job.mode = 'collect'
  AND job.collection_acquisition_contract = 'cdp-x402-bazaar-v1'
  AND job.collection_source_id = 'cdp-x402-bazaar-base';

REVOKE ALL ON agent_economy.catalog_crawl_coverage FROM PUBLIC;
GRANT SELECT ON agent_economy.catalog_crawl_coverage TO agent_economy_dashboard_reader;

COMMIT;
