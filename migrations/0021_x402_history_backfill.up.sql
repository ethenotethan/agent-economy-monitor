BEGIN;

ALTER TABLE agent_economy.worker_jobs
    ADD COLUMN collection_registry_version text,
    ADD COLUMN collection_registry_sha256 text;
ALTER TABLE agent_economy.pending_collection_batches
    ADD COLUMN registry_version text,
    ADD COLUMN registry_sha256 text,
    ADD CONSTRAINT pending_collection_registry_binding CHECK (
        (registry_version IS NULL AND registry_sha256 IS NULL)
        OR (registry_version IS NOT NULL AND registry_version <> ''
            AND registry_sha256 ~ '^[0-9a-f]{64}$')
    );

ALTER TABLE agent_economy.worker_jobs DROP CONSTRAINT worker_jobs_collection_admission;
ALTER TABLE agent_economy.worker_jobs ADD CONSTRAINT worker_jobs_collection_admission CHECK (
    (mode = 'collect'
        AND collection_chain_scope IS NOT NULL
        AND collection_source_id IS NOT NULL
        AND collection_start_height >= 0
        AND collection_end_height >= collection_start_height
        AND collection_end_height - collection_start_height + 1 <= 10000
        AND (
            (collection_acquisition_contract = 'alchemy-rpc-block-v1'
                AND collection_chain_scope IN ('ethereum', 'base', 'solana', 'tempo')
                AND collection_source_id = 'alchemy-' || collection_chain_scope)
            OR (collection_acquisition_contract = 'cdp-x402-bazaar-v1'
                AND collection_chain_scope = 'base'
                AND collection_source_id = 'cdp-x402-bazaar-base'
                AND collection_end_height - collection_start_height BETWEEN 0 AND 399)
            OR (collection_acquisition_contract = 'facilitator-rpc-block-v1'
                AND collection_chain_scope IN ('base', 'solana')
                AND collection_source_id = 'x402-facilitator-' || collection_chain_scope
                AND collection_registry_version IS NOT NULL
                AND collection_registry_version <> ''
                AND collection_registry_sha256 ~ '^[0-9a-f]{64}$')
        )
        AND (collection_acquisition_contract = 'facilitator-rpc-block-v1'
             OR (collection_registry_version IS NULL AND collection_registry_sha256 IS NULL))
        AND collection_evidence_contract = 'evidence-store-create-read-sha256-v1')
    OR (mode <> 'collect'
        AND collection_chain_scope IS NULL
        AND collection_source_id IS NULL
        AND collection_start_height IS NULL
        AND collection_end_height IS NULL
        AND collection_acquisition_contract IS NULL
        AND collection_evidence_contract IS NULL
        AND collection_registry_version IS NULL
        AND collection_registry_sha256 IS NULL)
);

DO $$
DECLARE definition text;
BEGIN
    SELECT pg_get_functiondef(
        'agent_economy.stage_collection_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb)'::regprocedure
    ) INTO STRICT definition;
    definition := replace(
        definition,
        'job.collection_acquisition_contract IN (''alchemy-rpc-block-v1'', ''cdp-x402-bazaar-v1'')',
        'job.collection_acquisition_contract IN (''alchemy-rpc-block-v1'', ''cdp-x402-bazaar-v1'', ''facilitator-rpc-block-v1'')'
    );
    EXECUTE definition;

    SELECT pg_get_functiondef(
        'agent_economy.commit_collection_batch(uuid,uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,jsonb)'::regprocedure
    ) INTO STRICT definition;
    definition := replace(
        definition,
        'job.collection_acquisition_contract IN (''alchemy-rpc-block-v1'', ''cdp-x402-bazaar-v1'')',
        'job.collection_acquisition_contract IN (''alchemy-rpc-block-v1'', ''cdp-x402-bazaar-v1'', ''facilitator-rpc-block-v1'')'
    );
    definition := replace(
        definition,
        'CASE WHEN p_source_id = ''cdp-x402-bazaar-base'' THEN 7 ELSE 6 END',
        'CASE WHEN p_source_id = ''cdp-x402-bazaar-base'' THEN 7 WHEN p_source_id = ''x402-facilitator-'' || p_chain_scope THEN 23 ELSE 6 END'
    );
    definition := replace(
        definition,
        'OR (p_source_id <> ''cdp-x402-bazaar-base'' AND item ? ''catalog_resource_id'')',
        'OR (p_source_id <> ''cdp-x402-bazaar-base'' AND item ? ''catalog_resource_id'')
            OR (p_source_id = ''x402-facilitator-'' || p_chain_scope AND NOT item ?& ARRAY[
                ''transaction_reference'',''settlement_position'',''payer_handle_id'',
                ''payer_handle_value'',''recipient_handle'',''asset'',''amount_atomic'',
                ''settled_at_unix_seconds'',''finality'',''explicit_attribution_digest'',
                ''facilitator_id'',''facilitator_address'',''facilitator_active'',
                ''registry_version'',''registry_sha256'',''attribution_class'',''rpc_verification_evidence_id''
            ])'
    );
    EXECUTE definition;
END
$$;

CREATE TABLE agent_economy.x402_history_sources (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    source_id text NOT NULL CHECK (source_id ~ '^[a-z0-9][a-z0-9_.-]{1,62}[a-z0-9]$'),
    chain_scope text NOT NULL CHECK (chain_scope ~ '^[a-z0-9][a-z0-9_.-]{1,62}[a-z0-9]$'),
    source_kind text NOT NULL CHECK (source_kind IN ('facilitator_index', 'protocol_explicit_chain_index', 'unavailable')),
    earliest_available_coordinate bigint CHECK (earliest_available_coordinate >= 0),
    earliest_available_at timestamptz,
    retention_limit text NOT NULL CHECK (retention_limit <> ''),
    pagination_semantics text NOT NULL CHECK (pagination_semantics <> ''),
    attribution_strength text NOT NULL CHECK (
        attribution_strength IN (
            'signed_payment_attestation_plus_rpc',
            'protocol_explicit_chain_evidence',
            'known_facilitator_onchain',
            'unavailable'
        )
    ),
    unavailability_reason text,
    chain_exhaustive boolean NOT NULL DEFAULT false,
    admitted_provenance_id uuid NOT NULL,
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, source_id, chain_scope),
    FOREIGN KEY (namespace_id, admitted_provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id),
    CHECK (num_nonnulls(earliest_available_coordinate, earliest_available_at) IN (0, 2)),
    CHECK (
        (source_kind = 'unavailable'
            AND earliest_available_coordinate IS NULL
            AND earliest_available_at IS NULL
            AND attribution_strength = 'unavailable'
            AND unavailability_reason IS NOT NULL
            AND unavailability_reason <> '')
        OR (source_kind <> 'unavailable' AND unavailability_reason IS NULL)
    ),
    CHECK (NOT chain_exhaustive OR source_kind = 'protocol_explicit_chain_index')
);

CREATE TABLE agent_economy.x402_history_plans (
    namespace_id uuid NOT NULL,
    source_id text NOT NULL,
    chain_scope text NOT NULL,
    frozen_upper_coordinate bigint NOT NULL CHECK (frozen_upper_coordinate >= 0),
    registry_version text NOT NULL CHECK (registry_version <> ''),
    registry_sha256 text NOT NULL CHECK (registry_sha256 ~ '^[0-9a-f]{64}$'),
    range_size bigint NOT NULL CHECK (range_size BETWEEN 1 AND 10000),
    tail_interval_seconds bigint NOT NULL CHECK (tail_interval_seconds BETWEEN 60 AND 86400),
    admitted_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, source_id, chain_scope, frozen_upper_coordinate),
    FOREIGN KEY (namespace_id, source_id, chain_scope)
        REFERENCES agent_economy.x402_history_sources (namespace_id, source_id, chain_scope)
);

CREATE FUNCTION agent_economy.admit_x402_history_source(
    p_namespace_id uuid, p_source_id text, p_chain_scope text,
    p_earliest_coordinate bigint, p_earliest_at timestamptz,
    p_frozen_upper_coordinate bigint, p_registry_version text,
    p_registry_sha256 text, p_range_size bigint, p_tail_interval_seconds bigint,
    p_provenance_id uuid
)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF p_chain_scope NOT IN ('base', 'solana')
       OR p_source_id <> 'x402-facilitator-' || p_chain_scope
       OR p_earliest_coordinate < 0
       OR p_frozen_upper_coordinate < p_earliest_coordinate
       OR p_earliest_at IS NULL
       OR p_registry_version !~ '^[A-Za-z0-9_.:/-]{3,128}$'
       OR p_registry_sha256 !~ '^[0-9a-f]{64}$'
       OR p_range_size NOT BETWEEN 1 AND 10000
       OR p_tail_interval_seconds NOT BETWEEN 60 AND 86400
       OR NOT EXISTS (
           SELECT 1 FROM agent_economy.provenance_records provenance
           WHERE provenance.namespace_id = p_namespace_id
             AND provenance.provenance_id = p_provenance_id
             AND provenance.chain_scope = p_chain_scope
             AND provenance.source_id = p_source_id
       )
    THEN
        RAISE EXCEPTION 'invalid x402 history source admission';
    END IF;
    INSERT INTO agent_economy.x402_history_sources (
        namespace_id, source_id, chain_scope, source_kind,
        earliest_available_coordinate, earliest_available_at, retention_limit,
        pagination_semantics, attribution_strength, chain_exhaustive,
        admitted_provenance_id)
    VALUES (p_namespace_id, p_source_id, p_chain_scope, 'facilitator_index',
        p_earliest_coordinate, p_earliest_at, 'archival-rpc-provider-dependent',
        'inclusive-finalized-coordinate-range', 'known_facilitator_onchain',
        false, p_provenance_id)
    ON CONFLICT DO NOTHING;
    INSERT INTO agent_economy.x402_history_sources (
        namespace_id, source_id, chain_scope, source_kind,
        retention_limit, pagination_semantics, attribution_strength,
        unavailability_reason, chain_exhaustive, admitted_provenance_id)
    VALUES
        (p_namespace_id, 'x402-facilitator-registry', 'ethereum', 'unavailable',
         'not-available', 'not-applicable', 'unavailable',
         'no-admitted-x402-facilitator-registry', false, p_provenance_id),
        (p_namespace_id, 'x402-facilitator-registry', 'tempo', 'unavailable',
         'not-available', 'not-applicable', 'unavailable',
         'mpp-is-a-separate-protocol-series', false, p_provenance_id)
    ON CONFLICT DO NOTHING;
    INSERT INTO agent_economy.x402_history_plans (
        namespace_id, source_id, chain_scope, frozen_upper_coordinate,
        registry_version, registry_sha256, range_size, tail_interval_seconds)
    VALUES (p_namespace_id, p_source_id, p_chain_scope, p_frozen_upper_coordinate,
        p_registry_version, p_registry_sha256, p_range_size, p_tail_interval_seconds)
    ON CONFLICT DO NOTHING;
    RETURN FOUND;
END
$$;

CREATE FUNCTION agent_economy.plan_x402_history_jobs(
    p_namespace_id uuid, p_source_id text, p_chain_scope text, p_jobs jsonb
)
RETURNS bigint
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE planned bigint;
BEGIN
    IF jsonb_typeof(p_jobs) <> 'array' OR jsonb_array_length(p_jobs) > 10000
       OR NOT EXISTS (
           SELECT 1 FROM agent_economy.x402_history_plans plan
           WHERE plan.namespace_id = p_namespace_id AND plan.source_id = p_source_id
             AND plan.chain_scope = p_chain_scope
       )
       OR EXISTS (
           SELECT 1 FROM jsonb_array_elements(p_jobs) job(item)
           WHERE (SELECT count(*) FROM jsonb_object_keys(item)) <> 3
              OR NOT item ?& ARRAY['start','end','input_sha256']
              OR (item->>'input_sha256') !~ '^[0-9a-f]{64}$'
              OR (item->>'start') !~ '^[0-9]{1,19}$'
              OR (item->>'end') !~ '^[0-9]{1,19}$'
              OR (item->>'end')::numeric < (item->>'start')::numeric
              OR (item->>'end')::numeric - (item->>'start')::numeric + 1 > 10000
       )
       OR EXISTS (
           WITH authority AS (
               SELECT source.earliest_available_coordinate AS earliest,
                      max(plan.frozen_upper_coordinate) AS frozen
               FROM agent_economy.x402_history_sources source
               JOIN agent_economy.x402_history_plans plan USING (namespace_id, source_id, chain_scope)
               WHERE source.namespace_id = p_namespace_id AND source.source_id = p_source_id
                 AND source.chain_scope = p_chain_scope
               GROUP BY source.earliest_available_coordinate
           ), ordered AS (
               SELECT (item->>'start')::bigint AS start_height,
                      (item->>'end')::bigint AS end_height,
                      lag((item->>'end')::bigint) OVER (ORDER BY (item->>'start')::bigint) AS prior_end,
                      row_number() OVER (ORDER BY (item->>'start')::bigint) AS sequence,
                      count(*) OVER () AS total
               FROM jsonb_array_elements(p_jobs) job(item)
           )
           SELECT 1 FROM ordered CROSS JOIN authority
           WHERE (sequence = 1 AND start_height <> earliest)
              OR (sequence > 1 AND start_height <> prior_end + 1)
              OR end_height > frozen
              OR (sequence = total AND end_height <> frozen)
       )
    THEN
        RAISE EXCEPTION 'invalid x402 history job plan';
    END IF;
    WITH authority AS (
        SELECT source.earliest_available_coordinate AS earliest,
               plan.frozen_upper_coordinate AS frozen,
               plan.registry_version, plan.registry_sha256
        FROM agent_economy.x402_history_sources source
        JOIN LATERAL (
            SELECT candidate.* FROM agent_economy.x402_history_plans candidate
            WHERE candidate.namespace_id = source.namespace_id
              AND candidate.source_id = source.source_id
              AND candidate.chain_scope = source.chain_scope
            ORDER BY candidate.frozen_upper_coordinate DESC LIMIT 1
        ) plan ON true
        WHERE source.namespace_id = p_namespace_id AND source.source_id = p_source_id
          AND source.chain_scope = p_chain_scope
    ), ordered AS (
        SELECT (item->>'start')::bigint AS start_height,
               (item->>'end')::bigint AS end_height,
               item->>'input_sha256' AS input_sha256,
               lag((item->>'end')::bigint) OVER (ORDER BY (item->>'start')::bigint) AS prior_end
        FROM jsonb_array_elements(p_jobs) job(item)
    ), qualified AS (
        SELECT ordered.* FROM ordered CROSS JOIN authority
        WHERE (prior_end IS NULL AND start_height = earliest)
           OR (prior_end IS NOT NULL AND start_height = prior_end + 1)
    )
    INSERT INTO agent_economy.worker_jobs (
        namespace_id, job_id, mode, job_kind, idempotency_key, input_sha256,
        collection_chain_scope, collection_source_id, collection_start_height,
        collection_end_height, collection_acquisition_contract, collection_evidence_contract,
        collection_registry_version, collection_registry_sha256)
    SELECT p_namespace_id, gen_random_uuid(), 'collect', 'chain-protocol-range',
        p_source_id || ':' || start_height || ':' || end_height,
        input_sha256, p_chain_scope, p_source_id, start_height, end_height,
        'facilitator-rpc-block-v1', 'evidence-store-create-read-sha256-v1',
        authority.registry_version, authority.registry_sha256
    FROM qualified CROSS JOIN authority
    ON CONFLICT (namespace_id, idempotency_key) DO NOTHING;
    GET DIAGNOSTICS planned = ROW_COUNT;
    IF jsonb_array_length(p_jobs) > 0 AND planned = 0 AND NOT EXISTS (
        SELECT 1 FROM agent_economy.worker_jobs job
        WHERE job.namespace_id = p_namespace_id AND job.collection_source_id = p_source_id
    ) THEN
        RAISE EXCEPTION 'noncontiguous x402 history job plan';
    END IF;
    RETURN planned;
END
$$;

CREATE FUNCTION agent_economy.bind_pending_collection_registry()
RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    SELECT job.collection_registry_version, job.collection_registry_sha256
    INTO NEW.registry_version, NEW.registry_sha256
    FROM agent_economy.worker_jobs job
    WHERE job.namespace_id = NEW.namespace_id AND job.job_id = NEW.job_id;
    RETURN NEW;
END
$$;

CREATE TRIGGER bind_pending_collection_registry
BEFORE INSERT ON agent_economy.pending_collection_batches
FOR EACH ROW EXECUTE FUNCTION agent_economy.bind_pending_collection_registry();

CREATE TABLE agent_economy.x402_history_ranges (
    namespace_id uuid NOT NULL,
    source_id text NOT NULL,
    chain_scope text NOT NULL,
    range_id uuid NOT NULL,
    earliest_available_coordinate bigint CHECK (earliest_available_coordinate >= 0),
    frozen_upper_coordinate bigint NOT NULL CHECK (frozen_upper_coordinate >= 0),
    indexed_through_coordinate bigint CHECK (indexed_through_coordinate >= 0),
    source_total bigint CHECK (source_total >= 0),
    archived_records bigint NOT NULL CHECK (archived_records >= 0),
    parsed_records bigint NOT NULL CHECK (parsed_records >= 0),
    verified_transactions bigint NOT NULL CHECK (verified_transactions >= 0),
    duplicate_observations bigint NOT NULL CHECK (duplicate_observations >= 0),
    conflicting_observations bigint NOT NULL CHECK (conflicting_observations >= 0),
    gap_count bigint NOT NULL CHECK (gap_count >= 0),
    state text NOT NULL CHECK (
        state IN ('source-complete', 'partial', 'stale', 'failed', 'unknown')
    ),
    failure_code text,
    evidence_manifest_sha256 text NOT NULL CHECK (evidence_manifest_sha256 ~ '^[0-9a-f]{64}$'),
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, source_id, chain_scope, range_id),
    FOREIGN KEY (namespace_id, source_id, chain_scope)
        REFERENCES agent_economy.x402_history_sources (namespace_id, source_id, chain_scope),
    CHECK (
        indexed_through_coordinate IS NULL
        OR earliest_available_coordinate IS NULL
        OR indexed_through_coordinate >= earliest_available_coordinate
    ),
    CHECK (
        indexed_through_coordinate IS NULL
        OR indexed_through_coordinate <= frozen_upper_coordinate
    ),
    CHECK (verified_transactions <= parsed_records),
    CHECK (
        (state = 'source-complete'
            AND indexed_through_coordinate = frozen_upper_coordinate
            AND gap_count = 0
            AND conflicting_observations = 0
            AND failure_code IS NULL
            AND (source_total IS NULL OR archived_records = source_total))
        OR (state <> 'source-complete')
    ),
    CHECK ((state = 'failed' AND failure_code IS NOT NULL) OR state <> 'failed')
);

CREATE TABLE agent_economy.x402_payment_observations (
    namespace_id uuid NOT NULL,
    source_id text NOT NULL,
    chain_scope text NOT NULL,
    observation_id text NOT NULL CHECK (observation_id ~ '^sha256:[0-9a-f]{64}$'),
    source_coordinate bigint NOT NULL CHECK (source_coordinate >= 0),
    transaction_reference text NOT NULL CHECK (transaction_reference <> ''),
    settlement_position bigint NOT NULL CHECK (settlement_position >= 0),
    payer_handle_id text NOT NULL,
    recipient_handle text NOT NULL CHECK (recipient_handle <> ''),
    asset text NOT NULL CHECK (asset <> ''),
    amount_atomic numeric(78, 0) NOT NULL CHECK (amount_atomic >= 0),
    settled_at timestamptz NOT NULL,
    finality text NOT NULL CHECK (finality = 'finalized'),
    explicit_attribution_digest text NOT NULL CHECK (explicit_attribution_digest ~ '^[0-9a-f]{64}$'),
    evidence_id text NOT NULL,
    rpc_verification_evidence_id text NOT NULL,
    registry_version text NOT NULL CHECK (registry_version <> ''),
    registry_sha256 text NOT NULL CHECK (registry_sha256 ~ '^[0-9a-f]{64}$'),
    provenance_id uuid NOT NULL,
    parser_version text NOT NULL CHECK (parser_version <> ''),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, source_id, chain_scope, observation_id),
    UNIQUE (namespace_id, chain_scope, transaction_reference, settlement_position, source_id),
    FOREIGN KEY (namespace_id, source_id, chain_scope)
        REFERENCES agent_economy.x402_history_sources (namespace_id, source_id, chain_scope),
    FOREIGN KEY (namespace_id, payer_handle_id)
        REFERENCES agent_economy.buyer_handles (namespace_id, buyer_handle_id),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id),
    FOREIGN KEY (namespace_id, rpc_verification_evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.x402_settlement_observation_links (
    namespace_id uuid NOT NULL,
    chain_scope text NOT NULL,
    settlement_id text NOT NULL,
    source_id text NOT NULL,
    observation_id text NOT NULL,
    support_role text NOT NULL CHECK (support_role IN ('supporting', 'conflicting')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (namespace_id, chain_scope, settlement_id, source_id, observation_id),
    FOREIGN KEY (namespace_id, chain_scope, settlement_id)
        REFERENCES agent_economy.settlements (namespace_id, chain_scope, settlement_id),
    FOREIGN KEY (namespace_id, source_id, chain_scope, observation_id)
        REFERENCES agent_economy.x402_payment_observations (
            namespace_id, source_id, chain_scope, observation_id
        )
);

CREATE FUNCTION agent_economy.reconcile_x402_history_range(
    p_namespace_id uuid, p_source_id text, p_chain_scope text,
    p_start_coordinate bigint, p_end_coordinate bigint
)
RETURNS TABLE (verified bigint, duplicates bigint, conflicts bigint)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
BEGIN
    IF p_start_coordinate < 0 OR p_end_coordinate < p_start_coordinate THEN
        RAISE EXCEPTION 'invalid x402 reconciliation range';
    END IF;
    CREATE TEMP TABLE x402_reconcile_groups ON COMMIT DROP AS
    SELECT observation.transaction_reference, observation.settlement_position,
        min(observation.source_id) AS source_id,
        min(observation.provenance_id::text)::uuid AS provenance_id,
        min(observation.payer_handle_id) AS payer_handle_id,
        min(observation.asset) AS asset, min(observation.amount_atomic) AS amount_atomic,
        min(observation.settled_at) AS settled_at, count(*) AS observation_count,
        count(DISTINCT ROW(observation.payer_handle_id, observation.recipient_handle,
            observation.asset, observation.amount_atomic, observation.settled_at)) AS payload_count,
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            p_chain_scope || ':' || observation.transaction_reference || ':' ||
            observation.settlement_position::text, 'UTF8')), 'hex') AS identity_digest
    FROM agent_economy.x402_payment_observations observation
    WHERE observation.namespace_id = p_namespace_id
      AND observation.chain_scope = p_chain_scope
      AND observation.source_coordinate BETWEEN p_start_coordinate AND p_end_coordinate
    GROUP BY observation.transaction_reference, observation.settlement_position;

    INSERT INTO agent_economy.canonical_events (
        namespace_id, protocol, canonical_event_id, chain_scope, event_at,
        reducer_version, canonical_state_hash)
    SELECT p_namespace_id, 'x402', 'event:x402:sha256:' || identity_digest,
        p_chain_scope, settled_at, 'x402-history-reconciler-v1',
        pg_catalog.encode(pg_catalog.sha256(pg_catalog.convert_to(
            payer_handle_id || ':' || asset || ':' || amount_atomic::text || ':' ||
            settled_at::text, 'UTF8')), 'hex')
    FROM x402_reconcile_groups WHERE payload_count = 1 ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.canonical_event_observations (
        namespace_id, protocol, canonical_event_id, chain_scope,
        source_id, observation_id, support_role)
    SELECT p_namespace_id, 'x402', 'event:x402:sha256:' || grouped.identity_digest,
        p_chain_scope, observation.source_id, observation.observation_id, 'supporting'
    FROM x402_reconcile_groups grouped
    JOIN agent_economy.x402_payment_observations observation
      ON observation.namespace_id = p_namespace_id
     AND observation.chain_scope = p_chain_scope
     AND observation.transaction_reference = grouped.transaction_reference
     AND observation.settlement_position = grouped.settlement_position
    WHERE grouped.payload_count = 1 ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.event_finality_updates (
        namespace_id, protocol, chain_scope, canonical_event_id, finality_sequence,
        finality_status, asserted_at, source_id, provenance_id)
    SELECT p_namespace_id, 'x402', p_chain_scope, 'event:x402:sha256:' || identity_digest,
        1, 'finalized', settled_at, source_id, provenance_id
    FROM x402_reconcile_groups WHERE payload_count = 1 ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.settlements (
        namespace_id, chain_scope, settlement_id, protocol, canonical_event_id,
        source_id, buyer_handle_id, asset, amount_atomic, settled_at, provenance_id)
    SELECT p_namespace_id, p_chain_scope, 'settlement:x402:sha256:' || identity_digest,
        'x402', 'event:x402:sha256:' || identity_digest, source_id, payer_handle_id,
        asset, amount_atomic, settled_at, provenance_id
    FROM x402_reconcile_groups WHERE payload_count = 1 ON CONFLICT DO NOTHING;

    INSERT INTO agent_economy.x402_settlement_observation_links (
        namespace_id, chain_scope, settlement_id, source_id, observation_id, support_role)
    SELECT p_namespace_id, p_chain_scope, settlement.settlement_id,
        observation.source_id, observation.observation_id,
        CASE WHEN observation.payer_handle_id = settlement.buyer_handle_id
                  AND observation.asset = settlement.asset
                  AND observation.amount_atomic = settlement.amount_atomic
                  AND observation.settled_at = settlement.settled_at
             THEN 'supporting' ELSE 'conflicting' END
    FROM x402_reconcile_groups grouped
    JOIN agent_economy.settlements settlement
      ON settlement.namespace_id = p_namespace_id AND settlement.chain_scope = p_chain_scope
     AND settlement.settlement_id = 'settlement:x402:sha256:' || grouped.identity_digest
    JOIN agent_economy.x402_payment_observations observation
      ON observation.namespace_id = p_namespace_id
     AND observation.chain_scope = p_chain_scope
     AND observation.transaction_reference = grouped.transaction_reference
     AND observation.settlement_position = grouped.settlement_position
    ON CONFLICT DO NOTHING;

    RETURN QUERY SELECT
        count(*) FILTER (WHERE payload_count = 1),
        COALESCE(sum(observation_count - 1) FILTER (WHERE payload_count = 1), 0),
        count(*) FILTER (WHERE payload_count > 1)
    FROM x402_reconcile_groups;
END
$$;

CREATE INDEX x402_history_ranges_latest_idx
    ON agent_economy.x402_history_ranges (
        namespace_id, source_id, chain_scope, observed_at DESC, range_id DESC
    );
CREATE INDEX x402_payment_observations_payer_idx
    ON agent_economy.x402_payment_observations (
        namespace_id, payer_handle_id, settled_at DESC, observation_id
    );

CREATE OR REPLACE FUNCTION agent_economy.promote_pending_collection_batch(
    p_pending_id uuid, p_verifier_owner text, p_verifier_token uuid,
    p_observations_json jsonb, p_verifier_result_sha256 text
)
RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog AS $$
DECLARE
    pending agent_economy.pending_collection_batches%ROWTYPE;
    promoted boolean;
    bound_namespace uuid;
BEGIN
    bound_namespace := agent_economy.bound_collection_namespace('verify-evidence');
    SELECT * INTO pending FROM agent_economy.pending_collection_batches batch
    WHERE batch.namespace_id = bound_namespace AND batch.pending_id = p_pending_id
      AND batch.status = 'verifying' AND batch.verifier_owner = p_verifier_owner
      AND batch.verifier_token = p_verifier_token
      AND batch.verifier_expires_at > clock_timestamp() FOR UPDATE;
    IF NOT FOUND THEN RETURN false; END IF;
    IF p_verifier_result_sha256 !~ '^[0-9a-f]{64}$' THEN
        RAISE EXCEPTION 'invalid verifier result';
    END IF;
    SELECT agent_economy.commit_collection_batch(
      pending.namespace_id, pending.job_id, p_verifier_owner, p_verifier_token,
      pending.input_sha256, pending.batch_sha256, pending.chain_scope, pending.source_id,
      pending.observed_at_unix_ms, pending.start_height, pending.end_height,
      pending.evidence_json, p_observations_json) INTO promoted;
    IF promoted AND pending.source_id = 'x402-facilitator-' || pending.chain_scope THEN
        IF NOT EXISTS (
            SELECT 1 FROM agent_economy.x402_history_plans plan
            WHERE plan.namespace_id = bound_namespace
              AND plan.source_id = pending.source_id
              AND plan.chain_scope = pending.chain_scope
              AND pending.registry_version = plan.registry_version
              AND pending.registry_sha256 = plan.registry_sha256
        ) OR EXISTS (
            SELECT 1 FROM jsonb_array_elements(p_observations_json) input(item)
            WHERE item ->> 'registry_version' IS DISTINCT FROM pending.registry_version
               OR item ->> 'registry_sha256' IS DISTINCT FROM pending.registry_sha256
        ) THEN
            RAISE EXCEPTION 'x402 history registry binding does not match admission';
        END IF;

        INSERT INTO agent_economy.buyer_handles (
            namespace_id, buyer_handle_id, handle_kind, chain_scope,
            handle_value, provenance_id)
        SELECT bound_namespace, item ->> 'payer_handle_id',
            CASE WHEN pending.chain_scope = 'solana' THEN 'public_key' ELSE 'wallet' END,
            pending.chain_scope, item ->> 'payer_handle_value', observation.provenance_id
        FROM jsonb_array_elements(p_observations_json) input(item)
        JOIN agent_economy.observations observation
          ON observation.namespace_id = bound_namespace
         AND observation.chain_scope = pending.chain_scope
         AND observation.source_id = pending.source_id
         AND observation.observation_id = item ->> 'observation_id'
        ON CONFLICT (namespace_id, buyer_handle_id) DO NOTHING;

        INSERT INTO agent_economy.x402_payment_observations (
            namespace_id, source_id, chain_scope, observation_id, source_coordinate,
            transaction_reference, settlement_position, payer_handle_id, recipient_handle,
            asset, amount_atomic, settled_at, finality, explicit_attribution_digest,
            evidence_id, rpc_verification_evidence_id, registry_version, registry_sha256,
            provenance_id, parser_version)
        SELECT bound_namespace, pending.source_id, pending.chain_scope,
            item ->> 'observation_id', (item ->> 'height')::bigint,
            item ->> 'transaction_reference', (item ->> 'settlement_position')::bigint,
            item ->> 'payer_handle_id', item ->> 'recipient_handle', item ->> 'asset',
            (item ->> 'amount_atomic')::numeric,
            to_timestamp((item ->> 'settled_at_unix_seconds')::bigint),
            item ->> 'finality', item ->> 'explicit_attribution_digest',
            item ->> 'evidence_id', item ->> 'rpc_verification_evidence_id',
            item ->> 'registry_version', item ->> 'registry_sha256',
            observation.provenance_id, item ->> 'parser_version'
        FROM jsonb_array_elements(p_observations_json) input(item)
        JOIN agent_economy.observations observation
          ON observation.namespace_id = bound_namespace
         AND observation.chain_scope = pending.chain_scope
         AND observation.source_id = pending.source_id
         AND observation.observation_id = item ->> 'observation_id'
        ON CONFLICT DO NOTHING;

        PERFORM agent_economy.reconcile_x402_history_range(
            bound_namespace, pending.source_id, pending.chain_scope,
            pending.start_height, pending.end_height
        );

        INSERT INTO agent_economy.x402_history_ranges (
            namespace_id, source_id, chain_scope, range_id,
            earliest_available_coordinate, frozen_upper_coordinate,
            indexed_through_coordinate, source_total, archived_records, parsed_records,
            verified_transactions, duplicate_observations, conflicting_observations,
            gap_count, state, evidence_manifest_sha256, observed_at)
        WITH ordered AS (
            SELECT receipt.start_height, receipt.end_height,
                lag(receipt.end_height) OVER (ORDER BY receipt.start_height) AS prior_end
            FROM agent_economy.collection_range_receipts receipt
            WHERE receipt.namespace_id = bound_namespace
              AND receipt.source_id = pending.source_id
              AND receipt.chain_scope = pending.chain_scope
        ), aggregate AS (
            SELECT min(receipt.start_height) AS earliest,
                max(receipt.end_height) AS indexed_through,
                sum(receipt.end_height - receipt.start_height + 1) AS archived,
                bool_and(receipt.start_height = COALESCE(receipt.prior_end + 1, source.earliest_available_coordinate)) AS contiguous
            FROM ordered receipt
            CROSS JOIN agent_economy.x402_history_sources source
            WHERE source.namespace_id = bound_namespace
              AND source.source_id = pending.source_id
              AND source.chain_scope = pending.chain_scope
            GROUP BY source.earliest_available_coordinate
        ), reconciled AS (
            SELECT count(*) FILTER (WHERE observation.payload_count = 1) AS verified,
                COALESCE(sum(observation.observation_count - 1)
                    FILTER (WHERE observation.payload_count = 1), 0) AS duplicates,
                count(*) FILTER (WHERE observation.payload_count > 1) AS conflicts
            FROM (
                SELECT transaction_reference, settlement_position,
                    count(*) AS observation_count,
                    count(DISTINCT ROW(payer_handle_id, recipient_handle, asset,
                        amount_atomic, settled_at)) AS payload_count
                FROM agent_economy.x402_payment_observations
                WHERE namespace_id = bound_namespace
                  AND source_id = pending.source_id
                  AND chain_scope = pending.chain_scope
                GROUP BY transaction_reference, settlement_position
            ) observation
        )
        SELECT bound_namespace, pending.source_id, pending.chain_scope, pending.pending_id,
            source.earliest_available_coordinate, plan.frozen_upper_coordinate,
            aggregate.indexed_through,
            plan.frozen_upper_coordinate - source.earliest_available_coordinate + 1,
            aggregate.archived,
            (SELECT count(*) FROM agent_economy.x402_payment_observations observation
             WHERE observation.namespace_id = bound_namespace
               AND observation.source_id = pending.source_id
               AND observation.chain_scope = pending.chain_scope),
            reconciled.verified,
            reconciled.duplicates, reconciled.conflicts,
            CASE WHEN aggregate.contiguous
                    AND aggregate.indexed_through = plan.frozen_upper_coordinate
                    AND reconciled.conflicts = 0
                 THEN 0 ELSE 1 END,
            CASE WHEN aggregate.contiguous
                    AND aggregate.indexed_through = plan.frozen_upper_coordinate
                    AND reconciled.conflicts = 0
                 THEN 'source-complete' ELSE 'partial' END,
            pending.batch_sha256, to_timestamp(pending.observed_at_unix_ms / 1000.0)
        FROM agent_economy.x402_history_sources source
        JOIN LATERAL (
            SELECT candidate.frozen_upper_coordinate
            FROM agent_economy.x402_history_plans candidate
            WHERE candidate.namespace_id = source.namespace_id
              AND candidate.source_id = source.source_id
              AND candidate.chain_scope = source.chain_scope
            ORDER BY candidate.frozen_upper_coordinate DESC
            LIMIT 1
        ) plan ON true
        CROSS JOIN aggregate CROSS JOIN reconciled
        WHERE source.namespace_id = bound_namespace
          AND source.source_id = pending.source_id
          AND source.chain_scope = pending.chain_scope
        ON CONFLICT DO NOTHING;
    END IF;
    IF promoted THEN
      UPDATE agent_economy.pending_collection_batches SET status='succeeded',
        verifier_owner=NULL, verifier_token=NULL, verifier_expires_at=NULL,
        verifier_result_sha256=p_verifier_result_sha256, updated_at=clock_timestamp()
      WHERE pending_id=p_pending_id;
    END IF;
    RETURN promoted;
END
$$;

CREATE TRIGGER x402_history_sources_immutable
BEFORE UPDATE OR DELETE ON agent_economy.x402_history_sources
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER x402_history_plans_immutable
BEFORE UPDATE OR DELETE ON agent_economy.x402_history_plans
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER x402_history_ranges_immutable
BEFORE UPDATE OR DELETE ON agent_economy.x402_history_ranges
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER x402_payment_observations_immutable
BEFORE UPDATE OR DELETE ON agent_economy.x402_payment_observations
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER x402_settlement_observation_links_immutable
BEFORE UPDATE OR DELETE ON agent_economy.x402_settlement_observation_links
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE VIEW agent_economy.x402_history_coverage AS
SELECT
    source.namespace_id,
    source.source_id,
    source.chain_scope,
    source.source_kind,
    source.earliest_available_coordinate,
    source.earliest_available_at,
    range.frozen_upper_coordinate,
    range.indexed_through_coordinate,
    source.retention_limit,
    source.pagination_semantics,
    source.attribution_strength,
    range.source_total,
    COALESCE(range.archived_records, 0) AS archived_records,
    COALESCE(range.verified_transactions, 0) AS verified_transactions,
    COALESCE(range.duplicate_observations, 0) AS duplicate_observations,
    COALESCE(range.conflicting_observations, 0) AS conflicting_observations,
    CASE WHEN source.source_kind = 'unavailable' THEN 1
         ELSE COALESCE(range.gap_count, 0) END AS gap_count,
    CASE
        WHEN source.source_kind = 'unavailable' THEN 'unknown'
        WHEN range.state = 'source-complete' AND source.chain_exhaustive THEN 'chain-complete'
        ELSE COALESCE(range.state, 'unknown')
    END AS coverage_state,
    COALESCE(range.failure_code, source.unavailability_reason) AS failure_code,
    CASE WHEN source.source_kind = 'unavailable' THEN source.admitted_at
         ELSE range.observed_at END AS observed_at,
    CASE
        WHEN source.source_kind = 'unavailable' THEN NULL
        WHEN range.observed_at IS NULL THEN NULL
        ELSE GREATEST(0, EXTRACT(EPOCH FROM clock_timestamp() - range.observed_at)::bigint)
    END AS lag_seconds,
    source.admitted_provenance_id AS provenance_id
FROM agent_economy.x402_history_sources AS source
LEFT JOIN LATERAL (
    SELECT candidate.*
    FROM agent_economy.x402_history_ranges AS candidate
    WHERE candidate.namespace_id = source.namespace_id
      AND candidate.source_id = source.source_id
      AND candidate.chain_scope = source.chain_scope
    ORDER BY candidate.observed_at DESC, candidate.range_id DESC
    LIMIT 1
) AS range ON true;

REVOKE ALL ON agent_economy.x402_history_sources FROM PUBLIC;
REVOKE ALL ON agent_economy.x402_history_plans FROM PUBLIC;
REVOKE ALL ON agent_economy.x402_history_ranges FROM PUBLIC;
REVOKE ALL ON agent_economy.x402_payment_observations FROM PUBLIC;
REVOKE ALL ON agent_economy.x402_settlement_observation_links FROM PUBLIC;
REVOKE ALL ON agent_economy.x402_history_coverage FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.admit_x402_history_source(
    uuid, text, text, bigint, timestamptz, bigint, text, text, bigint, bigint, uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.plan_x402_history_jobs(uuid, text, text, jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.reconcile_x402_history_range(
    uuid, text, text, bigint, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.bind_pending_collection_registry() FROM PUBLIC;


GRANT EXECUTE ON FUNCTION agent_economy.admit_x402_history_source(
    uuid, text, text, bigint, timestamptz, bigint, text, text, bigint, bigint, uuid)
TO agent_economy_worker;
GRANT EXECUTE ON FUNCTION agent_economy.plan_x402_history_jobs(uuid, text, text, jsonb)
TO agent_economy_worker;
GRANT SELECT ON agent_economy.x402_history_coverage TO agent_economy_dashboard_reader;

COMMIT;
