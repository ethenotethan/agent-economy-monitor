BEGIN;

ALTER TABLE agent_economy.provenance_records
    ADD CONSTRAINT provenance_records_source_chain_key
    UNIQUE (namespace_id, provenance_id, source_id, chain_scope);
ALTER TABLE agent_economy.provenance_records
    ADD CONSTRAINT provenance_records_evidence_source_chain_key
    UNIQUE (namespace_id, provenance_id, evidence_id, source_id, chain_scope);
ALTER TABLE agent_economy.buyer_handles
    ADD CONSTRAINT buyer_handles_chain_key
    UNIQUE (namespace_id, buyer_handle_id, chain_scope);

CREATE TABLE agent_economy.collection_jobs (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    job_id uuid NOT NULL,
    job_kind text NOT NULL CHECK (job_kind <> ''),
    chain_scope text NOT NULL CHECK (chain_scope <> ''),
    source_id text NOT NULL CHECK (source_id <> ''),
    scheduled_for timestamptz NOT NULL,
    idempotency_key text NOT NULL CHECK (idempotency_key <> ''),
    status text NOT NULL CHECK (status IN ('pending', 'leased', 'succeeded', 'retryable', 'dead_letter')),
    lease_owner text,
    lease_expires_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    last_error_code text,
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, job_id),
    UNIQUE (namespace_id, idempotency_key),
    CHECK (
        (status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR (status <> 'leased' AND lease_owner IS NULL AND lease_expires_at IS NULL)
    )
);

CREATE INDEX collection_jobs_claim_idx
    ON agent_economy.collection_jobs (namespace_id, status, scheduled_for, lease_expires_at);
CREATE INDEX collection_jobs_chain_source_idx
    ON agent_economy.collection_jobs (namespace_id, chain_scope, source_id, scheduled_for);

CREATE TABLE agent_economy.observations (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    chain_scope text NOT NULL CHECK (chain_scope <> ''),
    source_id text NOT NULL CHECK (source_id <> ''),
    observation_id text NOT NULL CHECK (observation_id ~ '^sha256:[0-9a-f]{64}$'),
    observed_at timestamptz NOT NULL,
    parser_version text NOT NULL CHECK (parser_version <> ''),
    protocol text NOT NULL CHECK (protocol IN ('x402', 'mpp')),
    evidence_id text NOT NULL,
    provenance_id uuid NOT NULL,
    observation_hash text NOT NULL CHECK (observation_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, chain_scope, source_id, protocol, observation_id),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id),
    FOREIGN KEY (
        namespace_id, provenance_id, evidence_id, source_id, chain_scope
    )
        REFERENCES agent_economy.provenance_records (
            namespace_id, provenance_id, evidence_id, source_id, chain_scope
        )
) PARTITION BY HASH (namespace_id, chain_scope, source_id);

CREATE TABLE agent_economy.observations_p0 PARTITION OF agent_economy.observations FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE agent_economy.observations_p1 PARTITION OF agent_economy.observations FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE agent_economy.observations_p2 PARTITION OF agent_economy.observations FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE agent_economy.observations_p3 PARTITION OF agent_economy.observations FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE agent_economy.observations_p4 PARTITION OF agent_economy.observations FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE agent_economy.observations_p5 PARTITION OF agent_economy.observations FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE agent_economy.observations_p6 PARTITION OF agent_economy.observations FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE agent_economy.observations_p7 PARTITION OF agent_economy.observations FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX observations_chain_source_time_idx
    ON agent_economy.observations (namespace_id, chain_scope, source_id, observed_at DESC);
CREATE INDEX observations_evidence_idx
    ON agent_economy.observations (namespace_id, evidence_id);

CREATE TABLE agent_economy.canonical_events (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    protocol text NOT NULL CHECK (protocol IN ('x402', 'mpp')),
    canonical_event_id text NOT NULL,
    chain_scope text NOT NULL CHECK (chain_scope <> ''),
    event_at timestamptz NOT NULL,
    reducer_version text NOT NULL CHECK (reducer_version <> ''),
    canonical_state_hash text NOT NULL CHECK (canonical_state_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, protocol, chain_scope, canonical_event_id),
    CHECK (canonical_event_id LIKE 'event:' || protocol || ':sha256:%')
) PARTITION BY HASH (namespace_id, protocol);

CREATE TABLE agent_economy.canonical_events_p0 PARTITION OF agent_economy.canonical_events FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE agent_economy.canonical_events_p1 PARTITION OF agent_economy.canonical_events FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE agent_economy.canonical_events_p2 PARTITION OF agent_economy.canonical_events FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE agent_economy.canonical_events_p3 PARTITION OF agent_economy.canonical_events FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE agent_economy.canonical_events_p4 PARTITION OF agent_economy.canonical_events FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE agent_economy.canonical_events_p5 PARTITION OF agent_economy.canonical_events FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE agent_economy.canonical_events_p6 PARTITION OF agent_economy.canonical_events FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE agent_economy.canonical_events_p7 PARTITION OF agent_economy.canonical_events FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX canonical_events_protocol_time_idx
    ON agent_economy.canonical_events (namespace_id, protocol, event_at DESC);
CREATE INDEX canonical_events_chain_time_idx
    ON agent_economy.canonical_events (namespace_id, chain_scope, event_at DESC);

CREATE TABLE agent_economy.canonical_event_observations (
    namespace_id uuid NOT NULL,
    protocol text NOT NULL,
    canonical_event_id text NOT NULL,
    chain_scope text NOT NULL,
    source_id text NOT NULL,
    observation_id text NOT NULL,
    support_role text NOT NULL CHECK (support_role IN ('supporting', 'conflicting')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, protocol, chain_scope, canonical_event_id,
        source_id, observation_id
    ),
    FOREIGN KEY (namespace_id, protocol, chain_scope, canonical_event_id)
        REFERENCES agent_economy.canonical_events (
            namespace_id, protocol, chain_scope, canonical_event_id
        ),
    FOREIGN KEY (namespace_id, chain_scope, source_id, protocol, observation_id)
        REFERENCES agent_economy.observations (
            namespace_id, chain_scope, source_id, protocol, observation_id
        )
);

CREATE INDEX canonical_event_observations_observation_idx
    ON agent_economy.canonical_event_observations (
        namespace_id, chain_scope, source_id, observation_id
    );

CREATE TABLE agent_economy.event_finality_updates (
    namespace_id uuid NOT NULL,
    protocol text NOT NULL,
    chain_scope text NOT NULL,
    canonical_event_id text NOT NULL,
    finality_sequence integer NOT NULL CHECK (finality_sequence > 0),
    finality_status text NOT NULL CHECK (finality_status IN ('observed', 'confirmed', 'finalized', 'orphaned')),
    asserted_at timestamptz NOT NULL,
    source_id text NOT NULL CHECK (source_id <> ''),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, protocol, chain_scope, canonical_event_id, finality_sequence),
    FOREIGN KEY (namespace_id, protocol, chain_scope, canonical_event_id)
        REFERENCES agent_economy.canonical_events (
            namespace_id, protocol, chain_scope, canonical_event_id
        ),
    FOREIGN KEY (namespace_id, provenance_id, source_id, chain_scope)
        REFERENCES agent_economy.provenance_records (
            namespace_id, provenance_id, source_id, chain_scope
        )
);

CREATE INDEX event_finality_updates_current_idx
    ON agent_economy.event_finality_updates (
        namespace_id, protocol, chain_scope, canonical_event_id, finality_sequence DESC
    );

CREATE TABLE agent_economy.settlements (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    chain_scope text NOT NULL CHECK (chain_scope <> ''),
    settlement_id text NOT NULL CHECK (settlement_id <> ''),
    protocol text NOT NULL CHECK (protocol IN ('x402', 'mpp')),
    canonical_event_id text NOT NULL,
    source_id text NOT NULL CHECK (source_id <> ''),
    buyer_handle_id text,
    asset text NOT NULL CHECK (asset <> ''),
    amount_atomic numeric(78, 0) NOT NULL CHECK (amount_atomic >= 0),
    settled_at timestamptz NOT NULL,
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, chain_scope, settlement_id),
    FOREIGN KEY (namespace_id, protocol, chain_scope, canonical_event_id)
        REFERENCES agent_economy.canonical_events (
            namespace_id, protocol, chain_scope, canonical_event_id
        ),
    FOREIGN KEY (namespace_id, buyer_handle_id, chain_scope)
        REFERENCES agent_economy.buyer_handles (
            namespace_id, buyer_handle_id, chain_scope
        ),
    FOREIGN KEY (namespace_id, provenance_id, source_id, chain_scope)
        REFERENCES agent_economy.provenance_records (
            namespace_id, provenance_id, source_id, chain_scope
        )
) PARTITION BY HASH (namespace_id, chain_scope);

CREATE TABLE agent_economy.settlements_p0 PARTITION OF agent_economy.settlements FOR VALUES WITH (MODULUS 8, REMAINDER 0);
CREATE TABLE agent_economy.settlements_p1 PARTITION OF agent_economy.settlements FOR VALUES WITH (MODULUS 8, REMAINDER 1);
CREATE TABLE agent_economy.settlements_p2 PARTITION OF agent_economy.settlements FOR VALUES WITH (MODULUS 8, REMAINDER 2);
CREATE TABLE agent_economy.settlements_p3 PARTITION OF agent_economy.settlements FOR VALUES WITH (MODULUS 8, REMAINDER 3);
CREATE TABLE agent_economy.settlements_p4 PARTITION OF agent_economy.settlements FOR VALUES WITH (MODULUS 8, REMAINDER 4);
CREATE TABLE agent_economy.settlements_p5 PARTITION OF agent_economy.settlements FOR VALUES WITH (MODULUS 8, REMAINDER 5);
CREATE TABLE agent_economy.settlements_p6 PARTITION OF agent_economy.settlements FOR VALUES WITH (MODULUS 8, REMAINDER 6);
CREATE TABLE agent_economy.settlements_p7 PARTITION OF agent_economy.settlements FOR VALUES WITH (MODULUS 8, REMAINDER 7);

CREATE INDEX settlements_chain_time_idx
    ON agent_economy.settlements (namespace_id, chain_scope, settled_at DESC);
CREATE INDEX settlements_buyer_time_idx
    ON agent_economy.settlements (namespace_id, buyer_handle_id, settled_at DESC)
    WHERE buyer_handle_id IS NOT NULL;
CREATE INDEX settlements_event_idx
    ON agent_economy.settlements (namespace_id, protocol, canonical_event_id);

CREATE TABLE agent_economy.feature_windows (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    buyer_handle_id text NOT NULL,
    feature_name text NOT NULL CHECK (feature_name <> ''),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    feature_version text NOT NULL CHECK (feature_version <> ''),
    input_snapshot_hash text NOT NULL CHECK (input_snapshot_hash ~ '^[0-9a-f]{64}$'),
    feature_value jsonb NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, buyer_handle_id, feature_name,
        window_start, window_end, feature_version
    ),
    CHECK (window_end > window_start),
    FOREIGN KEY (namespace_id, buyer_handle_id)
        REFERENCES agent_economy.buyer_handles (namespace_id, buyer_handle_id)
);

CREATE INDEX feature_windows_buyer_window_idx
    ON agent_economy.feature_windows (
        namespace_id, buyer_handle_id, feature_name, window_end DESC
    );

CREATE TABLE agent_economy.analytics_daily_metrics (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    measured_on date NOT NULL,
    p95_query_latency_ms integer NOT NULL CHECK (p95_query_latency_ms >= 0),
    canonical_event_count bigint NOT NULL CHECK (canonical_event_count >= 0),
    analytics_cpu_percent numeric(5, 2) NOT NULL CHECK (analytics_cpu_percent >= 0 AND analytics_cpu_percent <= 100),
    materialized_views_tuned boolean NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, measured_on)
);

CREATE VIEW agent_economy.current_event_finality AS
SELECT DISTINCT ON (namespace_id, protocol, chain_scope, canonical_event_id)
    namespace_id,
    protocol,
    chain_scope,
    canonical_event_id,
    finality_sequence,
    finality_status,
    asserted_at,
    source_id,
    provenance_id
FROM agent_economy.event_finality_updates
ORDER BY namespace_id, protocol, chain_scope, canonical_event_id, finality_sequence DESC;

CREATE MATERIALIZED VIEW agent_economy.pulse_hourly AS
SELECT
    s.namespace_id,
    date_trunc('hour', s.settled_at) AS hour,
    s.protocol,
    s.chain_scope,
    s.asset,
    count(*)::bigint AS settlement_count,
    count(DISTINCT s.buyer_handle_id)::bigint AS active_buyers,
    sum(s.amount_atomic) AS amount_atomic
FROM agent_economy.settlements s
JOIN agent_economy.current_event_finality f
  ON f.namespace_id = s.namespace_id
 AND f.protocol = s.protocol
 AND f.chain_scope = s.chain_scope
 AND f.canonical_event_id = s.canonical_event_id
WHERE f.finality_status = 'finalized'
GROUP BY s.namespace_id, date_trunc('hour', s.settled_at), s.protocol, s.chain_scope, s.asset
WITH NO DATA;

CREATE UNIQUE INDEX pulse_hourly_identity_idx
    ON agent_economy.pulse_hourly (namespace_id, hour, protocol, chain_scope, asset);

CREATE VIEW agent_economy.bigquery_projection_readiness AS
WITH measured AS (
    SELECT
        namespace_id,
        measured_on,
        canonical_event_count,
        analytics_cpu_percent,
        count(*) OVER rolling AS sample_days,
        min(measured_on) OVER rolling AS window_start,
        count(*) FILTER (
            WHERE materialized_views_tuned AND p95_query_latency_ms > 2000
        ) OVER rolling AS latency_days,
        count(*) FILTER (
            WHERE canonical_event_count > 100000000 OR analytics_cpu_percent > 30
        ) OVER rolling AS scale_days
    FROM agent_economy.analytics_daily_metrics
    WINDOW rolling AS (
        PARTITION BY namespace_id
        ORDER BY measured_on
        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
    )
)
SELECT
    namespace_id,
    measured_on,
    latency_days,
    scale_days,
    sample_days = 14
        AND window_start = measured_on - 13
        AND latency_days = 14
        AND (canonical_event_count > 100000000 OR analytics_cpu_percent > 30) AS ready
FROM measured;

CREATE TRIGGER observations_immutable
BEFORE UPDATE OR DELETE ON agent_economy.observations
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER canonical_events_immutable
BEFORE UPDATE OR DELETE ON agent_economy.canonical_events
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER canonical_event_observations_immutable
BEFORE UPDATE OR DELETE ON agent_economy.canonical_event_observations
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER event_finality_updates_immutable
BEFORE UPDATE OR DELETE ON agent_economy.event_finality_updates
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER settlements_immutable
BEFORE UPDATE OR DELETE ON agent_economy.settlements
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER feature_windows_immutable
BEFORE UPDATE OR DELETE ON agent_economy.feature_windows
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER analytics_daily_metrics_immutable
BEFORE UPDATE OR DELETE ON agent_economy.analytics_daily_metrics
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER observations_truncate_immutable
BEFORE TRUNCATE ON agent_economy.observations
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER canonical_events_truncate_immutable
BEFORE TRUNCATE ON agent_economy.canonical_events
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER canonical_event_observations_truncate_immutable
BEFORE TRUNCATE ON agent_economy.canonical_event_observations
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER event_finality_updates_truncate_immutable
BEFORE TRUNCATE ON agent_economy.event_finality_updates
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER settlements_truncate_immutable
BEFORE TRUNCATE ON agent_economy.settlements
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER feature_windows_truncate_immutable
BEFORE TRUNCATE ON agent_economy.feature_windows
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER analytics_daily_metrics_truncate_immutable
BEFORE TRUNCATE ON agent_economy.analytics_daily_metrics
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

DO $partition_triggers$
DECLARE
    partition_table regclass;
BEGIN
    FOREACH partition_table IN ARRAY ARRAY[
        'agent_economy.observations_p0'::regclass,
        'agent_economy.observations_p1'::regclass,
        'agent_economy.observations_p2'::regclass,
        'agent_economy.observations_p3'::regclass,
        'agent_economy.observations_p4'::regclass,
        'agent_economy.observations_p5'::regclass,
        'agent_economy.observations_p6'::regclass,
        'agent_economy.observations_p7'::regclass,
        'agent_economy.canonical_events_p0'::regclass,
        'agent_economy.canonical_events_p1'::regclass,
        'agent_economy.canonical_events_p2'::regclass,
        'agent_economy.canonical_events_p3'::regclass,
        'agent_economy.canonical_events_p4'::regclass,
        'agent_economy.canonical_events_p5'::regclass,
        'agent_economy.canonical_events_p6'::regclass,
        'agent_economy.canonical_events_p7'::regclass,
        'agent_economy.settlements_p0'::regclass,
        'agent_economy.settlements_p1'::regclass,
        'agent_economy.settlements_p2'::regclass,
        'agent_economy.settlements_p3'::regclass,
        'agent_economy.settlements_p4'::regclass,
        'agent_economy.settlements_p5'::regclass,
        'agent_economy.settlements_p6'::regclass,
        'agent_economy.settlements_p7'::regclass
    ]
    LOOP
        EXECUTE format(
            'CREATE TRIGGER partition_truncate_immutable '
            'BEFORE TRUNCATE ON %s FOR EACH STATEMENT '
            'EXECUTE FUNCTION agent_economy.reject_immutable_mutation()',
            partition_table
        );
    END LOOP;
END
$partition_triggers$;

COMMIT;
