BEGIN;

CREATE TABLE agent_economy.classification_label_definitions (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    label_id text NOT NULL CHECK (label_id <> ''),
    label_version integer NOT NULL CHECK (label_version > 0),
    label_kind text NOT NULL CHECK (label_kind IN ('core', 'extension')),
    rule_definition jsonb NOT NULL CHECK (jsonb_typeof(rule_definition) = 'object'),
    definition_hash text NOT NULL CHECK (definition_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, label_id, label_version),
    UNIQUE (namespace_id, definition_hash)
);

CREATE TABLE agent_economy.classification_runs (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    run_id text NOT NULL CHECK (run_id <> ''),
    run_version integer NOT NULL CHECK (run_version > 0),
    supersedes_version integer,
    buyer_handle_id text,
    cluster_id uuid,
    cluster_version integer CHECK (cluster_version > 0),
    classifier_version text NOT NULL CHECK (classifier_version <> ''),
    feature_version text NOT NULL CHECK (feature_version <> ''),
    label_set_hash text NOT NULL CHECK (label_set_hash ~ '^[0-9a-f]{64}$'),
    input_snapshot_hash text NOT NULL CHECK (input_snapshot_hash ~ '^[0-9a-f]{64}$'),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, run_id, run_version),
    CHECK (
        (run_version = 1 AND supersedes_version IS NULL)
        OR (run_version > 1 AND supersedes_version = run_version - 1)
    ),
    CONSTRAINT classification_runs_target_check CHECK (
        (buyer_handle_id IS NOT NULL AND cluster_id IS NULL AND cluster_version IS NULL)
        OR (buyer_handle_id IS NULL AND cluster_id IS NOT NULL AND cluster_version IS NOT NULL)
    ),
    CHECK (window_end > window_start),
    FOREIGN KEY (namespace_id, buyer_handle_id)
        REFERENCES agent_economy.buyer_handles (namespace_id, buyer_handle_id),
    FOREIGN KEY (namespace_id, cluster_id, cluster_version)
        REFERENCES agent_economy.buyer_cluster_versions (
            namespace_id, cluster_id, version
        ),
    FOREIGN KEY (namespace_id, run_id, supersedes_version)
        REFERENCES agent_economy.classification_runs (namespace_id, run_id, run_version)
);

CREATE TABLE agent_economy.classification_run_label_definitions (
    namespace_id uuid NOT NULL,
    run_id text NOT NULL,
    run_version integer NOT NULL,
    label_id text NOT NULL,
    label_version integer NOT NULL CHECK (label_version > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, run_id, run_version, label_id, label_version
    ),
    FOREIGN KEY (namespace_id, run_id, run_version)
        REFERENCES agent_economy.classification_runs (namespace_id, run_id, run_version),
    FOREIGN KEY (namespace_id, label_id, label_version)
        REFERENCES agent_economy.classification_label_definitions (
            namespace_id, label_id, label_version
        )
);

CREATE TABLE agent_economy.classification_run_features (
    namespace_id uuid NOT NULL,
    run_id text NOT NULL,
    run_version integer NOT NULL,
    feature_name text NOT NULL CHECK (feature_name IN (
        'total_spend_atomic',
        'payment_count',
        'median_cadence_seconds',
        'x402_count',
        'mpp_count',
        'unique_counterparties',
        'autonomous_count',
        'autonomy_observed_count'
    )),
    feature_value jsonb NOT NULL CHECK (
        (feature_name = 'median_cadence_seconds' AND feature_value = 'null'::jsonb)
        OR (
            jsonb_typeof(feature_value) = 'number'
            AND feature_value::text ~ '^(0|[1-9][0-9]*)$'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, run_id, run_version, feature_name),
    FOREIGN KEY (namespace_id, run_id, run_version)
        REFERENCES agent_economy.classification_runs (namespace_id, run_id, run_version)
);

CREATE TABLE agent_economy.classification_run_evidence (
    namespace_id uuid NOT NULL,
    run_id text NOT NULL,
    run_version integer NOT NULL,
    evidence_id text NOT NULL,
    evidence_role text NOT NULL CHECK (evidence_role IN ('supporting', 'conflicting')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, run_id, run_version, evidence_id),
    FOREIGN KEY (namespace_id, run_id, run_version)
        REFERENCES agent_economy.classification_runs (namespace_id, run_id, run_version),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id)
);

CREATE TABLE agent_economy.classification_run_claims (
    namespace_id uuid NOT NULL,
    run_id text NOT NULL,
    run_version integer NOT NULL,
    claim_id text NOT NULL,
    claim_version integer NOT NULL CHECK (claim_version > 0),
    label_id text NOT NULL,
    label_version integer NOT NULL CHECK (label_version > 0),
    status text NOT NULL CHECK (status IN ('verified', 'inferred', 'disputed')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, run_id, run_version, claim_id, claim_version),
    UNIQUE (namespace_id, run_id, run_version, label_id, label_version),
    FOREIGN KEY (namespace_id, run_id, run_version)
        REFERENCES agent_economy.classification_runs (namespace_id, run_id, run_version),
    FOREIGN KEY (namespace_id, claim_id, claim_version)
        REFERENCES agent_economy.classification_claims (namespace_id, claim_id, version),
    FOREIGN KEY (namespace_id, label_id, label_version)
        REFERENCES agent_economy.classification_label_definitions (
            namespace_id, label_id, label_version
        )
);

CREATE TABLE agent_economy.classification_run_seals (
    namespace_id uuid NOT NULL,
    run_id text NOT NULL,
    run_version integer NOT NULL,
    result_encoding bytea NOT NULL,
    state_hash text NOT NULL CHECK (state_hash ~ '^[0-9a-f]{64}$'),
    content_hash text NOT NULL CHECK (content_hash ~ '^[0-9a-f]{64}$'),
    sealed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, run_id, run_version),
    UNIQUE (namespace_id, run_id, run_version, content_hash),
    FOREIGN KEY (namespace_id, run_id, run_version)
        REFERENCES agent_economy.classification_runs (namespace_id, run_id, run_version)
);

CREATE TABLE agent_economy.classification_replay_drift (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    baseline_run_id text NOT NULL,
    baseline_run_version integer NOT NULL,
    replay_run_id text NOT NULL,
    replay_run_version integer NOT NULL,
    baseline_content_hash text NOT NULL CHECK (baseline_content_hash ~ '^[0-9a-f]{64}$'),
    replay_content_hash text NOT NULL CHECK (replay_content_hash ~ '^[0-9a-f]{64}$'),
    input_changed boolean NOT NULL,
    added_labels text[] NOT NULL DEFAULT '{}',
    removed_labels text[] NOT NULL DEFAULT '{}',
    label_churn_count integer NOT NULL CHECK (label_churn_count >= 0),
    measured_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, baseline_run_id, baseline_run_version,
        replay_run_id, replay_run_version
    ),
    CHECK (
        baseline_run_id <> replay_run_id
        OR baseline_run_version <> replay_run_version
    ),
    CHECK (label_churn_count = cardinality(added_labels) + cardinality(removed_labels)),
    FOREIGN KEY (
        namespace_id, baseline_run_id, baseline_run_version, baseline_content_hash
    ) REFERENCES agent_economy.classification_run_seals (
        namespace_id, run_id, run_version, content_hash
    ),
    FOREIGN KEY (
        namespace_id, replay_run_id, replay_run_version, replay_content_hash
    ) REFERENCES agent_economy.classification_run_seals (
        namespace_id, run_id, run_version, content_hash
    )
);

CREATE FUNCTION agent_economy.lock_classification_series()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            NEW.namespace_id::pg_catalog.text || ':' || NEW.run_id,
            0
        )
    );
    IF NEW.run_version > 1 AND NOT EXISTS (
        SELECT 1
        FROM agent_economy.classification_run_seals
        WHERE namespace_id = NEW.namespace_id
          AND run_id = NEW.run_id
          AND run_version = NEW.supersedes_version
    ) THEN
        RAISE EXCEPTION 'classification predecessor must be sealed'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.guard_classification_child_append()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            NEW.namespace_id::pg_catalog.text || ':' || NEW.run_id,
            0
        )
    );
    IF EXISTS (
        SELECT 1
        FROM agent_economy.classification_run_seals
        WHERE namespace_id = NEW.namespace_id
          AND run_id = NEW.run_id
          AND run_version = NEW.run_version
    ) THEN
        RAISE EXCEPTION 'classification run is sealed'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.validate_classification_run_seal()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    run_row agent_economy.classification_runs%ROWTYPE;
    feature_count bigint;
    label_set_count bigint;
    feature_values jsonb;
    content_document jsonb;
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            NEW.namespace_id::pg_catalog.text || ':' || NEW.run_id,
            0
        )
    );
    IF NEW.state_hash <> pg_catalog.encode(pg_catalog.sha256(NEW.result_encoding), 'hex') THEN
        RAISE EXCEPTION 'classification state hash does not match result encoding'
            USING ERRCODE = '23514';
    END IF;
    SELECT * INTO STRICT run_row
    FROM agent_economy.classification_runs
    WHERE namespace_id = NEW.namespace_id
      AND run_id = NEW.run_id
      AND run_version = NEW.run_version;
    SELECT pg_catalog.count(*), pg_catalog.jsonb_object_agg(feature_name, feature_value)
      INTO feature_count, feature_values
    FROM agent_economy.classification_run_features
    WHERE namespace_id = NEW.namespace_id
      AND run_id = NEW.run_id
      AND run_version = NEW.run_version;
    IF feature_count <> 8 THEN
        RAISE EXCEPTION 'classification run must contain the complete feature vector'
            USING ERRCODE = '23514';
    END IF;
    IF (feature_values ->> 'payment_count')::pg_catalog.numeric
           <> (feature_values ->> 'x402_count')::pg_catalog.numeric
              + (feature_values ->> 'mpp_count')::pg_catalog.numeric
       OR (feature_values ->> 'unique_counterparties')::pg_catalog.numeric
           > (feature_values ->> 'payment_count')::pg_catalog.numeric
       OR (feature_values ->> 'autonomous_count')::pg_catalog.numeric
           > (feature_values ->> 'autonomy_observed_count')::pg_catalog.numeric
       OR (feature_values ->> 'autonomy_observed_count')::pg_catalog.numeric
           > (feature_values ->> 'payment_count')::pg_catalog.numeric
       OR (
           (feature_values ->> 'payment_count')::pg_catalog.numeric < 2
           AND feature_values -> 'median_cadence_seconds' <> 'null'::jsonb
       )
       OR (
           (feature_values ->> 'payment_count')::pg_catalog.numeric >= 2
           AND feature_values -> 'median_cadence_seconds' = 'null'::jsonb
       )
    THEN
        RAISE EXCEPTION 'classification feature values are internally inconsistent'
            USING ERRCODE = '23514';
    END IF;
    SELECT pg_catalog.count(*) INTO label_set_count
    FROM agent_economy.classification_run_label_definitions
    WHERE namespace_id = NEW.namespace_id
      AND run_id = NEW.run_id
      AND run_version = NEW.run_version;
    IF label_set_count = 0 THEN
        RAISE EXCEPTION 'classification run must bind its complete label set'
            USING ERRCODE = '23514';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM agent_economy.classification_run_evidence
        WHERE namespace_id = NEW.namespace_id
          AND run_id = NEW.run_id
          AND run_version = NEW.run_version
          AND evidence_role = 'supporting'
    ) THEN
        RAISE EXCEPTION 'classification run must cite supporting evidence'
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM agent_economy.classification_run_claims AS run_claim
        JOIN agent_economy.classification_claims AS claim
          ON claim.namespace_id = run_claim.namespace_id
         AND claim.claim_id = run_claim.claim_id
         AND claim.version = run_claim.claim_version
        WHERE run_claim.namespace_id = NEW.namespace_id
          AND run_claim.run_id = NEW.run_id
          AND run_claim.run_version = NEW.run_version
          AND (
              claim.label <> run_claim.label_id
              OR claim.status <> run_claim.status
              OR claim.method <> run_row.classifier_version
              OR claim.evidence_window_start <> run_row.window_start
              OR claim.evidence_window_end <> run_row.window_end
              OR claim.buyer_handle_id IS DISTINCT FROM run_row.buyer_handle_id
              OR claim.cluster_id IS DISTINCT FROM run_row.cluster_id
          )
    ) THEN
        RAISE EXCEPTION 'classification claim does not match its run target, window, method, label, or status'
            USING ERRCODE = '23514';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM agent_economy.classification_run_claims AS run_claim
        WHERE run_claim.namespace_id = NEW.namespace_id
          AND run_claim.run_id = NEW.run_id
          AND run_claim.run_version = NEW.run_version
          AND NOT EXISTS (
              SELECT 1
              FROM agent_economy.classification_run_label_definitions AS run_label
              WHERE run_label.namespace_id = run_claim.namespace_id
                AND run_label.run_id = run_claim.run_id
                AND run_label.run_version = run_claim.run_version
                AND run_label.label_id = run_claim.label_id
                AND run_label.label_version = run_claim.label_version
          )
    ) THEN
        RAISE EXCEPTION 'classification claim label is absent from the bound label set'
            USING ERRCODE = '23514';
    END IF;
    SELECT pg_catalog.jsonb_build_object(
        'run', pg_catalog.jsonb_build_object(
            'run_id', run_row.run_id,
            'run_version', run_row.run_version,
            'supersedes_version', run_row.supersedes_version,
            'buyer_handle_id', run_row.buyer_handle_id,
            'cluster_id', run_row.cluster_id,
            'cluster_version', run_row.cluster_version,
            'classifier_version', run_row.classifier_version,
            'feature_version', run_row.feature_version,
            'label_set_hash', run_row.label_set_hash,
            'input_snapshot_hash', run_row.input_snapshot_hash,
            'window_start_epoch_micros',
                (extract(epoch FROM run_row.window_start) * 1000000)::pg_catalog.int8,
            'window_end_epoch_micros',
                (extract(epoch FROM run_row.window_end) * 1000000)::pg_catalog.int8
        ),
        'label_definitions', (
            SELECT pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object(
                    'label_id', definition.label_id,
                    'label_version', definition.label_version,
                    'label_kind', definition.label_kind,
                    'rule_definition', definition.rule_definition,
                    'definition_hash', definition.definition_hash
                ) ORDER BY definition.label_id, definition.label_version
            )
            FROM agent_economy.classification_run_label_definitions AS run_label
            JOIN agent_economy.classification_label_definitions AS definition
              ON definition.namespace_id = run_label.namespace_id
             AND definition.label_id = run_label.label_id
             AND definition.label_version = run_label.label_version
            WHERE run_label.namespace_id = NEW.namespace_id
              AND run_label.run_id = NEW.run_id
              AND run_label.run_version = NEW.run_version
        ),
        'features', feature_values,
        'evidence', (
            SELECT pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_array(evidence_id, evidence_role)
                ORDER BY evidence_id
            )
            FROM agent_economy.classification_run_evidence
            WHERE namespace_id = NEW.namespace_id
              AND run_id = NEW.run_id
              AND run_version = NEW.run_version
        ),
        'claims', coalesce((
            SELECT pg_catalog.jsonb_agg(
                pg_catalog.jsonb_build_object(
                    'claim_id', claim_id,
                    'claim_version', claim_version,
                    'label_id', label_id,
                    'label_version', label_version,
                    'status', status
                ) ORDER BY label_id, label_version
            )
            FROM agent_economy.classification_run_claims
            WHERE namespace_id = NEW.namespace_id
              AND run_id = NEW.run_id
              AND run_version = NEW.run_version
        ), '[]'::jsonb)
    ) INTO content_document;
    NEW.content_hash := pg_catalog.encode(
        pg_catalog.sha256(pg_catalog.convert_to(content_document::pg_catalog.text, 'UTF8')),
        'hex'
    );
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.validate_classification_replay_drift()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    baseline agent_economy.classification_runs%ROWTYPE;
    replay agent_economy.classification_runs%ROWTYPE;
    expected_added text[];
    expected_removed text[];
BEGIN
    SELECT * INTO STRICT baseline
    FROM agent_economy.classification_runs
    WHERE namespace_id = NEW.namespace_id
      AND run_id = NEW.baseline_run_id
      AND run_version = NEW.baseline_run_version;
    SELECT * INTO STRICT replay
    FROM agent_economy.classification_runs
    WHERE namespace_id = NEW.namespace_id
      AND run_id = NEW.replay_run_id
      AND run_version = NEW.replay_run_version;
    IF baseline.buyer_handle_id IS DISTINCT FROM replay.buyer_handle_id
       OR baseline.cluster_id IS DISTINCT FROM replay.cluster_id
       OR baseline.cluster_version IS DISTINCT FROM replay.cluster_version
       OR baseline.window_start <> replay.window_start
       OR baseline.window_end <> replay.window_end
    THEN
        RAISE EXCEPTION 'classification drift subjects or windows do not match'
            USING ERRCODE = '23514';
    END IF;
    SELECT coalesce(
        pg_catalog.array_agg(
            candidate.label_id || '@' || candidate.label_version::pg_catalog.text
            ORDER BY candidate.label_id, candidate.label_version
        ),
        '{}'::pg_catalog.text[]
    ) INTO expected_added
    FROM agent_economy.classification_run_claims AS candidate
    WHERE candidate.namespace_id = NEW.namespace_id
      AND candidate.run_id = NEW.replay_run_id
      AND candidate.run_version = NEW.replay_run_version
      AND NOT EXISTS (
          SELECT 1
          FROM agent_economy.classification_run_claims AS prior
          WHERE prior.namespace_id = candidate.namespace_id
            AND prior.run_id = NEW.baseline_run_id
            AND prior.run_version = NEW.baseline_run_version
            AND prior.label_id = candidate.label_id
            AND prior.label_version = candidate.label_version
      );
    SELECT coalesce(
        pg_catalog.array_agg(
            prior.label_id || '@' || prior.label_version::pg_catalog.text
            ORDER BY prior.label_id, prior.label_version
        ),
        '{}'::pg_catalog.text[]
    ) INTO expected_removed
    FROM agent_economy.classification_run_claims AS prior
    WHERE prior.namespace_id = NEW.namespace_id
      AND prior.run_id = NEW.baseline_run_id
      AND prior.run_version = NEW.baseline_run_version
      AND NOT EXISTS (
          SELECT 1
          FROM agent_economy.classification_run_claims AS candidate
          WHERE candidate.namespace_id = prior.namespace_id
            AND candidate.run_id = NEW.replay_run_id
            AND candidate.run_version = NEW.replay_run_version
            AND candidate.label_id = prior.label_id
            AND candidate.label_version = prior.label_version
      );
    IF NEW.input_changed <> (baseline.input_snapshot_hash <> replay.input_snapshot_hash)
       OR NEW.added_labels <> expected_added
       OR NEW.removed_labels <> expected_removed
    THEN
        RAISE EXCEPTION 'classification drift does not match sealed replay results'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.lock_classification_series() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.guard_classification_child_append() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.validate_classification_run_seal() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.validate_classification_replay_drift() FROM PUBLIC;

CREATE TRIGGER classification_label_definitions_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_label_definitions
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_label_definitions_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_label_definitions
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER classification_runs_serialize_insert
BEFORE INSERT ON agent_economy.classification_runs
FOR EACH ROW EXECUTE FUNCTION agent_economy.lock_classification_series();
CREATE TRIGGER classification_runs_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_runs
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_runs_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_runs
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER classification_run_label_definitions_guard_insert
BEFORE INSERT ON agent_economy.classification_run_label_definitions
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_classification_child_append();
CREATE TRIGGER classification_run_label_definitions_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_run_label_definitions
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_run_label_definitions_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_run_label_definitions
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER classification_run_features_guard_insert
BEFORE INSERT ON agent_economy.classification_run_features
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_classification_child_append();
CREATE TRIGGER classification_run_features_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_run_features
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_run_features_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_run_features
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER classification_run_evidence_guard_insert
BEFORE INSERT ON agent_economy.classification_run_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_classification_child_append();
CREATE TRIGGER classification_run_evidence_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_run_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_run_evidence_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_run_evidence
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER classification_run_claims_guard_insert
BEFORE INSERT ON agent_economy.classification_run_claims
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_classification_child_append();
CREATE TRIGGER classification_run_claims_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_run_claims
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_run_claims_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_run_claims
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER classification_run_seals_validate_insert
BEFORE INSERT ON agent_economy.classification_run_seals
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_classification_run_seal();
CREATE TRIGGER classification_run_seals_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_run_seals
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_run_seals_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_run_seals
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER classification_replay_drift_validate_insert
BEFORE INSERT ON agent_economy.classification_replay_drift
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_classification_replay_drift();
CREATE TRIGGER classification_replay_drift_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_replay_drift
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_replay_drift_truncate_immutable
BEFORE TRUNCATE ON agent_economy.classification_replay_drift
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE INDEX classification_runs_handle_window_idx
    ON agent_economy.classification_runs (
        namespace_id, buyer_handle_id, window_end DESC
    ) WHERE buyer_handle_id IS NOT NULL;
CREATE INDEX classification_runs_cluster_window_idx
    ON agent_economy.classification_runs (
        namespace_id, cluster_id, cluster_version, window_end DESC
    ) WHERE cluster_id IS NOT NULL;

COMMIT;
