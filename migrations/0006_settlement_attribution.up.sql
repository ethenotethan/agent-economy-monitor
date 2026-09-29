BEGIN;

CREATE TABLE agent_economy.payment_requirements (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    requirement_id text NOT NULL CHECK (requirement_id <> ''),
    payment_option_id text NOT NULL,
    endpoint_id text NOT NULL,
    service_id text NOT NULL,
    evidence_id text NOT NULL,
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, requirement_id),
    UNIQUE (
        namespace_id, requirement_id, payment_option_id, endpoint_id, service_id, evidence_id
    ),
    FOREIGN KEY (namespace_id, payment_option_id)
        REFERENCES agent_economy.payment_options (namespace_id, payment_option_id),
    FOREIGN KEY (namespace_id, endpoint_id)
        REFERENCES agent_economy.endpoints (namespace_id, endpoint_id),
    FOREIGN KEY (namespace_id, service_id)
        REFERENCES agent_economy.services (namespace_id, service_id),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE FUNCTION agent_economy.validate_payment_requirement_hierarchy()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM agent_economy.payment_options AS payment_option
        JOIN agent_economy.offers AS offer
          ON offer.namespace_id = payment_option.namespace_id
         AND offer.offer_id = payment_option.offer_id
        JOIN agent_economy.endpoints AS endpoint
          ON endpoint.namespace_id = offer.namespace_id
         AND endpoint.endpoint_id = offer.endpoint_id
        JOIN agent_economy.provenance_records AS provenance
          ON provenance.namespace_id = NEW.namespace_id
         AND provenance.provenance_id = NEW.provenance_id
        WHERE payment_option.namespace_id = NEW.namespace_id
          AND payment_option.payment_option_id = NEW.payment_option_id
          AND endpoint.endpoint_id = NEW.endpoint_id
          AND endpoint.service_id = NEW.service_id
          AND provenance.evidence_id = NEW.evidence_id
    ) THEN
        RAISE EXCEPTION 'payment requirement hierarchy does not match canonical catalog'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;

CREATE TABLE agent_economy.attribution_runs (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    chain_scope text NOT NULL CHECK (chain_scope <> ''),
    settlement_id text NOT NULL CHECK (settlement_id <> ''),
    attribution_version integer NOT NULL CHECK (attribution_version > 0),
    supersedes_version integer,
    engine_version text NOT NULL CHECK (engine_version <> ''),
    encoding_version text NOT NULL CHECK (encoding_version <> ''),
    match_method text NOT NULL CHECK (
        match_method IN ('explicit_requirement', 'unique_exact', 'shared_exact', 'none')
    ),
    explicit_requirement_id text,
    input_snapshot_hash text NOT NULL CHECK (input_snapshot_hash ~ '^[0-9a-f]{64}$'),
    level text NOT NULL CHECK (level IN ('verified', 'strong', 'weak', 'unknown')),
    settlement_evidence_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, chain_scope, settlement_id, attribution_version),
    CHECK (
        (attribution_version = 1 AND supersedes_version IS NULL)
        OR (attribution_version > 1 AND supersedes_version = attribution_version - 1)
    ),
    CHECK (
        (match_method = 'explicit_requirement' AND explicit_requirement_id IS NOT NULL)
        OR match_method <> 'explicit_requirement'
    ),
    FOREIGN KEY (namespace_id, chain_scope, settlement_id)
        REFERENCES agent_economy.settlements (namespace_id, chain_scope, settlement_id),
    FOREIGN KEY (namespace_id, chain_scope, settlement_id, supersedes_version)
        REFERENCES agent_economy.attribution_runs (
            namespace_id, chain_scope, settlement_id, attribution_version
        ),
    FOREIGN KEY (namespace_id, settlement_evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id)
);

CREATE TABLE agent_economy.attribution_run_evidence (
    namespace_id uuid NOT NULL,
    chain_scope text NOT NULL,
    settlement_id text NOT NULL,
    attribution_version integer NOT NULL,
    evidence_id text NOT NULL,
    evidence_role text NOT NULL CHECK (
        evidence_role IN ('settlement', 'catalog_snapshot', 'supporting', 'conflicting')
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, chain_scope, settlement_id, attribution_version, evidence_id
    ),
    FOREIGN KEY (namespace_id, chain_scope, settlement_id, attribution_version)
        REFERENCES agent_economy.attribution_runs (
            namespace_id, chain_scope, settlement_id, attribution_version
        ),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id)
);

CREATE TABLE agent_economy.attribution_run_requirements (
    namespace_id uuid NOT NULL,
    chain_scope text NOT NULL,
    settlement_id text NOT NULL,
    attribution_version integer NOT NULL,
    requirement_id text NOT NULL,
    payment_option_id text NOT NULL,
    endpoint_id text NOT NULL,
    service_id text NOT NULL,
    requirement_evidence_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, chain_scope, settlement_id, attribution_version, requirement_id
    ),
    UNIQUE (
        namespace_id, chain_scope, settlement_id, attribution_version,
        requirement_id, payment_option_id, endpoint_id, service_id,
        requirement_evidence_id
    ),
    FOREIGN KEY (namespace_id, chain_scope, settlement_id, attribution_version)
        REFERENCES agent_economy.attribution_runs (
            namespace_id, chain_scope, settlement_id, attribution_version
        ),
    FOREIGN KEY (
        namespace_id, requirement_id, payment_option_id, endpoint_id,
        service_id, requirement_evidence_id
    ) REFERENCES agent_economy.payment_requirements (
        namespace_id, requirement_id, payment_option_id, endpoint_id,
        service_id, evidence_id
    )
);

CREATE TABLE agent_economy.attribution_candidates (
    namespace_id uuid NOT NULL,
    chain_scope text NOT NULL,
    settlement_id text NOT NULL,
    attribution_version integer NOT NULL,
    candidate_id text NOT NULL CHECK (candidate_id <> ''),
    requirement_id text NOT NULL,
    payment_option_id text NOT NULL,
    endpoint_id text NOT NULL,
    service_id text NOT NULL,
    confidence numeric(5, 4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    settlement_evidence_id text NOT NULL,
    requirement_evidence_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, chain_scope, settlement_id, attribution_version, candidate_id
    ),
    UNIQUE (
        namespace_id, chain_scope, settlement_id, attribution_version, requirement_id
    ),
    FOREIGN KEY (namespace_id, chain_scope, settlement_id, attribution_version)
        REFERENCES agent_economy.attribution_runs (
            namespace_id, chain_scope, settlement_id, attribution_version
        ),
    FOREIGN KEY (
        namespace_id, chain_scope, settlement_id, attribution_version,
        requirement_id, payment_option_id, endpoint_id, service_id,
        requirement_evidence_id
    ) REFERENCES agent_economy.attribution_run_requirements (
        namespace_id, chain_scope, settlement_id, attribution_version,
        requirement_id, payment_option_id, endpoint_id, service_id,
        requirement_evidence_id
    ),
    FOREIGN KEY (namespace_id, settlement_evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id)
);

CREATE TABLE agent_economy.attribution_candidate_evidence (
    namespace_id uuid NOT NULL,
    chain_scope text NOT NULL,
    settlement_id text NOT NULL,
    attribution_version integer NOT NULL,
    candidate_id text NOT NULL,
    evidence_id text NOT NULL,
    evidence_role text NOT NULL CHECK (
        evidence_role IN ('settlement', 'requirement', 'supporting', 'conflicting')
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, chain_scope, settlement_id, attribution_version,
        candidate_id, evidence_id
    ),
    FOREIGN KEY (
        namespace_id, chain_scope, settlement_id, attribution_version, candidate_id
    ) REFERENCES agent_economy.attribution_candidates (
        namespace_id, chain_scope, settlement_id, attribution_version, candidate_id
    ),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id)
);

CREATE TABLE agent_economy.attribution_run_seals (
    namespace_id uuid NOT NULL,
    chain_scope text NOT NULL,
    settlement_id text NOT NULL,
    attribution_version integer NOT NULL,
    result_encoding bytea NOT NULL,
    state_hash text NOT NULL CHECK (state_hash ~ '^[0-9a-f]{64}$'),
    sealed_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, chain_scope, settlement_id, attribution_version),
    FOREIGN KEY (namespace_id, chain_scope, settlement_id, attribution_version)
        REFERENCES agent_economy.attribution_runs (
            namespace_id, chain_scope, settlement_id, attribution_version
        )
);

CREATE FUNCTION agent_economy.validate_attribution_settlement_evidence()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM agent_economy.settlements AS settlement
        JOIN agent_economy.provenance_records AS provenance
          ON provenance.namespace_id = settlement.namespace_id
         AND provenance.provenance_id = settlement.provenance_id
        WHERE settlement.namespace_id = NEW.namespace_id
          AND settlement.chain_scope = NEW.chain_scope
          AND settlement.settlement_id = NEW.settlement_id
          AND provenance.evidence_id = NEW.settlement_evidence_id
    ) THEN
        RAISE EXCEPTION 'attribution settlement evidence does not support settlement'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.lock_attribution_series()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            NEW.namespace_id::pg_catalog.text || ':' || NEW.chain_scope || ':' || NEW.settlement_id,
            0
        )
    );
    IF NEW.attribution_version > 1 AND NOT EXISTS (
        SELECT 1
        FROM agent_economy.attribution_run_seals
        WHERE namespace_id = NEW.namespace_id
          AND chain_scope = NEW.chain_scope
          AND settlement_id = NEW.settlement_id
          AND attribution_version = NEW.supersedes_version
    ) THEN
        RAISE EXCEPTION 'attribution predecessor must be sealed'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.guard_attribution_child_append()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            NEW.namespace_id::pg_catalog.text || ':' || NEW.chain_scope || ':' || NEW.settlement_id,
            0
        )
    );
    IF EXISTS (
        SELECT 1
        FROM agent_economy.attribution_run_seals
        WHERE namespace_id = NEW.namespace_id
          AND chain_scope = NEW.chain_scope
          AND settlement_id = NEW.settlement_id
          AND attribution_version = NEW.attribution_version
    ) THEN
        RAISE EXCEPTION 'attribution run is sealed'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.validate_attribution_run_seal()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
DECLARE
    run_level text;
    run_method text;
    explicit_requirement text;
    settlement_evidence text;
    candidate_count bigint;
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            NEW.namespace_id::pg_catalog.text || ':' || NEW.chain_scope || ':' || NEW.settlement_id,
            0
        )
    );
    IF NEW.state_hash <> pg_catalog.encode(pg_catalog.sha256(NEW.result_encoding), 'hex') THEN
        RAISE EXCEPTION 'attribution state hash does not match result encoding'
            USING ERRCODE = '23514';
    END IF;

    SELECT level, match_method, explicit_requirement_id, settlement_evidence_id
      INTO STRICT run_level, run_method, explicit_requirement, settlement_evidence
      FROM agent_economy.attribution_runs
     WHERE namespace_id = NEW.namespace_id
       AND chain_scope = NEW.chain_scope
       AND settlement_id = NEW.settlement_id
       AND attribution_version = NEW.attribution_version;
    SELECT pg_catalog.count(*) INTO candidate_count
      FROM agent_economy.attribution_candidates
     WHERE namespace_id = NEW.namespace_id
       AND chain_scope = NEW.chain_scope
       AND settlement_id = NEW.settlement_id
       AND attribution_version = NEW.attribution_version;

    IF (run_level = 'unknown' AND (run_method <> 'none' OR candidate_count <> 0))
       OR (run_level = 'verified' AND (
            run_method <> 'explicit_requirement' OR candidate_count <> 1
            OR NOT EXISTS (
                SELECT 1 FROM agent_economy.attribution_candidates
                 WHERE namespace_id = NEW.namespace_id
                   AND chain_scope = NEW.chain_scope
                   AND settlement_id = NEW.settlement_id
                   AND attribution_version = NEW.attribution_version
                   AND requirement_id = explicit_requirement
                   AND confidence = 1.0000
            )
       ))
       OR (run_level = 'strong' AND (
            run_method <> 'unique_exact' OR candidate_count <> 1
            OR NOT EXISTS (
                SELECT 1 FROM agent_economy.attribution_candidates
                 WHERE namespace_id = NEW.namespace_id
                   AND chain_scope = NEW.chain_scope
                   AND settlement_id = NEW.settlement_id
                   AND attribution_version = NEW.attribution_version
                   AND confidence = 0.8500
            )
       ))
       OR (run_level = 'weak' AND (
            run_method <> 'shared_exact' OR candidate_count < 2
            OR EXISTS (
                SELECT 1 FROM agent_economy.attribution_candidates
                 WHERE namespace_id = NEW.namespace_id
                   AND chain_scope = NEW.chain_scope
                   AND settlement_id = NEW.settlement_id
                   AND attribution_version = NEW.attribution_version
                   AND confidence <> 0.5000
            )
       ))
    THEN
        RAISE EXCEPTION 'attribution level, method, candidate count, or confidence is inconsistent'
            USING ERRCODE = '23514';
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM agent_economy.attribution_run_evidence
         WHERE namespace_id = NEW.namespace_id
           AND chain_scope = NEW.chain_scope
           AND settlement_id = NEW.settlement_id
           AND attribution_version = NEW.attribution_version
           AND evidence_id = settlement_evidence
           AND evidence_role = 'settlement'
    ) OR EXISTS (
        SELECT 1 FROM agent_economy.attribution_run_requirements AS requirement
         WHERE requirement.namespace_id = NEW.namespace_id
           AND requirement.chain_scope = NEW.chain_scope
           AND requirement.settlement_id = NEW.settlement_id
           AND requirement.attribution_version = NEW.attribution_version
           AND NOT EXISTS (
                SELECT 1 FROM agent_economy.attribution_run_evidence AS evidence
                 WHERE evidence.namespace_id = requirement.namespace_id
                   AND evidence.chain_scope = requirement.chain_scope
                   AND evidence.settlement_id = requirement.settlement_id
                   AND evidence.attribution_version = requirement.attribution_version
                   AND evidence.evidence_id = requirement.requirement_evidence_id
                   AND evidence.evidence_role = 'catalog_snapshot'
           )
    ) OR EXISTS (
        SELECT 1 FROM agent_economy.attribution_candidates AS candidate
         WHERE candidate.namespace_id = NEW.namespace_id
           AND candidate.chain_scope = NEW.chain_scope
           AND candidate.settlement_id = NEW.settlement_id
           AND candidate.attribution_version = NEW.attribution_version
           AND (
                NOT EXISTS (
                    SELECT 1 FROM agent_economy.attribution_candidate_evidence AS evidence
                     WHERE evidence.namespace_id = candidate.namespace_id
                       AND evidence.chain_scope = candidate.chain_scope
                       AND evidence.settlement_id = candidate.settlement_id
                       AND evidence.attribution_version = candidate.attribution_version
                       AND evidence.candidate_id = candidate.candidate_id
                       AND evidence.evidence_id = candidate.settlement_evidence_id
                       AND evidence.evidence_role = 'settlement'
                ) OR NOT EXISTS (
                    SELECT 1 FROM agent_economy.attribution_candidate_evidence AS evidence
                     WHERE evidence.namespace_id = candidate.namespace_id
                       AND evidence.chain_scope = candidate.chain_scope
                       AND evidence.settlement_id = candidate.settlement_id
                       AND evidence.attribution_version = candidate.attribution_version
                       AND evidence.candidate_id = candidate.candidate_id
                       AND evidence.evidence_id = candidate.requirement_evidence_id
                       AND evidence.evidence_role = 'requirement'
                )
           )
    ) THEN
        RAISE EXCEPTION 'attribution run evidence is incomplete or mislabelled'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.validate_payment_requirement_hierarchy() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.validate_attribution_settlement_evidence() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.lock_attribution_series() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.guard_attribution_child_append() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.validate_attribution_run_seal() FROM PUBLIC;

CREATE TRIGGER payment_requirements_validate_insert
BEFORE INSERT ON agent_economy.payment_requirements
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_payment_requirement_hierarchy();
CREATE TRIGGER payment_requirements_immutable
BEFORE UPDATE OR DELETE ON agent_economy.payment_requirements
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER payment_requirements_truncate_immutable
BEFORE TRUNCATE ON agent_economy.payment_requirements
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER attribution_runs_serialize_insert
BEFORE INSERT ON agent_economy.attribution_runs
FOR EACH ROW EXECUTE FUNCTION agent_economy.lock_attribution_series();
CREATE TRIGGER attribution_runs_validate_evidence_insert
BEFORE INSERT ON agent_economy.attribution_runs
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_attribution_settlement_evidence();
CREATE TRIGGER attribution_runs_immutable
BEFORE UPDATE OR DELETE ON agent_economy.attribution_runs
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER attribution_runs_truncate_immutable
BEFORE TRUNCATE ON agent_economy.attribution_runs
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER attribution_run_evidence_guard_insert
BEFORE INSERT ON agent_economy.attribution_run_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_attribution_child_append();
CREATE TRIGGER attribution_run_evidence_immutable
BEFORE UPDATE OR DELETE ON agent_economy.attribution_run_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER attribution_run_evidence_truncate_immutable
BEFORE TRUNCATE ON agent_economy.attribution_run_evidence
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER attribution_run_requirements_guard_insert
BEFORE INSERT ON agent_economy.attribution_run_requirements
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_attribution_child_append();
CREATE TRIGGER attribution_run_requirements_immutable
BEFORE UPDATE OR DELETE ON agent_economy.attribution_run_requirements
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER attribution_run_requirements_truncate_immutable
BEFORE TRUNCATE ON agent_economy.attribution_run_requirements
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER attribution_candidates_guard_insert
BEFORE INSERT ON agent_economy.attribution_candidates
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_attribution_child_append();
CREATE TRIGGER attribution_candidates_validate_evidence_insert
BEFORE INSERT ON agent_economy.attribution_candidates
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_attribution_settlement_evidence();
CREATE TRIGGER attribution_candidates_immutable
BEFORE UPDATE OR DELETE ON agent_economy.attribution_candidates
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER attribution_candidates_truncate_immutable
BEFORE TRUNCATE ON agent_economy.attribution_candidates
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER attribution_candidate_evidence_guard_insert
BEFORE INSERT ON agent_economy.attribution_candidate_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_attribution_child_append();
CREATE TRIGGER attribution_candidate_evidence_immutable
BEFORE UPDATE OR DELETE ON agent_economy.attribution_candidate_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER attribution_candidate_evidence_truncate_immutable
BEFORE TRUNCATE ON agent_economy.attribution_candidate_evidence
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER attribution_run_seals_validate_insert
BEFORE INSERT ON agent_economy.attribution_run_seals
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_attribution_run_seal();
CREATE TRIGGER attribution_run_seals_immutable
BEFORE UPDATE OR DELETE ON agent_economy.attribution_run_seals
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER attribution_run_seals_truncate_immutable
BEFORE TRUNCATE ON agent_economy.attribution_run_seals
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE INDEX attribution_runs_settlement_idx
    ON agent_economy.attribution_runs (
        namespace_id, chain_scope, settlement_id, attribution_version DESC
    );
CREATE INDEX attribution_candidates_service_idx
    ON agent_economy.attribution_candidates (
        namespace_id, service_id, chain_scope, settlement_id
    );
CREATE INDEX attribution_candidates_endpoint_idx
    ON agent_economy.attribution_candidates (namespace_id, endpoint_id);

COMMIT;
