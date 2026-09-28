BEGIN;

ALTER TABLE agent_economy.provenance_records
    ADD CONSTRAINT provenance_records_observed_at_key
    UNIQUE (namespace_id, provenance_id, observed_at);

CREATE TABLE agent_economy.catalog_candidates (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    candidate_id text NOT NULL CHECK (candidate_id ~ '^candidate:sha256:[0-9a-f]{64}$'),
    protocol text NOT NULL CHECK (protocol IN ('x402', 'mpp')),
    endpoint_uri text NOT NULL CHECK (endpoint_uri <> ''),
    http_method text NOT NULL CHECK (http_method ~ '^[A-Z]+$'),
    display_name text NOT NULL CHECK (display_name <> ''),
    discovered_at timestamptz NOT NULL,
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, candidate_id),
    UNIQUE (namespace_id, protocol, http_method, endpoint_uri),
    FOREIGN KEY (namespace_id, provenance_id, discovered_at)
        REFERENCES agent_economy.provenance_records (
            namespace_id, provenance_id, observed_at
        )
);

CREATE TABLE agent_economy.catalog_candidate_aliases (
    namespace_id uuid NOT NULL,
    candidate_id text NOT NULL,
    alias_kind text NOT NULL CHECK (alias_kind IN ('x402scan', 'agentcash', 'discovery')),
    alias_source_id text NOT NULL CHECK (alias_source_id <> ''),
    upstream_id text NOT NULL CHECK (upstream_id <> ''),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, candidate_id, alias_source_id, upstream_id),
    UNIQUE (namespace_id, alias_source_id, upstream_id),
    FOREIGN KEY (namespace_id, candidate_id)
        REFERENCES agent_economy.catalog_candidates (namespace_id, candidate_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.catalog_verification_signals (
    namespace_id uuid NOT NULL,
    candidate_id text NOT NULL,
    signal_id text NOT NULL CHECK (signal_id ~ '^signal:sha256:[0-9a-f]{64}$'),
    verification_kind text NOT NULL CHECK (verification_kind IN ('metadata', 'runtime', 'settlement')),
    verifier_scope text NOT NULL CHECK (verifier_scope <> ''),
    observed_at timestamptz NOT NULL,
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, candidate_id, signal_id),
    UNIQUE (namespace_id, candidate_id, verification_kind, verifier_scope, provenance_id),
    FOREIGN KEY (namespace_id, candidate_id)
        REFERENCES agent_economy.catalog_candidates (namespace_id, candidate_id),
    FOREIGN KEY (namespace_id, provenance_id, observed_at)
        REFERENCES agent_economy.provenance_records (
            namespace_id, provenance_id, observed_at
        )
);

CREATE TABLE agent_economy.catalog_liveness_checks (
    namespace_id uuid NOT NULL,
    candidate_id text NOT NULL,
    check_id text NOT NULL CHECK (check_id ~ '^check:sha256:[0-9a-f]{64}$'),
    checked_at timestamptz NOT NULL,
    outcome text NOT NULL CHECK (outcome IN ('live', 'dead')),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, candidate_id, check_id),
    FOREIGN KEY (namespace_id, candidate_id)
        REFERENCES agent_economy.catalog_candidates (namespace_id, candidate_id),
    FOREIGN KEY (namespace_id, provenance_id, checked_at)
        REFERENCES agent_economy.provenance_records (
            namespace_id, provenance_id, observed_at
        )
);

CREATE TABLE agent_economy.catalog_promotions (
    namespace_id uuid NOT NULL,
    candidate_id text NOT NULL,
    service_id text NOT NULL,
    endpoint_id text NOT NULL,
    policy_version text NOT NULL CHECK (policy_version = 'two-signal-v1'),
    promoted_at timestamptz NOT NULL,
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, candidate_id),
    UNIQUE (namespace_id, service_id, endpoint_id),
    FOREIGN KEY (namespace_id, candidate_id)
        REFERENCES agent_economy.catalog_candidates (namespace_id, candidate_id),
    FOREIGN KEY (namespace_id, service_id)
        REFERENCES agent_economy.services (namespace_id, service_id),
    FOREIGN KEY (namespace_id, endpoint_id)
        REFERENCES agent_economy.endpoints (namespace_id, endpoint_id),
    FOREIGN KEY (namespace_id, provenance_id, promoted_at)
        REFERENCES agent_economy.provenance_records (
            namespace_id, provenance_id, observed_at
        )
);

CREATE FUNCTION agent_economy.guard_catalog_alias_source()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, agent_economy
AS $function$
BEGIN
    IF NOT EXISTS (
           SELECT 1
           FROM agent_economy.provenance_records AS provenance
           WHERE provenance.namespace_id = NEW.namespace_id
             AND provenance.provenance_id = NEW.provenance_id
             AND provenance.source_id = NEW.alias_source_id
       )
    THEN
        RAISE EXCEPTION 'catalog alias source does not match provenance'
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.guard_catalog_alias_source() FROM PUBLIC;

CREATE TRIGGER catalog_candidate_aliases_source_policy
BEFORE INSERT ON agent_economy.catalog_candidate_aliases
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_catalog_alias_source();

CREATE FUNCTION agent_economy.guard_direct_verified_service()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    IF NEW.trust_state = 'verified' THEN
        RAISE EXCEPTION 'verified catalog state requires promotion'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.guard_direct_verified_service() FROM PUBLIC;

CREATE TRIGGER services_verified_require_promotion
BEFORE INSERT ON agent_economy.services
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_direct_verified_service();

CREATE FUNCTION agent_economy.guard_catalog_promotion()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, agent_economy
AS $function$
DECLARE
    signal_kind_count bigint;
    signal_source_count bigint;
BEGIN
    SELECT
        count(DISTINCT signal.verification_kind),
        count(DISTINCT provenance.source_id)
    INTO signal_kind_count, signal_source_count
    FROM agent_economy.catalog_verification_signals AS signal
    JOIN agent_economy.provenance_records AS provenance
      ON provenance.namespace_id = signal.namespace_id
     AND provenance.provenance_id = signal.provenance_id
    WHERE signal.namespace_id = NEW.namespace_id
      AND signal.candidate_id = NEW.candidate_id
      AND signal.observed_at <= NEW.promoted_at;

    IF signal_kind_count < 2 OR signal_source_count < 2 THEN
        RAISE EXCEPTION 'catalog promotion requires two independent verification signal kinds'
            USING ERRCODE = '55000';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM agent_economy.catalog_candidates AS candidate
        JOIN agent_economy.endpoints AS endpoint
          ON endpoint.namespace_id = candidate.namespace_id
         AND endpoint.endpoint_id = NEW.endpoint_id
         AND endpoint.service_id = NEW.service_id
         AND endpoint.http_method = candidate.http_method
         AND endpoint.endpoint_uri = candidate.endpoint_uri
        JOIN agent_economy.services AS service
          ON service.namespace_id = endpoint.namespace_id
         AND service.service_id = endpoint.service_id
         AND service.trust_state = 'observed'
        WHERE candidate.namespace_id = NEW.namespace_id
          AND candidate.candidate_id = NEW.candidate_id
    ) THEN
        RAISE EXCEPTION 'catalog promotion target does not match the verified candidate endpoint'
            USING ERRCODE = '55000';
    END IF;

    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.guard_catalog_promotion() FROM PUBLIC;

CREATE TRIGGER catalog_promotions_two_signal_policy
BEFORE INSERT ON agent_economy.catalog_promotions
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_catalog_promotion();

CREATE FUNCTION agent_economy.catalog_candidate_health(
    requested_namespace uuid,
    requested_candidate text,
    as_of timestamptz
)
RETURNS text
LANGUAGE sql
STABLE
SET search_path = pg_catalog, agent_economy
AS $function$
    SELECT CASE
        WHEN count(*) = 0 THEN 'unseen'
        WHEN (array_agg(recent.outcome ORDER BY recent.checked_at DESC, recent.check_id DESC))[1] = 'live'
            THEN 'live'
        WHEN count(*) = 3
         AND (array_agg(recent.outcome ORDER BY recent.checked_at DESC, recent.check_id DESC))[1:3]
             = ARRAY['dead', 'dead', 'dead']::text[]
            THEN 'dead'
        ELSE 'stale'
    END
    FROM (
        SELECT checked_at, check_id, outcome
        FROM agent_economy.catalog_liveness_checks
        WHERE namespace_id = requested_namespace
          AND candidate_id = requested_candidate
          AND checked_at <= as_of
        ORDER BY checked_at DESC, check_id DESC
        LIMIT 3
    ) AS recent
$function$;

REVOKE ALL ON FUNCTION agent_economy.catalog_candidate_health(uuid, text, timestamptz)
    FROM PUBLIC;

CREATE TRIGGER catalog_candidates_immutable
BEFORE UPDATE OR DELETE ON agent_economy.catalog_candidates
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER catalog_candidate_aliases_immutable
BEFORE UPDATE OR DELETE ON agent_economy.catalog_candidate_aliases
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER catalog_verification_signals_immutable
BEFORE UPDATE OR DELETE ON agent_economy.catalog_verification_signals
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER catalog_liveness_checks_immutable
BEFORE UPDATE OR DELETE ON agent_economy.catalog_liveness_checks
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER catalog_promotions_immutable
BEFORE UPDATE OR DELETE ON agent_economy.catalog_promotions
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER catalog_candidates_truncate_immutable
BEFORE TRUNCATE ON agent_economy.catalog_candidates
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER catalog_candidate_aliases_truncate_immutable
BEFORE TRUNCATE ON agent_economy.catalog_candidate_aliases
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER catalog_verification_signals_truncate_immutable
BEFORE TRUNCATE ON agent_economy.catalog_verification_signals
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER catalog_liveness_checks_truncate_immutable
BEFORE TRUNCATE ON agent_economy.catalog_liveness_checks
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER catalog_promotions_truncate_immutable
BEFORE TRUNCATE ON agent_economy.catalog_promotions
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE VIEW agent_economy.shadow_catalog AS
SELECT
    candidate.namespace_id,
    candidate.candidate_id,
    candidate.protocol,
    candidate.endpoint_uri,
    candidate.http_method,
    candidate.display_name,
    candidate.discovered_at,
    agent_economy.catalog_candidate_health(
        candidate.namespace_id,
        candidate.candidate_id,
        statement_timestamp()
    ) AS health_status,
    (
        SELECT count(*)
        FROM agent_economy.catalog_liveness_checks AS history
        WHERE history.namespace_id = candidate.namespace_id
          AND history.candidate_id = candidate.candidate_id
    ) AS liveness_check_count,
    (
        SELECT max(history.checked_at)
        FROM agent_economy.catalog_liveness_checks AS history
        WHERE history.namespace_id = candidate.namespace_id
          AND history.candidate_id = candidate.candidate_id
    ) AS last_checked_at
FROM agent_economy.catalog_candidates AS candidate
LEFT JOIN agent_economy.catalog_promotions AS promotion
  ON promotion.namespace_id = candidate.namespace_id
 AND promotion.candidate_id = candidate.candidate_id
WHERE promotion.candidate_id IS NULL;

CREATE VIEW agent_economy.verified_catalog AS
SELECT
    promotion.namespace_id,
    promotion.candidate_id,
    candidate.protocol,
    service.service_id,
    service.display_name,
    'verified'::text AS trust_state,
    endpoint.endpoint_id,
    endpoint.http_method,
    endpoint.endpoint_uri,
    promotion.policy_version,
    promotion.promoted_at,
    agent_economy.catalog_candidate_health(
        promotion.namespace_id,
        promotion.candidate_id,
        statement_timestamp()
    ) AS health_status
FROM agent_economy.catalog_promotions AS promotion
JOIN agent_economy.catalog_candidates AS candidate
  ON candidate.namespace_id = promotion.namespace_id
 AND candidate.candidate_id = promotion.candidate_id
JOIN agent_economy.services AS service
  ON service.namespace_id = promotion.namespace_id
 AND service.service_id = promotion.service_id
JOIN agent_economy.endpoints AS endpoint
  ON endpoint.namespace_id = promotion.namespace_id
 AND endpoint.endpoint_id = promotion.endpoint_id
 AND endpoint.service_id = promotion.service_id;

CREATE INDEX catalog_verification_signals_candidate_idx
    ON agent_economy.catalog_verification_signals
       (namespace_id, candidate_id, observed_at DESC);
CREATE INDEX catalog_liveness_checks_candidate_idx
    ON agent_economy.catalog_liveness_checks
       (namespace_id, candidate_id, checked_at DESC);

COMMIT;
