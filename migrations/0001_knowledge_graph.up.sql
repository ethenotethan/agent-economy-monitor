BEGIN;

CREATE SCHEMA agent_economy;

CREATE TABLE agent_economy.namespaces (
    namespace_id uuid PRIMARY KEY,
    namespace_kind text NOT NULL CHECK (namespace_kind <> ''),
    namespace_key text NOT NULL CHECK (namespace_key <> ''),
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (namespace_kind, namespace_key)
);

CREATE TABLE agent_economy.evidence_objects (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    evidence_id text NOT NULL CHECK (evidence_id <> ''),
    sha256 text NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    storage_uri text NOT NULL CHECK (storage_uri <> ''),
    media_type text NOT NULL CHECK (media_type <> ''),
    byte_length bigint NOT NULL CHECK (byte_length >= 0),
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, evidence_id),
    UNIQUE (namespace_id, sha256)
);

CREATE TABLE agent_economy.provenance_records (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    provenance_id uuid NOT NULL,
    source_id text NOT NULL CHECK (source_id <> ''),
    observed_at timestamptz NOT NULL,
    parser_version text NOT NULL CHECK (parser_version <> ''),
    provider text,
    chain_scope text,
    block_reference text,
    transaction_reference text,
    finality text,
    evidence_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, provenance_id),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id)
);

CREATE FUNCTION agent_economy.reject_immutable_mutation()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    RAISE EXCEPTION 'canonical evidence and knowledge rows are immutable'
        USING ERRCODE = '55000';
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.reject_immutable_mutation() FROM PUBLIC;

CREATE TRIGGER evidence_objects_immutable
BEFORE UPDATE OR DELETE ON agent_economy.evidence_objects
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TRIGGER provenance_records_immutable
BEFORE UPDATE OR DELETE ON agent_economy.provenance_records
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TABLE agent_economy.services (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    service_id text NOT NULL CHECK (service_id <> ''),
    display_name text NOT NULL CHECK (display_name <> ''),
    trust_state text NOT NULL CHECK (trust_state IN ('candidate', 'observed', 'verified', 'stale', 'disputed')),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, service_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.endpoints (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    endpoint_id text NOT NULL CHECK (endpoint_id <> ''),
    service_id text NOT NULL,
    http_method text NOT NULL CHECK (http_method <> ''),
    endpoint_uri text NOT NULL CHECK (endpoint_uri <> ''),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, endpoint_id),
    UNIQUE (namespace_id, service_id, http_method, endpoint_uri),
    FOREIGN KEY (namespace_id, service_id)
        REFERENCES agent_economy.services (namespace_id, service_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.offers (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    offer_id text NOT NULL CHECK (offer_id <> ''),
    endpoint_id text NOT NULL,
    offer_version integer NOT NULL CHECK (offer_version > 0),
    description text,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, offer_id),
    UNIQUE (namespace_id, endpoint_id, offer_version),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    FOREIGN KEY (namespace_id, endpoint_id)
        REFERENCES agent_economy.endpoints (namespace_id, endpoint_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.payment_options (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    payment_option_id text NOT NULL CHECK (payment_option_id <> ''),
    offer_id text NOT NULL,
    protocol text NOT NULL CHECK (protocol IN ('x402', 'mpp')),
    network text NOT NULL CHECK (network <> ''),
    asset text NOT NULL CHECK (asset <> ''),
    amount_atomic numeric(78, 0) CHECK (amount_atomic >= 0),
    payment_scheme text,
    pay_to text,
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, payment_option_id),
    FOREIGN KEY (namespace_id, offer_id)
        REFERENCES agent_economy.offers (namespace_id, offer_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.buyer_handles (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    buyer_handle_id text NOT NULL CHECK (buyer_handle_id <> ''),
    handle_kind text NOT NULL CHECK (handle_kind IN ('wallet', 'public_key', 'did', 'facilitator')),
    chain_scope text NOT NULL CHECK (chain_scope <> ''),
    handle_value text NOT NULL CHECK (handle_value <> ''),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, buyer_handle_id),
    UNIQUE (namespace_id, handle_kind, chain_scope, handle_value),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.buyer_clusters (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    cluster_id uuid NOT NULL,
    display_name text NOT NULL CHECK (display_name <> ''),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, cluster_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.buyer_cluster_versions (
    namespace_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    version integer NOT NULL CHECK (version > 0),
    supersedes_version integer,
    reverts_version integer,
    status text NOT NULL CHECK (status IN ('proposed', 'accepted', 'reverted', 'disputed')),
    method text NOT NULL CHECK (method <> ''),
    confidence numeric(5, 4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, cluster_id, version),
    CHECK (
        (version = 1 AND supersedes_version IS NULL)
        OR (version > 1 AND supersedes_version = version - 1)
    ),
    CHECK (
        (status = 'reverted' AND reverts_version IS NOT NULL)
        OR (status <> 'reverted' AND reverts_version IS NULL)
    ),
    CHECK (reverts_version IS NULL OR reverts_version < version),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    FOREIGN KEY (namespace_id, cluster_id)
        REFERENCES agent_economy.buyer_clusters (namespace_id, cluster_id),
    FOREIGN KEY (namespace_id, cluster_id, supersedes_version)
        REFERENCES agent_economy.buyer_cluster_versions (namespace_id, cluster_id, version),
    FOREIGN KEY (namespace_id, cluster_id, reverts_version)
        REFERENCES agent_economy.buyer_cluster_versions (namespace_id, cluster_id, version),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.buyer_cluster_memberships (
    namespace_id uuid NOT NULL,
    cluster_id uuid NOT NULL,
    cluster_version integer NOT NULL,
    buyer_handle_id text NOT NULL,
    membership_action text NOT NULL CHECK (membership_action IN ('add', 'remove')),
    method text NOT NULL CHECK (method <> ''),
    confidence numeric(5, 4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, cluster_id, cluster_version, buyer_handle_id),
    FOREIGN KEY (namespace_id, cluster_id, cluster_version)
        REFERENCES agent_economy.buyer_cluster_versions (namespace_id, cluster_id, version),
    FOREIGN KEY (namespace_id, buyer_handle_id)
        REFERENCES agent_economy.buyer_handles (namespace_id, buyer_handle_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.attribution_edges (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    attribution_id text NOT NULL CHECK (attribution_id <> ''),
    settlement_event_id text NOT NULL CHECK (settlement_event_id <> ''),
    service_id text,
    endpoint_id text,
    payment_option_id text,
    method text NOT NULL CHECK (method <> ''),
    confidence numeric(5, 4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    status text NOT NULL CHECK (status IN ('verified', 'inferred', 'disputed')),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, attribution_id),
    CHECK (num_nonnulls(service_id, endpoint_id, payment_option_id) = 1),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    FOREIGN KEY (namespace_id, service_id)
        REFERENCES agent_economy.services (namespace_id, service_id),
    FOREIGN KEY (namespace_id, endpoint_id)
        REFERENCES agent_economy.endpoints (namespace_id, endpoint_id),
    FOREIGN KEY (namespace_id, payment_option_id)
        REFERENCES agent_economy.payment_options (namespace_id, payment_option_id),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.classification_claims (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    claim_id text NOT NULL CHECK (claim_id <> ''),
    version integer NOT NULL CHECK (version > 0),
    supersedes_version integer,
    buyer_handle_id text,
    cluster_id uuid,
    label text NOT NULL CHECK (label <> ''),
    method text NOT NULL CHECK (method <> ''),
    confidence numeric(5, 4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1),
    evidence_window_start timestamptz NOT NULL,
    evidence_window_end timestamptz NOT NULL,
    valid_from timestamptz NOT NULL,
    valid_to timestamptz,
    status text NOT NULL CHECK (status IN ('verified', 'inferred', 'disputed')),
    provenance_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, claim_id, version),
    CHECK (
        (version = 1 AND supersedes_version IS NULL)
        OR (version > 1 AND supersedes_version = version - 1)
    ),
    CHECK (num_nonnulls(buyer_handle_id, cluster_id) = 1),
    CHECK (evidence_window_end > evidence_window_start),
    CHECK (valid_to IS NULL OR valid_to > valid_from),
    FOREIGN KEY (namespace_id, buyer_handle_id)
        REFERENCES agent_economy.buyer_handles (namespace_id, buyer_handle_id),
    FOREIGN KEY (namespace_id, cluster_id)
        REFERENCES agent_economy.buyer_clusters (namespace_id, cluster_id),
    FOREIGN KEY (namespace_id, claim_id, supersedes_version)
        REFERENCES agent_economy.classification_claims (namespace_id, claim_id, version),
    FOREIGN KEY (namespace_id, provenance_id)
        REFERENCES agent_economy.provenance_records (namespace_id, provenance_id)
);

CREATE TABLE agent_economy.classification_claim_evidence (
    namespace_id uuid NOT NULL,
    claim_id text NOT NULL,
    claim_version integer NOT NULL,
    evidence_id text NOT NULL,
    evidence_role text NOT NULL CHECK (evidence_role IN ('supporting', 'conflicting')),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, claim_id, claim_version, evidence_id),
    FOREIGN KEY (namespace_id, claim_id, claim_version)
        REFERENCES agent_economy.classification_claims (namespace_id, claim_id, version),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id)
);

CREATE INDEX attribution_edges_settlement_idx
    ON agent_economy.attribution_edges (namespace_id, settlement_event_id);
CREATE INDEX classification_claims_handle_idx
    ON agent_economy.classification_claims (namespace_id, buyer_handle_id)
    WHERE buyer_handle_id IS NOT NULL;
CREATE INDEX classification_claims_cluster_idx
    ON agent_economy.classification_claims (namespace_id, cluster_id)
    WHERE cluster_id IS NOT NULL;

CREATE FUNCTION agent_economy.lock_cluster_series()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM 1
    FROM agent_economy.buyer_clusters
    WHERE namespace_id = NEW.namespace_id AND cluster_id = NEW.cluster_id
    FOR UPDATE;
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.guard_cluster_membership_append()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM 1
    FROM agent_economy.buyer_clusters
    WHERE namespace_id = NEW.namespace_id AND cluster_id = NEW.cluster_id
    FOR UPDATE;
    IF EXISTS (
        SELECT 1
        FROM agent_economy.buyer_cluster_versions
        WHERE namespace_id = NEW.namespace_id
          AND cluster_id = NEW.cluster_id
          AND version > NEW.cluster_version
    ) THEN
        RAISE EXCEPTION 'cluster version is sealed'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.lock_claim_series()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(NEW.namespace_id::pg_catalog.text || ':' || NEW.claim_id, 0)
    );
    RETURN NEW;
END
$function$;

CREATE FUNCTION agent_economy.guard_claim_evidence_append()
RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $function$
BEGIN
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(NEW.namespace_id::pg_catalog.text || ':' || NEW.claim_id, 0)
    );
    IF EXISTS (
        SELECT 1
        FROM agent_economy.classification_claims
        WHERE namespace_id = NEW.namespace_id
          AND claim_id = NEW.claim_id
          AND version > NEW.claim_version
    ) THEN
        RAISE EXCEPTION 'claim version is sealed'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.lock_cluster_series() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.guard_cluster_membership_append() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.lock_claim_series() FROM PUBLIC;
REVOKE ALL ON FUNCTION agent_economy.guard_claim_evidence_append() FROM PUBLIC;

CREATE TRIGGER namespaces_immutable
BEFORE UPDATE OR DELETE ON agent_economy.namespaces
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER services_immutable
BEFORE UPDATE OR DELETE ON agent_economy.services
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER endpoints_immutable
BEFORE UPDATE OR DELETE ON agent_economy.endpoints
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER offers_immutable
BEFORE UPDATE OR DELETE ON agent_economy.offers
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER payment_options_immutable
BEFORE UPDATE OR DELETE ON agent_economy.payment_options
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER buyer_handles_immutable
BEFORE UPDATE OR DELETE ON agent_economy.buyer_handles
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER buyer_clusters_immutable
BEFORE UPDATE OR DELETE ON agent_economy.buyer_clusters
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER buyer_cluster_versions_immutable
BEFORE UPDATE OR DELETE ON agent_economy.buyer_cluster_versions
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER buyer_cluster_versions_serialize_insert
BEFORE INSERT ON agent_economy.buyer_cluster_versions
FOR EACH ROW EXECUTE FUNCTION agent_economy.lock_cluster_series();
CREATE TRIGGER buyer_cluster_memberships_immutable
BEFORE UPDATE OR DELETE ON agent_economy.buyer_cluster_memberships
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER buyer_cluster_memberships_guard_insert
BEFORE INSERT ON agent_economy.buyer_cluster_memberships
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_cluster_membership_append();
CREATE TRIGGER attribution_edges_immutable
BEFORE UPDATE OR DELETE ON agent_economy.attribution_edges
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_claims_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_claims
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_claims_serialize_insert
BEFORE INSERT ON agent_economy.classification_claims
FOR EACH ROW EXECUTE FUNCTION agent_economy.lock_claim_series();
CREATE TRIGGER classification_claim_evidence_immutable
BEFORE UPDATE OR DELETE ON agent_economy.classification_claim_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER classification_claim_evidence_guard_insert
BEFORE INSERT ON agent_economy.classification_claim_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.guard_claim_evidence_append();

COMMIT;
