BEGIN;

CREATE TABLE agent_economy.projection_snapshots (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    snapshot_id uuid NOT NULL,
    stable_entity_id text NOT NULL CHECK (stable_entity_id <> ''),
    snapshot_sha256 text NOT NULL CHECK (snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    storage_uri text NOT NULL CHECK (storage_uri ~ '^gcs://'),
    byte_length bigint NOT NULL CHECK (byte_length >= 0),
    snapshot_payload jsonb NOT NULL CHECK (
        jsonb_typeof(snapshot_payload) = 'object'
        AND snapshot_payload ?& ARRAY['bytes', 'sha256', 'projection_input', 'private_fragments']
        AND snapshot_payload ->> 'sha256' = snapshot_sha256
        AND jsonb_typeof(snapshot_payload -> 'bytes') = 'array'
        AND jsonb_typeof(snapshot_payload -> 'projection_input') = 'object'
        AND jsonb_typeof(snapshot_payload -> 'private_fragments') = 'array'
    ),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, snapshot_id),
    UNIQUE (namespace_id, snapshot_sha256),
    UNIQUE (namespace_id, snapshot_id, stable_entity_id, snapshot_sha256)
);

CREATE TRIGGER projection_snapshots_immutable_projection_row
BEFORE UPDATE OR DELETE ON agent_economy.projection_snapshots
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER projection_snapshots_immutable_projection_truncate
BEFORE TRUNCATE ON agent_economy.projection_snapshots
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TABLE agent_economy.projection_jobs (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    job_id uuid NOT NULL,
    snapshot_id uuid NOT NULL,
    stable_entity_id text NOT NULL CHECK (stable_entity_id <> ''),
    wiki_id text NOT NULL DEFAULT 'agentic-commerce',
    page_path text NOT NULL CHECK (page_path ~ '^(buyers|services|investigations|protocols)/[^:]+\.md$'),
    model_id text NOT NULL CHECK (model_id <> ''),
    model_sha256 text NOT NULL CHECK (model_sha256 ~ '^[0-9a-f]{64}$'),
    prompt_sha256 text NOT NULL CHECK (prompt_sha256 ~ '^[0-9a-f]{64}$'),
    snapshot_sha256 text NOT NULL CHECK (snapshot_sha256 ~ '^[0-9a-f]{64}$'),
    destination text NOT NULL CHECK (destination ~ '^gcs://agent-economy-projections/'),
    status text NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending', 'leased', 'projected', 'approved', 'published', 'failed')),
    lease_owner text,
    lease_expires_at timestamptz,
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, job_id),
    UNIQUE (namespace_id, snapshot_id, model_sha256, prompt_sha256, page_path),
    CHECK (wiki_id = 'agentic-commerce'),
    CHECK (
        (status = 'leased' AND lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)
        OR status <> 'leased'
    ),
    FOREIGN KEY (namespace_id, snapshot_id, stable_entity_id, snapshot_sha256)
        REFERENCES agent_economy.projection_snapshots
            (namespace_id, snapshot_id, stable_entity_id, snapshot_sha256)
);

CREATE INDEX projection_jobs_pull_idx
ON agent_economy.projection_jobs (namespace_id, status, created_at)
WHERE status IN ('pending', 'leased');

CREATE FUNCTION agent_economy.protect_projection_job_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(
        OLD.namespace_id, OLD.job_id, OLD.snapshot_id, OLD.stable_entity_id,
        OLD.wiki_id, OLD.page_path, OLD.model_id, OLD.model_sha256,
        OLD.prompt_sha256, OLD.snapshot_sha256, OLD.destination, OLD.created_at
    ) IS DISTINCT FROM ROW(
        NEW.namespace_id, NEW.job_id, NEW.snapshot_id, NEW.stable_entity_id,
        NEW.wiki_id, NEW.page_path, NEW.model_id, NEW.model_sha256,
        NEW.prompt_sha256, NEW.snapshot_sha256, NEW.destination, NEW.created_at
    ) THEN
        RAISE EXCEPTION 'projection job identity is immutable';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER projection_jobs_protect_identity
BEFORE UPDATE ON agent_economy.projection_jobs
FOR EACH ROW EXECUTE FUNCTION agent_economy.protect_projection_job_identity();

CREATE TABLE agent_economy.projection_approvals (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    candidate_sha256 text NOT NULL CHECK (candidate_sha256 ~ '^[0-9a-f]{64}$'),
    approved_payload jsonb NOT NULL CHECK (jsonb_typeof(approved_payload) = 'object'),
    approved_payload_bytes bytea NOT NULL,
    approved_by text NOT NULL CHECK (approved_by <> ''),
    approved_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, job_id, candidate_sha256),
    CHECK (candidate_sha256 = encode(sha256(approved_payload_bytes), 'hex')),
    CHECK (approved_payload = convert_from(approved_payload_bytes, 'UTF8')::jsonb),
    FOREIGN KEY (namespace_id, job_id)
        REFERENCES agent_economy.projection_jobs (namespace_id, job_id)
);

CREATE FUNCTION agent_economy.validate_projection_approval()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    projection_job agent_economy.projection_jobs%ROWTYPE;
BEGIN
    SELECT * INTO STRICT projection_job
    FROM agent_economy.projection_jobs
    WHERE namespace_id = NEW.namespace_id AND job_id = NEW.job_id;

    IF NOT (NEW.approved_payload ?& ARRAY[
            'job_id', 'stable_entity_id', 'wiki_id', 'page_path', 'model_id',
            'model_sha256', 'prompt_sha256', 'snapshot_sha256', 'destination',
            'output_sha256', 'generated_markdown', 'citations', 'wikilinks', 'changeset'
        ])
        OR NEW.approved_payload ->> 'job_id' IS DISTINCT FROM NEW.job_id::text
        OR NEW.approved_payload ->> 'stable_entity_id' IS DISTINCT FROM projection_job.stable_entity_id
        OR NEW.approved_payload ->> 'wiki_id' IS DISTINCT FROM projection_job.wiki_id
        OR NEW.approved_payload ->> 'page_path' IS DISTINCT FROM projection_job.page_path
        OR NEW.approved_payload ->> 'model_id' IS DISTINCT FROM projection_job.model_id
        OR NEW.approved_payload ->> 'model_sha256' IS DISTINCT FROM projection_job.model_sha256
        OR NEW.approved_payload ->> 'prompt_sha256' IS DISTINCT FROM projection_job.prompt_sha256
        OR NEW.approved_payload ->> 'snapshot_sha256' IS DISTINCT FROM projection_job.snapshot_sha256
        OR NEW.approved_payload ->> 'destination' IS DISTINCT FROM projection_job.destination
        OR COALESCE(NEW.approved_payload ->> 'output_sha256', '') !~ '^[0-9a-f]{64}$'
        OR COALESCE(NEW.approved_payload ->> 'generated_markdown', '') = ''
        OR jsonb_typeof(NEW.approved_payload -> 'citations') IS DISTINCT FROM 'array'
        OR jsonb_array_length(NEW.approved_payload -> 'citations') = 0
        OR jsonb_typeof(NEW.approved_payload -> 'wikilinks') IS DISTINCT FROM 'array'
        OR jsonb_typeof(NEW.approved_payload -> 'changeset') IS DISTINCT FROM 'object'
        OR NOT ((NEW.approved_payload -> 'changeset') ?& ARRAY[
            'id', 'sha256', 'page_revision_id', 'page_sha256', 'output_sha256'
        ])
        OR COALESCE(NEW.approved_payload -> 'changeset' ->> 'id', '') = ''
        OR COALESCE(NEW.approved_payload -> 'changeset' ->> 'sha256', '') !~ '^[0-9a-f]{64}$'
        OR COALESCE(NEW.approved_payload -> 'changeset' ->> 'page_revision_id', '') = ''
        OR COALESCE(NEW.approved_payload -> 'changeset' ->> 'page_sha256', '') !~ '^[0-9a-f]{64}$'
        OR NEW.approved_payload -> 'changeset' ->> 'output_sha256'
            IS DISTINCT FROM NEW.approved_payload ->> 'output_sha256'
        OR EXISTS (
            SELECT 1
            FROM jsonb_array_elements_text(NEW.approved_payload -> 'wikilinks') AS link(value)
            WHERE value !~ '^(buyers|services|investigations|protocols)/'
                OR value ~ '(^|/)\.\.(/|$)'
                OR value LIKE '%//%'
                OR value LIKE '%:%'
        )
    THEN
        RAISE EXCEPTION 'approved projection payload is not bound to its job and changeset';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER projection_approvals_validate
BEFORE INSERT ON agent_economy.projection_approvals
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_projection_approval();

CREATE TRIGGER projection_approvals_immutable_projection_row
BEFORE UPDATE OR DELETE ON agent_economy.projection_approvals
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER projection_approvals_immutable_projection_truncate
BEFORE TRUNCATE ON agent_economy.projection_approvals
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE TABLE agent_economy.projection_publications (
    namespace_id uuid NOT NULL,
    job_id uuid NOT NULL,
    payload_sha256 text NOT NULL CHECK (payload_sha256 ~ '^[0-9a-f]{64}$'),
    bundle_sha256 text NOT NULL CHECK (bundle_sha256 ~ '^[0-9a-f]{64}$'),
    bundle jsonb NOT NULL CHECK (jsonb_typeof(bundle) = 'object'),
    bundle_bytes bytea NOT NULL,
    page_revision_id text NOT NULL CHECK (page_revision_id <> ''),
    page_sha256 text NOT NULL CHECK (page_sha256 ~ '^[0-9a-f]{64}$'),
    changeset_id text NOT NULL CHECK (changeset_id <> ''),
    changeset_sha256 text NOT NULL CHECK (changeset_sha256 ~ '^[0-9a-f]{64}$'),
    published_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (namespace_id, job_id, bundle_sha256),
    UNIQUE (namespace_id, bundle_sha256),
    CHECK (bundle_sha256 = encode(sha256(bundle_bytes), 'hex')),
    CHECK (bundle = convert_from(bundle_bytes, 'UTF8')::jsonb),
    FOREIGN KEY (namespace_id, job_id, payload_sha256)
        REFERENCES agent_economy.projection_approvals
            (namespace_id, job_id, candidate_sha256)
);

CREATE FUNCTION agent_economy.validate_projection_publication()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    approval agent_economy.projection_approvals%ROWTYPE;
BEGIN
    SELECT * INTO STRICT approval
    FROM agent_economy.projection_approvals
    WHERE namespace_id = NEW.namespace_id
        AND job_id = NEW.job_id
        AND candidate_sha256 = NEW.payload_sha256;

    IF NOT (NEW.bundle ?& ARRAY['payload', 'payload_sha256', 'approval'])
        OR jsonb_typeof(NEW.bundle -> 'approval') IS DISTINCT FROM 'object'
        OR position(
            convert_to('{"payload":', 'UTF8')
                || approval.approved_payload_bytes
                || convert_to(',"payload_sha256":', 'UTF8')
            IN NEW.bundle_bytes
        ) = 0
        OR NEW.bundle ->> 'payload_sha256' IS DISTINCT FROM NEW.payload_sha256
        OR NEW.bundle -> 'payload' IS DISTINCT FROM approval.approved_payload
        OR NEW.bundle -> 'approval' ->> 'candidate_sha256' IS DISTINCT FROM NEW.payload_sha256
        OR NEW.bundle -> 'approval' ->> 'approved_by' IS DISTINCT FROM approval.approved_by
        OR (NEW.bundle -> 'approval' ->> 'approved_at')::timestamptz IS DISTINCT FROM approval.approved_at
        OR NEW.page_revision_id
            IS DISTINCT FROM approval.approved_payload -> 'changeset' ->> 'page_revision_id'
        OR NEW.page_sha256 IS DISTINCT FROM approval.approved_payload -> 'changeset' ->> 'page_sha256'
        OR NEW.changeset_id IS DISTINCT FROM approval.approved_payload -> 'changeset' ->> 'id'
        OR NEW.changeset_sha256 IS DISTINCT FROM approval.approved_payload -> 'changeset' ->> 'sha256'
    THEN
        RAISE EXCEPTION 'published projection bundle is not bound to its approval';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER projection_publications_validate
BEFORE INSERT ON agent_economy.projection_publications
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_projection_publication();

CREATE TRIGGER projection_publications_immutable_projection_row
BEFORE UPDATE OR DELETE ON agent_economy.projection_publications
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER projection_publications_immutable_projection_truncate
BEFORE TRUNCATE ON agent_economy.projection_publications
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE VIEW agent_economy.projection_mirror_pages AS
SELECT
    publication.namespace_id,
    publication.job_id,
    approval.approved_payload ->> 'stable_entity_id' AS stable_entity_id,
    approval.approved_payload ->> 'wiki_id' AS wiki_id,
    approval.approved_payload ->> 'page_path' AS page_path,
    approval.approved_payload ->> 'model_id' AS model_id,
    approval.approved_payload ->> 'model_sha256' AS model_sha256,
    approval.approved_payload ->> 'prompt_sha256' AS prompt_sha256,
    approval.approved_payload ->> 'snapshot_sha256' AS snapshot_sha256,
    approval.approved_payload ->> 'output_sha256' AS output_sha256,
    publication.bundle_sha256,
    publication.changeset_id,
    publication.changeset_sha256,
    approval.approved_payload ->> 'generated_markdown' AS generated_markdown,
    approval.approved_payload -> 'citations' AS citations,
    ARRAY(
        SELECT jsonb_array_elements_text(approval.approved_payload -> 'wikilinks')
    ) AS wikilinks,
    approval.approved_by,
    approval.approved_at,
    publication.published_at
FROM agent_economy.projection_publications AS publication
JOIN agent_economy.projection_approvals AS approval
    ON approval.namespace_id = publication.namespace_id
    AND approval.job_id = publication.job_id
    AND approval.candidate_sha256 = publication.payload_sha256
JOIN agent_economy.projection_jobs AS job
    ON job.namespace_id = publication.namespace_id
    AND job.job_id = publication.job_id
WHERE approval.approved_payload ->> 'wiki_id' = 'agentic-commerce'
    AND approval.approved_payload ->> 'destination' ~ '^gcs://agent-economy-projections/'
    AND approval.approved_payload ?& ARRAY[
        'generated_markdown', 'citations', 'wikilinks', 'output_sha256'
    ];

DO $$
BEGIN
    CREATE ROLE agent_economy_projection_writer NOLOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
    CREATE ROLE agent_economy_dashboard_reader NOLOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END
$$;

DO $$
BEGIN
    CREATE ROLE agent_economy_projection_approver NOLOGIN;
EXCEPTION WHEN duplicate_object THEN NULL;
END
$$;

REVOKE ALL ON agent_economy.projection_snapshots FROM PUBLIC;
REVOKE ALL ON agent_economy.projection_jobs FROM PUBLIC;
REVOKE ALL ON agent_economy.projection_approvals FROM PUBLIC;
REVOKE ALL ON agent_economy.projection_publications FROM PUBLIC;
REVOKE ALL ON agent_economy.projection_mirror_pages FROM PUBLIC;

GRANT USAGE ON SCHEMA agent_economy TO agent_economy_projection_writer;
GRANT SELECT ON agent_economy.projection_snapshots TO agent_economy_projection_writer;
GRANT SELECT ON agent_economy.projection_jobs TO agent_economy_projection_writer;
GRANT SELECT ON agent_economy.projection_approvals TO agent_economy_projection_writer;
GRANT SELECT ON agent_economy.projection_publications TO agent_economy_projection_writer;
GRANT UPDATE (status, lease_owner, lease_expires_at, attempt_count, updated_at)
    ON agent_economy.projection_jobs TO agent_economy_projection_writer;
GRANT INSERT ON agent_economy.projection_publications TO agent_economy_projection_writer;

GRANT USAGE ON SCHEMA agent_economy TO agent_economy_projection_approver;
GRANT SELECT ON agent_economy.projection_jobs TO agent_economy_projection_approver;
GRANT INSERT ON agent_economy.projection_approvals TO agent_economy_projection_approver;

GRANT USAGE ON SCHEMA agent_economy TO agent_economy_dashboard_reader;
GRANT SELECT ON
    agent_economy.dashboard_facts,
    agent_economy.dashboard_pulse,
    agent_economy.dashboard_system,
    agent_economy.projection_mirror_pages,
    agent_economy.namespaces,
    agent_economy.attribution_runs,
    agent_economy.attribution_run_seals,
    agent_economy.attribution_candidates,
    agent_economy.settlements,
    agent_economy.payment_requirements,
    agent_economy.provenance_records,
    agent_economy.evidence_objects
TO agent_economy_dashboard_reader;

GRANT agent_economy_dashboard_reader TO CURRENT_USER;
GRANT agent_economy_projection_writer TO CURRENT_USER;

COMMIT;