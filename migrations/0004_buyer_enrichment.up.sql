BEGIN;

ALTER TABLE agent_economy.buyer_handles
    ADD CONSTRAINT buyer_handles_enrichment_chain_key
    UNIQUE (namespace_id, buyer_handle_id, chain_scope, handle_value);

CREATE TABLE agent_economy.buyer_enrichment_cursors (
    namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces (namespace_id),
    buyer_handle_id text NOT NULL,
    chain_scope text NOT NULL CHECK (chain_scope IN ('ethereum', 'base', 'solana', 'tempo')),
    handle_value text NOT NULL CHECK (handle_value <> ''),
    cursor text,
    version bigint NOT NULL DEFAULT 0 CHECK (version >= 0),
    requests_used_total bigint NOT NULL DEFAULT 0 CHECK (requests_used_total >= 0),
    last_run_budget bigint NOT NULL DEFAULT 0 CHECK (last_run_budget >= 0),
    last_run_requests_used bigint NOT NULL DEFAULT 0 CHECK (
        last_run_requests_used >= 0 AND last_run_requests_used <= last_run_budget
    ),
    reservation_owner text CHECK (reservation_owner IS NULL OR reservation_owner <> ''),
    reservation_expires_at timestamptz,
    complete boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT now(),
    CHECK ((reservation_owner IS NULL) = (reservation_expires_at IS NULL)),
    PRIMARY KEY (namespace_id, buyer_handle_id, chain_scope),
    FOREIGN KEY (namespace_id, buyer_handle_id, chain_scope, handle_value)
        REFERENCES agent_economy.buyer_handles (
            namespace_id, buyer_handle_id, chain_scope, handle_value
        )
);

CREATE TABLE agent_economy.buyer_finalized_history (
    namespace_id uuid NOT NULL,
    buyer_handle_id text NOT NULL,
    chain_scope text NOT NULL CHECK (chain_scope IN ('ethereum', 'base', 'solana', 'tempo')),
    transaction_reference text NOT NULL CHECK (transaction_reference <> ''),
    block_reference text NOT NULL CHECK (block_reference <> ''),
    finality text NOT NULL DEFAULT 'finalized' CHECK (finality = 'finalized'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, buyer_handle_id, chain_scope, transaction_reference
    ),
    FOREIGN KEY (namespace_id, buyer_handle_id, chain_scope)
        REFERENCES agent_economy.buyer_enrichment_cursors (
            namespace_id, buyer_handle_id, chain_scope
        )
);

CREATE TABLE agent_economy.buyer_finalized_history_evidence (
    namespace_id uuid NOT NULL,
    buyer_handle_id text NOT NULL,
    chain_scope text NOT NULL,
    transaction_reference text NOT NULL,
    evidence_id text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, buyer_handle_id, chain_scope,
        transaction_reference, evidence_id
    ),
    FOREIGN KEY (
        namespace_id, buyer_handle_id, chain_scope, transaction_reference
    ) REFERENCES agent_economy.buyer_finalized_history (
        namespace_id, buyer_handle_id, chain_scope, transaction_reference
    ),
    FOREIGN KEY (namespace_id, evidence_id)
        REFERENCES agent_economy.evidence_objects (namespace_id, evidence_id)
);

CREATE INDEX buyer_finalized_history_block_idx
    ON agent_economy.buyer_finalized_history (
        namespace_id, buyer_handle_id, chain_scope, block_reference
    );

CREATE TRIGGER buyer_finalized_history_immutable
BEFORE UPDATE OR DELETE ON agent_economy.buyer_finalized_history
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER buyer_finalized_history_truncate_immutable
BEFORE TRUNCATE ON agent_economy.buyer_finalized_history
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER buyer_finalized_history_evidence_immutable
BEFORE UPDATE OR DELETE ON agent_economy.buyer_finalized_history_evidence
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER buyer_finalized_history_evidence_truncate_immutable
BEFORE TRUNCATE ON agent_economy.buyer_finalized_history_evidence
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

COMMIT;
