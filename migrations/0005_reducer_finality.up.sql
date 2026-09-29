BEGIN;

ALTER TABLE agent_economy.event_finality_updates
    DROP CONSTRAINT event_finality_updates_finality_status_check;
ALTER TABLE agent_economy.event_finality_updates
    ADD CONSTRAINT event_finality_updates_finality_status_check
    CHECK (finality_status IN ('observed', 'confirmed', 'finalized', 'orphaned', 'reverted'));

CREATE FUNCTION agent_economy.validate_finality_assertion_provenance()
RETURNS trigger
LANGUAGE plpgsql
SECURITY INVOKER
SET search_path = pg_catalog, agent_economy
AS $function$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM agent_economy.provenance_records AS provenance
        JOIN agent_economy.observations AS observation
          ON observation.namespace_id = provenance.namespace_id
         AND observation.provenance_id = provenance.provenance_id
         AND observation.evidence_id = provenance.evidence_id
         AND observation.source_id = provenance.source_id
         AND observation.chain_scope = provenance.chain_scope
        JOIN agent_economy.canonical_event_observations AS event_observation
          ON event_observation.namespace_id = observation.namespace_id
         AND event_observation.protocol = observation.protocol
         AND event_observation.chain_scope = observation.chain_scope
         AND event_observation.source_id = observation.source_id
         AND event_observation.observation_id = observation.observation_id
        WHERE provenance.namespace_id = NEW.namespace_id
          AND provenance.provenance_id = NEW.provenance_id
          AND provenance.source_id = NEW.source_id
          AND provenance.chain_scope = NEW.chain_scope
          AND provenance.transaction_reference = NEW.transaction_id
          AND event_observation.protocol = NEW.protocol
          AND event_observation.canonical_event_id = NEW.canonical_event_id
    ) THEN
        RAISE EXCEPTION 'finality assertion provenance does not match its subject'
            USING ERRCODE = '23503';
    END IF;
    RETURN NEW;
END
$function$;

REVOKE ALL ON FUNCTION agent_economy.validate_finality_assertion_provenance() FROM PUBLIC;

CREATE TABLE agent_economy.event_finality_assertions (
    namespace_id uuid NOT NULL,
    protocol text NOT NULL CHECK (protocol IN ('x402', 'mpp')),
    chain_scope text NOT NULL CHECK (chain_scope <> ''),
    canonical_event_id text NOT NULL,
    assertion_sequence integer NOT NULL CHECK (assertion_sequence > 0),
    accepted boolean NOT NULL,
    asserted_status text NOT NULL CHECK (asserted_status IN ('observed', 'confirmed', 'finalized', 'orphaned', 'reverted')),
    current_status text NOT NULL CHECK (current_status IN ('observed', 'confirmed', 'finalized', 'orphaned', 'reverted')),
    asserted_at timestamptz NOT NULL,
    source_id text NOT NULL CHECK (source_id <> ''),
    provenance_id uuid NOT NULL,
    transaction_id text NOT NULL CHECK (transaction_id <> ''),
    basis_kind text NOT NULL CHECK (basis_kind IN ('evm', 'solana')),
    position bigint NOT NULL CHECK (position >= 0),
    block_hash text NOT NULL CHECK (block_hash <> ''),
    canonical_block_hash text NOT NULL CHECK (canonical_block_hash <> ''),
    latest_position bigint CHECK (latest_position >= position),
    finalized_position bigint CHECK (
        finalized_position >= 0
        AND (latest_position IS NULL OR finalized_position <= latest_position)
    ),
    confirmations_required bigint CHECK (confirmations_required > 0),
    commitment text CHECK (commitment IN ('processed', 'confirmed', 'finalized')),
    execution_outcome text NOT NULL CHECK (execution_outcome IN ('succeeded', 'reverted')),
    finality_state_hash text NOT NULL CHECK (finality_state_hash ~ '^[0-9a-f]{64}$'),
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (
        namespace_id, protocol, chain_scope, canonical_event_id, assertion_sequence
    ),
    CHECK (
        (basis_kind = 'evm'
            AND latest_position IS NOT NULL
            AND finalized_position IS NOT NULL
            AND confirmations_required IS NOT NULL
            AND commitment IS NULL)
        OR
        (basis_kind = 'solana'
            AND latest_position IS NULL
            AND finalized_position IS NULL
            AND confirmations_required IS NULL
            AND commitment IS NOT NULL)
    ),
    FOREIGN KEY (namespace_id, protocol, chain_scope, canonical_event_id)
        REFERENCES agent_economy.canonical_events (
            namespace_id, protocol, chain_scope, canonical_event_id
        ),
    FOREIGN KEY (namespace_id, provenance_id, source_id, chain_scope)
        REFERENCES agent_economy.provenance_records (
            namespace_id, provenance_id, source_id, chain_scope
        )
);

CREATE INDEX event_finality_assertions_event_idx
    ON agent_economy.event_finality_assertions (
        namespace_id, protocol, chain_scope, canonical_event_id, assertion_sequence
    );
CREATE INDEX event_finality_assertions_conflict_idx
    ON agent_economy.event_finality_assertions (
        namespace_id, protocol, chain_scope, canonical_event_id, asserted_at
    ) WHERE NOT accepted;

CREATE TRIGGER event_finality_assertions_validate_provenance
BEFORE INSERT ON agent_economy.event_finality_assertions
FOR EACH ROW EXECUTE FUNCTION agent_economy.validate_finality_assertion_provenance();
CREATE TRIGGER event_finality_assertions_immutable
BEFORE UPDATE OR DELETE ON agent_economy.event_finality_assertions
FOR EACH ROW EXECUTE FUNCTION agent_economy.reject_immutable_mutation();
CREATE TRIGGER event_finality_assertions_truncate_immutable
BEFORE TRUNCATE ON agent_economy.event_finality_assertions
FOR EACH STATEMENT EXECUTE FUNCTION agent_economy.reject_immutable_mutation();

CREATE VIEW agent_economy.event_finality_conflicts AS
SELECT *
FROM agent_economy.event_finality_assertions
WHERE NOT accepted;

COMMIT;
