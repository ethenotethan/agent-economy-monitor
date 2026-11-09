BEGIN;

CREATE TABLE agent_economy.discovery_source_registry (
    source_id text NOT NULL,
    parser_version text NOT NULL,
    protocol text NOT NULL,
    discovery_kind text NOT NULL CHECK (discovery_kind IN ('offer_catalog')),
    PRIMARY KEY (source_id, parser_version, protocol)
);

INSERT INTO agent_economy.discovery_source_registry
    (source_id, parser_version, protocol, discovery_kind)
VALUES
    ('cdp-x402-bazaar-base', 'x402-adapter@1', 'x402', 'offer_catalog');

CREATE VIEW agent_economy.dashboard_discovery_activity AS
WITH accepted_observations AS (
    SELECT DISTINCT
        observation.namespace_id,
        observation.source_id,
        observation.chain_scope,
        observation.protocol,
        observation.parser_version,
        observation.observation_id,
        observation.observed_at,
        observation.evidence_id,
        observation.provenance_id
    FROM agent_economy.observations AS observation
    JOIN agent_economy.discovery_source_registry AS discovery_source
      ON discovery_source.source_id = observation.source_id
     AND discovery_source.parser_version = observation.parser_version
     AND discovery_source.protocol = observation.protocol
    JOIN agent_economy.canonical_event_observations AS support
      ON support.namespace_id = observation.namespace_id
     AND support.protocol = observation.protocol
     AND support.chain_scope = observation.chain_scope
     AND support.source_id = observation.source_id
     AND support.observation_id = observation.observation_id
     AND support.support_role = 'supporting'
    JOIN agent_economy.canonical_events AS canonical
      ON canonical.namespace_id = support.namespace_id
     AND canonical.protocol = support.protocol
     AND canonical.chain_scope = support.chain_scope
     AND canonical.canonical_event_id = support.canonical_event_id
), ranked_observations AS (
    SELECT
        accepted.*,
        count(*) OVER (
            PARTITION BY accepted.namespace_id, accepted.source_id,
                accepted.chain_scope, accepted.protocol, accepted.parser_version
        ) AS observation_count,
        row_number() OVER (
            PARTITION BY accepted.namespace_id, accepted.source_id,
                accepted.chain_scope, accepted.protocol, accepted.parser_version
            ORDER BY accepted.observed_at DESC, accepted.observation_id DESC
        ) AS lineage_position
    FROM accepted_observations AS accepted
)
SELECT
    accepted.namespace_id,
    concat_ws(
        ':',
        'discovery',
        accepted.source_id,
        accepted.chain_scope,
        accepted.protocol,
        accepted.parser_version
    ) AS id,
    'discovery_observation'::text AS kind,
    'Observed ' || accepted.protocol || ' offers/resources from '
        || accepted.source_id || ' on ' || accepted.chain_scope AS label,
    jsonb_build_object(
        'source_id', accepted.source_id,
        'chain_scope', accepted.chain_scope,
        'protocol', accepted.protocol,
        'parser_version', accepted.parser_version,
        'observation_count', max(accepted.observation_count)::text,
        'latest_observed_at', max(accepted.observed_at),
        'lineage_limit', 100,
        'lineage_truncated', max(accepted.observation_count) > 100,
        'evidence_ids', array_agg(DISTINCT accepted.evidence_id
            ORDER BY accepted.evidence_id)
    ) AS value,
    max(accepted.observed_at) AS observed_at,
    array_agg(DISTINCT accepted.provenance_id::text
        ORDER BY accepted.provenance_id::text) AS provenance_ids
FROM ranked_observations AS accepted
WHERE accepted.lineage_position <= 100
GROUP BY
    accepted.namespace_id,
    accepted.source_id,
    accepted.chain_scope,
    accepted.protocol,
    accepted.parser_version;

GRANT SELECT ON agent_economy.dashboard_discovery_activity TO agent_economy_dashboard_reader;

COMMIT;
