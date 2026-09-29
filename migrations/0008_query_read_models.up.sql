BEGIN;

CREATE VIEW agent_economy.dashboard_facts AS
SELECT
    buyer.namespace_id,
    buyer.buyer_handle_id AS id,
    'buyer'::text AS kind,
    buyer.buyer_handle_id AS label,
    jsonb_build_object(
        'handle_kind', buyer.handle_kind,
        'chain_scope', buyer.chain_scope
    ) AS value,
    provenance.observed_at,
    ARRAY[buyer.provenance_id::text] AS provenance_ids
FROM agent_economy.buyer_handles AS buyer
JOIN agent_economy.provenance_records AS provenance
  ON provenance.namespace_id = buyer.namespace_id
 AND provenance.provenance_id = buyer.provenance_id
UNION ALL
SELECT
    service.namespace_id,
    service.service_id,
    'service'::text,
    service.display_name,
    jsonb_build_object('trust_state', service.trust_state),
    provenance.observed_at,
    ARRAY[service.provenance_id::text]
FROM agent_economy.services AS service
JOIN agent_economy.provenance_records AS provenance
  ON provenance.namespace_id = service.namespace_id
 AND provenance.provenance_id = service.provenance_id
UNION ALL
SELECT
    settlement.namespace_id,
    settlement.chain_scope || ':' || settlement.settlement_id,
    'settlement'::text,
    settlement.protocol || ' settlement on ' || settlement.chain_scope,
    jsonb_build_object(
        'protocol', settlement.protocol,
        'chain_scope', settlement.chain_scope,
        'settlement_id', settlement.settlement_id,
        'buyer_handle_id', settlement.buyer_handle_id,
        'asset', settlement.asset,
        'amount_atomic', settlement.amount_atomic::text,
        'settled_at', settlement.settled_at
    ),
    provenance.observed_at,
    ARRAY[settlement.provenance_id::text]
FROM agent_economy.settlements AS settlement
JOIN agent_economy.provenance_records AS provenance
  ON provenance.namespace_id = settlement.namespace_id
 AND provenance.provenance_id = settlement.provenance_id;

CREATE VIEW agent_economy.dashboard_pulse AS
SELECT
    settlement.namespace_id,
    concat_ws(':',
        'pulse',
        extract(epoch FROM date_trunc('hour', settlement.settled_at))::bigint::text,
        settlement.protocol,
        settlement.chain_scope,
        settlement.asset
    ) AS id,
    'pulse_metric'::text AS kind,
    settlement.protocol || ' on ' || settlement.chain_scope AS label,
    jsonb_build_object(
        'hour', date_trunc('hour', settlement.settled_at),
        'protocol', settlement.protocol,
        'chain_scope', settlement.chain_scope,
        'asset', settlement.asset,
        'settlement_count', count(*)::text,
        'active_buyers', count(DISTINCT settlement.buyer_handle_id)::text,
        'amount_atomic', sum(settlement.amount_atomic)::text
    ) AS value,
    max(provenance.observed_at) AS observed_at,
    array_agg(DISTINCT settlement.provenance_id::text
              ORDER BY settlement.provenance_id::text) AS provenance_ids
FROM agent_economy.settlements AS settlement
JOIN agent_economy.current_event_finality AS finality
  ON finality.namespace_id = settlement.namespace_id
 AND finality.protocol = settlement.protocol
 AND finality.chain_scope = settlement.chain_scope
 AND finality.canonical_event_id = settlement.canonical_event_id
JOIN agent_economy.provenance_records AS provenance
  ON provenance.namespace_id = settlement.namespace_id
 AND provenance.provenance_id = settlement.provenance_id
WHERE finality.finality_status = 'finalized'
GROUP BY
    settlement.namespace_id,
    date_trunc('hour', settlement.settled_at),
    settlement.protocol,
    settlement.chain_scope,
    settlement.asset;

CREATE VIEW agent_economy.dashboard_system AS
SELECT
    observation.namespace_id,
    concat_ws(':',
        'source', observation.source_id, observation.chain_scope,
        observation.protocol, observation.parser_version
    ) AS id,
    'system_metric'::text AS kind,
    observation.source_id || ' on ' || observation.chain_scope AS label,
    jsonb_build_object(
        'source_id', observation.source_id,
        'chain_scope', observation.chain_scope,
        'protocol', observation.protocol,
        'parser_version', observation.parser_version,
        'observation_count', count(*)::text,
        'latest_observed_at', max(observation.observed_at)
    ) AS value,
    max(observation.observed_at) AS observed_at,
    array_agg(DISTINCT observation.provenance_id::text
              ORDER BY observation.provenance_id::text) AS provenance_ids
FROM agent_economy.observations AS observation
GROUP BY
    observation.namespace_id,
    observation.source_id,
    observation.chain_scope,
    observation.protocol,
    observation.parser_version;

COMMIT;
