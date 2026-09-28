BEGIN;

DO $rollback$
DECLARE
    graph_nonempty boolean;
BEGIN
    IF to_regclass('agent_economy.namespaces') IS NOT NULL THEN
        EXECUTE 'SELECT EXISTS (SELECT 1 FROM agent_economy.namespaces)'
            INTO graph_nonempty;
        IF graph_nonempty THEN
            RAISE EXCEPTION 'cannot roll back non-empty canonical knowledge graph';
        END IF;
    END IF;
END
$rollback$;

DROP TABLE agent_economy.classification_claim_evidence;
DROP TABLE agent_economy.classification_claims;
DROP TABLE agent_economy.attribution_edges;
DROP TABLE agent_economy.buyer_cluster_memberships;
DROP TABLE agent_economy.buyer_cluster_versions;
DROP TABLE agent_economy.buyer_clusters;
DROP TABLE agent_economy.buyer_handles;
DROP TABLE agent_economy.payment_options;
DROP TABLE agent_economy.offers;
DROP TABLE agent_economy.endpoints;
DROP TABLE agent_economy.services;
DROP TABLE agent_economy.provenance_records;
DROP TABLE agent_economy.evidence_objects;
DROP TABLE agent_economy.namespaces;
DROP FUNCTION agent_economy.guard_claim_evidence_append();
DROP FUNCTION agent_economy.lock_claim_series();
DROP FUNCTION agent_economy.guard_cluster_membership_append();
DROP FUNCTION agent_economy.lock_cluster_series();
DROP FUNCTION agent_economy.reject_immutable_mutation();
DROP SCHEMA agent_economy;

COMMIT;
