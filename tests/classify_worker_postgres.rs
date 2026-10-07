#[test]
fn classifier_migration_is_fail_closed_and_bound_to_one_mode_and_job_kind() {
    let sql = include_str!("../migrations/0017_classify_enrich_workers.up.sql");

    assert!(sql.contains("CREATE ROLE agent_economy_classifier_runtime LOGIN NOINHERIT"));
    assert!(sql.contains("WHERE binding.login_name = session_user"));
    assert!(sql.contains("queued.mode = 'classify'"));
    assert!(sql.contains("queued.job_kind = 'buyer-behavior-v1'"));
    assert!(sql.contains("JOIN agent_economy.classifier_job_inputs"));
    assert!(
        sql.contains(
            "REVOKE ALL ON agent_economy.worker_jobs FROM agent_economy_classifier_runtime"
        )
    );
    assert!(
        sql.contains("GRANT EXECUTE ON FUNCTION agent_economy.claim_classifier_job(text, bigint)")
    );
    assert!(!sql.contains("GRANT EXECUTE ON FUNCTION agent_economy.claim_worker_job("));
}

#[test]
fn classifier_commit_is_lease_gated_and_persists_complete_lineage() {
    let sql = include_str!("../migrations/0017_classify_enrich_workers.up.sql");

    for binding in [
        "job.lease_owner = p_lease_owner",
        "job.lease_token = p_lease_token",
        "job.lease_expires_at > clock_timestamp()",
        "job.input_sha256 = p_input_sha256",
        "input.input_manifest ->> 'provenance_id' <> p_provenance_id::text",
        "input.input_manifest -> 'activities'",
        "input.input_manifest -> 'labels'",
        "p_evidence_ids",
        "classification_runs",
        "classification_run_features",
        "classification_label_definitions",
        "classification_claims",
        "classification_claim_evidence",
    ] {
        assert!(sql.contains(binding), "missing lineage binding: {binding}");
    }
}

#[test]
fn enricher_migration_verifies_raw_evidence_before_reducing_to_classification_admission() {
    let sql = include_str!("../migrations/0017_classify_enrich_workers.up.sql");

    for contract in [
        "CREATE ROLE agent_economy_enricher_runtime LOGIN NOINHERIT",
        "queued.mode = 'enrich'",
        "queued.job_kind = 'buyer-public-history-v1'",
        "JOIN agent_economy.enrichment_job_inputs",
        "INSERT INTO agent_economy.evidence_objects",
        "INSERT INTO agent_economy.provenance_records",
        "INSERT INTO agent_economy.buyer_finalized_history",
        "INSERT INTO agent_economy.buyer_finalized_history_evidence",
        "p_evidence_bodies bytea[]",
        "canonical_records <> p_records_json",
        "INSERT INTO agent_economy.enrichment_reduction_receipts",
        "JOIN agent_economy.event_finality_assertions",
        "INSERT INTO agent_economy.classification_admission_requests",
        "protocol_attribution = false",
        "REVOKE ALL ON agent_economy.worker_jobs FROM agent_economy_enricher_runtime",
    ] {
        assert!(
            sql.contains(contract),
            "missing enrich contract: {contract}"
        );
    }
    assert!(!sql.contains("GRANT EXECUTE ON FUNCTION agent_economy.claim_worker_job("));
}
