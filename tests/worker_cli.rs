use std::process::Command;

#[test]
fn collect_without_required_configuration_fails_closed_before_claiming() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("collect")
        .env_remove("DATABASE_URL")
        .env_remove("COLLECTOR_DATABASE_URL")
        .env_remove("NAMESPACE_ID")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("DATABASE_URL") || stderr.contains("collection configuration"),
        "missing configuration must be explicit without exposing a value: {stderr}"
    );
}

#[test]
fn collect_requires_exactly_one_evidence_backend_before_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("collect")
        .env(
            "COLLECTOR_DATABASE_URL",
            "postgresql://127.0.0.1:1/unreachable",
        )
        .env("COLLECTION_INPUT_ROOT", ".")
        .env_remove("EVIDENCE_WRITE_ROOT")
        .env_remove("EVIDENCE_WRITE_BUCKET")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("exactly one create-only evidence backend"),
        "{stderr}"
    );
    assert!(!stderr.contains("Connection refused"), "{stderr}");
}

#[test]
fn collect_rejects_verifier_authority_before_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("collect")
        .env(
            "COLLECTOR_DATABASE_URL",
            "postgresql://127.0.0.1:1/unreachable",
        )
        .env(
            "EVIDENCE_VERIFIER_DATABASE_URL",
            "postgresql://127.0.0.1:1/unreachable",
        )
        .env("COLLECTION_INPUT_ROOT", ".")
        .env("EVIDENCE_WRITE_ROOT", ".")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("mixed collection and verification authority"),
        "{stderr}"
    );
    assert!(!stderr.contains("Connection refused"), "{stderr}");
}

#[test]
fn verify_evidence_rejects_collector_and_rpc_authority_before_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("verify-evidence")
        .env(
            "EVIDENCE_VERIFIER_DATABASE_URL",
            "postgresql://127.0.0.1:1/unreachable",
        )
        .env(
            "COLLECTOR_DATABASE_URL",
            "postgresql://127.0.0.1:1/unreachable",
        )
        .env("ALCHEMY_BASE_RPC_URL", "https://secret.invalid")
        .env("EVIDENCE_READ_ROOT", ".")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("mixed collection and verification authority"),
        "{stderr}"
    );
    assert!(!stderr.contains("Connection refused"), "{stderr}");
}

#[test]
fn classify_without_dedicated_database_authority_fails_closed() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("classify")
        .env_remove("CLASSIFIER_DATABASE_URL")
        .env_remove("NAMESPACE_ID")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("CLASSIFIER_DATABASE_URL"), "{stderr}");
    assert!(!stderr.contains("handler unavailable"), "{stderr}");
}

#[test]
fn enrich_requires_create_only_evidence_backend_before_connecting() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("enrich")
        .env(
            "ENRICHER_DATABASE_URL",
            "postgresql://127.0.0.1:1/unreachable",
        )
        .env_remove("EVIDENCE_WRITE_ROOT")
        .env_remove("EVIDENCE_WRITE_BUCKET")
        .env_remove("NAMESPACE_ID")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("exactly one create-only evidence backend"),
        "{stderr}"
    );
    assert!(!stderr.contains("Connection refused"), "{stderr}");
}

#[test]
fn unknown_process_mode_fails_nonzero() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("sweep-everything")
        .output()
        .unwrap();

    assert!(!output.status.success());
    assert!(
        String::from_utf8(output.stderr)
            .unwrap()
            .contains("unknown command: sweep-everything")
    );
}
