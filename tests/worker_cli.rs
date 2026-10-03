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
        .env(
            "EVIDENCE_VERIFIER_DATABASE_URL",
            "postgresql://127.0.0.1:1/unreachable",
        )
        .env("NAMESPACE_ID", "00000000-0000-0000-0000-000000000048")
        .env("COLLECTION_INPUT_ROOT", ".")
        .env_remove("EVIDENCE_ROOT")
        .env_remove("EVIDENCE_BUCKET")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("exactly one evidence backend"), "{stderr}");
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
