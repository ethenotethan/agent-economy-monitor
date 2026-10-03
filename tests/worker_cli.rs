use std::process::Command;

#[test]
fn explicit_worker_mode_without_a_registered_real_handler_fails_closed() {
    let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("collect")
        .env_remove("DATABASE_URL")
        .env_remove("NAMESPACE_ID")
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("MissingHandler(Collect)"), "{stderr}");
    assert!(
        !stderr.contains("DATABASE_URL"),
        "handler validation must precede secret/config access"
    );
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
