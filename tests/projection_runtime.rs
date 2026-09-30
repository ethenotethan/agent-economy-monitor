use agent_economy_monitor::projection_runtime::{
    ProjectionRuntimeConfig, SecretString, merge_generated_section,
};

#[test]
fn runtime_config_requires_outbound_https_and_redacts_bearer_tokens() {
    let config = ProjectionRuntimeConfig::new(
        "https://projection.example.test",
        SecretString::new("gateway-secret"),
        "http://127.0.0.1:8090/v1/project",
        SecretString::new("model-secret"),
        "http://127.0.0.1:8088/rpc",
        SecretString::new("wiki-secret"),
    )
    .expect("valid outbound configuration");

    let debug = format!("{config:?}");
    assert!(!debug.contains("gateway-secret"));
    assert!(!debug.contains("model-secret"));
    assert!(!debug.contains("wiki-secret"));
    assert!(debug.contains("[REDACTED]"));

    assert!(
        ProjectionRuntimeConfig::new(
            "http://projection.example.test",
            SecretString::new("secret"),
            "http://127.0.0.1:8090/v1/project",
            SecretString::new("secret"),
            "http://127.0.0.1:8088/rpc",
            SecretString::new("secret"),
        )
        .is_err()
    );
    assert!(
        ProjectionRuntimeConfig::new(
            "https://projection.example.test",
            SecretString::new("secret"),
            "http://127.0.0.1:8090/v1/project",
            SecretString::new("secret"),
            "http://wiki.example.test/rpc",
            SecretString::new("secret"),
        )
        .is_err()
    );
    assert!(
        ProjectionRuntimeConfig::new(
            "https://projection.example.test",
            SecretString::new("secret"),
            "https://remote-model.example.test/v1/project",
            SecretString::new("secret"),
            "http://127.0.0.1:8088/rpc",
            SecretString::new("secret"),
        )
        .is_err()
    );
}

#[test]
fn binary_wires_a_bounded_outbound_projection_mode() {
    let main = include_str!("../src/main.rs");
    assert!(main.contains("project-wiki"));
    assert!(main.contains("ProjectionRuntimeConfig::from_env"));
    assert!(main.contains("run_projection_once"));
}

#[test]
fn native_wiki_adapter_uses_real_rpc_and_preserves_curated_content() {
    let merged = merge_generated_section(
        "Curated owner note.\n\n<!-- BEGIN GENERATED: agent-economy-projection -->\nold\n<!-- END GENERATED: agent-economy-projection -->\n\nMore curated.\n",
        "new generated\n",
    )
    .expect("well-formed generated section");
    assert!(merged.contains("Curated owner note."));
    assert!(merged.contains("More curated."));
    assert!(merged.contains("new generated"));
    assert!(!merged.contains("\nold\n"));
    assert!(
        merge_generated_section(
            "<!-- BEGIN GENERATED: agent-economy-projection -->\na\n<!-- END GENERATED: agent-economy-projection -->\n<!-- BEGIN GENERATED: agent-economy-projection -->\nb\n<!-- END GENERATED: agent-economy-projection -->",
            "new",
        )
        .is_err()
    );

    let source = include_str!("../src/projection_runtime.rs");
    assert!(source.contains("\"wiki.update\""));
    assert!(source.contains("\"wiki.changesets\""));
    assert!(source.contains("captured.timestamp != updated.updated"));
    assert!(source.contains("Some(String::new())"));
    assert!(!source.contains("wiki.replace_generated_section"));
    assert!(!source.contains("wiki.capture_projection_changeset"));
}
