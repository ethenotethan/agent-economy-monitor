use agent_economy_monitor::projection_runtime::{
    ProjectionRuntimeConfig, SecretString, merge_generated_section,
};
use std::{collections::BTreeMap, process::Command};

const DOCUMENTED_PROJECTION_ENV: [&str; 6] = [
    "PROJECTION_GATEWAY_URL",
    "PROJECTION_GATEWAY_TOKEN",
    "PROJECTION_MODEL_URL",
    "PROJECTION_MODEL_TOKEN",
    "HERMES_WIKI_RPC_URL",
    "HERMES_WIKI_TOKEN",
];

#[test]
fn documented_projection_environment_loads_through_runtime_config() {
    let documented = include_str!("../.env.example")
        .lines()
        .filter_map(|line| line.split_once('='))
        .filter(|(name, _)| DOCUMENTED_PROJECTION_ENV.contains(name))
        .map(|(name, value)| (name.to_owned(), value.to_owned()))
        .collect::<BTreeMap<_, _>>();

    assert_eq!(
        documented.keys().map(String::as_str).collect::<Vec<_>>(),
        DOCUMENTED_PROJECTION_ENV
            .into_iter()
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>()
    );
    for legacy_name in [
        "MODEL_URL",
        "MODEL_BEARER_TOKEN",
        "HERMES_GATEWAY_URL",
        "HERMES_GATEWAY_TOKEN",
    ] {
        assert!(!include_str!("../.env.example").lines().any(|line| {
            line.split_once('=')
                .is_some_and(|(name, _)| name == legacy_name)
        }));
    }

    let output = Command::new(std::env::current_exe().expect("current test executable"))
        .arg("--exact")
        .arg("documented_projection_environment_child")
        .arg("--nocapture")
        .env("AEM_DOCUMENTED_PROJECTION_ENV_CHILD", "1")
        .envs(documented)
        .output()
        .expect("run isolated environment parser test");

    assert!(
        output.status.success(),
        "documented projection environment was rejected:\n{}",
        String::from_utf8_lossy(&output.stdout)
    );
}

#[test]
fn documented_projection_environment_child() {
    if std::env::var_os("AEM_DOCUMENTED_PROJECTION_ENV_CHILD").is_none() {
        return;
    }
    ProjectionRuntimeConfig::from_env().expect("documented projection environment must parse");
}

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
