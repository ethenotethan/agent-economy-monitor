use std::sync::Arc;

use agent_economy_monitor::query::{PostgresQueryStore, api_router};
use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio_postgres::NoTls;
use tower::ServiceExt;

#[tokio::test]
async fn admitted_registry_exposes_honest_attribution_and_unavailable_chain_gaps() {
    let database_url = std::env::var("AEM_COLLECT_TEST_DATABASE_URL")
        .expect("AEM_COLLECT_TEST_DATABASE_URL is required for x402 coverage tests");
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("connect to disposable PostgreSQL");
    tokio::spawn(async move {
        connection.await.expect("PostgreSQL connection");
    });

    let ids = client
        .query_one(
            "SELECT gen_random_uuid()::text, gen_random_uuid()::text",
            &[],
        )
        .await
        .expect("mint isolated fixture identifiers");
    let namespace: String = ids.get(0);
    let provenance: String = ids.get(1);
    let run_key = namespace.replace('-', "");
    client
        .batch_execute(&format!(
            r#"
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'x402-coverage-{run_key}');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
            VALUES ('{namespace}', 'evidence:x402-registry-{run_key}', repeat('a', 64),
                    'evidence://x402-registry-{run_key}', 'application/json', 2, now());
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at, parser_version,
                 chain_scope, evidence_id)
            VALUES ('{namespace}', '{provenance}', 'x402-facilitator-base', now(),
                    'x402-history-registry@1', 'base', 'evidence:x402-registry-{run_key}');
            SELECT agent_economy.admit_x402_history_source(
                '{namespace}', 'x402-facilitator-base', 'base', 1, now(), 41,
                'x402scan-5732bb83c8c4290f413f24318a838e3fee4ed2bf', repeat('b', 64),
                10, 60, '{provenance}');
            "#
        ))
        .await
        .expect("admit frozen facilitator registry");

    let response = api_router(Arc::new(PostgresQueryStore::new(client, namespace.clone())))
        .oneshot(
            Request::builder()
                .uri("/api/v1/x402/history/coverage")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let rows = body["data"].as_array().expect("coverage data array");

    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0]["chain_scope"], "base");
    assert_eq!(rows[0]["attribution_strength"], "known_facilitator_onchain");
    assert_eq!(rows[1]["chain_scope"], "ethereum");
    assert_eq!(rows[1]["coverage_state"], "unknown");
    assert_eq!(rows[1]["gap_count"], 1);
    assert_eq!(
        rows[1]["failure_code"],
        "no-admitted-x402-facilitator-registry"
    );
    assert_eq!(rows[2]["chain_scope"], "tempo");
    assert_eq!(rows[2]["coverage_state"], "unknown");
    assert_eq!(rows[2]["gap_count"], 1);
    assert_eq!(rows[2]["failure_code"], "mpp-is-a-separate-protocol-series");
}
