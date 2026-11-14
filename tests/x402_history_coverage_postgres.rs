use std::{str::FromStr, sync::Arc};

use agent_economy_monitor::query::{PostgresQueryStore, api_router};
use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use serde_json::Value;
use tokio_postgres::{Config, NoTls};
use tower::ServiceExt;

const ROLE_TEST_LOCK: i64 = 0x41454d434f4c4c45;
const ROLE_TEST_PASSWORD: &str = "aem-custody-test-password";

async fn connect_as(database_url: &str, user: &str) -> tokio_postgres::Client {
    let mut config = Config::from_str(database_url).unwrap();
    config.user(user);
    config.password(ROLE_TEST_PASSWORD);
    let (client, connection) = config.connect(NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

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

    let provenance_rows = client
        .query(
            "SELECT source.chain_scope, source.admitted_provenance_id::text, \
                    provenance.source_id, provenance.chain_scope \
             FROM agent_economy.x402_history_sources source \
             JOIN agent_economy.provenance_records provenance \
               ON provenance.namespace_id=source.namespace_id \
              AND provenance.provenance_id=source.admitted_provenance_id \
             WHERE source.namespace_id=$1::text::uuid \
             ORDER BY source.chain_scope",
            &[&namespace],
        )
        .await
        .unwrap();
    assert_eq!(provenance_rows.len(), 3);
    assert_eq!(provenance_rows[0].get::<_, String>(0), "base");
    assert_eq!(provenance_rows[0].get::<_, String>(1), provenance);
    assert_eq!(provenance_rows[1].get::<_, String>(0), "ethereum");
    assert_ne!(provenance_rows[1].get::<_, String>(1), provenance);
    assert_eq!(
        provenance_rows[1].get::<_, String>(2),
        "x402-facilitator-registry"
    );
    assert_eq!(
        provenance_rows[1].get::<_, Option<String>>(3).as_deref(),
        Some("ethereum")
    );
    assert_eq!(provenance_rows[2].get::<_, String>(0), "tempo");
    assert_ne!(provenance_rows[2].get::<_, String>(1), provenance);
    assert_eq!(
        provenance_rows[2].get::<_, String>(2),
        "x402-facilitator-registry"
    );
    assert_eq!(
        provenance_rows[2].get::<_, Option<String>>(3).as_deref(),
        Some("tempo")
    );

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

#[tokio::test]
async fn terminal_x402_verifier_failure_persists_failed_coverage_receipt() {
    let database_url = std::env::var("AEM_COLLECT_TEST_DATABASE_URL")
        .expect("AEM_COLLECT_TEST_DATABASE_URL is required for x402 coverage tests");
    let (admin, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("connect to disposable PostgreSQL");
    tokio::spawn(async move {
        connection.await.expect("PostgreSQL connection");
    });
    admin
        .query_one("SELECT pg_advisory_lock($1)", &[&ROLE_TEST_LOCK])
        .await
        .unwrap();

    let ids = admin
        .query_one(
            "SELECT gen_random_uuid()::text, gen_random_uuid()::text, \
                    gen_random_uuid()::text, gen_random_uuid()::text, gen_random_uuid()::text",
            &[],
        )
        .await
        .unwrap();
    let namespace: String = ids.get(0);
    let provenance: String = ids.get(1);
    let job: String = ids.get(2);
    let pending: String = ids.get(3);
    let verifier_token: String = ids.get(4);
    let run_key = namespace.replace('-', "");
    admin
        .batch_execute(&format!(
            r#"
            ALTER ROLE agent_economy_evidence_verifier_runtime LOGIN
                PASSWORD '{ROLE_TEST_PASSWORD}';
            DELETE FROM agent_economy.collection_runtime_namespaces
             WHERE login_name = 'agent_economy_evidence_verifier_runtime';
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'x402-failure-{run_key}');
            INSERT INTO agent_economy.collection_runtime_namespaces
                (login_name, namespace_id, purpose)
            VALUES ('agent_economy_evidence_verifier_runtime', '{namespace}', 'verify-evidence');
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
                '{namespace}', 'x402-facilitator-base', 'base', 40, now(), 49,
                'x402scan-5732bb83c8c4290f413f24318a838e3fee4ed2bf', repeat('b', 64),
                10, 60, '{provenance}');
            INSERT INTO agent_economy.worker_jobs(
                namespace_id, job_id, mode, job_kind, idempotency_key, input_sha256, output_sha256,
                status, max_attempts, scheduled_for, collection_chain_scope,
                collection_source_id, collection_start_height, collection_end_height,
                collection_acquisition_contract, collection_evidence_contract,
                collection_registry_version, collection_registry_sha256)
            VALUES ('{namespace}', '{job}', 'collect', 'chain-protocol-range', '{job}',
                    repeat('c', 64), repeat('d', 64), 'succeeded', 3, now(), 'base',
                    'x402-facilitator-base', 40, 49, 'facilitator-rpc-block-v1',
                    'evidence-store-create-read-sha256-v1',
                    'x402scan-5732bb83c8c4290f413f24318a838e3fee4ed2bf', repeat('b', 64));
            INSERT INTO agent_economy.pending_collection_batches(
                namespace_id, pending_id, job_id, input_sha256, batch_sha256,
                chain_scope, source_id, observed_at_unix_ms, start_height, end_height,
                evidence_json, status, attempt_count, max_attempts, verifier_owner,
                verifier_token, verifier_expires_at)
            VALUES ('{namespace}', '{pending}', '{job}', repeat('c', 64), repeat('d', 64),
                    'base', 'x402-facilitator-base', 1700000000000, 40, 49, '[]',
                    'verifying', 1, 3, 'terminal-verifier', '{verifier_token}',
                    clock_timestamp() + interval '2 minutes');
            "#
        ))
        .await
        .unwrap();

    let verifier = connect_as(&database_url, "agent_economy_evidence_verifier_runtime").await;
    let failed: bool = verifier
        .query_one(
            "SELECT agent_economy.fail_pending_collection_batch(\
             $1::text::uuid, 'terminal-verifier', $2::text::uuid, \
             'invalid_evidence_readback', false)",
            &[&pending, &verifier_token],
        )
        .await
        .unwrap()
        .get(0);
    assert!(failed);

    let receipt = admin
        .query_one(
            "SELECT state, failure_code, frozen_upper_coordinate, gap_count, \
                    indexed_through_coordinate, evidence_manifest_sha256 \
             FROM agent_economy.x402_history_ranges \
             WHERE namespace_id=$1::text::uuid AND source_id='x402-facilitator-base'",
            &[&namespace],
        )
        .await
        .expect("terminal failure must leave an immutable coverage receipt");
    assert_eq!(receipt.get::<_, String>(0), "failed");
    assert_eq!(receipt.get::<_, String>(1), "invalid_evidence_readback");
    assert_eq!(receipt.get::<_, i64>(2), 49);
    assert_eq!(receipt.get::<_, i64>(3), 1);
    assert_eq!(receipt.get::<_, Option<i64>>(4), None);
    assert_eq!(receipt.get::<_, String>(5), "d".repeat(64));
}
