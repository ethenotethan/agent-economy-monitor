use std::{fs, process::Command};

use base64::{Engine as _, engine::general_purpose::STANDARD};

use sha2::{Digest, Sha256};
use tokio_postgres::NoTls;

const NAMESPACE: &str = "00000000-0000-0000-0000-000000000048";
const ADVERSARIAL_NAMESPACE: &str = "00000000-0000-0000-0000-000000000049";

fn manifest() -> Vec<u8> {
    let payment = br#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"11","asset":"USDC","payTo":"0x1111111111111111111111111111111111111111","resource":"https://merchant.example/paid"}]}"#;
    let response = format!(
        "HTTP/1.1 402 Payment Required\r\npayment-required: {}\r\n\r\n",
        STANDARD.encode(payment)
    );
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "chain": "base",
        "source_id": "alchemy-base",
        "observed_at_unix_ms": 1790986800000_i64,
        "observed_date": "2026-10-03",
        "start_height": 700,
        "end_height": 700,
        "inputs": [
            {
                "height": 700,
                "kind": "x402_runtime",
                "context": "https://merchant.example/paid",
                "evidence_base64": STANDARD.encode(response)
            }
        ]
    }))
    .unwrap()
}

#[tokio::test]
async fn binary_leases_collect_job_and_atomically_commits_replayable_evidence_observation_and_cursor()
 {
    let Ok(database_url) = std::env::var("AEM_COLLECT_TEST_DATABASE_URL") else {
        eprintln!(
            "AEM_COLLECT_TEST_DATABASE_URL is unset; canonical verify runs this with PostgreSQL"
        );
        return;
    };
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
        .execute(
            "INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key) \
             VALUES ($1::text::uuid, 'tenant', 'collect-binary-test') ON CONFLICT DO NOTHING",
            &[&NAMESPACE],
        )
        .await
        .unwrap();
    client
        .batch_execute(&format!(
            "DELETE FROM agent_economy.worker_jobs WHERE namespace_id = '{NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.observations WHERE namespace_id = '{NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.provenance_records WHERE namespace_id = '{NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.evidence_objects WHERE namespace_id = '{NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.collection_range_receipts WHERE namespace_id = '{NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.collection_cursors WHERE namespace_id = '{NAMESPACE}'::uuid;"
        ))
        .await
        .unwrap();
    client
        .batch_execute(
            "ALTER ROLE agent_economy_collector_runtime PASSWORD 'collector_test_password'",
        )
        .await
        .unwrap();
    let collector_database_url = collector_database_url(&database_url);

    let root = std::env::current_dir()
        .unwrap()
        .join("target/collect-worker-postgres-test");
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("inputs")).unwrap();
    let root = fs::canonicalize(root).unwrap();
    let bytes = manifest();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(root.join("inputs").join(format!("{digest}.json")), bytes).unwrap();
    fs::write(
        root.join("base-700.json"),
        br#"{"jsonrpc":"2.0","id":1,"result":{"number":"0x2bc"}}"#,
    )
    .unwrap();

    for (job_id, idempotency_key) in [
        ("00000000-0000-0000-0000-000000000148", "collect-first"),
        ("00000000-0000-0000-0000-000000000248", "collect-replay"),
    ] {
        client
            .execute(
                "INSERT INTO agent_economy.worker_jobs \
                   (namespace_id, job_id, mode, job_kind, idempotency_key, input_sha256) \
                 VALUES ($1::text::uuid, $2::text::uuid, 'collect', 'chain-protocol-range', $3, $4)",
                &[&NAMESPACE, &job_id, &idempotency_key, &digest],
            )
            .await
            .unwrap();
        let output = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
            .arg("collect")
            .env("COLLECTOR_DATABASE_URL", &collector_database_url)
            .env("NAMESPACE_ID", NAMESPACE)
            .env("COLLECTION_INPUT_ROOT", root.join("inputs"))
            .env("EVIDENCE_ROOT", root.join("evidence"))
            .env("COLLECTION_RPC_REPLAY_ROOT", &root)
            .env("COLLECT_LEASE_OWNER", "collect-binary-test")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "binary failed without logging fixture payloads: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }

    let mut additive: serde_json::Value = serde_json::from_slice(&manifest()).unwrap();
    additive["inputs"]
        .as_array_mut()
        .unwrap()
        .push(serde_json::json!({
            "height": 700,
            "kind": "chain_transfer",
            "context": "tx:base:additive-replay",
            "evidence_base64": STANDARD.encode(br#"{"value":"1"}"#)
        }));
    let mut mutated: serde_json::Value = serde_json::from_slice(&manifest()).unwrap();
    mutated["inputs"][0]["evidence_base64"] = serde_json::Value::String(STANDARD.encode(
        "HTTP/1.1 402 Payment Required\r\npayment-required: eyJ4NDAyVmVyc2lvbiI6MiwiYWNjZXB0cyI6W3sic2NoZW1lIjoiZXhhY3QiLCJuZXR3b3JrIjoiZWlwMTU1Ojg0NTMiLCJhbW91bnQiOiIxMiIsImFzc2V0IjoiVVNEQyIsInBheVRvIjoiMHgxMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExMTExIiwicmVzb3VyY2UiOiJodHRwczovL21lcmNoYW50LmV4YW1wbGUvcGFpZCJ9XX0=\r\n\r\n",
    ));
    let mut partial: serde_json::Value = serde_json::from_slice(&manifest()).unwrap();
    partial["inputs"] = serde_json::json!([{
        "height": 700,
        "kind": "chain_transfer",
        "context": "tx:base:partial-replay",
        "evidence_base64": STANDARD.encode(br#"{"value":"1"}"#)
    }]);

    for (job_id, idempotency_key, candidate) in [
        (
            "00000000-0000-0000-0000-000000000348",
            "collect-additive-replay",
            additive,
        ),
        (
            "00000000-0000-0000-0000-000000000448",
            "collect-mutated-replay",
            mutated,
        ),
        (
            "00000000-0000-0000-0000-000000000548",
            "collect-partial-replay",
            partial,
        ),
    ] {
        let bytes = serde_json::to_vec(&candidate).unwrap();
        let candidate_digest = format!("{:x}", Sha256::digest(&bytes));
        fs::write(
            root.join("inputs").join(format!("{candidate_digest}.json")),
            bytes,
        )
        .unwrap();
        client
            .execute(
                "INSERT INTO agent_economy.worker_jobs \
                   (namespace_id, job_id, mode, job_kind, idempotency_key, input_sha256) \
                 VALUES ($1::text::uuid, $2::text::uuid, \
                         'collect', 'chain-protocol-range', $3, $4)",
                &[&NAMESPACE, &job_id, &idempotency_key, &candidate_digest],
            )
            .await
            .unwrap();
        let rejected = Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
            .arg("collect")
            .env("COLLECTOR_DATABASE_URL", &collector_database_url)
            .env("NAMESPACE_ID", NAMESPACE)
            .env("COLLECTION_INPUT_ROOT", root.join("inputs"))
            .env("EVIDENCE_ROOT", root.join("evidence"))
            .env("COLLECTION_RPC_REPLAY_ROOT", &root)
            .env("COLLECT_LEASE_OWNER", "collect-binary-test")
            .output()
            .unwrap();
        assert!(
            !rejected.status.success(),
            "{idempotency_key} unexpectedly changed an immutable range receipt"
        );
    }

    let state = client
        .query_one(
            "SELECT \
               (SELECT count(*) FROM agent_economy.evidence_objects WHERE namespace_id = $1::text::uuid), \
               (SELECT count(*) FROM agent_economy.provenance_records WHERE namespace_id = $1::text::uuid), \
               (SELECT count(*) FROM agent_economy.observations WHERE namespace_id = $1::text::uuid), \
               (SELECT next_height FROM agent_economy.collection_cursors \
                WHERE namespace_id = $1::text::uuid AND chain_scope = 'base' \
                  AND source_id = 'alchemy-base'), \
               (SELECT count(*) FROM agent_economy.worker_jobs \
                WHERE namespace_id = $1::text::uuid AND status = 'succeeded'), \
               (SELECT count(*) FROM agent_economy.collection_range_receipts \
                WHERE namespace_id = $1::text::uuid)",
            &[&NAMESPACE],
        )
        .await
        .unwrap();
    assert_eq!(state.get::<_, i64>(0), 2);
    assert_eq!(state.get::<_, i64>(1), 1);
    assert_eq!(state.get::<_, i64>(2), 1);
    assert_eq!(state.get::<_, i64>(3), 701);
    assert_eq!(state.get::<_, i64>(4), 2);
    assert_eq!(state.get::<_, i64>(5), 1);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn restricted_worker_cannot_forge_canonical_evidence_through_security_definer() {
    let Ok(database_url) = std::env::var("AEM_COLLECT_TEST_DATABASE_URL") else {
        eprintln!(
            "AEM_COLLECT_TEST_DATABASE_URL is unset; canonical verify runs this with PostgreSQL"
        );
        return;
    };
    let (mut client, connection) = tokio_postgres::connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
        .batch_execute(&format!(
            "INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key) \
             VALUES ('{ADVERSARIAL_NAMESPACE}'::uuid, 'tenant', 'collect-adversarial-test') \
             ON CONFLICT DO NOTHING; \
             DELETE FROM agent_economy.worker_jobs WHERE namespace_id = '{ADVERSARIAL_NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.observations WHERE namespace_id = '{ADVERSARIAL_NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.provenance_records WHERE namespace_id = '{ADVERSARIAL_NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.evidence_objects WHERE namespace_id = '{ADVERSARIAL_NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.collection_range_receipts WHERE namespace_id = '{ADVERSARIAL_NAMESPACE}'::uuid; \
             DELETE FROM agent_economy.collection_cursors WHERE namespace_id = '{ADVERSARIAL_NAMESPACE}'::uuid; \
             INSERT INTO agent_economy.worker_jobs \
               (namespace_id, job_id, mode, job_kind, idempotency_key, input_sha256) \
             VALUES ('{ADVERSARIAL_NAMESPACE}'::uuid, \
                     '00000000-0000-0000-0000-000000000149'::uuid, \
                     'collect', 'chain-protocol-range', 'collect-forgery', repeat('a', 64));"
        ))
        .await
        .unwrap();
    let transaction = client.transaction().await.unwrap();
    transaction
        .batch_execute("SET LOCAL ROLE agent_economy_worker")
        .await
        .unwrap();
    let lease = transaction
        .query_one(
            "SELECT lease_token::text \
             FROM agent_economy.claim_worker_job($1::text::uuid, 'collect', 'adversary', 300)",
            &[&ADVERSARIAL_NAMESPACE],
        )
        .await
        .unwrap();
    let lease_token: String = lease.get(0);
    let forged_evidence = serde_json::json!([{
        "evidence_id": format!("evidence:sha256:{}", "b".repeat(64)),
        "sha256": "c".repeat(64),
        "storage_uri": "forged/private/object",
        "media_type": "application/json",
        "byte_length": 1,
        "height": 900
    }]);
    let result = transaction
        .query_one(
            "SELECT agent_economy.commit_collection_batch( \
               $1::text::uuid, '00000000-0000-0000-0000-000000000149'::uuid, \
               'adversary', $2::text::uuid, repeat('a', 64), repeat('d', 64), \
               'base', 'adversarial-source', 1790986800000, 900, 900, \
               $3::text::jsonb, '[]'::jsonb)",
            &[
                &ADVERSARIAL_NAMESPACE,
                &lease_token,
                &forged_evidence.to_string(),
            ],
        )
        .await;

    assert!(result.is_err(), "forged evidence unexpectedly committed");
    transaction.rollback().await.unwrap();
    let count: i64 = client
        .query_one(
            "SELECT count(*) FROM agent_economy.evidence_objects \
             WHERE namespace_id = $1::text::uuid",
            &[&ADVERSARIAL_NAMESPACE],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(count, 0);

    let permissions = client
        .query_one(
            "SELECT \
               has_function_privilege('agent_economy_worker', \
                 'agent_economy.commit_collection_batch(uuid,uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,jsonb)', \
                 'EXECUTE'), \
               has_function_privilege('agent_economy_collector_runtime', \
                 'agent_economy.commit_collection_batch(uuid,uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb,jsonb)', \
                 'EXECUTE')",
            &[],
        )
        .await
        .unwrap();
    assert!(!permissions.get::<_, bool>(0));
    assert!(permissions.get::<_, bool>(1));
}

fn collector_database_url(database_url: &str) -> String {
    let (scheme, rest) = database_url
        .split_once("://")
        .expect("test database URL has a scheme");
    let (_, host_and_path) = rest
        .split_once('@')
        .expect("test database URL has explicit credentials");
    format!("{scheme}://agent_economy_collector_runtime:collector_test_password@{host_and_path}")
}
