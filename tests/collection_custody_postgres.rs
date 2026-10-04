use std::{fs, str::FromStr, sync::Arc};

use agent_economy_evidence_store::{
    EvidenceContext, EvidenceProvenance, EvidenceStore, FilesystemEvidenceStore,
};
use agent_economy_monitor::{
    collect::{ArchivedEvidence, verify_replayed_evidence},
    verify_evidence::PostgresEvidenceVerifier,
};
use agent_economy_rpc_collector::{Chain, RawRpcResponse, RpcEvidence};
use serde_json::json;
use tokio_postgres::{Config, NoTls};

const NAMESPACE_A: &str = "00000000-0000-0000-0000-000000009836";
const NAMESPACE_B: &str = "00000000-0000-0000-0000-000000009837";
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

fn rpc_evidence(height: u64) -> Vec<u8> {
    rpc_evidence_for(Chain::Base, height)
}

fn rpc_evidence_for(chain: Chain, height: u64) -> Vec<u8> {
    let result = if chain == Chain::Solana {
        json!({
            "blockHeight": height,
            "blockhash": format!("block-{height}"),
            "parentSlot": height.saturating_sub(1),
            "previousBlockhash": "block-parent",
            "transactions": []
        })
    } else {
        json!({"number": format!("0x{height:x}"), "transactions": []})
    };
    let body = serde_json::to_vec(&json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result
    }))
    .unwrap();
    let response = RawRpcResponse::try_new(200, body).unwrap();
    RpcEvidence::from_response(chain, height, 1, &response).encode()
}

async fn insert_chain_job(
    admin: &tokio_postgres::Client,
    job: &str,
    input: &str,
    chain: &str,
    source: &str,
    height: i64,
) {
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs(\
             namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256,status,max_attempts,scheduled_for,\
             collection_chain_scope,collection_source_id,collection_start_height,collection_end_height,\
             collection_acquisition_contract,collection_evidence_contract) \
             VALUES ($1::text::uuid,$2::text::uuid,'collect','chain-protocol-range',$2,$3,'pending',3,clock_timestamp(),\
             $4,$5,$6,$6,'alchemy-rpc-block-v1','evidence-store-create-read-sha256-v1')",
            &[&NAMESPACE_A, &job, &input, &chain, &source, &height],
        )
        .await
        .unwrap();
}

#[allow(clippy::too_many_arguments)]
async fn stage_chain_and_complete(
    collector: &tokio_postgres::Client,
    job: &str,
    input: &str,
    batch: &str,
    chain: &str,
    source: &str,
    height: i64,
    evidence: &str,
) {
    let claimed = collector
        .query_one(
            "SELECT job_id::text, lease_token::text FROM agent_economy.claim_bound_collection_job('collector-test',120)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(claimed.get::<_, String>(0), job);
    let token = claimed.get::<_, String>(1);
    collector
        .query_one(
            "SELECT agent_economy.stage_collection_batch(\
             $1::text::uuid,'collector-test',$2::text::uuid,$3,$4,$5,$6,\
             1700000000000,$7,$7,$8::text::jsonb)",
            &[
                &job, &token, &input, &batch, &chain, &source, &height, &evidence,
            ],
        )
        .await
        .unwrap();
    assert!(
        collector
            .query_one(
                "SELECT agent_economy.complete_bound_collection_job(\
             $1::text::uuid,'collector-test',$2::text::uuid,$3)",
                &[&job, &token, &batch],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
}

async fn insert_job(
    admin: &tokio_postgres::Client,
    namespace: &str,
    job: &str,
    input: &str,
    height: i64,
) {
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs(\
             namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256,status,max_attempts,scheduled_for,\
             collection_chain_scope,collection_source_id,collection_start_height,collection_end_height,\
             collection_acquisition_contract,collection_evidence_contract) \
             VALUES ($1::text::uuid,$2::text::uuid,'collect','chain-protocol-range',$2,$3,'pending',3,clock_timestamp(),\
             'base','alchemy-base',$4,$4,'alchemy-rpc-block-v1','evidence-store-create-read-sha256-v1')",
            &[&namespace, &job, &input, &height],
        )
        .await
        .unwrap();
}

async fn stage_and_complete(
    collector: &tokio_postgres::Client,
    job: &str,
    input: &str,
    batch: &str,
    height: i64,
    evidence: &str,
) {
    let claimed = collector
        .query_one(
            "SELECT job_id::text, lease_token::text \
             FROM agent_economy.claim_bound_collection_job('collector-test',120)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(claimed.get::<_, String>(0), job);
    let token = claimed.get::<_, String>(1);
    collector
        .query_one(
            "SELECT agent_economy.stage_collection_batch(\
            $1::text::uuid,'collector-test',$2::text::uuid,$3,$4,'base','alchemy-base',\
            1700000000000,$5,$5,$6::text::jsonb)",
            &[&job, &token, &input, &batch, &height, &evidence],
        )
        .await
        .unwrap();
    assert!(
        collector
            .query_one(
                "SELECT agent_economy.complete_bound_collection_job(\
                 $1::text::uuid,'collector-test',$2::text::uuid,$3)",
                &[&job, &token, &batch],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
}

#[tokio::test]
async fn split_runtimes_reject_fake_readback_and_promote_real_object() {
    let Ok(database_url) = std::env::var("AEM_COLLECT_TEST_DATABASE_URL") else {
        eprintln!(
            "AEM_COLLECT_TEST_DATABASE_URL is unset; canonical verify runs this with PostgreSQL"
        );
        return;
    };
    let (admin, admin_connection) = tokio_postgres::connect(&database_url, NoTls).await.unwrap();
    tokio::spawn(async move { admin_connection.await.unwrap() });
    admin
        .query_one("SELECT pg_advisory_lock($1)", &[&ROLE_TEST_LOCK])
        .await
        .unwrap();
    admin
        .batch_execute(&format!(
            r#"ALTER ROLE agent_economy_collector_runtime LOGIN PASSWORD 'aem-custody-test-password';
                ALTER ROLE agent_economy_evidence_verifier_runtime LOGIN PASSWORD 'aem-custody-test-password';
                DELETE FROM agent_economy.collection_runtime_namespaces
                 WHERE login_name IN ('agent_economy_collector_runtime','agent_economy_evidence_verifier_runtime');
                INSERT INTO agent_economy.namespaces(namespace_id,namespace_kind,namespace_key) VALUES
                 ('{NAMESPACE_A}','tenant','custody-9836'),('{NAMESPACE_B}','tenant','custody-9837')
                 ON CONFLICT DO NOTHING;
                INSERT INTO agent_economy.collection_runtime_namespaces(login_name,namespace_id,purpose) VALUES
                 ('agent_economy_collector_runtime','{NAMESPACE_A}','collect'),
                 ('agent_economy_evidence_verifier_runtime','{NAMESPACE_A}','verify-evidence');"#
        ))
        .await
        .unwrap();

    let collector = connect_as(&database_url, "agent_economy_collector_runtime").await;
    assert!(
        collector
            .query_one(
                "SELECT agent_economy.bound_collection_namespace('collect')",
                &[],
            )
            .await
            .is_ok(),
        "clean collector authority must pass before one-fault mutations"
    );
    for (mutation, restoration, label) in [
        (
            "GRANT INSERT ON agent_economy.evidence_objects TO agent_economy_collector_runtime",
            "REVOKE INSERT ON agent_economy.evidence_objects FROM agent_economy_collector_runtime",
            "direct INSERT",
        ),
        (
            "GRANT SELECT ON agent_economy.evidence_objects TO agent_economy_collector_runtime",
            "REVOKE SELECT ON agent_economy.evidence_objects FROM agent_economy_collector_runtime",
            "direct SELECT",
        ),
        (
            "GRANT agent_economy_collector_runtime TO agent_economy_evidence_verifier_runtime WITH INHERIT FALSE, SET TRUE",
            "REVOKE agent_economy_collector_runtime FROM agent_economy_evidence_verifier_runtime",
            "unexpected membership",
        ),
        (
            "GRANT SELECT ON agent_economy.evidence_objects TO PUBLIC",
            "REVOKE SELECT ON agent_economy.evidence_objects FROM PUBLIC",
            "PUBLIC exposure",
        ),
    ] {
        admin.batch_execute(mutation).await.unwrap();
        let rejected = collector
            .query_one(
                "SELECT agent_economy.bound_collection_namespace('collect')",
                &[],
            )
            .await;
        admin.batch_execute(restoration).await.unwrap();
        assert!(rejected.is_err(), "startup boundary accepted {label}");
        assert!(
            collector
                .query_one(
                    "SELECT agent_economy.bound_collection_namespace('collect')",
                    &[],
                )
                .await
                .is_ok(),
            "baseline did not recover after {label}"
        );
    }
    admin
        .batch_execute(
            "CREATE FUNCTION agent_economy.boundary_attack(integer) RETURNS integer \
             LANGUAGE sql IMMUTABLE AS 'SELECT $1'; \
             REVOKE ALL ON FUNCTION agent_economy.boundary_attack(integer) FROM PUBLIC; \
             GRANT EXECUTE ON FUNCTION agent_economy.boundary_attack(integer) \
             TO agent_economy_collector_runtime",
        )
        .await
        .unwrap();
    let routine_rejected = collector
        .query_one(
            "SELECT agent_economy.bound_collection_namespace('collect')",
            &[],
        )
        .await;
    admin
        .batch_execute("DROP FUNCTION agent_economy.boundary_attack(integer)")
        .await
        .unwrap();
    assert!(
        routine_rejected.is_err(),
        "startup boundary accepted a rogue routine grant"
    );

    let owner: String = admin
        .query_one(
            "SELECT r.rolname FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace \
             JOIN pg_roles r ON r.oid=c.relowner \
             WHERE n.nspname='agent_economy' AND c.relname='evidence_objects'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
    );
    admin
        .batch_execute(
            "ALTER TABLE agent_economy.evidence_objects OWNER TO agent_economy_collector_runtime",
        )
        .await
        .unwrap();
    let ownership_rejected = collector
        .query_one(
            "SELECT agent_economy.bound_collection_namespace('collect')",
            &[],
        )
        .await;
    admin
        .batch_execute(&format!(
            "ALTER TABLE agent_economy.evidence_objects OWNER TO {owner}"
        ))
        .await
        .unwrap();
    assert!(
        ownership_rejected.is_err(),
        "startup boundary accepted protected ownership"
    );
    let fake_job = "10000000-0000-0000-0000-000000009836";
    let fake_input = "11".repeat(32);
    let fake_batch = "22".repeat(32);
    let fake_sha = "33".repeat(32);
    insert_job(&admin, NAMESPACE_A, fake_job, &fake_input, 41).await;
    let fake_evidence = json!([{
        "evidence_id": format!("evidence:sha256:{fake_sha}"),
        "sha256": fake_sha,
        "storage_uri": format!("evidence/alchemy-base/2023-11-14/sha256/33/{fake_sha}"),
        "storage_generation": null,
        "media_type": "application/vnd.agent-economy.rpc",
        "byte_length": 128,
        "height": 41
    }])
    .to_string();
    stage_and_complete(
        &collector,
        fake_job,
        &fake_input,
        &fake_batch,
        41,
        &fake_evidence,
    )
    .await;

    let root = std::env::current_dir()
        .unwrap()
        .join(format!("target/aem-custody-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let store = Arc::new(FilesystemEvidenceStore::open(&root).unwrap());
    let verifier_client =
        connect_as(&database_url, "agent_economy_evidence_verifier_runtime").await;
    let verifier =
        PostgresEvidenceVerifier::new(verifier_client, store.clone(), "verifier-fake".into());
    assert!(verifier.run_once().await.is_err());
    let canonical_after_fake: i64 = admin
        .query_one(
            "SELECT count(*) FROM agent_economy.collection_range_receipts \
             WHERE namespace_id=$1::text::uuid",
            &[&NAMESPACE_A],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(canonical_after_fake, 0);

    let real_job = "20000000-0000-0000-0000-000000009836";
    let real_input = "44".repeat(32);
    let bytes = rpc_evidence(42);
    let context = EvidenceContext::new("alchemy-base", "2023-11-14")
        .unwrap()
        .with_provenance(
            EvidenceProvenance::new(
                "rpc-evidence-v1",
                "custody-test",
                [
                    ("attempt", "1"),
                    ("chain", "base"),
                    ("height", "42"),
                    ("http-status", "200"),
                    ("method", "eth_getBlockByNumber"),
                    ("provider", "alchemy"),
                ],
            )
            .unwrap(),
        );
    let receipt = EvidenceStore::create_only(store.as_ref(), &context, &bytes)
        .await
        .unwrap();
    let sha = receipt.object.sha256();
    let archived = ArchivedEvidence::from_verified_readback(
        receipt.object.name().to_owned(),
        sha.clone(),
        "application/vnd.agent-economy.rpc".into(),
        42,
        bytes,
    )
    .unwrap();
    let batch = verify_replayed_evidence(
        "base".into(),
        "alchemy-base".into(),
        1700000000000,
        42,
        42,
        vec![archived],
    )
    .unwrap()
    .output_sha256();
    let forged_generation_job = "15000000-0000-0000-0000-000000009836";
    let forged_generation_input = "43".repeat(32);
    let forged_generation_evidence = json!([{
        "evidence_id": format!("evidence:sha256:{sha}"),
        "sha256": sha,
        "storage_uri": receipt.object.name(),
        "storage_generation": "attacker-asserted-generation-999",
        "media_type": "application/vnd.agent-economy.rpc",
        "byte_length": EvidenceStore::read(store.as_ref(), &receipt.object).await.unwrap().len(),
        "height": 42
    }])
    .to_string();
    insert_job(
        &admin,
        NAMESPACE_A,
        forged_generation_job,
        &forged_generation_input,
        42,
    )
    .await;
    stage_and_complete(
        &collector,
        forged_generation_job,
        &forged_generation_input,
        &batch,
        42,
        &forged_generation_evidence,
    )
    .await;

    let verifier_client =
        connect_as(&database_url, "agent_economy_evidence_verifier_runtime").await;
    let verifier = PostgresEvidenceVerifier::new(
        verifier_client,
        store.clone(),
        "verifier-forged-generation".into(),
    );
    assert!(
        verifier.run_once().await.is_err(),
        "filesystem readback must reject a caller-asserted storage generation"
    );
    let state_after_forged_generation = admin
        .query_one(
            "SELECT\
              (SELECT count(*) FROM agent_economy.collection_range_receipts WHERE namespace_id=$1::text::uuid),\
              (SELECT count(*) FROM agent_economy.evidence_objects WHERE namespace_id=$1::text::uuid),\
              (SELECT count(*) FROM agent_economy.observations WHERE namespace_id=$1::text::uuid),\
              (SELECT count(*) FROM agent_economy.collection_cursors WHERE namespace_id=$1::text::uuid)",
            &[&NAMESPACE_A],
        )
        .await
        .unwrap();
    assert_eq!(state_after_forged_generation.get::<_, i64>(0), 0);
    assert_eq!(state_after_forged_generation.get::<_, i64>(1), 0);
    assert_eq!(state_after_forged_generation.get::<_, i64>(2), 0);
    assert_eq!(state_after_forged_generation.get::<_, i64>(3), 0);

    let real_evidence = json!([{
        "evidence_id": format!("evidence:sha256:{sha}"),
        "sha256": sha,
        "storage_uri": receipt.object.name(),
        "storage_generation": null,
        "media_type": "application/vnd.agent-economy.rpc",
        "byte_length": EvidenceStore::read(store.as_ref(), &receipt.object).await.unwrap().len(),
        "height": 42
    }])
    .to_string();
    insert_job(&admin, NAMESPACE_A, real_job, &real_input, 42).await;
    stage_and_complete(
        &collector,
        real_job,
        &real_input,
        &batch,
        42,
        &real_evidence,
    )
    .await;

    let verifier_client =
        connect_as(&database_url, "agent_economy_evidence_verifier_runtime").await;
    let verifier =
        PostgresEvidenceVerifier::new(verifier_client, store.clone(), "verifier-real".into());
    assert!(verifier.run_once().await.unwrap().is_some());
    let state = admin
        .query_one(
            "SELECT\
              (SELECT count(*) FROM agent_economy.collection_range_receipts WHERE namespace_id=$1::text::uuid),\
              (SELECT next_height FROM agent_economy.collection_cursors WHERE namespace_id=$1::text::uuid AND chain_scope='base' AND source_id='alchemy-base')",
            &[&NAMESPACE_A],
        )
        .await
        .unwrap();
    assert_eq!(state.get::<_, i64>(0), 1);
    assert_eq!(state.get::<_, i64>(1), 43);

    for (chain, chain_name, source, job, input, height) in [
        (
            Chain::Ethereum,
            "ethereum",
            "alchemy-ethereum",
            "21000000-0000-0000-0000-000000009836",
            "51".repeat(32),
            43_i64,
        ),
        (
            Chain::Solana,
            "solana",
            "alchemy-solana",
            "22000000-0000-0000-0000-000000009836",
            "52".repeat(32),
            44_i64,
        ),
        (
            Chain::Tempo,
            "tempo",
            "alchemy-tempo",
            "23000000-0000-0000-0000-000000009836",
            "53".repeat(32),
            45_i64,
        ),
    ] {
        let bytes = rpc_evidence_for(chain, height as u64);
        let observation_id = format!("custody-test-{chain_name}");
        let height_text = height.to_string();
        let context = EvidenceContext::new(source, "2023-11-14")
            .unwrap()
            .with_provenance(
                EvidenceProvenance::new(
                    "rpc-evidence-v1",
                    observation_id.as_str(),
                    [
                        ("attempt", "1"),
                        ("chain", chain_name),
                        ("height", height_text.as_str()),
                        ("http-status", "200"),
                        (
                            "method",
                            if chain == Chain::Solana {
                                "getBlock"
                            } else {
                                "eth_getBlockByNumber"
                            },
                        ),
                        ("provider", "alchemy"),
                    ],
                )
                .unwrap(),
            );
        let receipt = EvidenceStore::create_only(store.as_ref(), &context, &bytes)
            .await
            .unwrap();
        let sha = receipt.object.sha256();
        let archived = ArchivedEvidence::from_verified_readback(
            receipt.object.name().to_owned(),
            sha.clone(),
            "application/vnd.agent-economy.rpc".into(),
            height as u64,
            bytes,
        )
        .unwrap();
        let batch = verify_replayed_evidence(
            chain_name.into(),
            source.into(),
            1700000000000,
            height as u64,
            height as u64,
            vec![archived],
        )
        .unwrap()
        .output_sha256();
        let evidence = json!([{
            "evidence_id": format!("evidence:sha256:{sha}"),
            "sha256": sha,
            "storage_uri": receipt.object.name(),
            "storage_generation": null,
            "media_type": "application/vnd.agent-economy.rpc",
            "byte_length": EvidenceStore::read(store.as_ref(), &receipt.object).await.unwrap().len(),
            "height": height
        }])
        .to_string();
        insert_chain_job(&admin, job, &input, chain_name, source, height).await;
        stage_chain_and_complete(
            &collector, job, &input, &batch, chain_name, source, height, &evidence,
        )
        .await;
        let verifier_client =
            connect_as(&database_url, "agent_economy_evidence_verifier_runtime").await;
        let verifier = PostgresEvidenceVerifier::new(
            verifier_client,
            store.clone(),
            format!("verifier-{chain_name}"),
        );
        assert!(verifier.run_once().await.unwrap().is_some());
    }
    let four_chain_receipts: i64 = admin
        .query_one(
            "SELECT count(*) FROM agent_economy.collection_range_receipts WHERE namespace_id=$1::text::uuid",
            &[&NAMESPACE_A],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(four_chain_receipts, 4);

    insert_job(
        &admin,
        NAMESPACE_B,
        "30000000-0000-0000-0000-000000009837",
        &"55".repeat(32),
        50,
    )
    .await;
    assert!(
        collector
            .query_opt(
                "SELECT job_id FROM agent_economy.claim_bound_collection_job('collector-cross',120)",
                &[],
            )
            .await
            .unwrap()
            .is_none(),
        "collector mapped to namespace A must not claim namespace B"
    );
    assert!(
        !collector
            .query_one(
                "SELECT has_function_privilege(current_user,'agent_economy.promote_pending_collection_batch(uuid,text,uuid,jsonb,text)','EXECUTE')",
                &[],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let verifier_acl = connect_as(&database_url, "agent_economy_evidence_verifier_runtime").await;
    assert!(
        !verifier_acl
            .query_one(
                "SELECT has_function_privilege(current_user,'agent_economy.claim_bound_collection_job(text,bigint)','EXECUTE')",
                &[],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );

    fs::remove_dir_all(root).unwrap();
    admin
        .batch_execute(
            r#"DELETE FROM agent_economy.collection_runtime_namespaces
               WHERE login_name IN ('agent_economy_collector_runtime','agent_economy_evidence_verifier_runtime');
               ALTER ROLE agent_economy_collector_runtime NOLOGIN PASSWORD NULL;
               ALTER ROLE agent_economy_evidence_verifier_runtime NOLOGIN PASSWORD NULL;"#,
        )
        .await
        .unwrap();
    admin
        .query_one("SELECT pg_advisory_unlock($1)", &[&ROLE_TEST_LOCK])
        .await
        .unwrap();
}
