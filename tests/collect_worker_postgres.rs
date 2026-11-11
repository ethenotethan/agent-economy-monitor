use std::{fs, process::Command};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, NoTls};

const COLLECTOR_PASSWORD: &str = "collector_test_password";
const VERIFIER_PASSWORD: &str = "verifier_test_password";

fn manifest_for_chain(chain: &str, height: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "chain": chain,
        "source_id": format!("alchemy-{chain}"),
        "observed_at_unix_ms": 1790986800000_i64,
        "observed_date": "2026-10-03",
        "start_height": height,
        "end_height": height,
        "inputs": [{
            "height": height,
            "kind": "chain_transfer",
            "context": format!("fixture:{chain}:{height}"),
            "evidence_base64": STANDARD.encode(br#"{"ordinary_transfer":true}"#)
        }]
    }))
    .unwrap()
}

fn rpc_height_fixture(chain: &str, height: u64) -> Vec<u8> {
    let result = if chain == "solana" {
        serde_json::json!({
            "blockHeight": height,
            "blockhash": format!("block-{height}"),
            "parentSlot": height.saturating_sub(1),
            "previousBlockhash": "block-parent",
            "transactions": []
        })
    } else {
        serde_json::json!({"number": format!("0x{height:x}"), "transactions": []})
    };
    serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result
    }))
    .unwrap()
}

fn runtime_database_url(database_url: &str, user: &str, password: &str) -> String {
    let (scheme, rest) = database_url
        .split_once("://")
        .expect("test database URL has a scheme");
    let (_, host_and_path) = rest
        .split_once('@')
        .expect("test database URL has explicit credentials");
    format!("{scheme}://{user}:{password}@{host_and_path}")
}

async fn connect(database_url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

async fn assert_catalog_stage_survives_worker_crash(admin: &Client, collector: &Client) {
    let run_key = u64::from(std::process::id()) * 10 + 9;
    let namespace_id = format!("00000000-0000-0000-0000-{run_key:012}");
    let job_id = format!("00000000-0000-0000-0001-{run_key:012}");
    let input_sha = "1".repeat(64);
    let batch_sha = "2".repeat(64);
    let evidence = serde_json::json!([
        {
            "evidence_id": format!("evidence:sha256:{}", "3".repeat(64)),
            "sha256": "3".repeat(64),
            "storage_uri": format!("evidence/cdp-x402-bazaar-base/2026-11-11/sha256/33/{}", "3".repeat(64)),
            "storage_generation": "1", "media_type": "application/json",
            "byte_length": 1, "height": 0
        },
        {
            "evidence_id": format!("evidence:sha256:{}", "4".repeat(64)),
            "sha256": "4".repeat(64),
            "storage_uri": format!("evidence/cdp-x402-bazaar-base/2026-11-11/sha256/44/{}", "4".repeat(64)),
            "storage_generation": "1", "media_type": "application/json",
            "byte_length": 1, "height": 1
        }
    ])
    .to_string();
    admin
        .execute(
            "INSERT INTO agent_economy.namespaces(namespace_id,namespace_kind,namespace_key) \
             VALUES ($1::text::uuid,'tenant',$2)",
            &[
                &namespace_id,
                &format!("catalog-crash-{}", std::process::id()),
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.collection_runtime_namespaces(login_name,namespace_id,purpose) \
             VALUES ('agent_economy_collector_runtime',$1::text::uuid,'collect')",
            &[&namespace_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
             namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256, \
             collection_chain_scope,collection_source_id,collection_start_height, \
             collection_end_height,collection_acquisition_contract,collection_evidence_contract) \
             VALUES ($1::text::uuid,$2::text::uuid,'collect','chain-protocol-range',$3,$4, \
             'base','cdp-x402-bazaar-base',0,0,'cdp-x402-bazaar-v1', \
             'evidence-store-create-read-sha256-v1')",
            &[
                &namespace_id,
                &job_id,
                &format!("catalog-crash-{run_key}"),
                &input_sha,
            ],
        )
        .await
        .unwrap();

    let first = collector
        .query_one(
            "SELECT lease_token::text FROM agent_economy.claim_bound_collection_job('catalog-crash-first',60)",
            &[],
        )
        .await
        .unwrap();
    let first_token: String = first.get(0);
    collector
        .query_one(
            "SELECT agent_economy.stage_catalog_crawl_batch( \
             $1::text::uuid,'catalog-crash-first',$2::text::uuid,$3,$4,'base', \
             'cdp-x402-bazaar-base',1794355200000,0,1,$5::text::jsonb,1,1)",
            &[&job_id, &first_token, &input_sha, &batch_sha, &evidence],
        )
        .await
        .unwrap();
    let staged_end: i64 = admin
        .query_one(
            "SELECT collection_end_height FROM agent_economy.worker_jobs \
             WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid",
            &[&namespace_id, &job_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(staged_end, 0, "staging changed the admitted job range");

    admin
        .execute(
            "UPDATE agent_economy.worker_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' \
             WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid",
            &[&namespace_id, &job_id],
        )
        .await
        .unwrap();
    let retry = collector
        .query_one(
            "SELECT lease_token::text FROM agent_economy.claim_bound_collection_job('catalog-crash-retry',60)",
            &[],
        )
        .await
        .unwrap();
    let retry_token: String = retry.get(0);
    collector
        .query_one(
            "SELECT agent_economy.stage_catalog_crawl_batch( \
             $1::text::uuid,'catalog-crash-retry',$2::text::uuid,$3,$4,'base', \
             'cdp-x402-bazaar-base',1794355200000,0,1,$5::text::jsonb,1,1)",
            &[&job_id, &retry_token, &input_sha, &batch_sha, &evidence],
        )
        .await
        .unwrap();
    let completed: bool = collector
        .query_one(
            "SELECT agent_economy.complete_bound_collection_job( \
             $1::text::uuid,'catalog-crash-retry',$2::text::uuid,$3)",
            &[&job_id, &retry_token, &batch_sha],
        )
        .await
        .unwrap()
        .get(0);
    assert!(completed);
    let completed_job = admin
        .query_one(
            "SELECT status,collection_end_height FROM agent_economy.worker_jobs \
             WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid",
            &[&namespace_id, &job_id],
        )
        .await
        .unwrap();
    assert_eq!(completed_job.get::<_, String>(0), "succeeded");
    assert_eq!(
        completed_job.get::<_, i64>(1),
        0,
        "completion changed the admitted job range"
    );
    admin
        .execute(
            "DELETE FROM agent_economy.collection_runtime_namespaces WHERE namespace_id=$1::text::uuid",
            &[&namespace_id],
        )
        .await
        .unwrap();
}

async fn assert_catalog_coverage_deduplicates_resources(admin: &Client) {
    let run_key = u64::from(std::process::id()) * 10 + 8;
    let namespace_id = format!("00000000-0000-0000-0000-{run_key:012}");
    let job_id = format!("00000000-0000-0000-0001-{run_key:012}");
    let first_provenance = format!("00000000-0000-0000-0002-{run_key:012}");
    let second_provenance = format!("00000000-0000-0000-0003-{run_key:012}");
    let input_sha = "1".repeat(64);
    let batch_sha = "2".repeat(64);
    let evidence_sha = "3".repeat(64);
    let evidence_id = format!("evidence:sha256:{evidence_sha}");
    let storage_uri = format!("evidence/cdp-x402-bazaar-base/2026-11-11/sha256/33/{evidence_sha}");
    let evidence = serde_json::json!([{
        "evidence_id": evidence_id,
        "sha256": evidence_sha,
        "storage_uri": storage_uri,
        "storage_generation": "1",
        "media_type": "application/json",
        "byte_length": 1,
        "height": 0
    }]);
    let resource_id = format!("sha256:{}", "c".repeat(64));
    let observations = serde_json::json!([
        {
            "observation_id": format!("sha256:{}", "a".repeat(64)),
            "protocol": "x402",
            "evidence_id": evidence_id,
            "observation_hash": "d".repeat(64),
            "parser_version": "x402-adapter@1",
            "height": 0,
            "catalog_resource_id": resource_id
        },
        {
            "observation_id": format!("sha256:{}", "b".repeat(64)),
            "protocol": "x402",
            "evidence_id": evidence_id,
            "observation_hash": "e".repeat(64),
            "parser_version": "x402-adapter@1",
            "height": 0,
            "catalog_resource_id": resource_id
        }
    ]);

    admin
        .execute(
            "INSERT INTO agent_economy.namespaces(namespace_id,namespace_kind,namespace_key) \
             VALUES ($1::text::uuid,'tenant',$2)",
            &[
                &namespace_id,
                &format!("catalog-coverage-{}", std::process::id()),
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
             namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256,output_sha256,status, \
             collection_chain_scope,collection_source_id,collection_start_height,collection_end_height, \
             collection_acquisition_contract,collection_evidence_contract) \
             VALUES ($1::text::uuid,$2::text::uuid,'collect','chain-protocol-range',$3,$4,$5,'succeeded', \
             'base','cdp-x402-bazaar-base',0,0,'cdp-x402-bazaar-v1', \
             'evidence-store-create-read-sha256-v1')",
            &[
                &namespace_id,
                &job_id,
                &format!("catalog-coverage-{run_key}"),
                &input_sha,
                &batch_sha,
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.catalog_crawl_staging( \
             namespace_id,job_id,source_reported_total,resources_observed,pages_archived) \
             VALUES ($1::text::uuid,$2::text::uuid,2,2,1)",
            &[&namespace_id, &job_id],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.pending_collection_batches( \
             namespace_id,job_id,input_sha256,batch_sha256,chain_scope,source_id, \
             observed_at_unix_ms,start_height,end_height,evidence_json,status,verifier_result_sha256) \
             VALUES ($1::text::uuid,$2::text::uuid,$3,$4,'base','cdp-x402-bazaar-base', \
             1794355200000,0,0,$5::text::jsonb,'succeeded',$6)",
            &[
                &namespace_id,
                &job_id,
                &input_sha,
                &batch_sha,
                &evidence.to_string(),
                &"f".repeat(64),
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.evidence_objects( \
             namespace_id,evidence_id,sha256,storage_uri,media_type,byte_length,observed_at) \
             VALUES ($1::text::uuid,$2,$3,$4,'application/json',1, \
             to_timestamp(1794355200000 / 1000.0))",
            &[&namespace_id, &evidence_id, &evidence_sha, &storage_uri],
        )
        .await
        .unwrap();
    for (observation_id, provenance_id, observation_hash) in [
        (
            format!("sha256:{}", "a".repeat(64)),
            first_provenance,
            "d".repeat(64),
        ),
        (
            format!("sha256:{}", "b".repeat(64)),
            second_provenance,
            "e".repeat(64),
        ),
    ] {
        admin
            .execute(
                "INSERT INTO agent_economy.provenance_records( \
                 namespace_id,provenance_id,source_id,observed_at,parser_version,provider, \
                 chain_scope,block_reference,evidence_id) \
                 VALUES ($1::text::uuid,$2::text::uuid,'cdp-x402-bazaar-base', \
                 to_timestamp(1794355200000 / 1000.0),'x402-adapter@1','cdp-bazaar', \
                 'base','0',$3)",
                &[&namespace_id, &provenance_id, &evidence_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "INSERT INTO agent_economy.observations( \
                 namespace_id,chain_scope,source_id,observation_id,observed_at,parser_version, \
                 protocol,evidence_id,provenance_id,observation_hash) \
                 VALUES ($1::text::uuid,'base','cdp-x402-bazaar-base',$2, \
                 to_timestamp(1794355200000 / 1000.0),'x402-adapter@1','x402',$3, \
                 $4::text::uuid,$5)",
                &[
                    &namespace_id,
                    &observation_id,
                    &evidence_id,
                    &provenance_id,
                    &observation_hash,
                ],
            )
            .await
            .unwrap();
    }
    admin
        .execute(
            "INSERT INTO agent_economy.collection_range_receipts( \
             namespace_id,chain_scope,source_id,start_height,end_height,input_sha256,batch_sha256, \
             evidence_json,observations_json,evidence_count,observation_count) \
             VALUES ($1::text::uuid,'base','cdp-x402-bazaar-base',0,0,$2,$3, \
             $4::text::jsonb,$5::text::jsonb,1,2)",
            &[
                &namespace_id,
                &input_sha,
                &batch_sha,
                &evidence.to_string(),
                &observations.to_string(),
            ],
        )
        .await
        .unwrap();

    let coverage = admin
        .query_one(
            "SELECT verified_qualifying_resources, resources_observed \
             FROM agent_economy.catalog_crawl_coverage \
             WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid",
            &[&namespace_id, &job_id],
        )
        .await
        .unwrap();
    assert_eq!(
        coverage.get::<_, i64>(0),
        1,
        "multiple exact offers for one Bazaar resource were overcounted"
    );
    assert_eq!(
        coverage.get::<_, i64>(1),
        2,
        "unsupported Bazaar resources must stay observed but never verified"
    );
}

fn run_collect(
    collector_database_url: &str,
    root: &std::path::Path,
    chain: &str,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("collect")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("COLLECTOR_DATABASE_URL", collector_database_url)
        .env("COLLECTION_INPUT_ROOT", root.join("inputs"))
        .env("EVIDENCE_WRITE_ROOT", root.join("evidence"))
        .env("COLLECTION_RPC_REPLAY_ROOT", root)
        .env("COLLECT_LEASE_OWNER", format!("collect-{chain}-test"))
        .output()
        .unwrap()
}

fn run_verifier(
    verifier_database_url: &str,
    root: &std::path::Path,
    chain: &str,
) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("verify-evidence")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("EVIDENCE_VERIFIER_DATABASE_URL", verifier_database_url)
        .env("EVIDENCE_READ_ROOT", root.join("evidence"))
        .env(
            "VERIFY_EVIDENCE_LEASE_OWNER",
            format!("verify-{chain}-test"),
        )
        .output()
        .unwrap()
}

#[tokio::test]
async fn split_process_custody_executes_every_launch_chain() {
    let database_url = std::env::var("AEM_COLLECT_TEST_DATABASE_URL")
        .expect("AEM_COLLECT_TEST_DATABASE_URL is required for split-custody tests");
    let admin = connect(&database_url).await;
    admin
        .batch_execute(&format!(
            "ALTER ROLE agent_economy_collector_runtime LOGIN PASSWORD '{COLLECTOR_PASSWORD}'; \
             ALTER ROLE agent_economy_evidence_verifier_runtime LOGIN PASSWORD '{VERIFIER_PASSWORD}'; \
             DELETE FROM agent_economy.collection_runtime_namespaces \
             WHERE login_name IN ('agent_economy_collector_runtime','agent_economy_evidence_verifier_runtime')"
        ))
        .await
        .unwrap();

    let collector_database_url = runtime_database_url(
        &database_url,
        "agent_economy_collector_runtime",
        COLLECTOR_PASSWORD,
    );
    let verifier_database_url = runtime_database_url(
        &database_url,
        "agent_economy_evidence_verifier_runtime",
        VERIFIER_PASSWORD,
    );
    let collector = connect(&collector_database_url).await;
    let verifier = connect(&verifier_database_url).await;

    let collector_acl = collector
        .query_one(
            "SELECT \
             has_function_privilege(session_user,'agent_economy.promote_pending_collection_batch(uuid,text,uuid,jsonb,text)','EXECUTE'), \
             has_table_privilege(session_user,'agent_economy.evidence_objects','SELECT')",
            &[],
        )
        .await
        .unwrap();
    assert!(!collector_acl.get::<_, bool>(0), "collector could promote");
    assert!(
        !collector_acl.get::<_, bool>(1),
        "collector could read canonical evidence"
    );
    let verifier_acl = verifier
        .query_one(
            "SELECT \
             has_function_privilege(session_user,'agent_economy.claim_bound_collection_job(text,bigint)','EXECUTE'), \
             has_function_privilege(session_user,'agent_economy.stage_collection_batch(uuid,text,uuid,text,text,text,text,bigint,bigint,bigint,jsonb)','EXECUTE')",
            &[],
        )
        .await
        .unwrap();
    assert!(
        !verifier_acl.get::<_, bool>(0),
        "verifier could claim collect jobs"
    );
    assert!(
        !verifier_acl.get::<_, bool>(1),
        "verifier could stage collection batches"
    );

    let root = std::env::current_dir().unwrap().join(format!(
        "target/split-custody-postgres-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(root.join("inputs")).unwrap();
    let root = fs::canonicalize(root).unwrap();

    for (index, (chain, height)) in [
        ("ethereum", 810_u64),
        ("base", 811),
        ("solana", 812),
        ("tempo", 813),
    ]
    .into_iter()
    .enumerate()
    {
        let run_key = u64::from(std::process::id()) * 10 + u64::try_from(index).unwrap();
        let namespace_id = format!("00000000-0000-0000-0000-{run_key:012}");
        let job_id = format!("00000000-0000-0000-0001-{run_key:012}");
        let bytes = manifest_for_chain(chain, height);
        let digest = format!("{:x}", Sha256::digest(&bytes));
        fs::write(root.join("inputs").join(format!("{digest}.json")), bytes).unwrap();
        fs::write(
            root.join(format!("{chain}-{height}.json")),
            rpc_height_fixture(chain, height),
        )
        .unwrap();

        admin
            .execute(
                "INSERT INTO agent_economy.namespaces(namespace_id,namespace_kind,namespace_key) \
                 VALUES ($1::text::uuid,'tenant',$2) ON CONFLICT DO NOTHING",
                &[
                    &namespace_id,
                    &format!("split-custody-{chain}-{}", std::process::id()),
                ],
            )
            .await
            .unwrap();
        admin
            .execute(
                "INSERT INTO agent_economy.collection_runtime_namespaces(login_name,namespace_id,purpose) VALUES \
                 ('agent_economy_collector_runtime',$1::text::uuid,'collect'), \
                 ('agent_economy_evidence_verifier_runtime',$1::text::uuid,'verify-evidence')",
                &[&namespace_id],
            )
            .await
            .unwrap();
        admin
            .execute(
                "INSERT INTO agent_economy.worker_jobs( \
                 namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256, \
                 collection_chain_scope,collection_source_id,collection_start_height, \
                 collection_end_height,collection_acquisition_contract,collection_evidence_contract) \
                 VALUES ($1::text::uuid,$2::text::uuid,'collect','chain-protocol-range',$3,$4, \
                 $5,$6,$7,$7,'alchemy-rpc-block-v1','evidence-store-create-read-sha256-v1')",
                &[
                    &namespace_id,
                    &job_id,
                    &format!("split-{chain}-{height}"),
                    &digest,
                    &chain,
                    &format!("alchemy-{chain}"),
                    &i64::try_from(height).unwrap(),
                ],
            )
            .await
            .unwrap();

        let collect = run_collect(&collector_database_url, &root, chain);
        assert!(
            collect.status.success(),
            "{chain} collector failed: {}",
            String::from_utf8_lossy(&collect.stderr)
        );
        let before_verify = admin
            .query_one(
                "SELECT \
                 (SELECT count(*) FROM agent_economy.pending_collection_batches WHERE namespace_id=$1::text::uuid), \
                 (SELECT count(*) FROM agent_economy.collection_range_receipts WHERE namespace_id=$1::text::uuid), \
                 (SELECT count(*) FROM agent_economy.collection_cursors WHERE namespace_id=$1::text::uuid)",
                &[&namespace_id],
            )
            .await
            .unwrap();
        assert_eq!(before_verify.get::<_, i64>(0), 1);
        assert_eq!(before_verify.get::<_, i64>(1), 0);
        assert_eq!(before_verify.get::<_, i64>(2), 0);

        let verify = run_verifier(&verifier_database_url, &root, chain);
        assert!(
            verify.status.success(),
            "{chain} verifier failed: {}",
            String::from_utf8_lossy(&verify.stderr)
        );
        let state = admin
            .query_one(
                "SELECT \
                 (SELECT status='succeeded' FROM agent_economy.worker_jobs WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid), \
                 (SELECT next_height FROM agent_economy.collection_cursors WHERE namespace_id=$1::text::uuid AND chain_scope=$3 AND source_id=$4), \
                 (SELECT count(*) FROM agent_economy.evidence_objects WHERE namespace_id=$1::text::uuid), \
                 (SELECT count(*) FROM agent_economy.collection_range_receipts WHERE namespace_id=$1::text::uuid), \
                 (SELECT count(*) FROM agent_economy.observations WHERE namespace_id=$1::text::uuid)",
                &[&namespace_id, &job_id, &chain, &format!("alchemy-{chain}")],
            )
            .await
            .unwrap();
        assert!(state.get::<_, bool>(0));
        assert_eq!(state.get::<_, i64>(1), i64::try_from(height + 1).unwrap());
        assert_eq!(state.get::<_, i64>(2), 1);
        assert_eq!(state.get::<_, i64>(3), 1);
        assert_eq!(
            state.get::<_, i64>(4),
            0,
            "ordinary {chain} transfer became a protocol observation"
        );

        admin
            .execute(
                "DELETE FROM agent_economy.collection_runtime_namespaces \
                 WHERE namespace_id=$1::text::uuid",
                &[&namespace_id],
            )
            .await
            .unwrap();
    }

    assert_catalog_stage_survives_worker_crash(&admin, &collector).await;
    assert_catalog_coverage_deduplicates_resources(&admin).await;

    fs::remove_dir_all(root).unwrap();
    admin
        .batch_execute(
            "ALTER ROLE agent_economy_collector_runtime NOLOGIN PASSWORD NULL; \
             ALTER ROLE agent_economy_evidence_verifier_runtime NOLOGIN PASSWORD NULL",
        )
        .await
        .unwrap();
}
