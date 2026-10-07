use std::sync::Arc;

use agent_economy_evidence_store::FilesystemEvidenceStore;
use agent_economy_monitor::{
    classify::{ClassificationHandler, PostgresClassificationStore, derive_classification_batch},
    enrich::{
        EnrichmentHandler, EnrichmentTransport, EnrichmentWorkerError, EvidenceStoreHistoryArchive,
        PostgresEnrichmentStore,
    },
    verify_evidence::PostgresEvidenceVerifier,
    worker::{WorkerDispatcher, WorkerMode},
};
use agent_economy_rpc_collector::{BuyerHistoryTarget, RawRpcResponse};
use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, NoTls};

const CLASSIFIER_PASSWORD: &str = "classifier_test_password";
const ENRICHER_PASSWORD: &str = "enricher_test_password";
const VERIFIER_PASSWORD: &str = "verifier_test_password";
static POSTGRES_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn runtime_database_url(database_url: &str, login: &str, password: &str) -> String {
    let (scheme, rest) = database_url
        .split_once("://")
        .expect("test database URL has a scheme");
    let (_, host_and_path) = rest
        .split_once('@')
        .expect("test database URL has explicit credentials");
    format!("{scheme}://{login}:{password}@{host_and_path}")
}

async fn connect(database_url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

#[tokio::test]
async fn leased_classifier_persists_sealed_lineage_and_updates_the_promotable_read_model() {
    let _guard = POSTGRES_TEST_LOCK.lock().await;
    let database_url = std::env::var("AEM_COLLECT_TEST_DATABASE_URL")
        .expect("AEM_COLLECT_TEST_DATABASE_URL is required for classifier PostgreSQL tests");
    let admin = connect(&database_url).await;
    let run_key = u64::from(std::process::id());
    let namespace = format!("00000000-0000-0050-0000-{run_key:012}");
    let job = format!("00000000-0000-0050-0001-{run_key:012}");
    let provenance = format!("00000000-0000-0050-0002-{run_key:012}");
    let evidence_id = format!("evidence:sha256:{}", "a".repeat(64));
    let event_id = format!("event:x402:sha256:{}", "b".repeat(64));
    let settlement_id = format!("settlement:x402:base:{run_key}");
    let buyer_id = format!("buyer:base:{run_key}");
    let run_id = format!("classification:{buyer_id}");

    admin
        .batch_execute(&format!(
            "ALTER ROLE agent_economy_classifier_runtime LOGIN PASSWORD '{CLASSIFIER_PASSWORD}'; \
             DELETE FROM agent_economy.classifier_runtime_namespaces \
             WHERE login_name = 'agent_economy_classifier_runtime'; \
             INSERT INTO agent_economy.namespaces(namespace_id,namespace_kind,namespace_key) \
             VALUES ('{namespace}','tenant','classify-worker-postgres-{run_key}'); \
             INSERT INTO agent_economy.evidence_objects( \
                 namespace_id,evidence_id,sha256,storage_uri,media_type,byte_length,observed_at) \
             VALUES ('{namespace}','{evidence_id}','{}','evidence://classify/{run_key}', \
                 'application/json',2,to_timestamp(120)); \
             INSERT INTO agent_economy.provenance_records( \
                 namespace_id,provenance_id,source_id,observed_at,parser_version,provider, \
                 chain_scope,evidence_id) \
             VALUES ('{namespace}','{provenance}','rpc-primary',to_timestamp(120), \
                 'x402-adapter@1','fixture','base','{evidence_id}'); \
             INSERT INTO agent_economy.buyer_handles( \
                 namespace_id,buyer_handle_id,handle_kind,chain_scope,handle_value,provenance_id) \
             VALUES ('{namespace}','{buyer_id}','wallet','base', \
                 '0x1111111111111111111111111111111111111111','{provenance}'); \
             INSERT INTO agent_economy.canonical_events( \
                 namespace_id,protocol,canonical_event_id,chain_scope,event_at,reducer_version, \
                 canonical_state_hash) \
             VALUES ('{namespace}','x402','{event_id}','base',to_timestamp(120), \
                 'reducer@1','{}'); \
             INSERT INTO agent_economy.settlements( \
                 namespace_id,chain_scope,settlement_id,protocol,canonical_event_id,source_id, \
                 buyer_handle_id,asset,amount_atomic,settled_at,provenance_id) \
             VALUES ('{namespace}','base','{settlement_id}','x402','{event_id}', \
                 'rpc-primary','{buyer_id}','USDC',600,to_timestamp(120),'{provenance}'); \
             INSERT INTO agent_economy.attribution_runs( \
                 namespace_id,chain_scope,settlement_id,attribution_version,engine_version, \
                 encoding_version,match_method,input_snapshot_hash,level,settlement_evidence_id) \
             VALUES ('{namespace}','base','{settlement_id}',1,'attribution@1', \
                 'attribution-result-v1','none','{}','unknown','{evidence_id}'); \
             INSERT INTO agent_economy.attribution_run_evidence( \
                 namespace_id,chain_scope,settlement_id,attribution_version,evidence_id,evidence_role) \
             VALUES ('{namespace}','base','{settlement_id}',1,'{evidence_id}','settlement'); \
             INSERT INTO agent_economy.classifier_runtime_namespaces(login_name,namespace_id,purpose) \
             VALUES ('agent_economy_classifier_runtime','{namespace}','classify')",
            "a".repeat(64),
            "c".repeat(64),
            "d".repeat(64),
        ))
        .await
        .unwrap();

    let manifest = json!({
        "schema_version": 1,
        "buyer_handle_id": buyer_id,
        "run_id": run_id,
        "run_version": 1,
        "classifier_version": "buyer-classifier@1",
        "feature_version": "behavior-features@1",
        "window_start_unix_seconds": 100,
        "window_end_unix_seconds": 400,
        "provenance_id": provenance,
        "activities": [{
            "settlement_id": settlement_id,
            "amount_atomic": "600",
            "occurred_at_unix_seconds": 120,
            "protocol": "x402",
            "counterparty": "unattributed",
            "autonomy": "unknown",
            "evidence_id": evidence_id,
        }],
        "labels": [{
            "id": "core:high-spend",
            "version": 1,
            "kind": "core",
            "metric": "total_spend_atomic",
            "threshold": "500",
            "confidence_bps": 8500,
            "definition_hash": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        }],
    });
    let canonical = admin
        .query_one(
            "SELECT $1::text::jsonb::text, \
             encode(sha256(convert_to($1::text::jsonb::text, 'UTF8')), 'hex')",
            &[&manifest.to_string()],
        )
        .await
        .unwrap();
    let manifest_text = canonical.get::<_, String>(0);
    let input_sha256 = canonical.get::<_, String>(1);
    let expected_batch =
        derive_classification_batch(&input_sha256, manifest_text.as_bytes()).unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
                 namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256) \
             VALUES ($1::text::uuid,$2::text::uuid,'classify','buyer-behavior-v1', \
                 $3,$4)",
            &[
                &namespace,
                &job,
                &format!("classify-postgres-{run_key}"),
                &input_sha256,
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.classifier_job_inputs( \
                 namespace_id,job_id,input_manifest,expected_output_sha256, \
                 expected_input_snapshot_hash,expected_label_set_hash,expected_features_json, \
                 expected_claims_json,expected_evidence_ids,expected_result_encoding,expected_state_hash) \
             VALUES ($1::text::uuid,$2::text::uuid,$3::text::jsonb,$4,$5,$6, \
                 $7::text::jsonb,$8::text::jsonb,$9::text[],$10::bytea,$11)",
            &[
                &namespace,
                &job,
                &manifest_text,
                &expected_batch.output_sha256(),
                &expected_batch.input_snapshot_hash(),
                &expected_batch.label_set_hash(),
                &expected_batch.features_json().to_string(),
                &expected_batch.claims_json().to_string(),
                &expected_batch.evidence_ids(),
                &expected_batch.result_encoding(),
                &expected_batch.state_hash(),
            ],
        )
        .await
        .unwrap();

    let classifier_url = runtime_database_url(
        &database_url,
        "agent_economy_classifier_runtime",
        CLASSIFIER_PASSWORD,
    );
    let runtime = connect(&classifier_url).await;
    let store = Arc::new(PostgresClassificationStore::new(runtime));
    let completed = WorkerDispatcher::new(store.clone())
        .register(
            WorkerMode::Classify,
            Arc::new(ClassificationHandler::new(store)),
        )
        .run_once(WorkerMode::Classify, "classify-postgres-test")
        .await
        .expect("leased classifier must persist and complete its exact output");

    let persisted = admin
        .query_one(
            "SELECT \
             (SELECT status FROM agent_economy.worker_jobs \
              WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid), \
             (SELECT content_hash FROM agent_economy.classification_run_seals \
              WHERE namespace_id=$1::text::uuid AND run_id=$3 AND run_version=1), \
             (SELECT count(*) FROM agent_economy.classification_run_features \
              WHERE namespace_id=$1::text::uuid AND run_id=$3 AND run_version=1), \
             (SELECT confidence::text FROM agent_economy.classification_claims \
              WHERE namespace_id=$1::text::uuid AND buyer_handle_id=$4)",
            &[&namespace, &job, &run_id, &buyer_id],
        )
        .await
        .unwrap();
    assert_eq!(persisted.get::<_, String>(0), "succeeded");
    assert_eq!(persisted.get::<_, String>(1), completed.output_sha256);
    assert_eq!(persisted.get::<_, i64>(2), 8);
    assert_eq!(persisted.get::<_, String>(3), "0.8500");

    admin
        .query_one(
            "SELECT agent_economy.promote_buyer_classification_run( \
                 $1::text::uuid,$2,$3,1,'test-promotion',$4::text::uuid)",
            &[&namespace, &buyer_id, &run_id, &provenance],
        )
        .await
        .unwrap();
    let current = admin
        .query_one(
            "SELECT run_id,run_version FROM agent_economy.current_buyer_classification_runs \
             WHERE namespace_id=$1::text::uuid AND buyer_handle_id=$2",
            &[&namespace, &buyer_id],
        )
        .await
        .unwrap();
    assert_eq!(current.get::<_, String>(0), run_id);
    assert_eq!(current.get::<_, i32>(1), 1);

    let forged_job = format!("00000000-0000-0050-0005-{run_key:012}");
    let mut forged_manifest = manifest.clone();
    forged_manifest["run_version"] = json!(2);
    forged_manifest["activities"][0]["observation_id"] =
        json!(format!("sha256:{}", "f".repeat(64)));
    let forged_canonical = admin
        .query_one(
            "SELECT $1::text::jsonb::text, \
             encode(sha256(convert_to($1::text::jsonb::text, 'UTF8')), 'hex')",
            &[&forged_manifest.to_string()],
        )
        .await
        .unwrap();
    let forged_manifest_text = forged_canonical.get::<_, String>(0);
    let forged_input_sha256 = forged_canonical.get::<_, String>(1);
    let forged_expected_batch =
        derive_classification_batch(&forged_input_sha256, forged_manifest_text.as_bytes()).unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
                 namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256) \
             VALUES ($1::text::uuid,$2::text::uuid,'classify','buyer-behavior-v1',$3,$4)",
            &[
                &namespace,
                &forged_job,
                &format!("classify-forgery-{run_key}"),
                &forged_input_sha256,
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.classifier_job_inputs( \
                 namespace_id,job_id,input_manifest,expected_output_sha256, \
                 expected_input_snapshot_hash,expected_label_set_hash,expected_features_json, \
                 expected_claims_json,expected_evidence_ids,expected_result_encoding,expected_state_hash) \
             VALUES ($1::text::uuid,$2::text::uuid,$3::text::jsonb,$4,$5,$6, \
                 $7::text::jsonb,$8::text::jsonb,$9::text[],$10::bytea,$11)",
            &[
                &namespace,
                &forged_job,
                &forged_manifest_text,
                &forged_expected_batch.output_sha256(),
                &forged_expected_batch.input_snapshot_hash(),
                &forged_expected_batch.label_set_hash(),
                &forged_expected_batch.features_json().to_string(),
                &forged_expected_batch.claims_json().to_string(),
                &forged_expected_batch.evidence_ids(),
                &forged_expected_batch.result_encoding(),
                &forged_expected_batch.state_hash(),
            ],
        )
        .await
        .unwrap();
    let runtime = connect(&runtime_database_url(
        &database_url,
        "agent_economy_classifier_runtime",
        CLASSIFIER_PASSWORD,
    ))
    .await;
    let claimed = runtime
        .query_one(
            "SELECT job_id::text, lease_token::text \
             FROM agent_economy.claim_classifier_job('forger', 60)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(claimed.get::<_, String>(0), forged_job);
    let lease_token: String = claimed.get(1);
    let forged = runtime
        .query_one(
            "SELECT agent_economy.commit_classification_batch( \
                 $1::text::uuid,'forger',$2::text::uuid,$3,$4,$5,2,$6, \
                 'buyer-classifier@1',$7::text::uuid,100,400,$8,$9, \
                 $10::text::jsonb,$11::text::jsonb, \
                 $12::text::jsonb,$13::text[],$14::bytea,$15)",
            &[
                &forged_job,
                &lease_token,
                &forged_input_sha256,
                &forged_expected_batch.output_sha256(),
                &run_id,
                &buyer_id,
                &provenance,
                &forged_expected_batch.input_snapshot_hash(),
                &forged_expected_batch.label_set_hash(),
                &forged_expected_batch.features_json().to_string(),
                &manifest["labels"].to_string(),
                &forged_expected_batch.claims_json().to_string(),
                &forged_expected_batch.evidence_ids(),
                &forged_expected_batch.result_encoding(),
                &forged_expected_batch.state_hash(),
            ],
        )
        .await;
    assert!(
        forged.is_err(),
        "the restricted classifier runtime must not forge history-observation lineage"
    );
    let forged_residue = admin
        .query_one(
            "SELECT count(*) FROM agent_economy.classifier_job_receipts \
             WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid",
            &[&namespace, &forged_job],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(forged_residue, 0);
}

struct OnePageHistoryTransport;

#[async_trait]
impl EnrichmentTransport for OnePageHistoryTransport {
    async fn fetch(
        &mut self,
        _target: &BuyerHistoryTarget,
        cursor: Option<&str>,
    ) -> Result<RawRpcResponse, EnrichmentWorkerError> {
        assert_eq!(cursor, None);
        RawRpcResponse::try_new(
            200,
            br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","blockNum":"0x10","from":"0x1111111111111111111111111111111111111111"}]}}"#.to_vec(),
        )
        .map_err(|_| EnrichmentWorkerError::Unavailable)
    }
}

#[tokio::test]
async fn leased_enricher_persists_evidence_first_history_without_inventing_protocol_activity() {
    let _guard = POSTGRES_TEST_LOCK.lock().await;
    let database_url = std::env::var("AEM_COLLECT_TEST_DATABASE_URL")
        .expect("AEM_COLLECT_TEST_DATABASE_URL is required for enricher PostgreSQL tests");
    let admin = connect(&database_url).await;
    let run_key = u64::from(std::process::id());
    let namespace = format!("00000000-0000-0051-0000-{run_key:012}");
    let job = format!("00000000-0000-0051-0001-{run_key:012}");
    let provenance = format!("00000000-0000-0051-0002-{run_key:012}");
    let buyer_id = format!("buyer:base:enrich:{run_key}");
    let event_id = format!("event:x402:sha256:{}", "1".repeat(64));
    let settlement_id = format!("settlement:x402:base:enrich:{run_key}");
    let handle_value = "0x1111111111111111111111111111111111111111";

    admin
        .batch_execute(&format!(
            "ALTER ROLE agent_economy_enricher_runtime LOGIN PASSWORD '{ENRICHER_PASSWORD}'; \
             ALTER ROLE agent_economy_evidence_verifier_runtime LOGIN PASSWORD '{VERIFIER_PASSWORD}'; \
             DELETE FROM agent_economy.enricher_runtime_namespaces \
             WHERE login_name = 'agent_economy_enricher_runtime'; \
             DELETE FROM agent_economy.collection_runtime_namespaces \
             WHERE login_name = 'agent_economy_evidence_verifier_runtime'; \
             INSERT INTO agent_economy.namespaces(namespace_id,namespace_kind,namespace_key) \
             VALUES ('{namespace}','tenant','enrich-worker-postgres-{run_key}'); \
             INSERT INTO agent_economy.evidence_objects( \
                 namespace_id,evidence_id,sha256,storage_uri,media_type,byte_length,observed_at) \
             VALUES ('{namespace}','evidence:seed:{}','{}','evidence://seed/{run_key}', \
                 'application/json',2,to_timestamp(100)); \
             INSERT INTO agent_economy.provenance_records( \
                 namespace_id,provenance_id,source_id,observed_at,parser_version,provider, \
                 chain_scope,evidence_id,transaction_reference) \
             VALUES ('{namespace}','{provenance}','discovery',to_timestamp(100), \
                 'discovery@1','fixture','base','evidence:seed:{}', \
                 '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'); \
             INSERT INTO agent_economy.buyer_handles( \
                 namespace_id,buyer_handle_id,handle_kind,chain_scope,handle_value,provenance_id) \
             VALUES ('{namespace}','{buyer_id}','wallet','base','{handle_value}','{provenance}'); \
             INSERT INTO agent_economy.canonical_events( \
                 namespace_id,protocol,canonical_event_id,chain_scope,event_at,reducer_version, \
                 canonical_state_hash) \
             VALUES ('{namespace}','x402','{event_id}','base',to_timestamp(120), \
                 'reducer@1','{}'); \
             INSERT INTO agent_economy.observations( \
                 namespace_id,chain_scope,source_id,observation_id,observed_at,parser_version, \
                 protocol,evidence_id,provenance_id,observation_hash) \
             VALUES ('{namespace}','base','discovery','sha256:{}',to_timestamp(100), \
                 'discovery@1','x402','evidence:seed:{}','{provenance}','{}'); \
             INSERT INTO agent_economy.canonical_event_observations( \
                 namespace_id,protocol,canonical_event_id,chain_scope,source_id,observation_id,support_role) \
             VALUES ('{namespace}','x402','{event_id}','base','discovery','sha256:{}','supporting'); \
             INSERT INTO agent_economy.settlements( \
                 namespace_id,chain_scope,settlement_id,protocol,canonical_event_id,source_id, \
                 buyer_handle_id,asset,amount_atomic,settled_at,provenance_id) \
             VALUES ('{namespace}','base','{settlement_id}','x402','{event_id}', \
                 'discovery','{buyer_id}','USDC',700,to_timestamp(120),'{provenance}'); \
             INSERT INTO agent_economy.event_finality_assertions( \
                 namespace_id,protocol,chain_scope,canonical_event_id,assertion_sequence, \
                 accepted,asserted_status,current_status,asserted_at,source_id,provenance_id, \
                 transaction_id,basis_kind,position,block_hash,canonical_block_hash, \
                 latest_position,finalized_position,confirmations_required,execution_outcome, \
                 finality_state_hash) \
             VALUES ('{namespace}','x402','base','{event_id}',1,true,'finalized','finalized', \
                 to_timestamp(120),'discovery','{provenance}', \
                 '0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', \
                 'evm',16,'0x10','0x10',20,18,1,'succeeded','{}'); \
             INSERT INTO agent_economy.attribution_runs( \
                 namespace_id,chain_scope,settlement_id,attribution_version,engine_version, \
                 encoding_version,match_method,input_snapshot_hash,level,settlement_evidence_id) \
             VALUES ('{namespace}','base','{settlement_id}',1,'attribution@1', \
                 'attribution-result-v1','none','{}','unknown','evidence:seed:{}'); \
             INSERT INTO agent_economy.attribution_run_evidence( \
                 namespace_id,chain_scope,settlement_id,attribution_version,evidence_id,evidence_role) \
             VALUES ('{namespace}','base','{settlement_id}',1,'evidence:seed:{}','settlement'); \
             INSERT INTO agent_economy.enricher_runtime_namespaces(login_name,namespace_id,purpose) \
             VALUES ('agent_economy_enricher_runtime','{namespace}','enrich'); \
             INSERT INTO agent_economy.collection_runtime_namespaces(login_name,namespace_id,purpose) \
             VALUES ('agent_economy_evidence_verifier_runtime','{namespace}','verify-evidence')",
            "f".repeat(64),
            "f".repeat(64),
            "f".repeat(64),
            "2".repeat(64),
            "5".repeat(64),
            "f".repeat(64),
            "6".repeat(64),
            "5".repeat(64),
            "4".repeat(64),
            "3".repeat(64),
            "f".repeat(64),
            "f".repeat(64),
        ))
        .await
        .unwrap();

    let manifest = json!({
        "schema_version": 1,
        "namespace_id": namespace,
        "buyer_handle_id": buyer_id,
        "chain_scope": "base",
        "handle_value": handle_value,
        "enrichment_mode": "automatic",
        "max_pages": 1,
        "request_budget": 1,
        "observed_date": "2026-10-07",
        "cursor_version": 0,
        "start_cursor": null,
        "classification_labels": [{
            "id": "core:active",
            "version": 1,
            "kind": "core",
            "metric": "payment_count",
            "threshold": "1",
            "confidence_bps": 8500,
            "definition_hash": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
        }],
    });
    let canonical = admin
        .query_one(
            "SELECT $1::text::jsonb::text, \
             encode(sha256(convert_to($1::text::jsonb::text, 'UTF8')), 'hex')",
            &[&manifest.to_string()],
        )
        .await
        .unwrap();
    let manifest_text = canonical.get::<_, String>(0);
    let input_sha256 = canonical.get::<_, String>(1);
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
                 namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256) \
             VALUES ($1::text::uuid,$2::text::uuid,'enrich','buyer-public-history-v1',$3,$4)",
            &[
                &namespace,
                &job,
                &format!("enrich-postgres-{run_key}"),
                &input_sha256,
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.enrichment_job_inputs(namespace_id,job_id,input_manifest) \
             VALUES ($1::text::uuid,$2::text::uuid,$3::text::jsonb)",
            &[&namespace, &job, &manifest_text],
        )
        .await
        .unwrap();

    let pre_enrichment = admin
        .query_one(
            "SELECT \
             (SELECT count(*) FROM agent_economy.buyer_finalized_history \
              WHERE namespace_id=$1::text::uuid AND buyer_handle_id=$2), \
             (SELECT count(*) FROM agent_economy.observations \
              WHERE namespace_id=$1::text::uuid AND source_id='alchemy-history'), \
             (SELECT count(*) FROM agent_economy.classification_claims \
              WHERE namespace_id=$1::text::uuid AND buyer_handle_id=$2)",
            &[&namespace, &buyer_id],
        )
        .await
        .unwrap();
    assert_eq!(pre_enrichment.get::<_, i64>(0), 0);
    assert_eq!(pre_enrichment.get::<_, i64>(1), 0);
    assert_eq!(pre_enrichment.get::<_, i64>(2), 0);

    let enricher_url = runtime_database_url(
        &database_url,
        "agent_economy_enricher_runtime",
        ENRICHER_PASSWORD,
    );
    let runtime = connect(&enricher_url).await;
    let verifier_runtime = connect(&runtime_database_url(
        &database_url,
        "agent_economy_evidence_verifier_runtime",
        VERIFIER_PASSWORD,
    ))
    .await;
    let evidence_root =
        std::path::PathBuf::from(format!("/private/tmp/aem-enrichment-evidence-{run_key}"));
    let _ = std::fs::remove_dir_all(&evidence_root);
    std::fs::create_dir_all(&evidence_root).unwrap();
    let evidence_store = Arc::new(FilesystemEvidenceStore::open(&evidence_root).unwrap());
    let store = Arc::new(PostgresEnrichmentStore::new(runtime));
    let completed = WorkerDispatcher::new(store.clone())
        .register(
            WorkerMode::Enrich,
            Arc::new(EnrichmentHandler::new(
                OnePageHistoryTransport,
                Arc::new(EvidenceStoreHistoryArchive::new(evidence_store.clone())),
                store,
            )),
        )
        .run_once(WorkerMode::Enrich, "enrich-postgres-test")
        .await
        .expect("leased enricher must persist evidence and complete");

    let before_verification = admin
        .query_one(
            "SELECT \
             (SELECT count(*) FROM agent_economy.enrichment_job_receipts \
              WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid), \
             (SELECT count(*) FROM agent_economy.pending_enrichment_batches \
              WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid AND status='pending')",
            &[&namespace, &job],
        )
        .await
        .unwrap();
    assert_eq!(before_verification.get::<_, i64>(0), 0);
    assert_eq!(before_verification.get::<_, i64>(1), 1);
    let verifier = PostgresEvidenceVerifier::new(
        verifier_runtime,
        evidence_store.clone(),
        "enrichment-evidence-verifier".to_owned(),
    );
    assert_eq!(
        verifier.run_once().await.unwrap().as_deref(),
        Some(job.as_str())
    );

    let persisted = admin
        .query_one(
            "SELECT \
             (SELECT status FROM agent_economy.worker_jobs \
              WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid), \
             (SELECT output_sha256 FROM agent_economy.enrichment_job_receipts \
              WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid), \
             (SELECT count(*) FROM agent_economy.buyer_finalized_history \
              WHERE namespace_id=$1::text::uuid AND buyer_handle_id=$3), \
             (SELECT count(*) FROM agent_economy.evidence_objects \
              WHERE namespace_id=$1::text::uuid AND storage_uri LIKE 'evidence/alchemy-history/%'), \
             (SELECT count(*) FROM agent_economy.provenance_records \
              WHERE namespace_id=$1::text::uuid AND source_id='alchemy-history'), \
             (SELECT count(*) FROM agent_economy.enrichment_reduction_receipts \
              WHERE namespace_id=$1::text::uuid AND enrichment_job_id=$2::text::uuid), \
             (SELECT count(*) FROM agent_economy.classification_admission_requests \
              WHERE namespace_id=$1::text::uuid AND enrichment_job_id=$2::text::uuid), \
             (SELECT count(*) FROM agent_economy.observations \
              WHERE namespace_id=$1::text::uuid AND source_id='alchemy-history'), \
             (SELECT reduced_activities -> 0 ->> 'observation_id' \
              FROM agent_economy.enrichment_reduction_receipts \
              WHERE namespace_id=$1::text::uuid AND enrichment_job_id=$2::text::uuid)",
            &[&namespace, &job, &buyer_id],
        )
        .await
        .unwrap();
    assert_eq!(persisted.get::<_, String>(0), "succeeded");
    assert_eq!(persisted.get::<_, String>(1), completed.output_sha256);
    assert_eq!(persisted.get::<_, i64>(2), 1);
    assert_eq!(persisted.get::<_, i64>(3), 1);
    assert_eq!(persisted.get::<_, i64>(4), 1);
    assert_eq!(persisted.get::<_, i64>(5), 1);
    assert_eq!(persisted.get::<_, i64>(6), 1);
    assert_eq!(persisted.get::<_, i64>(7), 1);
    assert!(
        persisted
            .get::<_, Option<String>>(8)
            .is_some_and(|observation_id| observation_id.starts_with("sha256:")),
        "deterministic reduction must consume an immutable history observation"
    );
    let forged_job = format!("00000000-0000-0051-0005-{run_key:012}");
    let mut forged_manifest = manifest.clone();
    forged_manifest["cursor_version"] = json!(1);
    let forged_canonical = admin
        .query_one(
            "SELECT $1::text::jsonb::text, \
                    encode(sha256(convert_to($1::text::jsonb::text, 'UTF8')), 'hex')",
            &[&forged_manifest.to_string()],
        )
        .await
        .unwrap();
    let forged_manifest_text = forged_canonical.get::<_, String>(0);
    let forged_input_sha256 = forged_canonical.get::<_, String>(1);
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
                 namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256) \
             VALUES ($1::text::uuid,$2::text::uuid,'enrich','buyer-public-history-v1',$3,$4)",
            &[
                &namespace,
                &forged_job,
                &format!("enrich-forged:{buyer_id}"),
                &forged_input_sha256,
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.enrichment_job_inputs(namespace_id,job_id,input_manifest) \
             VALUES ($1::text::uuid,$2::text::uuid,$3::text::jsonb)",
            &[&namespace, &forged_job, &forged_manifest_text],
        )
        .await
        .unwrap();
    let attacker = connect(&runtime_database_url(
        &database_url,
        "agent_economy_enricher_runtime",
        ENRICHER_PASSWORD,
    ))
    .await;
    let forged_lease = attacker
        .query_one(
            "SELECT lease_token::text FROM agent_economy.claim_enrichment_job($1,$2)",
            &[&"forged-enricher", &60_i64],
        )
        .await
        .unwrap();
    let forged_lease_token = forged_lease.get::<_, String>(0);
    let forged_body = br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","blockNum":"0x10"}],"pageKey":null}}"#.to_vec();
    let forged_body_sha256 = format!("{:x}", Sha256::digest(&forged_body));
    let forged_evidence_id = format!(
        "evidence/alchemy-history/2026-10-07/sha256/{}/{}",
        &forged_body_sha256[..2],
        forged_body_sha256
    );
    let forged_evidence = json!([{
        "object_name": forged_evidence_id,
        "sha256": forged_body_sha256,
        "byte_length": forged_body.len(),
        "storage_generation": null,
    }]);
    let forged_records = json!([{
        "transaction_reference": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "block_reference": "0x10",
        "evidence_id": forged_evidence_id,
    }]);
    let forged_stage = attacker
        .query_one(
            "SELECT agent_economy.stage_enrichment_batch( \
                 $1::text::uuid,$2,$3::text::uuid,$4,$5,$6::text::uuid,$7,$8,$9, \
                 $10::text::date,$11::bigint,$12,$13,$14::bigint,$15, \
                 $16::text::jsonb,$17::text::jsonb,$18::text::jsonb)",
            &[
                &forged_job,
                &"forged-enricher",
                &forged_lease_token,
                &forged_input_sha256,
                &"d".repeat(64),
                &namespace,
                &buyer_id,
                &"base",
                &handle_value,
                &"2026-10-07",
                &1_i64,
                &Option::<String>::None,
                &Option::<String>::None,
                &1_i64,
                &true,
                &forged_evidence.to_string(),
                &forged_records.to_string(),
                &manifest["classification_labels"].to_string(),
            ],
        )
        .await
        .unwrap()
        .get::<_, bool>(0);
    assert!(
        forged_stage,
        "the untrusted runtime may only stage evidence claims"
    );
    assert!(
        verifier.run_once().await.is_err(),
        "independent readback must reject a valid-shaped object claim absent from custody"
    );
    let forged_residue = admin
        .query_one(
            "SELECT \
             (SELECT count(*) FROM agent_economy.enrichment_job_receipts \
              WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid), \
             (SELECT count(*) FROM agent_economy.evidence_objects \
              WHERE namespace_id=$1::text::uuid AND evidence_id=$3)",
            &[&namespace, &forged_job, &forged_evidence_id],
        )
        .await
        .unwrap();
    assert_eq!(forged_residue.get::<_, i64>(0), 0);
    assert_eq!(forged_residue.get::<_, i64>(1), 0);
    std::fs::remove_dir_all(&evidence_root).unwrap();

    let admission = admin
        .query_one(
            "SELECT input_manifest::text, \
                    encode(sha256(convert_to(input_manifest::text, 'UTF8')), 'hex') \
             FROM agent_economy.classification_admission_requests \
             WHERE namespace_id=$1::text::uuid AND enrichment_job_id=$2::text::uuid",
            &[&namespace, &job],
        )
        .await
        .unwrap();
    let classification_manifest = admission.get::<_, String>(0);
    let classification_input_sha256 = admission.get::<_, String>(1);
    let classification_batch = derive_classification_batch(
        &classification_input_sha256,
        classification_manifest.as_bytes(),
    )
    .unwrap();
    let classification_job = format!("00000000-0000-0051-0004-{run_key:012}");
    admin
        .batch_execute(&format!(
            "ALTER ROLE agent_economy_classifier_runtime LOGIN PASSWORD '{CLASSIFIER_PASSWORD}'; \
             DELETE FROM agent_economy.classifier_runtime_namespaces \
             WHERE login_name='agent_economy_classifier_runtime'; \
             INSERT INTO agent_economy.classifier_runtime_namespaces(login_name,namespace_id,purpose) \
             VALUES ('agent_economy_classifier_runtime','{namespace}','classify')"
        ))
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
                 namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256) \
             VALUES ($1::text::uuid,$2::text::uuid,'classify','buyer-behavior-v1',$3,$4)",
            &[
                &namespace,
                &classification_job,
                &format!("enrich:{job}:classify"),
                &classification_input_sha256,
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.classifier_job_inputs( \
                 namespace_id,job_id,input_manifest,expected_output_sha256, \
                 expected_input_snapshot_hash,expected_label_set_hash,expected_features_json, \
                 expected_claims_json,expected_evidence_ids,expected_result_encoding,expected_state_hash) \
             VALUES ($1::text::uuid,$2::text::uuid,$3::text::jsonb,$4,$5,$6, \
                 $7::text::jsonb,$8::text::jsonb,$9::text[],$10::bytea,$11)",
            &[
                &namespace,
                &classification_job,
                &classification_manifest,
                &classification_batch.output_sha256(),
                &classification_batch.input_snapshot_hash(),
                &classification_batch.label_set_hash(),
                &classification_batch.features_json().to_string(),
                &classification_batch.claims_json().to_string(),
                &classification_batch.evidence_ids(),
                &classification_batch.result_encoding(),
                &classification_batch.state_hash(),
            ],
        )
        .await
        .unwrap();
    let classifier_runtime = connect(&runtime_database_url(
        &database_url,
        "agent_economy_classifier_runtime",
        CLASSIFIER_PASSWORD,
    ))
    .await;
    let classifier_store = Arc::new(PostgresClassificationStore::new(classifier_runtime));
    WorkerDispatcher::new(classifier_store.clone())
        .register(
            WorkerMode::Classify,
            Arc::new(ClassificationHandler::new(classifier_store)),
        )
        .run_once(WorkerMode::Classify, "enrichment-classify-test")
        .await
        .expect("verified enriched history must drive deterministic classification");
    let claim_count = admin
        .query_one(
            "SELECT count(*) FROM agent_economy.classification_claims \
             WHERE namespace_id=$1::text::uuid AND buyer_handle_id=$2 AND label='core:active'",
            &[&namespace, &buyer_id],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(claim_count, 1);
}
