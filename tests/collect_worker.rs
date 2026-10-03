use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    sync::{Arc, Mutex},
};

use agent_economy_evidence_store::{
    FilesystemEvidenceStore, GcsClientError, GcsCreateRequest, GcsEvidenceStore, GcsObjectClient,
    GcsReadObject, GcsRetryPolicy,
};
use agent_economy_monitor::{
    collect::{CollectionBatch, CollectionCommitStore, CollectionError, CollectionHandler},
    worker::{LeasedJob, WorkerHandler, WorkerMode},
};
use agent_economy_rpc_collector::{Chain, CollectorError, RawRpcResponse, RpcTransport};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};

#[derive(Default)]
struct RecordingCommitStore(Mutex<Vec<CollectionBatch>>);

#[derive(Clone, Default)]
struct RecordingGcsClient {
    objects: Arc<Mutex<BTreeMap<String, GcsReadObject>>>,
    requests: Arc<Mutex<Vec<GcsCreateRequest>>>,
}

struct FakeRpcTransport {
    responses: VecDeque<Result<RawRpcResponse, CollectorError>>,
    requests: Arc<Mutex<Vec<(Chain, u64)>>>,
}

impl RpcTransport for FakeRpcTransport {
    async fn fetch_block(
        &mut self,
        chain: Chain,
        height: u64,
    ) -> Result<RawRpcResponse, CollectorError> {
        self.requests.lock().unwrap().push((chain, height));
        self.responses.pop_front().expect("RPC response queued")
    }
}

#[async_trait]
impl GcsObjectClient for RecordingGcsClient {
    async fn create_object(&self, request: GcsCreateRequest) -> Result<(), GcsClientError> {
        self.objects.lock().unwrap().insert(
            request.name().to_owned(),
            GcsReadObject::new(request.bytes().to_vec(), request.metadata().clone()),
        );
        self.requests.lock().unwrap().push(request);
        Ok(())
    }

    async fn read_object(
        &self,
        _bucket: &str,
        name: &str,
    ) -> Result<GcsReadObject, GcsClientError> {
        self.objects
            .lock()
            .unwrap()
            .get(name)
            .cloned()
            .ok_or(GcsClientError::Fatal)
    }
}

#[async_trait]
impl CollectionCommitStore for RecordingCommitStore {
    async fn commit(
        &self,
        _job: &LeasedJob,
        batch: CollectionBatch,
    ) -> Result<(), CollectionError> {
        self.0.lock().unwrap().push(batch);
        Ok(())
    }
}

fn leased_job(input_sha256: String) -> LeasedJob {
    LeasedJob {
        job_id: "00000000-0000-0000-0000-000000000048".into(),
        mode: WorkerMode::Collect,
        job_kind: "chain-protocol-range".into(),
        input_sha256,
        attempt: 1,
        lease_owner: "collect-test".into(),
        lease_token: "00000000-0000-0000-0000-000000000148".into(),
    }
}

fn x402_fixture() -> Vec<u8> {
    let payload = br#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"1000","asset":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","payTo":"0x1111111111111111111111111111111111111111","resource":"https://merchant.example/paid","extra":{"name":"USD Coin"}}]}"#;
    format!(
        "HTTP/1.1 402 Payment Required\r\npayment-required: {}\r\n\r\n",
        STANDARD.encode(payload)
    )
    .into_bytes()
}

fn mpp_fixture() -> Vec<u8> {
    br#"{"openapi":"3.1.0","info":{"title":"Weather","version":"1.0"},"paths":{"/forecast":{"get":{"x-payment-info":{"intent":"charge","method":"tempo","amount":"7","currency":"USD"},"responses":{"402":{"description":"payment required"}}}}}}"#.to_vec()
}

fn manifest(chain: &str, protocol_kind: &str, protocol_payload: &[u8]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "chain": chain,
        "source_id": format!("alchemy-{chain}"),
        "observed_at_unix_ms": 1790986800000_i64,
        "observed_date": "2026-10-03",
        "start_height": 42,
        "end_height": 42,
        "inputs": [
            {
                "height": 42,
                "kind": protocol_kind,
                "context": if protocol_kind == "mpp_openapi" {
                    format!("service:{chain}")
                } else {
                    "https://merchant.example/paid".into()
                },
                "evidence_base64": STANDARD.encode(protocol_payload)
            },
            {
                "height": 42,
                "kind": "chain_transfer",
                "context": format!("tx:{chain}:ordinary"),
                "evidence_base64": STANDARD.encode(br#"{"from":"buyer","to":"seller","value":"7"}"#)
            }
        ]
    }))
    .unwrap()
}

fn rpc_transport(chain: &str, reported_height: u64) -> FakeRpcTransport {
    let chain = manifest_chain(chain);
    let body = if chain == Chain::Solana {
        format!(
            r#"{{"jsonrpc":"2.0","id":1,"result":{{"blockHeight":{reported_height},"blockhash":"block-{reported_height}","parentSlot":{},"previousBlockhash":"block-parent","transactions":[]}}}}"#,
            reported_height.saturating_sub(1)
        )
        .into_bytes()
    } else {
        format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"number":"0x{reported_height:x}"}}}}"#)
            .into_bytes()
    };
    FakeRpcTransport {
        responses: VecDeque::from([RawRpcResponse::try_new(200, body)]),
        requests: Arc::new(Mutex::new(Vec::new())),
    }
}

fn manifest_chain(chain: &str) -> Chain {
    match chain {
        "ethereum" => Chain::Ethereum,
        "base" => Chain::Base,
        "solana" => Chain::Solana,
        "tempo" => Chain::Tempo,
        _ => unreachable!(),
    }
}

#[tokio::test]
async fn bounded_launch_chain_fixtures_archive_before_emitting_only_explicit_protocol_observations()
{
    for (chain, kind, payload, expected_protocol) in [
        ("ethereum", "x402_runtime", x402_fixture(), "x402"),
        ("base", "x402_runtime", x402_fixture(), "x402"),
        ("solana", "mpp_openapi", mpp_fixture(), "mpp"),
        ("tempo", "mpp_openapi", mpp_fixture(), "mpp"),
    ] {
        let root =
            std::env::temp_dir().join(format!("aem-collect-test-{chain}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&root);
        let input_root = root.join("inputs");
        fs::create_dir_all(&input_root).unwrap();
        let root = fs::canonicalize(&root).unwrap();
        let input_root = root.join("inputs");
        let evidence_store =
            Arc::new(FilesystemEvidenceStore::open(root.join("evidence")).unwrap());
        let commit_store = Arc::new(RecordingCommitStore::default());
        let bytes = manifest(chain, kind, &payload);
        let digest = format!("{:x}", Sha256::digest(&bytes));
        fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
        let rpc = rpc_transport(chain, 42);
        let rpc_requests = Arc::clone(&rpc.requests);
        let handler = CollectionHandler::new(input_root, evidence_store, commit_store.clone(), rpc);

        let result = handler
            .process(&leased_job(digest))
            .await
            .unwrap_or_else(|error| panic!("{chain} fixture failed: {error:?}"));

        assert_eq!(result.output_sha256().len(), 64);
        let batches = commit_store.0.lock().unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].chain(), chain);
        assert_eq!(batches[0].start_height(), 42);
        assert_eq!(batches[0].end_height(), 42);
        assert_eq!(batches[0].evidence().len(), 3);
        assert_eq!(batches[0].observations().len(), 1);
        assert_eq!(batches[0].observations()[0].protocol(), expected_protocol);
        assert!(batches[0].evidence().iter().all(|item| item.verified()));
        assert_eq!(
            rpc_requests.lock().unwrap().as_slice(),
            &[(manifest_chain(chain), 42)]
        );
        drop(batches);
        fs::remove_dir_all(root).unwrap();
    }
}

#[tokio::test]
async fn malformed_explicit_attribution_fails_before_cursor_or_observation_commit() {
    let root = std::env::temp_dir().join(format!("aem-collect-invalid-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let input_root = root.join("inputs");
    fs::create_dir_all(&input_root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let input_root = root.join("inputs");
    let evidence_store = Arc::new(FilesystemEvidenceStore::open(root.join("evidence")).unwrap());
    let commit_store = Arc::new(RecordingCommitStore::default());
    let bytes = manifest("base", "x402_runtime", b"ordinary transfer, not x402");
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
    let handler = CollectionHandler::new(
        input_root,
        evidence_store,
        commit_store.clone(),
        rpc_transport("base", 42),
    );

    let failure = handler.process(&leased_job(digest)).await.unwrap_err();

    assert_eq!(failure.code(), "invalid_protocol_evidence");
    assert!(!failure.is_retryable());
    assert!(commit_store.0.lock().unwrap().is_empty());
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn production_gcs_store_receives_bounded_collection_provenance_before_commit() {
    let root = std::env::temp_dir().join(format!("aem-collect-gcs-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    fs::create_dir_all(&root).unwrap();
    let bytes = manifest("base", "x402_runtime", &x402_fixture());
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(root.join(format!("{digest}.json")), bytes).unwrap();
    let client = RecordingGcsClient::default();
    let evidence_store = Arc::new(
        GcsEvidenceStore::new(
            client.clone(),
            "agent-economy-evidence",
            GcsRetryPolicy::new(3).unwrap(),
        )
        .unwrap(),
    );
    let commit_store = Arc::new(RecordingCommitStore::default());
    let handler = CollectionHandler::new(
        root.clone(),
        evidence_store,
        commit_store.clone(),
        rpc_transport("base", 42),
    );

    handler.process(&leased_job(digest)).await.unwrap();

    let requests = client.requests.lock().unwrap();
    assert_eq!(requests.len(), 3);
    assert!(requests.iter().all(|request| {
        request.if_generation_match() == Some(0)
            && request.metadata().get("evidence-source") == Some(&"alchemy-base".to_owned())
    }));
    assert!(requests.iter().any(|request| {
        request
            .metadata()
            .get("evidence-replay-inputs")
            .is_some_and(|inputs| inputs.contains("\"provider\":\"alchemy\""))
    }));
    assert_eq!(commit_store.0.lock().unwrap().len(), 1);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn oversized_evidence_is_rejected_before_archive_or_commit() {
    let root =
        std::env::temp_dir().join(format!("aem-collect-oversized-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let input_root = root.join("inputs");
    fs::create_dir_all(&input_root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let input_root = root.join("inputs");
    let evidence_store = Arc::new(FilesystemEvidenceStore::open(root.join("evidence")).unwrap());
    let commit_store = Arc::new(RecordingCommitStore::default());
    let bytes = manifest("base", "x402_runtime", &vec![b'x'; 4 * 1024 * 1024 + 1]);
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
    let handler = CollectionHandler::new(
        input_root,
        evidence_store,
        commit_store.clone(),
        rpc_transport("base", 42),
    );

    let error = handler.process(&leased_job(digest)).await.unwrap_err();

    assert_eq!(error.code(), "invalid_protocol_evidence");
    assert!(commit_store.0.lock().unwrap().is_empty());
    assert!(
        fs::read_dir(root.join("evidence"))
            .unwrap()
            .next()
            .is_none()
    );
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn rpc_acquisition_represents_every_height_when_protocol_inputs_are_sparse() {
    let root = std::env::temp_dir().join(format!("aem-collect-sparse-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let input_root = root.join("inputs");
    fs::create_dir_all(&input_root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let input_root = root.join("inputs");
    let evidence_store = Arc::new(FilesystemEvidenceStore::open(root.join("evidence")).unwrap());
    let commit_store = Arc::new(RecordingCommitStore::default());
    let mut value: serde_json::Value =
        serde_json::from_slice(&manifest("base", "x402_runtime", &x402_fixture())).unwrap();
    value["end_height"] = 43.into();
    let bytes = serde_json::to_vec(&value).unwrap();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
    let mut rpc = rpc_transport("base", 42);
    rpc.responses
        .push_back(rpc_transport("base", 43).responses.pop_front().unwrap());
    let handler = CollectionHandler::new(input_root, evidence_store, commit_store.clone(), rpc);

    handler.process(&leased_job(digest)).await.unwrap();

    let batches = commit_store.0.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].evidence().len(), 4);
    drop(batches);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn rpc_acquisition_rejects_a_reported_height_mismatch_before_commit() {
    let root = std::env::temp_dir().join(format!("aem-collect-no-rpc-test-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let input_root = root.join("inputs");
    fs::create_dir_all(&input_root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let input_root = root.join("inputs");
    let evidence_store = Arc::new(FilesystemEvidenceStore::open(root.join("evidence")).unwrap());
    let commit_store = Arc::new(RecordingCommitStore::default());
    let value: serde_json::Value =
        serde_json::from_slice(&manifest("base", "x402_runtime", &x402_fixture())).unwrap();
    let bytes = serde_json::to_vec(&value).unwrap();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
    let handler = CollectionHandler::new(
        input_root,
        evidence_store,
        commit_store.clone(),
        rpc_transport("base", 41),
    );

    let error = handler.process(&leased_job(digest)).await.unwrap_err();

    assert_eq!(error.code(), "invalid_protocol_evidence");
    assert!(commit_store.0.lock().unwrap().is_empty());
    fs::remove_dir_all(root).unwrap();
}
