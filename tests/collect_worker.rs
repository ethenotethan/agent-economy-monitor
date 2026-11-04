use std::{
    collections::{BTreeMap, VecDeque},
    fs,
    sync::{Arc, Mutex},
};

use agent_economy_evidence_store::{
    EvidenceObject, EvidenceStore, FilesystemEvidenceStore, GcsClientError, GcsCreateRequest,
    GcsEvidenceStore, GcsObjectClient, GcsObjectIdentity, GcsReadObject, GcsRetryPolicy,
};
use agent_economy_monitor::{
    collect::{
        ArchivedEvidence, CollectionBatch, CollectionCommitStore, CollectionError,
        CollectionHandler, PublicSourceResponse, PublicSourceTransport, verify_replayed_evidence,
    },
    worker::{CollectionAdmission, LeasedJob, WorkerHandler, WorkerMode},
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

struct FakePublicSourceTransport {
    response: PublicSourceResponse,
    requests: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl PublicSourceTransport for FakePublicSourceTransport {
    async fn fetch(&self, url: &str) -> Result<PublicSourceResponse, CollectionError> {
        self.requests.lock().unwrap().push(url.to_owned());
        Ok(self.response.clone())
    }
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
    async fn create_object(
        &self,
        request: GcsCreateRequest,
    ) -> Result<agent_economy_evidence_store::GcsCreatedObject, GcsClientError> {
        self.objects.lock().unwrap().insert(
            request.name().to_owned(),
            GcsReadObject::new(request.bytes().to_vec(), request.metadata().clone(), "1"),
        );
        self.requests.lock().unwrap().push(request);
        Ok(agent_economy_evidence_store::GcsCreatedObject::new("1"))
    }

    async fn inspect_object(
        &self,
        _bucket: &str,
        name: &str,
    ) -> Result<GcsObjectIdentity, GcsClientError> {
        let objects = self.objects.lock().unwrap();
        let object = objects.get(name).ok_or(GcsClientError::Fatal)?;
        Ok(GcsObjectIdentity::new(
            object.metadata().clone(),
            object.generation(),
        ))
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
    leased_job_for(input_sha256, "base", 42, 42)
}

fn leased_job_for(
    input_sha256: String,
    chain: &str,
    start_height: u64,
    end_height: u64,
) -> LeasedJob {
    LeasedJob {
        job_id: "00000000-0000-0000-0000-000000000048".into(),
        mode: WorkerMode::Collect,
        job_kind: "chain-protocol-range".into(),
        input_sha256,
        attempt: 1,
        lease_owner: "collect-test".into(),
        lease_token: "00000000-0000-0000-0000-000000000148".into(),
        collection_admission: Some(CollectionAdmission {
            chain_scope: chain.into(),
            source_id: format!("alchemy-{chain}"),
            start_height,
            end_height,
            acquisition_contract: "alchemy-rpc-block-v1".into(),
            evidence_contract: "evidence-store-create-read-sha256-v1".into(),
        }),
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

fn bazaar_manifest() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "chain": "base",
        "source_id": "cdp-x402-bazaar-base",
        "observed_at_unix_ms": 1790986800000_i64,
        "observed_date": "2026-10-03",
        "start_height": 0,
        "end_height": 0,
        "inputs": [{
            "height": 0,
            "kind": "x402_bazaar",
            "context": "https://api.cdp.coinbase.com/platform/v2/x402/discovery/resources?limit=100&offset=0"
        }]
    }))
    .unwrap()
}

fn bazaar_job(input_sha256: String) -> LeasedJob {
    let mut job = leased_job(input_sha256);
    job.collection_admission = Some(CollectionAdmission {
        chain_scope: "base".into(),
        source_id: "cdp-x402-bazaar-base".into(),
        start_height: 0,
        end_height: 0,
        acquisition_contract: "cdp-x402-bazaar-v1".into(),
        evidence_contract: "evidence-store-create-read-sha256-v1".into(),
    });
    job
}

fn bazaar_fixture() -> Vec<u8> {
    br#"{"items":[{"accepts":[{"amount":"5000","asset":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","extra":{"name":"USD Coin","version":"2"},"maxAmountRequired":"5000","maxTimeoutSeconds":60,"network":"eip155:8453","payTo":"0x589a2314a2E05F45e40C4823Da3Ba58D421dB3d8","resource":"https://www.api-xpay.com/x402/quote","scheme":"exact"}],"resource":"https://www.api-xpay.com/x402/quote","x402Version":2}]}"#.to_vec()
}

fn rpc_transport(chain: &str, reported_height: u64) -> FakeRpcTransport {
    rpc_transport_with_protocol(chain, reported_height, None)
}

fn rpc_transport_with_protocol(
    chain: &str,
    reported_height: u64,
    protocol: Option<(&str, &[u8])>,
) -> FakeRpcTransport {
    let chain = manifest_chain(chain);
    let mut result = if chain == Chain::Solana {
        serde_json::json!({
            "blockHeight": reported_height,
            "blockhash": format!("block-{reported_height}"),
            "parentSlot": reported_height.saturating_sub(1),
            "previousBlockhash": "block-parent",
            "transactions": []
        })
    } else {
        serde_json::json!({"number": format!("0x{reported_height:x}")})
    };
    if let Some((kind, payload)) = protocol {
        result["agentEconomyProtocolEvidence"] = serde_json::json!([{
            "height": reported_height,
            "kind": kind,
            "context": if kind == "mpp_openapi" {
                format!("service:{}", chain.as_str())
            } else {
                "https://merchant.example/paid".to_owned()
            },
            "evidence_base64": STANDARD.encode(payload)
        }]);
    }
    let body = serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result
    }))
    .unwrap();
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
async fn genuine_public_bazaar_evidence_is_archived_then_replayed_into_an_observation() {
    let root = std::env::temp_dir().join(format!("aem-collect-bazaar-{}", std::process::id()));
    let _ = fs::remove_dir_all(&root);
    let input_root = root.join("inputs");
    fs::create_dir_all(&input_root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let input_root = root.join("inputs");
    let evidence_store = Arc::new(FilesystemEvidenceStore::open(root.join("evidence")).unwrap());
    let commit_store = Arc::new(RecordingCommitStore::default());
    let bytes = bazaar_manifest();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
    let source_requests = Arc::new(Mutex::new(Vec::new()));
    let handler = CollectionHandler::new(
        input_root,
        Arc::clone(&evidence_store),
        commit_store.clone(),
        rpc_transport("base", 42),
    )
    .with_public_source_transport(Arc::new(FakePublicSourceTransport {
        response: PublicSourceResponse::try_new(200, bazaar_fixture()).unwrap(),
        requests: Arc::clone(&source_requests),
    }));

    handler.process(&bazaar_job(digest.clone())).await.unwrap();
    handler.process(&bazaar_job(digest)).await.unwrap();
    let (stored, sha256) = {
        let batches = commit_store.0.lock().unwrap();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].source_id(), "cdp-x402-bazaar-base");
        assert_eq!(batches[0].observed_at_unix_ms(), 1_790_986_800_000);
        assert!(
            batches[0].evidence()[0]
                .storage_uri()
                .contains("/2026-10-03/")
        );
        assert_eq!(batches[0].evidence()[0].media_type(), "application/json");
        assert_eq!(
            batches[0].evidence()[0].storage_uri(),
            batches[1].evidence()[0].storage_uri(),
            "duplicate source bytes must address the same immutable object"
        );
        (
            batches[0].evidence()[0].storage_uri().to_owned(),
            batches[0].evidence()[0].sha256().to_owned(),
        )
    };
    let object = EvidenceObject::parse(&stored).unwrap();
    let readback = EvidenceStore::read(evidence_store.as_ref(), &object)
        .await
        .unwrap();
    let verified = ArchivedEvidence::from_verified_readback(
        stored,
        sha256,
        "application/json".into(),
        0,
        readback,
    )
    .unwrap();
    let promoted = verify_replayed_evidence(
        "base".into(),
        "cdp-x402-bazaar-base".into(),
        1790986800000,
        0,
        0,
        vec![verified],
    )
    .unwrap();
    assert_eq!(promoted.observations().len(), 1);
    assert_eq!(promoted.observations()[0].protocol(), "x402");
    assert_eq!(source_requests.lock().unwrap().len(), 2);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn public_bazaar_page_coordinate_must_match_the_fixed_source_request() {
    let root = std::env::temp_dir().join(format!(
        "aem-collect-bazaar-coordinate-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    let input_root = root.join("inputs");
    fs::create_dir_all(&input_root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let input_root = root.join("inputs");
    let evidence_store = Arc::new(FilesystemEvidenceStore::open(root.join("evidence")).unwrap());
    let commit_store = Arc::new(RecordingCommitStore::default());
    let mut manifest: serde_json::Value = serde_json::from_slice(&bazaar_manifest()).unwrap();
    manifest["start_height"] = serde_json::json!(42);
    manifest["end_height"] = serde_json::json!(42);
    manifest["inputs"][0]["height"] = serde_json::json!(42);
    let bytes = serde_json::to_vec(&manifest).unwrap();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
    let mut job = bazaar_job(digest);
    job.collection_admission.as_mut().unwrap().start_height = 42;
    job.collection_admission.as_mut().unwrap().end_height = 42;
    let handler = CollectionHandler::new(
        input_root,
        evidence_store,
        commit_store.clone(),
        rpc_transport("base", 42),
    )
    .with_public_source_transport(Arc::new(FakePublicSourceTransport {
        response: PublicSourceResponse::try_new(200, bazaar_fixture()).unwrap(),
        requests: Arc::new(Mutex::new(Vec::new())),
    }));

    assert!(handler.process(&job).await.is_err());
    assert!(commit_store.0.lock().unwrap().is_empty());
    fs::remove_dir_all(root).unwrap();
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
    let bytes = bazaar_manifest();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
    let handler = CollectionHandler::new(
        input_root,
        Arc::clone(&evidence_store),
        commit_store.clone(),
        rpc_transport("base", 42),
    )
    .with_public_source_transport(Arc::new(FakePublicSourceTransport {
        response: PublicSourceResponse::try_new(200, br#"{"items":[{"accepts":"bad"}]}"#.to_vec())
            .unwrap(),
        requests: Arc::new(Mutex::new(Vec::new())),
    }));

    assert!(handler.process(&bazaar_job(digest)).await.is_err());
    assert!(commit_store.0.lock().unwrap().is_empty());
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn generic_rpc_protocol_shaped_field_cannot_create_an_observation() {
    let root = std::env::temp_dir().join(format!(
        "aem-collect-manifest-divergence-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&root);
    let input_root = root.join("inputs");
    fs::create_dir_all(&input_root).unwrap();
    let root = fs::canonicalize(&root).unwrap();
    let input_root = root.join("inputs");
    let evidence_store = Arc::new(FilesystemEvidenceStore::open(root.join("evidence")).unwrap());
    let commit_store = Arc::new(RecordingCommitStore::default());
    let bytes = manifest("base", "x402_runtime", &x402_fixture());
    let digest = format!("{:x}", Sha256::digest(&bytes));
    fs::write(input_root.join(format!("{digest}.json")), bytes).unwrap();
    let handler = CollectionHandler::new(
        input_root,
        Arc::clone(&evidence_store),
        commit_store.clone(),
        rpc_transport_with_protocol("base", 42, Some(("x402_runtime", &x402_fixture()))),
    );

    handler.process(&leased_job(digest)).await.unwrap();

    let (stored, sha256) = {
        let batches = commit_store.0.lock().unwrap();
        assert!(batches[0].observations().is_empty());
        assert_eq!(batches[0].evidence().len(), 1);
        (
            batches[0].evidence()[0].storage_uri().to_owned(),
            batches[0].evidence()[0].sha256().to_owned(),
        )
    };
    let object = EvidenceObject::parse(&stored).unwrap();
    let readback = EvidenceStore::read(evidence_store.as_ref(), &object)
        .await
        .unwrap();
    let verified = ArchivedEvidence::from_verified_readback(
        stored,
        sha256,
        "application/vnd.agent-economy.rpc".into(),
        42,
        readback,
    )
    .unwrap();
    let promoted = verify_replayed_evidence(
        "base".into(),
        "alchemy-base".into(),
        1790986800000,
        42,
        42,
        vec![verified],
    )
    .unwrap();
    assert!(promoted.observations().is_empty());
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
        rpc_transport_with_protocol("base", 42, Some(("x402_runtime", &x402_fixture()))),
    );

    handler.process(&leased_job(digest)).await.unwrap();

    let requests = client.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
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
    let batches = commit_store.0.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(
        batches[0].evidence()[0].storage_generation(),
        Some("1"),
        "collector must persist the backend-owned object generation"
    );
    drop(batches);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn oversized_protocol_evidence_is_rejected_before_archive_or_commit() {
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
        rpc_transport_with_protocol(
            "base",
            42,
            Some(("x402_runtime", &vec![b'x'; 4 * 1024 * 1024 + 1])),
        ),
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

    handler
        .process(&leased_job_for(digest, "base", 42, 43))
        .await
        .unwrap();

    let batches = commit_store.0.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].evidence().len(), 2);
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
