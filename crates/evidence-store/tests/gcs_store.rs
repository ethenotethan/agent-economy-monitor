use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
};

use agent_economy_evidence_store::{
    CreateDisposition, EvidenceContext, EvidenceProvenance, EvidenceStore, GcsClientError,
    GcsCreateRequest, GcsCreatedObject, GcsEvidenceStore, GcsObjectClient, GcsReadObject,
    GcsRetryPolicy, StoreError,
};
use async_trait::async_trait;

#[derive(Clone, Debug, Default)]
struct FakeGcsClient {
    state: Arc<Mutex<FakeState>>,
}

#[derive(Debug, Default)]
struct FakeState {
    create_results: VecDeque<Result<GcsCreatedObject, GcsClientError>>,
    objects: BTreeMap<String, GcsReadObject>,
    requests: Vec<GcsCreateRequest>,
    reads: usize,
}

impl FakeGcsClient {
    fn with_create_results(
        results: impl IntoIterator<Item = Result<GcsCreatedObject, GcsClientError>>,
    ) -> Self {
        let client = Self::default();
        client
            .state
            .lock()
            .expect("fake state")
            .create_results
            .extend(results);
        client
    }

    fn overwrite(&self, name: &str, bytes: &[u8]) {
        let mut state = self.state.lock().expect("fake state");
        let object = state.objects.get_mut(name).expect("stored fake object");
        *object = GcsReadObject::new(bytes.to_vec(), object.metadata().clone(), "2");
    }
}

#[async_trait]
impl GcsObjectClient for FakeGcsClient {
    async fn create_object(
        &self,
        request: GcsCreateRequest,
    ) -> Result<GcsCreatedObject, GcsClientError> {
        let mut state = self.state.lock().expect("fake state");
        state.requests.push(request.clone());
        let result = state
            .create_results
            .pop_front()
            .unwrap_or_else(|| Ok(GcsCreatedObject::new("1")));
        if let Ok(created) = &result {
            state.objects.insert(
                request.name().to_owned(),
                GcsReadObject::new(
                    request.bytes().to_vec(),
                    request.metadata().clone(),
                    created.generation(),
                ),
            );
        }
        result
    }

    async fn read_object(
        &self,
        _bucket: &str,
        name: &str,
    ) -> Result<GcsReadObject, GcsClientError> {
        let mut state = self.state.lock().expect("fake state");
        state.reads += 1;
        state
            .objects
            .get(name)
            .cloned()
            .ok_or(GcsClientError::Fatal)
    }
}

fn context_with_observation(observation_id: &str) -> EvidenceContext {
    let provenance = EvidenceProvenance::new(
        "rpc-evidence-v1",
        observation_id,
        [
            ("attempt", "1"),
            ("chain", "base"),
            ("height", "42"),
            ("http-status", "200"),
            ("method", "eth_getBlockByNumber"),
            ("provider", "alchemy"),
        ],
    )
    .expect("valid provenance");
    EvidenceContext::new("alchemy-base", "2026-10-02")
        .expect("valid context")
        .with_provenance(provenance)
}

fn context() -> EvidenceContext {
    context_with_observation("alchemy:base:block:42:attempt:1")
}

fn gcs_store(client: FakeGcsClient) -> GcsEvidenceStore<FakeGcsClient> {
    GcsEvidenceStore::new(
        client,
        "agent-economy-evidence",
        GcsRetryPolicy::new(3).expect("bounded retries"),
    )
    .expect("valid store")
}

#[tokio::test]
async fn writes_create_only_with_deterministic_provenance_metadata() {
    let client = FakeGcsClient::default();
    let store = gcs_store(client.clone());

    let receipt = store
        .create(&context(), b"immutable evidence")
        .await
        .expect("create evidence");

    assert_eq!(CreateDisposition::Created, receipt.disposition);
    let state = client.state.lock().expect("fake state");
    assert_eq!(1, state.requests.len());
    let request = &state.requests[0];
    assert_eq!("agent-economy-evidence", request.bucket());
    assert_eq!(Some(0), request.if_generation_match());
    assert_eq!(receipt.object.name(), request.name());
    assert_eq!(
        BTreeMap::from([
            ("evidence-observation-id".to_owned(), "alchemy:base:block:42:attempt:1".to_owned()),
            ("evidence-observed-date".to_owned(), "2026-10-02".to_owned()),
            ("evidence-parser-version".to_owned(), "rpc-evidence-v1".to_owned()),
            (
                "evidence-replay-inputs".to_owned(),
                r#"{"attempt":"1","chain":"base","height":"42","http-status":"200","method":"eth_getBlockByNumber","provider":"alchemy"}"#.to_owned(),
            ),
            ("evidence-sha256".to_owned(), receipt.object.sha256()),
            ("evidence-source".to_owned(), "alchemy-base".to_owned()),
        ]),
        request.metadata().clone()
    );
}

#[tokio::test]
async fn precondition_failure_is_idempotent_only_when_stored_bytes_match() {
    let client = FakeGcsClient::with_create_results([
        Ok(GcsCreatedObject::new("1")),
        Err(GcsClientError::PreconditionFailed),
        Err(GcsClientError::PreconditionFailed),
    ]);
    let store = gcs_store(client.clone());

    let first = store
        .create(&context(), b"same immutable evidence")
        .await
        .expect("first create");
    let second = store
        .create(&context(), b"same immutable evidence")
        .await
        .expect("idempotent create");

    assert_eq!(CreateDisposition::Created, first.disposition);
    assert_eq!(CreateDisposition::AlreadyPresent, second.disposition);
    assert_eq!(first.object, second.object);

    client.overwrite(first.object.name(), b"substituted bytes");
    let error = store
        .create(&context(), b"same immutable evidence")
        .await
        .expect_err("conflicting stored bytes must fail closed");
    assert!(matches!(error, StoreError::DigestMismatch { .. }));
}

#[tokio::test]
async fn precondition_failure_rejects_different_provenance_metadata() {
    let client = FakeGcsClient::with_create_results([
        Ok(GcsCreatedObject::new("1")),
        Err(GcsClientError::PreconditionFailed),
    ]);
    let store = gcs_store(client);
    let payload = b"same immutable evidence";

    store
        .create(&context(), payload)
        .await
        .expect("first create");
    let error = store
        .create(
            &context_with_observation("alchemy:base:block:42:attempt:2"),
            payload,
        )
        .await
        .expect_err("different provenance must not be reported as idempotent");

    assert!(matches!(error, StoreError::MetadataMismatch { .. }));
}

#[tokio::test]
async fn reads_reject_corrupted_truncated_and_substituted_objects() {
    let client = FakeGcsClient::default();
    let store = gcs_store(client.clone());
    let receipt = store
        .create(&context(), b"complete immutable evidence")
        .await
        .expect("create evidence");

    for bad_bytes in [
        b"complete immutable evidenc".as_slice(),
        b"corrupted immutable evidence".as_slice(),
        b"substituted private payload".as_slice(),
    ] {
        client.overwrite(receipt.object.name(), bad_bytes);
        let error = store
            .read(&receipt.object)
            .await
            .expect_err("invalid replay must fail closed");
        assert!(matches!(error, StoreError::DigestMismatch { .. }));
    }
}

#[tokio::test]
async fn read_identity_is_owned_by_backend_metadata_not_the_caller() {
    let client = FakeGcsClient::default();
    let store = gcs_store(client.clone());
    let receipt = store
        .create(&context(), b"generation-bound evidence")
        .await
        .expect("create evidence");
    assert_eq!(receipt.generation.as_deref(), Some("1"));

    let first = store
        .read_with_identity(&receipt.object)
        .await
        .expect("read generation one");
    assert_eq!(first.generation.as_deref(), Some("1"));

    client.overwrite(receipt.object.name(), b"generation-bound evidence");
    let replaced = store
        .read_with_identity(&receipt.object)
        .await
        .expect("same bytes at replacement generation");
    assert_eq!(replaced.generation.as_deref(), Some("2"));
}

#[tokio::test]
async fn retries_only_retryable_failures_with_a_hard_attempt_limit() {
    let client = FakeGcsClient::with_create_results([
        Err(GcsClientError::Retryable),
        Err(GcsClientError::Retryable),
        Ok(GcsCreatedObject::new("1")),
    ]);
    let store = gcs_store(client.clone());

    store
        .create(&context(), b"eventually archived")
        .await
        .expect("bounded retry succeeds");
    assert_eq!(3, client.state.lock().expect("fake state").requests.len());

    let client = FakeGcsClient::with_create_results([
        Err(GcsClientError::Retryable),
        Err(GcsClientError::Retryable),
        Err(GcsClientError::Retryable),
        Ok(GcsCreatedObject::new("1")),
    ]);
    let store = gcs_store(client.clone());
    let error = store
        .create(&context(), b"retry budget exhausted")
        .await
        .expect_err("retry budget must be hard");
    assert!(matches!(error, StoreError::Remote));
    assert_eq!(3, client.state.lock().expect("fake state").requests.len());
}

#[tokio::test]
async fn rejects_missing_or_payload_shaped_provenance_before_network_io() {
    let client = FakeGcsClient::default();
    let store = gcs_store(client.clone());
    let context = EvidenceContext::new("alchemy-base", "2026-10-02").expect("valid context");

    let error = store
        .create(&context, b"evidence")
        .await
        .expect_err("production writes require provenance");
    assert!(matches!(error, StoreError::MissingProvenance));
    assert!(client.state.lock().expect("fake state").requests.is_empty());

    assert!(
        EvidenceProvenance::new(
            "rpc-evidence-v1",
            "observation-1",
            [("payload", "private-token-abcdef")],
        )
        .is_err()
    );
    assert!(
        EvidenceProvenance::new(
            "rpc-evidence-v1",
            "alchemy:base:block:42:attempt:0",
            [
                ("attempt", "0"),
                ("chain", "base"),
                ("height", "42"),
                ("http-status", "200"),
                ("method", "eth_getBlockByNumber"),
                ("provider", "alchemy"),
            ],
        )
        .is_ok()
    );
}
