use std::{io::Read, path::PathBuf, sync::Arc};

use agent_economy_adapter_api::ProtocolAdapter;
use agent_economy_contracts::{Observation, ProtocolObservation};
use agent_economy_evidence_store::{
    CreateReceipt, EvidenceContext, EvidenceProvenance, EvidenceStore, FilesystemEvidenceStore,
    GcsEvidenceStore, GcsObjectClient,
};
use agent_economy_mpp_adapter::MppDiscoveryAdapter;
use agent_economy_rpc_collector::{
    Chain, CollectRequest, Collector, CollectorError, CursorCheckpoint, CursorStore,
    EvidenceArchive, RetryDelay, RetryPolicy, RpcEvidence, RpcTransport,
};
use agent_economy_x402_adapter::{
    EvidenceContext as X402EvidenceContext, EvidenceKind, X402Adapter,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::worker::{
    CollectionAdmission, HandlerFailure, JobResult, LeasedJob, WorkerHandler, WorkerJobStore,
    WorkerMode, WorkerStoreError,
};

const MAX_RANGE_HEIGHTS: u64 = 10_000;
const MAX_INPUTS: usize = 10_000;
const MAX_MANIFEST_BYTES: u64 = 8 * 1024 * 1024;
const MAX_EVIDENCE_BYTES: usize = 4 * 1024 * 1024;

const MAX_OBSERVATIONS: usize = 50_000;
const X402_PARSER_VERSION: &str = "x402-adapter@1";
const MPP_PARSER_VERSION: &str = "mpp-adapter@1";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionError {
    InvalidInput,
    EvidenceUnavailable,
    CommitUnavailable,
}

impl std::fmt::Display for CollectionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInput => "invalid collection input",
            Self::EvidenceUnavailable => "collection evidence store unavailable",
            Self::CommitUnavailable => "collection commit store unavailable",
        })
    }
}

impl std::error::Error for CollectionError {}

#[derive(Clone, Debug)]
pub struct ArchivedEvidence {
    evidence_id: String,
    sha256: String,
    storage_uri: String,
    media_type: String,
    byte_length: u64,
    height: u64,
    verified: bool,
    archived_bytes: Vec<u8>,
}

impl ArchivedEvidence {
    pub fn evidence_id(&self) -> &str {
        &self.evidence_id
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn storage_uri(&self) -> &str {
        &self.storage_uri
    }

    pub fn media_type(&self) -> &str {
        &self.media_type
    }

    pub const fn byte_length(&self) -> u64 {
        self.byte_length
    }

    pub const fn height(&self) -> u64 {
        self.height
    }

    pub const fn verified(&self) -> bool {
        self.verified
    }
}

#[derive(Clone, Debug)]
pub struct CollectedObservation {
    observation: Observation,
    protocol: &'static str,
    evidence_id: String,
    observation_hash: String,
    height: u64,
}

impl CollectedObservation {
    pub fn id(&self) -> &str {
        self.observation.id()
    }

    pub const fn protocol(&self) -> &'static str {
        self.protocol
    }

    pub fn evidence_id(&self) -> &str {
        &self.evidence_id
    }

    pub fn observation_hash(&self) -> &str {
        &self.observation_hash
    }

    pub const fn height(&self) -> u64 {
        self.height
    }

    pub fn observation(&self) -> &Observation {
        &self.observation
    }
}

#[derive(Clone, Debug)]
pub struct CollectionBatch {
    chain: String,
    source_id: String,
    observed_at_unix_ms: i64,
    start_height: u64,
    end_height: u64,
    evidence: Vec<ArchivedEvidence>,
    observations: Vec<CollectedObservation>,
}

impl CollectionBatch {
    pub fn chain(&self) -> &str {
        &self.chain
    }

    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub const fn observed_at_unix_ms(&self) -> i64 {
        self.observed_at_unix_ms
    }

    pub const fn start_height(&self) -> u64 {
        self.start_height
    }

    pub const fn end_height(&self) -> u64 {
        self.end_height
    }

    pub fn evidence(&self) -> &[ArchivedEvidence] {
        &self.evidence
    }

    pub fn observations(&self) -> &[CollectedObservation] {
        &self.observations
    }

    fn output_sha256(&self) -> String {
        let mut evidence = self
            .evidence
            .iter()
            .map(|item| {
                json!({
                    "byte_length": item.byte_length,
                    "evidence_id": item.evidence_id,
                    "height": item.height,
                    "media_type": item.media_type,
                    "sha256": item.sha256,
                    "storage_uri": item.storage_uri,
                })
            })
            .collect::<Vec<_>>();
        evidence.sort_by_key(|item| {
            (
                item["height"].as_u64().unwrap_or_default(),
                item["evidence_id"].as_str().unwrap_or_default().to_owned(),
            )
        });
        let mut observations = self
            .observations
            .iter()
            .map(|item| {
                json!({
                    "evidence_id": item.evidence_id,
                    "height": item.height,
                    "observation_hash": item.observation_hash,
                    "observation_id": item.id(),
                    "parser_version": item.observation.provenance().parser_version(),
                    "protocol": item.protocol,
                })
            })
            .collect::<Vec<_>>();
        observations.sort_by_key(|item| {
            (
                item["height"].as_u64().unwrap_or_default(),
                item["protocol"].as_str().unwrap_or_default().to_owned(),
                item["observation_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            )
        });
        let canonical = json!({
            "chain": self.chain,
            "end_height": self.end_height,
            "evidence": evidence,
            "observations": observations,
            "source_id": self.source_id,
            "start_height": self.start_height,
        });
        format!("{:x}", Sha256::digest(canonical.to_string().as_bytes()))
    }
}

#[async_trait]
pub trait CollectionCommitStore: Send + Sync + 'static {
    async fn commit(&self, job: &LeasedJob, batch: CollectionBatch) -> Result<(), CollectionError>;
}

pub struct PostgresCollectionCommitStore {
    client: Arc<Mutex<Client>>,
    evidence_verifier: Arc<Mutex<Client>>,
    namespace_id: String,
}

impl PostgresCollectionCommitStore {
    pub fn new(client: Client, evidence_verifier: Client, namespace_id: String) -> Self {
        Self::from_shared(
            Arc::new(Mutex::new(client)),
            Arc::new(Mutex::new(evidence_verifier)),
            namespace_id,
        )
    }

    pub fn from_shared(
        client: Arc<Mutex<Client>>,
        evidence_verifier: Arc<Mutex<Client>>,
        namespace_id: String,
    ) -> Self {
        Self {
            client,
            evidence_verifier,
            namespace_id,
        }
    }
}

#[async_trait]
impl CollectionCommitStore for PostgresCollectionCommitStore {
    async fn commit(&self, job: &LeasedJob, batch: CollectionBatch) -> Result<(), CollectionError> {
        {
            let verifier = self.evidence_verifier.lock().await;
            for item in batch.evidence() {
                let height =
                    i64::try_from(item.height).map_err(|_| CollectionError::InvalidInput)?;
                let byte_length =
                    i64::try_from(item.byte_length).map_err(|_| CollectionError::InvalidInput)?;
                verifier
                    .execute(
                        "SELECT agent_economy.attest_collection_evidence(\
                         $1::text::uuid, $2::text::uuid, $3::text::uuid, $4, $5, $6, $7, \
                         $8::bigint, $9::bigint)",
                        &[
                            &self.namespace_id,
                            &job.job_id,
                            &job.lease_token,
                            &item.evidence_id,
                            &item.sha256,
                            &item.storage_uri,
                            &item.media_type,
                            &byte_length,
                            &height,
                        ],
                    )
                    .await
                    .map_err(|_| CollectionError::CommitUnavailable)?;
            }
        }
        let start_height =
            i64::try_from(batch.start_height).map_err(|_| CollectionError::InvalidInput)?;
        let end_height =
            i64::try_from(batch.end_height).map_err(|_| CollectionError::InvalidInput)?;
        let evidence_json = serde_json::to_string(
            &batch
                .evidence
                .iter()
                .map(|item| {
                    json!({
                        "evidence_id": item.evidence_id,
                        "sha256": item.sha256,
                        "storage_uri": item.storage_uri,
                        "media_type": item.media_type,
                        "byte_length": item.byte_length,
                        "height": item.height,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|_| CollectionError::InvalidInput)?;
        let observations_json = serde_json::to_string(
            &batch
                .observations
                .iter()
                .map(|item| {
                    json!({
                        "observation_id": item.id(),
                        "protocol": item.protocol,
                        "evidence_id": item.evidence_id,
                        "observation_hash": item.observation_hash,
                        "parser_version": item.observation.provenance().parser_version(),
                        "height": item.height,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|_| CollectionError::InvalidInput)?;
        let batch_sha256 = batch.output_sha256();
        let client = self.client.lock().await;
        let committed = client
            .query_one(
                "SELECT agent_economy.commit_collection_batch(\
                    $1::text::uuid, $2::text::uuid, $3, $4::text::uuid, $5, $6, $7, $8, \
                    $9::bigint, $10::bigint, $11::bigint, $12::text::jsonb, $13::text::jsonb)",
                &[
                    &self.namespace_id,
                    &job.job_id,
                    &job.lease_owner,
                    &job.lease_token,
                    &job.input_sha256,
                    &batch_sha256,
                    &batch.chain,
                    &batch.source_id,
                    &batch.observed_at_unix_ms,
                    &start_height,
                    &end_height,
                    &evidence_json,
                    &observations_json,
                ],
            )
            .await
            .map_err(|error| {
                tracing::error!(
                    database_code = error.code().map(tokio_postgres::error::SqlState::code),
                    database_message = error
                        .as_db_error()
                        .map(tokio_postgres::error::DbError::message),
                    "collection commit rejected"
                );
                CollectionError::CommitUnavailable
            })?
            .get::<_, bool>(0);
        if committed {
            Ok(())
        } else {
            Err(CollectionError::CommitUnavailable)
        }
    }
}

pub struct PostgresCollectionJobStore {
    client: Arc<Mutex<Client>>,
    namespace_id: String,
}

impl PostgresCollectionJobStore {
    pub fn from_shared(client: Arc<Mutex<Client>>, namespace_id: String) -> Self {
        Self {
            client,
            namespace_id,
        }
    }
}

#[async_trait]
impl WorkerJobStore for PostgresCollectionJobStore {
    async fn claim(
        &self,
        mode: WorkerMode,
        lease_owner: &str,
        lease_seconds: u64,
    ) -> Result<Option<LeasedJob>, WorkerStoreError> {
        if mode != WorkerMode::Collect
            || !valid_lease_owner(lease_owner)
            || !(1..=3_600).contains(&lease_seconds)
        {
            return Err(WorkerStoreError::Invalid);
        }
        let lease_seconds = i64::try_from(lease_seconds).map_err(|_| WorkerStoreError::Invalid)?;
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT job_id::text, mode, job_kind, input_sha256, attempt_count, \
                        lease_owner, lease_token::text, collection_chain_scope, \
                        collection_source_id, collection_start_height, collection_end_height, \
                        collection_acquisition_contract, collection_evidence_contract \
                 FROM agent_economy.claim_collection_job($1::text::uuid, $2, $3::bigint)",
                &[&self.namespace_id, &lease_owner, &lease_seconds],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?;
        row.map(collection_row_to_job).transpose()
    }

    async fn renew(&self, job: &LeasedJob, lease_seconds: u64) -> Result<(), WorkerStoreError> {
        if job.mode != WorkerMode::Collect || !(1..=3_600).contains(&lease_seconds) {
            return Err(WorkerStoreError::Invalid);
        }
        let lease_seconds = i64::try_from(lease_seconds).map_err(|_| WorkerStoreError::Invalid)?;
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.renew_collection_job_lease(\
                    $1::text::uuid, $2::text::uuid, $3, $4::text::uuid, $5::bigint)",
                &[
                    &self.namespace_id,
                    &job.job_id,
                    &job.lease_owner,
                    &job.lease_token,
                    &lease_seconds,
                ],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?
            .get::<_, bool>(0);
        changed.then_some(()).ok_or(WorkerStoreError::Conflict)
    }

    async fn complete(&self, job: &LeasedJob, output_sha256: &str) -> Result<(), WorkerStoreError> {
        if job.mode != WorkerMode::Collect || !valid_sha256(output_sha256) {
            return Err(WorkerStoreError::Invalid);
        }
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.complete_collection_job(\
                    $1::text::uuid, $2::text::uuid, $3, $4::text::uuid, $5)",
                &[
                    &self.namespace_id,
                    &job.job_id,
                    &job.lease_owner,
                    &job.lease_token,
                    &output_sha256,
                ],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?
            .get::<_, bool>(0);
        changed.then_some(()).ok_or(WorkerStoreError::Conflict)
    }

    async fn fail(
        &self,
        job: &LeasedJob,
        error_code: &str,
        retryable: bool,
    ) -> Result<(), WorkerStoreError> {
        if job.mode != WorkerMode::Collect || !valid_collection_error_code(error_code) {
            return Err(WorkerStoreError::Invalid);
        }
        let retry_delay_seconds = 30_i64;
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.fail_collection_job(\
                    $1::text::uuid, $2::text::uuid, $3, $4::text::uuid, $5, $6, $7::bigint)",
                &[
                    &self.namespace_id,
                    &job.job_id,
                    &job.lease_owner,
                    &job.lease_token,
                    &error_code,
                    &retryable,
                    &retry_delay_seconds,
                ],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?
            .get::<_, bool>(0);
        changed.then_some(()).ok_or(WorkerStoreError::Conflict)
    }
}

fn collection_row_to_job(row: tokio_postgres::Row) -> Result<LeasedJob, WorkerStoreError> {
    let mode =
        WorkerMode::parse(&row.get::<_, String>(1)).map_err(|_| WorkerStoreError::Unavailable)?;
    let attempt = row.get::<_, i16>(4);
    Ok(LeasedJob {
        job_id: row.get(0),
        mode,
        job_kind: row.get(2),
        input_sha256: row.get(3),
        attempt: u16::try_from(attempt).map_err(|_| WorkerStoreError::Unavailable)?,
        lease_owner: row.get(5),
        lease_token: row.get(6),
        collection_admission: Some(CollectionAdmission {
            chain_scope: row.get(7),
            source_id: row.get(8),
            start_height: u64::try_from(row.get::<_, i64>(9))
                .map_err(|_| WorkerStoreError::Unavailable)?,
            end_height: u64::try_from(row.get::<_, i64>(10))
                .map_err(|_| WorkerStoreError::Unavailable)?,
            acquisition_contract: row.get(11),
            evidence_contract: row.get(12),
        }),
    })
}

fn valid_lease_owner(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn valid_collection_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

#[async_trait]
pub trait CollectionEvidenceStore: Send + Sync + 'static {
    async fn archive_verified(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, CollectionError>;
}

#[async_trait]
impl CollectionEvidenceStore for FilesystemEvidenceStore {
    async fn archive_verified(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, CollectionError> {
        let receipt = EvidenceStore::create(self, context, evidence)
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)?;
        let replayed = EvidenceStore::read(self, &receipt.object)
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)?;
        if replayed != evidence {
            return Err(CollectionError::EvidenceUnavailable);
        }
        Ok(receipt)
    }
}

#[async_trait]
impl<C> CollectionEvidenceStore for GcsEvidenceStore<C>
where
    C: GcsObjectClient + 'static,
{
    async fn archive_verified(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, CollectionError> {
        let receipt = EvidenceStore::create(self, context, evidence)
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)?;
        let replayed = EvidenceStore::read(self, &receipt.object)
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)?;
        if replayed != evidence {
            return Err(CollectionError::EvidenceUnavailable);
        }
        Ok(receipt)
    }
}

struct BatchRpcArchive<'a, S: ?Sized> {
    store: &'a S,
    evidence: Vec<ArchivedEvidence>,
}

impl<'a, S: ?Sized> BatchRpcArchive<'a, S> {
    fn new(store: &'a S) -> Self {
        Self {
            store,
            evidence: Vec::new(),
        }
    }
}

impl<S> EvidenceArchive for BatchRpcArchive<'_, S>
where
    S: CollectionEvidenceStore + ?Sized,
{
    async fn archive(
        &mut self,
        observed_date: &str,
        evidence: &RpcEvidence,
    ) -> Result<String, CollectorError> {
        let source = format!("alchemy-{}", evidence.chain());
        let encoded = evidence.encode();
        if encoded.len() > MAX_EVIDENCE_BYTES {
            return Err(CollectorError::ResponseTooLarge);
        }
        let provenance = EvidenceProvenance::new(
            "rpc-evidence-v1",
            &format!(
                "alchemy:{}:block:{}:attempt:{}",
                evidence.chain(),
                evidence.requested_height(),
                evidence.attempt()
            ),
            [
                ("attempt", evidence.attempt().to_string()),
                ("chain", evidence.chain().as_str().to_owned()),
                ("height", evidence.requested_height().to_string()),
                ("http-status", evidence.http_status().to_string()),
                ("method", evidence.method().to_owned()),
                ("provider", evidence.provider().to_owned()),
            ],
        )
        .map_err(|_| CollectorError::Evidence("invalid RPC provenance".into()))?;
        let context = EvidenceContext::new(&source, observed_date)
            .map_err(|_| CollectorError::Evidence("invalid RPC context".into()))?
            .with_provenance(provenance);
        let receipt = self
            .store
            .archive_verified(&context, &encoded)
            .await
            .map_err(|_| CollectorError::Evidence("RPC archive unavailable".into()))?;
        let object_name = receipt.object.name().to_owned();
        self.evidence.push(ArchivedEvidence {
            evidence_id: format!("evidence:sha256:{}", receipt.object.sha256()),
            sha256: receipt.object.sha256(),
            storage_uri: object_name.clone(),
            media_type: "application/vnd.agent-economy.rpc".into(),
            byte_length: encoded.len() as u64,
            height: evidence.requested_height(),
            verified: true,
            archived_bytes: encoded,
        });
        Ok(object_name)
    }
}

#[derive(Default)]
struct StagedCursorStore {
    checkpoint: Option<CursorCheckpoint>,
}

impl CursorStore for StagedCursorStore {
    async fn load(&mut self, _chain: Chain) -> Result<Option<CursorCheckpoint>, CollectorError> {
        Ok(self.checkpoint)
    }

    async fn initialize(
        &mut self,
        _chain: Chain,
        start_height: u64,
    ) -> Result<CursorCheckpoint, CollectorError> {
        let checkpoint = CursorCheckpoint::new(start_height, 0);
        self.checkpoint = Some(checkpoint);
        Ok(checkpoint)
    }

    async fn compare_and_set(
        &mut self,
        _chain: Chain,
        expected: CursorCheckpoint,
        next_height: u64,
    ) -> Result<CursorCheckpoint, CollectorError> {
        if self.checkpoint != Some(expected) {
            return Err(CollectorError::CursorConflict);
        }
        let checkpoint = CursorCheckpoint::new(next_height, expected.version() + 1);
        self.checkpoint = Some(checkpoint);
        Ok(checkpoint)
    }
}

#[derive(Default)]
struct CollectionRetryDelay;

impl RetryDelay for CollectionRetryDelay {
    async fn wait(&mut self, milliseconds: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
    }
}

pub struct CollectionHandler<S: ?Sized, C, T> {
    input_root: PathBuf,
    evidence_store: Arc<S>,
    commit_store: Arc<C>,
    rpc_transport: Mutex<T>,
}

impl<S: ?Sized, C, T> CollectionHandler<S, C, T> {
    pub fn new(
        input_root: PathBuf,
        evidence_store: Arc<S>,
        commit_store: Arc<C>,
        rpc_transport: T,
    ) -> Self {
        Self {
            input_root,
            evidence_store,
            commit_store,
            rpc_transport: Mutex::new(rpc_transport),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    schema_version: u32,
    chain: String,
    source_id: String,
    observed_at_unix_ms: i64,
    observed_date: String,
    start_height: u64,
    end_height: u64,
    inputs: Vec<ManifestInput>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestInput {
    height: u64,
    kind: InputKind,
    context: String,
    evidence_base64: String,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InputKind {
    ChainTransfer,
    X402Runtime,
    X402WellKnown,
    #[serde(rename = "x402_openapi")]
    X402OpenApi,
    #[serde(rename = "mpp_openapi")]
    MppOpenApi,
}

#[async_trait]
impl<S, C, T> WorkerHandler for CollectionHandler<S, C, T>
where
    S: CollectionEvidenceStore + ?Sized,
    C: CollectionCommitStore,
    T: RpcTransport + Send + 'static,
{
    async fn process(&self, job: &LeasedJob) -> Result<JobResult, HandlerFailure> {
        if job.mode != WorkerMode::Collect || job.job_kind != "chain-protocol-range" {
            return Err(poison("invalid_collect_job"));
        }
        let manifest = self
            .load_manifest(job)
            .map_err(|_| poison("invalid_collect_input"))?;
        let batch = self
            .build_batch(job, manifest)
            .await
            .map_err(|error| match error {
                CollectionError::InvalidInput => poison("invalid_protocol_evidence"),
                CollectionError::EvidenceUnavailable => retryable("evidence_unavailable"),
                CollectionError::CommitUnavailable => retryable("commit_unavailable"),
            })?;
        let output_sha256 = batch.output_sha256();
        self.commit_store
            .commit(job, batch)
            .await
            .map_err(|error| match error {
                CollectionError::InvalidInput => poison("invalid_collect_commit"),
                CollectionError::EvidenceUnavailable => retryable("evidence_unavailable"),
                CollectionError::CommitUnavailable => retryable("commit_unavailable"),
            })?;
        JobResult::new(&output_sha256).map_err(|_| poison("invalid_collect_output"))
    }
}

impl<S, C, T> CollectionHandler<S, C, T>
where
    S: CollectionEvidenceStore + ?Sized,
    C: CollectionCommitStore,
    T: RpcTransport + Send,
{
    fn load_manifest(&self, job: &LeasedJob) -> Result<Manifest, CollectionError> {
        if !valid_sha256(&job.input_sha256) {
            return Err(CollectionError::InvalidInput);
        }
        let path = self.input_root.join(format!("{}.json", job.input_sha256));
        let root =
            std::fs::canonicalize(&self.input_root).map_err(|_| CollectionError::InvalidInput)?;
        let path = std::fs::canonicalize(path).map_err(|_| CollectionError::InvalidInput)?;
        if path.parent() != Some(root.as_path()) {
            return Err(CollectionError::InvalidInput);
        }
        let file = std::fs::File::open(path).map_err(|_| CollectionError::InvalidInput)?;
        if file
            .metadata()
            .map_err(|_| CollectionError::InvalidInput)?
            .len()
            > MAX_MANIFEST_BYTES
        {
            return Err(CollectionError::InvalidInput);
        }
        let mut bytes = Vec::new();
        file.take(MAX_MANIFEST_BYTES + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| CollectionError::InvalidInput)?;
        if bytes.len() as u64 > MAX_MANIFEST_BYTES {
            return Err(CollectionError::InvalidInput);
        }
        if format!("{:x}", Sha256::digest(&bytes)) != job.input_sha256 {
            return Err(CollectionError::InvalidInput);
        }
        let manifest: Manifest =
            serde_json::from_slice(&bytes).map_err(|_| CollectionError::InvalidInput)?;
        validate_manifest(&manifest)?;
        Ok(manifest)
    }

    async fn build_batch(
        &self,
        job: &LeasedJob,
        manifest: Manifest,
    ) -> Result<CollectionBatch, CollectionError> {
        let admission = job
            .collection_admission
            .as_ref()
            .ok_or(CollectionError::InvalidInput)?;
        if admission.chain_scope != manifest.chain
            || admission.source_id != manifest.source_id
            || admission.start_height != manifest.start_height
            || admission.end_height != manifest.end_height
            || admission.acquisition_contract != "alchemy-rpc-block-v1"
            || admission.evidence_contract != "evidence-store-create-read-sha256-v1"
        {
            return Err(CollectionError::InvalidInput);
        }
        let chain = manifest_chain(&manifest.chain)?;
        let height_count = manifest
            .end_height
            .checked_sub(manifest.start_height)
            .and_then(|span| span.checked_add(1))
            .ok_or(CollectionError::InvalidInput)?;
        let request_budget = height_count.saturating_mul(4).min(MAX_RANGE_HEIGHTS);
        let request = CollectRequest::try_new(
            chain,
            manifest.start_height,
            height_count,
            request_budget,
            manifest.observed_date.clone(),
        )
        .map_err(|_| CollectionError::InvalidInput)?;
        let mut transport = self.rpc_transport.lock().await;
        let mut rpc_archive = BatchRpcArchive::new(self.evidence_store.as_ref());
        let mut staged_cursor = StagedCursorStore::default();
        let mut retry_delay = CollectionRetryDelay;
        let report = Collector::new(
            &mut *transport,
            &mut rpc_archive,
            &mut staged_cursor,
            &mut retry_delay,
        )
        .with_retry_policy(RetryPolicy::default())
        .collect(request)
        .await
        .map_err(|error| match error.cause() {
            CollectorError::Evidence(_)
            | CollectorError::Transport
            | CollectorError::HttpStatus(_) => CollectionError::EvidenceUnavailable,
            _ => CollectionError::InvalidInput,
        })?;
        let expected_next_height = manifest
            .end_height
            .checked_add(1)
            .ok_or(CollectionError::InvalidInput)?;
        if report.next_height() != expected_next_height
            || report.budget_exhausted()
            || !report.gaps().is_empty()
            || rpc_archive.evidence.len() < height_count as usize
        {
            return Err(CollectionError::InvalidInput);
        }
        let evidence = rpc_archive.evidence;
        let mut observations = Vec::new();
        for archived in &evidence {
            let rpc = RpcEvidence::decode(&archived.archived_bytes)
                .map_err(|_| CollectionError::InvalidInput)?;
            for input in protocol_inputs_from_rpc(rpc.body(), archived.height)? {
                let bytes = STANDARD
                    .decode(&input.evidence_base64)
                    .map_err(|_| CollectionError::InvalidInput)?;
                if bytes.is_empty() || bytes.len() > MAX_EVIDENCE_BYTES {
                    return Err(CollectionError::InvalidInput);
                }
                for observation in parse_protocol_input(&manifest, &input, &bytes)? {
                    if observations.len() >= MAX_OBSERVATIONS {
                        return Err(CollectionError::InvalidInput);
                    }
                    let protocol = match observation.protocol() {
                        ProtocolObservation::X402(_) => "x402",
                        ProtocolObservation::Mpp(_) | ProtocolObservation::MppDiscovery(_) => "mpp",
                    };
                    let observation_hash = format!("{:x}", Sha256::digest(observation.encode()));
                    observations.push(CollectedObservation {
                        observation,
                        protocol,
                        evidence_id: archived.evidence_id.clone(),
                        observation_hash,
                        height: input.height,
                    });
                }
            }
        }
        Ok(CollectionBatch {
            chain: manifest.chain,
            source_id: manifest.source_id,
            observed_at_unix_ms: manifest.observed_at_unix_ms,
            start_height: manifest.start_height,
            end_height: manifest.end_height,
            evidence,
            observations,
        })
    }
}

fn protocol_inputs_from_rpc(
    bytes: &[u8],
    height: u64,
) -> Result<Vec<ManifestInput>, CollectionError> {
    let value: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|_| CollectionError::InvalidInput)?;
    let Some(inputs) = value
        .get("result")
        .and_then(|result| result.get("agentEconomyProtocolEvidence"))
    else {
        return Ok(Vec::new());
    };
    let inputs = inputs.as_array().ok_or(CollectionError::InvalidInput)?;
    if inputs.len() > MAX_INPUTS {
        return Err(CollectionError::InvalidInput);
    }
    inputs
        .iter()
        .map(|input| {
            let parsed: ManifestInput =
                serde_json::from_value(input.clone()).map_err(|_| CollectionError::InvalidInput)?;
            if parsed.height != height || parsed.context.trim().is_empty() {
                return Err(CollectionError::InvalidInput);
            }
            Ok(parsed)
        })
        .collect()
}

fn parse_protocol_input(
    manifest: &Manifest,
    input: &ManifestInput,
    bytes: &[u8],
) -> Result<Vec<Observation>, CollectionError> {
    match input.kind {
        InputKind::ChainTransfer => Ok(Vec::new()),
        InputKind::X402Runtime | InputKind::X402WellKnown | InputKind::X402OpenApi => {
            let kind = match input.kind {
                InputKind::X402Runtime => EvidenceKind::Runtime402,
                InputKind::X402WellKnown => EvidenceKind::WellKnown,
                InputKind::X402OpenApi => EvidenceKind::OpenApi,
                _ => unreachable!(),
            };
            let context = X402EvidenceContext::new(
                kind,
                manifest.source_id.clone(),
                input.context.clone(),
                manifest.observed_at_unix_ms,
            )
            .map_err(|_| CollectionError::InvalidInput)?;
            X402Adapter::new(context, X402_PARSER_VERSION)
                .map_err(|_| CollectionError::InvalidInput)?
                .observe(bytes)
                .map_err(|_| CollectionError::InvalidInput)
        }
        InputKind::MppOpenApi => MppDiscoveryAdapter::new(
            input.context.clone(),
            manifest.source_id.clone(),
            manifest.observed_at_unix_ms,
            MPP_PARSER_VERSION,
        )
        .map_err(|_| CollectionError::InvalidInput)?
        .observe(bytes)
        .map_err(|_| CollectionError::InvalidInput),
    }
}

fn validate_manifest(manifest: &Manifest) -> Result<(), CollectionError> {
    let covered_heights = manifest
        .inputs
        .iter()
        .map(|input| input.height)
        .collect::<std::collections::BTreeSet<_>>();
    let expected_source = format!("alchemy-{}", manifest.chain);
    if manifest.schema_version != 1
        || !matches!(
            manifest.chain.as_str(),
            "ethereum" | "base" | "solana" | "tempo"
        )
        || manifest.source_id != expected_source
        || manifest.observed_at_unix_ms <= 0
        || manifest.end_height < manifest.start_height
        || manifest
            .end_height
            .saturating_sub(manifest.start_height)
            .saturating_add(1)
            > MAX_RANGE_HEIGHTS
        || manifest.inputs.is_empty()
        || manifest.inputs.len() > MAX_INPUTS
        || covered_heights
            .iter()
            .any(|height| *height < manifest.start_height || *height > manifest.end_height)
        || manifest.inputs.iter().any(|input| {
            input.height < manifest.start_height
                || input.height > manifest.end_height
                || input.context.trim().is_empty()
        })
    {
        return Err(CollectionError::InvalidInput);
    }
    EvidenceContext::new(&manifest.source_id, &manifest.observed_date)
        .map(|_| ())
        .map_err(|_| CollectionError::InvalidInput)
}

fn manifest_chain(value: &str) -> Result<Chain, CollectionError> {
    match value {
        "ethereum" => Ok(Chain::Ethereum),
        "base" => Ok(Chain::Base),
        "solana" => Ok(Chain::Solana),
        "tempo" => Ok(Chain::Tempo),
        _ => Err(CollectionError::InvalidInput),
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn poison(code: &str) -> HandlerFailure {
    HandlerFailure::poison(code).expect("static collection error codes are valid")
}

fn retryable(code: &str) -> HandlerFailure {
    HandlerFailure::retryable(code).expect("static collection error codes are valid")
}
