use std::{io::Read, path::PathBuf, sync::Arc, time::Duration};

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

const MAX_OBSERVATIONS: usize = 200_000;
const X402_PARSER_VERSION: &str = "x402-adapter@1";
const MPP_PARSER_VERSION: &str = "mpp-adapter@1";
const CDP_BAZAAR_BASE_URL: &str =
    "https://api.cdp.coinbase.com/platform/v2/x402/discovery/resources";
const CDP_BAZAAR_PAGE_LIMIT: u64 = 100;
const MAX_BAZAAR_PAGES: u64 = 400;
const MAX_BAZAAR_CRAWL_BYTES: u64 = 256 * 1024 * 1024;
const BAZAAR_REQUEST_INTERVAL_MS: u64 = 100;
const CDP_BAZAAR_URL: &str =
    "https://api.cdp.coinbase.com/platform/v2/x402/discovery/resources?limit=100&offset=0";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CollectionError {
    InvalidInput,
    EvidenceUnavailable,
    CommitUnavailable,
}

#[derive(Clone)]
pub struct PublicSourceResponse {
    body: Vec<u8>,
}

impl PublicSourceResponse {
    pub fn try_new(status: u16, body: Vec<u8>) -> Result<Self, CollectionError> {
        if status != 200 || body.is_empty() || body.len() > MAX_EVIDENCE_BYTES {
            return Err(CollectionError::EvidenceUnavailable);
        }
        Ok(Self { body })
    }
}

#[async_trait]
pub trait PublicSourceTransport: Send + Sync + 'static {
    async fn fetch(&self, url: &str) -> Result<PublicSourceResponse, CollectionError>;
}

struct DisabledPublicSourceTransport;

#[async_trait]
impl PublicSourceTransport for DisabledPublicSourceTransport {
    async fn fetch(&self, _url: &str) -> Result<PublicSourceResponse, CollectionError> {
        Err(CollectionError::EvidenceUnavailable)
    }
}

pub struct CdpBazaarTransport {
    client: reqwest::Client,
}

impl CdpBazaarTransport {
    pub fn new() -> Result<Self, CollectionError> {
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .map_err(|_| CollectionError::EvidenceUnavailable)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl PublicSourceTransport for CdpBazaarTransport {
    async fn fetch(&self, url: &str) -> Result<PublicSourceResponse, CollectionError> {
        if !valid_bazaar_url(url) {
            return Err(CollectionError::InvalidInput);
        }
        let mut response = self
            .client
            .get(url)
            .header(reqwest::header::USER_AGENT, "agent-economy-monitor/0.1")
            .send()
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)?;
        let status = response.status().as_u16();
        if response
            .content_length()
            .is_some_and(|size| size > MAX_EVIDENCE_BYTES as u64)
        {
            return Err(CollectionError::EvidenceUnavailable);
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)?
        {
            if body
                .len()
                .checked_add(chunk.len())
                .is_none_or(|size| size > MAX_EVIDENCE_BYTES)
            {
                return Err(CollectionError::EvidenceUnavailable);
            }
            body.extend_from_slice(&chunk);
        }
        PublicSourceResponse::try_new(status, body)
    }
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
    storage_generation: Option<String>,
    media_type: String,
    byte_length: u64,
    height: u64,
    verified: bool,
    archived_bytes: Vec<u8>,
}

impl ArchivedEvidence {
    pub fn from_verified_readback(
        storage_uri: String,
        sha256: String,
        media_type: String,
        height: u64,
        archived_bytes: Vec<u8>,
    ) -> Result<Self, CollectionError> {
        if !matches!(
            media_type.as_str(),
            "application/vnd.agent-economy.rpc" | "application/json"
        ) || archived_bytes.is_empty()
            || archived_bytes.len() > MAX_EVIDENCE_BYTES
            || format!("{:x}", Sha256::digest(&archived_bytes)) != sha256
        {
            return Err(CollectionError::InvalidInput);
        }
        Ok(Self {
            evidence_id: format!("evidence:sha256:{sha256}"),
            sha256,
            storage_uri,
            storage_generation: None,
            media_type,
            byte_length: archived_bytes.len() as u64,
            height,
            verified: true,
            archived_bytes,
        })
    }

    pub fn evidence_id(&self) -> &str {
        &self.evidence_id
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn storage_uri(&self) -> &str {
        &self.storage_uri
    }

    pub fn storage_generation(&self) -> Option<&str> {
        self.storage_generation.as_deref()
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

    pub fn output_sha256(&self) -> String {
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

    pub fn evidence_manifest_sha256(&self) -> String {
        let mut evidence_only = self.clone();
        evidence_only.observations.clear();
        evidence_only.output_sha256()
    }
}

#[async_trait]
pub trait CollectionCommitStore: Send + Sync + 'static {
    async fn commit(&self, job: &LeasedJob, batch: CollectionBatch) -> Result<(), CollectionError>;
}

pub struct PostgresCollectionCommitStore {
    client: Arc<Mutex<Client>>,
}

impl PostgresCollectionCommitStore {
    pub fn new(client: Client) -> Self {
        Self::from_shared(Arc::new(Mutex::new(client)))
    }

    pub fn from_shared(client: Arc<Mutex<Client>>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl CollectionCommitStore for PostgresCollectionCommitStore {
    async fn commit(&self, job: &LeasedJob, batch: CollectionBatch) -> Result<(), CollectionError> {
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
                        "storage_generation": item.storage_generation,
                        "media_type": item.media_type,
                        "byte_length": item.byte_length,
                        "height": item.height,
                    })
                })
                .collect::<Vec<_>>(),
        )
        .map_err(|_| CollectionError::InvalidInput)?;
        let batch_sha256 = batch.evidence_manifest_sha256();
        let catalog_metrics = if batch.source_id == "cdp-x402-bazaar-base" {
            let first = batch
                .evidence
                .first()
                .ok_or(CollectionError::InvalidInput)?;
            let first_page = parse_bazaar_page(&first.archived_bytes)?;
            let reported_total = first_page
                .pagination
                .as_ref()
                .map_or(first_page.items.len() as u64, |page| page.total);
            let resources_observed = batch.evidence.iter().try_fold(0_u64, |count, evidence| {
                let page = parse_bazaar_page(&evidence.archived_bytes)?;
                count
                    .checked_add(page.items.len() as u64)
                    .ok_or(CollectionError::InvalidInput)
            })?;
            Some((
                i64::try_from(reported_total).map_err(|_| CollectionError::InvalidInput)?,
                i64::try_from(resources_observed).map_err(|_| CollectionError::InvalidInput)?,
            ))
        } else {
            None
        };
        let stage_statement = if catalog_metrics.is_some() {
            "SELECT agent_economy.stage_catalog_crawl_batch(\
                $1::text::uuid, $2, $3::text::uuid, $4, $5, $6, $7, \
                $8::bigint, $9::bigint, $10::bigint, $11::text::jsonb, \
                $12::bigint, $13::bigint)"
        } else {
            "SELECT agent_economy.stage_collection_batch(\
                $1::text::uuid, $2, $3::text::uuid, $4, $5, $6, $7, \
                $8::bigint, $9::bigint, $10::bigint, $11::text::jsonb)"
        };
        let client = self.client.lock().await;
        let mut parameters: Vec<&(dyn tokio_postgres::types::ToSql + Sync)> = vec![
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
        ];
        if let Some((reported_total, resources_observed)) = &catalog_metrics {
            parameters.push(reported_total);
            parameters.push(resources_observed);
        }
        client
            .query_one(stage_statement, &parameters)
            .await
            .map_err(|error| {
                tracing::error!(
                    database_code = error.code().map(tokio_postgres::error::SqlState::code),
                    database_message = error
                        .as_db_error()
                        .map(tokio_postgres::error::DbError::message),
                    "collection staging rejected"
                );
                CollectionError::CommitUnavailable
            })?;
        Ok(())
    }
}

pub struct PostgresCollectionJobStore {
    client: Arc<Mutex<Client>>,
}

impl PostgresCollectionJobStore {
    pub fn from_shared(client: Arc<Mutex<Client>>) -> Self {
        Self { client }
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
                 FROM agent_economy.claim_bound_collection_job($1, $2::bigint)",
                &[&lease_owner, &lease_seconds],
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
                "SELECT agent_economy.renew_bound_collection_job(\
                    $1::text::uuid, $2, $3::text::uuid, $4::bigint)",
                &[
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
                "SELECT agent_economy.complete_bound_collection_job(\
                    $1::text::uuid, $2, $3::text::uuid, $4)",
                &[
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
                "SELECT agent_economy.fail_bound_collection_job(\
                    $1::text::uuid, $2, $3::text::uuid, $4, $5, $6::bigint)",
                &[
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
        EvidenceStore::create_only(self, context, evidence)
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)
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
        EvidenceStore::create_only(self, context, evidence)
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)
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
            storage_generation: receipt.generation,
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
    public_source_transport: Arc<dyn PublicSourceTransport>,
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
            public_source_transport: Arc::new(DisabledPublicSourceTransport),
        }
    }

    pub fn with_public_source_transport(
        mut self,
        transport: Arc<dyn PublicSourceTransport>,
    ) -> Self {
        self.public_source_transport = transport;
        self
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
    #[serde(default)]
    evidence_base64: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BazaarPagination {
    limit: u64,
    offset: u64,
    total: u64,
}

#[derive(Deserialize)]
struct BazaarPage {
    items: Vec<serde_json::Value>,
    #[serde(default)]
    pagination: Option<BazaarPagination>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum InputKind {
    ChainTransfer,
    X402Runtime,
    X402WellKnown,
    #[serde(rename = "x402_openapi")]
    X402OpenApi,
    X402Bazaar,
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
        let output_sha256 = batch.evidence_manifest_sha256();
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
            || admission.evidence_contract != "evidence-store-create-read-sha256-v1"
        {
            return Err(CollectionError::InvalidInput);
        }
        if admission.acquisition_contract == "cdp-x402-bazaar-v1" {
            return self.build_public_source_batch(manifest).await;
        }
        if admission.acquisition_contract != "alchemy-rpc-block-v1" {
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
        Ok(CollectionBatch {
            chain: manifest.chain,
            source_id: manifest.source_id,
            observed_at_unix_ms: manifest.observed_at_unix_ms,
            start_height: manifest.start_height,
            end_height: manifest.end_height,
            evidence,
            observations: Vec::new(),
        })
    }

    async fn build_public_source_batch(
        &self,
        manifest: Manifest,
    ) -> Result<CollectionBatch, CollectionError> {
        if manifest.source_id != "cdp-x402-bazaar-base"
            || manifest.chain != "base"
            || manifest.end_height != manifest.start_height
            || manifest.inputs.len() != 1
        {
            return Err(CollectionError::InvalidInput);
        }
        let input = &manifest.inputs[0];
        if !matches!(input.kind, InputKind::X402Bazaar)
            || input.height != manifest.start_height
            || input.context != CDP_BAZAAR_URL
            || !input.evidence_base64.is_empty()
        {
            return Err(CollectionError::InvalidInput);
        }
        let mut archived_pages = Vec::new();
        let mut archived_bytes = 0_u64;
        let mut offset = 0_u64;
        let mut frozen_total = None;
        loop {
            let page_index =
                u64::try_from(archived_pages.len()).map_err(|_| CollectionError::InvalidInput)?;
            if page_index >= MAX_BAZAAR_PAGES {
                return Err(CollectionError::InvalidInput);
            }
            let evidence_height = manifest
                .start_height
                .checked_add(page_index)
                .ok_or(CollectionError::InvalidInput)?;
            let url = bazaar_url(offset);
            let response = self.public_source_transport.fetch(&url).await?;
            archived_bytes = archived_bytes
                .checked_add(
                    u64::try_from(response.body.len())
                        .map_err(|_| CollectionError::InvalidInput)?,
                )
                .ok_or(CollectionError::InvalidInput)?;
            if archived_bytes > MAX_BAZAAR_CRAWL_BYTES {
                return Err(CollectionError::InvalidInput);
            }
            let provenance = EvidenceProvenance::new(
                "public-http-source-v1",
                &format!("cdp-x402-bazaar:offset:{offset}:page:{page_index}"),
                [
                    ("chain", manifest.chain.clone()),
                    ("height", evidence_height.to_string()),
                    ("input-kind", "x402_bazaar".to_owned()),
                    ("provider", "cdp-bazaar".to_owned()),
                    ("source", manifest.source_id.clone()),
                ],
            )
            .map_err(|_| CollectionError::InvalidInput)?;
            let context = EvidenceContext::new(&manifest.source_id, &manifest.observed_date)
                .map_err(|_| CollectionError::InvalidInput)?
                .with_provenance(provenance);
            let receipt = self
                .evidence_store
                .archive_verified(&context, &response.body)
                .await?;
            let archived = ArchivedEvidence {
                evidence_id: format!("evidence:sha256:{}", receipt.object.sha256()),
                sha256: receipt.object.sha256(),
                storage_uri: receipt.object.name().to_owned(),
                storage_generation: receipt.generation,
                media_type: "application/json".to_owned(),
                byte_length: response.body.len() as u64,
                height: evidence_height,
                verified: true,
                archived_bytes: response.body,
            };
            let page_input = ManifestInput {
                height: evidence_height,
                kind: InputKind::X402Bazaar,
                context: url,
                evidence_base64: String::new(),
            };
            parse_protocol_input(&manifest, &page_input, &archived.archived_bytes)?;
            let page = parse_bazaar_page(&archived.archived_bytes)?;
            archived_pages.push(archived);
            let Some(pagination) = page.pagination else {
                break;
            };
            if pagination.limit != CDP_BAZAAR_PAGE_LIMIT || pagination.offset != offset {
                return Err(CollectionError::InvalidInput);
            }
            let boundary = *frozen_total.get_or_insert(pagination.total);
            if pagination.total < boundary || boundary > MAX_BAZAAR_PAGES * CDP_BAZAAR_PAGE_LIMIT {
                return Err(CollectionError::InvalidInput);
            }
            let item_count =
                u64::try_from(page.items.len()).map_err(|_| CollectionError::InvalidInput)?;
            if item_count > CDP_BAZAAR_PAGE_LIMIT
                || (offset < boundary && item_count == 0)
                || (item_count < CDP_BAZAAR_PAGE_LIMIT
                    && offset.saturating_add(item_count) < boundary)
            {
                return Err(CollectionError::InvalidInput);
            }
            if offset.saturating_add(item_count) >= boundary {
                break;
            }
            offset = offset
                .checked_add(CDP_BAZAAR_PAGE_LIMIT)
                .ok_or(CollectionError::InvalidInput)?;
            tokio::time::sleep(Duration::from_millis(BAZAAR_REQUEST_INTERVAL_MS)).await;
        }
        let end_height = manifest
            .start_height
            .checked_add(
                u64::try_from(archived_pages.len().saturating_sub(1))
                    .map_err(|_| CollectionError::InvalidInput)?,
            )
            .ok_or(CollectionError::InvalidInput)?;
        Ok(CollectionBatch {
            chain: manifest.chain,
            source_id: manifest.source_id,
            observed_at_unix_ms: manifest.observed_at_unix_ms,
            start_height: manifest.start_height,
            end_height,
            evidence: archived_pages,
            observations: Vec::new(),
        })
    }
}

fn bazaar_url(offset: u64) -> String {
    format!("{CDP_BAZAAR_BASE_URL}?limit={CDP_BAZAAR_PAGE_LIMIT}&offset={offset}")
}

fn valid_bazaar_url(url: &str) -> bool {
    let prefix = format!("{CDP_BAZAAR_BASE_URL}?limit={CDP_BAZAAR_PAGE_LIMIT}&offset=");
    let Some(offset) = url.strip_prefix(&prefix) else {
        return false;
    };
    !offset.is_empty()
        && (offset == "0" || !offset.starts_with('0'))
        && offset.bytes().all(|byte| byte.is_ascii_digit())
        && offset
            .parse::<u64>()
            .is_ok_and(|value| value % CDP_BAZAAR_PAGE_LIMIT == 0)
}

fn parse_bazaar_page(bytes: &[u8]) -> Result<BazaarPage, CollectionError> {
    serde_json::from_slice(bytes).map_err(|_| CollectionError::InvalidInput)
}

fn validate_replayed_bazaar_pages(evidence: &[ArchivedEvidence]) -> Result<(), CollectionError> {
    let mut frozen_total = None;
    for (index, archived) in evidence.iter().enumerate() {
        let page = parse_bazaar_page(&archived.archived_bytes)?;
        let Some(pagination) = page.pagination else {
            return if evidence.len() == 1 {
                Ok(())
            } else {
                Err(CollectionError::InvalidInput)
            };
        };
        let expected_offset = u64::try_from(index)
            .map_err(|_| CollectionError::InvalidInput)?
            .checked_mul(CDP_BAZAAR_PAGE_LIMIT)
            .ok_or(CollectionError::InvalidInput)?;
        if pagination.limit != CDP_BAZAAR_PAGE_LIMIT || pagination.offset != expected_offset {
            return Err(CollectionError::InvalidInput);
        }
        let boundary = *frozen_total.get_or_insert(pagination.total);
        let item_count =
            u64::try_from(page.items.len()).map_err(|_| CollectionError::InvalidInput)?;
        if pagination.total < boundary
            || boundary > MAX_BAZAAR_PAGES * CDP_BAZAAR_PAGE_LIMIT
            || item_count > CDP_BAZAAR_PAGE_LIMIT
            || (expected_offset < boundary && item_count == 0)
            || (item_count < CDP_BAZAAR_PAGE_LIMIT
                && expected_offset.saturating_add(item_count) < boundary)
        {
            return Err(CollectionError::InvalidInput);
        }
        let reaches_boundary = expected_offset.saturating_add(item_count) >= boundary;
        if reaches_boundary != (index + 1 == evidence.len()) {
            return Err(CollectionError::InvalidInput);
        }
    }
    Ok(())
}

pub fn verify_replayed_evidence(
    chain: String,
    source_id: String,
    observed_at_unix_ms: i64,
    start_height: u64,
    end_height: u64,
    evidence: Vec<ArchivedEvidence>,
) -> Result<CollectionBatch, CollectionError> {
    verify_evidence(
        chain,
        source_id,
        observed_at_unix_ms,
        start_height,
        end_height,
        evidence,
        false,
    )
}

#[doc(hidden)]
pub fn verify_recovery_rpc_evidence(
    chain: String,
    source_id: String,
    observed_at_unix_ms: i64,
    start_height: u64,
    end_height: u64,
    evidence: Vec<ArchivedEvidence>,
) -> Result<CollectionBatch, CollectionError> {
    verify_evidence(
        chain,
        source_id,
        observed_at_unix_ms,
        start_height,
        end_height,
        evidence,
        true,
    )
}

fn verify_evidence(
    chain: String,
    source_id: String,
    observed_at_unix_ms: i64,
    start_height: u64,
    end_height: u64,
    mut evidence: Vec<ArchivedEvidence>,
    allow_recovery_envelope: bool,
) -> Result<CollectionBatch, CollectionError> {
    evidence.sort_by_key(ArchivedEvidence::height);
    if evidence.len() != end_height.saturating_sub(start_height).saturating_add(1) as usize
        || evidence.iter().enumerate().any(|(index, item)| {
            item.height != start_height.saturating_add(index as u64) || !item.verified
        })
    {
        return Err(CollectionError::InvalidInput);
    }
    let is_bazaar = source_id == "cdp-x402-bazaar-base";
    if is_bazaar {
        validate_replayed_bazaar_pages(&evidence)?;
    }
    let manifest = Manifest {
        schema_version: 1,
        chain: chain.clone(),
        source_id: source_id.clone(),
        observed_at_unix_ms,
        observed_date: "1970-01-01".to_owned(),
        start_height,
        end_height,
        inputs: vec![ManifestInput {
            height: start_height,
            kind: if is_bazaar {
                InputKind::X402Bazaar
            } else {
                InputKind::ChainTransfer
            },
            context: if is_bazaar {
                bazaar_url(0)
            } else {
                "readback".to_owned()
            },
            evidence_base64: String::new(),
        }],
    };
    let mut observations = Vec::new();
    for archived in &evidence {
        let inputs = if archived.media_type == "application/vnd.agent-economy.rpc" {
            let rpc = RpcEvidence::decode(&archived.archived_bytes)
                .map_err(|_| CollectionError::InvalidInput)?;
            if allow_recovery_envelope {
                protocol_inputs_from_recovery_envelope(rpc.body(), archived.height)?
            } else {
                Vec::new()
            }
        } else if archived.media_type == "application/json" && is_bazaar {
            let page_index = archived
                .height
                .checked_sub(start_height)
                .ok_or(CollectionError::InvalidInput)?;
            vec![ManifestInput {
                height: archived.height,
                kind: InputKind::X402Bazaar,
                context: bazaar_url(
                    page_index
                        .checked_mul(CDP_BAZAAR_PAGE_LIMIT)
                        .ok_or(CollectionError::InvalidInput)?,
                ),
                evidence_base64: String::new(),
            }]
        } else {
            return Err(CollectionError::InvalidInput);
        };
        for input in inputs {
            let input_bytes = if input.evidence_base64.is_empty() {
                archived.archived_bytes.clone()
            } else {
                STANDARD
                    .decode(&input.evidence_base64)
                    .map_err(|_| CollectionError::InvalidInput)?
            };
            for observation in parse_protocol_input(&manifest, &input, &input_bytes)? {
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
        chain,
        source_id,
        observed_at_unix_ms,
        start_height,
        end_height,
        evidence,
        observations,
    })
}

fn protocol_inputs_from_recovery_envelope(
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
        InputKind::X402Runtime
        | InputKind::X402WellKnown
        | InputKind::X402OpenApi
        | InputKind::X402Bazaar => {
            let kind = match input.kind {
                InputKind::X402Runtime => EvidenceKind::Runtime402,
                InputKind::X402WellKnown => EvidenceKind::WellKnown,
                InputKind::X402OpenApi => EvidenceKind::OpenApi,
                InputKind::X402Bazaar => EvidenceKind::Bazaar,
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
    let expected_source = if manifest
        .inputs
        .iter()
        .all(|input| matches!(input.kind, InputKind::X402Bazaar))
    {
        "cdp-x402-bazaar-base".to_owned()
    } else {
        format!("alchemy-{}", manifest.chain)
    };
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
