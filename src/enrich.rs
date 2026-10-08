use std::sync::Arc;

use agent_economy_evidence_store::EvidenceContext;
use agent_economy_rpc_collector::{
    AlchemyTransport, BuyerHistoryTarget, Chain, EnrichmentMode, EnrichmentRequest,
    HistoryFinality, HistoryPage, HistoryTransport, RawRpcResponse,
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::{
    collect::CollectionEvidenceStore,
    worker::{
        HandlerFailure, JobResult, LeasedJob, WorkerHandler, WorkerJobStore, WorkerMode,
        WorkerStoreError,
    },
};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrichmentWorkerError {
    InvalidInput,
    Unavailable,
    Conflict,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArchivedHistoryEvidence {
    object_name: String,
    sha256: String,
    byte_length: usize,
    storage_generation: Option<String>,
    body: Vec<u8>,
}

impl ArchivedHistoryEvidence {
    pub fn new(object_name: String, body: Vec<u8>) -> Self {
        let sha256 = format!("{:x}", Sha256::digest(&body));
        let byte_length = body.len();
        Self {
            object_name,
            sha256,
            byte_length,
            storage_generation: None,
            body,
        }
    }

    pub fn object_name(&self) -> &str {
        &self.object_name
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub const fn byte_length(&self) -> usize {
        self.byte_length
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    fn with_storage_generation(mut self, generation: Option<String>) -> Self {
        self.storage_generation = generation;
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichedHistoryRecord {
    transaction_reference: String,
    block_reference: String,
    evidence_id: String,
}

impl EnrichedHistoryRecord {
    pub fn transaction_reference(&self) -> &str {
        &self.transaction_reference
    }

    pub fn block_reference(&self) -> &str {
        &self.block_reference
    }

    pub fn evidence_id(&self) -> &str {
        &self.evidence_id
    }
}

#[derive(Clone, Debug)]
pub struct EnrichmentBatch {
    namespace_id: String,
    buyer_handle_id: String,
    chain_scope: String,
    handle_value: String,
    observed_date: String,
    cursor_version: u64,
    start_cursor: Option<String>,
    next_cursor: Option<String>,
    requests_used: u64,
    complete: bool,
    evidence: Vec<ArchivedHistoryEvidence>,
    records: Vec<EnrichedHistoryRecord>,
    classification_labels: Value,
    output_sha256: String,
}

impl EnrichmentBatch {
    pub const fn requests_used(&self) -> u64 {
        self.requests_used
    }

    pub fn evidence(&self) -> &[ArchivedHistoryEvidence] {
        &self.evidence
    }

    pub fn records(&self) -> &[EnrichedHistoryRecord] {
        &self.records
    }

    pub const fn complete(&self) -> bool {
        self.complete
    }

    pub fn output_sha256(&self) -> &str {
        &self.output_sha256
    }
}

#[async_trait]
pub trait EnrichmentTransport: Send + 'static {
    async fn fetch(
        &mut self,
        target: &BuyerHistoryTarget,
        cursor: Option<&str>,
    ) -> Result<RawRpcResponse, EnrichmentWorkerError>;
}

#[async_trait]
impl EnrichmentTransport for AlchemyTransport {
    async fn fetch(
        &mut self,
        target: &BuyerHistoryTarget,
        cursor: Option<&str>,
    ) -> Result<RawRpcResponse, EnrichmentWorkerError> {
        HistoryTransport::fetch_history(self, target, cursor)
            .await
            .map_err(|_| EnrichmentWorkerError::Unavailable)
    }
}

#[async_trait]
pub trait EnrichmentEvidenceArchive: Send + Sync + 'static {
    async fn archive(
        &self,
        observed_date: &str,
        body: &[u8],
    ) -> Result<ArchivedHistoryEvidence, EnrichmentWorkerError>;
}

pub struct EvidenceStoreHistoryArchive<S: ?Sized> {
    store: Arc<S>,
}

impl<S: ?Sized> EvidenceStoreHistoryArchive<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> EnrichmentEvidenceArchive for EvidenceStoreHistoryArchive<S>
where
    S: CollectionEvidenceStore + ?Sized,
{
    async fn archive(
        &self,
        observed_date: &str,
        body: &[u8],
    ) -> Result<ArchivedHistoryEvidence, EnrichmentWorkerError> {
        let context = EvidenceContext::new("alchemy-history", observed_date)
            .map_err(|_| EnrichmentWorkerError::InvalidInput)?;
        let receipt = self
            .store
            .archive_verified(&context, body)
            .await
            .map_err(|_| EnrichmentWorkerError::Unavailable)?;
        Ok(
            ArchivedHistoryEvidence::new(receipt.object.name().to_owned(), body.to_vec())
                .with_storage_generation(receipt.generation),
        )
    }
}

#[async_trait]
pub trait EnrichmentJobStore: Send + Sync + 'static {
    async fn load_input(&self, job: &LeasedJob) -> Result<Vec<u8>, EnrichmentWorkerError>;
    async fn commit_batch(
        &self,
        job: &LeasedJob,
        batch: &EnrichmentBatch,
    ) -> Result<bool, EnrichmentWorkerError>;
}

pub struct PostgresEnrichmentStore {
    client: Arc<Mutex<Client>>,
}

impl PostgresEnrichmentStore {
    pub fn new(client: Client) -> Self {
        Self::from_shared(Arc::new(Mutex::new(client)))
    }

    pub fn from_shared(client: Arc<Mutex<Client>>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl EnrichmentJobStore for PostgresEnrichmentStore {
    async fn load_input(&self, job: &LeasedJob) -> Result<Vec<u8>, EnrichmentWorkerError> {
        let client = self.client.lock().await;
        client
            .query_opt(
                "SELECT agent_economy.load_enrichment_job_input(\
                    $1::text::uuid, $2, $3::text::uuid)",
                &[&job.job_id, &job.lease_owner, &job.lease_token],
            )
            .await
            .map_err(|_| EnrichmentWorkerError::Unavailable)?
            .map(|row| row.get::<_, Vec<u8>>(0))
            .ok_or(EnrichmentWorkerError::Conflict)
    }

    async fn commit_batch(
        &self,
        job: &LeasedJob,
        batch: &EnrichmentBatch,
    ) -> Result<bool, EnrichmentWorkerError> {
        let cursor_version =
            i64::try_from(batch.cursor_version).map_err(|_| EnrichmentWorkerError::InvalidInput)?;
        let requests_used =
            i64::try_from(batch.requests_used).map_err(|_| EnrichmentWorkerError::InvalidInput)?;
        let evidence_json = Value::Array(
            batch
                .evidence
                .iter()
                .map(|item| {
                    json!({
                        "byte_length": item.byte_length,
                        "object_name": item.object_name,
                        "sha256": item.sha256,
                        "storage_generation": item.storage_generation,
                    })
                })
                .collect(),
        )
        .to_string();
        let records_json = Value::Array(
            batch
                .records
                .iter()
                .map(|item| {
                    json!({
                        "block_reference": item.block_reference,
                        "evidence_id": item.evidence_id,
                        "transaction_reference": item.transaction_reference,
                    })
                })
                .collect(),
        )
        .to_string();
        let labels_json = batch.classification_labels.to_string();
        let client = self.client.lock().await;
        client
            .query_one(
                "SELECT agent_economy.stage_enrichment_batch(\
                    $1::text::uuid, $2, $3::text::uuid, $4, $5, $6::text::uuid, \
                    $7, $8, $9, $10::text::date, $11::bigint, $12, $13, $14::bigint, \
                    $15, $16::text::jsonb, $17::text::jsonb, $18::text::jsonb)",
                &[
                    &job.job_id,
                    &job.lease_owner,
                    &job.lease_token,
                    &job.input_sha256,
                    &batch.output_sha256,
                    &batch.namespace_id,
                    &batch.buyer_handle_id,
                    &batch.chain_scope,
                    &batch.handle_value,
                    &batch.observed_date,
                    &cursor_version,
                    &batch.start_cursor,
                    &batch.next_cursor,
                    &requests_used,
                    &batch.complete,
                    &evidence_json,
                    &records_json,
                    &labels_json,
                ],
            )
            .await
            .map_err(|_| EnrichmentWorkerError::Unavailable)
            .map(|row| row.get(0))
    }
}

#[async_trait]
impl WorkerJobStore for PostgresEnrichmentStore {
    async fn claim(
        &self,
        mode: WorkerMode,
        lease_owner: &str,
        lease_seconds: u64,
    ) -> Result<Option<LeasedJob>, WorkerStoreError> {
        if mode != WorkerMode::Enrich || !(1..=3_600).contains(&lease_seconds) {
            return Err(WorkerStoreError::Invalid);
        }
        let seconds = i64::try_from(lease_seconds).map_err(|_| WorkerStoreError::Invalid)?;
        let client = self.client.lock().await;
        client
            .query_opt(
                "SELECT job_id::text, mode, job_kind, input_sha256, attempt_count, \
                        lease_owner, lease_token::text \
                 FROM agent_economy.claim_enrichment_job($1, $2::bigint)",
                &[&lease_owner, &seconds],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?
            .map(|row| {
                let returned_mode: String = row.get(1);
                let job_kind: String = row.get(2);
                if returned_mode != "enrich" || job_kind != "buyer-public-history-v1" {
                    return Err(WorkerStoreError::Invalid);
                }
                Ok(LeasedJob {
                    job_id: row.get(0),
                    mode: WorkerMode::Enrich,
                    job_kind,
                    input_sha256: row.get(3),
                    attempt: u16::try_from(row.get::<_, i16>(4))
                        .map_err(|_| WorkerStoreError::Invalid)?,
                    lease_owner: row.get(5),
                    lease_token: row.get(6),
                    collection_admission: None,
                })
            })
            .transpose()
    }

    async fn renew(&self, job: &LeasedJob, lease_seconds: u64) -> Result<(), WorkerStoreError> {
        let seconds = i64::try_from(lease_seconds).map_err(|_| WorkerStoreError::Invalid)?;
        self.mutate(
            "SELECT agent_economy.renew_enrichment_job_lease(\
                $1::text::uuid, $2, $3::text::uuid, $4::bigint)",
            &[&job.job_id, &job.lease_owner, &job.lease_token, &seconds],
        )
        .await
    }

    async fn complete(&self, job: &LeasedJob, output_sha256: &str) -> Result<(), WorkerStoreError> {
        self.mutate(
            "SELECT agent_economy.complete_enrichment_job(\
                $1::text::uuid, $2, $3::text::uuid, $4)",
            &[
                &job.job_id,
                &job.lease_owner,
                &job.lease_token,
                &output_sha256,
            ],
        )
        .await
    }

    async fn fail(
        &self,
        job: &LeasedJob,
        error_code: &str,
        retryable: bool,
    ) -> Result<(), WorkerStoreError> {
        self.mutate(
            "SELECT agent_economy.fail_enrichment_job(\
                $1::text::uuid, $2, $3::text::uuid, $4, $5, 30::bigint)",
            &[
                &job.job_id,
                &job.lease_owner,
                &job.lease_token,
                &error_code,
                &retryable,
            ],
        )
        .await
    }
}

impl PostgresEnrichmentStore {
    async fn mutate(
        &self,
        statement: &str,
        parameters: &[&(dyn tokio_postgres::types::ToSql + Sync)],
    ) -> Result<(), WorkerStoreError> {
        let changed = self
            .client
            .lock()
            .await
            .query_one(statement, parameters)
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?
            .get::<_, bool>(0);
        if changed {
            Ok(())
        } else {
            Err(WorkerStoreError::Conflict)
        }
    }
}

pub struct EnrichmentHandler<T, A: ?Sized, S: ?Sized> {
    transport: Mutex<T>,
    archive: Arc<A>,
    store: Arc<S>,
}

impl<T, A: ?Sized, S: ?Sized> EnrichmentHandler<T, A, S> {
    pub fn new(transport: T, archive: Arc<A>, store: Arc<S>) -> Self {
        Self {
            transport: Mutex::new(transport),
            archive,
            store,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnrichmentManifest {
    schema_version: u32,
    namespace_id: String,
    buyer_handle_id: String,
    chain_scope: String,
    handle_value: String,
    enrichment_mode: String,
    max_pages: u64,
    request_budget: u64,
    observed_date: String,
    cursor_version: u64,
    start_cursor: Option<String>,
    classification_labels: Value,
}

#[async_trait]
impl<T, A, S> WorkerHandler for EnrichmentHandler<T, A, S>
where
    T: EnrichmentTransport,
    A: EnrichmentEvidenceArchive + ?Sized,
    S: EnrichmentJobStore + ?Sized,
{
    async fn process(&self, job: &LeasedJob) -> Result<JobResult, HandlerFailure> {
        if job.mode != WorkerMode::Enrich || job.job_kind != "buyer-public-history-v1" {
            return Err(HandlerFailure::poison("unexpected_job_kind").unwrap());
        }
        let bytes = self.store.load_input(job).await.map_err(handler_failure)?;
        if format!("{:x}", Sha256::digest(&bytes)) != job.input_sha256 {
            return Err(HandlerFailure::poison("invalid_enrichment_input").unwrap());
        }
        let manifest: EnrichmentManifest = serde_json::from_slice(&bytes)
            .map_err(|_| HandlerFailure::poison("invalid_enrichment_input").unwrap())?;
        let (target, mode) = validate_manifest(&manifest)
            .map_err(|_| HandlerFailure::poison("invalid_enrichment_input").unwrap())?;
        EnrichmentRequest::try_new(
            target.clone(),
            mode,
            manifest.max_pages,
            manifest.request_budget,
            &manifest.observed_date,
        )
        .map_err(|_| HandlerFailure::poison("invalid_enrichment_input").unwrap())?;

        let mut cursor = manifest.start_cursor.clone();
        let mut evidence = Vec::new();
        let mut records = Vec::new();
        let limit = manifest.max_pages.min(manifest.request_budget);
        let mut transport = self.transport.lock().await;
        for _ in 0..limit {
            let response = transport
                .fetch(&target, cursor.as_deref())
                .await
                .map_err(handler_failure)?;
            let archived = self
                .archive
                .archive(&manifest.observed_date, response.bytes())
                .await
                .map_err(handler_failure)?;
            evidence.push(archived.clone());
            if response.status() != 200 {
                return Err(HandlerFailure::retryable("enrichment_provider_unavailable").unwrap());
            }
            let page = HistoryPage::decode_alchemy(&target, response.bytes())
                .map_err(|_| HandlerFailure::poison("invalid_enrichment_response").unwrap())?;
            records.extend(
                page.items()
                    .iter()
                    .filter(|item| item.finality() == HistoryFinality::Finalized)
                    .map(|item| EnrichedHistoryRecord {
                        transaction_reference: item.transaction_reference().to_owned(),
                        block_reference: item.block_reference().to_owned(),
                        evidence_id: archived.object_name().to_owned(),
                    }),
            );
            cursor = page.next_cursor().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
        }
        let requests_used = u64::try_from(evidence.len())
            .map_err(|_| HandlerFailure::poison("invalid_enrichment_output").unwrap())?;
        let complete = cursor.is_none();
        let output_document = json!({
            "buyer_handle_id": manifest.buyer_handle_id,
            "chain_scope": manifest.chain_scope,
            "complete": complete,
            "cursor_version": manifest.cursor_version,
            "evidence": evidence.iter().map(|item| json!({
                "byte_length": item.byte_length,
                "object_name": item.object_name,
                "sha256": item.sha256,
            })).collect::<Vec<_>>(),
            "next_cursor": cursor,
            "records": records.iter().map(|item| json!({
                "block_reference": item.block_reference,
                "evidence_id": item.evidence_id,
                "transaction_reference": item.transaction_reference,
            })).collect::<Vec<_>>(),
            "requests_used": requests_used,
        });
        let output_sha256 = format!(
            "{:x}",
            Sha256::digest(output_document.to_string().as_bytes())
        );
        let batch = EnrichmentBatch {
            namespace_id: manifest.namespace_id,
            buyer_handle_id: manifest.buyer_handle_id,
            chain_scope: manifest.chain_scope,
            handle_value: manifest.handle_value,
            observed_date: manifest.observed_date,
            cursor_version: manifest.cursor_version,
            start_cursor: manifest.start_cursor,
            next_cursor: cursor,
            requests_used,
            complete,
            evidence,
            records,
            classification_labels: manifest.classification_labels,
            output_sha256,
        };
        if !self
            .store
            .commit_batch(job, &batch)
            .await
            .map_err(handler_failure)?
        {
            return Err(HandlerFailure::retryable("enrichment_commit_conflict").unwrap());
        }
        JobResult::new(batch.output_sha256())
    }
}

fn validate_manifest(
    manifest: &EnrichmentManifest,
) -> Result<(BuyerHistoryTarget, EnrichmentMode), EnrichmentWorkerError> {
    if manifest.schema_version != 1
        || manifest.namespace_id.is_empty()
        || manifest
            .classification_labels
            .as_array()
            .is_none_or(Vec::is_empty)
    {
        return Err(EnrichmentWorkerError::InvalidInput);
    }
    let chain = match manifest.chain_scope.as_str() {
        "ethereum" => Chain::Ethereum,
        "base" => Chain::Base,
        "solana" => Chain::Solana,
        "tempo" => Chain::Tempo,
        _ => return Err(EnrichmentWorkerError::InvalidInput),
    };
    let mode = match manifest.enrichment_mode.as_str() {
        "automatic" => EnrichmentMode::Automatic,
        "manual_deep_scan" => EnrichmentMode::ManualDeepScan,
        _ => return Err(EnrichmentWorkerError::InvalidInput),
    };
    let target = BuyerHistoryTarget::try_new(
        &manifest.namespace_id,
        &manifest.buyer_handle_id,
        chain,
        &manifest.handle_value,
    )
    .map_err(|_| EnrichmentWorkerError::InvalidInput)?;
    Ok((target, mode))
}

fn handler_failure(error: EnrichmentWorkerError) -> HandlerFailure {
    match error {
        EnrichmentWorkerError::InvalidInput => {
            HandlerFailure::poison("invalid_enrichment_input").unwrap()
        }
        EnrichmentWorkerError::Unavailable => {
            HandlerFailure::retryable("enrichment_dependency_unavailable").unwrap()
        }
        EnrichmentWorkerError::Conflict => {
            HandlerFailure::retryable("enrichment_commit_conflict").unwrap()
        }
    }
}
