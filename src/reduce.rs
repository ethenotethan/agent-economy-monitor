use std::{collections::BTreeMap, fmt, sync::Arc};

use agent_economy_attribution::{
    AttributionEngine, AttributionLevel, AttributionMethod, PaymentRequirement, Settlement,
    SettlementFinality,
};
use agent_economy_contracts::Observation;
use agent_economy_reducer::{
    EvmFinalityEvidence, ExecutionOutcome, FinalityBasis, FinalityEngine, FinalityStatus,
    FinalitySubject, ObservationRole, Reducer, SolanaCommitment, SolanaFinalityEvidence,
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::worker::{
    HandlerFailure, JobResult, LeasedJob, WorkerHandler, WorkerJobStore, WorkerMode,
    WorkerStoreError,
};

const MAX_REDUCTION_INPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_OBSERVATIONS: usize = 50_000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReductionError {
    InvalidInput,
    InputUnavailable,
    CommitUnavailable,
}

impl fmt::Display for ReductionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidInput => "invalid reduction input",
            Self::InputUnavailable => "reduction input unavailable",
            Self::CommitUnavailable => "reduction commit unavailable",
        })
    }
}

impl std::error::Error for ReductionError {}

#[derive(Clone, Debug)]
pub struct ReducedObservationLink {
    source_id: String,
    observation_id: String,
    role: ObservationRole,
}

impl ReducedObservationLink {
    pub fn source_id(&self) -> &str {
        &self.source_id
    }

    pub fn observation_id(&self) -> &str {
        &self.observation_id
    }

    pub const fn role(&self) -> ObservationRole {
        self.role
    }
}

#[derive(Clone, Debug)]
pub struct PersistedReducedEvent {
    protocol: String,
    canonical_event_id: String,
    event_at_unix_ms: i64,
    observation_links: Vec<ReducedObservationLink>,
}

#[derive(Clone, Debug)]
pub struct PersistedFinalityUpdate {
    canonical_event_id: String,
    transaction_id: String,
    source_id: String,
    provenance_id: String,
    evidence_id: String,
    asserted_at_unix_ms: i64,
    status: &'static str,
    position: u64,
    block_hash: String,
    basis_json: serde_json::Value,
    timeline_state_hash: String,
    accepted: bool,
    current_status: &'static str,
    current: bool,
}

#[derive(Clone, Debug)]
pub struct PersistedAttribution {
    settlement_id: String,
    canonical_event_id: String,
    transaction_id: String,
    settled_at_unix_ms: i64,
    source_id: String,
    provenance_id: String,
    protocol: String,
    asset: String,
    amount_atomic: String,
    level: &'static str,
    method: &'static str,
    explicit_requirement_id: Option<String>,
    engine_version: String,
    evidence_ids: Vec<String>,
    input_snapshot_hash: String,
    state_hash: String,
    result_encoded_base64: String,
    requirements: Vec<PersistedRequirement>,
    candidates: Vec<PersistedAttributionCandidate>,
}

impl PersistedAttribution {
    pub fn candidate_count(&self) -> usize {
        self.candidates.len()
    }
}

#[derive(Clone, Debug)]
struct PersistedAttributionCandidate {
    attribution_id: String,
    requirement_id: String,
    payment_option_id: String,
    endpoint_id: String,
    service_id: String,
    confidence_bps: u16,
    evidence_ids: Vec<String>,
    requirement_evidence_id: String,
}

#[derive(Clone, Debug)]
struct PersistedRequirement {
    requirement_id: String,
    payment_option_id: String,
    endpoint_id: String,
    service_id: String,
    evidence_ids: Vec<String>,
}

impl PersistedReducedEvent {
    pub fn protocol(&self) -> &str {
        &self.protocol
    }

    pub fn canonical_event_id(&self) -> &str {
        &self.canonical_event_id
    }

    pub const fn event_at_unix_ms(&self) -> i64 {
        self.event_at_unix_ms
    }

    pub fn supporting_observation_ids(&self) -> Vec<&str> {
        self.observation_links
            .iter()
            .filter(|link| link.role == ObservationRole::Supporting)
            .map(|link| link.observation_id.as_str())
            .collect()
    }

    pub fn conflicting_observation_ids(&self) -> Vec<&str> {
        self.observation_links
            .iter()
            .filter(|link| link.role == ObservationRole::Conflicting)
            .map(|link| link.observation_id.as_str())
            .collect()
    }

    pub fn observation_links(&self) -> &[ReducedObservationLink] {
        &self.observation_links
    }
}

#[derive(Clone, Debug)]
pub struct ReductionBatch {
    chain_scope: String,
    start_height: u64,
    end_height: u64,
    reducer_version: String,
    state_hash: String,
    events: Vec<PersistedReducedEvent>,
    finality_updates: Vec<PersistedFinalityUpdate>,
    attributions: Vec<PersistedAttribution>,
    output_sha256: String,
}

impl ReductionBatch {
    pub fn chain_scope(&self) -> &str {
        &self.chain_scope
    }

    pub const fn start_height(&self) -> u64 {
        self.start_height
    }

    pub const fn end_height(&self) -> u64 {
        self.end_height
    }

    pub fn reducer_version(&self) -> &str {
        &self.reducer_version
    }

    pub fn state_hash(&self) -> &str {
        &self.state_hash
    }

    pub fn events(&self) -> &[PersistedReducedEvent] {
        &self.events
    }

    pub fn finality_updates(&self) -> &[PersistedFinalityUpdate] {
        &self.finality_updates
    }

    pub fn attributions(&self) -> &[PersistedAttribution] {
        &self.attributions
    }

    pub fn output_sha256(&self) -> &str {
        &self.output_sha256
    }

    pub fn events_json(&self) -> serde_json::Value {
        serde_json::Value::Array(
            self.events
                .iter()
                .map(|event| {
                    json!({
                        "protocol": event.protocol,
                        "canonical_event_id": event.canonical_event_id,
                        "event_at_unix_ms": event.event_at_unix_ms,
                        "observation_links": event.observation_links.iter().map(|link| json!({
                            "source_id": link.source_id,
                            "observation_id": link.observation_id,
                            "support_role": match link.role {
                                ObservationRole::Supporting => "supporting",
                                ObservationRole::Conflicting => "conflicting",
                            }
                        })).collect::<Vec<_>>()
                    })
                })
                .collect(),
        )
    }

    pub fn finality_json(&self) -> serde_json::Value {
        json!(
            self.finality_updates
                .iter()
                .map(|update| json!({
                    "canonical_event_id": update.canonical_event_id,
                    "transaction_id": update.transaction_id,
                    "source_id": update.source_id,
                    "provenance_id": update.provenance_id,
                    "evidence_id": update.evidence_id,
                    "asserted_at_unix_ms": update.asserted_at_unix_ms,
                    "status": update.status,
                    "position": update.position,
                    "block_hash": update.block_hash,
                    "basis": update.basis_json,
                    "timeline_state_hash": update.timeline_state_hash,
                    "accepted": update.accepted,
                    "current_status": update.current_status,
                    "current": update.current,
                }))
                .collect::<Vec<_>>()
        )
    }

    pub fn attributions_json(&self) -> serde_json::Value {
        json!(
            self.attributions
                .iter()
                .map(|attribution| json!({
                    "settlement_id": attribution.settlement_id,
                    "canonical_event_id": attribution.canonical_event_id,
                    "transaction_id": attribution.transaction_id,
                    "settled_at_unix_ms": attribution.settled_at_unix_ms,
                    "source_id": attribution.source_id,
                    "provenance_id": attribution.provenance_id,
                    "protocol": attribution.protocol,
                    "asset": attribution.asset,
                    "amount_atomic": attribution.amount_atomic,
                    "level": attribution.level,
                    "method": attribution.method,
                    "explicit_requirement_id": attribution.explicit_requirement_id,
                    "engine_version": attribution.engine_version,
                    "evidence_ids": attribution.evidence_ids,
                    "input_snapshot_hash": attribution.input_snapshot_hash,
                    "state_hash": attribution.state_hash,
                    "result_encoded_base64": attribution.result_encoded_base64,
                    "requirements": attribution.requirements.iter().map(|requirement| json!({
                        "requirement_id": requirement.requirement_id,
                        "payment_option_id": requirement.payment_option_id,
                        "endpoint_id": requirement.endpoint_id,
                        "service_id": requirement.service_id,
                        "evidence_ids": requirement.evidence_ids,
                    })).collect::<Vec<_>>(),
                    "candidates": attribution.candidates.iter().map(|candidate| json!({
                        "attribution_id": candidate.attribution_id,
                        "requirement_id": candidate.requirement_id,
                        "payment_option_id": candidate.payment_option_id,
                        "endpoint_id": candidate.endpoint_id,
                        "service_id": candidate.service_id,
                        "confidence_bps": candidate.confidence_bps,
                        "evidence_ids": candidate.evidence_ids,
                        "requirement_evidence_id": candidate.requirement_evidence_id,
                    })).collect::<Vec<_>>()
                }))
                .collect::<Vec<_>>()
        )
    }
}

#[async_trait]
pub trait ReductionInputStore: Send + Sync + 'static {
    async fn load(&self, job: &LeasedJob) -> Result<Vec<u8>, ReductionError>;
}

#[async_trait]
pub trait ReductionCommitStore: Send + Sync + 'static {
    async fn commit(&self, job: &LeasedJob, batch: ReductionBatch) -> Result<(), ReductionError>;
}

pub struct ReductionHandler<I: ?Sized, C: ?Sized> {
    inputs: Arc<I>,
    commits: Arc<C>,
}

impl<I: ?Sized, C: ?Sized> ReductionHandler<I, C> {
    pub fn new(inputs: Arc<I>, commits: Arc<C>) -> Self {
        Self { inputs, commits }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReductionManifest {
    schema_version: u32,
    reducer_version: String,
    #[serde(default = "default_attribution_version")]
    attribution_version: String,
    chain_scope: String,
    start_height: u64,
    end_height: u64,
    observations: Vec<ManifestObservation>,
    finality: Vec<ManifestFinality>,
    settlements: Vec<ManifestSettlementAttribution>,
}

fn default_attribution_version() -> String {
    "attribution@1".to_owned()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestObservation {
    source_id: String,
    observation_id: String,
    height: u64,
    encoded_base64: String,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ManifestFinality {
    Evm {
        canonical_event_id: String,
        transaction_id: String,
        source_id: String,
        provenance_id: String,
        evidence_id: String,
        asserted_at_unix_ms: i64,
        block_number: u64,
        block_hash: String,
        canonical_block_hash: String,
        latest_block: u64,
        finalized_block: u64,
        execution: ManifestExecution,
        confirmations: u64,
    },
    Solana {
        canonical_event_id: String,
        transaction_id: String,
        source_id: String,
        provenance_id: String,
        evidence_id: String,
        asserted_at_unix_ms: i64,
        slot: u64,
        block_hash: String,
        canonical_block_hash: String,
        commitment: ManifestCommitment,
        execution: ManifestExecution,
    },
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManifestExecution {
    Succeeded,
    Reverted,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManifestCommitment {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSettlementAttribution {
    canonical_event_id: String,
    transaction_id: String,
    settled_at_unix_ms: i64,
    source_id: String,
    provenance_id: String,
    settlement: ManifestSettlement,
    requirements: Vec<ManifestRequirement>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestSettlement {
    id: String,
    protocol: String,
    network: String,
    asset: String,
    amount_atomic: String,
    pay_to: String,
    finality: ManifestSettlementFinality,
    requirement_id: Option<String>,
    evidence_ids: Vec<String>,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ManifestSettlementFinality {
    Confirmed,
    Finalized,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestRequirement {
    id: String,
    payment_option_id: String,
    endpoint_id: String,
    service_id: String,
    protocol: String,
    network: String,
    asset: String,
    amount_atomic: String,
    pay_to: String,
    evidence_ids: Vec<String>,
}

#[async_trait]
impl<I, C> WorkerHandler for ReductionHandler<I, C>
where
    I: ReductionInputStore + ?Sized,
    C: ReductionCommitStore + ?Sized,
{
    async fn process(&self, job: &LeasedJob) -> Result<JobResult, HandlerFailure> {
        if job.mode != WorkerMode::Reduce || job.job_kind != "canonical-observation-range-v1" {
            return Err(poison("invalid_reduce_job"));
        }
        let bytes = self.inputs.load(job).await.map_err(|error| match error {
            ReductionError::InputUnavailable => retryable("reduce_input_unavailable"),
            ReductionError::InvalidInput | ReductionError::CommitUnavailable => {
                poison("invalid_reduce_input")
            }
        })?;
        let batch = build_batch(job, &bytes).map_err(|_| poison("invalid_reduce_input"))?;
        let output_sha256 = batch.output_sha256.clone();
        self.commits
            .commit(job, batch)
            .await
            .map_err(|error| match error {
                ReductionError::CommitUnavailable => retryable("reduce_commit_unavailable"),
                ReductionError::InvalidInput | ReductionError::InputUnavailable => {
                    poison("invalid_reduce_commit")
                }
            })?;
        JobResult::new(&output_sha256).map_err(|_| poison("invalid_reduce_output"))
    }
}

pub struct PostgresReductionStore {
    client: Arc<Mutex<Client>>,
}

impl PostgresReductionStore {
    pub fn new(client: Client) -> Self {
        Self::from_shared(Arc::new(Mutex::new(client)))
    }

    pub fn from_shared(client: Arc<Mutex<Client>>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl WorkerJobStore for PostgresReductionStore {
    async fn claim(
        &self,
        mode: WorkerMode,
        lease_owner: &str,
        lease_seconds: u64,
    ) -> Result<Option<LeasedJob>, WorkerStoreError> {
        if mode != WorkerMode::Reduce
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
                        lease_owner, lease_token::text \
                 FROM agent_economy.claim_reducer_job($1, $2::bigint)",
                &[&lease_owner, &lease_seconds],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?;
        row.map(reduction_row_to_job).transpose()
    }

    async fn renew(&self, job: &LeasedJob, lease_seconds: u64) -> Result<(), WorkerStoreError> {
        if job.mode != WorkerMode::Reduce || !(1..=3_600).contains(&lease_seconds) {
            return Err(WorkerStoreError::Invalid);
        }
        let lease_seconds = i64::try_from(lease_seconds).map_err(|_| WorkerStoreError::Invalid)?;
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.renew_reducer_job_lease(\
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
        if job.mode != WorkerMode::Reduce || !valid_sha256(output_sha256) {
            return Err(WorkerStoreError::Invalid);
        }
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.complete_reducer_job(\
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
        if job.mode != WorkerMode::Reduce || !valid_error_code(error_code) {
            return Err(WorkerStoreError::Invalid);
        }
        let retry_delay_seconds = 30_i64;
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.fail_reducer_job(\
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

#[async_trait]
impl ReductionInputStore for PostgresReductionStore {
    async fn load(&self, job: &LeasedJob) -> Result<Vec<u8>, ReductionError> {
        if job.mode != WorkerMode::Reduce {
            return Err(ReductionError::InvalidInput);
        }
        let client = self.client.lock().await;
        client
            .query_opt(
                "SELECT agent_economy.load_reducer_job_input(\
                    $1::text::uuid, $2, $3::text::uuid)",
                &[&job.job_id, &job.lease_owner, &job.lease_token],
            )
            .await
            .map_err(|_| ReductionError::InputUnavailable)?
            .and_then(|row| row.get::<_, Option<Vec<u8>>>(0))
            .ok_or(ReductionError::InvalidInput)
    }
}

#[async_trait]
impl ReductionCommitStore for PostgresReductionStore {
    async fn commit(&self, job: &LeasedJob, batch: ReductionBatch) -> Result<(), ReductionError> {
        let start_height =
            i64::try_from(batch.start_height).map_err(|_| ReductionError::InvalidInput)?;
        let end_height =
            i64::try_from(batch.end_height).map_err(|_| ReductionError::InvalidInput)?;
        let events_json = batch.events_json().to_string();
        let finality_json = batch.finality_json().to_string();
        let attributions_json = batch.attributions_json().to_string();
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.commit_reduction_batch(\
                    $1::text::uuid, $2, $3::text::uuid, $4, $5, $6, $7, \
                    $8::bigint, $9::bigint, $10::text::jsonb, \
                    $11::text::jsonb, $12::text::jsonb)",
                &[
                    &job.job_id,
                    &job.lease_owner,
                    &job.lease_token,
                    &job.input_sha256,
                    &batch.output_sha256,
                    &batch.reducer_version,
                    &batch.chain_scope,
                    &start_height,
                    &end_height,
                    &events_json,
                    &finality_json,
                    &attributions_json,
                ],
            )
            .await
            .map_err(|_| ReductionError::CommitUnavailable)?
            .get::<_, bool>(0);
        changed
            .then_some(())
            .ok_or(ReductionError::CommitUnavailable)
    }
}

fn reduction_row_to_job(row: tokio_postgres::Row) -> Result<LeasedJob, WorkerStoreError> {
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
        collection_admission: None,
    })
}

fn valid_lease_owner(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

fn valid_error_code(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn build_batch(job: &LeasedJob, bytes: &[u8]) -> Result<ReductionBatch, ReductionError> {
    if bytes.is_empty()
        || bytes.len() > MAX_REDUCTION_INPUT_BYTES
        || format!("{:x}", Sha256::digest(bytes)) != job.input_sha256
    {
        return Err(ReductionError::InvalidInput);
    }
    let manifest: ReductionManifest =
        serde_json::from_slice(bytes).map_err(|_| ReductionError::InvalidInput)?;
    if manifest.schema_version != 1
        || manifest.reducer_version.trim().is_empty()
        || !matches!(
            manifest.chain_scope.as_str(),
            "ethereum" | "base" | "solana" | "tempo"
        )
        || manifest.end_height < manifest.start_height
        || manifest.observations.is_empty()
        || manifest.observations.len() > MAX_OBSERVATIONS
        || manifest.attribution_version != "attribution@1"
        || manifest.finality.len() > MAX_OBSERVATIONS
        || manifest.settlements.len() > MAX_OBSERVATIONS
    {
        return Err(ReductionError::InvalidInput);
    }

    let mut metadata = BTreeMap::new();
    let mut observations = Vec::with_capacity(manifest.observations.len());
    for item in manifest.observations {
        if item.source_id.trim().is_empty()
            || item.height < manifest.start_height
            || item.height > manifest.end_height
            || metadata.contains_key(&item.observation_id)
        {
            return Err(ReductionError::InvalidInput);
        }
        let encoded = STANDARD
            .decode(&item.encoded_base64)
            .map_err(|_| ReductionError::InvalidInput)?;
        let observation =
            Observation::decode(&encoded).map_err(|_| ReductionError::InvalidInput)?;
        let provenance = observation.provenance();
        if observation.id() != item.observation_id
            || provenance.source_id() != item.source_id
            || !matches!(
                provenance.parser_version(),
                "x402-adapter@1" | "mpp-adapter@1"
            )
        {
            return Err(ReductionError::InvalidInput);
        }
        metadata.insert(
            item.observation_id,
            (item.source_id, provenance.observed_at_unix_ms()),
        );
        observations.push(observation);
    }

    let snapshot = Reducer::new(&manifest.reducer_version)
        .reduce(observations)
        .map_err(|_| ReductionError::InvalidInput)?;
    let mut events = Vec::with_capacity(snapshot.events().len());
    for event in snapshot.events() {
        let protocol = event
            .id()
            .strip_prefix("event:")
            .and_then(|value| value.split_once(':'))
            .map(|(protocol, _)| protocol)
            .filter(|protocol| matches!(*protocol, "x402" | "mpp"))
            .ok_or(ReductionError::InvalidInput)?;
        let mut event_at_unix_ms = 0_i64;
        let mut observation_links = Vec::with_capacity(event.observations().len());
        for (observation_id, role) in event.observations() {
            let (source_id, observed_at) = metadata
                .get(observation_id)
                .ok_or(ReductionError::InvalidInput)?;
            event_at_unix_ms = event_at_unix_ms.max(*observed_at);
            observation_links.push(ReducedObservationLink {
                source_id: source_id.clone(),
                observation_id: observation_id.clone(),
                role: *role,
            });
        }
        events.push(PersistedReducedEvent {
            protocol: protocol.to_owned(),
            canonical_event_id: event.id().to_owned(),
            event_at_unix_ms,
            observation_links,
        });
    }
    let canonical_event_ids = events
        .iter()
        .map(|event| event.canonical_event_id.as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let mut finality_updates = reconcile_finality(
        &manifest.chain_scope,
        manifest.finality,
        &canonical_event_ids,
    )?;
    let mut attributions = reconcile_attributions(
        &manifest.attribution_version,
        &manifest.chain_scope,
        manifest.settlements,
        &canonical_event_ids,
    )?;
    finality_updates.sort_by(|left, right| {
        left.canonical_event_id
            .cmp(&right.canonical_event_id)
            .then_with(|| left.asserted_at_unix_ms.cmp(&right.asserted_at_unix_ms))
            .then_with(|| left.evidence_id.cmp(&right.evidence_id))
    });
    attributions.sort_by(|left, right| left.settlement_id.cmp(&right.settlement_id));
    let mut output_hasher = Sha256::new();
    output_hasher.update(snapshot.encode());
    for update in &finality_updates {
        output_hasher.update(update.timeline_state_hash.as_bytes());
        output_hasher.update(update.evidence_id.as_bytes());
    }
    for attribution in &attributions {
        output_hasher.update(attribution.state_hash.as_bytes());
    }
    let output_sha256 = format!("{:x}", output_hasher.finalize());
    Ok(ReductionBatch {
        chain_scope: manifest.chain_scope,
        start_height: manifest.start_height,
        end_height: manifest.end_height,
        reducer_version: manifest.reducer_version,
        state_hash: snapshot.state_hash().to_owned(),
        events,
        finality_updates,
        attributions,
        output_sha256,
    })
}

fn reconcile_finality(
    chain_scope: &str,
    inputs: Vec<ManifestFinality>,
    canonical_event_ids: &std::collections::BTreeSet<&str>,
) -> Result<Vec<PersistedFinalityUpdate>, ReductionError> {
    let mut grouped = BTreeMap::<(String, String), Vec<ManifestFinality>>::new();
    for input in inputs {
        let (canonical_event_id, transaction_id, chain_matches) = match &input {
            ManifestFinality::Evm {
                canonical_event_id,
                transaction_id,
                ..
            } => (canonical_event_id, transaction_id, chain_scope != "solana"),
            ManifestFinality::Solana {
                canonical_event_id,
                transaction_id,
                ..
            } => (canonical_event_id, transaction_id, chain_scope == "solana"),
        };
        if !chain_matches || !canonical_event_ids.contains(canonical_event_id.as_str()) {
            return Err(ReductionError::InvalidInput);
        }
        grouped
            .entry((canonical_event_id.clone(), transaction_id.clone()))
            .or_default()
            .push(input);
    }

    let mut output = Vec::new();
    for ((canonical_event_id, transaction_id), group) in grouped {
        let subject = FinalitySubject::new(canonical_event_id, chain_scope, transaction_id.clone())
            .map_err(|_| ReductionError::InvalidInput)?;
        match group.first().ok_or(ReductionError::InvalidInput)? {
            ManifestFinality::Evm { confirmations, .. } => {
                let confirmations = *confirmations;
                let evidence = group
                    .into_iter()
                    .map(|input| match input {
                        ManifestFinality::Evm {
                            source_id,
                            provenance_id,
                            evidence_id,
                            asserted_at_unix_ms,
                            block_number,
                            block_hash,
                            canonical_block_hash,
                            latest_block,
                            finalized_block,
                            execution,
                            confirmations: item_confirmations,
                            ..
                        } if item_confirmations == confirmations => Ok(EvmFinalityEvidence::new(
                            subject.clone(),
                            source_id,
                            provenance_id,
                            evidence_id,
                            asserted_at_unix_ms,
                            block_number,
                            block_hash,
                            canonical_block_hash,
                            latest_block,
                            finalized_block,
                            execution.into(),
                        )),
                        _ => Err(ReductionError::InvalidInput),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let timeline = FinalityEngine::evm(confirmations)
                    .reconcile_for(&subject, evidence)
                    .map_err(|_| ReductionError::InvalidInput)?;
                append_finality_updates(&mut output, &timeline, &transaction_id);
            }
            ManifestFinality::Solana { .. } => {
                let evidence = group
                    .into_iter()
                    .map(|input| match input {
                        ManifestFinality::Solana {
                            source_id,
                            provenance_id,
                            evidence_id,
                            asserted_at_unix_ms,
                            slot,
                            block_hash,
                            canonical_block_hash,
                            commitment,
                            execution,
                            ..
                        } => Ok(SolanaFinalityEvidence::new(
                            subject.clone(),
                            source_id,
                            provenance_id,
                            evidence_id,
                            asserted_at_unix_ms,
                            slot,
                            block_hash,
                            canonical_block_hash,
                            commitment.into(),
                            execution.into(),
                        )),
                        _ => Err(ReductionError::InvalidInput),
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                let timeline = FinalityEngine::solana()
                    .reconcile_for(&subject, evidence)
                    .map_err(|_| ReductionError::InvalidInput)?;
                append_finality_updates(&mut output, &timeline, &transaction_id);
            }
        }
    }
    Ok(output)
}

fn append_finality_updates(
    output: &mut Vec<PersistedFinalityUpdate>,
    timeline: &agent_economy_reducer::FinalityTimeline,
    transaction_id: &str,
) {
    let current = timeline.current();
    for update in timeline.updates() {
        let (position, block_hash, basis_json) = finality_basis(update.basis());
        output.push(PersistedFinalityUpdate {
            canonical_event_id: update.subject().canonical_event_id().to_owned(),
            transaction_id: transaction_id.to_owned(),
            source_id: update.source_id().to_owned(),
            provenance_id: update.provenance_id().to_owned(),
            evidence_id: update.evidence_id().to_owned(),
            asserted_at_unix_ms: update.asserted_at(),
            status: finality_status(update.status()),
            position,
            block_hash,
            basis_json,
            timeline_state_hash: timeline.state_hash().to_owned(),
            accepted: true,
            current_status: finality_status(current.unwrap_or(update.status())),
            current: current == Some(update.status()),
        });
    }
    for conflict in timeline.conflicts() {
        let update = conflict.update();
        let (position, block_hash, basis_json) = finality_basis(update.basis());
        output.push(PersistedFinalityUpdate {
            canonical_event_id: update.subject().canonical_event_id().to_owned(),
            transaction_id: transaction_id.to_owned(),
            source_id: update.source_id().to_owned(),
            provenance_id: update.provenance_id().to_owned(),
            evidence_id: update.evidence_id().to_owned(),
            asserted_at_unix_ms: update.asserted_at(),
            status: finality_status(update.status()),
            position,
            block_hash,
            basis_json,
            timeline_state_hash: timeline.state_hash().to_owned(),
            accepted: false,
            current_status: finality_status(conflict.current_status()),
            current: false,
        });
    }
}

fn finality_basis(basis: &FinalityBasis) -> (u64, String, serde_json::Value) {
    match basis {
        FinalityBasis::Evm {
            block_number,
            block_hash,
            canonical_block_hash,
            latest_block,
            finalized_block,
            execution,
        } => (
            *block_number,
            block_hash.clone(),
            json!({
                "kind": "evm", "block_number": block_number, "block_hash": block_hash,
                "canonical_block_hash": canonical_block_hash, "latest_block": latest_block,
                "finalized_block": finalized_block, "execution": execution_name(*execution)
            }),
        ),
        FinalityBasis::Solana {
            slot,
            block_hash,
            canonical_block_hash,
            commitment,
            execution,
        } => (
            *slot,
            block_hash.clone(),
            json!({
                "kind": "solana", "slot": slot, "block_hash": block_hash,
                "canonical_block_hash": canonical_block_hash,
                "commitment": commitment_name(*commitment), "execution": execution_name(*execution)
            }),
        ),
    }
}

fn reconcile_attributions(
    version: &str,
    chain_scope: &str,
    inputs: Vec<ManifestSettlementAttribution>,
    canonical_event_ids: &std::collections::BTreeSet<&str>,
) -> Result<Vec<PersistedAttribution>, ReductionError> {
    let engine = AttributionEngine::new(version).map_err(|_| ReductionError::InvalidInput)?;
    let mut output = Vec::with_capacity(inputs.len());
    for input in inputs {
        if !canonical_event_ids.contains(input.canonical_event_id.as_str())
            || input.settled_at_unix_ms <= 0
            || input.settlement.network != chain_scope
        {
            return Err(ReductionError::InvalidInput);
        }
        let finality = match input.settlement.finality {
            ManifestSettlementFinality::Confirmed => SettlementFinality::Confirmed,
            ManifestSettlementFinality::Finalized => SettlementFinality::Finalized,
        };
        let settlement = Settlement::new(
            &input.settlement.id,
            &input.settlement.protocol,
            &input.settlement.network,
            &input.settlement.asset,
            &input.settlement.amount_atomic,
            &input.settlement.pay_to,
            finality,
            input.settlement.requirement_id.as_deref(),
            input.settlement.evidence_ids.iter().cloned(),
        )
        .map_err(|_| ReductionError::InvalidInput)?;
        let requirement_evidence = input
            .requirements
            .iter()
            .map(|item| {
                item.evidence_ids
                    .first()
                    .cloned()
                    .map(|evidence_id| (item.id.clone(), evidence_id))
                    .ok_or(ReductionError::InvalidInput)
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        let persisted_requirements = input
            .requirements
            .iter()
            .map(|item| PersistedRequirement {
                requirement_id: item.id.clone(),
                payment_option_id: item.payment_option_id.clone(),
                endpoint_id: item.endpoint_id.clone(),
                service_id: item.service_id.clone(),
                evidence_ids: item.evidence_ids.clone(),
            })
            .collect();
        let requirements = input
            .requirements
            .into_iter()
            .map(|item| {
                PaymentRequirement::new(
                    item.id,
                    item.payment_option_id,
                    item.endpoint_id,
                    item.service_id,
                    item.protocol,
                    item.network,
                    item.asset,
                    item.amount_atomic,
                    item.pay_to,
                    item.evidence_ids,
                )
            })
            .collect::<Result<Vec<_>, _>>()
            .map_err(|_| ReductionError::InvalidInput)?;
        let result = engine
            .attribute(&settlement, requirements)
            .map_err(|_| ReductionError::InvalidInput)?;
        output.push(PersistedAttribution {
            settlement_id: input.settlement.id,
            canonical_event_id: input.canonical_event_id,
            transaction_id: input.transaction_id,
            settled_at_unix_ms: input.settled_at_unix_ms,
            source_id: input.source_id,
            provenance_id: input.provenance_id,
            protocol: input.settlement.protocol,
            asset: input.settlement.asset,
            amount_atomic: input.settlement.amount_atomic,
            level: attribution_level(result.level()),
            method: attribution_method(result.method()),
            explicit_requirement_id: result.explicit_requirement_id().map(str::to_owned),
            engine_version: result.engine_version().to_owned(),
            evidence_ids: result.evidence_ids().to_vec(),
            input_snapshot_hash: result.input_snapshot_hash().to_owned(),
            state_hash: result.state_hash().to_owned(),
            result_encoded_base64: STANDARD.encode(result.encode()),
            requirements: persisted_requirements,
            candidates: result
                .candidates()
                .iter()
                .map(|candidate| PersistedAttributionCandidate {
                    attribution_id: candidate.id().to_owned(),
                    requirement_id: candidate.requirement_id().to_owned(),
                    payment_option_id: candidate.payment_option_id().to_owned(),
                    endpoint_id: candidate.endpoint_id().to_owned(),
                    service_id: candidate.service_id().to_owned(),
                    confidence_bps: candidate.confidence_bps(),
                    evidence_ids: candidate.evidence_ids().to_vec(),
                    requirement_evidence_id: requirement_evidence
                        .get(candidate.requirement_id())
                        .expect("attribution candidates cite an input requirement")
                        .clone(),
                })
                .collect(),
        });
    }
    Ok(output)
}

impl From<ManifestExecution> for ExecutionOutcome {
    fn from(value: ManifestExecution) -> Self {
        match value {
            ManifestExecution::Succeeded => Self::Succeeded,
            ManifestExecution::Reverted => Self::Reverted,
        }
    }
}

impl From<ManifestCommitment> for SolanaCommitment {
    fn from(value: ManifestCommitment) -> Self {
        match value {
            ManifestCommitment::Processed => Self::Processed,
            ManifestCommitment::Confirmed => Self::Confirmed,
            ManifestCommitment::Finalized => Self::Finalized,
        }
    }
}

fn finality_status(status: FinalityStatus) -> &'static str {
    match status {
        FinalityStatus::Observed => "observed",
        FinalityStatus::Confirmed => "confirmed",
        FinalityStatus::Finalized => "finalized",
        FinalityStatus::Orphaned => "orphaned",
        FinalityStatus::Reverted => "reverted",
    }
}

fn execution_name(execution: ExecutionOutcome) -> &'static str {
    match execution {
        ExecutionOutcome::Succeeded => "succeeded",
        ExecutionOutcome::Reverted => "reverted",
    }
}

fn commitment_name(commitment: SolanaCommitment) -> &'static str {
    match commitment {
        SolanaCommitment::Processed => "processed",
        SolanaCommitment::Confirmed => "confirmed",
        SolanaCommitment::Finalized => "finalized",
    }
}

fn attribution_level(level: AttributionLevel) -> &'static str {
    match level {
        AttributionLevel::Verified => "verified",
        AttributionLevel::Strong => "strong",
        AttributionLevel::Weak => "weak",
        AttributionLevel::Unknown => "unknown",
    }
}

fn attribution_method(method: AttributionMethod) -> &'static str {
    match method {
        AttributionMethod::ExplicitRequirement => "explicit_requirement",
        AttributionMethod::UniqueExact => "unique_exact",
        AttributionMethod::SharedExact => "shared_exact",
        AttributionMethod::None => "none",
    }
}

fn poison(code: &str) -> HandlerFailure {
    HandlerFailure::poison(code).expect("static reduction error codes are valid")
}

fn retryable(code: &str) -> HandlerFailure {
    HandlerFailure::retryable(code).expect("static reduction error codes are valid")
}
