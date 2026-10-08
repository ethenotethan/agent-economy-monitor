use std::{collections::BTreeMap, sync::Arc};

use agent_economy_classification::{
    Activity, AutonomySignal, BehavioralClassifier, FeatureEngine, FeatureMetric, LabelDefinition,
    LabelKind, Protocol, Rule,
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::worker::{
    HandlerFailure, JobResult, LeasedJob, WorkerHandler, WorkerJobStore, WorkerMode,
    WorkerStoreError,
};

const MAX_ACTIVITIES: usize = 50_000;
const MAX_LABELS: usize = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClassificationWorkerError {
    InvalidInput,
    Unavailable,
    Conflict,
}

#[derive(Clone, Debug)]
pub struct ClassificationBatch {
    buyer_handle_id: String,
    run_id: String,
    run_version: u32,
    classifier_version: String,
    feature_version: String,
    provenance_id: String,
    window_start_unix_seconds: i64,
    window_end_unix_seconds: i64,
    input_snapshot_hash: String,
    label_set_hash: String,
    features_json: Value,
    labels_json: Value,
    claims_json: Value,
    evidence_ids: Vec<String>,
    result_encoding: Vec<u8>,
    state_hash: String,
    output_sha256: String,
}

#[async_trait]
pub trait ClassificationStore: Send + Sync + 'static {
    async fn load_input(&self, lease: &LeasedJob) -> Result<Vec<u8>, ClassificationWorkerError>;

    async fn commit_batch(
        &self,
        lease: &LeasedJob,
        batch: &ClassificationBatch,
    ) -> Result<Option<String>, ClassificationWorkerError>;
}

pub struct ClassificationHandler<S: ?Sized> {
    store: Arc<S>,
}

impl<S: ?Sized> ClassificationHandler<S> {
    pub fn new(store: Arc<S>) -> Self {
        Self { store }
    }
}

#[async_trait]
impl<S> WorkerHandler for ClassificationHandler<S>
where
    S: ClassificationStore + ?Sized,
{
    async fn process(&self, job: &LeasedJob) -> Result<JobResult, HandlerFailure> {
        if job.mode != WorkerMode::Classify || job.job_kind != "buyer-behavior-v1" {
            return Err(HandlerFailure::poison("unexpected_job_kind").unwrap());
        }
        let input = self.store.load_input(job).await.map_err(store_failure)?;
        let batch = derive_classification_batch(&job.input_sha256, &input)
            .map_err(|_| HandlerFailure::poison("invalid_classification_input").unwrap())?;
        let output_sha256 = self
            .store
            .commit_batch(job, &batch)
            .await
            .map_err(store_failure)?
            .ok_or_else(|| HandlerFailure::retryable("classification_commit_conflict").unwrap())?;
        JobResult::new(&output_sha256)
    }
}

fn store_failure(error: ClassificationWorkerError) -> HandlerFailure {
    match error {
        ClassificationWorkerError::InvalidInput => {
            HandlerFailure::poison("invalid_classification_input").unwrap()
        }
        ClassificationWorkerError::Unavailable => {
            HandlerFailure::retryable("classification_store_unavailable").unwrap()
        }
        ClassificationWorkerError::Conflict => {
            HandlerFailure::retryable("classification_commit_conflict").unwrap()
        }
    }
}

impl ClassificationBatch {
    pub fn buyer_handle_id(&self) -> &str {
        &self.buyer_handle_id
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub const fn run_version(&self) -> u32 {
        self.run_version
    }

    pub fn classifier_version(&self) -> &str {
        &self.classifier_version
    }

    pub fn feature_version(&self) -> &str {
        &self.feature_version
    }

    pub fn provenance_id(&self) -> &str {
        &self.provenance_id
    }

    pub const fn window(&self) -> (i64, i64) {
        (self.window_start_unix_seconds, self.window_end_unix_seconds)
    }

    pub fn input_snapshot_hash(&self) -> &str {
        &self.input_snapshot_hash
    }

    pub fn label_set_hash(&self) -> &str {
        &self.label_set_hash
    }

    pub const fn features_json(&self) -> &Value {
        &self.features_json
    }

    pub const fn labels_json(&self) -> &Value {
        &self.labels_json
    }

    pub const fn claims_json(&self) -> &Value {
        &self.claims_json
    }

    pub fn evidence_ids(&self) -> &[String] {
        &self.evidence_ids
    }

    pub fn result_encoding(&self) -> &[u8] {
        &self.result_encoding
    }

    pub fn state_hash(&self) -> &str {
        &self.state_hash
    }

    pub fn output_sha256(&self) -> &str {
        &self.output_sha256
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ClassificationManifest {
    schema_version: u32,
    buyer_handle_id: String,
    run_id: String,
    run_version: u32,
    classifier_version: String,
    feature_version: String,
    window_start_unix_seconds: i64,
    window_end_unix_seconds: i64,
    provenance_id: String,
    activities: Vec<ManifestActivity>,
    labels: Vec<ManifestLabel>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestActivity {
    settlement_id: String,
    amount_atomic: String,
    occurred_at_unix_seconds: i64,
    protocol: String,
    counterparty: String,
    autonomy: String,
    evidence_id: String,
    #[serde(default)]
    observation_id: Option<String>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManifestLabel {
    id: String,
    version: u32,
    kind: String,
    metric: String,
    threshold: String,
    confidence_bps: u16,
    definition_hash: String,
}

pub struct PostgresClassificationStore {
    client: Arc<Mutex<Client>>,
}

impl PostgresClassificationStore {
    pub fn new(client: Client) -> Self {
        Self::from_shared(Arc::new(Mutex::new(client)))
    }

    pub fn from_shared(client: Arc<Mutex<Client>>) -> Self {
        Self { client }
    }
}

#[async_trait]
impl ClassificationStore for PostgresClassificationStore {
    async fn load_input(&self, lease: &LeasedJob) -> Result<Vec<u8>, ClassificationWorkerError> {
        let client = self.client.lock().await;
        client
            .query_opt(
                "SELECT agent_economy.load_classifier_job_input(\
                    $1::text::uuid, $2, $3::text::uuid)",
                &[&lease.job_id, &lease.lease_owner, &lease.lease_token],
            )
            .await
            .map_err(|_| ClassificationWorkerError::Unavailable)?
            .map(|row| row.get::<_, Vec<u8>>(0))
            .ok_or(ClassificationWorkerError::Conflict)
    }

    async fn commit_batch(
        &self,
        lease: &LeasedJob,
        batch: &ClassificationBatch,
    ) -> Result<Option<String>, ClassificationWorkerError> {
        let run_version = i32::try_from(batch.run_version)
            .map_err(|_| ClassificationWorkerError::InvalidInput)?;
        let (window_start, window_end) = batch.window();
        let features_json = batch.features_json.to_string();
        let labels_json = batch.labels_json.to_string();
        let claims_json = batch.claims_json.to_string();
        let client = self.client.lock().await;
        client
            .query_one(
                "SELECT agent_economy.commit_classification_batch(\
                    $1::text::uuid, $2, $3::text::uuid, $4, $5, $6, $7::integer, \
                    $8, $9, $10::text::uuid, $11::bigint, $12::bigint, $13, $14, \
                    $15::text::jsonb, $16::text::jsonb, $17::text::jsonb, $18::text[], $19::bytea, $20)",
                &[
                    &lease.job_id,
                    &lease.lease_owner,
                    &lease.lease_token,
                    &lease.input_sha256,
                    &batch.output_sha256,
                    &batch.run_id,
                    &run_version,
                    &batch.buyer_handle_id,
                    &batch.classifier_version,
                    &batch.provenance_id,
                    &window_start,
                    &window_end,
                    &batch.input_snapshot_hash,
                    &batch.label_set_hash,
                    &features_json,
                    &labels_json,
                    &claims_json,
                    &batch.evidence_ids,
                    &batch.result_encoding,
                    &batch.state_hash,
                ],
            )
            .await
            .map_err(|_| ClassificationWorkerError::Unavailable)
            .map(|row| row.get::<_, Option<String>>(0))
    }
}

#[async_trait]
impl WorkerJobStore for PostgresClassificationStore {
    async fn claim(
        &self,
        mode: WorkerMode,
        lease_owner: &str,
        lease_seconds: u64,
    ) -> Result<Option<LeasedJob>, WorkerStoreError> {
        if mode != WorkerMode::Classify || !(1..=3_600).contains(&lease_seconds) {
            return Err(WorkerStoreError::Invalid);
        }
        let lease_seconds = i64::try_from(lease_seconds).map_err(|_| WorkerStoreError::Invalid)?;
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT job_id::text, mode, job_kind, input_sha256, attempt_count, \
                        lease_owner, lease_token::text \
                 FROM agent_economy.claim_classifier_job($1, $2::bigint)",
                &[&lease_owner, &lease_seconds],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?;
        row.map(|row| {
            let returned_mode: String = row.get(1);
            let job_kind: String = row.get(2);
            if returned_mode != WorkerMode::Classify.as_str() || job_kind != "buyer-behavior-v1" {
                return Err(WorkerStoreError::Invalid);
            }
            Ok(LeasedJob {
                job_id: row.get(0),
                mode: WorkerMode::Classify,
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
        mutate_classifier_lease(
            &self.client,
            "renew_classifier_job_lease",
            job,
            Some(lease_seconds),
            None,
            None,
        )
        .await
    }

    async fn complete(&self, job: &LeasedJob, output_sha256: &str) -> Result<(), WorkerStoreError> {
        mutate_classifier_lease(
            &self.client,
            "complete_classifier_job",
            job,
            None,
            Some(output_sha256),
            None,
        )
        .await
    }

    async fn fail(
        &self,
        job: &LeasedJob,
        error_code: &str,
        retryable: bool,
    ) -> Result<(), WorkerStoreError> {
        mutate_classifier_lease(
            &self.client,
            "fail_classifier_job",
            job,
            None,
            None,
            Some((error_code, retryable)),
        )
        .await
    }
}

async fn mutate_classifier_lease(
    client: &Arc<Mutex<Client>>,
    function: &str,
    job: &LeasedJob,
    lease_seconds: Option<u64>,
    output_sha256: Option<&str>,
    failure: Option<(&str, bool)>,
) -> Result<(), WorkerStoreError> {
    let client = client.lock().await;
    let row = match function {
        "renew_classifier_job_lease" => {
            let seconds = i64::try_from(lease_seconds.ok_or(WorkerStoreError::Invalid)?)
                .map_err(|_| WorkerStoreError::Invalid)?;
            client.query_one(
                "SELECT agent_economy.renew_classifier_job_lease($1::text::uuid, $2, $3::text::uuid, $4::bigint)",
                &[&job.job_id, &job.lease_owner, &job.lease_token, &seconds],
            ).await
        }
        "complete_classifier_job" => client.query_one(
            "SELECT agent_economy.complete_classifier_job($1::text::uuid, $2, $3::text::uuid, $4)",
            &[&job.job_id, &job.lease_owner, &job.lease_token, &output_sha256.ok_or(WorkerStoreError::Invalid)?],
        ).await,
        "fail_classifier_job" => {
            let (code, retryable) = failure.ok_or(WorkerStoreError::Invalid)?;
            client.query_one(
                "SELECT agent_economy.fail_classifier_job($1::text::uuid, $2, $3::text::uuid, $4, $5, 30::bigint)",
                &[&job.job_id, &job.lease_owner, &job.lease_token, &code, &retryable],
            ).await
        }
        _ => return Err(WorkerStoreError::Invalid),
    }.map_err(|_| WorkerStoreError::Unavailable)?;
    if row.get::<_, bool>(0) {
        Ok(())
    } else {
        Err(WorkerStoreError::Conflict)
    }
}

pub fn derive_classification_batch(
    input_sha256: &str,
    bytes: &[u8],
) -> Result<ClassificationBatch, ClassificationWorkerError> {
    if !valid_sha256(input_sha256)
        || bytes.is_empty()
        || format!("{:x}", Sha256::digest(bytes)) != input_sha256
    {
        return Err(ClassificationWorkerError::InvalidInput);
    }
    let manifest: ClassificationManifest =
        serde_json::from_slice(bytes).map_err(|_| ClassificationWorkerError::InvalidInput)?;
    if manifest.schema_version != 1
        || manifest.buyer_handle_id.trim().is_empty()
        || manifest.run_id.trim().is_empty()
        || manifest.run_version == 0
        || manifest.provenance_id.trim().is_empty()
        || manifest.activities.is_empty()
        || manifest.activities.len() > MAX_ACTIVITIES
        || manifest.labels.is_empty()
        || manifest.labels.len() > MAX_LABELS
    {
        return Err(ClassificationWorkerError::InvalidInput);
    }

    let activities = manifest
        .activities
        .iter()
        .map(|activity| {
            if activity.settlement_id.trim().is_empty()
                || activity.observation_id.as_deref().is_some_and(|value| {
                    value
                        .strip_prefix("sha256:")
                        .is_none_or(|digest| !valid_sha256(digest))
                })
            {
                return Err(ClassificationWorkerError::InvalidInput);
            }
            let amount = activity
                .amount_atomic
                .parse::<u128>()
                .map_err(|_| ClassificationWorkerError::InvalidInput)?;
            Activity::new(
                amount,
                activity.occurred_at_unix_seconds,
                protocol(&activity.protocol)?,
                &activity.counterparty,
                autonomy(&activity.autonomy)?,
                &activity.evidence_id,
            )
            .map_err(|_| ClassificationWorkerError::InvalidInput)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let snapshot = FeatureEngine::new(&manifest.feature_version)
        .map_err(|_| ClassificationWorkerError::InvalidInput)?
        .compute(
            &manifest.buyer_handle_id,
            manifest.window_start_unix_seconds,
            manifest.window_end_unix_seconds,
            activities,
        )
        .map_err(|_| ClassificationWorkerError::InvalidInput)?;

    let mut labels = manifest.labels;
    labels.sort_by(|left, right| (&left.id, left.version).cmp(&(&right.id, right.version)));
    let confidence = labels
        .iter()
        .map(|label| ((label.id.clone(), label.version), label.confidence_bps))
        .collect::<BTreeMap<_, _>>();
    let definitions = labels
        .iter()
        .map(|label| {
            if label.confidence_bps > 10_000 || !valid_sha256(&label.definition_hash) {
                return Err(ClassificationWorkerError::InvalidInput);
            }
            LabelDefinition::new(
                &label.id,
                label.version,
                label_kind(&label.kind)?,
                Rule::at_least(
                    feature_metric(&label.metric)?,
                    label
                        .threshold
                        .parse::<u128>()
                        .map_err(|_| ClassificationWorkerError::InvalidInput)?,
                ),
            )
            .map_err(|_| ClassificationWorkerError::InvalidInput)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let result = BehavioralClassifier::new(&manifest.classifier_version, definitions)
        .map_err(|_| ClassificationWorkerError::InvalidInput)?
        .classify(&snapshot);

    let features_json = json!({
        "autonomous_count": snapshot.autonomous_count().to_string(),
        "autonomy_observed_count": snapshot.autonomy_observed_count().to_string(),
        "median_cadence_seconds": snapshot.median_cadence_seconds().map(|value| value.to_string()),
        "mpp_count": snapshot.protocol_count(Protocol::Mpp).to_string(),
        "payment_count": snapshot.payment_count().to_string(),
        "total_spend_atomic": snapshot.total_spend_atomic().to_string(),
        "unique_counterparties": snapshot.unique_counterparties().to_string(),
        "x402_count": snapshot.protocol_count(Protocol::X402).to_string(),
    });
    let labels_json = Value::Array(
        labels
            .iter()
            .map(|label| {
                json!({
                    "confidence_bps": label.confidence_bps,
                    "definition_hash": label.definition_hash,
                    "id": label.id,
                    "kind": label.kind,
                    "metric": label.metric,
                    "threshold": label.threshold,
                    "version": label.version,
                })
            })
            .collect(),
    );
    let claims_json = Value::Array(
        result
            .claims()
            .iter()
            .map(|claim| {
                json!({
                    "confidence_bps": confidence[&(claim.label_id().to_owned(), claim.label_version())],
                    "label_id": claim.label_id(),
                    "label_version": claim.label_version(),
                    "status": "inferred",
                })
            })
            .collect(),
    );
    let result_encoding = result.encode().to_vec();
    let state_hash = result.state_hash().to_owned();
    let output_document = json!({
        "claims": claims_json,
        "features": features_json,
        "input_snapshot_hash": snapshot.input_snapshot_hash(),
        "label_set_hash": result.label_set_hash(),
        "labels": labels_json,
        "result_state_hash": state_hash,
    });
    let output_sha256 = format!(
        "{:x}",
        Sha256::digest(output_document.to_string().as_bytes())
    );

    Ok(ClassificationBatch {
        buyer_handle_id: manifest.buyer_handle_id,
        run_id: manifest.run_id,
        run_version: manifest.run_version,
        classifier_version: manifest.classifier_version,
        feature_version: manifest.feature_version,
        provenance_id: manifest.provenance_id,
        window_start_unix_seconds: manifest.window_start_unix_seconds,
        window_end_unix_seconds: manifest.window_end_unix_seconds,
        input_snapshot_hash: snapshot.input_snapshot_hash().to_owned(),
        label_set_hash: result.label_set_hash().to_owned(),
        features_json,
        labels_json,
        claims_json,
        evidence_ids: snapshot.evidence_ids().to_vec(),
        result_encoding,
        state_hash,
        output_sha256,
    })
}

fn protocol(value: &str) -> Result<Protocol, ClassificationWorkerError> {
    match value {
        "x402" => Ok(Protocol::X402),
        "mpp" => Ok(Protocol::Mpp),
        _ => Err(ClassificationWorkerError::InvalidInput),
    }
}

fn autonomy(value: &str) -> Result<AutonomySignal, ClassificationWorkerError> {
    match value {
        "verified_agent" => Ok(AutonomySignal::VerifiedAgent),
        "verified_human" => Ok(AutonomySignal::VerifiedHuman),
        "unknown" => Ok(AutonomySignal::Unknown),
        _ => Err(ClassificationWorkerError::InvalidInput),
    }
}

fn label_kind(value: &str) -> Result<LabelKind, ClassificationWorkerError> {
    match value {
        "core" => Ok(LabelKind::Core),
        "extension" => Ok(LabelKind::Extension),
        _ => Err(ClassificationWorkerError::InvalidInput),
    }
}

fn feature_metric(value: &str) -> Result<FeatureMetric, ClassificationWorkerError> {
    match value {
        "total_spend_atomic" => Ok(FeatureMetric::TotalSpendAtomic),
        "payment_count" => Ok(FeatureMetric::PaymentCount),
        "median_cadence_seconds" => Ok(FeatureMetric::MedianCadenceSeconds),
        "x402_count" => Ok(FeatureMetric::X402Count),
        "mpp_count" => Ok(FeatureMetric::MppCount),
        "unique_counterparties" => Ok(FeatureMetric::UniqueCounterparties),
        "autonomous_count" => Ok(FeatureMetric::AutonomousCount),
        "autonomy_observed_count" => Ok(FeatureMetric::AutonomyObservedCount),
        _ => Err(ClassificationWorkerError::InvalidInput),
    }
}

fn valid_sha256(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}
