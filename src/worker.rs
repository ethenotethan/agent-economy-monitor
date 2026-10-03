use std::{collections::BTreeMap, fmt, sync::Arc};

use async_trait::async_trait;
use tokio::sync::Mutex;
use tokio_postgres::Client;
use tracing::{info, warn};

const DEFAULT_LEASE_SECONDS: u64 = 300;
const MAX_LEASE_SECONDS: u64 = 3_600;
const RETRY_DELAY_SECONDS: i64 = 30;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WorkerMode {
    Collect,
    Reduce,
    Classify,
    Enrich,
}

impl WorkerMode {
    pub fn parse(value: &str) -> Result<Self, WorkerDispatchError> {
        match value {
            "collect" => Ok(Self::Collect),
            "reduce" => Ok(Self::Reduce),
            "classify" => Ok(Self::Classify),
            "enrich" => Ok(Self::Enrich),
            _ => Err(WorkerDispatchError::UnknownMode(value.to_owned())),
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Collect => "collect",
            Self::Reduce => "reduce",
            Self::Classify => "classify",
            Self::Enrich => "enrich",
        }
    }
}

impl fmt::Display for WorkerMode {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LeasedJob {
    pub job_id: String,
    pub mode: WorkerMode,
    pub job_kind: String,
    pub input_sha256: String,
    pub attempt: u16,
    pub lease_owner: String,
    pub lease_token: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobResult {
    output_sha256: String,
}

impl JobResult {
    pub fn new(output_sha256: &str) -> Result<Self, HandlerFailure> {
        validate_hash(output_sha256)
            .map_err(|_| HandlerFailure::poison("invalid_output_digest").unwrap())?;
        Ok(Self {
            output_sha256: output_sha256.to_owned(),
        })
    }

    pub fn output_sha256(&self) -> &str {
        &self.output_sha256
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HandlerFailure {
    code: String,
    retryable: bool,
}

impl HandlerFailure {
    pub fn retryable(code: &str) -> Result<Self, WorkerDispatchError> {
        Self::new(code, true)
    }

    pub fn poison(code: &str) -> Result<Self, WorkerDispatchError> {
        Self::new(code, false)
    }

    fn new(code: &str, retryable: bool) -> Result<Self, WorkerDispatchError> {
        validate_error_code(code)?;
        Ok(Self {
            code: code.to_owned(),
            retryable,
        })
    }

    pub fn code(&self) -> &str {
        &self.code
    }

    pub const fn is_retryable(&self) -> bool {
        self.retryable
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CompletedJob {
    pub job_id: String,
    pub mode: WorkerMode,
    pub attempt: u16,
    pub input_sha256: String,
    pub output_sha256: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerStoreError {
    Invalid,
    Conflict,
    Unavailable,
}

impl fmt::Display for WorkerStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Invalid => "invalid worker lease operation",
            Self::Conflict => "worker lease is no longer live",
            Self::Unavailable => "worker lease store unavailable",
        })
    }
}

impl std::error::Error for WorkerStoreError {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkerDispatchError {
    UnknownMode(String),
    MissingHandler(WorkerMode),
    NoJob(WorkerMode),
    HandlerFailed(String),
    InvalidConfiguration,
    Store(WorkerStoreError),
}

impl fmt::Display for WorkerDispatchError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownMode(mode) => write!(formatter, "unknown worker mode: {mode}"),
            Self::MissingHandler(mode) => write!(formatter, "worker handler unavailable: {mode}"),
            Self::NoJob(mode) => write!(formatter, "no {mode} job was processed"),
            Self::HandlerFailed(code) => write!(formatter, "worker handler failed: {code}"),
            Self::InvalidConfiguration => formatter.write_str("invalid worker configuration"),
            Self::Store(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for WorkerDispatchError {}

impl From<WorkerStoreError> for WorkerDispatchError {
    fn from(value: WorkerStoreError) -> Self {
        Self::Store(value)
    }
}

#[async_trait]
pub trait WorkerHandler: Send + Sync + 'static {
    async fn process(&self, job: &LeasedJob) -> Result<JobResult, HandlerFailure>;
}

#[async_trait]
pub trait WorkerJobStore: Send + Sync + 'static {
    async fn claim(
        &self,
        mode: WorkerMode,
        lease_owner: &str,
        lease_seconds: u64,
    ) -> Result<Option<LeasedJob>, WorkerStoreError>;

    async fn renew(&self, job: &LeasedJob, lease_seconds: u64) -> Result<(), WorkerStoreError>;

    async fn complete(&self, job: &LeasedJob, output_sha256: &str) -> Result<(), WorkerStoreError>;

    async fn fail(
        &self,
        job: &LeasedJob,
        error_code: &str,
        retryable: bool,
    ) -> Result<(), WorkerStoreError>;
}

pub struct WorkerDispatcher {
    store: Arc<dyn WorkerJobStore>,
    handlers: BTreeMap<WorkerMode, Arc<dyn WorkerHandler>>,
    lease_seconds: u64,
}

impl WorkerDispatcher {
    pub fn new(store: Arc<dyn WorkerJobStore>) -> Self {
        Self {
            store,
            handlers: BTreeMap::new(),
            lease_seconds: DEFAULT_LEASE_SECONDS,
        }
    }

    pub fn register(mut self, mode: WorkerMode, handler: Arc<dyn WorkerHandler>) -> Self {
        self.handlers.insert(mode, handler);
        self
    }

    pub fn with_lease_seconds(mut self, lease_seconds: u64) -> Result<Self, WorkerDispatchError> {
        validate_lease_seconds(lease_seconds).map_err(WorkerDispatchError::Store)?;
        self.lease_seconds = lease_seconds;
        Ok(self)
    }

    pub async fn run_once(
        &self,
        mode: WorkerMode,
        lease_owner: &str,
    ) -> Result<CompletedJob, WorkerDispatchError> {
        let handler = self
            .handlers
            .get(&mode)
            .ok_or(WorkerDispatchError::MissingHandler(mode))?;
        validate_owner(lease_owner).map_err(WorkerDispatchError::Store)?;
        let job = self
            .store
            .claim(mode, lease_owner, self.lease_seconds)
            .await?
            .ok_or(WorkerDispatchError::NoJob(mode))?;
        info!(
            job_id = %job.job_id,
            mode = %job.mode,
            attempt = job.attempt,
            input_sha256 = %job.input_sha256,
            "claimed bounded worker job"
        );
        match handler.process(&job).await {
            Ok(result) => {
                self.store.complete(&job, result.output_sha256()).await?;
                info!(
                    job_id = %job.job_id,
                    mode = %job.mode,
                    attempt = job.attempt,
                    input_sha256 = %job.input_sha256,
                    output_sha256 = %result.output_sha256(),
                    "committed bounded worker result"
                );
                Ok(CompletedJob {
                    job_id: job.job_id,
                    mode,
                    attempt: job.attempt,
                    input_sha256: job.input_sha256,
                    output_sha256: result.output_sha256,
                })
            }
            Err(failure) => {
                warn!(
                    job_id = %job.job_id,
                    mode = %job.mode,
                    attempt = job.attempt,
                    input_sha256 = %job.input_sha256,
                    error_code = %failure.code(),
                    retryable = failure.is_retryable(),
                    "bounded worker handler failed"
                );
                self.store
                    .fail(&job, failure.code(), failure.is_retryable())
                    .await?;
                Err(WorkerDispatchError::HandlerFailed(failure.code))
            }
        }
    }
}

pub struct PostgresWorkerJobStore {
    client: Arc<Mutex<Client>>,
    namespace_id: String,
}

impl PostgresWorkerJobStore {
    pub fn new(client: Client, namespace_id: String) -> Self {
        Self::from_shared(Arc::new(Mutex::new(client)), namespace_id)
    }

    pub fn from_shared(client: Arc<Mutex<Client>>, namespace_id: String) -> Self {
        Self {
            client,
            namespace_id,
        }
    }

    pub async fn cancel(&self, job: &LeasedJob) -> Result<(), WorkerStoreError> {
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.cancel_worker_job(\
                    $1::text::uuid, $2::text::uuid, $3, $4::text::uuid)",
                &[
                    &self.namespace_id,
                    &job.job_id,
                    &job.lease_owner,
                    &job.lease_token,
                ],
            )
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

#[async_trait]
impl WorkerJobStore for PostgresWorkerJobStore {
    async fn claim(
        &self,
        mode: WorkerMode,
        lease_owner: &str,
        lease_seconds: u64,
    ) -> Result<Option<LeasedJob>, WorkerStoreError> {
        validate_owner(lease_owner)?;
        validate_lease_seconds(lease_seconds)?;
        let lease_seconds = i64::try_from(lease_seconds).map_err(|_| WorkerStoreError::Invalid)?;
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT job_id::text, mode, job_kind, input_sha256, attempt_count, \
                        lease_owner, lease_token::text \
                 FROM agent_economy.claim_worker_job(\
                    $1::text::uuid, $2, $3, $4::bigint)",
                &[
                    &self.namespace_id,
                    &mode.as_str(),
                    &lease_owner,
                    &lease_seconds,
                ],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?;
        row.map(row_to_job).transpose()
    }

    async fn renew(&self, job: &LeasedJob, lease_seconds: u64) -> Result<(), WorkerStoreError> {
        validate_lease_seconds(lease_seconds)?;
        let lease_seconds = i64::try_from(lease_seconds).map_err(|_| WorkerStoreError::Invalid)?;
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.renew_worker_job_lease(\
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
        validate_hash(output_sha256)?;
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.complete_worker_job(\
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
        validate_error_code(error_code).map_err(|_| WorkerStoreError::Invalid)?;
        let client = self.client.lock().await;
        let changed = client
            .query_one(
                "SELECT agent_economy.fail_worker_job(\
                    $1::text::uuid, $2::text::uuid, $3, $4::text::uuid, $5, $6, $7::bigint)",
                &[
                    &self.namespace_id,
                    &job.job_id,
                    &job.lease_owner,
                    &job.lease_token,
                    &error_code,
                    &retryable,
                    &RETRY_DELAY_SECONDS,
                ],
            )
            .await
            .map_err(|_| WorkerStoreError::Unavailable)?
            .get::<_, bool>(0);
        changed.then_some(()).ok_or(WorkerStoreError::Conflict)
    }
}

fn row_to_job(row: tokio_postgres::Row) -> Result<LeasedJob, WorkerStoreError> {
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
    })
}

fn validate_owner(owner: &str) -> Result<(), WorkerStoreError> {
    if owner.is_empty()
        || owner.len() > 128
        || !owner
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':'))
    {
        return Err(WorkerStoreError::Invalid);
    }
    Ok(())
}

fn validate_lease_seconds(seconds: u64) -> Result<(), WorkerStoreError> {
    if seconds == 0 || seconds > MAX_LEASE_SECONDS {
        return Err(WorkerStoreError::Invalid);
    }
    Ok(())
}

fn validate_hash(value: &str) -> Result<(), WorkerStoreError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(WorkerStoreError::Invalid);
    }
    Ok(())
}

fn validate_error_code(value: &str) -> Result<(), WorkerDispatchError> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    {
        return Err(WorkerDispatchError::InvalidConfiguration);
    }
    Ok(())
}
