use std::{fmt, sync::Arc, time::Duration};

use async_trait::async_trait;
use axum::{
    Json, Router,
    body::Bytes,
    extract::{Extension, Path},
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, put},
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::projection::{ProjectionApproval, ProjectionJob, Snapshot};

const MAX_BUNDLE_BYTES: usize = 1024 * 1024;
const LEASE_SECONDS: i64 = 300;

#[derive(Debug)]
pub enum ProjectionGatewayError {
    Invalid,
    Unauthorized,
    NotFound,
    Conflict,
    Unavailable,
}

impl fmt::Display for ProjectionGatewayError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Invalid => "invalid projection gateway request",
            Self::Unauthorized => "projection gateway authentication required",
            Self::NotFound => "projection gateway resource not found",
            Self::Conflict => "projection publication conflicts with existing bytes",
            Self::Unavailable => "projection gateway store unavailable",
        })
    }
}

impl std::error::Error for ProjectionGatewayError {}

impl IntoResponse for ProjectionGatewayError {
    fn into_response(self) -> Response {
        let status = match self {
            Self::Invalid => StatusCode::BAD_REQUEST,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::NotFound => StatusCode::NOT_FOUND,
            Self::Conflict => StatusCode::PRECONDITION_FAILED,
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        };
        status.into_response()
    }
}

#[async_trait]
pub trait ProjectionGatewayStore: Send + Sync + 'static {
    async fn lease_next(
        &self,
        lease_owner: &str,
    ) -> Result<Option<ProjectionJob>, ProjectionGatewayError>;
    async fn snapshot(
        &self,
        job_id: &str,
        lease_owner: &str,
    ) -> Result<Option<Snapshot>, ProjectionGatewayError>;
    async fn approval(
        &self,
        job_id: &str,
        candidate_sha256: &str,
        lease_owner: &str,
    ) -> Result<Option<ProjectionApproval>, ProjectionGatewayError>;
    async fn publish(
        &self,
        job_id: &str,
        bundle_sha256: &str,
        bundle_bytes: &[u8],
        lease_owner: &str,
    ) -> Result<(), ProjectionGatewayError>;
}

#[derive(Clone)]
pub struct ProjectionGatewayState {
    store: Arc<dyn ProjectionGatewayStore>,
    token_sha256: [u8; 32],
    lease_owner: Arc<str>,
}

impl ProjectionGatewayState {
    pub fn new(
        store: Arc<dyn ProjectionGatewayStore>,
        bearer_token: &str,
        lease_owner: &str,
    ) -> Self {
        Self {
            store,
            token_sha256: Sha256::digest(bearer_token.as_bytes()).into(),
            lease_owner: Arc::from(lease_owner),
        }
    }

    fn authorize(&self, headers: &HeaderMap) -> Result<(), StatusCode> {
        let supplied = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .ok_or(StatusCode::UNAUTHORIZED)?;
        let supplied: [u8; 32] = Sha256::digest(supplied.as_bytes()).into();
        let difference = supplied
            .iter()
            .zip(self.token_sha256)
            .fold(0_u8, |difference, (left, right)| {
                difference | (left ^ right)
            });
        (difference == 0)
            .then_some(())
            .ok_or(StatusCode::UNAUTHORIZED)
    }
}

pub fn projection_gateway_router(state: ProjectionGatewayState) -> Router {
    mount_projection_gateway(Router::new(), state)
}

#[rustfmt::skip]
pub fn mount_projection_gateway(router: Router, state: ProjectionGatewayState) -> Router {
    router
        .route("/api/v1/projection/jobs/next", get(pull_job))
        .route("/api/v1/projection/jobs/{job_id}/snapshot", get(fetch_snapshot))
        .route("/api/v1/projection/jobs/{job_id}/approvals/{candidate_sha256}", get(fetch_approval))
        .route("/api/v1/projection/jobs/{job_id}/bundles/{bundle_sha256}", put(publish_bundle))
        .layer(Extension(state))
}

async fn pull_job(
    Extension(state): Extension<ProjectionGatewayState>,
    headers: HeaderMap,
) -> Result<Response, ProjectionGatewayError> {
    state
        .authorize(&headers)
        .map_err(|_| ProjectionGatewayError::Unauthorized)?;
    match state.store.lease_next(&state.lease_owner).await? {
        Some(job) => Ok(Json(job).into_response()),
        None => Ok(StatusCode::NO_CONTENT.into_response()),
    }
}

async fn fetch_snapshot(
    Extension(state): Extension<ProjectionGatewayState>,
    headers: HeaderMap,
    Path(job_id): Path<String>,
) -> Result<Json<Snapshot>, ProjectionGatewayError> {
    state
        .authorize(&headers)
        .map_err(|_| ProjectionGatewayError::Unauthorized)?;
    validate_id(&job_id)?;
    state
        .store
        .snapshot(&job_id, &state.lease_owner)
        .await?
        .map(Json)
        .ok_or(ProjectionGatewayError::NotFound)
}

async fn fetch_approval(
    Extension(state): Extension<ProjectionGatewayState>,
    headers: HeaderMap,
    Path((job_id, candidate_sha256)): Path<(String, String)>,
) -> Result<Json<ProjectionApproval>, ProjectionGatewayError> {
    state
        .authorize(&headers)
        .map_err(|_| ProjectionGatewayError::Unauthorized)?;
    validate_id(&job_id)?;
    validate_hash(&candidate_sha256)?;
    state
        .store
        .approval(&job_id, &candidate_sha256, &state.lease_owner)
        .await?
        .map(Json)
        .ok_or(ProjectionGatewayError::NotFound)
}

async fn publish_bundle(
    Extension(state): Extension<ProjectionGatewayState>,
    headers: HeaderMap,
    Path((job_id, bundle_sha256)): Path<(String, String)>,
    bundle_bytes: Bytes,
) -> Result<StatusCode, ProjectionGatewayError> {
    state
        .authorize(&headers)
        .map_err(|_| ProjectionGatewayError::Unauthorized)?;
    validate_id(&job_id)?;
    validate_hash(&bundle_sha256)?;
    if headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        != Some("*")
    {
        return Err(ProjectionGatewayError::Invalid);
    }
    if bundle_bytes.len() > MAX_BUNDLE_BYTES {
        return Err(ProjectionGatewayError::Invalid);
    }
    if format!("{:x}", Sha256::digest(&bundle_bytes)) != bundle_sha256 {
        return Err(ProjectionGatewayError::Invalid);
    }
    state
        .store
        .publish(&job_id, &bundle_sha256, &bundle_bytes, &state.lease_owner)
        .await?;
    Ok(StatusCode::NO_CONTENT)
}

fn validate_id(value: &str) -> Result<(), ProjectionGatewayError> {
    if value.is_empty()
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(ProjectionGatewayError::Invalid);
    }
    Ok(())
}

fn validate_hash(value: &str) -> Result<(), ProjectionGatewayError> {
    if value.len() != 64
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(ProjectionGatewayError::Invalid);
    }
    Ok(())
}

pub struct PostgresProjectionGatewayStore {
    client: Mutex<Client>,
    namespace_id: String,
}

impl PostgresProjectionGatewayStore {
    pub fn new(client: Client, namespace_id: String) -> Self {
        Self {
            client: Mutex::new(client),
            namespace_id,
        }
    }
}

#[async_trait]
impl ProjectionGatewayStore for PostgresProjectionGatewayStore {
    async fn lease_next(
        &self,
        lease_owner: &str,
    ) -> Result<Option<ProjectionJob>, ProjectionGatewayError> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "WITH candidate AS (\
                   SELECT namespace_id, job_id FROM agent_economy.projection_jobs \
                   WHERE namespace_id = $1::text::uuid \
                     AND (status = 'pending' OR (status = 'leased' AND lease_expires_at <= clock_timestamp())) \
                   ORDER BY created_at, job_id FOR UPDATE SKIP LOCKED LIMIT 1\
                 ) \
                 UPDATE agent_economy.projection_jobs AS job \
                 SET status = 'leased', lease_owner = $2, \
                     lease_expires_at = clock_timestamp() + ($3::bigint * interval '1 second'), \
                     attempt_count = attempt_count + 1, updated_at = clock_timestamp() \
                 FROM candidate \
                 WHERE job.namespace_id = candidate.namespace_id AND job.job_id = candidate.job_id \
                 RETURNING job.job_id::text, job.stable_entity_id, job.page_path, job.model_id, \
                           job.model_sha256, job.prompt_sha256, job.snapshot_sha256, job.destination",
                &[&self.namespace_id, &lease_owner, &LEASE_SECONDS],
            )
            .await
            .map_err(|_| ProjectionGatewayError::Unavailable)?;
        Ok(row.map(|row| ProjectionJob {
            job_id: row.get(0),
            stable_entity_id: row.get(1),
            page_path: row.get(2),
            model_id: row.get(3),
            model_sha256: row.get(4),
            prompt_sha256: row.get(5),
            snapshot_sha256: row.get(6),
            destination: row.get(7),
        }))
    }

    async fn snapshot(
        &self,
        job_id: &str,
        lease_owner: &str,
    ) -> Result<Option<Snapshot>, ProjectionGatewayError> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT snapshot.snapshot_payload::text \
                 FROM agent_economy.projection_jobs AS job \
                 JOIN agent_economy.projection_snapshots AS snapshot \
                   USING (namespace_id, snapshot_id, stable_entity_id, snapshot_sha256) \
                 WHERE job.namespace_id = $1::text::uuid AND job.job_id = $2::text::uuid \
                   AND job.status = 'leased' AND job.lease_owner = $3 \
                   AND job.lease_expires_at > clock_timestamp()",
                &[&self.namespace_id, &job_id, &lease_owner],
            )
            .await
            .map_err(|_| ProjectionGatewayError::Unavailable)?;
        row.map(|row| {
            serde_json::from_str(&row.get::<_, String>(0))
                .map_err(|_| ProjectionGatewayError::Unavailable)
        })
        .transpose()
    }

    async fn approval(
        &self,
        job_id: &str,
        candidate_sha256: &str,
        lease_owner: &str,
    ) -> Result<Option<ProjectionApproval>, ProjectionGatewayError> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT approval.approved_by, approval.approved_at::text \
                 FROM agent_economy.projection_approvals AS approval \
                 JOIN agent_economy.projection_jobs AS job USING (namespace_id, job_id) \
                 WHERE approval.namespace_id = $1::text::uuid \
                   AND approval.job_id = $2::text::uuid AND approval.candidate_sha256 = $3 \
                   AND job.status = 'leased' AND job.lease_owner = $4 \
                   AND job.lease_expires_at > clock_timestamp()",
                &[&self.namespace_id, &job_id, &candidate_sha256, &lease_owner],
            )
            .await
            .map_err(|_| ProjectionGatewayError::Unavailable)?;
        Ok(row.map(|row| ProjectionApproval {
            candidate_sha256: candidate_sha256.to_owned(),
            approved_by: row.get(0),
            approved_at: row.get(1),
        }))
    }

    async fn publish(
        &self,
        job_id: &str,
        bundle_sha256: &str,
        bundle_bytes: &[u8],
        lease_owner: &str,
    ) -> Result<(), ProjectionGatewayError> {
        let bundle: Value =
            serde_json::from_slice(bundle_bytes).map_err(|_| ProjectionGatewayError::Invalid)?;
        let payload_sha256 = bundle
            .get("payload_sha256")
            .and_then(Value::as_str)
            .ok_or(ProjectionGatewayError::Invalid)?;
        validate_hash(payload_sha256)?;
        let payload = bundle
            .get("payload")
            .ok_or(ProjectionGatewayError::Invalid)?;
        let changeset = payload
            .get("changeset")
            .ok_or(ProjectionGatewayError::Invalid)?;
        let page_revision_id = required_string(changeset, "page_revision_id")?;
        let page_sha256 = required_string(changeset, "page_sha256")?;
        let changeset_id = required_string(changeset, "id")?;
        let changeset_sha256 = required_string(changeset, "sha256")?;

        let mut client = self.client.lock().await;
        let transaction = client
            .transaction()
            .await
            .map_err(|_| ProjectionGatewayError::Unavailable)?;
        let job = transaction
            .query_opt(
                "SELECT status, lease_owner, lease_expires_at > clock_timestamp() \
                 FROM agent_economy.projection_jobs \
                 WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid \
                 FOR UPDATE",
                &[&self.namespace_id, &job_id],
            )
            .await
            .map_err(|_| ProjectionGatewayError::Unavailable)?
            .ok_or(ProjectionGatewayError::NotFound)?;
        let status: String = job.get(0);
        if status == "published" {
            let exact = transaction
                .query_opt(
                    "SELECT bundle_bytes = $4 \
                     FROM agent_economy.projection_publications \
                     WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid \
                       AND bundle_sha256 = $3",
                    &[&self.namespace_id, &job_id, &bundle_sha256, &bundle_bytes],
                )
                .await
                .map_err(|_| ProjectionGatewayError::Unavailable)?
                .is_some_and(|row| row.get::<_, bool>(0));
            return if exact {
                transaction
                    .commit()
                    .await
                    .map_err(|_| ProjectionGatewayError::Unavailable)
            } else {
                Err(ProjectionGatewayError::Conflict)
            };
        }
        let owns_live_lease = status == "leased"
            && job.get::<_, Option<String>>(1).as_deref() == Some(lease_owner)
            && job.get::<_, Option<bool>>(2) == Some(true);
        if !owns_live_lease {
            return Err(ProjectionGatewayError::Conflict);
        }
        let inserted = transaction
            .execute(
                "INSERT INTO agent_economy.projection_publications \
                   (namespace_id, job_id, payload_sha256, bundle_sha256, bundle, bundle_bytes, \
                    page_revision_id, page_sha256, changeset_id, changeset_sha256) \
                 VALUES ($1::text::uuid, $2::text::uuid, $3, $4, $5::text::jsonb, $6, $7, $8, $9, $10) \
                 ON CONFLICT (namespace_id, job_id, bundle_sha256) DO NOTHING",
                &[
                    &self.namespace_id,
                    &job_id,
                    &payload_sha256,
                    &bundle_sha256,
                    &String::from_utf8_lossy(bundle_bytes).as_ref(),
                    &bundle_bytes,
                    &page_revision_id,
                    &page_sha256,
                    &changeset_id,
                    &changeset_sha256,
                ],
            )
            .await
            .map_err(|_| ProjectionGatewayError::Invalid)?;
        if inserted == 0 {
            let exact = transaction
                .query_one(
                    "SELECT bundle_bytes = $4 \
                     FROM agent_economy.projection_publications \
                     WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid AND bundle_sha256 = $3",
                    &[&self.namespace_id, &job_id, &bundle_sha256, &bundle_bytes],
                )
                .await
                .map_err(|_| ProjectionGatewayError::Unavailable)?
                .get::<_, bool>(0);
            if !exact {
                return Err(ProjectionGatewayError::Conflict);
            }
        }
        let advanced = transaction
            .execute(
                "UPDATE agent_economy.projection_jobs \
                 SET status = 'published', lease_owner = NULL, lease_expires_at = NULL, \
                     updated_at = clock_timestamp() \
                 WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid \
                   AND status = 'leased' AND lease_owner = $3 \
                   AND lease_expires_at > clock_timestamp()",
                &[&self.namespace_id, &job_id, &lease_owner],
            )
            .await
            .map_err(|_| ProjectionGatewayError::Unavailable)?;
        if advanced != 1 {
            return Err(ProjectionGatewayError::Conflict);
        }
        transaction
            .commit()
            .await
            .map_err(|_| ProjectionGatewayError::Unavailable)
    }
}

fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ProjectionGatewayError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or(ProjectionGatewayError::Invalid)
}

pub fn lease_duration() -> Duration {
    Duration::from_secs(LEASE_SECONDS as u64)
}
