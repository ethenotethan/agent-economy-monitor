use std::{
    collections::HashMap,
    env,
    fmt::{Display, Formatter},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use argon2::{Argon2, PasswordHash, PasswordVerifier};
use async_trait::async_trait;
use axum::{
    Json, Router,
    body::to_bytes,
    extract::{Request, State},
    http::{HeaderMap, HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::post,
};
use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD};
use rand_core::{OsRng, RngCore};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio_postgres::Client;
use zeroize::Zeroize;

const COOKIE_NAME: &str = "__Host-aem_session";
const CSRF_HEADER: &str = "x-csrf-token";
const MAX_SESSION_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const DEFAULT_SESSION_TTL: Duration = Duration::from_secs(15 * 60);
const MAX_SESSIONS: usize = 1_024;
const LOGIN_WINDOW: Duration = Duration::from_secs(60);
const MAX_LOGIN_FAILURES: usize = 5;

#[derive(Debug)]
pub struct AuthConfigError(&'static str);

impl Display for AuthConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

impl std::error::Error for AuthConfigError {}

#[derive(Clone)]
pub struct AuthConfig {
    password_hash: String,
    session_ttl: Duration,
}

impl AuthConfig {
    pub fn new(password_hash: String, session_ttl: Duration) -> Result<Self, AuthConfigError> {
        let parsed = PasswordHash::new(&password_hash)
            .map_err(|_| AuthConfigError("COCKPIT_PASSWORD_HASH must be a valid PHC string"))?;
        if parsed.algorithm.as_str() != "argon2id" {
            return Err(AuthConfigError("COCKPIT_PASSWORD_HASH must use Argon2id"));
        }
        let mut salt_bytes = [0_u8; 64];
        let has_supported_salt = parsed
            .salt
            .and_then(|salt| salt.decode_b64(&mut salt_bytes).ok())
            .is_some_and(|salt| salt.len() >= 16);
        if parsed.version != Some(19)
            || parsed.params.get_decimal("m").unwrap_or(0) < 19_456
            || parsed.params.get_decimal("m").unwrap_or(u32::MAX) > 262_144
            || parsed.params.get_decimal("t").unwrap_or(0) < 2
            || parsed.params.get_decimal("t").unwrap_or(u32::MAX) > 10
            || parsed.params.get_decimal("p").unwrap_or(0) < 1
            || parsed.params.get_decimal("p").unwrap_or(u32::MAX) > 4
            || !has_supported_salt
            || parsed.hash.is_none_or(|hash| hash.len() < 32)
        {
            return Err(AuthConfigError(
                "COCKPIT_PASSWORD_HASH Argon2id parameters are outside the supported bounds",
            ));
        }
        if session_ttl.is_zero() || session_ttl > MAX_SESSION_TTL {
            return Err(AuthConfigError(
                "SESSION_TTL_SECONDS must be between 1 and 86400",
            ));
        }
        Ok(Self {
            password_hash,
            session_ttl,
        })
    }

    pub fn from_env() -> Result<Self, AuthConfigError> {
        let password_hash = env::var("COCKPIT_PASSWORD_HASH")
            .map_err(|_| AuthConfigError("COCKPIT_PASSWORD_HASH is required"))?;
        let session_ttl = env::var("SESSION_TTL_SECONDS")
            .map(|value| {
                value
                    .parse::<u64>()
                    .map(Duration::from_secs)
                    .map_err(|_| AuthConfigError("SESSION_TTL_SECONDS must be an integer"))
            })
            .unwrap_or(Ok(DEFAULT_SESSION_TTL))?;
        Self::new(password_hash, session_ttl)
    }
}

#[derive(Clone)]
pub struct AuthState {
    config: AuthConfig,
    store: Arc<dyn AuthStore>,
}

#[derive(Debug)]
pub struct AuthStoreError;

impl Display for AuthStoreError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("authentication state is unavailable")
    }
}

impl std::error::Error for AuthStoreError {}

#[async_trait]
pub trait AuthStore: Send + Sync {
    async fn begin_login(&self, reservation: &str) -> Result<bool, AuthStoreError>;
    async fn finish_login(&self, reservation: &str, succeeded: bool) -> Result<(), AuthStoreError>;
    async fn create_session(
        &self,
        token_hash: &str,
        csrf_hash: &str,
        ttl: Duration,
    ) -> Result<(), AuthStoreError>;
    async fn authorize(
        &self,
        token_hash: &str,
        csrf_hash: Option<&str>,
    ) -> Result<bool, AuthStoreError>;
    async fn remove_session(&self, token_hash: &str) -> Result<(), AuthStoreError>;
}

#[derive(Default)]
pub struct InMemoryAuthStore {
    state: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    sessions: HashMap<String, Session>,
    login_attempts: LoginAttempts,
}

#[derive(Default)]
struct LoginAttempts {
    attempts: HashMap<String, LoginAttempt>,
}

struct LoginAttempt {
    started_at: Instant,
    failed: bool,
}

struct Session {
    csrf_hash: String,
    created_at: Instant,
    expires_at: Instant,
}

#[async_trait]
impl AuthStore for InMemoryAuthStore {
    async fn begin_login(&self, reservation: &str) -> Result<bool, AuthStoreError> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state
            .login_attempts
            .attempts
            .retain(|_, attempt| now.duration_since(attempt.started_at) < LOGIN_WINDOW);
        if state.login_attempts.attempts.len() >= MAX_LOGIN_FAILURES {
            return Ok(false);
        }
        state.login_attempts.attempts.insert(
            reservation.to_owned(),
            LoginAttempt {
                started_at: now,
                failed: false,
            },
        );
        Ok(true)
    }

    async fn finish_login(&self, reservation: &str, succeeded: bool) -> Result<(), AuthStoreError> {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if succeeded {
            state.login_attempts.attempts.clear();
        } else if let Some(attempt) = state.login_attempts.attempts.get_mut(reservation) {
            attempt.failed = true;
        }
        Ok(())
    }

    async fn create_session(
        &self,
        token_hash: &str,
        csrf_hash: &str,
        ttl: Duration,
    ) -> Result<(), AuthStoreError> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sessions.retain(|_, session| session.expires_at > now);
        if state.sessions.len() >= MAX_SESSIONS
            && let Some(oldest) = state
                .sessions
                .iter()
                .min_by_key(|(_, session)| session.created_at)
                .map(|(token, _)| token.clone())
        {
            state.sessions.remove(&oldest);
        }
        state.sessions.insert(
            token_hash.to_owned(),
            Session {
                csrf_hash: csrf_hash.to_owned(),
                created_at: now,
                expires_at: now + ttl,
            },
        );
        Ok(())
    }

    async fn authorize(
        &self,
        token_hash: &str,
        csrf_hash: Option<&str>,
    ) -> Result<bool, AuthStoreError> {
        let now = Instant::now();
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        state.sessions.retain(|_, session| session.expires_at > now);
        Ok(state.sessions.get(token_hash).is_some_and(|session| {
            csrf_hash.is_none_or(|csrf_hash| {
                constant_time_eq(csrf_hash.as_bytes(), session.csrf_hash.as_bytes())
            })
        }))
    }

    async fn remove_session(&self, token_hash: &str) -> Result<(), AuthStoreError> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .sessions
            .remove(token_hash);
        Ok(())
    }
}

pub struct PostgresAuthStore {
    client: Arc<Client>,
    namespace_id: String,
}

impl PostgresAuthStore {
    pub fn new(client: Arc<Client>, namespace_id: String) -> Self {
        Self {
            client,
            namespace_id,
        }
    }
}

#[async_trait]
impl AuthStore for PostgresAuthStore {
    async fn begin_login(&self, _reservation: &str) -> Result<bool, AuthStoreError> {
        let row = self
            .client
            .query_opt(
                "INSERT INTO agent_economy.auth_login_limits \
                   (namespace_id, window_started_at, attempt_count) \
                 VALUES ($1::uuid, clock_timestamp(), 1) \
                 ON CONFLICT (namespace_id) DO UPDATE SET \
                   window_started_at = CASE \
                     WHEN auth_login_limits.window_started_at \
                          <= clock_timestamp() - interval '60 seconds' \
                     THEN clock_timestamp() \
                     ELSE auth_login_limits.window_started_at END, \
                   attempt_count = CASE \
                     WHEN auth_login_limits.window_started_at \
                          <= clock_timestamp() - interval '60 seconds' \
                     THEN 1 ELSE auth_login_limits.attempt_count + 1 END \
                 WHERE auth_login_limits.window_started_at \
                         <= clock_timestamp() - interval '60 seconds' \
                    OR auth_login_limits.attempt_count < 5 \
                 RETURNING attempt_count",
                &[&self.namespace_id],
            )
            .await
            .map_err(|_| AuthStoreError)?;
        Ok(row.is_some())
    }

    async fn finish_login(
        &self,
        _reservation: &str,
        succeeded: bool,
    ) -> Result<(), AuthStoreError> {
        if succeeded {
            self.client
                .execute(
                    "UPDATE agent_economy.auth_login_limits \
                     SET window_started_at = clock_timestamp(), attempt_count = 0 \
                     WHERE namespace_id = $1::uuid",
                    &[&self.namespace_id],
                )
                .await
                .map_err(|_| AuthStoreError)?;
        }
        Ok(())
    }

    async fn create_session(
        &self,
        token_hash: &str,
        csrf_hash: &str,
        ttl: Duration,
    ) -> Result<(), AuthStoreError> {
        let ttl_seconds = i64::try_from(ttl.as_secs()).map_err(|_| AuthStoreError)?;
        self.client
            .execute(
                "WITH purged AS (\
                   DELETE FROM agent_economy.auth_sessions \
                   WHERE namespace_id = $1::uuid AND expires_at <= clock_timestamp()\
                 ), trimmed AS (\
                   DELETE FROM agent_economy.auth_sessions \
                   WHERE namespace_id = $1::uuid AND token_hash IN (\
                     SELECT token_hash FROM agent_economy.auth_sessions \
                     WHERE namespace_id = $1::uuid \
                     ORDER BY created_at DESC, token_hash DESC OFFSET 1023\
                   )\
                 ) \
                 INSERT INTO agent_economy.auth_sessions \
                   (namespace_id, token_hash, csrf_hash, created_at, expires_at) \
                 VALUES ($1::uuid, $2, $3, clock_timestamp(), \
                         clock_timestamp() + make_interval(secs => $4::double precision))",
                &[&self.namespace_id, &token_hash, &csrf_hash, &ttl_seconds],
            )
            .await
            .map_err(|_| AuthStoreError)?;
        Ok(())
    }

    async fn authorize(
        &self,
        token_hash: &str,
        csrf_hash: Option<&str>,
    ) -> Result<bool, AuthStoreError> {
        let row = self
            .client
            .query_opt(
                "WITH purged AS (\
                   DELETE FROM agent_economy.auth_sessions \
                   WHERE namespace_id = $1::uuid AND expires_at <= clock_timestamp()\
                 ) \
                 SELECT 1 FROM agent_economy.auth_sessions \
                 WHERE namespace_id = $1::uuid AND token_hash = $2 \
                   AND ($3::text IS NULL OR csrf_hash = $3) \
                   AND expires_at > clock_timestamp()",
                &[&self.namespace_id, &token_hash, &csrf_hash],
            )
            .await
            .map_err(|_| AuthStoreError)?;
        Ok(row.is_some())
    }

    async fn remove_session(&self, token_hash: &str) -> Result<(), AuthStoreError> {
        self.client
            .execute(
                "DELETE FROM agent_economy.auth_sessions \
                 WHERE namespace_id = $1::uuid AND token_hash = $2",
                &[&self.namespace_id, &token_hash],
            )
            .await
            .map_err(|_| AuthStoreError)?;
        Ok(())
    }
}

impl AuthState {
    pub fn new(config: AuthConfig) -> Self {
        Self::with_store(config, Arc::new(InMemoryAuthStore::default()))
    }

    pub fn with_store(config: AuthConfig, store: Arc<dyn AuthStore>) -> Self {
        Self { config, store }
    }

    pub fn from_env(store: Arc<dyn AuthStore>) -> Result<Self, AuthConfigError> {
        Ok(Self::with_store(AuthConfig::from_env()?, store))
    }

    async fn authorize(
        &self,
        headers: &HeaderMap,
        require_csrf: bool,
    ) -> Result<String, StatusCode> {
        let token = cookie_value(headers, COOKIE_NAME).ok_or(StatusCode::UNAUTHORIZED)?;
        let csrf_hash = if require_csrf {
            let supplied = headers
                .get(CSRF_HEADER)
                .and_then(|value| value.to_str().ok())
                .ok_or(StatusCode::FORBIDDEN)?;
            Some(secret_hash(supplied))
        } else {
            None
        };
        let authorized = self
            .store
            .authorize(&secret_hash(&token), csrf_hash.as_deref())
            .await
            .map_err(|_| StatusCode::SERVICE_UNAVAILABLE)?;
        authorized.then_some(token).ok_or(StatusCode::UNAUTHORIZED)
    }
}

#[derive(Deserialize)]
struct LoginRequest {
    password: String,
}

#[derive(Serialize)]
struct LoginResponse {
    csrf_token: String,
    expires_in_seconds: u64,
}

pub fn protect_router(protected: Router, state: AuthState) -> Router {
    let protected = protected.route_layer(middleware::from_fn_with_state(
        state.clone(),
        require_session,
    ));
    let app: Router<AuthState> = protected.with_state(());
    app.route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .with_state(state)
}

async fn login(State(state): State<AuthState>, request: Request) -> Response {
    let reservation = random_token();
    match state.store.begin_login(&reservation).await {
        Ok(true) => {}
        Ok(false) => return auth_error(StatusCode::TOO_MANY_REQUESTS, Some(LOGIN_WINDOW)),
        Err(_) => return auth_error(StatusCode::SERVICE_UNAVAILABLE, None),
    };
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if !content_type
        .split(';')
        .next()
        .is_some_and(|value| value.trim().eq_ignore_ascii_case("application/json"))
    {
        if state.store.finish_login(&reservation, false).await.is_err() {
            return auth_error(StatusCode::SERVICE_UNAVAILABLE, None);
        }
        return auth_error(StatusCode::UNSUPPORTED_MEDIA_TYPE, None);
    }
    let mut body = match to_bytes(request.into_body(), 1_024).await {
        Ok(body) => body.to_vec(),
        Err(_) => {
            if state.store.finish_login(&reservation, false).await.is_err() {
                return auth_error(StatusCode::SERVICE_UNAVAILABLE, None);
            }
            return auth_error(StatusCode::PAYLOAD_TOO_LARGE, None);
        }
    };
    let mut request: LoginRequest = match serde_json::from_slice(&body) {
        Ok(request) => {
            body.zeroize();
            request
        }
        Err(_) => {
            body.zeroize();
            if state.store.finish_login(&reservation, false).await.is_err() {
                return auth_error(StatusCode::SERVICE_UNAVAILABLE, None);
            }
            return auth_error(StatusCode::BAD_REQUEST, None);
        }
    };
    let hash = state.config.password_hash.clone();
    let mut password = std::mem::take(&mut request.password);
    let verified = tokio::task::spawn_blocking(move || {
        let result = PasswordHash::new(&hash).ok().is_some_and(|parsed| {
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok()
        });
        password.zeroize();
        result
    })
    .await
    .unwrap_or(false);
    if state
        .store
        .finish_login(&reservation, verified)
        .await
        .is_err()
    {
        return auth_error(StatusCode::SERVICE_UNAVAILABLE, None);
    }

    if !verified {
        return auth_error(StatusCode::UNAUTHORIZED, None);
    }
    let token = random_token();
    let csrf_token = random_token();
    if state
        .store
        .create_session(
            &secret_hash(&token),
            &secret_hash(&csrf_token),
            state.config.session_ttl,
        )
        .await
        .is_err()
    {
        return auth_error(StatusCode::SERVICE_UNAVAILABLE, None);
    }
    let max_age = state.config.session_ttl.as_secs();
    let cookie = format!(
        "{COOKIE_NAME}={token}; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age={max_age}"
    );
    let mut response = Json(LoginResponse {
        csrf_token,
        expires_in_seconds: max_age,
    })
    .into_response();
    response
        .headers_mut()
        .insert(header::SET_COOKIE, HeaderValue::from_str(&cookie).unwrap());
    no_store(response)
}

async fn logout(State(state): State<AuthState>, headers: HeaderMap) -> Response {
    let token = match state.authorize(&headers, true).await {
        Ok(token) => token,
        Err(status) => return auth_error(status, None),
    };
    if state
        .store
        .remove_session(&secret_hash(&token))
        .await
        .is_err()
    {
        return auth_error(StatusCode::SERVICE_UNAVAILABLE, None);
    }
    let mut response = StatusCode::NO_CONTENT.into_response();
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static(
            "__Host-aem_session=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0",
        ),
    );
    no_store(response)
}

async fn require_session(State(state): State<AuthState>, request: Request, next: Next) -> Response {
    let require_csrf = !matches!(
        *request.method(),
        Method::GET | Method::HEAD | Method::OPTIONS
    );
    match state.authorize(request.headers(), require_csrf).await {
        Ok(_) => next.run(request).await,
        Err(status) => auth_error(status, None),
    }
}

fn auth_error(status: StatusCode, retry_after: Option<Duration>) -> Response {
    let code = match status {
        StatusCode::FORBIDDEN => "csrf_rejected",
        StatusCode::TOO_MANY_REQUESTS => "login_rate_limited",
        StatusCode::BAD_REQUEST
        | StatusCode::PAYLOAD_TOO_LARGE
        | StatusCode::UNSUPPORTED_MEDIA_TYPE => "invalid_login_request",
        _ => "authentication_required",
    };
    let mut response = (status, Json(json!({"error": {"code": code}}))).into_response();
    if let Some(duration) = retry_after {
        response.headers_mut().insert(
            header::RETRY_AFTER,
            HeaderValue::from_str(&duration.as_secs().to_string()).unwrap(),
        );
    }
    no_store(response)
}

fn no_store(mut response: Response) -> Response {
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));
    response
}

fn random_token() -> String {
    let mut bytes = [0_u8; 32];
    OsRng.fill_bytes(&mut bytes);
    URL_SAFE_NO_PAD.encode(bytes)
}

fn secret_hash(secret: &str) -> String {
    format!("{:x}", Sha256::digest(secret.as_bytes()))
}

fn cookie_value(headers: &HeaderMap, name: &str) -> Option<String> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|cookie| cookie.trim().split_once('='))
        .find_map(|(cookie_name, value)| (cookie_name == name).then(|| value.to_owned()))
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}
