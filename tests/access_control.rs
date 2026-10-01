use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use agent_economy_monitor::{
    auth::{AuthConfig, AuthState, InMemoryAuthStore, protect_router},
    query::{
        BuyerDossierReadModel, DashboardPage, Fact, GraphReadModel, ProjectionPageList,
        ProvenanceReadModel, QueryError, QueryStore, SystemReadModel, api_router,
    },
};
use argon2::{Algorithm, Argon2, Params, PasswordHasher, Version, password_hash::SaltString};
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tower::ServiceExt;

const PASSWORD: &str = "correct horse battery staple";

struct CountingStore {
    calls: Arc<AtomicUsize>,
}

fn fact() -> Fact {
    Fact {
        id: "buyer:1".into(),
        kind: "buyer".into(),
        label: "Buyer 1".into(),
        value: json!({}),
        observed_at: "2026-09-30T00:00:00Z".into(),
        provenance_ids: vec!["11111111-1111-1111-1111-111111111111".into()],
    }
}

#[async_trait]
impl QueryStore for CountingStore {
    async fn pulse(&self) -> Result<Vec<Fact>, QueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(vec![fact()])
    }

    async fn buyers(
        &self,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(DashboardPage::new(vec![fact()], None))
    }

    async fn buyer(&self, _id: &str) -> Result<Option<Fact>, QueryError> {
        unreachable!()
    }

    async fn buyer_dossier(&self, _id: &str) -> Result<Option<BuyerDossierReadModel>, QueryError> {
        unreachable!()
    }

    async fn buyer_timeline(
        &self,
        _id: &str,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        unreachable!()
    }

    async fn services(
        &self,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        unreachable!()
    }

    async fn service(&self, _id: &str) -> Result<Option<Fact>, QueryError> {
        unreachable!()
    }

    async fn graph(
        &self,
        _kind: &str,
        _id: &str,
        _limit: usize,
    ) -> Result<GraphReadModel, QueryError> {
        unreachable!()
    }

    async fn provenance(&self, _id: &str) -> Result<Option<ProvenanceReadModel>, QueryError> {
        unreachable!()
    }

    async fn search(
        &self,
        _query: &str,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        unreachable!()
    }

    async fn investigations(
        &self,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<ProjectionPageList, QueryError> {
        unreachable!()
    }

    async fn system(&self) -> Result<SystemReadModel, QueryError> {
        unreachable!()
    }
}

fn auth_config() -> AuthConfig {
    let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
    let hash = Argon2::default()
        .hash_password(PASSWORD.as_bytes(), &salt)
        .unwrap()
        .to_string();
    AuthConfig::new(hash, Duration::from_secs(15 * 60)).unwrap()
}

fn auth_state() -> AuthState {
    AuthState::new(auth_config())
}

fn app(calls: Arc<AtomicUsize>) -> axum::Router {
    protect_router(api_router(Arc::new(CountingStore { calls })), auth_state())
}

fn app_with_auth(calls: Arc<AtomicUsize>, auth: AuthState) -> axum::Router {
    protect_router(api_router(Arc::new(CountingStore { calls })), auth)
}

async fn login(app: &axum::Router, password: &str) -> axum::response::Response {
    let request = Request::builder()
        .method("POST")
        .uri("/auth/login")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&json!({"password": password})).unwrap(),
        ))
        .unwrap();
    app.clone().oneshot(request).await.unwrap()
}

#[tokio::test]
async fn protected_api_rejects_before_extractors_and_store_access() {
    let calls = Arc::new(AtomicUsize::new(0));
    let response = app(calls.clone())
        .oneshot(
            Request::builder()
                .uri("/api/v1/buyers?limit=not-a-number")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn login_issues_secure_bounded_session_and_logout_requires_csrf() {
    let calls = Arc::new(AtomicUsize::new(0));
    let app = app(calls.clone());
    let response = login(&app, PASSWORD).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");

    let set_cookie = response.headers()[header::SET_COOKIE].to_str().unwrap();
    assert!(set_cookie.starts_with("__Host-aem_session="));
    assert!(set_cookie.contains("; Path=/"));
    assert!(set_cookie.contains("; Secure"));
    assert!(set_cookie.contains("; HttpOnly"));
    assert!(set_cookie.contains("; SameSite=Strict"));
    assert!(set_cookie.contains("; Max-Age=900"));
    let cookie = set_cookie.split(';').next().unwrap().to_owned();
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    let csrf = body["csrf_token"].as_str().unwrap();
    assert!(csrf.len() >= 32);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/pulse")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(calls.load(Ordering::SeqCst), 1);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/logout")
                .header(header::COOKIE, &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::FORBIDDEN);

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/auth/logout")
                .header(header::COOKIE, &cookie)
                .header("x-csrf-token", csrf)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(
        response.headers()[header::SET_COOKIE]
            .to_str()
            .unwrap()
            .contains("Max-Age=0")
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/pulse")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn repeated_bad_passwords_are_rate_limited() {
    let app = app(Arc::new(AtomicUsize::new(0)));
    for _ in 0..5 {
        assert_eq!(
            login(&app, "wrong password").await.status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let response = login(&app, PASSWORD).await;
    assert_eq!(response.status(), StatusCode::TOO_MANY_REQUESTS);
    assert_eq!(response.headers()[header::RETRY_AFTER], "60");
}

#[tokio::test]
async fn concurrent_bad_passwords_cannot_bypass_the_attempt_budget() {
    let app = app(Arc::new(AtomicUsize::new(0)));
    let attempts = (0..12)
        .map(|_| {
            let app = app.clone();
            tokio::spawn(async move { login(&app, "wrong password").await.status() })
        })
        .collect::<Vec<_>>();
    let mut unauthorized = 0;
    let mut rate_limited = 0;
    for attempt in attempts {
        match attempt.await.unwrap() {
            StatusCode::UNAUTHORIZED => unauthorized += 1,
            StatusCode::TOO_MANY_REQUESTS => rate_limited += 1,
            status => panic!("unexpected login status {status}"),
        }
    }
    assert_eq!(unauthorized, 5);
    assert_eq!(rate_limited, 7);
}

#[tokio::test]
async fn sessions_and_rate_limits_are_shared_across_runtime_instances() {
    let store = Arc::new(InMemoryAuthStore::default());
    let first = app_with_auth(
        Arc::new(AtomicUsize::new(0)),
        AuthState::with_store(auth_config(), store.clone()),
    );
    let second = app_with_auth(
        Arc::new(AtomicUsize::new(0)),
        AuthState::with_store(auth_config(), store.clone()),
    );

    let response = login(&first, PASSWORD).await;
    let cookie = response.headers()[header::SET_COOKIE]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let response = second
        .oneshot(
            Request::builder()
                .uri("/api/v1/pulse")
                .header(header::COOKIE, cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    for _ in 0..5 {
        let instance = app_with_auth(
            Arc::new(AtomicUsize::new(0)),
            AuthState::with_store(auth_config(), store.clone()),
        );
        assert_eq!(
            login(&instance, "wrong password").await.status(),
            StatusCode::UNAUTHORIZED
        );
    }
    let instance = app_with_auth(
        Arc::new(AtomicUsize::new(0)),
        AuthState::with_store(auth_config(), store),
    );
    assert_eq!(
        login(&instance, PASSWORD).await.status(),
        StatusCode::TOO_MANY_REQUESTS
    );
}

#[tokio::test]
async fn login_body_is_bounded_before_password_verification() {
    let app = app(Arc::new(AtomicUsize::new(0)));
    let response = login(&app, &"x".repeat(2_048)).await;
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[test]
fn configuration_requires_argon2id_and_bounded_session_lifetime() {
    assert!(AuthConfig::new("not-a-password-hash".into(), Duration::from_secs(900)).is_err());
    assert!(
        AuthConfig::new(
            "$argon2i$v=19$m=19456,t=2,p=1$c29tZXNhbHQ$KkV8dVQJ4h9uE+K7VqF0HQ".into(),
            Duration::from_secs(900),
        )
        .is_err()
    );

    let salt = SaltString::encode_b64(b"0123456789abcdef").unwrap();
    let weak_hash = Argon2::new(
        Algorithm::Argon2id,
        Version::V0x13,
        Params::new(8, 1, 1, None).unwrap(),
    )
    .hash_password(PASSWORD.as_bytes(), &salt)
    .unwrap()
    .to_string();
    assert!(AuthConfig::new(weak_hash, Duration::from_secs(900)).is_err());
    let hash = Argon2::default()
        .hash_password(PASSWORD.as_bytes(), &salt)
        .unwrap()
        .to_string();
    let digestless_hash = hash.rsplit_once('$').unwrap().0.to_owned();
    assert!(AuthConfig::new(digestless_hash, Duration::from_secs(900)).is_err());
    let short_salt_hash = hash.replace(salt.as_str(), "YWJj");
    assert!(AuthConfig::new(short_salt_hash, Duration::from_secs(900)).is_err());
    let excessive_memory_hash = hash.replace("m=19456", "m=262145");
    assert!(AuthConfig::new(excessive_memory_hash, Duration::from_secs(900)).is_err());
    assert!(AuthConfig::new(hash.clone(), Duration::ZERO).is_err());
    assert!(AuthConfig::new(hash, Duration::from_secs(24 * 60 * 60 + 1)).is_err());
}
