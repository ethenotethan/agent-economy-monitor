use std::sync::{Arc, Mutex};

use agent_economy_monitor::{
    projection::{
        Citation, ProjectionApproval, ProjectionFact, ProjectionInput, ProjectionJob, Snapshot,
    },
    projection_gateway::{
        ProjectionGatewayError, ProjectionGatewayState, ProjectionGatewayStore,
        projection_gateway_router,
    },
};
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use serde_json::json;
use sha2::{Digest, Sha256};
use tower::ServiceExt;

#[derive(Clone)]
struct FixtureStore {
    published: Arc<Mutex<Vec<Vec<u8>>>>,
}

fn job() -> ProjectionJob {
    ProjectionJob {
        job_id: "00000000-0000-0000-0000-000000000017".into(),
        stable_entity_id: "service:weather".into(),
        page_path: "services/weather.md".into(),
        model_id: "projector-v1".into(),
        model_sha256: "a".repeat(64),
        prompt_sha256: "b".repeat(64),
        snapshot_sha256: "c".repeat(64),
        destination: "gcs://agent-economy-projections/namespaces/test/weather.json".into(),
    }
}

fn snapshot() -> Snapshot {
    let citation = Citation {
        stable_id: "evidence:weather".into(),
        evidence_sha256: "d".repeat(64),
    };
    Snapshot {
        bytes: br#"{"bounded":true}"#.to_vec(),
        sha256: "c".repeat(64),
        projection_input: ProjectionInput {
            stable_entity_id: "service:weather".into(),
            facts: vec![ProjectionFact {
                name: "availability".into(),
                value: json!("online"),
                citation: citation.clone(),
            }],
            citations: vec![citation],
        },
        private_fragments: vec!["private-canary".into()],
    }
}

#[async_trait]
impl ProjectionGatewayStore for FixtureStore {
    async fn lease_next(
        &self,
        _lease_owner: &str,
    ) -> Result<Option<ProjectionJob>, ProjectionGatewayError> {
        Ok(Some(job()))
    }

    async fn snapshot(
        &self,
        job_id: &str,
        _lease_owner: &str,
    ) -> Result<Option<Snapshot>, ProjectionGatewayError> {
        Ok((job_id == job().job_id).then(snapshot))
    }

    async fn approval(
        &self,
        job_id: &str,
        candidate_sha256: &str,
        _lease_owner: &str,
    ) -> Result<Option<ProjectionApproval>, ProjectionGatewayError> {
        Ok((job_id == job().job_id).then(|| ProjectionApproval {
            candidate_sha256: candidate_sha256.into(),
            approved_by: "owner".into(),
            approved_at: "2026-10-02T00:00:00Z".into(),
        }))
    }

    async fn publish(
        &self,
        job_id: &str,
        bundle_sha256: &str,
        bundle_bytes: &[u8],
        _lease_owner: &str,
    ) -> Result<(), ProjectionGatewayError> {
        assert_eq!(job_id, job().job_id);
        assert_eq!(bundle_sha256, format!("{:x}", Sha256::digest(bundle_bytes)));
        self.published.lock().unwrap().push(bundle_bytes.to_vec());
        Ok(())
    }
}

fn request(method: &str, uri: &str, authorized: bool, body: Body) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if authorized {
        builder = builder.header("authorization", "Bearer gateway-token");
    }
    builder.body(body).unwrap()
}

#[tokio::test]
async fn gateway_routes_require_bearer_auth_and_expose_the_complete_flow() {
    let published = Arc::new(Mutex::new(Vec::new()));
    let app = projection_gateway_router(ProjectionGatewayState::new(
        Arc::new(FixtureStore {
            published: published.clone(),
        }),
        "gateway-token",
        "worker-1",
    ));

    let unauthorized = app
        .clone()
        .oneshot(request(
            "GET",
            "/api/v1/projection/jobs/next",
            false,
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(unauthorized.status(), StatusCode::UNAUTHORIZED);

    let leased = app
        .clone()
        .oneshot(request(
            "GET",
            "/api/v1/projection/jobs/next",
            true,
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(leased.status(), StatusCode::OK);
    let leased_body = leased.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        serde_json::from_slice::<ProjectionJob>(&leased_body).unwrap(),
        job()
    );

    let snapshot_response = app
        .clone()
        .oneshot(request(
            "GET",
            &format!("/api/v1/projection/jobs/{}/snapshot", job().job_id),
            true,
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(snapshot_response.status(), StatusCode::OK);

    let approval_response = app
        .clone()
        .oneshot(request(
            "GET",
            &format!(
                "/api/v1/projection/jobs/{}/approvals/{}",
                job().job_id,
                "f".repeat(64)
            ),
            true,
            Body::empty(),
        ))
        .await
        .unwrap();
    assert_eq!(approval_response.status(), StatusCode::OK);

    let bundle = br#"{"payload":{},"payload_sha256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff","approval":{}}"#;
    let bundle_sha256 = format!("{:x}", Sha256::digest(bundle));
    let mut publication_request = request(
        "PUT",
        &format!(
            "/api/v1/projection/jobs/{}/bundles/{}",
            job().job_id,
            bundle_sha256
        ),
        true,
        Body::from(bundle.as_slice()),
    );
    publication_request
        .headers_mut()
        .insert("if-none-match", "*".parse().unwrap());
    let publication = app.oneshot(publication_request).await.unwrap();
    assert_eq!(publication.status(), StatusCode::NO_CONTENT);
    assert_eq!(published.lock().unwrap().as_slice(), [bundle.as_slice()]);
}
