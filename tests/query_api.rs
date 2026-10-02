use std::sync::Arc;

use agent_economy_monitor::query::{
    BuyerDossierReadModel, ClassificationReadModel, DashboardPage, EvidenceProvenanceBinding, Fact,
    GraphReadModel, PostgresQueryStore, ProjectionPage, ProjectionPageList, ProvenanceReadModel,
    QueryError, QueryStore, SystemReadModel, api_router,
};
use async_trait::async_trait;
use axum::{
    body::Body,
    http::{Request, StatusCode, header},
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use tokio_postgres::NoTls;
use tower::ServiceExt;

#[derive(Default)]
struct FixtureStore;

fn fact(id: &str, kind: &str) -> Fact {
    Fact {
        id: id.into(),
        kind: kind.into(),
        label: id.into(),
        value: json!({"count": 3}),
        observed_at: "2026-09-30T00:00:00Z".into(),
        provenance_ids: vec!["11111111-1111-1111-1111-111111111111".into()],
    }
}

#[async_trait]
impl QueryStore for FixtureStore {
    async fn pulse(&self) -> Result<Vec<Fact>, QueryError> {
        Ok(vec![fact("pulse:1", "pulse_metric")])
    }

    async fn buyers(
        &self,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        Ok(DashboardPage::new(vec![fact("buyer:1", "buyer")], None))
    }

    async fn buyer(&self, id: &str) -> Result<Option<Fact>, QueryError> {
        Ok(Some(fact(id, "buyer")))
    }

    async fn buyer_dossier(&self, id: &str) -> Result<Option<BuyerDossierReadModel>, QueryError> {
        Ok(Some(BuyerDossierReadModel {
            buyer: fact(id, "buyer"),
            classifications: vec![ClassificationReadModel {
                claim_id: "claim:automation".into(),
                label: "automated-buyer".into(),
                status: "disputed".into(),
                confidence: "0.7300".into(),
                method: "rules@1".into(),
                evidence_window_start: "2026-09-01T00:00:00Z".into(),
                evidence_window_end: "2026-09-30T00:00:00Z".into(),
                valid_to: Some("2026-09-30T01:00:00Z".into()),
                is_stale: true,
                provenance_ids: vec!["11111111-1111-1111-1111-111111111111".into()],
                supporting_evidence_ids: vec!["sha256:supporting".into()],
                conflicting_evidence_ids: vec!["sha256:conflicting".into()],
                supporting_evidence: vec![EvidenceProvenanceBinding {
                    evidence_id: "sha256:supporting".into(),
                    provenance_ids: vec!["22222222-2222-2222-2222-222222222222".into()],
                }],
                conflicting_evidence: vec![EvidenceProvenanceBinding {
                    evidence_id: "sha256:conflicting".into(),
                    provenance_ids: vec!["33333333-3333-3333-3333-333333333333".into()],
                }],
            }],
            timeline: DashboardPage::new(vec![fact(&format!("timeline:{id}"), "settlement")], None),
            graph: GraphReadModel {
                root: fact(id, "buyer"),
                nodes: vec![fact("service:1", "service")],
                edges: vec![Fact {
                    value: json!({
                        "source": {"kind": "buyer", "id": id},
                        "target": {"kind": "service", "id": "service:1"},
                        "predicate": "paid_for",
                        "direction": "outbound",
                        "attribution_method": "explicit_requirement",
                        "confidence": "0.9100",
                        "supporting_evidence_ids": ["sha256:supporting"],
                        "conflicting_evidence_ids": ["sha256:conflicting"]
                    }),
                    ..fact("edge:1", "attribution")
                }],
            },
        }))
    }

    async fn buyer_timeline(
        &self,
        id: &str,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        Ok(DashboardPage::new(
            vec![fact(&format!("timeline:{id}"), "settlement")],
            None,
        ))
    }

    async fn services(
        &self,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        Ok(DashboardPage::new(vec![fact("service:1", "service")], None))
    }

    async fn service(&self, id: &str) -> Result<Option<Fact>, QueryError> {
        Ok(Some(fact(id, "service")))
    }

    async fn graph(
        &self,
        kind: &str,
        id: &str,
        _limit: usize,
    ) -> Result<GraphReadModel, QueryError> {
        Ok(GraphReadModel {
            root: fact(id, kind),
            nodes: vec![fact("service:1", "service")],
            edges: vec![fact("edge:1", "attribution")],
        })
    }

    async fn provenance(&self, id: &str) -> Result<Option<ProvenanceReadModel>, QueryError> {
        Ok(Some(ProvenanceReadModel {
            provenance_id: id.into(),
            source_id: "fixture".into(),
            observed_at: "2026-09-30T00:00:00Z".into(),
            parser_version: "fixture-v1".into(),
            provider: None,
            chain_scope: Some("base".into()),
            block_reference: None,
            transaction_reference: None,
            finality: Some("finalized".into()),
            evidence_id: "sha256:fixture".into(),
            evidence_sha256: "a".repeat(64),
        }))
    }

    async fn search(
        &self,
        _query: &str,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        Ok(DashboardPage::new(vec![fact("buyer:1", "buyer")], None))
    }

    async fn system(&self) -> Result<SystemReadModel, QueryError> {
        Ok(SystemReadModel {
            facts: vec![fact("system:jobs", "system_metric")],
        })
    }

    async fn investigations(
        &self,
        _after: Option<&str>,
        _limit: usize,
    ) -> Result<ProjectionPageList, QueryError> {
        Ok(ProjectionPageList::new(
            vec![ProjectionPage {
                page_path: "investigations/weather".into(),
                stable_entity_id: "service:weather".into(),
                generated_markdown: "# Weather".into(),
                citations: json!([{"stable_id": "evidence:weather", "evidence_sha256": "d".repeat(64)}]),
                wikilinks: vec!["services/weather".into()],
                model_id: "projector-v1".into(),
                model_sha256: "a".repeat(64),
                prompt_sha256: "b".repeat(64),
                snapshot_sha256: "c".repeat(64),
                output_sha256: "d".repeat(64),
                bundle_sha256: "e".repeat(64),
                changeset_id: "changeset-7".into(),
                changeset_sha256: "f".repeat(64),
                approved_by: "owner".into(),
                approved_at: "2026-09-30T00:00:00Z".into(),
                published_at: "2026-09-30T00:01:00Z".into(),
            }],
            None,
        ))
    }
}

#[tokio::test]
async fn list_queries_are_bounded_and_cacheable_with_provenance() {
    let app = api_router(Arc::new(FixtureStore));

    let too_large = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/buyers?limit=101")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(too_large.status(), StatusCode::BAD_REQUEST);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/buyers?limit=100")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CACHE_CONTROL],
        "private, max-age=30, stale-while-revalidate=120"
    );
    assert!(response.headers().contains_key(header::ETAG));

    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["api_version"], "v1");
    assert_eq!(
        body["items"][0]["provenance_ids"][0],
        "11111111-1111-1111-1111-111111111111"
    );
}

#[tokio::test]
async fn purpose_built_surface_is_documented_without_graphql_or_sql() {
    let app = api_router(Arc::new(FixtureStore));
    let paths = [
        "/api/v1/pulse",
        "/api/v1/buyers",
        "/api/v1/buyers/buyer:1",
        "/api/v1/buyers/buyer:1/dossier",
        "/api/v1/buyers/buyer:1/timeline",
        "/api/v1/services",
        "/api/v1/services/service:1",
        "/api/v1/graph/buyer/buyer:1",
        "/api/v1/provenance/11111111-1111-1111-1111-111111111111",
        "/api/v1/search?q=buyer",
        "/api/v1/investigations",
        "/api/v1/system",
    ];
    for path in paths {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/api/v1/openapi.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let document: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(document["info"]["version"], "1.0.0");
    assert!(
        document["paths"]
            .as_object()
            .unwrap()
            .contains_key("/api/v1/provenance/{id}")
    );
    for path in [
        "/api/v1/buyers/{id}",
        "/api/v1/buyers/{id}/dossier",
        "/api/v1/buyers/{id}/timeline",
        "/api/v1/services/{id}",
        "/api/v1/graph/{kind}/{id}",
        "/api/v1/provenance/{id}",
    ] {
        let parameters = document["paths"][path]["get"]["parameters"]
            .as_array()
            .unwrap();
        assert!(
            parameters
                .iter()
                .any(|parameter| parameter["in"] == "path" && parameter["required"] == true),
            "{path} must document required path parameters"
        );
    }
    let buyer_parameters = document["paths"]["/api/v1/buyers"]["get"]["parameters"]
        .as_array()
        .unwrap();
    assert!(buyer_parameters.iter().any(|parameter| {
        parameter["name"] == "limit" && parameter["schema"]["maximum"] == 100
    }));
    assert!(
        buyer_parameters
            .iter()
            .any(|parameter| parameter["name"] == "cursor")
    );
    assert_eq!(
        document["paths"]["/api/v1/search"]["get"]["parameters"][0]["name"],
        "q"
    );
    assert!(document["components"]["schemas"]["Fact"].is_object());
    assert!(
        document["paths"]
            .as_object()
            .unwrap()
            .contains_key("/api/v1/investigations")
    );
    let serialized = serde_json::to_string(&document).unwrap().to_lowercase();
    assert!(!serialized.contains("graphql"));
    assert!(!serialized.contains("sql"));

    for forbidden in ["/graphql", "/api/v1/sql"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(forbidden)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND, "{forbidden}");
    }
}

#[test]
fn postgres_store_is_the_runtime_query_backend() {
    let _constructor: fn(tokio_postgres::Client, String) -> PostgresQueryStore =
        PostgresQueryStore::new;
    assert!(include_str!("../src/main.rs").contains("SET ROLE agent_economy_dashboard_reader"));
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL migrated through 0011"]
async fn restricted_dashboard_role_serves_every_query_path() {
    let database_url = std::env::var("AEM_QUERY_TEST_DATABASE_URL")
        .expect("AEM_QUERY_TEST_DATABASE_URL must identify disposable PostgreSQL");
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("connect to disposable PostgreSQL");
    tokio::spawn(async move {
        connection.await.expect("PostgreSQL test connection");
    });

    let namespace = "00000000-0000-0000-0000-000000000017";
    let provenance = "00000000-0000-0000-0000-000000000117";
    client
        .batch_execute(&format!(
            r#"
            DO $$ BEGIN
                CREATE ROLE agent_economy_dashboard_test_login LOGIN NOINHERIT;
            EXCEPTION WHEN duplicate_object THEN NULL; END $$;
            GRANT agent_economy_dashboard_reader TO agent_economy_dashboard_test_login;
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'dashboard-role-test');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
            VALUES ('{namespace}', 'evidence:dashboard-role', repeat('a', 64),
                    'evidence://dashboard-role', 'application/json', 2, now());
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at, parser_version,
                 chain_scope, evidence_id)
            VALUES ('{namespace}', '{provenance}', 'source:test', now(), 'test@1',
                    'base', 'evidence:dashboard-role');
            INSERT INTO agent_economy.services
                (namespace_id, service_id, display_name, trust_state, provenance_id)
            VALUES ('{namespace}', 'service:test', 'Test service', 'observed', '{provenance}');
            INSERT INTO agent_economy.buyer_handles
                (namespace_id, buyer_handle_id, handle_kind, chain_scope, handle_value, provenance_id)
            VALUES ('{namespace}', 'buyer:test', 'wallet', 'base', '0xtest', '{provenance}');
            SET SESSION AUTHORIZATION agent_economy_dashboard_test_login;
            SET ROLE agent_economy_dashboard_reader;
            "#
        ))
        .await
        .expect("prepare restricted dashboard fixture");

    let write_error = client
        .execute(
            "INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key) VALUES ('00000000-0000-0000-0000-000000000999', 'tenant', 'forbidden')",
            &[],
        )
        .await
        .expect_err("dashboard reader must remain read-only");
    assert_eq!(
        write_error.code(),
        Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE)
    );

    let app = api_router(Arc::new(PostgresQueryStore::new(client, namespace.into())));
    let provenance_path = format!("/api/v1/provenance/{provenance}");
    for path in [
        "/api/v1/pulse",
        "/api/v1/buyers",
        "/api/v1/buyers/buyer:test",
        "/api/v1/buyers/buyer:test/dossier",
        "/api/v1/buyers/buyer:test/timeline",
        "/api/v1/services",
        "/api/v1/services/service:test",
        "/api/v1/graph/service/service:test",
        provenance_path.as_str(),
        "/api/v1/search?q=test",
        "/api/v1/investigations",
        "/api/v1/system",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
    }
}

#[tokio::test]
async fn buyer_dossier_keeps_classification_and_relationship_evidence_explicit() {
    let response = api_router(Arc::new(FixtureStore))
        .oneshot(
            Request::builder()
                .uri("/api/v1/buyers/buyer:1/dossier")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();

    let classification = &body["data"]["classifications"][0];
    assert_eq!(classification["status"], "disputed");
    assert_eq!(classification["confidence"], "0.7300");
    assert!(
        classification["valid_to"].is_string(),
        "stale validity must remain visible"
    );
    assert_eq!(classification["is_stale"], true);
    assert_eq!(
        classification["conflicting_evidence_ids"][0],
        "sha256:conflicting"
    );
    assert_eq!(
        classification["supporting_evidence"][0]["provenance_ids"][0],
        "22222222-2222-2222-2222-222222222222"
    );
    assert_eq!(
        classification["conflicting_evidence"][0]["provenance_ids"][0],
        "33333333-3333-3333-3333-333333333333"
    );

    let edge = &body["data"]["graph"]["edges"][0]["value"];
    assert_eq!(edge["source"]["kind"], "buyer");
    assert_eq!(edge["target"]["kind"], "service");
    assert_eq!(edge["predicate"], "paid_for");
    assert_eq!(edge["direction"], "outbound");
    assert_eq!(edge["attribution_method"], "explicit_requirement");
    assert_eq!(edge["confidence"], "0.9100");
}
