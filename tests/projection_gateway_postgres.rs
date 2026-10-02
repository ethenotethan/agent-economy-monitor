use std::sync::Arc;

use agent_economy_monitor::{
    projection::{
        Citation, NativeChangeset, ProjectionApproval, ProjectionFact, ProjectionInput,
        ProjectionJob, ProjectionPayload, Snapshot,
    },
    projection_gateway::{
        PostgresProjectionGatewayStore, ProjectionGatewayState, ProjectionGatewayStore,
        mount_projection_gateway,
    },
    projection_runtime::{ProjectionRuntimeConfig, SecretString, run_projection_once},
};
use axum::Router;
use serde::Serialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use tokio_postgres::NoTls;

const NAMESPACE: &str = "00000000-0000-0000-0000-000000000217";
const SNAPSHOT_ID: &str = "00000000-0000-0000-0000-000000000317";
const JOB_ID: &str = "00000000-0000-0000-0000-000000000417";
const UPDATED: &str = "2026-10-02T00:00:00Z";
const PAGE_SHA: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

fn snapshot_sha(snapshot: &Snapshot) -> String {
    sha256(
        &serde_json::to_vec(&json!({
            "bytes": snapshot.bytes,
            "projection_input": snapshot.projection_input,
            "private_fragments": snapshot.private_fragments,
        }))
        .unwrap(),
    )
}

fn output_sha(markdown: &str, citations: &[Citation], wikilinks: &[String]) -> String {
    sha256(
        &serde_json::to_vec(&json!({
            "generated_markdown": markdown,
            "citations": citations,
            "wikilinks": wikilinks,
        }))
        .unwrap(),
    )
}

#[derive(Serialize)]
struct CapturedChangeset<'a> {
    id: &'a str,
    timestamp: &'a str,
    page: &'a str,
    trigger: &'a str,
    after_sha256: &'a str,
}

fn model_response() -> Value {
    json!({
        "generated_markdown": "# Weather\n\n[[services/weather.md]]",
        "citations": [{
            "stable_id": "evidence:weather",
            "evidence_sha256": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
        }],
        "wikilinks": ["services/weather.md"]
    })
}

fn wiki_response(request: &Value) -> Value {
    let method = request["method"].as_str().unwrap();
    match method {
        "wiki.page" => json!({"error": {"code": 4040}}),
        "wiki.update" => json!({"result": {"updated": UPDATED}}),
        "wiki.changesets" => {
            let trigger = request["params"]["trigger"].as_str().unwrap();
            json!({"result": {"changesets": [{
                "id": "changeset-17",
                "timestamp": UPDATED,
                "page": "services/weather.md",
                "trigger": trigger,
                "after_sha256": PAGE_SHA
            }]}})
        }
        _ => json!({"error": {"code": -32601}}),
    }
}

async fn spawn_server(router: Router) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    (format!("http://{address}/"), handle)
}

async fn serve_mock_connection(mut stream: TcpStream) {
    let mut request_bytes = Vec::new();
    let (body_offset, content_length) = loop {
        let mut chunk = [0_u8; 4096];
        let count = stream.read(&mut chunk).await.unwrap();
        assert_ne!(count, 0, "mock client closed before completing request");
        request_bytes.extend_from_slice(&chunk[..count]);
        let Some(header_end) = request_bytes
            .windows(4)
            .position(|window| window == b"\r\n\r\n")
        else {
            continue;
        };
        let body_offset = header_end + 4;
        let headers = std::str::from_utf8(&request_bytes[..header_end]).unwrap();
        let content_length = headers
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(str::trim)
                    .map(str::parse::<usize>)
            })
            .transpose()
            .unwrap()
            .unwrap_or(0);
        if request_bytes.len() >= body_offset + content_length {
            break (body_offset, content_length);
        }
    };

    let request_line = std::str::from_utf8(&request_bytes)
        .unwrap()
        .lines()
        .next()
        .unwrap();
    let path = request_line.split_whitespace().nth(1).unwrap();
    let response = if path == "/v1/project" {
        model_response()
    } else {
        let request: Value =
            serde_json::from_slice(&request_bytes[body_offset..body_offset + content_length])
                .unwrap();
        wiki_response(&request)
    };
    let body = serde_json::to_vec(&response).unwrap();
    let headers = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(headers.as_bytes()).await.unwrap();
    stream.write_all(&body).await.unwrap();
}

async fn spawn_mock_server() -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::spawn(serve_mock_connection(stream));
        }
    });
    (format!("http://{address}/"), handle)
}

#[tokio::test]
#[ignore = "requires a disposable PostgreSQL migrated through 0011"]
async fn project_wiki_uses_authenticated_postgres_gateway_end_to_end() {
    let database_url = std::env::var("AEM_PROJECTION_TEST_DATABASE_URL")
        .expect("AEM_PROJECTION_TEST_DATABASE_URL must identify disposable PostgreSQL");
    let (fixture_client, fixture_connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("connect fixture client");
    tokio::spawn(async move { fixture_connection.await.unwrap() });
    let (gateway_client, gateway_connection) = tokio_postgres::connect(&database_url, NoTls)
        .await
        .expect("connect gateway client");
    tokio::spawn(async move { gateway_connection.await.unwrap() });
    gateway_client
        .batch_execute("SET ROLE agent_economy_projection_writer")
        .await
        .expect("assume projection writer role");

    let citation = Citation {
        stable_id: "evidence:weather".into(),
        evidence_sha256: "d".repeat(64),
    };
    let mut snapshot = Snapshot {
        bytes: br#"{"bounded":true,"private":"PRIVATE_CANARY"}"#.to_vec(),
        sha256: String::new(),
        projection_input: ProjectionInput {
            stable_entity_id: "service:weather".into(),
            facts: vec![ProjectionFact {
                name: "availability".into(),
                value: json!("online"),
                citation: citation.clone(),
            }],
            citations: vec![citation.clone()],
        },
        private_fragments: vec!["PRIVATE_CANARY".into()],
    };
    snapshot.sha256 = snapshot_sha(&snapshot);
    let job = ProjectionJob {
        job_id: JOB_ID.into(),
        stable_entity_id: "service:weather".into(),
        page_path: "services/weather.md".into(),
        model_id: "projector-v1".into(),
        model_sha256: "a".repeat(64),
        prompt_sha256: "b".repeat(64),
        snapshot_sha256: snapshot.sha256.clone(),
        destination: "gcs://agent-economy-projections/namespaces/test/weather.json".into(),
    };
    let markdown = "# Weather\n\n[[services/weather.md]]";
    let wikilinks = vec!["services/weather.md".into()];
    let output_sha256 = output_sha(markdown, std::slice::from_ref(&citation), &wikilinks);
    let trigger = format!("projection:{output_sha256}");
    let captured = CapturedChangeset {
        id: "changeset-17",
        timestamp: UPDATED,
        page: "services/weather.md",
        trigger: &trigger,
        after_sha256: PAGE_SHA,
    };
    let changeset = NativeChangeset {
        id: "changeset-17".into(),
        sha256: sha256(&serde_json::to_vec(&captured).unwrap()),
        page_revision_id: "changeset-17".into(),
        page_sha256: PAGE_SHA.into(),
        output_sha256: output_sha256.clone(),
    };
    let payload = ProjectionPayload {
        wiki_id: "agentic-commerce".into(),
        job_id: JOB_ID.into(),
        stable_entity_id: "service:weather".into(),
        page_path: "services/weather.md".into(),
        model_id: "projector-v1".into(),
        model_sha256: "a".repeat(64),
        prompt_sha256: "b".repeat(64),
        snapshot_sha256: snapshot.sha256.clone(),
        output_sha256,
        destination: job.destination.clone(),
        generated_markdown: markdown.into(),
        citations: vec![citation],
        wikilinks,
        changeset,
    };
    let payload_bytes = serde_json::to_vec(&payload).unwrap();
    let candidate_sha256 = sha256(&payload_bytes);
    let approval = ProjectionApproval {
        candidate_sha256: candidate_sha256.clone(),
        approved_by: "owner".into(),
        approved_at: UPDATED.into(),
    };

    fixture_client
        .execute(
            "INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key) \
             VALUES ($1::text::uuid, 'tenant', 'projection-gateway-test') ON CONFLICT DO NOTHING",
            &[&NAMESPACE],
        )
        .await
        .unwrap();
    fixture_client
        .execute(
            "INSERT INTO agent_economy.projection_snapshots \
               (namespace_id, snapshot_id, stable_entity_id, snapshot_sha256, storage_uri, byte_length, snapshot_payload) \
             VALUES ($1::text::uuid, $2::text::uuid, $3, $4, $5, $6, $7::text::jsonb)",
            &[
                &NAMESPACE,
                &SNAPSHOT_ID,
                &job.stable_entity_id,
                &job.snapshot_sha256,
                &"gcs://agent-economy-snapshots/test.json",
                &(snapshot.bytes.len() as i64),
                &serde_json::to_string(&snapshot).unwrap(),
            ],
        )
        .await
        .unwrap();
    fixture_client
        .execute(
            "INSERT INTO agent_economy.projection_jobs \
               (namespace_id, job_id, snapshot_id, stable_entity_id, page_path, model_id, \
                model_sha256, prompt_sha256, snapshot_sha256, destination) \
             VALUES ($1::text::uuid, $2::text::uuid, $3::text::uuid, $4, $5, $6, $7, $8, $9, $10)",
            &[
                &NAMESPACE,
                &JOB_ID,
                &SNAPSHOT_ID,
                &job.stable_entity_id,
                &job.page_path,
                &job.model_id,
                &job.model_sha256,
                &job.prompt_sha256,
                &job.snapshot_sha256,
                &job.destination,
            ],
        )
        .await
        .unwrap();
    fixture_client
        .execute(
            "INSERT INTO agent_economy.projection_approvals \
               (namespace_id, job_id, candidate_sha256, approved_payload, approved_payload_bytes, approved_by, approved_at) \
             VALUES ($1::text::uuid, $2::text::uuid, $3, $4::text::jsonb, $5, $6, $7::text::timestamptz)",
            &[
                &NAMESPACE,
                &JOB_ID,
                &candidate_sha256,
                &String::from_utf8(payload_bytes.clone()).unwrap(),
                &payload_bytes,
                &approval.approved_by,
                &approval.approved_at,
            ],
        )
        .await
        .unwrap();

    let gateway_store = Arc::new(PostgresProjectionGatewayStore::new(
        gateway_client,
        NAMESPACE.into(),
    ));
    assert!(
        gateway_store
            .lease_next("worker-test")
            .await
            .unwrap()
            .is_some()
    );
    assert!(
        gateway_store
            .approval(JOB_ID, &candidate_sha256, "worker-test")
            .await
            .unwrap()
            .is_some()
    );
    let status = fixture_client
        .query_one(
            "SELECT status FROM agent_economy.projection_jobs \
             WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid",
            &[&NAMESPACE, &JOB_ID],
        )
        .await
        .unwrap()
        .get::<_, String>(0);
    assert_eq!(
        status, "leased",
        "reading approval must not strand the job before publication"
    );
    fixture_client
        .execute(
            "UPDATE agent_economy.projection_jobs \
             SET lease_expires_at = clock_timestamp() - interval '1 second' \
             WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid",
            &[&NAMESPACE, &JOB_ID],
        )
        .await
        .unwrap();

    let gateway = ProjectionGatewayState::new(gateway_store, "gateway-token", "worker-test");
    let (gateway_url, gateway_server) =
        spawn_server(mount_projection_gateway(Router::new(), gateway)).await;
    let (mock_url, mock_server) = spawn_mock_server().await;

    let result = run_projection_once(
        ProjectionRuntimeConfig::new(
            &gateway_url,
            SecretString::new("gateway-token"),
            &format!("{mock_url}v1/project"),
            SecretString::new("model-token"),
            &format!("{mock_url}rpc"),
            SecretString::new("wiki-token"),
        )
        .unwrap(),
    )
    .await
    .unwrap()
    .expect("leased projection job");
    gateway_server.abort();
    mock_server.abort();

    assert_eq!(result.payload_sha256, candidate_sha256);
    let row = fixture_client
        .query_one(
            "SELECT job.status, publication.bundle_bytes \
             FROM agent_economy.projection_jobs AS job \
             JOIN agent_economy.projection_publications AS publication USING (namespace_id, job_id) \
             WHERE job.namespace_id = $1::text::uuid AND job.job_id = $2::text::uuid",
            &[&NAMESPACE, &JOB_ID],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, String>(0), "published");
    let stored: Vec<u8> = row.get(1);
    assert_eq!(sha256(&stored), result.bundle_sha256);
}
