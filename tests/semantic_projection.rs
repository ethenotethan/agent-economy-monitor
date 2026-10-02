use std::sync::{Arc, Mutex};

use agent_economy_monitor::projection::{
    Citation, NativeChangeset, NativeRevision, NativeWiki, ProjectionApproval, ProjectionError,
    ProjectionFact, ProjectionGateway, ProjectionInput, ProjectionJob, ProjectionOutput,
    ProjectionWorker, Projector, Snapshot,
};
use async_trait::async_trait;
use serde_json::json;
use sha2::{Digest, Sha256};

type EventLog = Arc<Mutex<Vec<String>>>;
type PublishedBundles = Arc<Mutex<Vec<(Vec<u8>, String)>>>;

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

#[derive(Clone)]
struct FixtureGateway {
    events: EventLog,
    job: ProjectionJob,
    snapshot: Snapshot,
    approval_candidate_override: Option<String>,
    published: PublishedBundles,
}

#[async_trait]
impl ProjectionGateway for FixtureGateway {
    async fn pull_job(&self) -> Result<Option<ProjectionJob>, ProjectionError> {
        self.events.lock().unwrap().push("pull_job".into());
        Ok(Some(self.job.clone()))
    }

    async fn fetch_snapshot(&self, _job: &ProjectionJob) -> Result<Snapshot, ProjectionError> {
        self.events.lock().unwrap().push("fetch_snapshot".into());
        Ok(self.snapshot.clone())
    }

    async fn approval_for(
        &self,
        _job: &ProjectionJob,
        candidate_sha256: &str,
    ) -> Result<Option<ProjectionApproval>, ProjectionError> {
        self.events.lock().unwrap().push("approval".into());
        Ok(Some(ProjectionApproval {
            candidate_sha256: self
                .approval_candidate_override
                .clone()
                .unwrap_or_else(|| candidate_sha256.to_owned()),
            approved_by: "owner".into(),
            approved_at: "2026-09-30T00:00:00Z".into(),
        }))
    }

    async fn publish_bundle_if_absent(
        &self,
        _job: &ProjectionJob,
        bundle_bytes: &[u8],
        bundle_sha256: &str,
    ) -> Result<(), ProjectionError> {
        self.events.lock().unwrap().push("publish".into());
        self.published
            .lock()
            .unwrap()
            .push((bundle_bytes.to_vec(), bundle_sha256.to_owned()));
        Ok(())
    }
}

#[derive(Clone)]
struct FixtureProjector {
    events: EventLog,
    output: ProjectionOutput,
}

#[async_trait]
impl Projector for FixtureProjector {
    async fn project(
        &self,
        _job: &ProjectionJob,
        input: &ProjectionInput,
    ) -> Result<ProjectionOutput, ProjectionError> {
        assert_eq!(input.stable_entity_id, "service:weather");
        assert!(!serde_json::to_string(input).unwrap().contains("SECRET_RAW"));
        self.events.lock().unwrap().push("project".into());
        Ok(self.output.clone())
    }
}

#[derive(Clone)]
struct FixtureWiki {
    events: EventLog,
    revision: NativeRevision,
    changeset_override: Option<NativeChangeset>,
}

#[async_trait]
impl NativeWiki for FixtureWiki {
    async fn replace_generated_section(
        &self,
        wiki_id: &str,
        page_path: &str,
        _generated_markdown: &str,
        _wikilinks: &[String],
        _output_sha256: &str,
    ) -> Result<NativeRevision, ProjectionError> {
        assert_eq!(wiki_id, "agentic-commerce");
        assert_eq!(page_path, "services/weather.md");
        self.events.lock().unwrap().push("write_generated".into());
        Ok(self.revision.clone())
    }

    async fn capture_changeset(
        &self,
        wiki_id: &str,
        page_path: &str,
        revision: &NativeRevision,
        output_sha256: &str,
    ) -> Result<NativeChangeset, ProjectionError> {
        assert_eq!(wiki_id, "agentic-commerce");
        assert_eq!(page_path, "services/weather.md");
        assert_eq!(revision, &self.revision);
        self.events.lock().unwrap().push("changeset".into());
        Ok(self
            .changeset_override
            .clone()
            .unwrap_or_else(|| NativeChangeset {
                id: "changeset-7".into(),
                sha256: "c".repeat(64),
                page_revision_id: revision.id.clone(),
                page_sha256: revision.page_sha256.clone(),
                output_sha256: output_sha256.to_owned(),
            }))
    }
}

fn fixture(
    snapshot_bytes: &[u8],
    output: ProjectionOutput,
) -> (
    FixtureGateway,
    FixtureProjector,
    FixtureWiki,
    EventLog,
    PublishedBundles,
) {
    let events = Arc::new(Mutex::new(Vec::new()));
    let published = Arc::new(Mutex::new(Vec::new()));
    let citation = Citation {
        stable_id: "evidence:weather".into(),
        evidence_sha256: "d".repeat(64),
    };
    let mut job = ProjectionJob {
        job_id: "job-1".into(),
        stable_entity_id: "service:weather".into(),
        page_path: "services/weather.md".into(),
        model_id: "projector-v1".into(),
        model_sha256: "a".repeat(64),
        prompt_sha256: "b".repeat(64),
        snapshot_sha256: String::new(),
        destination: "gcs://agent-economy-projections/namespaces/test/".into(),
    };
    let mut snapshot = Snapshot {
        bytes: snapshot_bytes.to_vec(),
        sha256: String::new(),
        projection_input: ProjectionInput {
            stable_entity_id: "service:weather".into(),
            facts: vec![ProjectionFact {
                name: "availability".into(),
                value: json!("online"),
                citation: citation.clone(),
            }],
            citations: vec![citation],
        },
        private_fragments: vec!["SECRET_RAW".into()],
    };
    snapshot.sha256 = snapshot_sha(&snapshot);
    job.snapshot_sha256 = snapshot.sha256.clone();
    let gateway = FixtureGateway {
        events: events.clone(),
        job,
        snapshot,
        approval_candidate_override: None,
        published: published.clone(),
    };
    let projector = FixtureProjector {
        events: events.clone(),
        output,
    };
    let revision = NativeRevision {
        id: "revision-1".into(),
        page_sha256: "e".repeat(64),
    };
    let wiki = FixtureWiki {
        events: events.clone(),
        revision,
        changeset_override: None,
    };
    (gateway, projector, wiki, events, published)
}

fn valid_output() -> ProjectionOutput {
    let citations = vec![Citation {
        stable_id: "evidence:weather".into(),
        evidence_sha256: "d".repeat(64),
    }];
    let wikilinks = vec!["services/weather.md".into()];
    ProjectionOutput {
        generated_markdown: "# Weather\n\n[[services/weather.md]]".into(),
        citations: citations.clone(),
        wikilinks: wikilinks.clone(),
        output_sha256: output_sha(
            "# Weather\n\n[[services/weather.md]]",
            &citations,
            &wikilinks,
        ),
    }
}

#[tokio::test]
async fn projection_binds_revision_and_approval_before_exact_byte_publication() {
    let snapshot_bytes = br#"{"entity_id":"service:weather","private":"SECRET_RAW"}"#;
    let (gateway, projector, wiki, events, published) = fixture(snapshot_bytes, valid_output());

    let result = ProjectionWorker::new(gateway, projector, wiki)
        .run_once()
        .await
        .unwrap()
        .expect("job");

    assert_eq!(
        events.lock().unwrap().as_slice(),
        [
            "pull_job",
            "fetch_snapshot",
            "project",
            "write_generated",
            "changeset",
            "approval",
            "publish"
        ]
    );
    let published = published.lock().unwrap();
    assert_eq!(published.len(), 1);
    assert_eq!(sha256(&published[0].0), published[0].1);
    assert_eq!(result.bundle_sha256, published[0].1);
    assert!(!String::from_utf8_lossy(&published[0].0).contains("SECRET_RAW"));
    assert_eq!(result.payload.changeset.page_revision_id, "revision-1");
}

#[tokio::test]
async fn snapshot_hash_mismatch_stops_before_projection_or_wiki_write() {
    let (mut gateway, projector, wiki, events, _) = fixture(b"actual", valid_output());
    gateway.job.snapshot_sha256 = sha256(b"different");

    let error = ProjectionWorker::new(gateway, projector, wiki)
        .run_once()
        .await
        .unwrap_err();

    assert!(matches!(error, ProjectionError::HashMismatch("snapshot")));
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["pull_job", "fetch_snapshot"]
    );
}

#[tokio::test]
async fn snapshot_hash_binds_the_exact_model_input_and_privacy_policy() {
    let (mut gateway, projector, wiki, events, _) = fixture(b"snapshot", valid_output());
    gateway.snapshot.sha256 = sha256(&gateway.snapshot.bytes);
    gateway.job.snapshot_sha256 = gateway.snapshot.sha256.clone();
    gateway.snapshot.projection_input.facts[0].value = json!("substituted");

    let error = ProjectionWorker::new(gateway, projector, wiki)
        .run_once()
        .await
        .unwrap_err();

    assert!(matches!(error, ProjectionError::HashMismatch("snapshot")));
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["pull_job", "fetch_snapshot"]
    );
}

#[tokio::test]
async fn rendered_markdown_links_must_exactly_match_safe_sidecar_links() {
    let mut output = valid_output();
    output.generated_markdown =
        "# Weather\n\n[[services/weather.md]] [[research/private-note.md]]".into();
    output.output_sha256 = output_sha(
        &output.generated_markdown,
        &output.citations,
        &output.wikilinks,
    );
    let (gateway, projector, wiki, events, _) = fixture(b"snapshot", output);

    let error = ProjectionWorker::new(gateway, projector, wiki)
        .run_once()
        .await
        .unwrap_err();

    assert!(matches!(error, ProjectionError::UnsafeProjection(_)));
    assert_eq!(
        events.lock().unwrap().as_slice(),
        ["pull_job", "fetch_snapshot", "project"]
    );
}

#[tokio::test]
async fn publication_rejects_private_snapshot_canaries_and_unauthorized_citations() {
    let mut private = valid_output();
    private.generated_markdown.push_str(" SECRET_RAW");
    private.output_sha256 = output_sha(
        &private.generated_markdown,
        &private.citations,
        &private.wikilinks,
    );
    let (gateway, projector, wiki, _, _) = fixture(b"SECRET_RAW", private);
    assert!(matches!(
        ProjectionWorker::new(gateway, projector, wiki)
            .run_once()
            .await
            .unwrap_err(),
        ProjectionError::UnsafeProjection(_)
    ));

    let mut unauthorized = valid_output();
    unauthorized.citations[0].stable_id = "evidence:not-in-snapshot".into();
    unauthorized.output_sha256 = output_sha(
        &unauthorized.generated_markdown,
        &unauthorized.citations,
        &unauthorized.wikilinks,
    );
    let (gateway, projector, wiki, _, _) = fixture(b"snapshot", unauthorized);
    assert!(matches!(
        ProjectionWorker::new(gateway, projector, wiki)
            .run_once()
            .await
            .unwrap_err(),
        ProjectionError::UnsafeProjection(_)
    ));
}

#[tokio::test]
async fn unrelated_changeset_revision_and_wrong_approval_candidate_are_rejected() {
    let (gateway, projector, mut wiki, _, _) = fixture(b"snapshot", valid_output());
    wiki.changeset_override = Some(NativeChangeset {
        id: "changeset-other".into(),
        sha256: "c".repeat(64),
        page_revision_id: "revision-other".into(),
        page_sha256: "e".repeat(64),
        output_sha256: valid_output().output_sha256,
    });
    assert!(matches!(
        ProjectionWorker::new(gateway, projector, wiki)
            .run_once()
            .await
            .unwrap_err(),
        ProjectionError::ChangesetMismatch
    ));

    let (mut gateway, projector, wiki, _, published) = fixture(b"snapshot", valid_output());
    gateway.approval_candidate_override = Some("0".repeat(64));
    assert!(matches!(
        ProjectionWorker::new(gateway, projector, wiki)
            .run_once()
            .await
            .unwrap_err(),
        ProjectionError::ApprovalMismatch
    ));
    assert!(published.lock().unwrap().is_empty());
}
