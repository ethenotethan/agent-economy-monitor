use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{error::Error, fmt};

const WIKI_ID: &str = "agentic-commerce";
const DESTINATION_PREFIX: &str = "gcs://agent-economy-projections/";
const ALLOWED_LINK_PREFIXES: [&str; 4] = ["buyers/", "services/", "investigations/", "protocols/"];

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Citation {
    pub stable_id: String,
    pub evidence_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionJob {
    pub job_id: String,
    pub stable_entity_id: String,
    pub page_path: String,
    pub model_id: String,
    pub model_sha256: String,
    pub prompt_sha256: String,
    pub snapshot_sha256: String,
    pub destination: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionFact {
    pub name: String,
    pub value: serde_json::Value,
    pub citation: Citation,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionInput {
    pub stable_entity_id: String,
    pub facts: Vec<ProjectionFact>,
    pub citations: Vec<Citation>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Snapshot {
    /// Exact cloud snapshot bytes used only for integrity verification, never model input.
    pub bytes: Vec<u8>,
    pub sha256: String,
    /// Explicit allowlisted facts supplied to the projector instead of the raw snapshot.
    pub projection_input: ProjectionInput,
    /// Distinctive values that must never occur in any projector output or published field.
    pub private_fragments: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionOutput {
    pub generated_markdown: String,
    pub citations: Vec<Citation>,
    pub wikilinks: Vec<String>,
    pub output_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NativeRevision {
    pub id: String,
    pub page_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct NativeChangeset {
    pub id: String,
    pub sha256: String,
    pub page_revision_id: String,
    pub page_sha256: String,
    pub output_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionApproval {
    pub candidate_sha256: String,
    pub approved_by: String,
    pub approved_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ProjectionPayload {
    pub wiki_id: String,
    pub job_id: String,
    pub stable_entity_id: String,
    pub page_path: String,
    pub model_id: String,
    pub model_sha256: String,
    pub prompt_sha256: String,
    pub snapshot_sha256: String,
    pub output_sha256: String,
    pub destination: String,
    pub generated_markdown: String,
    pub citations: Vec<Citation>,
    pub wikilinks: Vec<String>,
    pub changeset: NativeChangeset,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct PublicationEnvelope {
    payload: ProjectionPayload,
    payload_sha256: String,
    approval: ProjectionApproval,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct PublishedProjection {
    pub payload: ProjectionPayload,
    pub payload_sha256: String,
    pub approval: ProjectionApproval,
    /// SHA-256 of the exact serialized PublicationEnvelope bytes uploaded by the gateway.
    pub bundle_sha256: String,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ProjectionError {
    Gateway(String),
    Projector(String),
    Wiki(String),
    HashMismatch(&'static str),
    UnsafeProjection(&'static str),
    ChangesetMismatch,
    ApprovalMismatch,
    Serialization,
}

impl fmt::Display for ProjectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Gateway(message) => write!(f, "projection gateway error: {message}"),
            Self::Projector(message) => write!(f, "projection model error: {message}"),
            Self::Wiki(message) => write!(f, "native wiki error: {message}"),
            Self::HashMismatch(kind) => write!(f, "{kind} hash mismatch"),
            Self::UnsafeProjection(message) => write!(f, "unsafe projection: {message}"),
            Self::ChangesetMismatch => write!(f, "changeset is not bound to the written revision"),
            Self::ApprovalMismatch => {
                write!(f, "approval is not bound to the publication candidate")
            }
            Self::Serialization => write!(f, "projection serialization failed"),
        }
    }
}

impl Error for ProjectionError {}

#[async_trait]
pub trait ProjectionGateway: Send + Sync {
    async fn pull_job(&self) -> Result<Option<ProjectionJob>, ProjectionError>;
    async fn fetch_snapshot(&self, job: &ProjectionJob) -> Result<Snapshot, ProjectionError>;
    async fn approval_for(
        &self,
        job: &ProjectionJob,
        candidate_sha256: &str,
    ) -> Result<Option<ProjectionApproval>, ProjectionError>;
    async fn publish_bundle_if_absent(
        &self,
        job: &ProjectionJob,
        bundle_bytes: &[u8],
        bundle_sha256: &str,
    ) -> Result<(), ProjectionError>;
}

#[async_trait]
pub trait Projector: Send + Sync {
    async fn project(
        &self,
        job: &ProjectionJob,
        input: &ProjectionInput,
    ) -> Result<ProjectionOutput, ProjectionError>;
}

#[async_trait]
pub trait NativeWiki: Send + Sync {
    async fn replace_generated_section(
        &self,
        wiki_id: &str,
        page_path: &str,
        generated_markdown: &str,
        wikilinks: &[String],
        output_sha256: &str,
    ) -> Result<NativeRevision, ProjectionError>;
    async fn capture_changeset(
        &self,
        wiki_id: &str,
        page_path: &str,
        revision: &NativeRevision,
        output_sha256: &str,
    ) -> Result<NativeChangeset, ProjectionError>;
}

pub struct ProjectionWorker<G, P, W> {
    gateway: G,
    projector: P,
    wiki: W,
}

impl<G, P, W> ProjectionWorker<G, P, W>
where
    G: ProjectionGateway,
    P: Projector,
    W: NativeWiki,
{
    pub fn new(gateway: G, projector: P, wiki: W) -> Self {
        Self {
            gateway,
            projector,
            wiki,
        }
    }

    pub async fn run_once(&self) -> Result<Option<PublishedProjection>, ProjectionError> {
        let Some(job) = self.gateway.pull_job().await? else {
            return Ok(None);
        };
        validate_job(&job)?;

        let snapshot = self.gateway.fetch_snapshot(&job).await?;
        verify_snapshot_hash(&snapshot)?;
        if snapshot.sha256 != job.snapshot_sha256 {
            return Err(ProjectionError::HashMismatch("snapshot"));
        }
        validate_snapshot_policy(&job, &snapshot)?;

        let output = self
            .projector
            .project(&job, &snapshot.projection_input)
            .await?;
        validate_output(&job, &snapshot.projection_input, &snapshot, &output)?;
        verify_output_hash(&output)?;

        let revision = self
            .wiki
            .replace_generated_section(
                WIKI_ID,
                &job.page_path,
                &output.generated_markdown,
                &output.wikilinks,
                &output.output_sha256,
            )
            .await?;
        validate_revision(&revision)?;

        let changeset = self
            .wiki
            .capture_changeset(WIKI_ID, &job.page_path, &revision, &output.output_sha256)
            .await?;
        validate_changeset(&changeset, &revision, &output.output_sha256)?;

        let payload = ProjectionPayload {
            wiki_id: WIKI_ID.into(),
            job_id: job.job_id.clone(),
            stable_entity_id: job.stable_entity_id.clone(),
            page_path: job.page_path.clone(),
            model_id: job.model_id.clone(),
            model_sha256: job.model_sha256.clone(),
            prompt_sha256: job.prompt_sha256.clone(),
            snapshot_sha256: job.snapshot_sha256.clone(),
            output_sha256: output.output_sha256.clone(),
            destination: job.destination.clone(),
            generated_markdown: output.generated_markdown,
            citations: output.citations,
            wikilinks: output.wikilinks,
            changeset,
        };
        let payload_bytes =
            serde_json::to_vec(&payload).map_err(|_| ProjectionError::Serialization)?;
        let payload_sha256 = sha256(&payload_bytes);

        let approval = self
            .gateway
            .approval_for(&job, &payload_sha256)
            .await?
            .ok_or_else(|| ProjectionError::Gateway("projection is not approved".into()))?;
        if approval.candidate_sha256 != payload_sha256 {
            return Err(ProjectionError::ApprovalMismatch);
        }
        validate_hash("approval candidate", &approval.candidate_sha256)?;
        if approval.approved_by.is_empty() || approval.approved_at.is_empty() {
            return Err(ProjectionError::ApprovalMismatch);
        }

        let envelope = PublicationEnvelope {
            payload: payload.clone(),
            payload_sha256: payload_sha256.clone(),
            approval: approval.clone(),
        };
        let bundle_bytes =
            serde_json::to_vec(&envelope).map_err(|_| ProjectionError::Serialization)?;
        ensure_private_fragments_absent(&snapshot, &bundle_bytes)?;
        let bundle_sha256 = sha256(&bundle_bytes);
        self.gateway
            .publish_bundle_if_absent(&job, &bundle_bytes, &bundle_sha256)
            .await?;

        Ok(Some(PublishedProjection {
            payload,
            payload_sha256,
            approval,
            bundle_sha256,
        }))
    }
}

fn validate_job(job: &ProjectionJob) -> Result<(), ProjectionError> {
    if job.job_id.is_empty()
        || !job
            .job_id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        || job.stable_entity_id.is_empty()
        || job.model_id.is_empty()
        || !allowed_path(&job.page_path)
        || !job.destination.starts_with(DESTINATION_PREFIX)
        || job.destination.contains("..")
    {
        return Err(ProjectionError::UnsafeProjection(
            "job scope is not allowlisted",
        ));
    }
    validate_hash("model", &job.model_sha256)?;
    validate_hash("prompt", &job.prompt_sha256)?;
    validate_hash("snapshot", &job.snapshot_sha256)
}

fn validate_snapshot_policy(
    job: &ProjectionJob,
    snapshot: &Snapshot,
) -> Result<(), ProjectionError> {
    if snapshot.private_fragments.iter().any(String::is_empty) {
        return Err(ProjectionError::UnsafeProjection(
            "private-fragment policy contains an empty value",
        ));
    }
    if snapshot.projection_input.stable_entity_id != job.stable_entity_id
        || snapshot.projection_input.facts.is_empty()
        || snapshot.projection_input.citations.is_empty()
    {
        return Err(ProjectionError::UnsafeProjection(
            "projection input is not bound to the job or has no allowlisted facts",
        ));
    }
    for citation in &snapshot.projection_input.citations {
        validate_citation(citation)?;
    }
    for fact in &snapshot.projection_input.facts {
        if fact.name.is_empty() || !snapshot.projection_input.citations.contains(&fact.citation) {
            return Err(ProjectionError::UnsafeProjection(
                "projection fact is not citation-authorized",
            ));
        }
    }
    let serialized = serde_json::to_vec(&snapshot.projection_input)
        .map_err(|_| ProjectionError::Serialization)?;
    ensure_private_fragments_absent(snapshot, &serialized)?;
    Ok(())
}

fn validate_output(
    job: &ProjectionJob,
    input: &ProjectionInput,
    snapshot: &Snapshot,
    output: &ProjectionOutput,
) -> Result<(), ProjectionError> {
    if output.generated_markdown.is_empty() || output.citations.is_empty() {
        return Err(ProjectionError::UnsafeProjection(
            "markdown and citations are required",
        ));
    }
    if output.wikilinks.iter().any(|link| !allowed_path(link)) {
        return Err(ProjectionError::UnsafeProjection(
            "cross-wiki links are forbidden",
        ));
    }
    let rendered_links = extract_wikilinks(&output.generated_markdown)?;
    if rendered_links != output.wikilinks
        || !rendered_links.iter().any(|link| link == &job.page_path)
    {
        return Err(ProjectionError::UnsafeProjection(
            "rendered wikilinks do not match the validated sidecar",
        ));
    }
    for citation in &output.citations {
        validate_citation(citation)?;
        if !input.citations.contains(citation) {
            return Err(ProjectionError::UnsafeProjection(
                "citation is not authorized by the bounded snapshot",
            ));
        }
    }
    let serialized = serde_json::to_vec(output).map_err(|_| ProjectionError::Serialization)?;
    ensure_private_fragments_absent(snapshot, &serialized)
}

fn validate_citation(citation: &Citation) -> Result<(), ProjectionError> {
    if citation.stable_id.is_empty() {
        return Err(ProjectionError::UnsafeProjection(
            "citation stable ID is required",
        ));
    }
    validate_hash("citation evidence", &citation.evidence_sha256)
}

fn validate_revision(revision: &NativeRevision) -> Result<(), ProjectionError> {
    if revision.id.is_empty() {
        return Err(ProjectionError::ChangesetMismatch);
    }
    validate_hash("page revision", &revision.page_sha256)
}

fn validate_changeset(
    changeset: &NativeChangeset,
    revision: &NativeRevision,
    output_sha256: &str,
) -> Result<(), ProjectionError> {
    if changeset.id.is_empty()
        || changeset.page_revision_id != revision.id
        || changeset.page_sha256 != revision.page_sha256
        || changeset.output_sha256 != output_sha256
    {
        return Err(ProjectionError::ChangesetMismatch);
    }
    validate_hash("changeset", &changeset.sha256)?;
    Ok(())
}

fn verify_output_hash(output: &ProjectionOutput) -> Result<(), ProjectionError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "generated_markdown": output.generated_markdown,
        "citations": output.citations,
        "wikilinks": output.wikilinks,
    }))
    .map_err(|_| ProjectionError::Serialization)?;
    verify_hash("output", &bytes, &output.output_sha256)
}

fn extract_wikilinks(markdown: &str) -> Result<Vec<String>, ProjectionError> {
    let mut links = Vec::new();
    let mut rest = markdown;
    while let Some(start) = rest.find("[[") {
        let after = &rest[start + 2..];
        let Some(end) = after.find("]]") else {
            return Err(ProjectionError::UnsafeProjection("malformed wikilink"));
        };
        let link = &after[..end];
        if link.is_empty() || link.contains(['[', ']', '|', '\\']) || !allowed_path(link) {
            return Err(ProjectionError::UnsafeProjection(
                "unsafe rendered wikilink",
            ));
        }
        links.push(link.to_owned());
        rest = &after[end + 2..];
    }
    if rest.contains("]]") {
        return Err(ProjectionError::UnsafeProjection("malformed wikilink"));
    }
    Ok(links)
}

fn ensure_private_fragments_absent(
    snapshot: &Snapshot,
    serialized: &[u8],
) -> Result<(), ProjectionError> {
    if snapshot.private_fragments.iter().any(|fragment| {
        if fragment.is_empty() {
            return true;
        }
        let escaped = serde_json::to_string(fragment).unwrap_or_default();
        let escaped = escaped.trim_matches('"').as_bytes();
        serialized
            .windows(fragment.len())
            .any(|window| window == fragment.as_bytes())
            || serialized
                .windows(escaped.len())
                .any(|window| window == escaped)
    }) {
        return Err(ProjectionError::UnsafeProjection(
            "projection contains a private snapshot fragment",
        ));
    }
    Ok(())
}

fn allowed_path(path: &str) -> bool {
    !path.is_empty()
        && path.ends_with(".md")
        && !path.starts_with('/')
        && !path.contains("..")
        && !path.contains(':')
        && !path.contains("//")
        && ALLOWED_LINK_PREFIXES
            .iter()
            .any(|prefix| path.starts_with(prefix))
}

fn validate_hash(kind: &'static str, hash: &str) -> Result<(), ProjectionError> {
    if hash.len() != 64
        || !hash
            .bytes()
            .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
    {
        return Err(ProjectionError::UnsafeProjection(match kind {
            "model" => "model hash must be lowercase SHA-256",
            "prompt" => "prompt hash must be lowercase SHA-256",
            "snapshot" => "snapshot hash must be lowercase SHA-256",
            "output" => "output hash must be lowercase SHA-256",
            "citation evidence" => "citation hash must be lowercase SHA-256",
            "page revision" => "page revision hash must be lowercase SHA-256",
            "changeset" => "changeset hash must be lowercase SHA-256",
            "approval candidate" => "approval hash must be lowercase SHA-256",
            _ => "hash must be lowercase SHA-256",
        }));
    }
    Ok(())
}

fn verify_hash(kind: &'static str, bytes: &[u8], expected: &str) -> Result<(), ProjectionError> {
    validate_hash(kind, expected)?;
    if sha256(bytes) != expected {
        return Err(ProjectionError::HashMismatch(kind));
    }
    Ok(())
}

fn verify_snapshot_hash(snapshot: &Snapshot) -> Result<(), ProjectionError> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "bytes": snapshot.bytes,
        "projection_input": snapshot.projection_input,
        "private_fragments": snapshot.private_fragments,
    }))
    .map_err(|_| ProjectionError::Serialization)?;
    verify_hash("snapshot", &bytes, &snapshot.sha256)
}

fn sha256(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
