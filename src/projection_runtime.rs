use std::{
    collections::BTreeMap,
    env, fmt,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use reqwest::{Client, StatusCode, Url};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use sha2::{Digest, Sha256};

use crate::projection::{
    NativeChangeset, NativeRevision, NativeWiki, ProjectionApproval, ProjectionError,
    ProjectionGateway, ProjectionInput, ProjectionJob, ProjectionOutput, ProjectionWorker,
    Projector, PublishedProjection, Snapshot,
};

#[derive(Clone)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("[REDACTED]")
    }
}

#[derive(Clone, Debug)]
pub struct ProjectionRuntimeConfig {
    gateway_url: Url,
    gateway_token: SecretString,
    model_url: Url,
    model_token: SecretString,
    wiki_rpc_url: Url,
    wiki_token: SecretString,
}

impl ProjectionRuntimeConfig {
    pub fn new(
        gateway_url: &str,
        gateway_token: SecretString,
        model_url: &str,
        model_token: SecretString,
        wiki_rpc_url: &str,
        wiki_token: SecretString,
    ) -> Result<Self, String> {
        let gateway_url = secure_url(gateway_url, false)?;
        let model_url = secure_url(model_url, true)?;
        let wiki_rpc_url = secure_url(wiki_rpc_url, true)?;
        if !is_loopback(&model_url) {
            return Err("projection model endpoint must be loopback-local".into());
        }
        for token in [&gateway_token, &model_token, &wiki_token] {
            if token.expose().is_empty() {
                return Err("projection bearer tokens must not be empty".into());
            }
        }
        Ok(Self {
            gateway_url,
            gateway_token,
            model_url,
            model_token,
            wiki_rpc_url,
            wiki_token,
        })
    }

    pub fn from_env() -> Result<Self, String> {
        Self::new(
            &required_env("PROJECTION_GATEWAY_URL")?,
            SecretString::new(required_env("PROJECTION_GATEWAY_TOKEN")?),
            &required_env("PROJECTION_MODEL_URL")?,
            SecretString::new(required_env("PROJECTION_MODEL_TOKEN")?),
            &required_env("HERMES_WIKI_RPC_URL")?,
            SecretString::new(required_env("HERMES_WIKI_TOKEN")?),
        )
    }
}

fn required_env(name: &str) -> Result<String, String> {
    env::var(name).map_err(|_| format!("missing required projection setting {name}"))
}

fn secure_url(value: &str, allow_loopback_http: bool) -> Result<Url, String> {
    let url = Url::parse(value).map_err(|_| "projection endpoint is not a valid URL")?;
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(
            "projection endpoints cannot contain credentials, queries, or fragments".into(),
        );
    }
    let loopback = is_loopback(&url);
    if url.scheme() != "https" && !(allow_loopback_http && url.scheme() == "http" && loopback) {
        return Err("projection endpoints require HTTPS except for a loopback wiki RPC".into());
    }
    Ok(url)
}

fn is_loopback(url: &Url) -> bool {
    matches!(url.host_str(), Some("127.0.0.1" | "::1" | "localhost"))
}

fn endpoint(base: &Url, path: &str) -> Result<Url, ProjectionError> {
    base.join(path)
        .map_err(|_| ProjectionError::Gateway("invalid configured endpoint".into()))
}

#[derive(Clone)]
struct HttpProjectionGateway {
    client: Client,
    base_url: Url,
    token: SecretString,
}

#[async_trait]
impl ProjectionGateway for HttpProjectionGateway {
    async fn pull_job(&self) -> Result<Option<ProjectionJob>, ProjectionError> {
        let response = self
            .client
            .get(endpoint(&self.base_url, "api/v1/projection/jobs/next")?)
            .bearer_auth(self.token.expose())
            .send()
            .await
            .map_err(|_| ProjectionError::Gateway("job pull failed".into()))?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        decode_success(response, "job pull").await.map(Some)
    }

    async fn fetch_snapshot(&self, job: &ProjectionJob) -> Result<Snapshot, ProjectionError> {
        let path = format!("api/v1/projection/jobs/{}/snapshot", job.job_id);
        let response = self
            .client
            .get(endpoint(&self.base_url, &path)?)
            .bearer_auth(self.token.expose())
            .send()
            .await
            .map_err(|_| ProjectionError::Gateway("snapshot pull failed".into()))?;
        decode_success(response, "snapshot pull").await
    }

    async fn approval_for(
        &self,
        job: &ProjectionJob,
        candidate_sha256: &str,
    ) -> Result<Option<ProjectionApproval>, ProjectionError> {
        let path = format!(
            "api/v1/projection/jobs/{}/approvals/{candidate_sha256}",
            job.job_id
        );
        let response = self
            .client
            .get(endpoint(&self.base_url, &path)?)
            .bearer_auth(self.token.expose())
            .send()
            .await
            .map_err(|_| ProjectionError::Gateway("approval pull failed".into()))?;
        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }
        decode_success(response, "approval pull").await.map(Some)
    }

    async fn publish_bundle_if_absent(
        &self,
        job: &ProjectionJob,
        bundle_bytes: &[u8],
        bundle_sha256: &str,
    ) -> Result<(), ProjectionError> {
        let path = format!(
            "api/v1/projection/jobs/{}/bundles/{bundle_sha256}",
            job.job_id
        );
        let response = self
            .client
            .put(endpoint(&self.base_url, &path)?)
            .bearer_auth(self.token.expose())
            .header("if-none-match", "*")
            .header("content-type", "application/json")
            .body(bundle_bytes.to_vec())
            .send()
            .await
            .map_err(|_| ProjectionError::Gateway("bundle publication failed".into()))?;
        if response.status().is_success() {
            Ok(())
        } else if response.status() == StatusCode::PRECONDITION_FAILED {
            Err(ProjectionError::Gateway(
                "publication already exists but exact bytes were not verified".into(),
            ))
        } else {
            Err(ProjectionError::Gateway(
                "bundle publication was rejected".into(),
            ))
        }
    }
}

#[derive(Clone)]
struct HttpProjector {
    client: Client,
    url: Url,
    token: SecretString,
}

#[derive(Serialize)]
struct ModelProjectionRequest<'a> {
    job: &'a ProjectionJob,
    input: &'a ProjectionInput,
}

#[derive(Deserialize)]
struct ModelProjectionResponse {
    generated_markdown: String,
    citations: Vec<crate::projection::Citation>,
    wikilinks: Vec<String>,
}

#[async_trait]
impl Projector for HttpProjector {
    async fn project(
        &self,
        job: &ProjectionJob,
        input: &ProjectionInput,
    ) -> Result<ProjectionOutput, ProjectionError> {
        let response = self
            .client
            .post(self.url.clone())
            .bearer_auth(self.token.expose())
            .json(&ModelProjectionRequest { job, input })
            .send()
            .await
            .map_err(|_| ProjectionError::Projector("local model request failed".into()))?;
        if !response.status().is_success() {
            return Err(ProjectionError::Projector(
                "local model request was rejected".into(),
            ));
        }
        let projected: ModelProjectionResponse = response
            .json()
            .await
            .map_err(|_| ProjectionError::Projector("invalid local model response".into()))?;
        let hash_bytes = serde_json::to_vec(&serde_json::json!({
            "generated_markdown": projected.generated_markdown,
            "citations": projected.citations,
            "wikilinks": projected.wikilinks,
        }))
        .map_err(|_| ProjectionError::Serialization)?;
        Ok(ProjectionOutput {
            generated_markdown: projected.generated_markdown,
            citations: projected.citations,
            wikilinks: projected.wikilinks,
            output_sha256: format!("{:x}", Sha256::digest(hash_bytes)),
        })
    }
}

#[derive(Clone)]
struct HermesNativeWiki {
    client: Client,
    rpc_url: Url,
    token: SecretString,
    captured: Arc<Mutex<Option<CapturedWikiWrite>>>,
}

#[derive(Clone)]
struct CapturedWikiWrite {
    wiki_id: String,
    page_path: String,
    output_sha256: String,
    revision: NativeRevision,
    changeset: NativeChangeset,
}

#[derive(Serialize)]
struct RpcRequest<T> {
    jsonrpc: &'static str,
    id: &'static str,
    method: &'static str,
    params: T,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
}

impl HermesNativeWiki {
    async fn call_response<T: Serialize, R: DeserializeOwned>(
        &self,
        method: &'static str,
        params: T,
    ) -> Result<RpcResponse<R>, ProjectionError> {
        let response = self
            .client
            .post(self.rpc_url.clone())
            .bearer_auth(self.token.expose())
            .json(&RpcRequest {
                jsonrpc: "2.0",
                id: "projection-worker",
                method,
                params,
            })
            .send()
            .await
            .map_err(|_| ProjectionError::Wiki("native wiki RPC failed".into()))?;
        if !response.status().is_success() {
            return Err(ProjectionError::Wiki("native wiki RPC was rejected".into()));
        }
        response
            .json()
            .await
            .map_err(|_| ProjectionError::Wiki("invalid native wiki RPC response".into()))
    }

    async fn call<T: Serialize, R: DeserializeOwned>(
        &self,
        method: &'static str,
        params: T,
    ) -> Result<R, ProjectionError> {
        let rpc = self.call_response(method, params).await?;
        if let Some(error) = rpc.error {
            return Err(ProjectionError::Wiki(format!(
                "native wiki RPC error {}",
                error.code
            )));
        }
        rpc.result
            .ok_or_else(|| ProjectionError::Wiki("native wiki RPC returned no result".into()))
    }
}

const GENERATED_BEGIN: &str = "<!-- BEGIN GENERATED: agent-economy-projection -->";
const GENERATED_END: &str = "<!-- END GENERATED: agent-economy-projection -->";

pub fn merge_generated_section(existing: &str, generated: &str) -> Result<String, String> {
    if generated.contains(GENERATED_BEGIN) || generated.contains(GENERATED_END) {
        return Err("generated content contains reserved section markers".into());
    }
    if existing.matches(GENERATED_BEGIN).count() > 1 || existing.matches(GENERATED_END).count() > 1
    {
        return Err("existing page has duplicate generated section markers".into());
    }
    let section = format!(
        "{GENERATED_BEGIN}\n{}\n{GENERATED_END}",
        generated.trim_end()
    );
    match (existing.find(GENERATED_BEGIN), existing.find(GENERATED_END)) {
        (None, None) => {
            let separator = if existing.is_empty() || existing.ends_with("\n\n") {
                ""
            } else if existing.ends_with('\n') {
                "\n"
            } else {
                "\n\n"
            };
            Ok(format!("{existing}{separator}{section}\n"))
        }
        (Some(start), Some(end)) if start < end => {
            let suffix = end + GENERATED_END.len();
            Ok(format!(
                "{}{}{}",
                &existing[..start],
                section,
                &existing[suffix..]
            ))
        }
        _ => Err("existing page has malformed generated section markers".into()),
    }
}

#[derive(Serialize)]
struct WikiPageParams<'a> {
    wiki: &'a str,
    path: &'a str,
}

#[derive(Deserialize)]
struct WikiPageResult {
    body: String,
    #[serde(default)]
    frontmatter: BTreeMap<String, serde_json::Value>,
}

#[derive(Serialize)]
struct WikiUpdateParams<'a> {
    wiki: &'a str,
    path: &'a str,
    body: &'a str,
    if_match: Option<&'a str>,
    force: bool,
    trigger: &'a str,
    source_events: &'a [String],
    summary: &'a str,
}

#[derive(Deserialize)]
struct WikiUpdateResult {
    updated: String,
}

#[derive(Serialize)]
struct WikiChangesetsParams<'a> {
    wiki: &'a str,
    page: &'a str,
    trigger: &'a str,
    limit: u8,
    offset: u8,
}

#[derive(Deserialize)]
struct WikiChangesetsResult {
    changesets: Vec<WikiChangesetResult>,
}

#[derive(Deserialize, Serialize)]
struct WikiChangesetResult {
    id: String,
    timestamp: String,
    page: String,
    trigger: String,
    after_sha256: String,
}

#[async_trait]
impl NativeWiki for HermesNativeWiki {
    async fn replace_generated_section(
        &self,
        wiki_id: &str,
        page_path: &str,
        generated_markdown: &str,
        _wikilinks: &[String],
        output_sha256: &str,
    ) -> Result<NativeRevision, ProjectionError> {
        let page_rpc: RpcResponse<WikiPageResult> = self
            .call_response(
                "wiki.page",
                WikiPageParams {
                    wiki: wiki_id,
                    path: page_path,
                },
            )
            .await?;
        let (existing_body, if_match) = match (page_rpc.result, page_rpc.error) {
            (Some(page), None) => {
                let updated = page
                    .frontmatter
                    .get("updated")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_owned)
                    .ok_or_else(|| {
                        ProjectionError::Wiki(
                            "existing page has no optimistic-concurrency revision".into(),
                        )
                    })?;
                (page.body, Some(updated))
            }
            (None, Some(error)) if error.code == 4040 => (String::new(), Some(String::new())),
            (_, Some(error)) => {
                return Err(ProjectionError::Wiki(format!(
                    "native wiki RPC error {}",
                    error.code
                )));
            }
            _ => {
                return Err(ProjectionError::Wiki(
                    "native wiki page lookup returned no result".into(),
                ));
            }
        };
        let body = merge_generated_section(&existing_body, generated_markdown)
            .map_err(ProjectionError::Wiki)?;
        let trigger = format!("projection:{output_sha256}");
        let summary = format!("Approved semantic projection {output_sha256}");
        let updated: WikiUpdateResult = self
            .call(
                "wiki.update",
                WikiUpdateParams {
                    wiki: wiki_id,
                    path: page_path,
                    body: &body,
                    if_match: if_match.as_deref(),
                    force: false,
                    trigger: &trigger,
                    source_events: &[],
                    summary: &summary,
                },
            )
            .await?;
        let result: WikiChangesetsResult = self
            .call(
                "wiki.changesets",
                WikiChangesetsParams {
                    wiki: wiki_id,
                    page: page_path,
                    trigger: &trigger,
                    limit: 1,
                    offset: 0,
                },
            )
            .await?;
        let captured = result.changesets.into_iter().next().ok_or_else(|| {
            ProjectionError::Wiki("wiki.update did not capture a changeset".into())
        })?;
        if captured.page != page_path
            || captured.trigger != trigger
            || captured.timestamp != updated.updated
            || !is_sha256(&captured.after_sha256)
        {
            return Err(ProjectionError::Wiki(
                "captured wiki changeset does not match the projection write".into(),
            ));
        }
        let changeset_sha256 = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&captured).map_err(|_| ProjectionError::Serialization)?
            )
        );
        let revision = NativeRevision {
            id: captured.id.clone(),
            page_sha256: captured.after_sha256.clone(),
        };
        let changeset = NativeChangeset {
            id: captured.id,
            sha256: changeset_sha256,
            page_revision_id: revision.id.clone(),
            page_sha256: revision.page_sha256.clone(),
            output_sha256: output_sha256.to_owned(),
        };
        *self
            .captured
            .lock()
            .map_err(|_| ProjectionError::Wiki("wiki changeset cache is unavailable".into()))? =
            Some(CapturedWikiWrite {
                wiki_id: wiki_id.to_owned(),
                page_path: page_path.to_owned(),
                output_sha256: output_sha256.to_owned(),
                revision: revision.clone(),
                changeset,
            });
        Ok(revision)
    }

    async fn capture_changeset(
        &self,
        wiki_id: &str,
        page_path: &str,
        revision: &NativeRevision,
        output_sha256: &str,
    ) -> Result<NativeChangeset, ProjectionError> {
        let captured = self
            .captured
            .lock()
            .map_err(|_| ProjectionError::Wiki("wiki changeset cache is unavailable".into()))?
            .take()
            .ok_or_else(|| {
                ProjectionError::Wiki("no captured wiki changeset is available".into())
            })?;
        if captured.wiki_id != wiki_id
            || captured.page_path != page_path
            || captured.output_sha256 != output_sha256
            || captured.revision != *revision
        {
            return Err(ProjectionError::Wiki(
                "captured wiki changeset does not match the requested projection".into(),
            ));
        }
        Ok(captured.changeset)
    }
}

fn is_sha256(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn decode_success<T: DeserializeOwned>(
    response: reqwest::Response,
    operation: &'static str,
) -> Result<T, ProjectionError> {
    if !response.status().is_success() {
        return Err(ProjectionError::Gateway(format!(
            "{operation} was rejected"
        )));
    }
    response
        .json()
        .await
        .map_err(|_| ProjectionError::Gateway(format!("invalid {operation} response")))
}

pub async fn run_projection_once(
    config: ProjectionRuntimeConfig,
) -> Result<Option<PublishedProjection>, ProjectionError> {
    let client = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|_| ProjectionError::Gateway("HTTP client initialization failed".into()))?;
    let gateway = HttpProjectionGateway {
        client: client.clone(),
        base_url: config.gateway_url,
        token: config.gateway_token,
    };
    let projector = HttpProjector {
        client: client.clone(),
        url: config.model_url,
        token: config.model_token,
    };
    let wiki = HermesNativeWiki {
        client,
        rpc_url: config.wiki_rpc_url,
        token: config.wiki_token,
        captured: Arc::new(Mutex::new(None)),
    };
    ProjectionWorker::new(gateway, projector, wiki)
        .run_once()
        .await
}
