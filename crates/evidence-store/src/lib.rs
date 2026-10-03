use std::{
    collections::BTreeMap,
    error::Error,
    fmt,
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use cap_fs_ext::{FollowSymlinks, OpenOptionsFollowExt};
use cap_primitives::fs::open_dir_nofollow;
use cap_std::{
    ambient_authority,
    fs::{Dir, OpenOptions},
};
use google_cloud_gax::{error::rpc::Code, retry_policy::NeverRetry};
use google_cloud_storage::{client::Storage, read_resume_policy::NeverResume};
use sha2::{Digest, Sha256};

static TEMP_FILE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceContext {
    source: String,
    observed_date: String,
    provenance: Option<EvidenceProvenance>,
}

impl EvidenceContext {
    pub fn new(source: &str, observed_date: &str) -> Result<Self, StoreError> {
        if !valid_source(source) {
            return Err(StoreError::InvalidContext("source"));
        }
        if !valid_date(observed_date) {
            return Err(StoreError::InvalidContext("observed_date"));
        }
        Ok(Self {
            source: source.to_owned(),
            observed_date: observed_date.to_owned(),
            provenance: None,
        })
    }

    pub fn with_provenance(mut self, provenance: EvidenceProvenance) -> Self {
        self.provenance = Some(provenance);
        self
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceProvenance {
    parser_version: String,
    observation_id: String,
    replay_inputs: BTreeMap<String, String>,
}

impl EvidenceProvenance {
    pub fn new<K, V>(
        parser_version: &str,
        observation_id: &str,
        replay_inputs: impl IntoIterator<Item = (K, V)>,
    ) -> Result<Self, StoreError>
    where
        K: Into<String>,
        V: Into<String>,
    {
        if !valid_metadata_value(parser_version, 128) {
            return Err(StoreError::InvalidContext("parser_version"));
        }
        if !valid_metadata_value(observation_id, 256) {
            return Err(StoreError::InvalidContext("observation_id"));
        }
        let replay_inputs = replay_inputs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect::<BTreeMap<_, _>>();
        if !valid_replay_inputs(&replay_inputs) {
            return Err(StoreError::InvalidContext("replay_inputs"));
        }
        Ok(Self {
            parser_version: parser_version.to_owned(),
            observation_id: observation_id.to_owned(),
            replay_inputs,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceObject {
    name: String,
    digest: [u8; 32],
}

impl EvidenceObject {
    pub fn parse(name: &str) -> Result<Self, StoreError> {
        let parts = name.split('/').collect::<Vec<_>>();
        if parts.len() != 6
            || parts[0] != "evidence"
            || !valid_source(parts[1])
            || !valid_date(parts[2])
            || parts[3] != "sha256"
            || parts[4].len() != 2
            || parts[5].len() != 64
            || !parts[5]
                .bytes()
                .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
            || parts[4] != &parts[5][..2]
        {
            return Err(StoreError::InvalidObjectName);
        }
        let digest = parse_hex_digest(parts[5]).ok_or(StoreError::InvalidObjectName)?;
        Ok(Self {
            name: name.to_owned(),
            digest,
        })
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn sha256(&self) -> String {
        hex_digest(&self.digest)
    }

    fn for_bytes(context: &EvidenceContext, bytes: &[u8]) -> Self {
        let digest: [u8; 32] = Sha256::digest(bytes).into();
        let digest_hex = hex_digest(&digest);
        Self {
            name: format!(
                "evidence/{}/{}/sha256/{}/{}",
                context.source,
                context.observed_date,
                &digest_hex[..2],
                digest_hex
            ),
            digest,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CreateDisposition {
    Created,
    AlreadyPresent,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CreateReceipt {
    pub object: EvidenceObject,
    pub disposition: CreateDisposition,
}

#[allow(async_fn_in_trait)]
pub trait EvidenceStore {
    /// Publish by content address without exercising read authority.
    async fn create_only(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, StoreError>;

    async fn create(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, StoreError>;

    async fn read(&self, object: &EvidenceObject) -> Result<Vec<u8>, StoreError>;
}

#[derive(Clone, Debug)]
pub struct FilesystemEvidenceStore {
    root: Arc<Dir>,
}

impl FilesystemEvidenceStore {
    pub fn open(root: impl AsRef<Path>) -> Result<Self, StoreError> {
        let root = if root.as_ref().is_absolute() {
            root.as_ref().to_owned()
        } else {
            std::env::current_dir()?.join(root)
        };
        let anchor = root
            .ancestors()
            .last()
            .filter(|path| !path.as_os_str().is_empty())
            .ok_or(StoreError::InvalidObjectPath)?;
        let relative = root
            .strip_prefix(anchor)
            .map_err(|_| StoreError::InvalidObjectPath)?;
        let mut current = Dir::open_ambient_dir(anchor, ambient_authority())?;
        let mut logical_path = anchor.to_owned();
        for component in relative.components() {
            let Component::Normal(component) = component else {
                return Err(StoreError::UnsafePath { path: root });
            };
            logical_path.push(component);
            current = open_or_create_directory(&current, component, &logical_path)?;
        }
        Ok(Self {
            root: Arc::new(current),
        })
    }

    fn secure_parent(&self, object: &EvidenceObject) -> Result<Dir, StoreError> {
        let relative_parent = Path::new(object.name())
            .parent()
            .ok_or(StoreError::InvalidObjectPath)?;
        let mut current = self.root.try_clone()?;
        let mut logical_path = PathBuf::new();
        for component in relative_parent.components() {
            let Component::Normal(component) = component else {
                return Err(StoreError::UnsafePath {
                    path: relative_parent.to_owned(),
                });
            };
            logical_path.push(component);
            current = open_or_create_directory(&current, component, &logical_path)?;
        }
        Ok(current)
    }

    fn secure_path(&self, object: &EvidenceObject) -> Result<(Dir, PathBuf), StoreError> {
        let parent = self.secure_parent(object)?;
        let file_name = Path::new(object.name())
            .file_name()
            .ok_or(StoreError::InvalidObjectPath)?;
        Ok((parent, file_name.into()))
    }

    fn read_verified(&self, object: &EvidenceObject) -> Result<Vec<u8>, StoreError> {
        let (parent, file_name) = self.secure_path(object)?;
        let mut file = open_regular_file_nofollow(&parent, &file_name)?;
        let mut bytes = Vec::new();
        file.read_to_end(&mut bytes)?;
        let actual: [u8; 32] = Sha256::digest(&bytes).into();
        if actual != object.digest {
            return Err(StoreError::DigestMismatch {
                object: object.name.clone(),
                expected: object.sha256(),
                actual: hex_digest(&actual),
            });
        }
        Ok(bytes)
    }
}

impl EvidenceStore for FilesystemEvidenceStore {
    async fn create_only(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, StoreError> {
        let object = EvidenceObject::for_bytes(context, evidence);
        let parent = self.secure_parent(&object)?;
        let file_name = Path::new(object.name())
            .file_name()
            .ok_or(StoreError::InvalidObjectPath)?;

        let sequence = TEMP_FILE_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temporary_path = format!(".evidence-{}-{sequence}.tmp", std::process::id());
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        let mut temporary = parent.open_with(&temporary_path, &options)?;
        temporary.write_all(evidence)?;
        temporary.sync_all()?;
        drop(temporary);

        let disposition = match parent.hard_link(&temporary_path, &parent, file_name) {
            Ok(()) => {
                sync_directory(&parent)?;
                CreateDisposition::Created
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                CreateDisposition::AlreadyPresent
            }
            Err(error) => {
                let _ = parent.remove_file(&temporary_path);
                return Err(error.into());
            }
        };
        parent.remove_file(&temporary_path)?;
        sync_directory(&parent)?;

        Ok(CreateReceipt {
            object,
            disposition,
        })
    }

    async fn create(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, StoreError> {
        let receipt = self.create_only(context, evidence).await?;
        if receipt.disposition == CreateDisposition::AlreadyPresent {
            self.read_verified(&receipt.object)?;
        }
        Ok(receipt)
    }

    async fn read(&self, object: &EvidenceObject) -> Result<Vec<u8>, StoreError> {
        self.read_verified(object)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcsCreateRequest {
    bucket: String,
    name: String,
    bytes: Vec<u8>,
    metadata: BTreeMap<String, String>,
    if_generation_match: Option<i64>,
}

impl GcsCreateRequest {
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }

    pub const fn if_generation_match(&self) -> Option<i64> {
        self.if_generation_match
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum GcsClientError {
    PreconditionFailed,
    Retryable,
    Fatal,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GcsReadObject {
    bytes: Vec<u8>,
    metadata: BTreeMap<String, String>,
}

impl GcsReadObject {
    pub fn new(bytes: Vec<u8>, metadata: BTreeMap<String, String>) -> Self {
        Self { bytes, metadata }
    }

    pub fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub fn metadata(&self) -> &BTreeMap<String, String> {
        &self.metadata
    }
}

#[async_trait]
pub trait GcsObjectClient: Send + Sync {
    async fn create_object(&self, request: GcsCreateRequest) -> Result<(), GcsClientError>;

    async fn read_object(&self, bucket: &str, name: &str) -> Result<GcsReadObject, GcsClientError>;
}

#[derive(Clone)]
pub struct GoogleCloudStorageClient {
    client: Storage,
}

impl GoogleCloudStorageClient {
    pub async fn from_application_default_credentials() -> Result<Self, StoreError> {
        let client = Storage::builder()
            .build()
            .await
            .map_err(|_| StoreError::Authentication)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl GcsObjectClient for GoogleCloudStorageClient {
    async fn create_object(&self, request: GcsCreateRequest) -> Result<(), GcsClientError> {
        self.client
            .write_object(
                bucket_resource(&request.bucket),
                request.name,
                bytes::Bytes::from(request.bytes),
            )
            .set_if_generation_match(
                request
                    .if_generation_match
                    .expect("GCS create request always carries a precondition"),
            )
            .set_metadata(request.metadata)
            .with_retry_policy(NeverRetry)
            .send_buffered()
            .await
            .map(|_| ())
            .map_err(classify_gcs_error)
    }

    async fn read_object(&self, bucket: &str, name: &str) -> Result<GcsReadObject, GcsClientError> {
        let mut reader = self
            .client
            .read_object(bucket_resource(bucket), name)
            .with_retry_policy(NeverRetry)
            .with_read_resume_policy(NeverResume)
            .send()
            .await
            .map_err(classify_gcs_error)?;
        let metadata = reader.object().metadata.into_iter().collect();
        let mut bytes = Vec::new();
        while let Some(chunk) = reader
            .next()
            .await
            .transpose()
            .map_err(classify_gcs_error)?
        {
            bytes.extend_from_slice(&chunk);
        }
        Ok(GcsReadObject { bytes, metadata })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct GcsRetryPolicy {
    max_attempts: u8,
    base_delay: Duration,
}

impl GcsRetryPolicy {
    pub fn new(max_attempts: u8) -> Result<Self, StoreError> {
        if !(1..=10).contains(&max_attempts) {
            return Err(StoreError::InvalidRetryPolicy);
        }
        Ok(Self {
            max_attempts,
            base_delay: Duration::ZERO,
        })
    }

    pub fn with_base_delay(mut self, base_delay: Duration) -> Result<Self, StoreError> {
        if base_delay > Duration::from_secs(5) {
            return Err(StoreError::InvalidRetryPolicy);
        }
        self.base_delay = base_delay;
        Ok(self)
    }

    async fn delay(self, completed_attempts: u8) {
        if !self.base_delay.is_zero() {
            let factor = 1_u32 << u32::from(completed_attempts.saturating_sub(1).min(6));
            tokio::time::sleep(self.base_delay.saturating_mul(factor)).await;
        }
    }
}

#[derive(Clone, Debug)]
pub struct GcsEvidenceStore<C> {
    client: C,
    bucket: String,
    retry: GcsRetryPolicy,
}

impl<C> GcsEvidenceStore<C> {
    pub fn new(client: C, bucket: &str, retry: GcsRetryPolicy) -> Result<Self, StoreError> {
        if !valid_bucket(bucket) {
            return Err(StoreError::InvalidContext("bucket"));
        }
        Ok(Self {
            client,
            bucket: bucket.to_owned(),
            retry,
        })
    }
}

impl<C: GcsObjectClient> GcsEvidenceStore<C> {
    fn metadata_for(
        context: &EvidenceContext,
        object: &EvidenceObject,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        let provenance = context
            .provenance
            .as_ref()
            .ok_or(StoreError::MissingProvenance)?;
        let replay_inputs = serde_json::to_string(&provenance.replay_inputs)
            .map_err(|_| StoreError::InvalidContext("replay_inputs"))?;
        Ok(BTreeMap::from([
            (
                "evidence-observation-id".to_owned(),
                provenance.observation_id.clone(),
            ),
            (
                "evidence-observed-date".to_owned(),
                context.observed_date.clone(),
            ),
            (
                "evidence-parser-version".to_owned(),
                provenance.parser_version.clone(),
            ),
            ("evidence-replay-inputs".to_owned(), replay_inputs),
            ("evidence-sha256".to_owned(), object.sha256()),
            ("evidence-source".to_owned(), context.source.clone()),
        ]))
    }

    async fn read_verified(
        &self,
        object: &EvidenceObject,
        expected_metadata: Option<&BTreeMap<String, String>>,
    ) -> Result<Vec<u8>, StoreError> {
        let mut attempts = 0_u8;
        let response = loop {
            attempts += 1;
            match self.client.read_object(&self.bucket, object.name()).await {
                Ok(response) => break response,
                Err(GcsClientError::Retryable) if attempts < self.retry.max_attempts => {
                    self.retry.delay(attempts).await;
                }
                Err(_) => return Err(StoreError::Remote),
            }
        };
        let bytes = verify_digest(object, response.bytes)?;
        if expected_metadata.is_some_and(|expected| expected != &response.metadata) {
            return Err(StoreError::MetadataMismatch {
                object: object.name.clone(),
            });
        }
        Ok(bytes)
    }
}

impl<C: GcsObjectClient> EvidenceStore for GcsEvidenceStore<C> {
    async fn create_only(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, StoreError> {
        let object = EvidenceObject::for_bytes(context, evidence);
        let metadata = Self::metadata_for(context, &object)?;
        let request = GcsCreateRequest {
            bucket: self.bucket.clone(),
            name: object.name().to_owned(),
            bytes: evidence.to_vec(),
            metadata,
            if_generation_match: Some(0),
        };

        let mut attempts = 0_u8;
        let disposition = loop {
            attempts += 1;
            match self.client.create_object(request.clone()).await {
                Ok(()) => break CreateDisposition::Created,
                Err(GcsClientError::PreconditionFailed) => break CreateDisposition::AlreadyPresent,
                Err(GcsClientError::Retryable) if attempts < self.retry.max_attempts => {
                    self.retry.delay(attempts).await;
                }
                Err(_) => return Err(StoreError::Remote),
            }
        };
        Ok(CreateReceipt {
            object,
            disposition,
        })
    }

    async fn create(
        &self,
        context: &EvidenceContext,
        evidence: &[u8],
    ) -> Result<CreateReceipt, StoreError> {
        let receipt = self.create_only(context, evidence).await?;
        if receipt.disposition == CreateDisposition::AlreadyPresent {
            let metadata = Self::metadata_for(context, &receipt.object)?;
            self.read_verified(&receipt.object, Some(&metadata)).await?;
        }
        Ok(receipt)
    }

    async fn read(&self, object: &EvidenceObject) -> Result<Vec<u8>, StoreError> {
        self.read_verified(object, None).await
    }
}

#[derive(Debug)]
pub enum StoreError {
    InvalidContext(&'static str),
    InvalidObjectName,
    InvalidObjectPath,
    UnsafePath {
        path: PathBuf,
    },
    DigestMismatch {
        object: String,
        expected: String,
        actual: String,
    },
    MetadataMismatch {
        object: String,
    },
    MissingProvenance,
    InvalidRetryPolicy,
    Authentication,
    Remote,
    Io(std::io::Error),
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidContext(field) => write!(formatter, "invalid evidence {field}"),
            Self::InvalidObjectName => formatter.write_str("invalid evidence object name"),
            Self::InvalidObjectPath => formatter.write_str("invalid evidence object path"),
            Self::UnsafePath { path } => {
                write!(formatter, "unsafe evidence path: {}", path.display())
            }
            Self::DigestMismatch { object, .. } => {
                write!(formatter, "evidence digest mismatch for {object}")
            }
            Self::MetadataMismatch { object } => {
                write!(formatter, "evidence metadata mismatch for {object}")
            }
            Self::MissingProvenance => formatter.write_str("evidence provenance is required"),
            Self::InvalidRetryPolicy => formatter.write_str("invalid evidence retry policy"),
            Self::Authentication => {
                formatter.write_str("GCS application-default authentication failed")
            }
            Self::Remote => formatter.write_str("evidence remote storage operation failed"),
            Self::Io(error) => write!(formatter, "evidence storage I/O failed: {error}"),
        }
    }
}

impl Error for StoreError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            _ => None,
        }
    }
}

impl From<std::io::Error> for StoreError {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

fn valid_source(source: &str) -> bool {
    !source.is_empty()
        && source.len() <= 64
        && source.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
        && source
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && source
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn valid_replay_inputs(inputs: &BTreeMap<String, String>) -> bool {
    valid_rpc_replay_inputs(inputs) || valid_collection_replay_inputs(inputs)
}

fn valid_rpc_replay_inputs(inputs: &BTreeMap<String, String>) -> bool {
    if inputs.len() != 6 {
        return false;
    }
    let valid_attempt = inputs
        .get("attempt")
        .is_some_and(|value| value.parse::<u16>().is_ok());
    let valid_chain = inputs
        .get("chain")
        .is_some_and(|value| matches!(value.as_str(), "ethereum" | "base" | "solana" | "tempo"));
    let valid_height = inputs
        .get("height")
        .is_some_and(|value| value.parse::<u64>().is_ok());
    let valid_http_status = inputs
        .get("http-status")
        .and_then(|value| value.parse::<u16>().ok())
        .is_some_and(|value| (100..=599).contains(&value));
    let valid_method = inputs
        .get("method")
        .is_some_and(|value| matches!(value.as_str(), "eth_getBlockByNumber" | "getBlock"));
    let valid_provider = inputs
        .get("provider")
        .is_some_and(|value| value == "alchemy");
    valid_attempt
        && valid_chain
        && valid_height
        && valid_http_status
        && valid_method
        && valid_provider
}

fn valid_collection_replay_inputs(inputs: &BTreeMap<String, String>) -> bool {
    inputs.len() == 5
        && inputs
            .get("chain")
            .is_some_and(|value| matches!(value.as_str(), "ethereum" | "base" | "solana" | "tempo"))
        && inputs
            .get("height")
            .is_some_and(|value| value.parse::<u64>().is_ok())
        && inputs.get("input-kind").is_some_and(|value| {
            matches!(
                value.as_str(),
                "chain_transfer"
                    | "x402_runtime"
                    | "x402_well_known"
                    | "x402_openapi"
                    | "mpp_openapi"
            )
        })
        && inputs
            .get("provider")
            .is_some_and(|value| value == "collection-manifest")
        && inputs
            .get("source")
            .is_some_and(|value| valid_source(value))
}

fn valid_metadata_value(value: &str, max_length: usize) -> bool {
    !value.is_empty()
        && value.len() <= max_length
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/' | b'@')
        })
}

fn valid_bucket(bucket: &str) -> bool {
    (3..=222).contains(&bucket.len())
        && bucket.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || matches!(byte, b'-' | b'_' | b'.')
        })
        && bucket
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && bucket
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn bucket_resource(bucket: &str) -> String {
    format!("projects/_/buckets/{bucket}")
}

fn classify_gcs_error(error: google_cloud_storage::Error) -> GcsClientError {
    if matches!(error.http_status_code(), Some(409 | 412)) {
        return GcsClientError::PreconditionFailed;
    }
    match error.status().map(|status| status.code) {
        Some(Code::FailedPrecondition | Code::AlreadyExists) => GcsClientError::PreconditionFailed,
        Some(
            Code::DeadlineExceeded | Code::ResourceExhausted | Code::Internal | Code::Unavailable,
        ) => GcsClientError::Retryable,
        None if error.is_timeout() => GcsClientError::Retryable,
        _ => GcsClientError::Fatal,
    }
}

fn valid_date(date: &str) -> bool {
    let bytes = date.as_bytes();
    if bytes.len() != 10
        || bytes[4] != b'-'
        || bytes[7] != b'-'
        || bytes
            .iter()
            .enumerate()
            .any(|(index, byte)| !matches!(index, 4 | 7) && !byte.is_ascii_digit())
    {
        return false;
    }
    let month = date[5..7].parse::<u8>().ok();
    let day = date[8..10].parse::<u8>().ok();
    matches!(month, Some(1..=12)) && matches!(day, Some(1..=31))
}

fn hex_digest(digest: &[u8; 32]) -> String {
    use fmt::Write as _;

    let mut value = String::with_capacity(64);
    for byte in digest {
        write!(&mut value, "{byte:02x}").expect("writing to String cannot fail");
    }
    value
}

fn parse_hex_digest(value: &str) -> Option<[u8; 32]> {
    let mut digest = [0_u8; 32];
    for (index, output) in digest.iter_mut().enumerate() {
        let offset = index * 2;
        *output = u8::from_str_radix(&value[offset..offset + 2], 16).ok()?;
    }
    if hex_digest(&digest) == value {
        Some(digest)
    } else {
        None
    }
}

fn verify_digest(object: &EvidenceObject, bytes: Vec<u8>) -> Result<Vec<u8>, StoreError> {
    let actual: [u8; 32] = Sha256::digest(&bytes).into();
    if actual != object.digest {
        return Err(StoreError::DigestMismatch {
            object: object.name.clone(),
            expected: object.sha256(),
            actual: hex_digest(&actual),
        });
    }
    Ok(bytes)
}

fn open_or_create_directory(
    parent: &Dir,
    component: &std::ffi::OsStr,
    logical_path: &Path,
) -> Result<Dir, StoreError> {
    match open_directory_nofollow(parent, component, logical_path) {
        Ok(directory) => Ok(directory),
        Err(StoreError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
            match parent.create_dir(component) {
                Ok(()) => sync_directory(parent)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
            open_directory_nofollow(parent, component, logical_path)
        }
        Err(error) => Err(error),
    }
}

fn open_directory_nofollow(
    parent: &Dir,
    component: &std::ffi::OsStr,
    logical_path: &Path,
) -> Result<Dir, StoreError> {
    let parent_file = parent.try_clone()?.into_std_file();
    open_dir_nofollow(&parent_file, Path::new(component))
        .map(Dir::from_std_file)
        .map_err(|error| {
            if matches!(
                parent.symlink_metadata(component),
                Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir()
            ) {
                StoreError::UnsafePath {
                    path: logical_path.to_owned(),
                }
            } else {
                error.into()
            }
        })
}

fn open_regular_file_nofollow(directory: &Dir, path: &Path) -> std::io::Result<cap_std::fs::File> {
    let mut options = OpenOptions::new();
    options.read(true).follow(FollowSymlinks::No);
    #[cfg(unix)]
    {
        use cap_fs_ext::OpenOptionsExt;

        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = directory.open_with(path, &options)?;
    if !file.metadata()?.file_type().is_file() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "evidence object is not a regular file",
        ));
    }
    Ok(file)
}

fn sync_directory(directory: &Dir) -> Result<(), StoreError> {
    #[cfg(unix)]
    directory.open(".")?.sync_all()?;
    #[cfg(not(unix))]
    directory.try_clone()?.into_std_file().sync_all()?;
    Ok(())
}

#[cfg(all(test, unix))]
mod tests {
    use std::{fs, os::unix::fs::symlink, process::Command, sync::mpsc, thread, time::Duration};

    use super::*;

    #[test]
    fn final_object_symlink_is_never_followed() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = fs::canonicalize(temporary.path()).expect("canonical temporary root");
        let outside = root
            .parent()
            .expect("temporary parent")
            .join("outside-evidence");
        fs::write(&outside, b"outside").expect("write outside fixture");
        symlink(&outside, root.join("object")).expect("install final symlink");
        let directory = Dir::open_ambient_dir(&root, ambient_authority()).expect("open root");

        let error = open_regular_file_nofollow(&directory, Path::new("object"))
            .expect_err("final symlink must fail closed");

        assert_ne!(error.kind(), std::io::ErrorKind::NotFound);
        fs::remove_file(outside).expect("remove outside fixture");
    }

    #[test]
    fn final_object_fifo_is_rejected_promptly() {
        let temporary = tempfile::tempdir().expect("temporary directory");
        let root = fs::canonicalize(temporary.path()).expect("canonical temporary root");
        let fifo = root.join("object");
        assert!(
            Command::new("mkfifo")
                .arg(&fifo)
                .status()
                .expect("run mkfifo")
                .success()
        );
        let directory = Dir::open_ambient_dir(&root, ambient_authority()).expect("open root");
        let (sender, receiver) = mpsc::channel();

        thread::spawn(move || {
            let result = open_regular_file_nofollow(&directory, Path::new("object"));
            let _ = sender.send(result.map(|_| ()));
        });

        let result = receiver
            .recv_timeout(Duration::from_secs(1))
            .expect("FIFO rejection must not block");
        assert!(result.is_err());
    }
}
