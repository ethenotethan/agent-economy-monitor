use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use agent_economy_evidence_store::{EvidenceContext, EvidenceProvenance, EvidenceStore};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use serde_json::{Value, json};
use thiserror::Error;
use tokio_postgres::Client;

mod enrichment;

pub use enrichment::{
    AlchemyHistoryRequest, BuyerHistoryEnricher, BuyerHistoryTarget, EnrichmentCandidate,
    EnrichmentCheckpoint, EnrichmentMode, EnrichmentReport, EnrichmentRequest,
    EnrichmentStateStore, FinalizedHistoryCache, FinalizedHistoryRecord, HistoryEvidenceArchive,
    HistoryFinality, HistoryItem, HistoryPage, HistoryTransport, POSTGRES_BUYER_ENRICHMENT_SCHEMA,
    PostgresEnrichmentStateStore, PostgresFinalizedHistoryCache, prioritize_automatic,
};

const MAX_RETRY_AFTER_MILLISECONDS: u64 = 60_000;
const MAX_RPC_RESPONSE_BYTES: usize = 8 * 1024 * 1024;
const MAX_RETRIES: u32 = 20;
const MAX_RUN_BLOCKS: u64 = 10_000;
const MAX_RUN_REQUESTS: u64 = 10_000;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Chain {
    Ethereum,
    Base,
    Solana,
    Tempo,
}

impl Chain {
    pub const ALL: [Self; 4] = [Self::Ethereum, Self::Base, Self::Solana, Self::Tempo];

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ethereum => "ethereum",
            Self::Base => "base",
            Self::Solana => "solana",
            Self::Tempo => "tempo",
        }
    }

    const fn is_solana(self) -> bool {
        matches!(self, Self::Solana)
    }

    const fn alchemy_host(self) -> &'static str {
        match self {
            Self::Ethereum => "eth-mainnet.g.alchemy.com",
            Self::Base => "base-mainnet.g.alchemy.com",
            Self::Solana => "solana-mainnet.g.alchemy.com",
            Self::Tempo => "tempo-mainnet.g.alchemy.com",
        }
    }
}

impl fmt::Display for Chain {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

#[derive(Clone)]
pub struct RpcEndpoint(String);

impl RpcEndpoint {
    pub fn parse(value: &str) -> Result<Self, CollectorError> {
        let parsed = reqwest::Url::parse(value).map_err(|_| CollectorError::InvalidEndpoint)?;
        if parsed.scheme() != "https" || parsed.host_str().is_none() {
            return Err(CollectorError::InvalidEndpoint);
        }
        Ok(Self(value.to_owned()))
    }

    fn expose(&self) -> &str {
        &self.0
    }

    fn matches_chain(&self, chain: Chain) -> bool {
        reqwest::Url::parse(&self.0)
            .ok()
            .and_then(|url| url.host_str().map(str::to_owned))
            .is_some_and(|host| host == chain.alchemy_host())
    }
}

impl fmt::Debug for RpcEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("RpcEndpoint([REDACTED])")
    }
}

pub struct AlchemyRequest {
    chain: Chain,
    requested_height: u64,
    body: Value,
}

impl AlchemyRequest {
    pub fn block(chain: Chain, height: u64) -> Self {
        let (method, params) = if chain.is_solana() {
            (
                "getBlock",
                json!([
                    height,
                    {
                        "commitment": "finalized",
                        "transactionDetails": "full",
                        "rewards": false
                    }
                ]),
            )
        } else {
            (
                "eth_getBlockByNumber",
                json!([format!("0x{height:x}"), true]),
            )
        };
        Self {
            chain,
            requested_height: height,
            body: json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
        }
    }

    pub fn method(&self) -> &'static str {
        if self.chain.is_solana() {
            "getBlock"
        } else {
            "eth_getBlockByNumber"
        }
    }

    pub const fn requested_height(&self) -> u64 {
        self.requested_height
    }
}

pub struct AlchemyTransport {
    client: reqwest::Client,
    endpoints: BTreeMap<Chain, RpcEndpoint>,
}

impl AlchemyTransport {
    pub fn new(endpoints: [(Chain, RpcEndpoint); 4]) -> Result<Self, CollectorError> {
        let endpoints = endpoints.into_iter().collect::<BTreeMap<_, _>>();
        if Chain::ALL.iter().any(|chain| {
            !endpoints
                .get(chain)
                .is_some_and(|endpoint| endpoint.matches_chain(*chain))
        }) {
            return Err(CollectorError::InvalidEndpoint);
        }
        Ok(Self {
            client: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(30))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| CollectorError::Transport)?,
            endpoints,
        })
    }
}

pub struct RawRpcResponse {
    status: u16,
    bytes: Vec<u8>,
    retry_after_milliseconds: Option<u64>,
}

impl RawRpcResponse {
    pub fn try_new(status: u16, bytes: Vec<u8>) -> Result<Self, CollectorError> {
        if bytes.len() > MAX_RPC_RESPONSE_BYTES {
            return Err(CollectorError::ResponseTooLarge);
        }
        Ok(Self {
            status,
            bytes,
            retry_after_milliseconds: None,
        })
    }

    pub fn with_retry_after(mut self, milliseconds: u64) -> Self {
        self.retry_after_milliseconds = Some(milliseconds);
        self
    }

    fn with_optional_retry_after(mut self, milliseconds: Option<u64>) -> Self {
        self.retry_after_milliseconds = milliseconds;
        self
    }
}

pub struct RpcEvidence {
    chain: Chain,
    requested_height: u64,
    http_status: u16,
    attempt: u32,
    retry_after_milliseconds: Option<u64>,
    body: Vec<u8>,
}

impl RpcEvidence {
    const MAGIC: &'static [u8; 8] = b"AEMRPC1\0";
    const HEADER_LENGTH: usize = 36;

    pub fn from_response(
        chain: Chain,
        requested_height: u64,
        attempt: u32,
        response: &RawRpcResponse,
    ) -> Self {
        Self {
            chain,
            requested_height,
            http_status: response.status,
            attempt,
            retry_after_milliseconds: response.retry_after_milliseconds,
            body: response.bytes.clone(),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut encoded = Vec::with_capacity(Self::HEADER_LENGTH + self.body.len());
        encoded.extend_from_slice(Self::MAGIC);
        encoded.push(chain_code(self.chain));
        encoded.extend_from_slice(&self.requested_height.to_be_bytes());
        encoded.extend_from_slice(&self.http_status.to_be_bytes());
        encoded.extend_from_slice(&self.attempt.to_be_bytes());
        encoded.push(u8::from(self.retry_after_milliseconds.is_some()));
        encoded.extend_from_slice(&self.retry_after_milliseconds.unwrap_or(0).to_be_bytes());
        encoded.extend_from_slice(&(self.body.len() as u32).to_be_bytes());
        encoded.extend_from_slice(&self.body);
        encoded
    }

    pub fn decode(encoded: &[u8]) -> Result<Self, CollectorError> {
        if encoded.len() < Self::HEADER_LENGTH || &encoded[..8] != Self::MAGIC {
            return Err(CollectorError::InvalidEvidence("header"));
        }
        let chain = chain_from_code(encoded[8])?;
        let requested_height = u64::from_be_bytes(encoded[9..17].try_into().unwrap());
        let http_status = u16::from_be_bytes(encoded[17..19].try_into().unwrap());
        let attempt = u32::from_be_bytes(encoded[19..23].try_into().unwrap());
        let has_retry_after = match encoded[23] {
            0 => false,
            1 => true,
            _ => return Err(CollectorError::InvalidEvidence("retry marker")),
        };
        let retry_after = u64::from_be_bytes(encoded[24..32].try_into().unwrap());
        let body_length = u32::from_be_bytes(encoded[32..36].try_into().unwrap()) as usize;
        if body_length > MAX_RPC_RESPONSE_BYTES
            || encoded.len() != Self::HEADER_LENGTH + body_length
        {
            return Err(CollectorError::InvalidEvidence("body length"));
        }
        Ok(Self {
            chain,
            requested_height,
            http_status,
            attempt,
            retry_after_milliseconds: has_retry_after.then_some(retry_after),
            body: encoded[Self::HEADER_LENGTH..].to_vec(),
        })
    }

    pub const fn provider(&self) -> &'static str {
        "alchemy"
    }

    pub const fn chain(&self) -> Chain {
        self.chain
    }

    pub fn method(&self) -> &'static str {
        AlchemyRequest::block(self.chain, self.requested_height).method()
    }

    pub const fn requested_height(&self) -> u64 {
        self.requested_height
    }

    pub const fn http_status(&self) -> u16 {
        self.http_status
    }

    pub const fn attempt(&self) -> u32 {
        self.attempt
    }

    pub const fn retry_after_milliseconds(&self) -> Option<u64> {
        self.retry_after_milliseconds
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }
}

const fn chain_code(chain: Chain) -> u8 {
    match chain {
        Chain::Ethereum => 1,
        Chain::Base => 2,
        Chain::Solana => 3,
        Chain::Tempo => 4,
    }
}

fn chain_from_code(code: u8) -> Result<Chain, CollectorError> {
    match code {
        1 => Ok(Chain::Ethereum),
        2 => Ok(Chain::Base),
        3 => Ok(Chain::Solana),
        4 => Ok(Chain::Tempo),
        _ => Err(CollectorError::InvalidEvidence("chain")),
    }
}

#[allow(async_fn_in_trait)]
pub trait RpcTransport: Send {
    fn fetch_block(
        &mut self,
        chain: Chain,
        height: u64,
    ) -> impl std::future::Future<Output = Result<RawRpcResponse, CollectorError>> + Send;
}

impl RpcTransport for AlchemyTransport {
    async fn fetch_block(
        &mut self,
        chain: Chain,
        height: u64,
    ) -> Result<RawRpcResponse, CollectorError> {
        let endpoint = self
            .endpoints
            .get(&chain)
            .ok_or(CollectorError::MissingEndpoint)?;
        let request = AlchemyRequest::block(chain, height);
        let mut response = self
            .client
            .post(endpoint.expose())
            .json(&request.body)
            .send()
            .await
            .map_err(|_| CollectorError::Transport)?;
        let status = response.status().as_u16();
        let retry_after_milliseconds = response
            .headers()
            .get(reqwest::header::RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse::<u64>().ok())
            .and_then(|seconds| seconds.checked_mul(1_000));
        if response
            .content_length()
            .is_some_and(|length| length > MAX_RPC_RESPONSE_BYTES as u64)
        {
            return Err(CollectorError::ResponseTooLarge);
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| CollectorError::Transport)?
        {
            if bytes.len().saturating_add(chunk.len()) > MAX_RPC_RESPONSE_BYTES {
                return Err(CollectorError::ResponseTooLarge);
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(RawRpcResponse::try_new(status, bytes)?
            .with_optional_retry_after(retry_after_milliseconds))
    }
}

#[allow(async_fn_in_trait)]
pub trait EvidenceArchive {
    async fn archive(
        &mut self,
        observed_date: &str,
        evidence: &RpcEvidence,
    ) -> Result<String, CollectorError>;
}

pub struct StoreEvidenceArchive<'a, S> {
    store: &'a S,
}

impl<'a, S> StoreEvidenceArchive<'a, S> {
    pub const fn new(store: &'a S) -> Self {
        Self { store }
    }
}

impl<S: EvidenceStore> EvidenceArchive for StoreEvidenceArchive<'_, S> {
    async fn archive(
        &mut self,
        observed_date: &str,
        evidence: &RpcEvidence,
    ) -> Result<String, CollectorError> {
        let source = format!("alchemy-{}", evidence.chain());
        let observation_id = format!(
            "alchemy:{}:block:{}:attempt:{}",
            evidence.chain(),
            evidence.requested_height(),
            evidence.attempt()
        );
        let height = evidence.requested_height().to_string();
        let attempt = evidence.attempt().to_string();
        let http_status = evidence.http_status().to_string();
        let provenance = EvidenceProvenance::new(
            "rpc-evidence-v1",
            &observation_id,
            [
                ("attempt", attempt.as_str()),
                ("chain", evidence.chain().as_str()),
                ("height", height.as_str()),
                ("http-status", http_status.as_str()),
                ("method", evidence.method()),
                ("provider", evidence.provider()),
            ],
        )
        .map_err(|error| CollectorError::Evidence(error.to_string()))?;
        let context = EvidenceContext::new(&source, observed_date)
            .map_err(|error| CollectorError::Evidence(error.to_string()))?
            .with_provenance(provenance);
        let encoded = evidence.encode();
        self.store
            .create(&context, &encoded)
            .await
            .map(|receipt| receipt.object.name().to_owned())
            .map_err(|error| CollectorError::Evidence(error.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CursorCheckpoint {
    next_height: u64,
    version: u64,
}

impl CursorCheckpoint {
    pub const fn new(next_height: u64, version: u64) -> Self {
        Self {
            next_height,
            version,
        }
    }

    pub const fn next_height(self) -> u64 {
        self.next_height
    }

    pub const fn version(self) -> u64 {
        self.version
    }
}

#[allow(async_fn_in_trait)]
pub trait CursorStore {
    async fn load(&mut self, chain: Chain) -> Result<Option<CursorCheckpoint>, CollectorError>;

    async fn initialize(
        &mut self,
        chain: Chain,
        start_height: u64,
    ) -> Result<CursorCheckpoint, CollectorError>;

    async fn compare_and_set(
        &mut self,
        chain: Chain,
        expected: CursorCheckpoint,
        next_height: u64,
    ) -> Result<CursorCheckpoint, CollectorError>;
}

pub const POSTGRES_CURSOR_SCHEMA: &str = include_str!("../migrations/0001_rpc_cursors.sql");

pub struct PostgresCursorStore {
    client: Client,
}

impl PostgresCursorStore {
    pub const fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn migrate(&self) -> Result<(), CollectorError> {
        self.client
            .batch_execute(POSTGRES_CURSOR_SCHEMA)
            .await
            .map_err(|_| CollectorError::CursorStorage)
    }
}

impl CursorStore for PostgresCursorStore {
    async fn load(&mut self, chain: Chain) -> Result<Option<CursorCheckpoint>, CollectorError> {
        self.client
            .query_opt(
                "SELECT next_height, version FROM rpc_collection_cursors WHERE chain = $1",
                &[&chain.as_str()],
            )
            .await
            .map_err(|_| CollectorError::CursorStorage)?
            .map(checkpoint_from_row)
            .transpose()
    }

    async fn initialize(
        &mut self,
        chain: Chain,
        start_height: u64,
    ) -> Result<CursorCheckpoint, CollectorError> {
        let start_height = database_integer(start_height)?;
        let row = self
            .client
            .query_one(
                "INSERT INTO rpc_collection_cursors (chain, next_height) VALUES ($1, $2) \
                 ON CONFLICT (chain) DO UPDATE SET chain = EXCLUDED.chain \
                 RETURNING next_height, version",
                &[&chain.as_str(), &start_height],
            )
            .await
            .map_err(|_| CollectorError::CursorStorage)?;
        checkpoint_from_row(row)
    }

    async fn compare_and_set(
        &mut self,
        chain: Chain,
        expected: CursorCheckpoint,
        next_height: u64,
    ) -> Result<CursorCheckpoint, CollectorError> {
        let expected_height = database_integer(expected.next_height)?;
        let expected_version = database_integer(expected.version)?;
        let next_height = database_integer(next_height)?;
        let row = self
            .client
            .query_opt(
                "UPDATE rpc_collection_cursors \
                 SET next_height = $4, version = version + 1, updated_at = now() \
                 WHERE chain = $1 AND next_height = $2 AND version = $3 \
                 RETURNING next_height, version",
                &[
                    &chain.as_str(),
                    &expected_height,
                    &expected_version,
                    &next_height,
                ],
            )
            .await
            .map_err(|_| CollectorError::CursorStorage)?
            .ok_or(CollectorError::CursorConflict)?;
        checkpoint_from_row(row)
    }
}

fn database_integer(value: u64) -> Result<i64, CollectorError> {
    i64::try_from(value).map_err(|_| CollectorError::InvalidHeight)
}

fn checkpoint_from_row(row: tokio_postgres::Row) -> Result<CursorCheckpoint, CollectorError> {
    let height: i64 = row.get("next_height");
    let version: i64 = row.get("version");
    Ok(CursorCheckpoint::new(
        u64::try_from(height).map_err(|_| CollectorError::CursorStorage)?,
        u64::try_from(version).map_err(|_| CollectorError::CursorStorage)?,
    ))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RetryPolicy {
    max_retries: u32,
    base_delay_milliseconds: u64,
}

impl RetryPolicy {
    pub const fn try_new(
        max_retries: u32,
        base_delay_milliseconds: u64,
    ) -> Result<Self, CollectorError> {
        if max_retries > MAX_RETRIES || base_delay_milliseconds > MAX_RETRY_AFTER_MILLISECONDS {
            return Err(CollectorError::InvalidConfiguration);
        }
        Ok(Self {
            max_retries,
            base_delay_milliseconds,
        })
    }

    fn delay(self, retry: u32) -> u64 {
        self.base_delay_milliseconds
            .saturating_mul(1_u64 << retry.min(20))
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            base_delay_milliseconds: 1_000,
        }
    }
}

#[allow(async_fn_in_trait)]
pub trait RetryDelay {
    async fn wait(&mut self, milliseconds: u64);
}

#[derive(Default)]
pub struct TokioRetryDelay;

impl RetryDelay for TokioRetryDelay {
    async fn wait(&mut self, milliseconds: u64) {
        tokio::time::sleep(std::time::Duration::from_millis(milliseconds)).await;
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectRequest {
    chain: Chain,
    start_height: u64,
    max_blocks: u64,
    request_budget: u64,
    observed_date: String,
}

impl CollectRequest {
    pub fn try_new(
        chain: Chain,
        start_height: u64,
        max_blocks: u64,
        request_budget: u64,
        observed_date: impl Into<String>,
    ) -> Result<Self, CollectorError> {
        if max_blocks > MAX_RUN_BLOCKS || request_budget > MAX_RUN_REQUESTS {
            return Err(CollectorError::InvalidConfiguration);
        }
        Ok(Self {
            chain,
            start_height,
            max_blocks,
            request_budget,
            observed_date: observed_date.into(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Gap {
    expected_height: u64,
    observed_height: Option<u64>,
}

impl Gap {
    pub const fn expected_height(&self) -> u64 {
        self.expected_height
    }

    pub const fn observed_height(&self) -> Option<u64> {
        self.observed_height
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CollectionReport {
    chain: Chain,
    requests_used: u64,
    request_budget: u64,
    retry_count: u64,
    responses_archived: u64,
    next_height: u64,
    gaps: Vec<Gap>,
    evidence_objects: Vec<String>,
    budget_exhausted: bool,
}

impl CollectionReport {
    pub const fn requests_used(&self) -> u64 {
        self.requests_used
    }

    pub const fn requests_remaining(&self) -> u64 {
        self.request_budget.saturating_sub(self.requests_used)
    }

    pub const fn retry_count(&self) -> u64 {
        self.retry_count
    }

    pub const fn responses_archived(&self) -> u64 {
        self.responses_archived
    }

    pub const fn next_height(&self) -> u64 {
        self.next_height
    }

    pub fn gaps(&self) -> &[Gap] {
        &self.gaps
    }

    pub fn evidence_objects(&self) -> &[String] {
        &self.evidence_objects
    }

    pub const fn budget_exhausted(&self) -> bool {
        self.budget_exhausted
    }

    pub const fn chain(&self) -> Chain {
        self.chain
    }
}

pub struct Collector<'a, T, A, C, D> {
    transport: &'a mut T,
    archive: &'a mut A,
    cursors: &'a mut C,
    delay: &'a mut D,
    retry_policy: RetryPolicy,
}

impl<'a, T, A, C, D> Collector<'a, T, A, C, D>
where
    T: RpcTransport,
    A: EvidenceArchive,
    C: CursorStore,
    D: RetryDelay,
{
    pub fn new(
        transport: &'a mut T,
        archive: &'a mut A,
        cursors: &'a mut C,
        delay: &'a mut D,
    ) -> Self {
        Self {
            transport,
            archive,
            cursors,
            delay,
            retry_policy: RetryPolicy::default(),
        }
    }

    pub const fn with_retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub async fn collect(
        &mut self,
        request: CollectRequest,
    ) -> Result<CollectionReport, CollectorError> {
        let mut checkpoint = match self.cursors.load(request.chain).await? {
            Some(checkpoint) => checkpoint,
            None => {
                self.cursors
                    .initialize(request.chain, request.start_height)
                    .await?
            }
        };
        let mut report = CollectionReport {
            chain: request.chain,
            requests_used: 0,
            request_budget: request.request_budget,
            retry_count: 0,
            responses_archived: 0,
            next_height: checkpoint.next_height,
            gaps: Vec::new(),
            evidence_objects: Vec::new(),
            budget_exhausted: false,
        };
        let mut collected = 0;

        while collected < request.max_blocks {
            if report.requests_used >= request.request_budget {
                report.budget_exhausted = true;
                break;
            }
            let expected_height = checkpoint.next_height;
            let mut retries = 0;
            let response = loop {
                if report.requests_used >= request.request_budget {
                    report.budget_exhausted = true;
                    return Ok(report);
                }
                report.requests_used += 1;
                match self
                    .transport
                    .fetch_block(request.chain, expected_height)
                    .await
                {
                    Ok(response) => {
                        let evidence = RpcEvidence::from_response(
                            request.chain,
                            expected_height,
                            retries,
                            &response,
                        );
                        let object = match self
                            .archive
                            .archive(&request.observed_date, &evidence)
                            .await
                        {
                            Ok(object) => object,
                            Err(error) => return Err(error.with_report(report)),
                        };
                        report.responses_archived += 1;
                        report.evidence_objects.push(object);
                        if retryable_status(response.status)
                            && retries < self.retry_policy.max_retries
                        {
                            if report.requests_used >= request.request_budget {
                                report.budget_exhausted = true;
                                break response;
                            }
                            let wait = response
                                .retry_after_milliseconds
                                .unwrap_or_else(|| self.retry_policy.delay(retries))
                                .min(MAX_RETRY_AFTER_MILLISECONDS);
                            retries += 1;
                            report.retry_count += 1;
                            self.delay.wait(wait).await;
                            continue;
                        }
                        break response;
                    }
                    Err(CollectorError::Transport) if retries < self.retry_policy.max_retries => {
                        if report.requests_used >= request.request_budget {
                            report.budget_exhausted = true;
                            return Err(CollectorError::Transport.with_report(report));
                        }
                        let wait = self
                            .retry_policy
                            .delay(retries)
                            .min(MAX_RETRY_AFTER_MILLISECONDS);
                        retries += 1;
                        report.retry_count += 1;
                        self.delay.wait(wait).await;
                    }
                    Err(error) => return Err(error.with_report(report)),
                }
            };

            if !(200..300).contains(&response.status) {
                return Err(CollectorError::HttpStatus(response.status).with_report(report));
            }
            let observed_height =
                match decode_height(request.chain, expected_height, &response.bytes) {
                    Ok(height) => height,
                    Err(error) => return Err(error.with_report(report)),
                };
            if observed_height != Some(expected_height) {
                report.gaps.push(Gap {
                    expected_height,
                    observed_height,
                });
                break;
            }
            let next_height = expected_height
                .checked_add(1)
                .ok_or_else(|| CollectorError::InvalidHeight.with_report(report.clone()))?;
            checkpoint = match self
                .cursors
                .compare_and_set(request.chain, checkpoint, next_height)
                .await
            {
                Ok(checkpoint) => checkpoint,
                Err(error) => return Err(error.with_report(report)),
            };
            report.next_height = checkpoint.next_height;
            collected += 1;
        }
        Ok(report)
    }
}

fn retryable_status(status: u16) -> bool {
    status == 429 || (500..600).contains(&status)
}

fn decode_height(
    chain: Chain,
    requested_height: u64,
    bytes: &[u8],
) -> Result<Option<u64>, CollectorError> {
    reject_duplicate_json_members(bytes)?;
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| CollectorError::InvalidResponse("json"))?;
    if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0") {
        return Err(CollectorError::InvalidResponse("json-rpc version"));
    }
    if value.get("id").and_then(Value::as_u64) != Some(1) {
        return Err(CollectorError::InvalidResponse("json-rpc id"));
    }
    if value.get("result").is_some() && value.get("error").is_some() {
        return Err(CollectorError::InvalidResponse("result and error"));
    }
    if let Some(error) = value.get("error") {
        let code = error.get("code").and_then(Value::as_i64);
        if chain.is_solana() && matches!(code, Some(-32007 | -32009)) {
            return Ok(None);
        }
        return Err(CollectorError::InvalidResponse("provider error"));
    }
    let Some(result) = value.get("result") else {
        return Err(CollectorError::InvalidResponse("result"));
    };
    if result.is_null() {
        return Ok(None);
    }
    if chain.is_solana() {
        let Some(block) = result.as_object() else {
            return Err(CollectorError::InvalidResponse("solana block"));
        };
        let valid_block_height = block
            .get("blockHeight")
            .is_some_and(|height| height.is_null() || height.as_u64().is_some());
        let valid_parent_slot = block
            .get("parentSlot")
            .and_then(Value::as_u64)
            .is_some_and(|parent| requested_height == 0 || parent < requested_height);
        let valid_hashes = ["blockhash", "previousBlockhash"].iter().all(|field| {
            block
                .get(*field)
                .and_then(Value::as_str)
                .is_some_and(|hash| !hash.is_empty())
        });
        if !valid_block_height
            || !valid_parent_slot
            || !valid_hashes
            || !block.get("transactions").is_some_and(Value::is_array)
        {
            return Err(CollectorError::InvalidResponse("solana block"));
        }
        Ok(Some(requested_height))
    } else {
        let encoded = result
            .get("number")
            .and_then(Value::as_str)
            .ok_or(CollectorError::InvalidResponse("number"))?;
        let encoded = encoded
            .strip_prefix("0x")
            .ok_or(CollectorError::InvalidResponse("number"))?;
        let canonical = encoded == "0"
            || encoded
                .as_bytes()
                .split_first()
                .is_some_and(|(first, rest)| {
                    matches!(first, b'1'..=b'9' | b'a'..=b'f')
                        && rest.iter().all(u8::is_ascii_hexdigit)
                        && rest.iter().all(|byte| !byte.is_ascii_uppercase())
                });
        if !canonical {
            return Err(CollectorError::InvalidResponse("number"));
        }
        u64::from_str_radix(encoded, 16)
            .map(Some)
            .map_err(|_| CollectorError::InvalidResponse("number"))
    }
}

pub fn validate_archived_success(
    encoded: &[u8],
    expected_chain: Chain,
    expected_height: u64,
) -> Result<(), CollectorError> {
    let evidence = RpcEvidence::decode(encoded)?;
    if evidence.chain() != expected_chain
        || evidence.requested_height() != expected_height
        || !(200..300).contains(&evidence.http_status())
        || decode_height(expected_chain, expected_height, evidence.body())? != Some(expected_height)
    {
        return Err(CollectorError::InvalidEvidence(
            "successful response binding",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
struct RejectDuplicateMembers;

impl<'de> DeserializeSeed<'de> for RejectDuplicateMembers {
    type Value = ();

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }
}

impl<'de> Visitor<'de> for RejectDuplicateMembers {
    type Value = ();

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("unambiguous JSON")
    }

    fn visit_bool<E>(self, _value: bool) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_i64<E>(self, _value: i64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_u64<E>(self, _value: u64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_f64<E>(self, _value: f64) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_str<E>(self, _value: &str) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_string<E>(self, _value: String) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(())
    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        deserializer.deserialize_any(self)
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        while sequence.next_element_seed(self)?.is_some() {}
        Ok(())
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut keys = BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(de::Error::custom("duplicate JSON object member"));
            }
            map.next_value_seed(self)?;
        }
        Ok(())
    }
}

fn reject_duplicate_json_members(bytes: &[u8]) -> Result<(), CollectorError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    RejectDuplicateMembers
        .deserialize(&mut deserializer)
        .map_err(|_| CollectorError::InvalidResponse("json"))?;
    deserializer
        .end()
        .map_err(|_| CollectorError::InvalidResponse("json"))
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum CollectorError {
    #[error("RPC endpoint is invalid")]
    InvalidEndpoint,
    #[error("RPC endpoint is missing for a launch chain")]
    MissingEndpoint,
    #[error("RPC transport failed")]
    Transport,
    #[error("RPC provider response exceeds the eight MiB limit")]
    ResponseTooLarge,
    #[error("RPC provider returned HTTP status {0}")]
    HttpStatus(u16),
    #[error("RPC provider returned an invalid {0} response")]
    InvalidResponse(&'static str),
    #[error("archived RPC evidence has an invalid {0}")]
    InvalidEvidence(&'static str),
    #[error("evidence archival failed: {0}")]
    Evidence(String),
    #[error("cursor storage failed")]
    CursorStorage,
    #[error("cursor changed concurrently")]
    CursorConflict,
    #[error("block height exceeds PostgreSQL integer range")]
    InvalidHeight,
    #[error("collector run configuration exceeds a bounded limit")]
    InvalidConfiguration,
    #[error("buyer-history enrichment target is invalid")]
    InvalidTarget,
    #[error("no discovered buyer targets are eligible for enrichment")]
    NoEnrichmentTargets,
    #[error("buyer-history enrichment state storage failed")]
    EnrichmentStateStorage,
    #[error("finalized buyer-history cache storage failed")]
    FinalizedHistoryStorage,
    #[error("finalized buyer-history cache conflicts with immutable history")]
    FinalizedHistoryConflict,
    #[error("buyer-history provider returned an invalid page")]
    InvalidHistoryPage,
    #[error("RPC collection failed: {error}")]
    RunFailed {
        error: Box<CollectorError>,
        report: Box<CollectionReport>,
    },
}

impl CollectorError {
    fn with_report(self, report: CollectionReport) -> Self {
        Self::RunFailed {
            error: Box::new(self),
            report: Box::new(report),
        }
    }

    pub fn cause(&self) -> &Self {
        match self {
            Self::RunFailed { error, .. } => error.cause(),
            error => error,
        }
    }

    pub fn report(&self) -> Option<&CollectionReport> {
        match self {
            Self::RunFailed { report, .. } => Some(report),
            _ => None,
        }
    }
}
