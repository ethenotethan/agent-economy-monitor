use serde_json::{Value, json};
use tokio_postgres::Client;
use uuid::Uuid;

use crate::{
    AlchemyTransport, Chain, CollectorError, MAX_RPC_RESPONSE_BYTES, RawRpcResponse,
    database_integer, reject_duplicate_json_members,
};

const MAX_AUTOMATIC_PAGES: u64 = 25;
const MAX_AUTOMATIC_REQUESTS: u64 = 50;
const MAX_MANUAL_PAGES: u64 = 500;
const MAX_MANUAL_REQUESTS: u64 = 1_000;
const MAX_HISTORY_CURSOR_BYTES: usize = 4_096;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BuyerHistoryTarget {
    namespace_id: String,
    buyer_handle_id: String,
    chain: Chain,
    handle_value: String,
}

impl BuyerHistoryTarget {
    pub fn try_new(
        namespace_id: impl Into<String>,
        buyer_handle_id: impl Into<String>,
        chain: Chain,
        handle_value: impl Into<String>,
    ) -> Result<Self, CollectorError> {
        let target = Self {
            namespace_id: namespace_id.into(),
            buyer_handle_id: buyer_handle_id.into(),
            chain,
            handle_value: handle_value.into(),
        };
        if target.namespace_id.is_empty()
            || target.buyer_handle_id.is_empty()
            || target.handle_value.is_empty()
        {
            return Err(CollectorError::InvalidTarget);
        }
        Ok(target)
    }

    pub const fn chain(&self) -> Chain {
        self.chain
    }

    pub fn buyer_handle_id(&self) -> &str {
        &self.buyer_handle_id
    }
}

pub struct AlchemyHistoryRequest {
    method: &'static str,
    body: Value,
}

impl AlchemyHistoryRequest {
    pub fn try_new(
        target: &BuyerHistoryTarget,
        cursor: Option<&str>,
    ) -> Result<Self, CollectorError> {
        if cursor.is_some_and(|value| value.is_empty() || value.len() > MAX_HISTORY_CURSOR_BYTES) {
            return Err(CollectorError::InvalidHistoryPage);
        }
        let (method, params) = if target.chain.is_solana() {
            let mut options = json!({"commitment": "finalized", "limit": 1000});
            if let Some(before) = cursor {
                options["before"] = Value::String(before.to_owned());
            }
            (
                "getSignaturesForAddress",
                json!([target.handle_value, options]),
            )
        } else {
            let mut options = json!({
                "fromBlock": "0x0",
                "toBlock": "finalized",
                "fromAddress": target.handle_value,
                "category": ["external", "erc20", "erc721", "erc1155"],
                "excludeZeroValue": true,
                "withMetadata": false,
                "maxCount": "0x64"
            });
            if let Some(page_key) = cursor {
                options["pageKey"] = Value::String(page_key.to_owned());
            }
            ("alchemy_getAssetTransfers", json!([options]))
        };
        Ok(Self {
            method,
            body: json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params}),
        })
    }

    pub const fn method(&self) -> &'static str {
        self.method
    }

    pub const fn body(&self) -> &Value {
        &self.body
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichmentCandidate {
    target: BuyerHistoryTarget,
    observed_events: u64,
    stale_blocks: u64,
    never_enriched: bool,
}

impl EnrichmentCandidate {
    pub const fn new(
        target: BuyerHistoryTarget,
        observed_events: u64,
        stale_blocks: u64,
        never_enriched: bool,
    ) -> Self {
        Self {
            target,
            observed_events,
            stale_blocks,
            never_enriched,
        }
    }
}

pub fn prioritize_automatic(
    mut candidates: Vec<EnrichmentCandidate>,
    limit: usize,
) -> Result<Vec<BuyerHistoryTarget>, CollectorError> {
    if candidates.is_empty() {
        return Err(CollectorError::NoEnrichmentTargets);
    }
    if limit == 0 || limit > 100 {
        return Err(CollectorError::InvalidConfiguration);
    }
    candidates.sort_by(|left, right| {
        right
            .never_enriched
            .cmp(&left.never_enriched)
            .then_with(|| right.observed_events.cmp(&left.observed_events))
            .then_with(|| right.stale_blocks.cmp(&left.stale_blocks))
            .then_with(|| left.target.chain.cmp(&right.target.chain))
            .then_with(|| {
                left.target
                    .buyer_handle_id
                    .cmp(&right.target.buyer_handle_id)
            })
    });
    Ok(candidates
        .into_iter()
        .take(limit)
        .map(|candidate| candidate.target)
        .collect())
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnrichmentMode {
    Automatic,
    ManualDeepScan,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichmentRequest {
    target: BuyerHistoryTarget,
    mode: EnrichmentMode,
    max_pages: u64,
    request_budget: u64,
    observed_date: String,
}

impl EnrichmentRequest {
    pub fn try_new(
        target: BuyerHistoryTarget,
        mode: EnrichmentMode,
        max_pages: u64,
        request_budget: u64,
        observed_date: impl Into<String>,
    ) -> Result<Self, CollectorError> {
        let (max_allowed_pages, max_allowed_requests) = match mode {
            EnrichmentMode::Automatic => (MAX_AUTOMATIC_PAGES, MAX_AUTOMATIC_REQUESTS),
            EnrichmentMode::ManualDeepScan => (MAX_MANUAL_PAGES, MAX_MANUAL_REQUESTS),
        };
        let observed_date = observed_date.into();
        if max_pages == 0
            || request_budget == 0
            || max_pages > max_allowed_pages
            || request_budget > max_allowed_requests
            || observed_date.is_empty()
        {
            return Err(CollectorError::InvalidConfiguration);
        }
        Ok(Self {
            target,
            mode,
            max_pages,
            request_budget,
            observed_date,
        })
    }

    pub const fn target(&self) -> &BuyerHistoryTarget {
        &self.target
    }

    pub const fn mode(&self) -> EnrichmentMode {
        self.mode
    }

    pub const fn max_pages(&self) -> u64 {
        self.max_pages
    }

    pub const fn request_budget(&self) -> u64 {
        self.request_budget
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichmentCheckpoint {
    cursor: Option<String>,
    version: u64,
    requests_used_total: u64,
    last_run_budget: u64,
    last_run_requests_used: u64,
    reservation_owner: Option<String>,
    complete: bool,
}

impl EnrichmentCheckpoint {
    pub const fn new(
        cursor: Option<String>,
        version: u64,
        requests_used_total: u64,
        complete: bool,
    ) -> Self {
        Self {
            cursor,
            version,
            requests_used_total,
            last_run_budget: 0,
            last_run_requests_used: 0,
            reservation_owner: None,
            complete,
        }
    }

    pub const fn with_budget_state(
        cursor: Option<String>,
        version: u64,
        requests_used_total: u64,
        last_run_budget: u64,
        last_run_requests_used: u64,
        complete: bool,
    ) -> Self {
        Self {
            cursor,
            version,
            requests_used_total,
            last_run_budget,
            last_run_requests_used,
            reservation_owner: None,
            complete,
        }
    }

    pub fn with_reservation(
        cursor: Option<String>,
        version: u64,
        requests_used_total: u64,
        last_run_budget: u64,
        last_run_requests_used: u64,
        reservation_owner: Option<String>,
        complete: bool,
    ) -> Self {
        Self {
            cursor,
            version,
            requests_used_total,
            last_run_budget,
            last_run_requests_used,
            reservation_owner,
            complete,
        }
    }

    pub fn cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }

    pub const fn version(&self) -> u64 {
        self.version
    }

    pub const fn requests_used_total(&self) -> u64 {
        self.requests_used_total
    }

    pub const fn last_run_budget(&self) -> u64 {
        self.last_run_budget
    }

    pub const fn last_run_requests_used(&self) -> u64 {
        self.last_run_requests_used
    }

    pub const fn reservation_active(&self) -> bool {
        self.reservation_owner.is_some()
    }

    pub const fn complete(&self) -> bool {
        self.complete
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HistoryFinality {
    Pending,
    Confirmed,
    Finalized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryItem {
    transaction_reference: String,
    block_reference: String,
    finality: HistoryFinality,
}

impl HistoryItem {
    pub fn new(
        transaction_reference: impl Into<String>,
        block_reference: impl Into<String>,
        finality: HistoryFinality,
    ) -> Self {
        Self {
            transaction_reference: transaction_reference.into(),
            block_reference: block_reference.into(),
            finality,
        }
    }

    pub fn transaction_reference(&self) -> &str {
        &self.transaction_reference
    }

    pub fn block_reference(&self) -> &str {
        &self.block_reference
    }

    pub const fn finality(&self) -> HistoryFinality {
        self.finality
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HistoryPage {
    items: Vec<HistoryItem>,
    next_cursor: Option<String>,
}

impl HistoryPage {
    pub const fn new(items: Vec<HistoryItem>, next_cursor: Option<String>) -> Self {
        Self { items, next_cursor }
    }

    pub fn decode_alchemy(
        target: &BuyerHistoryTarget,
        raw_body: &[u8],
    ) -> Result<Self, CollectorError> {
        reject_duplicate_json_members(raw_body)?;
        let value: Value =
            serde_json::from_slice(raw_body).map_err(|_| CollectorError::InvalidHistoryPage)?;
        if value.get("jsonrpc").and_then(Value::as_str) != Some("2.0")
            || value.get("id").and_then(Value::as_u64) != Some(1)
            || value.get("error").is_some()
        {
            return Err(CollectorError::InvalidHistoryPage);
        }
        let result = value
            .get("result")
            .ok_or(CollectorError::InvalidHistoryPage)?;
        let (items, next_cursor) = if target.chain.is_solana() {
            let entries = result
                .as_array()
                .filter(|entries| entries.len() <= 1_000)
                .ok_or(CollectorError::InvalidHistoryPage)?;
            let mut items = Vec::with_capacity(entries.len());
            for entry in entries {
                let signature = entry
                    .get("signature")
                    .and_then(Value::as_str)
                    .filter(|value| is_solana_signature(value))
                    .ok_or(CollectorError::InvalidHistoryPage)?;
                let slot = entry
                    .get("slot")
                    .and_then(Value::as_u64)
                    .ok_or(CollectorError::InvalidHistoryPage)?;
                let finality = match entry.get("confirmationStatus").and_then(Value::as_str) {
                    Some("finalized") => HistoryFinality::Finalized,
                    Some("confirmed") => HistoryFinality::Confirmed,
                    Some("processed") | None => HistoryFinality::Pending,
                    Some(_) => return Err(CollectorError::InvalidHistoryPage),
                };
                items.push(HistoryItem::new(
                    signature,
                    format!("slot:{slot}"),
                    finality,
                ));
            }
            let next_cursor = (entries.len() == 1_000)
                .then(|| items.last().map(|item| item.transaction_reference.clone()))
                .flatten();
            (items, next_cursor)
        } else {
            let result = result
                .as_object()
                .ok_or(CollectorError::InvalidHistoryPage)?;
            let transfers = result
                .get("transfers")
                .and_then(Value::as_array)
                .filter(|transfers| transfers.len() <= 100)
                .ok_or(CollectorError::InvalidHistoryPage)?;
            let mut items = Vec::with_capacity(transfers.len());
            for transfer in transfers {
                let transaction = transfer
                    .get("hash")
                    .and_then(Value::as_str)
                    .filter(|value| is_evm_hash(value))
                    .ok_or(CollectorError::InvalidHistoryPage)?;
                let block = transfer
                    .get("blockNum")
                    .and_then(Value::as_str)
                    .filter(|value| is_canonical_evm_quantity(value))
                    .ok_or(CollectorError::InvalidHistoryPage)?;
                transfer
                    .get("from")
                    .and_then(Value::as_str)
                    .filter(|value| value.eq_ignore_ascii_case(&target.handle_value))
                    .ok_or(CollectorError::InvalidHistoryPage)?;
                items.push(HistoryItem::new(
                    transaction,
                    block,
                    HistoryFinality::Finalized,
                ));
            }
            let next_cursor = result
                .get("pageKey")
                .map(|value| {
                    value
                        .as_str()
                        .filter(|cursor| {
                            !cursor.is_empty() && cursor.len() <= MAX_HISTORY_CURSOR_BYTES
                        })
                        .map(str::to_owned)
                        .ok_or(CollectorError::InvalidHistoryPage)
                })
                .transpose()?;
            (items, next_cursor)
        };
        Ok(Self::new(items, next_cursor))
    }

    pub fn items(&self) -> &[HistoryItem] {
        &self.items
    }

    pub fn next_cursor(&self) -> Option<&str> {
        self.next_cursor.as_deref()
    }
}

fn is_evm_hash(value: &str) -> bool {
    value.len() == 66
        && value.strip_prefix("0x").is_some_and(|hex| {
            hex.bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

fn is_canonical_evm_quantity(value: &str) -> bool {
    value.strip_prefix("0x").is_some_and(|hex| {
        !hex.is_empty()
            && (hex == "0" || !hex.starts_with('0'))
            && hex
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    })
}

fn is_solana_signature(value: &str) -> bool {
    (64..=88).contains(&value.len())
        && value.bytes().all(|byte| {
            matches!(byte, b'1'..=b'9' | b'A'..=b'H' | b'J'..=b'N' | b'P'..=b'Z' | b'a'..=b'k' | b'm'..=b'z')
        })
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalizedHistoryRecord {
    namespace_id: String,
    buyer_handle_id: String,
    chain: Chain,
    transaction_reference: String,
    block_reference: String,
    evidence_id: String,
}

impl FinalizedHistoryRecord {
    pub(crate) fn try_new(
        target: &BuyerHistoryTarget,
        transaction_reference: impl Into<String>,
        block_reference: impl Into<String>,
        evidence_id: impl Into<String>,
    ) -> Result<Self, CollectorError> {
        let record = Self {
            namespace_id: target.namespace_id.clone(),
            buyer_handle_id: target.buyer_handle_id.clone(),
            chain: target.chain,
            transaction_reference: transaction_reference.into(),
            block_reference: block_reference.into(),
            evidence_id: evidence_id.into(),
        };
        if record.transaction_reference.is_empty()
            || record.block_reference.is_empty()
            || record.evidence_id.is_empty()
        {
            return Err(CollectorError::InvalidHistoryPage);
        }
        Ok(record)
    }

    pub fn transaction_reference(&self) -> &str {
        &self.transaction_reference
    }

    pub const fn protocol_attribution(&self) -> Option<&str> {
        None
    }
}

#[allow(async_fn_in_trait)]
pub trait HistoryTransport {
    async fn fetch_history(
        &mut self,
        target: &BuyerHistoryTarget,
        cursor: Option<&str>,
    ) -> Result<RawRpcResponse, CollectorError>;
}

impl HistoryTransport for AlchemyTransport {
    async fn fetch_history(
        &mut self,
        target: &BuyerHistoryTarget,
        cursor: Option<&str>,
    ) -> Result<RawRpcResponse, CollectorError> {
        let endpoint = self
            .endpoints
            .get(&target.chain)
            .ok_or(CollectorError::MissingEndpoint)?;
        let request = AlchemyHistoryRequest::try_new(target, cursor)?;
        let mut response = self
            .client
            .post(endpoint.expose())
            .json(request.body())
            .send()
            .await
            .map_err(|_| CollectorError::Transport)?;
        let status = response.status().as_u16();
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
        RawRpcResponse::try_new(status, bytes)
    }
}

pub trait HistoryEvidenceArchive {
    fn archive_history(
        &mut self,
        target: &BuyerHistoryTarget,
        observed_date: &str,
        http_status: u16,
        body: &[u8],
    ) -> Result<String, CollectorError>;
}

#[allow(async_fn_in_trait)]
pub trait EnrichmentStateStore {
    async fn load(
        &mut self,
        target: &BuyerHistoryTarget,
    ) -> Result<Option<EnrichmentCheckpoint>, CollectorError>;

    async fn initialize(
        &mut self,
        target: &BuyerHistoryTarget,
    ) -> Result<EnrichmentCheckpoint, CollectorError>;

    async fn record_request(
        &mut self,
        target: &BuyerHistoryTarget,
        expected: &EnrichmentCheckpoint,
        request_budget: u64,
        run_requests_used: u64,
        reservation_owner: &str,
    ) -> Result<EnrichmentCheckpoint, CollectorError>;

    async fn compare_and_set(
        &mut self,
        target: &BuyerHistoryTarget,
        expected: &EnrichmentCheckpoint,
        next_cursor: Option<String>,
        complete: bool,
    ) -> Result<EnrichmentCheckpoint, CollectorError>;
}

#[allow(async_fn_in_trait)]
pub trait FinalizedHistoryCache {
    async fn store_finalized(
        &mut self,
        record: FinalizedHistoryRecord,
    ) -> Result<(), CollectorError>;
}

pub const POSTGRES_BUYER_ENRICHMENT_SCHEMA: &str =
    include_str!("../../../migrations/0004_buyer_enrichment.up.sql");

pub struct PostgresEnrichmentStateStore {
    client: Client,
}

impl PostgresEnrichmentStateStore {
    pub const fn new(client: Client) -> Self {
        Self { client }
    }

    pub async fn migrate(&self) -> Result<(), CollectorError> {
        self.client
            .batch_execute(POSTGRES_BUYER_ENRICHMENT_SCHEMA)
            .await
            .map_err(|_| CollectorError::EnrichmentStateStorage)
    }
}

impl EnrichmentStateStore for PostgresEnrichmentStateStore {
    async fn load(
        &mut self,
        target: &BuyerHistoryTarget,
    ) -> Result<Option<EnrichmentCheckpoint>, CollectorError> {
        self.client
            .query_opt(
                "SELECT cursor, version, requests_used_total, last_run_budget, \
                        last_run_requests_used, reservation_owner, complete \
                 FROM agent_economy.buyer_enrichment_cursors \
                 WHERE namespace_id = $1::text::uuid \
                   AND buyer_handle_id = $2 AND chain_scope = $3 AND handle_value = $4",
                &[
                    &target.namespace_id,
                    &target.buyer_handle_id,
                    &target.chain.as_str(),
                    &target.handle_value,
                ],
            )
            .await
            .map_err(|_| CollectorError::EnrichmentStateStorage)?
            .map(checkpoint_from_enrichment_row)
            .transpose()
    }

    async fn initialize(
        &mut self,
        target: &BuyerHistoryTarget,
    ) -> Result<EnrichmentCheckpoint, CollectorError> {
        self.client
            .execute(
                "INSERT INTO agent_economy.buyer_enrichment_cursors \
                    (namespace_id, buyer_handle_id, chain_scope, handle_value) \
                 SELECT $1::text::uuid, $2, $3, $4 \
                 FROM agent_economy.buyer_handles \
                 WHERE namespace_id = $1::text::uuid AND buyer_handle_id = $2 \
                   AND chain_scope = $3 AND handle_value = $4 \
                 ON CONFLICT (namespace_id, buyer_handle_id, chain_scope) DO NOTHING",
                &[
                    &target.namespace_id,
                    &target.buyer_handle_id,
                    &target.chain.as_str(),
                    &target.handle_value,
                ],
            )
            .await
            .map_err(|_| CollectorError::EnrichmentStateStorage)?;
        self.load(target)
            .await?
            .ok_or(CollectorError::InvalidTarget)
    }

    async fn record_request(
        &mut self,
        target: &BuyerHistoryTarget,
        expected: &EnrichmentCheckpoint,
        request_budget: u64,
        run_requests_used: u64,
        reservation_owner: &str,
    ) -> Result<EnrichmentCheckpoint, CollectorError> {
        let expected_version = database_integer(expected.version)?;
        let request_budget = database_integer(request_budget)?;
        let run_requests_used = database_integer(run_requests_used)?;
        let row = self
            .client
            .query_opt(
                "UPDATE agent_economy.buyer_enrichment_cursors \
                 SET version = version + 1, requests_used_total = requests_used_total + 1, \
                     last_run_budget = $7, last_run_requests_used = $8, \
                     reservation_owner = $9, \
                     reservation_expires_at = now() + interval '5 minutes', \
                     updated_at = now() \
                 WHERE namespace_id = $1::text::uuid \
                   AND buyer_handle_id = $2 AND chain_scope = $3 \
                   AND handle_value = $4 AND version = $5 \
                   AND cursor IS NOT DISTINCT FROM $6 \
                   AND (reservation_owner IS NULL OR reservation_expires_at <= now()) \
                 RETURNING cursor, version, requests_used_total, last_run_budget, \
                           last_run_requests_used, reservation_owner, complete",
                &[
                    &target.namespace_id,
                    &target.buyer_handle_id,
                    &target.chain.as_str(),
                    &target.handle_value,
                    &expected_version,
                    &expected.cursor,
                    &request_budget,
                    &run_requests_used,
                    &reservation_owner,
                ],
            )
            .await
            .map_err(|_| CollectorError::EnrichmentStateStorage)?
            .ok_or(CollectorError::CursorConflict)?;
        checkpoint_from_enrichment_row(row)
    }

    async fn compare_and_set(
        &mut self,
        target: &BuyerHistoryTarget,
        expected: &EnrichmentCheckpoint,
        next_cursor: Option<String>,
        complete: bool,
    ) -> Result<EnrichmentCheckpoint, CollectorError> {
        let expected_version = database_integer(expected.version)?;
        let row = self
            .client
            .query_opt(
                "UPDATE agent_economy.buyer_enrichment_cursors \
                 SET cursor = $7, version = version + 1, complete = $8, \
                     reservation_owner = NULL, reservation_expires_at = NULL, updated_at = now() \
                 WHERE namespace_id = $1::text::uuid \
                   AND buyer_handle_id = $2 AND chain_scope = $3 \
                   AND handle_value = $4 AND version = $5 \
                   AND cursor IS NOT DISTINCT FROM $6 \
                   AND reservation_owner IS NOT DISTINCT FROM $9 \
                   AND (reservation_owner IS NULL OR reservation_expires_at > now()) \
                 RETURNING cursor, version, requests_used_total, last_run_budget, \
                           last_run_requests_used, reservation_owner, complete",
                &[
                    &target.namespace_id,
                    &target.buyer_handle_id,
                    &target.chain.as_str(),
                    &target.handle_value,
                    &expected_version,
                    &expected.cursor,
                    &next_cursor,
                    &complete,
                    &expected.reservation_owner,
                ],
            )
            .await
            .map_err(|_| CollectorError::EnrichmentStateStorage)?
            .ok_or(CollectorError::CursorConflict)?;
        checkpoint_from_enrichment_row(row)
    }
}

fn checkpoint_from_enrichment_row(
    row: tokio_postgres::Row,
) -> Result<EnrichmentCheckpoint, CollectorError> {
    let version: i64 = row.get("version");
    let requests_used_total: i64 = row.get("requests_used_total");
    let last_run_budget: i64 = row.get("last_run_budget");
    let last_run_requests_used: i64 = row.get("last_run_requests_used");
    Ok(EnrichmentCheckpoint::with_reservation(
        row.get("cursor"),
        u64::try_from(version).map_err(|_| CollectorError::EnrichmentStateStorage)?,
        u64::try_from(requests_used_total).map_err(|_| CollectorError::EnrichmentStateStorage)?,
        u64::try_from(last_run_budget).map_err(|_| CollectorError::EnrichmentStateStorage)?,
        u64::try_from(last_run_requests_used)
            .map_err(|_| CollectorError::EnrichmentStateStorage)?,
        row.get("reservation_owner"),
        row.get("complete"),
    ))
}

pub struct PostgresFinalizedHistoryCache {
    client: Client,
}

impl PostgresFinalizedHistoryCache {
    pub const fn new(client: Client) -> Self {
        Self { client }
    }
}

impl FinalizedHistoryCache for PostgresFinalizedHistoryCache {
    async fn store_finalized(
        &mut self,
        record: FinalizedHistoryRecord,
    ) -> Result<(), CollectorError> {
        let transaction = self
            .client
            .transaction()
            .await
            .map_err(|_| CollectorError::FinalizedHistoryStorage)?;
        transaction
            .execute(
                "INSERT INTO agent_economy.buyer_finalized_history \
                    (namespace_id, buyer_handle_id, chain_scope, transaction_reference, \
                     block_reference) \
                 VALUES ($1::text::uuid, $2, $3, $4, $5) \
                 ON CONFLICT (namespace_id, buyer_handle_id, chain_scope, \
                              transaction_reference) DO NOTHING",
                &[
                    &record.namespace_id,
                    &record.buyer_handle_id,
                    &record.chain.as_str(),
                    &record.transaction_reference,
                    &record.block_reference,
                ],
            )
            .await
            .map_err(|_| CollectorError::FinalizedHistoryStorage)?;
        let stored = transaction
            .query_one(
                "SELECT block_reference \
                 FROM agent_economy.buyer_finalized_history \
                 WHERE namespace_id = $1::text::uuid AND buyer_handle_id = $2 \
                   AND chain_scope = $3 AND transaction_reference = $4",
                &[
                    &record.namespace_id,
                    &record.buyer_handle_id,
                    &record.chain.as_str(),
                    &record.transaction_reference,
                ],
            )
            .await
            .map_err(|_| CollectorError::FinalizedHistoryStorage)?;
        let stored_block: String = stored.get("block_reference");
        if stored_block != record.block_reference {
            return Err(CollectorError::FinalizedHistoryConflict);
        }
        transaction
            .execute(
                "INSERT INTO agent_economy.buyer_finalized_history_evidence \
                    (namespace_id, buyer_handle_id, chain_scope, transaction_reference, \
                     evidence_id) \
                 VALUES ($1::text::uuid, $2, $3, $4, $5) \
                 ON CONFLICT (namespace_id, buyer_handle_id, chain_scope, \
                              transaction_reference, evidence_id) DO NOTHING",
                &[
                    &record.namespace_id,
                    &record.buyer_handle_id,
                    &record.chain.as_str(),
                    &record.transaction_reference,
                    &record.evidence_id,
                ],
            )
            .await
            .map_err(|_| CollectorError::FinalizedHistoryStorage)?;
        transaction
            .commit()
            .await
            .map_err(|_| CollectorError::FinalizedHistoryStorage)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EnrichmentReport {
    requests_used: u64,
    finalized_records_cached: u64,
    unfinalized_records_skipped: u64,
    complete: bool,
    budget_exhausted: bool,
}

impl EnrichmentReport {
    pub const fn requests_used(&self) -> u64 {
        self.requests_used
    }

    pub const fn finalized_records_cached(&self) -> u64 {
        self.finalized_records_cached
    }

    pub const fn unfinalized_records_skipped(&self) -> u64 {
        self.unfinalized_records_skipped
    }

    pub const fn complete(&self) -> bool {
        self.complete
    }

    pub const fn budget_exhausted(&self) -> bool {
        self.budget_exhausted
    }
}

pub struct BuyerHistoryEnricher<'a, T, A, S, C> {
    transport: &'a mut T,
    archive: &'a mut A,
    state: &'a mut S,
    cache: &'a mut C,
}

impl<'a, T, A, S, C> BuyerHistoryEnricher<'a, T, A, S, C>
where
    T: HistoryTransport,
    A: HistoryEvidenceArchive,
    S: EnrichmentStateStore,
    C: FinalizedHistoryCache,
{
    pub const fn new(
        transport: &'a mut T,
        archive: &'a mut A,
        state: &'a mut S,
        cache: &'a mut C,
    ) -> Self {
        Self {
            transport,
            archive,
            state,
            cache,
        }
    }

    pub async fn enrich(
        &mut self,
        request: EnrichmentRequest,
    ) -> Result<EnrichmentReport, CollectorError> {
        let mut checkpoint = match self.state.load(&request.target).await? {
            Some(checkpoint) => checkpoint,
            None => self.state.initialize(&request.target).await?,
        };
        if checkpoint.complete {
            checkpoint = self
                .state
                .compare_and_set(&request.target, &checkpoint, None, false)
                .await?;
        }
        let mut report = EnrichmentReport {
            requests_used: 0,
            finalized_records_cached: 0,
            unfinalized_records_skipped: 0,
            complete: checkpoint.complete,
            budget_exhausted: false,
        };
        let mut pages = 0;

        while !checkpoint.complete
            && pages < request.max_pages
            && report.requests_used < request.request_budget
        {
            report.requests_used += 1;
            let reservation_owner = Uuid::new_v4().to_string();
            checkpoint = self
                .state
                .record_request(
                    &request.target,
                    &checkpoint,
                    request.request_budget,
                    report.requests_used,
                    &reservation_owner,
                )
                .await?;
            let response = self
                .transport
                .fetch_history(&request.target, checkpoint.cursor())
                .await?;
            let evidence_id = self.archive.archive_history(
                &request.target,
                &request.observed_date,
                response.status,
                &response.bytes,
            )?;
            if !(200..300).contains(&response.status) {
                return Err(CollectorError::HttpStatus(response.status));
            }
            let page = HistoryPage::decode_alchemy(&request.target, &response.bytes)?;
            if page.next_cursor.as_deref().is_some()
                && page.next_cursor.as_deref() == checkpoint.cursor()
            {
                return Err(CollectorError::InvalidHistoryPage);
            }

            for item in page.items {
                if item.finality == HistoryFinality::Finalized {
                    let record = FinalizedHistoryRecord::try_new(
                        &request.target,
                        item.transaction_reference,
                        item.block_reference,
                        evidence_id.clone(),
                    )?;
                    self.cache.store_finalized(record).await?;
                    report.finalized_records_cached += 1;
                } else {
                    report.unfinalized_records_skipped += 1;
                }
            }

            let complete = page.next_cursor.is_none();
            checkpoint = self
                .state
                .compare_and_set(&request.target, &checkpoint, page.next_cursor, complete)
                .await?;
            pages += 1;
        }

        report.complete = checkpoint.complete;
        report.budget_exhausted =
            !report.complete && report.requests_used >= request.request_budget;
        Ok(report)
    }
}
