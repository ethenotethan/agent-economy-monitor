use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use serde::{
    Deserialize, Serialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::Value;
use sha2::{Digest, Sha256};

const MAX_REGISTRY_BYTES: usize = 1024 * 1024;
const MAX_RPC_BYTES: usize = 8 * 1024 * 1024;
const TRANSFER_TOPIC: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum HistoryError {
    InvalidInput,
    InvalidRegistry,
    InvalidRpcEvidence,
    NonFinalSettlement,
    ConflictingSettlement,
}

pub fn plan_history_ranges(
    earliest: u64,
    frozen_upper: u64,
    indexed_through: Option<u64>,
    max_range: u64,
) -> Result<Vec<(u64, u64)>, HistoryError> {
    if earliest > frozen_upper
        || max_range == 0
        || max_range > 10_000
        || indexed_through.is_some_and(|coordinate| coordinate < earliest)
    {
        return Err(HistoryError::InvalidInput);
    }
    let mut next = indexed_through
        .map(|coordinate| coordinate.checked_add(1).ok_or(HistoryError::InvalidInput))
        .transpose()?
        .unwrap_or(earliest);
    if next > frozen_upper {
        return Ok(Vec::new());
    }
    let mut ranges = Vec::new();
    while next <= frozen_upper {
        let end = next.saturating_add(max_range - 1).min(frozen_upper);
        ranges.push((next, end));
        next = end.checked_add(1).ok_or(HistoryError::InvalidInput)?;
    }
    Ok(ranges)
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FacilitatorRegistry {
    schema_version: u32,
    registry_version: String,
    entries: Vec<FacilitatorEntry>,
    unavailable: Vec<UnavailableChain>,
    #[serde(skip)]
    sha256: String,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FacilitatorEntry {
    facilitator_id: String,
    chain: String,
    address: String,
    asset: String,
    start_unix_seconds: u64,
    active: bool,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct UnavailableChain {
    chain: String,
    reason: String,
}

impl FacilitatorRegistry {
    pub fn parse(bytes: &[u8]) -> Result<Self, HistoryError> {
        if bytes.is_empty() || bytes.len() > MAX_REGISTRY_BYTES {
            return Err(HistoryError::InvalidRegistry);
        }
        let mut registry: Self =
            serde_json::from_slice(bytes).map_err(|_| HistoryError::InvalidRegistry)?;
        if registry.schema_version != 1
            || !valid_version(&registry.registry_version)
            || registry.entries.is_empty()
            || registry.entries.len() > 10_000
            || registry.unavailable.len() > 100
        {
            return Err(HistoryError::InvalidRegistry);
        }
        let mut identities = BTreeSet::new();
        for entry in &registry.entries {
            if !valid_identifier(&entry.facilitator_id)
                || !matches!(entry.chain.as_str(), "base" | "solana")
                || !valid_chain_value(&entry.chain, &entry.address)
                || !valid_chain_value(&entry.chain, &entry.asset)
                || !identities.insert((
                    entry.chain.clone(),
                    normalize(&entry.chain, &entry.address),
                    normalize(&entry.chain, &entry.asset),
                    entry.start_unix_seconds,
                ))
            {
                return Err(HistoryError::InvalidRegistry);
            }
        }
        let mut unavailable = BTreeSet::new();
        for gap in &registry.unavailable {
            if !valid_identifier(&gap.chain)
                || !valid_identifier(&gap.reason)
                || !unavailable.insert(gap.chain.as_str())
                || registry
                    .entries
                    .iter()
                    .any(|entry| entry.chain == gap.chain)
            {
                return Err(HistoryError::InvalidRegistry);
            }
        }
        registry.sha256 = format!("{:x}", Sha256::digest(bytes));
        Ok(registry)
    }

    pub fn version(&self) -> &str {
        &self.registry_version
    }

    pub fn sha256(&self) -> &str {
        &self.sha256
    }

    pub fn entries_for_chain(&self, chain: &str) -> Vec<&FacilitatorEntry> {
        self.entries
            .iter()
            .filter(|entry| entry.chain == chain)
            .collect()
    }

    pub fn unavailable_reason(&self, chain: &str) -> Option<&str> {
        self.unavailable
            .iter()
            .find(|gap| gap.chain == chain)
            .map(|gap| gap.reason.as_str())
    }
}

pub fn parse_admitted_registry(
    bytes: &[u8],
    expected_version: &str,
    expected_sha256: &str,
) -> Result<FacilitatorRegistry, HistoryError> {
    let registry = FacilitatorRegistry::parse(bytes)?;
    if registry.version() != expected_version || registry.sha256() != expected_sha256 {
        return Err(HistoryError::InvalidRegistry);
    }
    Ok(registry)
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct VerifiedFacilitatorPayment {
    chain: String,
    facilitator_id: String,
    facilitator_address: String,
    facilitator_active: bool,
    registry_version: String,
    registry_sha256: String,
    source_coordinate: u64,
    transaction_reference: String,
    settlement_position: u64,
    payer: String,
    recipient: String,
    asset: String,
    amount_atomic: String,
    settled_at_unix_seconds: u64,
    evidence_id: String,
    attribution_class: &'static str,
}

impl VerifiedFacilitatorPayment {
    pub fn payer(&self) -> &str {
        &self.payer
    }

    pub fn recipient(&self) -> &str {
        &self.recipient
    }

    pub fn amount_atomic(&self) -> &str {
        &self.amount_atomic
    }

    pub fn facilitator_id(&self) -> &str {
        &self.facilitator_id
    }

    pub const fn attribution_class(&self) -> &'static str {
        self.attribution_class
    }

    pub fn verification_observation(&self) -> Value {
        let digest = format!(
            "{:x}",
            Sha256::digest(
                format!(
                    "{}:{}:{}:{}:{}:{}:{}:{}:{}",
                    self.chain,
                    self.transaction_reference,
                    self.settlement_position,
                    self.payer,
                    self.recipient,
                    self.asset,
                    self.amount_atomic,
                    self.facilitator_id,
                    self.evidence_id
                )
                .as_bytes()
            )
        );
        serde_json::json!({
            "observation_id": format!("sha256:{digest}"),
            "protocol": "x402",
            "evidence_id": self.evidence_id,
            "observation_hash": digest,
            "parser_version": "x402-facilitator-rpc-v1",
            "height": self.source_coordinate,
            "transaction_reference": self.transaction_reference,
            "settlement_position": self.settlement_position,
            "payer_handle_id": format!("{}:{}", self.chain, self.payer),
            "payer_handle_value": self.payer,
            "recipient_handle": self.recipient,
            "asset": self.asset,
            "amount_atomic": self.amount_atomic,
            "settled_at_unix_seconds": self.settled_at_unix_seconds,
            "finality": "finalized",
            "explicit_attribution_digest": digest,
            "facilitator_id": self.facilitator_id,
            "facilitator_address": self.facilitator_address,
            "facilitator_active": self.facilitator_active,
            "registry_version": self.registry_version,
            "registry_sha256": self.registry_sha256,
            "attribution_class": self.attribution_class,
            "rpc_verification_evidence_id": self.evidence_id,
        })
    }

    pub fn settlement_key(&self) -> (&str, &str, u64) {
        (
            &self.chain,
            &self.transaction_reference,
            self.settlement_position,
        )
    }
}

pub fn verify_facilitator_rpc_evidence(
    registry: &FacilitatorRegistry,
    chain: &str,
    evidence_id: &str,
    source_coordinate: u64,
    bytes: &[u8],
) -> Result<Vec<VerifiedFacilitatorPayment>, HistoryError> {
    if !valid_evidence_id(evidence_id) || bytes.is_empty() || bytes.len() > MAX_RPC_BYTES {
        return Err(HistoryError::InvalidInput);
    }
    if registry.unavailable_reason(chain).is_some() {
        return Err(HistoryError::InvalidInput);
    }
    let entries = registry.entries_for_chain(chain);
    if entries.is_empty() {
        return Err(HistoryError::InvalidRegistry);
    }
    reject_duplicate_json_members(bytes)?;
    let value: Value =
        serde_json::from_slice(bytes).map_err(|_| HistoryError::InvalidRpcEvidence)?;
    let mut payments = match chain {
        "base" => verify_evm(registry, &entries, evidence_id, source_coordinate, &value)?,
        "solana" => verify_solana(registry, &entries, evidence_id, source_coordinate, &value)?,
        _ => return Err(HistoryError::InvalidInput),
    };
    payments.sort_by_key(|payment| {
        (
            payment.source_coordinate,
            payment.transaction_reference.clone(),
            payment.settlement_position,
        )
    });
    for pair in payments.windows(2) {
        if pair[0].settlement_key() == pair[1].settlement_key() && pair[0] != pair[1] {
            return Err(HistoryError::ConflictingSettlement);
        }
    }
    payments.dedup_by(|left, right| left == right);
    Ok(payments)
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

fn reject_duplicate_json_members(bytes: &[u8]) -> Result<(), HistoryError> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    RejectDuplicateMembers
        .deserialize(&mut deserializer)
        .map_err(|_| HistoryError::InvalidRpcEvidence)?;
    deserializer
        .end()
        .map_err(|_| HistoryError::InvalidRpcEvidence)
}

fn verify_evm(
    registry: &FacilitatorRegistry,
    entries: &[&FacilitatorEntry],
    evidence_id: &str,
    source_coordinate: u64,
    value: &Value,
) -> Result<Vec<VerifiedFacilitatorPayment>, HistoryError> {
    let responses = value
        .as_array()
        .filter(|responses| responses.len() == 3)
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    let by_id = responses
        .iter()
        .map(|response| {
            let id = response
                .get("id")
                .and_then(Value::as_str)
                .ok_or(HistoryError::InvalidRpcEvidence)?;
            if response.get("error").is_some() {
                return Err(HistoryError::InvalidRpcEvidence);
            }
            Ok((id, response.get("result").unwrap_or(&Value::Null)))
        })
        .collect::<Result<BTreeMap<_, _>, HistoryError>>()?;
    let block = by_id
        .get("block")
        .and_then(|value| value.as_object())
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    let block_number = parse_hex_u64(
        block
            .get("number")
            .and_then(Value::as_str)
            .ok_or(HistoryError::InvalidRpcEvidence)?,
    )?;
    let timestamp = parse_hex_u64(
        block
            .get("timestamp")
            .and_then(Value::as_str)
            .ok_or(HistoryError::InvalidRpcEvidence)?,
    )?;
    let finalized = parse_hex_u64(
        by_id
            .get("finalized")
            .and_then(|value| value.get("number"))
            .and_then(Value::as_str)
            .ok_or(HistoryError::InvalidRpcEvidence)?,
    )?;
    if block_number != source_coordinate {
        return Err(HistoryError::InvalidRpcEvidence);
    }
    if finalized < block_number {
        return Err(HistoryError::NonFinalSettlement);
    }
    let receipts = by_id
        .get("receipts")
        .and_then(|value| value.as_array())
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    if receipts.len() > 100_000 {
        return Err(HistoryError::InvalidRpcEvidence);
    }
    let mut payments = Vec::new();
    for receipt in receipts {
        let from = receipt
            .get("from")
            .and_then(Value::as_str)
            .ok_or(HistoryError::InvalidRpcEvidence)?;
        let Some(entry) = entries.iter().find(|entry| {
            timestamp >= entry.start_unix_seconds && from.eq_ignore_ascii_case(&entry.address)
        }) else {
            continue;
        };
        if receipt.get("status").and_then(Value::as_str) != Some("0x1")
            || parse_hex_u64(
                receipt
                    .get("blockNumber")
                    .and_then(Value::as_str)
                    .ok_or(HistoryError::InvalidRpcEvidence)?,
            )? != block_number
        {
            return Err(HistoryError::InvalidRpcEvidence);
        }
        let transaction = receipt
            .get("transactionHash")
            .and_then(Value::as_str)
            .filter(|hash| valid_hash(hash))
            .ok_or(HistoryError::InvalidRpcEvidence)?;
        let logs = receipt
            .get("logs")
            .and_then(Value::as_array)
            .ok_or(HistoryError::InvalidRpcEvidence)?;
        for log in logs {
            if log.get("removed").and_then(Value::as_bool) != Some(false) {
                continue;
            }
            let asset = log
                .get("address")
                .and_then(Value::as_str)
                .ok_or(HistoryError::InvalidRpcEvidence)?;
            let topics = log
                .get("topics")
                .and_then(Value::as_array)
                .ok_or(HistoryError::InvalidRpcEvidence)?;
            if !asset.eq_ignore_ascii_case(&entry.asset)
                || topics.len() != 3
                || topics[0].as_str() != Some(TRANSFER_TOPIC)
            {
                continue;
            }
            let payer = topic_address(topics[1].as_str().ok_or(HistoryError::InvalidRpcEvidence)?)?;
            let recipient =
                topic_address(topics[2].as_str().ok_or(HistoryError::InvalidRpcEvidence)?)?;
            let amount = hex_to_decimal(
                log.get("data")
                    .and_then(Value::as_str)
                    .ok_or(HistoryError::InvalidRpcEvidence)?,
            )?;
            let position = parse_hex_u64(
                log.get("logIndex")
                    .and_then(Value::as_str)
                    .ok_or(HistoryError::InvalidRpcEvidence)?,
            )?;
            payments.push(payment(
                registry,
                entry,
                evidence_id,
                source_coordinate,
                transaction,
                position,
                payer,
                recipient,
                normalize("base", asset),
                amount,
                timestamp,
            ));
        }
    }
    Ok(payments)
}

fn verify_solana(
    registry: &FacilitatorRegistry,
    entries: &[&FacilitatorEntry],
    evidence_id: &str,
    source_coordinate: u64,
    value: &Value,
) -> Result<Vec<VerifiedFacilitatorPayment>, HistoryError> {
    let block = value
        .get("result")
        .and_then(Value::as_object)
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    block
        .get("blockHeight")
        .and_then(Value::as_u64)
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    let parent_slot = block
        .get("parentSlot")
        .and_then(Value::as_u64)
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    if parent_slot >= source_coordinate {
        return Err(HistoryError::InvalidRpcEvidence);
    }
    let timestamp = u64::try_from(
        block
            .get("blockTime")
            .and_then(Value::as_i64)
            .ok_or(HistoryError::InvalidRpcEvidence)?,
    )
    .map_err(|_| HistoryError::InvalidRpcEvidence)?;
    let transactions = block
        .get("transactions")
        .and_then(Value::as_array)
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    if transactions.len() > 100_000 {
        return Err(HistoryError::InvalidRpcEvidence);
    }
    let mut payments = Vec::new();
    for transaction in transactions {
        if !transaction
            .get("meta")
            .is_some_and(|meta| meta.get("err").is_some_and(Value::is_null))
        {
            continue;
        }
        let keys = transaction
            .pointer("/transaction/message/accountKeys")
            .and_then(Value::as_array)
            .ok_or(HistoryError::InvalidRpcEvidence)?;
        let Some(entry) = entries.iter().find(|entry| {
            timestamp >= entry.start_unix_seconds
                && keys.iter().any(|key| {
                    key.get("signer").and_then(Value::as_bool) == Some(true)
                        && key.get("pubkey").and_then(Value::as_str) == Some(entry.address.as_str())
                })
        }) else {
            continue;
        };
        let transaction_reference = transaction
            .pointer("/transaction/signatures/0")
            .and_then(Value::as_str)
            .filter(|signature| !signature.is_empty() && signature.len() <= 128)
            .ok_or(HistoryError::InvalidRpcEvidence)?;
        let instructions = transaction
            .pointer("/meta/innerInstructions")
            .and_then(Value::as_array)
            .ok_or(HistoryError::InvalidRpcEvidence)?;
        let mut position = 0_u64;
        for group in instructions {
            for instruction in group
                .get("instructions")
                .and_then(Value::as_array)
                .ok_or(HistoryError::InvalidRpcEvidence)?
            {
                let info = match (
                    instruction.get("program").and_then(Value::as_str),
                    instruction.pointer("/parsed/type").and_then(Value::as_str),
                    instruction.pointer("/parsed/info"),
                ) {
                    (Some("spl-token"), Some("transferChecked"), Some(info)) => info,
                    _ => continue,
                };
                let mint = info
                    .get("mint")
                    .and_then(Value::as_str)
                    .ok_or(HistoryError::InvalidRpcEvidence)?;
                if mint != entry.asset {
                    continue;
                }
                let payer = info
                    .get("authority")
                    .and_then(Value::as_str)
                    .filter(|value| valid_solana_value(value))
                    .ok_or(HistoryError::InvalidRpcEvidence)?;
                let recipient = info
                    .get("destination")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty() && value.len() <= 64)
                    .ok_or(HistoryError::InvalidRpcEvidence)?;
                let amount = info
                    .pointer("/tokenAmount/amount")
                    .and_then(Value::as_str)
                    .filter(|value| valid_decimal(value))
                    .ok_or(HistoryError::InvalidRpcEvidence)?;
                payments.push(payment(
                    registry,
                    entry,
                    evidence_id,
                    source_coordinate,
                    transaction_reference,
                    position,
                    payer.to_owned(),
                    recipient.to_owned(),
                    mint.to_owned(),
                    amount.to_owned(),
                    timestamp,
                ));
                position = position
                    .checked_add(1)
                    .ok_or(HistoryError::InvalidRpcEvidence)?;
            }
        }
    }
    Ok(payments)
}

#[allow(clippy::too_many_arguments)]
fn payment(
    registry: &FacilitatorRegistry,
    entry: &FacilitatorEntry,
    evidence_id: &str,
    source_coordinate: u64,
    transaction_reference: &str,
    settlement_position: u64,
    payer: String,
    recipient: String,
    asset: String,
    amount_atomic: String,
    settled_at_unix_seconds: u64,
) -> VerifiedFacilitatorPayment {
    VerifiedFacilitatorPayment {
        chain: entry.chain.clone(),
        facilitator_id: entry.facilitator_id.clone(),
        facilitator_address: entry.address.clone(),
        facilitator_active: entry.active,
        registry_version: registry.registry_version.clone(),
        registry_sha256: registry.sha256.clone(),
        source_coordinate,
        transaction_reference: transaction_reference.to_owned(),
        settlement_position,
        payer,
        recipient,
        asset,
        amount_atomic,
        settled_at_unix_seconds,
        evidence_id: evidence_id.to_owned(),
        attribution_class: "known_facilitator_onchain",
    }
}

fn topic_address(topic: &str) -> Result<String, HistoryError> {
    if topic.len() != 66
        || !topic.starts_with("0x")
        || !topic[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
        || topic[2..26].bytes().any(|byte| byte != b'0')
    {
        return Err(HistoryError::InvalidRpcEvidence);
    }
    Ok(format!("0x{}", topic[26..].to_ascii_lowercase()))
}

fn parse_hex_u64(value: &str) -> Result<u64, HistoryError> {
    let digits = value
        .strip_prefix("0x")
        .filter(|digits| !digits.is_empty() && (digits.len() == 1 || !digits.starts_with('0')))
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    u64::from_str_radix(digits, 16).map_err(|_| HistoryError::InvalidRpcEvidence)
}

fn hex_to_decimal(value: &str) -> Result<String, HistoryError> {
    let digits = value
        .strip_prefix("0x")
        .filter(|digits| !digits.is_empty() && digits.len() <= 64)
        .ok_or(HistoryError::InvalidRpcEvidence)?;
    if !digits.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err(HistoryError::InvalidRpcEvidence);
    }
    let mut decimal = vec![0_u8];
    for nibble in digits.bytes() {
        let value = match nibble {
            b'0'..=b'9' => nibble - b'0',
            b'a'..=b'f' => nibble - b'a' + 10,
            b'A'..=b'F' => nibble - b'A' + 10,
            _ => return Err(HistoryError::InvalidRpcEvidence),
        };
        let mut carry = value as u16;
        for digit in decimal.iter_mut().rev() {
            let next = u16::from(*digit) * 16 + carry;
            *digit = (next % 10) as u8;
            carry = next / 10;
        }
        while carry > 0 {
            decimal.insert(0, (carry % 10) as u8);
            carry /= 10;
        }
    }
    let first = decimal
        .iter()
        .position(|digit| *digit != 0)
        .unwrap_or(decimal.len() - 1);
    Ok(decimal[first..]
        .iter()
        .map(|digit| char::from(b'0' + *digit))
        .collect())
}

fn valid_identifier(value: &str) -> bool {
    (2..=128).contains(&value.len())
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

fn valid_version(value: &str) -> bool {
    (3..=128).contains(&value.len())
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b':' | b'/')
        })
}

fn valid_chain_value(chain: &str, value: &str) -> bool {
    match chain {
        "base" => valid_address(value),
        "solana" => valid_solana_value(value),
        _ => false,
    }
}

fn valid_address(value: &str) -> bool {
    value.len() == 42
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_hash(value: &str) -> bool {
    value.len() == 66
        && value.starts_with("0x")
        && value[2..].bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_solana_value(value: &str) -> bool {
    (32..=44).contains(&value.len())
        && value.bytes().all(|byte| {
            matches!(byte, b'1'..=b'9' | b'A'..=b'H' | b'J'..=b'N' | b'P'..=b'Z' | b'a'..=b'k' | b'm'..=b'z')
        })
}

fn valid_decimal(value: &str) -> bool {
    !value.is_empty()
        && (value.len() == 1 || !value.starts_with('0'))
        && value.bytes().all(|byte| byte.is_ascii_digit())
}

fn normalize(chain: &str, value: &str) -> String {
    if chain == "base" {
        value.to_ascii_lowercase()
    } else {
        value.to_owned()
    }
}

fn valid_evidence_id(value: &str) -> bool {
    value
        .strip_prefix("evidence:sha256:")
        .is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
        })
}
