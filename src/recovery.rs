use std::collections::BTreeSet;

use agent_economy_contracts::ProtocolObservation;
use agent_economy_evidence_store::EvidenceObject;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;

use crate::collect::{ArchivedEvidence, verify_replayed_evidence};

pub const MAX_MANIFEST_BYTES: usize = 8 * 1024 * 1024;
const MAX_EVIDENCE_OBJECTS: usize = 10_000;
const MAX_EVIDENCE_BYTES: usize = 4 * 1024 * 1024;
const MAX_AMOUNT_DIGITS: usize = 78;
const REQUIRED_COVERAGE: [&str; 8] = [
    "base:mpp",
    "base:x402",
    "ethereum:mpp",
    "ethereum:x402",
    "solana:mpp",
    "solana:x402",
    "tempo:mpp",
    "tempo:x402",
];

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct RecoveredBuyerDossier {
    buyer_handle_id: String,
    manifest_object: String,
    manifest_generation: u64,
    event_count: usize,
    total_amount_atomic: String,
    coverage: Vec<&'static str>,
    evidence_objects: Vec<String>,
    provenance_ids: Vec<String>,
    digest: String,
}

impl RecoveredBuyerDossier {
    pub fn buyer_handle_id(&self) -> &str {
        &self.buyer_handle_id
    }

    pub fn manifest_object(&self) -> &str {
        &self.manifest_object
    }

    pub const fn manifest_generation(&self) -> u64 {
        self.manifest_generation
    }

    pub const fn event_count(&self) -> usize {
        self.event_count
    }

    pub fn total_amount_atomic(&self) -> &str {
        &self.total_amount_atomic
    }

    pub fn coverage(&self) -> &[&str] {
        &self.coverage
    }

    pub fn evidence_objects(&self) -> &[String] {
        &self.evidence_objects
    }

    pub fn provenance_ids(&self) -> &[String] {
        &self.provenance_ids
    }

    pub fn digest(&self) -> &str {
        &self.digest
    }
}

pub struct RecoveryDrill;

impl RecoveryDrill {
    pub fn rebuild_manifest(
        buyer_handle_id: &str,
        manifest: &[u8],
    ) -> Result<RecoveredBuyerDossier, RecoveryError> {
        if manifest.len() > MAX_MANIFEST_BYTES {
            return Err(RecoveryError::ManifestTooLarge);
        }
        if buyer_handle_id.trim().is_empty() {
            return Err(RecoveryError::InvalidPayload);
        }
        let stored: StoredRecoveryManifest =
            serde_json::from_slice(manifest).map_err(|_| RecoveryError::InvalidManifest)?;
        if stored.manifest_generation == 0 || !valid_manifest_object(&stored.manifest_object) {
            return Err(RecoveryError::InvalidManifest);
        }
        let mut entries = stored.entries;
        if entries.is_empty() || entries.len() > MAX_EVIDENCE_OBJECTS {
            return Err(RecoveryError::InvalidManifest);
        }
        entries.sort_by(|left, right| {
            left.object_name
                .cmp(&right.object_name)
                .then_with(|| left.generation.cmp(&right.generation))
                .then_with(|| left.observation_id.cmp(&right.observation_id))
        });

        let mut coverage = BTreeSet::new();
        let mut settlements = BTreeSet::new();
        let mut evidence_objects = Vec::with_capacity(entries.len());
        let mut provenance_ids = BTreeSet::new();
        let mut total_amount_atomic = "0".to_owned();
        let mut digest = Sha256::new();
        digest.update(b"agent-economy-recovery-v2\0");
        digest.update(buyer_handle_id.as_bytes());
        digest.update(b"\0");
        digest.update(stored.manifest_object.as_bytes());
        digest.update(b"\0");
        digest.update(stored.manifest_generation.to_string().as_bytes());

        for entry in entries {
            let recovered = entry.replay(buyer_handle_id)?;
            if !settlements.insert((entry.chain.clone(), recovered.settlement_id)) {
                return Err(RecoveryError::DuplicateSettlement);
            }
            total_amount_atomic = add_decimal(&total_amount_atomic, &recovered.amount_atomic)?;
            coverage.insert(recovered.coverage);
            provenance_ids.insert(entry.observation_id.clone());
            digest.update(b"\0");
            digest.update(entry.object_name.as_bytes());
            digest.update(b"\0");
            digest.update(entry.generation.to_string().as_bytes());
            digest.update(b"\0");
            digest.update(entry.observation_id.as_bytes());
            digest.update(b"\0");
            digest.update(recovered.content_digest);
            evidence_objects.push(format!("{}#{}", entry.object_name, entry.generation));
        }

        if coverage.iter().copied().collect::<Vec<_>>() != REQUIRED_COVERAGE {
            return Err(RecoveryError::IncompleteCoverage);
        }

        Ok(RecoveredBuyerDossier {
            buyer_handle_id: buyer_handle_id.to_owned(),
            manifest_object: stored.manifest_object,
            manifest_generation: stored.manifest_generation,
            event_count: settlements.len(),
            total_amount_atomic,
            coverage: coverage.into_iter().collect(),
            evidence_objects,
            provenance_ids: provenance_ids.into_iter().collect(),
            digest: format!("{:x}", digest.finalize()),
        })
    }
}

fn valid_manifest_object(value: &str) -> bool {
    value.starts_with("recovery/manifests/")
        && value.ends_with(".json")
        && !value.split('/').any(|part| part.is_empty() || part == "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'/' | b'.' | b'_' | b'-'))
}

fn add_decimal(left: &str, right: &str) -> Result<String, RecoveryError> {
    let mut carry = 0_u8;
    let mut digits = Vec::with_capacity(left.len().max(right.len()) + 1);
    let mut left = left.bytes().rev();
    let mut right = right.bytes().rev();
    loop {
        let a = left.next().map(|byte| byte - b'0');
        let b = right.next().map(|byte| byte - b'0');
        if a.is_none() && b.is_none() && carry == 0 {
            break;
        }
        let sum = a.unwrap_or(0) + b.unwrap_or(0) + carry;
        digits.push(b'0' + sum % 10);
        carry = sum / 10;
    }
    if digits.len() > MAX_AMOUNT_DIGITS {
        return Err(RecoveryError::InvalidPayload);
    }
    digits.reverse();
    String::from_utf8(digits).map_err(|_| RecoveryError::InvalidPayload)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecoveryManifest {
    manifest_object: String,
    manifest_generation: u64,
    entries: Vec<StoredRecoveryEvidence>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StoredRecoveryEvidence {
    object_name: String,
    generation: u64,
    buyer_handle_id: String,
    chain: String,
    source_id: String,
    observed_at_unix_ms: i64,
    height: u64,
    observation_id: String,
    payload_base64: String,
}

struct ReplayedObservation {
    settlement_id: String,
    amount_atomic: String,
    coverage: &'static str,
    content_digest: [u8; 32],
}

impl StoredRecoveryEvidence {
    fn replay(&self, expected_buyer: &str) -> Result<ReplayedObservation, RecoveryError> {
        if self.generation == 0
            || self.buyer_handle_id != expected_buyer
            || self.source_id != format!("alchemy-{}", self.chain)
            || self.observed_at_unix_ms <= 0
            || !matches!(
                self.chain.as_str(),
                "ethereum" | "base" | "solana" | "tempo"
            )
        {
            return Err(RecoveryError::InvalidManifest);
        }
        let bytes = STANDARD
            .decode(&self.payload_base64)
            .map_err(|_| RecoveryError::InvalidPayload)?;
        if bytes.is_empty() || bytes.len() > MAX_EVIDENCE_BYTES {
            return Err(RecoveryError::InvalidPayload);
        }
        let object =
            EvidenceObject::parse(&self.object_name).map_err(|_| RecoveryError::InvalidObject)?;
        let digest = Sha256::digest(&bytes);
        if object.sha256() != format!("{digest:x}") {
            return Err(RecoveryError::DigestMismatch);
        }
        let content_digest: [u8; 32] = digest.into();
        let parts = self.object_name.split('/').collect::<Vec<_>>();
        let observed_at = OffsetDateTime::from_unix_timestamp(self.observed_at_unix_ms / 1_000)
            .map_err(|_| RecoveryError::InvalidPayload)?;
        if parts[1] != self.source_id || parts[2] != observed_at.date().to_string() {
            return Err(RecoveryError::InvalidPayload);
        }

        let archived = ArchivedEvidence::from_verified_readback(
            self.object_name.clone(),
            object.sha256(),
            "application/vnd.agent-economy.rpc".to_owned(),
            self.height,
            bytes,
        )
        .map_err(|_| RecoveryError::InvalidPayload)?;
        let batch = verify_replayed_evidence(
            self.chain.clone(),
            self.source_id.clone(),
            self.observed_at_unix_ms,
            self.height,
            self.height,
            vec![archived],
        )
        .map_err(|_| RecoveryError::InvalidPayload)?;
        let mut matching = batch
            .observations()
            .iter()
            .filter(|observation| observation.id() == self.observation_id);
        let observation = matching.next().ok_or(RecoveryError::InvalidPayload)?;
        if matching.next().is_some() {
            return Err(RecoveryError::InvalidPayload);
        }
        let (protocol, amount_atomic) = match observation.observation().protocol() {
            ProtocolObservation::X402(value) => ("x402", value.amount_atomic().to_owned()),
            ProtocolObservation::Mpp(value) => ("mpp", value.amount_atomic().to_owned()),
            ProtocolObservation::MppDiscovery(value) => (
                "mpp",
                value
                    .amount_atomic()
                    .ok_or(RecoveryError::InvalidPayload)?
                    .to_owned(),
            ),
        };
        if amount_atomic.is_empty()
            || amount_atomic.len() > MAX_AMOUNT_DIGITS
            || !amount_atomic.bytes().all(|byte| byte.is_ascii_digit())
            || (amount_atomic != "0" && amount_atomic.starts_with('0'))
        {
            return Err(RecoveryError::InvalidPayload);
        }
        let coverage = coverage(&self.chain, protocol)?;
        Ok(ReplayedObservation {
            settlement_id: observation.observation().event_key().canonical_id(),
            amount_atomic,
            coverage,
            content_digest,
        })
    }
}

fn coverage(chain: &str, protocol: &str) -> Result<&'static str, RecoveryError> {
    match (chain, protocol) {
        ("base", "mpp") => Ok("base:mpp"),
        ("base", "x402") => Ok("base:x402"),
        ("ethereum", "mpp") => Ok("ethereum:mpp"),
        ("ethereum", "x402") => Ok("ethereum:x402"),
        ("solana", "mpp") => Ok("solana:mpp"),
        ("solana", "x402") => Ok("solana:x402"),
        ("tempo", "mpp") => Ok("tempo:mpp"),
        ("tempo", "x402") => Ok("tempo:x402"),
        _ => Err(RecoveryError::UnsupportedCoverage),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    InvalidManifest,
    ManifestTooLarge,
    InvalidObject,
    DigestMismatch,
    InvalidPayload,
    UnsupportedCoverage,
    DuplicateSettlement,
    IncompleteCoverage,
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidManifest => "invalid recovery manifest",
            Self::ManifestTooLarge => "recovery manifest exceeds the bounded size limit",
            Self::InvalidObject => "invalid evidence object name",
            Self::DigestMismatch => "evidence digest mismatch",
            Self::InvalidPayload => "invalid recovery payload",
            Self::UnsupportedCoverage => "unsupported chain or protocol",
            Self::DuplicateSettlement => "duplicate settlement in recovery evidence",
            Self::IncompleteCoverage => {
                "recovery evidence does not cover every launch chain and protocol"
            }
        })
    }
}

impl std::error::Error for RecoveryError {}
