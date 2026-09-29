use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttributionLevel {
    Verified,
    Strong,
    Weak,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttributionMethod {
    ExplicitRequirement,
    UniqueExact,
    SharedExact,
    None,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SettlementFinality {
    Confirmed,
    Finalized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Settlement {
    id: String,
    protocol: String,
    network: String,
    asset: String,
    amount_atomic: String,
    pay_to: String,
    finality: SettlementFinality,
    requirement_id: Option<String>,
    evidence_ids: Vec<String>,
}

impl Settlement {
    #[allow(clippy::too_many_arguments)]
    pub fn new<I, E>(
        id: impl Into<String>,
        protocol: impl Into<String>,
        network: impl Into<String>,
        asset: impl Into<String>,
        amount_atomic: impl Into<String>,
        pay_to: impl Into<String>,
        finality: SettlementFinality,
        requirement_id: Option<&str>,
        evidence_ids: I,
    ) -> Result<Self, AttributionError>
    where
        I: IntoIterator<Item = E>,
        E: Into<String>,
    {
        let settlement = Self {
            id: id.into(),
            protocol: protocol.into(),
            network: network.into(),
            asset: asset.into(),
            amount_atomic: amount_atomic.into(),
            pay_to: pay_to.into(),
            finality,
            requirement_id: requirement_id.map(str::to_owned),
            evidence_ids: normalize_evidence(evidence_ids)?,
        };
        validate_required([
            &settlement.id,
            &settlement.protocol,
            &settlement.network,
            &settlement.asset,
            &settlement.amount_atomic,
            &settlement.pay_to,
        ])?;
        Ok(settlement)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PaymentRequirement {
    id: String,
    payment_option_id: String,
    endpoint_id: String,
    service_id: String,
    protocol: String,
    network: String,
    asset: String,
    amount_atomic: String,
    pay_to: String,
    evidence_ids: Vec<String>,
}

impl PaymentRequirement {
    #[allow(clippy::too_many_arguments)]
    pub fn new<I, E>(
        id: impl Into<String>,
        payment_option_id: impl Into<String>,
        endpoint_id: impl Into<String>,
        service_id: impl Into<String>,
        protocol: impl Into<String>,
        network: impl Into<String>,
        asset: impl Into<String>,
        amount_atomic: impl Into<String>,
        pay_to: impl Into<String>,
        evidence_ids: I,
    ) -> Result<Self, AttributionError>
    where
        I: IntoIterator<Item = E>,
        E: Into<String>,
    {
        let requirement = Self {
            id: id.into(),
            payment_option_id: payment_option_id.into(),
            endpoint_id: endpoint_id.into(),
            service_id: service_id.into(),
            protocol: protocol.into(),
            network: network.into(),
            asset: asset.into(),
            amount_atomic: amount_atomic.into(),
            pay_to: pay_to.into(),
            evidence_ids: normalize_evidence(evidence_ids)?,
        };
        validate_required([
            &requirement.id,
            &requirement.payment_option_id,
            &requirement.endpoint_id,
            &requirement.service_id,
            &requirement.protocol,
            &requirement.network,
            &requirement.asset,
            &requirement.amount_atomic,
            &requirement.pay_to,
        ])?;
        Ok(requirement)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributionCandidate {
    id: String,
    requirement_id: String,
    payment_option_id: String,
    endpoint_id: String,
    service_id: String,
    confidence_bps: u16,
    evidence_ids: Vec<String>,
}

impl AttributionCandidate {
    pub fn id(&self) -> &str {
        &self.id
    }

    pub fn requirement_id(&self) -> &str {
        &self.requirement_id
    }

    pub fn payment_option_id(&self) -> &str {
        &self.payment_option_id
    }

    pub fn endpoint_id(&self) -> &str {
        &self.endpoint_id
    }

    pub fn service_id(&self) -> &str {
        &self.service_id
    }

    pub const fn confidence_bps(&self) -> u16 {
        self.confidence_bps
    }

    pub fn evidence_ids(&self) -> &[String] {
        &self.evidence_ids
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributionResult {
    level: AttributionLevel,
    method: AttributionMethod,
    explicit_requirement_id: Option<String>,
    candidates: Vec<AttributionCandidate>,
    engine_version: String,
    evidence_ids: Vec<String>,
    input_snapshot_hash: String,
    encoded: Vec<u8>,
    state_hash: String,
}

impl AttributionResult {
    pub const fn level(&self) -> AttributionLevel {
        self.level
    }

    pub const fn method(&self) -> AttributionMethod {
        self.method
    }

    pub fn explicit_requirement_id(&self) -> Option<&str> {
        self.explicit_requirement_id.as_deref()
    }

    pub fn candidates(&self) -> &[AttributionCandidate] {
        &self.candidates
    }

    pub fn engine_version(&self) -> &str {
        &self.engine_version
    }

    pub fn evidence_ids(&self) -> &[String] {
        &self.evidence_ids
    }

    pub fn input_snapshot_hash(&self) -> &str {
        &self.input_snapshot_hash
    }

    pub fn encode(&self) -> &[u8] {
        &self.encoded
    }

    pub fn state_hash(&self) -> &str {
        &self.state_hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AttributionEngine {
    version: String,
}

impl AttributionEngine {
    pub fn new(version: impl Into<String>) -> Result<Self, AttributionError> {
        let version = version.into();
        validate_required([&version])?;
        Ok(Self { version })
    }

    pub fn attribute<I>(
        &self,
        settlement: &Settlement,
        requirements: I,
    ) -> Result<AttributionResult, AttributionError>
    where
        I: IntoIterator<Item = PaymentRequirement>,
    {
        if settlement.finality != SettlementFinality::Finalized {
            return Err(AttributionError::SettlementNotFinalized);
        }
        let mut requirements = requirements.into_iter().collect::<Vec<_>>();
        requirements.sort_by(|left, right| left.id.cmp(&right.id));
        if requirements.windows(2).any(|pair| pair[0].id == pair[1].id) {
            return Err(AttributionError::DuplicateRequirement);
        }
        let input_snapshot_hash = input_snapshot_hash(settlement, &requirements);
        let mut candidates = Vec::new();
        let (level, method) = if let Some(explicit_id) = &settlement.requirement_id {
            if let Some(requirement) = requirements.iter().find(|requirement| {
                requirement.id == *explicit_id && exact_match(settlement, requirement)
            }) {
                candidates.push(candidate(settlement, requirement, 10_000));
            }
            if candidates.is_empty() {
                (AttributionLevel::Unknown, AttributionMethod::None)
            } else {
                (
                    AttributionLevel::Verified,
                    AttributionMethod::ExplicitRequirement,
                )
            }
        } else {
            let exact = requirements
                .iter()
                .filter(|requirement| exact_match(settlement, requirement))
                .collect::<Vec<_>>();
            if exact.len() == 1 {
                candidates.push(candidate(settlement, exact[0], 8_500));
                (AttributionLevel::Strong, AttributionMethod::UniqueExact)
            } else if exact.len() > 1 {
                candidates.extend(
                    exact
                        .into_iter()
                        .map(|requirement| candidate(settlement, requirement, 5_000)),
                );
                (AttributionLevel::Weak, AttributionMethod::SharedExact)
            } else {
                (AttributionLevel::Unknown, AttributionMethod::None)
            }
        };
        Ok(finish_result(
            &self.version,
            settlement,
            &input_snapshot_hash,
            level,
            method,
            candidates,
        ))
    }
}

fn exact_match(settlement: &Settlement, requirement: &PaymentRequirement) -> bool {
    settlement.protocol == requirement.protocol
        && settlement.network == requirement.network
        && settlement.asset == requirement.asset
        && settlement.amount_atomic == requirement.amount_atomic
        && settlement.pay_to == requirement.pay_to
}

fn candidate(
    settlement: &Settlement,
    requirement: &PaymentRequirement,
    confidence_bps: u16,
) -> AttributionCandidate {
    let mut evidence_ids = settlement.evidence_ids.clone();
    evidence_ids.extend(requirement.evidence_ids.iter().cloned());
    evidence_ids.sort();
    evidence_ids.dedup();
    let digest = digest_fields([
        settlement.id.as_str(),
        requirement.id.as_str(),
        requirement.payment_option_id.as_str(),
        requirement.endpoint_id.as_str(),
        requirement.service_id.as_str(),
    ]);
    AttributionCandidate {
        id: format!("attribution:sha256:{digest}"),
        requirement_id: requirement.id.clone(),
        payment_option_id: requirement.payment_option_id.clone(),
        endpoint_id: requirement.endpoint_id.clone(),
        service_id: requirement.service_id.clone(),
        confidence_bps,
        evidence_ids,
    }
}

fn finish_result(
    version: &str,
    settlement: &Settlement,
    input_snapshot_hash: &str,
    level: AttributionLevel,
    method: AttributionMethod,
    mut candidates: Vec<AttributionCandidate>,
) -> AttributionResult {
    candidates.sort_by(|left, right| left.requirement_id.cmp(&right.requirement_id));
    let level_name = match level {
        AttributionLevel::Verified => "verified",
        AttributionLevel::Strong => "strong",
        AttributionLevel::Weak => "weak",
        AttributionLevel::Unknown => "unknown",
    };
    let method_name = match method {
        AttributionMethod::ExplicitRequirement => "explicit_requirement",
        AttributionMethod::UniqueExact => "unique_exact",
        AttributionMethod::SharedExact => "shared_exact",
        AttributionMethod::None => "none",
    };
    let mut encoded = Vec::new();
    encode_field(&mut encoded, "aem-attribution-result-v1");
    for field in [
        version,
        settlement.id.as_str(),
        input_snapshot_hash,
        level_name,
        method_name,
        settlement.requirement_id.as_deref().unwrap_or(""),
    ] {
        encode_field(&mut encoded, field);
    }
    encode_count(&mut encoded, settlement.evidence_ids.len());
    for evidence_id in &settlement.evidence_ids {
        encode_field(&mut encoded, evidence_id);
    }
    encode_count(&mut encoded, candidates.len());
    for edge in &candidates {
        for field in [
            edge.id.as_str(),
            edge.requirement_id.as_str(),
            edge.payment_option_id.as_str(),
            edge.endpoint_id.as_str(),
            edge.service_id.as_str(),
        ] {
            encode_field(&mut encoded, field);
        }
        encode_field(&mut encoded, &edge.confidence_bps.to_string());
        encode_count(&mut encoded, edge.evidence_ids.len());
        for evidence_id in &edge.evidence_ids {
            encode_field(&mut encoded, evidence_id);
        }
    }
    let state_hash = hex::encode(Sha256::digest(&encoded));
    AttributionResult {
        level,
        method,
        explicit_requirement_id: settlement.requirement_id.clone(),
        candidates,
        engine_version: version.to_owned(),
        evidence_ids: settlement.evidence_ids.clone(),
        input_snapshot_hash: input_snapshot_hash.to_owned(),
        encoded,
        state_hash,
    }
}

fn input_snapshot_hash(settlement: &Settlement, requirements: &[PaymentRequirement]) -> String {
    let mut encoded = Vec::new();
    encode_field(&mut encoded, "aem-attribution-input-v1");
    for field in [
        settlement.id.as_str(),
        settlement.protocol.as_str(),
        settlement.network.as_str(),
        settlement.asset.as_str(),
        settlement.amount_atomic.as_str(),
        settlement.pay_to.as_str(),
        match settlement.finality {
            SettlementFinality::Confirmed => "confirmed",
            SettlementFinality::Finalized => "finalized",
        },
        settlement.requirement_id.as_deref().unwrap_or(""),
    ] {
        encode_field(&mut encoded, field);
    }
    encode_count(&mut encoded, settlement.evidence_ids.len());
    for evidence_id in &settlement.evidence_ids {
        encode_field(&mut encoded, evidence_id);
    }
    encode_count(&mut encoded, requirements.len());
    for requirement in requirements {
        for field in [
            requirement.id.as_str(),
            requirement.payment_option_id.as_str(),
            requirement.endpoint_id.as_str(),
            requirement.service_id.as_str(),
            requirement.protocol.as_str(),
            requirement.network.as_str(),
            requirement.asset.as_str(),
            requirement.amount_atomic.as_str(),
            requirement.pay_to.as_str(),
        ] {
            encode_field(&mut encoded, field);
        }
        encode_count(&mut encoded, requirement.evidence_ids.len());
        for evidence_id in &requirement.evidence_ids {
            encode_field(&mut encoded, evidence_id);
        }
    }
    hex::encode(Sha256::digest(encoded))
}

fn normalize_evidence<I, E>(evidence_ids: I) -> Result<Vec<String>, AttributionError>
where
    I: IntoIterator<Item = E>,
    E: Into<String>,
{
    let mut evidence_ids = evidence_ids.into_iter().map(Into::into).collect::<Vec<_>>();
    validate_required(evidence_ids.iter())?;
    evidence_ids.sort();
    evidence_ids.dedup();
    if evidence_ids.is_empty() {
        return Err(AttributionError::MissingEvidence);
    }
    Ok(evidence_ids)
}

fn validate_required<'a, I>(values: I) -> Result<(), AttributionError>
where
    I: IntoIterator<Item = &'a String>,
{
    if values.into_iter().any(|value| value.trim().is_empty()) {
        return Err(AttributionError::InvalidField);
    }
    Ok(())
}

fn digest_fields<'a, I>(fields: I) -> String
where
    I: IntoIterator<Item = &'a str>,
{
    let mut hasher = Sha256::new();
    for field in fields {
        hash_field(&mut hasher, field);
    }
    hex::encode(hasher.finalize())
}

fn hash_field(hasher: &mut Sha256, field: &str) {
    hasher.update((field.len() as u64).to_be_bytes());
    hasher.update(field.as_bytes());
}

fn encode_field(encoded: &mut Vec<u8>, field: &str) {
    encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
    encoded.extend_from_slice(field.as_bytes());
}

fn encode_count(encoded: &mut Vec<u8>, count: usize) {
    encoded.extend_from_slice(&(count as u64).to_be_bytes());
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum AttributionError {
    #[error("attribution fields must not be empty")]
    InvalidField,
    #[error("attribution inputs must cite evidence")]
    MissingEvidence,
    #[error("payment requirement ids must be unique within a catalog snapshot")]
    DuplicateRequirement,
    #[error("only finalized settlements can be attributed")]
    SettlementNotFinalized,
}
