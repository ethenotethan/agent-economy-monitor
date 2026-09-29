use std::collections::BTreeMap;

use agent_economy_contracts::{CanonicalEvent, Observation, ObservationError, ProtocolObservation};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ObservationRole {
    Supporting,
    Conflicting,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecutionOutcome {
    Succeeded,
    Reverted,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalityStatus {
    Observed,
    Confirmed,
    Finalized,
    Orphaned,
    Reverted,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalitySubject {
    canonical_event_id: String,
    chain_scope: String,
    transaction_id: String,
}

impl FinalitySubject {
    pub fn new(
        canonical_event_id: impl Into<String>,
        chain_scope: impl Into<String>,
        transaction_id: impl Into<String>,
    ) -> Result<Self, ReducerError> {
        let subject = Self {
            canonical_event_id: canonical_event_id.into(),
            chain_scope: chain_scope.into(),
            transaction_id: transaction_id.into(),
        };
        if !(subject.canonical_event_id.starts_with("event:x402:sha256:")
            || subject.canonical_event_id.starts_with("event:mpp:sha256:"))
            || subject.chain_scope.trim().is_empty()
            || subject.transaction_id.trim().is_empty()
        {
            return Err(ReducerError::InvalidFinalityEvidence("finality subject"));
        }
        Ok(subject)
    }

    pub fn canonical_event_id(&self) -> &str {
        &self.canonical_event_id
    }
    pub fn chain_scope(&self) -> &str {
        &self.chain_scope
    }
    pub fn transaction_id(&self) -> &str {
        &self.transaction_id
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvmFinalityEvidence {
    subject: FinalitySubject,
    source_id: String,
    provenance_id: String,
    evidence_id: String,
    asserted_at: i64,
    block_number: u64,
    block_hash: String,
    canonical_block_hash: String,
    latest_block: u64,
    finalized_block: u64,
    execution: ExecutionOutcome,
}

impl EvmFinalityEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subject: FinalitySubject,
        source_id: impl Into<String>,
        provenance_id: impl Into<String>,
        evidence_id: impl Into<String>,
        asserted_at: i64,
        block_number: u64,
        block_hash: impl Into<String>,
        canonical_block_hash: impl Into<String>,
        latest_block: u64,
        finalized_block: u64,
        execution: ExecutionOutcome,
    ) -> Self {
        Self {
            subject,
            source_id: source_id.into(),
            provenance_id: provenance_id.into(),
            evidence_id: evidence_id.into(),
            asserted_at,
            block_number,
            block_hash: block_hash.into(),
            canonical_block_hash: canonical_block_hash.into(),
            latest_block,
            finalized_block,
            execution,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SolanaCommitment {
    Processed,
    Confirmed,
    Finalized,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SolanaFinalityEvidence {
    subject: FinalitySubject,
    source_id: String,
    provenance_id: String,
    evidence_id: String,
    asserted_at: i64,
    slot: u64,
    block_hash: String,
    canonical_block_hash: String,
    commitment: SolanaCommitment,
    execution: ExecutionOutcome,
}

impl SolanaFinalityEvidence {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        subject: FinalitySubject,
        source_id: impl Into<String>,
        provenance_id: impl Into<String>,
        evidence_id: impl Into<String>,
        asserted_at: i64,
        slot: u64,
        block_hash: impl Into<String>,
        canonical_block_hash: impl Into<String>,
        commitment: SolanaCommitment,
        execution: ExecutionOutcome,
    ) -> Self {
        Self {
            subject,
            source_id: source_id.into(),
            provenance_id: provenance_id.into(),
            evidence_id: evidence_id.into(),
            asserted_at,
            slot,
            block_hash: block_hash.into(),
            canonical_block_hash: canonical_block_hash.into(),
            commitment,
            execution,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum FinalityBasis {
    Evm {
        block_number: u64,
        block_hash: String,
        canonical_block_hash: String,
        latest_block: u64,
        finalized_block: u64,
        execution: ExecutionOutcome,
    },
    Solana {
        slot: u64,
        block_hash: String,
        canonical_block_hash: String,
        commitment: SolanaCommitment,
        execution: ExecutionOutcome,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalityPolicy {
    Evm { confirmations: u64 },
    Solana,
}

pub trait ChainFinalityEvidence {
    fn subject(&self) -> &FinalitySubject;
    fn source_id(&self) -> &str;
    fn provenance_id(&self) -> &str;
    fn evidence_id(&self) -> &str;
    fn asserted_at(&self) -> i64;
    fn block_hash(&self) -> &str;
    fn position(&self) -> u64;
    fn basis(&self) -> FinalityBasis;
    fn validate(&self) -> Result<(), ReducerError>;
    fn derive_status(&self, policy: FinalityPolicy) -> Result<FinalityStatus, ReducerError>;
}

impl ChainFinalityEvidence for EvmFinalityEvidence {
    fn subject(&self) -> &FinalitySubject {
        &self.subject
    }
    fn source_id(&self) -> &str {
        &self.source_id
    }
    fn provenance_id(&self) -> &str {
        &self.provenance_id
    }
    fn evidence_id(&self) -> &str {
        &self.evidence_id
    }
    fn asserted_at(&self) -> i64 {
        self.asserted_at
    }
    fn block_hash(&self) -> &str {
        &self.block_hash
    }
    fn position(&self) -> u64 {
        self.block_number
    }
    fn basis(&self) -> FinalityBasis {
        FinalityBasis::Evm {
            block_number: self.block_number,
            block_hash: self.block_hash.clone(),
            canonical_block_hash: self.canonical_block_hash.clone(),
            latest_block: self.latest_block,
            finalized_block: self.finalized_block,
            execution: self.execution,
        }
    }
    fn validate(&self) -> Result<(), ReducerError> {
        validate_finality_evidence(self)
    }
    fn derive_status(&self, policy: FinalityPolicy) -> Result<FinalityStatus, ReducerError> {
        match policy {
            FinalityPolicy::Evm { confirmations } => derive_evm_status(self, confirmations),
            FinalityPolicy::Solana => Err(ReducerError::FinalityPolicyMismatch),
        }
    }
}

impl ChainFinalityEvidence for SolanaFinalityEvidence {
    fn subject(&self) -> &FinalitySubject {
        &self.subject
    }
    fn source_id(&self) -> &str {
        &self.source_id
    }
    fn provenance_id(&self) -> &str {
        &self.provenance_id
    }
    fn evidence_id(&self) -> &str {
        &self.evidence_id
    }
    fn asserted_at(&self) -> i64 {
        self.asserted_at
    }
    fn block_hash(&self) -> &str {
        &self.block_hash
    }
    fn position(&self) -> u64 {
        self.slot
    }
    fn basis(&self) -> FinalityBasis {
        FinalityBasis::Solana {
            slot: self.slot,
            block_hash: self.block_hash.clone(),
            canonical_block_hash: self.canonical_block_hash.clone(),
            commitment: self.commitment,
            execution: self.execution,
        }
    }
    fn validate(&self) -> Result<(), ReducerError> {
        if self.source_id.trim().is_empty()
            || self.provenance_id.trim().is_empty()
            || self.evidence_id.trim().is_empty()
            || self.asserted_at <= 0
            || self.slot > i64::MAX as u64
            || self.block_hash.is_empty()
            || self.canonical_block_hash.is_empty()
        {
            let field = if self.slot > i64::MAX as u64 {
                "slot"
            } else {
                "Solana fields"
            };
            return Err(ReducerError::InvalidFinalityEvidence(field));
        }
        Ok(())
    }
    fn derive_status(&self, policy: FinalityPolicy) -> Result<FinalityStatus, ReducerError> {
        if policy != FinalityPolicy::Solana {
            return Err(ReducerError::FinalityPolicyMismatch);
        }
        if self.block_hash != self.canonical_block_hash {
            return Ok(FinalityStatus::Orphaned);
        }
        if self.execution == ExecutionOutcome::Reverted {
            return Ok(FinalityStatus::Reverted);
        }
        Ok(match self.commitment {
            SolanaCommitment::Processed => FinalityStatus::Observed,
            SolanaCommitment::Confirmed => FinalityStatus::Confirmed,
            SolanaCommitment::Finalized => FinalityStatus::Finalized,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalityUpdate {
    subject: FinalitySubject,
    source_id: String,
    provenance_id: String,
    evidence_id: String,
    asserted_at: i64,
    status: FinalityStatus,
    basis: FinalityBasis,
}

impl FinalityUpdate {
    pub fn subject(&self) -> &FinalitySubject {
        &self.subject
    }
    pub fn source_id(&self) -> &str {
        &self.source_id
    }
    pub fn provenance_id(&self) -> &str {
        &self.provenance_id
    }
    pub fn evidence_id(&self) -> &str {
        &self.evidence_id
    }
    pub const fn asserted_at(&self) -> i64 {
        self.asserted_at
    }
    pub const fn status(&self) -> FinalityStatus {
        self.status
    }
    pub fn basis(&self) -> &FinalityBasis {
        &self.basis
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalityConflict {
    update: FinalityUpdate,
    current_status: FinalityStatus,
}

impl FinalityConflict {
    pub fn update(&self) -> &FinalityUpdate {
        &self.update
    }
    pub const fn current_status(&self) -> FinalityStatus {
        self.current_status
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FinalityTimeline {
    updates: Vec<FinalityUpdate>,
    conflicts: Vec<FinalityConflict>,
    encoded: Vec<u8>,
    state_hash: String,
}

impl FinalityTimeline {
    pub fn updates(&self) -> &[FinalityUpdate] {
        &self.updates
    }
    pub fn conflicts(&self) -> &[FinalityConflict] {
        &self.conflicts
    }
    pub fn current(&self) -> Option<FinalityStatus> {
        self.updates.last().map(FinalityUpdate::status)
    }
    pub fn encode(&self) -> &[u8] {
        &self.encoded
    }
    pub fn state_hash(&self) -> &str {
        &self.state_hash
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FinalityEngine {
    policy: FinalityPolicy,
}

impl FinalityEngine {
    pub const fn evm(confirmations: u64) -> Self {
        Self {
            policy: FinalityPolicy::Evm { confirmations },
        }
    }

    pub const fn solana() -> Self {
        Self {
            policy: FinalityPolicy::Solana,
        }
    }

    pub fn reconcile_for<E: ChainFinalityEvidence>(
        &self,
        expected_subject: &FinalitySubject,
        evidence: impl IntoIterator<Item = E>,
    ) -> Result<FinalityTimeline, ReducerError> {
        if matches!(self.policy, FinalityPolicy::Evm { confirmations: 0 }) {
            return Err(ReducerError::InvalidFinalityEvidence("confirmation depth"));
        }
        let mut updates = evidence
            .into_iter()
            .map(|item| {
                item.validate()?;
                if item.subject() != expected_subject {
                    return Err(ReducerError::FinalitySubjectMismatch);
                }
                Ok(FinalityUpdate {
                    subject: expected_subject.clone(),
                    source_id: item.source_id().to_owned(),
                    provenance_id: item.provenance_id().to_owned(),
                    evidence_id: item.evidence_id().to_owned(),
                    asserted_at: item.asserted_at(),
                    status: item.derive_status(self.policy)?,
                    basis: item.basis(),
                })
            })
            .collect::<Result<Vec<_>, ReducerError>>()?;
        updates.sort_by(|left, right| {
            left.asserted_at
                .cmp(&right.asserted_at)
                .then_with(|| finality_order(left.status).cmp(&finality_order(right.status)))
                .then_with(|| basis_order(&left.basis).cmp(&basis_order(&right.basis)))
                .then_with(|| left.source_id.cmp(&right.source_id))
                .then_with(|| left.provenance_id.cmp(&right.provenance_id))
                .then_with(|| left.evidence_id.cmp(&right.evidence_id))
        });
        let mut timeline = FinalityTimeline {
            updates: Vec::new(),
            conflicts: Vec::new(),
            encoded: Vec::new(),
            state_hash: String::new(),
        };
        for update in updates {
            match timeline.current() {
                None => timeline.updates.push(update),
                Some(current) if current == update.status => timeline.updates.push(update),
                Some(current) if valid_finality_transition(current, update.status) => {
                    timeline.updates.push(update)
                }
                Some(current) => timeline.conflicts.push(FinalityConflict {
                    update,
                    current_status: current,
                }),
            }
        }
        timeline.encoded = encode_finality_timeline(self.policy, &timeline);
        timeline.state_hash = hex::encode(Sha256::digest(&timeline.encoded));
        Ok(timeline)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Conflict {
    fingerprint: String,
    observation_ids: Vec<String>,
}

impl Conflict {
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn observation_ids(&self) -> &[String] {
        &self.observation_ids
    }
}

#[derive(Debug)]
pub struct ReducedEvent {
    event: CanonicalEvent,
    observations: Vec<(String, ObservationRole)>,
    conflicts: Vec<Conflict>,
}

impl ReducedEvent {
    pub fn id(&self) -> &str {
        self.event.id()
    }

    pub fn observations(&self) -> &[(String, ObservationRole)] {
        &self.observations
    }

    pub fn conflicts(&self) -> &[Conflict] {
        &self.conflicts
    }
}

#[derive(Debug)]
pub struct ReductionSnapshot {
    events: Vec<ReducedEvent>,
    encoded: Vec<u8>,
    state_hash: String,
}

impl ReductionSnapshot {
    pub fn events(&self) -> &[ReducedEvent] {
        &self.events
    }

    pub fn encode(&self) -> &[u8] {
        &self.encoded
    }

    pub fn state_hash(&self) -> &str {
        &self.state_hash
    }
}

#[derive(Clone, Debug)]
pub struct Reducer {
    version: String,
}

impl Reducer {
    pub fn new(version: impl Into<String>) -> Self {
        Self {
            version: version.into(),
        }
    }

    pub fn reduce(
        &self,
        observations: impl IntoIterator<Item = Observation>,
    ) -> Result<ReductionSnapshot, ReducerError> {
        if self.version.trim().is_empty() {
            return Err(ReducerError::EmptyVersion);
        }
        let mut by_event = BTreeMap::<String, Vec<Observation>>::new();
        for observation in observations {
            by_event
                .entry(observation.event_key().canonical_id())
                .or_default()
                .push(observation);
        }

        let mut events = Vec::with_capacity(by_event.len());
        for observations in by_event.into_values() {
            events.push(reconcile_event(observations)?);
        }
        let encoded = encode_snapshot(&self.version, &events);
        let state_hash = hex::encode(Sha256::digest(&encoded));
        Ok(ReductionSnapshot {
            events,
            encoded,
            state_hash,
        })
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ReducerError {
    #[error("reducer version must not be empty")]
    EmptyVersion,
    #[error("invalid finality evidence: {0}")]
    InvalidFinalityEvidence(&'static str),
    #[error("finality evidence does not match the chain policy")]
    FinalityPolicyMismatch,
    #[error("finality evidence does not describe the expected canonical event")]
    FinalitySubjectMismatch,
    #[error(transparent)]
    Observation(#[from] ObservationError),
}

fn validate_finality_evidence(evidence: &EvmFinalityEvidence) -> Result<(), ReducerError> {
    if evidence.source_id.trim().is_empty()
        || evidence.provenance_id.trim().is_empty()
        || evidence.evidence_id.trim().is_empty()
    {
        return Err(ReducerError::InvalidFinalityEvidence("provenance"));
    }
    if evidence.asserted_at <= 0 {
        return Err(ReducerError::InvalidFinalityEvidence("asserted at"));
    }
    if evidence.block_hash.is_empty() || evidence.canonical_block_hash.is_empty() {
        return Err(ReducerError::InvalidFinalityEvidence("block hash"));
    }
    if evidence.block_number > i64::MAX as u64
        || evidence.latest_block > i64::MAX as u64
        || evidence.finalized_block > i64::MAX as u64
        || evidence.latest_block < evidence.block_number
        || evidence.finalized_block > evidence.latest_block
    {
        return Err(ReducerError::InvalidFinalityEvidence("block range"));
    }
    Ok(())
}

fn derive_evm_status(
    evidence: &EvmFinalityEvidence,
    confirmations: u64,
) -> Result<FinalityStatus, ReducerError> {
    if evidence.block_hash != evidence.canonical_block_hash {
        return Ok(FinalityStatus::Orphaned);
    }
    if evidence.execution == ExecutionOutcome::Reverted {
        return Ok(FinalityStatus::Reverted);
    }
    if evidence.finalized_block >= evidence.block_number {
        return Ok(FinalityStatus::Finalized);
    }
    let confirmed_at = evidence
        .block_number
        .checked_add(confirmations)
        .ok_or(ReducerError::InvalidFinalityEvidence("confirmation depth"))?;
    if evidence.latest_block >= confirmed_at {
        Ok(FinalityStatus::Confirmed)
    } else {
        Ok(FinalityStatus::Observed)
    }
}

const fn finality_order(status: FinalityStatus) -> u8 {
    match status {
        FinalityStatus::Observed => 0,
        FinalityStatus::Confirmed => 1,
        FinalityStatus::Finalized => 2,
        FinalityStatus::Orphaned => 3,
        FinalityStatus::Reverted => 4,
    }
}

fn basis_order(basis: &FinalityBasis) -> Vec<u8> {
    let mut encoded = Vec::new();
    match basis {
        FinalityBasis::Evm {
            block_number,
            block_hash,
            canonical_block_hash,
            latest_block,
            finalized_block,
            execution,
        } => {
            encoded.push(0);
            encoded.extend_from_slice(&block_number.to_be_bytes());
            append_field(&mut encoded, block_hash.as_bytes());
            append_field(&mut encoded, canonical_block_hash.as_bytes());
            encoded.extend_from_slice(&latest_block.to_be_bytes());
            encoded.extend_from_slice(&finalized_block.to_be_bytes());
            encoded.push(execution_order(*execution));
        }
        FinalityBasis::Solana {
            slot,
            block_hash,
            canonical_block_hash,
            commitment,
            execution,
        } => {
            encoded.push(1);
            encoded.extend_from_slice(&slot.to_be_bytes());
            append_field(&mut encoded, block_hash.as_bytes());
            append_field(&mut encoded, canonical_block_hash.as_bytes());
            encoded.push(commitment_order(*commitment));
            encoded.push(execution_order(*execution));
        }
    }
    encoded
}

fn encode_finality_timeline(policy: FinalityPolicy, timeline: &FinalityTimeline) -> Vec<u8> {
    let mut encoded = Vec::new();
    append_field(&mut encoded, b"agent-economy-finality-v1");
    match policy {
        FinalityPolicy::Evm { confirmations } => {
            encoded.push(0);
            encoded.extend_from_slice(&confirmations.to_be_bytes());
        }
        FinalityPolicy::Solana => encoded.push(1),
    }
    for update in &timeline.updates {
        encoded.push(1);
        encode_finality_update(&mut encoded, update);
    }
    for conflict in &timeline.conflicts {
        encoded.push(0);
        encode_finality_update(&mut encoded, &conflict.update);
        encoded.push(finality_order(conflict.current_status));
    }
    encoded
}

fn encode_finality_update(encoded: &mut Vec<u8>, update: &FinalityUpdate) {
    append_field(encoded, update.subject.canonical_event_id.as_bytes());
    append_field(encoded, update.subject.chain_scope.as_bytes());
    append_field(encoded, update.subject.transaction_id.as_bytes());
    append_field(encoded, update.source_id.as_bytes());
    append_field(encoded, update.provenance_id.as_bytes());
    append_field(encoded, update.evidence_id.as_bytes());
    encoded.extend_from_slice(&update.asserted_at.to_be_bytes());
    encoded.push(finality_order(update.status));
    append_field(encoded, &basis_order(&update.basis));
}

const fn execution_order(outcome: ExecutionOutcome) -> u8 {
    match outcome {
        ExecutionOutcome::Succeeded => 0,
        ExecutionOutcome::Reverted => 1,
    }
}

const fn commitment_order(commitment: SolanaCommitment) -> u8 {
    match commitment {
        SolanaCommitment::Processed => 0,
        SolanaCommitment::Confirmed => 1,
        SolanaCommitment::Finalized => 2,
    }
}

const fn valid_finality_transition(from: FinalityStatus, to: FinalityStatus) -> bool {
    match from {
        FinalityStatus::Observed => true,
        FinalityStatus::Confirmed => matches!(
            to,
            FinalityStatus::Finalized | FinalityStatus::Orphaned | FinalityStatus::Reverted
        ),
        FinalityStatus::Finalized => {
            matches!(to, FinalityStatus::Orphaned | FinalityStatus::Reverted)
        }
        FinalityStatus::Orphaned => matches!(
            to,
            FinalityStatus::Observed
                | FinalityStatus::Confirmed
                | FinalityStatus::Finalized
                | FinalityStatus::Reverted
        ),
        FinalityStatus::Reverted => false,
    }
}

fn reconcile_event(mut observations: Vec<Observation>) -> Result<ReducedEvent, ObservationError> {
    observations.sort_by(|left, right| left.id().cmp(right.id()));
    let mut cohorts = BTreeMap::<String, Vec<usize>>::new();
    for (index, observation) in observations.iter().enumerate() {
        cohorts
            .entry(payload_fingerprint(observation))
            .or_default()
            .push(index);
    }
    let winning_fingerprint = cohorts
        .iter()
        .max_by(|(left_fingerprint, left), (right_fingerprint, right)| {
            left.len()
                .cmp(&right.len())
                .then_with(|| right_fingerprint.cmp(left_fingerprint))
        })
        .map(|(fingerprint, _)| fingerprint.clone())
        .expect("an event group contains an observation");

    let supporting = cohorts[&winning_fingerprint]
        .iter()
        .map(|index| observations[*index].clone())
        .collect::<Vec<_>>();
    let event = CanonicalEvent::new(observations[0].event_key(), &supporting)?;
    let mut roles = observations
        .iter()
        .map(|observation| {
            let role = if payload_fingerprint(observation) == winning_fingerprint {
                ObservationRole::Supporting
            } else {
                ObservationRole::Conflicting
            };
            (observation.id().to_owned(), role)
        })
        .collect::<Vec<_>>();
    roles.sort_by(|(left_id, left_role), (right_id, right_role)| {
        role_order(*left_role)
            .cmp(&role_order(*right_role))
            .then_with(|| left_id.cmp(right_id))
    });
    let conflicts = cohorts
        .into_iter()
        .filter(|(fingerprint, _)| fingerprint != &winning_fingerprint)
        .map(|(fingerprint, indexes)| Conflict {
            fingerprint,
            observation_ids: indexes
                .into_iter()
                .map(|index| observations[index].id().to_owned())
                .collect(),
        })
        .collect();

    Ok(ReducedEvent {
        event,
        observations: roles,
        conflicts,
    })
}

const fn role_order(role: ObservationRole) -> u8 {
    match role {
        ObservationRole::Supporting => 0,
        ObservationRole::Conflicting => 1,
    }
}

fn payload_fingerprint(observation: &Observation) -> String {
    let mut content = Vec::new();
    match observation.protocol() {
        ProtocolObservation::X402(value) => {
            append_field(&mut content, b"x402");
            append_field(&mut content, value.asset().as_bytes());
            append_field(&mut content, value.amount_atomic().as_bytes());
            append_field(&mut content, &value.protocol_version().to_be_bytes());
            append_field(&mut content, value.scheme().as_bytes());
            append_field(&mut content, value.network().as_bytes());
            append_field(&mut content, value.pay_to().as_bytes());
            append_field(&mut content, value.resource().as_bytes());
        }
        ProtocolObservation::Mpp(value) => {
            append_field(&mut content, b"mpp");
            append_field(&mut content, value.intent().as_bytes());
            append_field(&mut content, value.amount_atomic().as_bytes());
        }
        ProtocolObservation::MppDiscovery(value) => {
            append_field(&mut content, b"mpp-discovery");
            append_field(&mut content, value.openapi_version().as_bytes());
            append_field(&mut content, value.service_title().as_bytes());
            append_field(&mut content, value.api_version().as_bytes());
            append_field(&mut content, value.intent().as_bytes());
            append_field(&mut content, value.method().as_bytes());
            append_optional(&mut content, value.amount_atomic().map(str::as_bytes));
            append_optional(&mut content, value.currency().map(str::as_bytes));
            append_optional(&mut content, value.description().map(str::as_bytes));
            append_field(&mut content, value.raw_payment_info_json());
        }
    }
    hex::encode(Sha256::digest(content))
}

fn encode_snapshot(version: &str, events: &[ReducedEvent]) -> Vec<u8> {
    let mut encoded = Vec::new();
    append_field(&mut encoded, b"agent-economy-reduction-v1");
    append_field(&mut encoded, version.as_bytes());
    for event in events {
        append_field(&mut encoded, event.event.encode());
        for (observation_id, role) in &event.observations {
            append_field(&mut encoded, observation_id.as_bytes());
            encoded.push(role_order(*role));
        }
        for conflict in &event.conflicts {
            append_field(&mut encoded, conflict.fingerprint.as_bytes());
            for observation_id in &conflict.observation_ids {
                append_field(&mut encoded, observation_id.as_bytes());
            }
        }
    }
    encoded
}

fn append_optional(target: &mut Vec<u8>, value: Option<&[u8]>) {
    match value {
        Some(value) => {
            target.push(1);
            append_field(target, value);
        }
        None => target.push(0),
    }
}

fn append_field(target: &mut Vec<u8>, value: &[u8]) {
    target.extend_from_slice(&(value.len() as u64).to_be_bytes());
    target.extend_from_slice(value);
}
