use sha2::{Digest, Sha256};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Protocol {
    X402,
    Mpp,
}

impl Protocol {
    const fn name(self) -> &'static str {
        match self {
            Self::X402 => "x402",
            Self::Mpp => "mpp",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum AutonomySignal {
    VerifiedAgent,
    VerifiedHuman,
    Unknown,
}

impl AutonomySignal {
    const fn name(self) -> &'static str {
        match self {
            Self::VerifiedAgent => "verified_agent",
            Self::VerifiedHuman => "verified_human",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Activity {
    amount_atomic: u128,
    occurred_at: i64,
    protocol: Protocol,
    counterparty: String,
    autonomy: AutonomySignal,
    evidence_id: String,
}

impl Activity {
    pub fn new(
        amount_atomic: u128,
        occurred_at: i64,
        protocol: Protocol,
        counterparty: impl Into<String>,
        autonomy: AutonomySignal,
        evidence_id: impl Into<String>,
    ) -> Result<Self, ClassificationError> {
        let activity = Self {
            amount_atomic,
            occurred_at,
            protocol,
            counterparty: counterparty.into(),
            autonomy,
            evidence_id: evidence_id.into(),
        };
        if activity.counterparty.trim().is_empty() || activity.evidence_id.trim().is_empty() {
            return Err(ClassificationError::InvalidField);
        }
        Ok(activity)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureSnapshot {
    subject_id: String,
    window_start: i64,
    window_end: i64,
    feature_version: String,
    total_spend_atomic: u128,
    payment_count: u64,
    median_cadence_seconds: Option<u64>,
    x402_count: u64,
    mpp_count: u64,
    unique_counterparties: u64,
    autonomous_count: u64,
    autonomy_observed_count: u64,
    evidence_ids: Vec<String>,
    input_snapshot_hash: String,
}

impl FeatureSnapshot {
    pub const fn total_spend_atomic(&self) -> u128 {
        self.total_spend_atomic
    }

    pub const fn payment_count(&self) -> u64 {
        self.payment_count
    }

    pub const fn median_cadence_seconds(&self) -> Option<u64> {
        self.median_cadence_seconds
    }

    pub const fn protocol_count(&self, protocol: Protocol) -> u64 {
        match protocol {
            Protocol::X402 => self.x402_count,
            Protocol::Mpp => self.mpp_count,
        }
    }

    pub const fn unique_counterparties(&self) -> u64 {
        self.unique_counterparties
    }

    pub const fn autonomous_count(&self) -> u64 {
        self.autonomous_count
    }

    pub const fn autonomy_observed_count(&self) -> u64 {
        self.autonomy_observed_count
    }

    pub fn feature_version(&self) -> &str {
        &self.feature_version
    }

    pub const fn window(&self) -> (i64, i64) {
        (self.window_start, self.window_end)
    }

    pub fn evidence_ids(&self) -> &[String] {
        &self.evidence_ids
    }

    pub fn input_snapshot_hash(&self) -> &str {
        &self.input_snapshot_hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FeatureEngine {
    version: String,
}

impl FeatureEngine {
    pub fn new(version: impl Into<String>) -> Result<Self, ClassificationError> {
        let version = version.into();
        if version.trim().is_empty() {
            return Err(ClassificationError::InvalidField);
        }
        Ok(Self { version })
    }

    pub fn compute<I>(
        &self,
        subject_id: impl Into<String>,
        window_start: i64,
        window_end: i64,
        activities: I,
    ) -> Result<FeatureSnapshot, ClassificationError>
    where
        I: IntoIterator<Item = Activity>,
    {
        let subject_id = subject_id.into();
        if subject_id.trim().is_empty() || window_end <= window_start {
            return Err(ClassificationError::InvalidWindow);
        }
        let mut activities = activities.into_iter().collect::<Vec<_>>();
        if activities.iter().any(|activity| {
            activity.occurred_at < window_start || activity.occurred_at >= window_end
        }) {
            return Err(ClassificationError::ActivityOutsideWindow);
        }
        activities.sort();
        let total_spend_atomic = activities.iter().try_fold(0_u128, |total, activity| {
            total
                .checked_add(activity.amount_atomic)
                .ok_or(ClassificationError::SpendOverflow)
        })?;
        let payment_count = activities.len() as u64;
        let mut times = activities
            .iter()
            .map(|activity| activity.occurred_at)
            .collect::<Vec<_>>();
        times.sort_unstable();
        let mut intervals = times
            .windows(2)
            .map(|pair| ((pair[1] as i128) - (pair[0] as i128)) as u64)
            .collect::<Vec<_>>();
        intervals.sort_unstable();
        let median_cadence_seconds = median(&intervals);
        let x402_count = activities
            .iter()
            .filter(|activity| activity.protocol == Protocol::X402)
            .count() as u64;
        let mpp_count = payment_count - x402_count;
        let unique_counterparties = activities
            .iter()
            .map(|activity| activity.counterparty.as_str())
            .collect::<BTreeSet<_>>()
            .len() as u64;
        let autonomous_count = activities
            .iter()
            .filter(|activity| activity.autonomy == AutonomySignal::VerifiedAgent)
            .count() as u64;
        let autonomy_observed_count = activities
            .iter()
            .filter(|activity| activity.autonomy != AutonomySignal::Unknown)
            .count() as u64;
        let evidence_ids = activities
            .iter()
            .map(|activity| activity.evidence_id.clone())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        let input_snapshot_hash = input_snapshot_hash(
            &subject_id,
            window_start,
            window_end,
            &self.version,
            &activities,
        );
        Ok(FeatureSnapshot {
            subject_id,
            window_start,
            window_end,
            feature_version: self.version.clone(),
            total_spend_atomic,
            payment_count,
            median_cadence_seconds,
            x402_count,
            mpp_count,
            unique_counterparties,
            autonomous_count,
            autonomy_observed_count,
            evidence_ids,
            input_snapshot_hash,
        })
    }
}

fn median(values: &[u64]) -> Option<u64> {
    match values.len() {
        0 => None,
        len if len % 2 == 1 => Some(values[len / 2]),
        len => {
            let lower = values[len / 2 - 1];
            let upper = values[len / 2];
            Some(lower + (upper - lower) / 2)
        }
    }
}

fn input_snapshot_hash(
    subject_id: &str,
    window_start: i64,
    window_end: i64,
    feature_version: &str,
    activities: &[Activity],
) -> String {
    let mut encoded = Vec::new();
    for field in [
        "aem-behavior-input-v1",
        subject_id,
        &window_start.to_string(),
        &window_end.to_string(),
        feature_version,
    ] {
        encode_field(&mut encoded, field);
    }
    encode_count(&mut encoded, activities.len());
    for activity in activities {
        for field in [
            activity.amount_atomic.to_string(),
            activity.occurred_at.to_string(),
            activity.protocol.name().to_owned(),
            activity.counterparty.clone(),
            activity.autonomy.name().to_owned(),
            activity.evidence_id.clone(),
        ] {
            encode_field(&mut encoded, &field);
        }
    }
    hex::encode(Sha256::digest(encoded))
}

fn encode_field(encoded: &mut Vec<u8>, field: &str) {
    encoded.extend_from_slice(&(field.len() as u64).to_be_bytes());
    encoded.extend_from_slice(field.as_bytes());
}

fn encode_count(encoded: &mut Vec<u8>, count: usize) {
    encoded.extend_from_slice(&(count as u64).to_be_bytes());
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum LabelKind {
    Core,
    Extension,
}

impl LabelKind {
    const fn name(self) -> &'static str {
        match self {
            Self::Core => "core",
            Self::Extension => "extension",
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum FeatureMetric {
    TotalSpendAtomic,
    PaymentCount,
    MedianCadenceSeconds,
    X402Count,
    MppCount,
    UniqueCounterparties,
    AutonomousCount,
    AutonomyObservedCount,
}

impl FeatureMetric {
    const fn name(self) -> &'static str {
        match self {
            Self::TotalSpendAtomic => "total_spend_atomic",
            Self::PaymentCount => "payment_count",
            Self::MedianCadenceSeconds => "median_cadence_seconds",
            Self::X402Count => "x402_count",
            Self::MppCount => "mpp_count",
            Self::UniqueCounterparties => "unique_counterparties",
            Self::AutonomousCount => "autonomous_count",
            Self::AutonomyObservedCount => "autonomy_observed_count",
        }
    }

    const fn value(self, snapshot: &FeatureSnapshot) -> u128 {
        match self {
            Self::TotalSpendAtomic => snapshot.total_spend_atomic,
            Self::PaymentCount => snapshot.payment_count as u128,
            Self::MedianCadenceSeconds => match snapshot.median_cadence_seconds {
                Some(value) => value as u128,
                None => 0,
            },
            Self::X402Count => snapshot.x402_count as u128,
            Self::MppCount => snapshot.mpp_count as u128,
            Self::UniqueCounterparties => snapshot.unique_counterparties as u128,
            Self::AutonomousCount => snapshot.autonomous_count as u128,
            Self::AutonomyObservedCount => snapshot.autonomy_observed_count as u128,
        }
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Rule {
    metric: FeatureMetric,
    threshold: u128,
}

impl Rule {
    pub const fn at_least(metric: FeatureMetric, threshold: u128) -> Self {
        Self { metric, threshold }
    }

    fn matches(&self, snapshot: &FeatureSnapshot) -> bool {
        self.metric.value(snapshot) >= self.threshold
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LabelDefinition {
    id: String,
    version: u32,
    kind: LabelKind,
    rule: Rule,
}

impl LabelDefinition {
    pub fn new(
        id: impl Into<String>,
        version: u32,
        kind: LabelKind,
        rule: Rule,
    ) -> Result<Self, ClassificationError> {
        let id = id.into();
        if id.trim().is_empty() || version == 0 {
            return Err(ClassificationError::InvalidLabelDefinition);
        }
        Ok(Self {
            id,
            version,
            kind,
            rule,
        })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ClaimStatus {
    Verified,
    Inferred,
    Disputed,
}

impl ClaimStatus {
    const fn name(self) -> &'static str {
        match self {
            Self::Verified => "verified",
            Self::Inferred => "inferred",
            Self::Disputed => "disputed",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassificationClaim {
    label_id: String,
    label_version: u32,
    label_kind: LabelKind,
    status: ClaimStatus,
}

impl ClassificationClaim {
    pub fn label_id(&self) -> &str {
        &self.label_id
    }

    pub const fn label_version(&self) -> u32 {
        self.label_version
    }

    pub const fn label_kind(&self) -> LabelKind {
        self.label_kind
    }

    pub const fn status(&self) -> ClaimStatus {
        self.status
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassificationResult {
    subject_id: String,
    classifier_version: String,
    feature_version: String,
    window_start: i64,
    window_end: i64,
    input_snapshot_hash: String,
    label_set_hash: String,
    claims: Vec<ClassificationClaim>,
    encoded: Vec<u8>,
    state_hash: String,
}

impl ClassificationResult {
    pub fn classifier_version(&self) -> &str {
        &self.classifier_version
    }

    pub fn feature_version(&self) -> &str {
        &self.feature_version
    }

    pub const fn window(&self) -> (i64, i64) {
        (self.window_start, self.window_end)
    }

    pub fn input_snapshot_hash(&self) -> &str {
        &self.input_snapshot_hash
    }

    pub fn label_set_hash(&self) -> &str {
        &self.label_set_hash
    }

    pub fn claims(&self) -> &[ClassificationClaim] {
        &self.claims
    }

    pub fn encode(&self) -> &[u8] {
        &self.encoded
    }

    pub fn state_hash(&self) -> &str {
        &self.state_hash
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct BehavioralClassifier {
    version: String,
    labels: Vec<LabelDefinition>,
    label_set_hash: String,
}

impl BehavioralClassifier {
    pub fn new<I>(version: impl Into<String>, labels: I) -> Result<Self, ClassificationError>
    where
        I: IntoIterator<Item = LabelDefinition>,
    {
        let version = version.into();
        if version.trim().is_empty() {
            return Err(ClassificationError::InvalidField);
        }
        let mut labels = labels.into_iter().collect::<Vec<_>>();
        if labels.is_empty() {
            return Err(ClassificationError::EmptyLabelSet);
        }
        labels.sort_by(|left, right| (&left.id, left.version).cmp(&(&right.id, right.version)));
        if labels
            .windows(2)
            .any(|pair| pair[0].id == pair[1].id && pair[0].version == pair[1].version)
        {
            return Err(ClassificationError::DuplicateLabelDefinition);
        }
        let label_set_hash = label_set_hash(&labels);
        Ok(Self {
            version,
            labels,
            label_set_hash,
        })
    }

    pub fn classify(&self, snapshot: &FeatureSnapshot) -> ClassificationResult {
        let claims = self
            .labels
            .iter()
            .filter(|label| label.rule.matches(snapshot))
            .map(|label| ClassificationClaim {
                label_id: label.id.clone(),
                label_version: label.version,
                label_kind: label.kind,
                status: ClaimStatus::Inferred,
            })
            .collect::<Vec<_>>();
        let mut encoded = Vec::new();
        for field in [
            "aem-classification-result-v1",
            self.version.as_str(),
            snapshot.subject_id.as_str(),
            snapshot.feature_version.as_str(),
            snapshot.input_snapshot_hash.as_str(),
            self.label_set_hash.as_str(),
            &snapshot.window_start.to_string(),
            &snapshot.window_end.to_string(),
        ] {
            encode_field(&mut encoded, field);
        }
        encode_count(&mut encoded, claims.len());
        for claim in &claims {
            for field in [
                claim.label_id.as_str(),
                &claim.label_version.to_string(),
                claim.label_kind.name(),
                claim.status.name(),
            ] {
                encode_field(&mut encoded, field);
            }
        }
        let state_hash = hex::encode(Sha256::digest(&encoded));
        ClassificationResult {
            subject_id: snapshot.subject_id.clone(),
            classifier_version: self.version.clone(),
            feature_version: snapshot.feature_version.clone(),
            window_start: snapshot.window_start,
            window_end: snapshot.window_end,
            input_snapshot_hash: snapshot.input_snapshot_hash.clone(),
            label_set_hash: self.label_set_hash.clone(),
            claims,
            encoded,
            state_hash,
        }
    }
}

fn label_set_hash(labels: &[LabelDefinition]) -> String {
    let mut encoded = Vec::new();
    encode_field(&mut encoded, "aem-label-set-v1");
    encode_count(&mut encoded, labels.len());
    for label in labels {
        for field in [
            label.id.as_str(),
            &label.version.to_string(),
            label.kind.name(),
            label.rule.metric.name(),
            &label.rule.threshold.to_string(),
        ] {
            encode_field(&mut encoded, field);
        }
    }
    hex::encode(Sha256::digest(encoded))
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClassificationDrift {
    input_changed: bool,
    added_labels: Vec<String>,
    removed_labels: Vec<String>,
    baseline_state_hash: String,
    replay_state_hash: String,
}

impl ClassificationDrift {
    pub const fn input_changed(&self) -> bool {
        self.input_changed
    }

    pub fn added_labels(&self) -> &[String] {
        &self.added_labels
    }

    pub fn removed_labels(&self) -> &[String] {
        &self.removed_labels
    }

    pub fn label_churn_count(&self) -> usize {
        self.added_labels.len() + self.removed_labels.len()
    }

    pub fn baseline_state_hash(&self) -> &str {
        &self.baseline_state_hash
    }

    pub fn replay_state_hash(&self) -> &str {
        &self.replay_state_hash
    }
}

pub fn measure_drift(
    baseline: &ClassificationResult,
    replay: &ClassificationResult,
) -> Result<ClassificationDrift, ClassificationError> {
    if baseline.subject_id != replay.subject_id
        || baseline.window_start != replay.window_start
        || baseline.window_end != replay.window_end
    {
        return Err(ClassificationError::IncomparableReplay);
    }
    let baseline_labels = baseline
        .claims
        .iter()
        .map(claim_key)
        .collect::<BTreeSet<_>>();
    let replay_labels = replay.claims.iter().map(claim_key).collect::<BTreeSet<_>>();
    Ok(ClassificationDrift {
        input_changed: baseline.input_snapshot_hash != replay.input_snapshot_hash,
        added_labels: replay_labels
            .difference(&baseline_labels)
            .cloned()
            .collect(),
        removed_labels: baseline_labels
            .difference(&replay_labels)
            .cloned()
            .collect(),
        baseline_state_hash: baseline.state_hash.clone(),
        replay_state_hash: replay.state_hash.clone(),
    })
}

fn claim_key(claim: &ClassificationClaim) -> String {
    format!("{}@{}", claim.label_id, claim.label_version)
}

#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum ClassificationError {
    #[error("classification fields must not be empty")]
    InvalidField,
    #[error("feature windows must have a non-empty subject and increasing bounds")]
    InvalidWindow,
    #[error("activity falls outside the requested feature window")]
    ActivityOutsideWindow,
    #[error("spend total exceeds the supported atomic amount range")]
    SpendOverflow,
    #[error("label ids must be non-empty and label versions must be positive")]
    InvalidLabelDefinition,
    #[error("classification requires at least one versioned label definition")]
    EmptyLabelSet,
    #[error("label id and version pairs must be unique")]
    DuplicateLabelDefinition,
    #[error("classification drift requires the same subject and feature window")]
    IncomparableReplay,
}
