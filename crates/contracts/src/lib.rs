use prost::Message;
use sha2::{Digest, Sha256};
use thiserror::Error;

#[allow(dead_code)]
mod wire {
    include!(concat!(env!("OUT_DIR"), "/agent_economy.observation.v1.rs"));
}

#[cfg(test)]
mod wire_v2 {
    include!(concat!(env!("OUT_DIR"), "/agent_economy.observation.v2.rs"));
}

const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ObservationError {
    #[error("{0} must not be empty")]
    Empty(&'static str),
    #[error("unsupported observation schema version {0}")]
    UnsupportedVersion(u32),
    #[error("observation content hash does not match its payload")]
    ContentHashMismatch,
    #[error("invalid protobuf observation: {0}")]
    Decode(String),
    #[error("invalid observation field: {0}")]
    Invalid(&'static str),
    #[error("canonical event support does not match its protocol event key")]
    MismatchedSupport,
    #[error("canonical event id does not match its protocol event key")]
    CanonicalEventIdMismatch,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceRef(wire::EvidenceRef);

impl EvidenceRef {
    pub fn sha256(content: &[u8], media_type: impl Into<String>) -> Result<Self, ObservationError> {
        let media_type = required(media_type.into(), "evidence media type")?;
        Ok(Self(wire::EvidenceRef {
            algorithm: "sha256".into(),
            digest: hex::encode(Sha256::digest(content)),
            media_type,
            byte_length: content.len() as u64,
        }))
    }

    pub fn algorithm(&self) -> &str {
        &self.0.algorithm
    }

    pub fn digest(&self) -> &str {
        &self.0.digest
    }

    pub fn media_type(&self) -> &str {
        &self.0.media_type
    }

    pub fn byte_length(&self) -> u64 {
        self.0.byte_length
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Provenance(wire::Provenance);

impl Provenance {
    pub fn new(
        source_id: impl Into<String>,
        observed_at_unix_ms: i64,
        parser_version: impl Into<String>,
    ) -> Result<Self, ObservationError> {
        if observed_at_unix_ms <= 0 {
            return Err(ObservationError::Invalid("observation timestamp"));
        }
        Ok(Self(wire::Provenance {
            source_id: required(source_id.into(), "provenance source id")?,
            observed_at_unix_ms,
            parser_version: required(parser_version.into(), "parser version")?,
        }))
    }

    pub fn source_id(&self) -> &str {
        &self.0.source_id
    }

    pub fn observed_at_unix_ms(&self) -> i64 {
        self.0.observed_at_unix_ms
    }

    pub fn parser_version(&self) -> &str {
        &self.0.parser_version
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct X402EventKey(wire::X402EventKey);

impl X402EventKey {
    pub fn payment_identifier(
        value: impl Into<String>,
        request_fingerprint: impl Into<String>,
        scope: impl Into<String>,
    ) -> Result<Self, ObservationError> {
        let payment_identifier = value.into();
        if !(16..=128).contains(&payment_identifier.len())
            || !payment_identifier
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
        {
            return Err(ObservationError::Invalid("x402 payment identifier"));
        }
        let request_fingerprint = request_fingerprint.into();
        if !valid_content_id(&request_fingerprint) {
            return Err(ObservationError::Invalid("x402 request fingerprint"));
        }
        Ok(Self(wire::X402EventKey {
            payment_identifier,
            request_fingerprint,
            scope: required(scope.into(), "x402 scope")?,
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MppEventKey(wire::MppEventKey);

impl MppEventKey {
    pub fn new(
        realm: impl Into<String>,
        method: impl Into<String>,
        challenge_id: impl Into<String>,
    ) -> Result<Self, ObservationError> {
        let method = method.into();
        if !valid_mpp_method(&method) {
            return Err(ObservationError::Invalid("mpp method"));
        }
        Ok(Self(wire::MppEventKey {
            realm: required(realm.into(), "mpp realm")?,
            method,
            challenge_id: required(challenge_id.into(), "mpp challenge id")?,
        }))
    }

    pub fn tempo(
        realm: impl Into<String>,
        challenge_id: impl Into<String>,
    ) -> Result<Self, ObservationError> {
        Self::new(realm, "tempo", challenge_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MppDiscoveryKey(wire::MppDiscoveryKey);

impl MppDiscoveryKey {
    pub fn new(
        service_id: impl Into<String>,
        http_method: impl Into<String>,
        path_template: impl Into<String>,
        offer_index: u32,
    ) -> Result<Self, ObservationError> {
        let http_method = http_method.into();
        if !valid_http_method(&http_method) {
            return Err(ObservationError::Invalid("MPP discovery HTTP method"));
        }
        Ok(Self(wire::MppDiscoveryKey {
            service_id: required(service_id.into(), "MPP discovery service id")?,
            http_method,
            path_template: required(path_template.into(), "MPP discovery path template")?,
            offer_index,
        }))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolEventKey {
    X402(X402EventKey),
    Mpp(MppEventKey),
    MppDiscovery(MppDiscoveryKey),
}

impl ProtocolEventKey {
    pub fn canonical_id(&self) -> String {
        let (protocol, key) = self.to_wire();
        let encoded = wire::CanonicalEventKey {
            protocol: Some(key),
        }
        .encode_to_vec();
        format!(
            "event:{protocol}:sha256:{}",
            hex::encode(Sha256::digest(encoded))
        )
    }

    fn to_wire(&self) -> (&'static str, wire::canonical_event_key::Protocol) {
        match self {
            Self::X402(value) => (
                "x402",
                wire::canonical_event_key::Protocol::X402(value.0.clone()),
            ),
            Self::Mpp(value) => (
                "mpp",
                wire::canonical_event_key::Protocol::Mpp(value.0.clone()),
            ),
            Self::MppDiscovery(value) => (
                "mpp",
                wire::canonical_event_key::Protocol::MppDiscovery(value.0.clone()),
            ),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct X402Observation(wire::X402Observation);

impl X402Observation {
    pub fn new(
        event_key: X402EventKey,
        asset: impl Into<String>,
        amount_atomic: impl Into<String>,
    ) -> Result<Self, ObservationError> {
        let amount_atomic = amount_atomic.into();
        validate_amount(&amount_atomic, "x402 atomic amount")?;
        Ok(Self(wire::X402Observation {
            event_key: Some(event_key.0),
            asset: required(asset.into(), "x402 asset")?,
            amount_atomic,
        }))
    }

    pub fn asset(&self) -> &str {
        &self.0.asset
    }

    pub fn amount_atomic(&self) -> &str {
        &self.0.amount_atomic
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MppObservation(wire::MppObservation);

impl MppObservation {
    pub fn new(
        event_key: MppEventKey,
        intent: impl Into<String>,
        amount_atomic: impl Into<String>,
    ) -> Result<Self, ObservationError> {
        let amount_atomic = amount_atomic.into();
        validate_amount(&amount_atomic, "mpp atomic amount")?;
        Ok(Self(wire::MppObservation {
            event_key: Some(event_key.0),
            intent: required(intent.into(), "mpp intent")?,
            amount_atomic,
        }))
    }

    pub fn intent(&self) -> &str {
        &self.0.intent
    }

    pub fn amount_atomic(&self) -> &str {
        &self.0.amount_atomic
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MppDiscoveryObservation(wire::MppDiscoveryObservation);

impl MppDiscoveryObservation {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        event_key: MppDiscoveryKey,
        openapi_version: impl Into<String>,
        service_title: impl Into<String>,
        api_version: impl Into<String>,
        intent: impl Into<String>,
        method: impl Into<String>,
        amount_atomic: Option<String>,
        currency: Option<String>,
        description: Option<String>,
        raw_payment_info_json: Vec<u8>,
    ) -> Result<Self, ObservationError> {
        let method = method.into();
        if !valid_mpp_method(&method) {
            return Err(ObservationError::Invalid("mpp method"));
        }
        if let Some(amount) = amount_atomic.as_deref() {
            validate_canonical_amount(amount, "mpp discovery atomic amount")?;
        }
        if raw_payment_info_json.is_empty() {
            return Err(ObservationError::Empty("MPP raw payment metadata"));
        }
        Ok(Self(wire::MppDiscoveryObservation {
            event_key: Some(event_key.0),
            openapi_version: required(openapi_version.into(), "OpenAPI version")?,
            service_title: required(service_title.into(), "MPP service title")?,
            api_version: required(api_version.into(), "MPP API version")?,
            intent: required(intent.into(), "mpp intent")?,
            method,
            amount_atomic,
            currency,
            description,
            raw_payment_info_json,
        }))
    }

    pub fn event_key(&self) -> ProtocolEventKey {
        ProtocolEventKey::MppDiscovery(MppDiscoveryKey(
            self.0.event_key.clone().expect("validated discovery key"),
        ))
    }

    pub fn openapi_version(&self) -> &str {
        &self.0.openapi_version
    }

    pub fn service_title(&self) -> &str {
        &self.0.service_title
    }

    pub fn api_version(&self) -> &str {
        &self.0.api_version
    }

    pub fn intent(&self) -> &str {
        &self.0.intent
    }

    pub fn method(&self) -> &str {
        &self.0.method
    }

    pub fn amount_atomic(&self) -> Option<&str> {
        self.0.amount_atomic.as_deref()
    }

    pub fn currency(&self) -> Option<&str> {
        self.0.currency.as_deref()
    }

    pub fn description(&self) -> Option<&str> {
        self.0.description.as_deref()
    }

    pub fn raw_payment_info_json(&self) -> &[u8] {
        &self.0.raw_payment_info_json
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProtocolObservation {
    X402(X402Observation),
    Mpp(MppObservation),
    MppDiscovery(MppDiscoveryObservation),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    envelope: wire::ObservationEnvelope,
    encoded: Vec<u8>,
}

impl Observation {
    pub fn new(
        provenance: Provenance,
        evidence: EvidenceRef,
        protocol: ProtocolObservation,
    ) -> Result<Self, ObservationError> {
        let protocol = match protocol {
            ProtocolObservation::X402(value) => wire::observation_envelope::Protocol::X402(value.0),
            ProtocolObservation::Mpp(value) => wire::observation_envelope::Protocol::Mpp(value.0),
            ProtocolObservation::MppDiscovery(value) => {
                wire::observation_envelope::Protocol::MppDiscovery(value.0)
            }
        };
        let mut envelope = wire::ObservationEnvelope {
            schema_version: SCHEMA_VERSION,
            observation_id: String::new(),
            provenance: Some(provenance.0),
            evidence: Some(evidence.0),
            protocol: Some(protocol),
        };
        envelope.observation_id = content_id(&envelope);
        let encoded = envelope.encode_to_vec();
        Ok(Self { envelope, encoded })
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, ObservationError> {
        let envelope = wire::ObservationEnvelope::decode(bytes)
            .map_err(|error| ObservationError::Decode(error.to_string()))?;
        if envelope.schema_version != SCHEMA_VERSION {
            return Err(ObservationError::UnsupportedVersion(
                envelope.schema_version,
            ));
        }
        validate_envelope(&envelope)?;
        if envelope.observation_id != content_id_from_wire(bytes)? {
            return Err(ObservationError::ContentHashMismatch);
        }
        let encoded = bytes.to_vec();
        Ok(Self { envelope, encoded })
    }

    pub fn id(&self) -> &str {
        &self.envelope.observation_id
    }

    pub fn encode(&self) -> &[u8] {
        &self.encoded
    }

    pub fn provenance(&self) -> Provenance {
        Provenance(
            self.envelope
                .provenance
                .clone()
                .expect("validated provenance"),
        )
    }

    pub fn evidence(&self) -> EvidenceRef {
        EvidenceRef(self.envelope.evidence.clone().expect("validated evidence"))
    }

    pub fn event_key(&self) -> ProtocolEventKey {
        match self.envelope.protocol.as_ref().expect("validated protocol") {
            wire::observation_envelope::Protocol::X402(value) => ProtocolEventKey::X402(
                X402EventKey(value.event_key.clone().expect("validated x402 event key")),
            ),
            wire::observation_envelope::Protocol::Mpp(value) => ProtocolEventKey::Mpp(MppEventKey(
                value.event_key.clone().expect("validated mpp event key"),
            )),
            wire::observation_envelope::Protocol::MppDiscovery(value) => {
                ProtocolEventKey::MppDiscovery(MppDiscoveryKey(
                    value.event_key.clone().expect("validated discovery key"),
                ))
            }
        }
    }

    pub fn protocol(&self) -> ProtocolObservation {
        match self.envelope.protocol.as_ref().expect("validated protocol") {
            wire::observation_envelope::Protocol::X402(value) => {
                ProtocolObservation::X402(X402Observation(value.clone()))
            }
            wire::observation_envelope::Protocol::Mpp(value) => {
                ProtocolObservation::Mpp(MppObservation(value.clone()))
            }
            wire::observation_envelope::Protocol::MppDiscovery(value) => {
                ProtocolObservation::MppDiscovery(MppDiscoveryObservation(value.clone()))
            }
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CanonicalEvent {
    event: wire::CanonicalEvent,
    encoded: Vec<u8>,
}

impl CanonicalEvent {
    pub fn new(
        event_key: ProtocolEventKey,
        supporting_observations: &[Observation],
    ) -> Result<Self, ObservationError> {
        if supporting_observations.is_empty() {
            return Err(ObservationError::Empty("supporting observations"));
        }
        if supporting_observations
            .iter()
            .any(|observation| observation.event_key() != event_key)
        {
            return Err(ObservationError::MismatchedSupport);
        }
        let event_id = event_key.canonical_id();
        let (_, protocol) = event_key.to_wire();
        let event = wire::CanonicalEvent {
            schema_version: SCHEMA_VERSION,
            event_id,
            event_key: Some(wire::CanonicalEventKey {
                protocol: Some(protocol),
            }),
            supporting_observation_ids: supporting_observations
                .iter()
                .map(|observation| observation.id().to_owned())
                .collect(),
        };
        let encoded = event.encode_to_vec();
        Ok(Self { event, encoded })
    }

    pub fn decode(
        bytes: &[u8],
        supporting_observations: &[Observation],
    ) -> Result<Self, ObservationError> {
        let event = wire::CanonicalEvent::decode(bytes)
            .map_err(|error| ObservationError::Decode(error.to_string()))?;
        if event.schema_version != SCHEMA_VERSION {
            return Err(ObservationError::UnsupportedVersion(event.schema_version));
        }
        let event_key =
            protocol_event_key(event.event_key.as_ref().ok_or_else(|| {
                ObservationError::Decode("canonical event key is missing".into())
            })?)?;
        if event.event_id != event_key.canonical_id() {
            return Err(ObservationError::CanonicalEventIdMismatch);
        }
        if event.supporting_observation_ids.is_empty() {
            return Err(ObservationError::Empty("supporting observation ids"));
        }
        if event
            .supporting_observation_ids
            .iter()
            .any(|id| !valid_content_id(id))
        {
            return Err(ObservationError::Invalid("supporting observation id"));
        }
        if event.supporting_observation_ids.len() != supporting_observations.len()
            || event
                .supporting_observation_ids
                .iter()
                .zip(supporting_observations)
                .any(|(id, observation)| id != observation.id())
            || supporting_observations
                .iter()
                .any(|observation| observation.event_key() != event_key)
        {
            return Err(ObservationError::MismatchedSupport);
        }
        Ok(Self {
            event,
            encoded: bytes.to_vec(),
        })
    }

    pub fn id(&self) -> &str {
        &self.event.event_id
    }

    pub fn supporting_observation_ids(&self) -> &[String] {
        &self.event.supporting_observation_ids
    }

    pub fn encode(&self) -> &[u8] {
        &self.encoded
    }
}

fn protocol_event_key(key: &wire::CanonicalEventKey) -> Result<ProtocolEventKey, ObservationError> {
    match key
        .protocol
        .as_ref()
        .ok_or_else(|| ObservationError::Decode("protocol event key is missing".into()))?
    {
        wire::canonical_event_key::Protocol::X402(value) => {
            Ok(ProtocolEventKey::X402(X402EventKey::payment_identifier(
                value.payment_identifier.clone(),
                value.request_fingerprint.clone(),
                value.scope.clone(),
            )?))
        }
        wire::canonical_event_key::Protocol::Mpp(value) => {
            Ok(ProtocolEventKey::Mpp(MppEventKey::new(
                value.realm.clone(),
                value.method.clone(),
                value.challenge_id.clone(),
            )?))
        }
        wire::canonical_event_key::Protocol::MppDiscovery(value) => {
            Ok(ProtocolEventKey::MppDiscovery(MppDiscoveryKey::new(
                value.service_id.clone(),
                value.http_method.clone(),
                value.path_template.clone(),
                value.offer_index,
            )?))
        }
    }
}

fn validate_envelope(envelope: &wire::ObservationEnvelope) -> Result<(), ObservationError> {
    let provenance = envelope
        .provenance
        .as_ref()
        .ok_or_else(|| ObservationError::Decode("provenance is missing".into()))?;
    required(provenance.source_id.clone(), "provenance source id")?;
    required(provenance.parser_version.clone(), "parser version")?;
    if provenance.observed_at_unix_ms <= 0 {
        return Err(ObservationError::Invalid("observation timestamp"));
    }

    let evidence = envelope
        .evidence
        .as_ref()
        .ok_or_else(|| ObservationError::Decode("evidence reference is missing".into()))?;
    if evidence.algorithm != "sha256"
        || evidence.digest.len() != 64
        || !evidence.digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(ObservationError::Invalid("evidence digest"));
    }
    required(evidence.media_type.clone(), "evidence media type")?;

    match envelope
        .protocol
        .as_ref()
        .ok_or_else(|| ObservationError::Decode("protocol extension is missing".into()))?
    {
        wire::observation_envelope::Protocol::X402(value) => {
            let key = value
                .event_key
                .as_ref()
                .ok_or_else(|| ObservationError::Decode("x402 event key is missing".into()))?;
            required(key.payment_identifier.clone(), "x402 payment identifier")?;
            if !(16..=128).contains(&key.payment_identifier.len())
                || !key
                    .payment_identifier
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
            {
                return Err(ObservationError::Invalid("x402 payment identifier"));
            }
            if !valid_content_id(&key.request_fingerprint) {
                return Err(ObservationError::Invalid("x402 request fingerprint"));
            }
            required(key.scope.clone(), "x402 scope")?;
            required(value.asset.clone(), "x402 asset")?;
            validate_amount(&value.amount_atomic, "x402 atomic amount")?;
        }
        wire::observation_envelope::Protocol::Mpp(value) => {
            let key = value
                .event_key
                .as_ref()
                .ok_or_else(|| ObservationError::Decode("mpp event key is missing".into()))?;
            required(key.realm.clone(), "mpp realm")?;
            if !valid_mpp_method(&key.method) {
                return Err(ObservationError::Invalid("mpp method"));
            }
            required(key.challenge_id.clone(), "mpp challenge id")?;
            required(value.intent.clone(), "mpp intent")?;
            validate_amount(&value.amount_atomic, "mpp atomic amount")?;
        }
        wire::observation_envelope::Protocol::MppDiscovery(value) => {
            let key = value.event_key.as_ref().ok_or_else(|| {
                ObservationError::Decode("MPP discovery event key is missing".into())
            })?;
            required(key.service_id.clone(), "MPP discovery service id")?;
            if !valid_http_method(&key.http_method) {
                return Err(ObservationError::Invalid("MPP discovery HTTP method"));
            }
            required(key.path_template.clone(), "MPP discovery path template")?;
            required(value.openapi_version.clone(), "OpenAPI version")?;
            required(value.service_title.clone(), "MPP service title")?;
            required(value.api_version.clone(), "MPP API version")?;
            required(value.intent.clone(), "mpp intent")?;
            if !valid_mpp_method(&value.method) {
                return Err(ObservationError::Invalid("mpp method"));
            }
            if let Some(amount) = value.amount_atomic.as_deref() {
                validate_canonical_amount(amount, "mpp discovery atomic amount")?;
            }
            if value.raw_payment_info_json.is_empty() {
                return Err(ObservationError::Empty("MPP raw payment metadata"));
            }
        }
    }
    Ok(())
}

fn validate_amount(value: &str, field: &'static str) -> Result<(), ObservationError> {
    if value.is_empty() || value.parse::<u128>().is_err() {
        Err(ObservationError::Invalid(field))
    } else {
        Ok(())
    }
}

fn validate_canonical_amount(value: &str, field: &'static str) -> Result<(), ObservationError> {
    if value == "0"
        || (!value.starts_with('0')
            && !value.is_empty()
            && value.bytes().all(|byte| byte.is_ascii_digit()))
    {
        Ok(())
    } else {
        Err(ObservationError::Invalid(field))
    }
}

fn content_id(envelope: &wire::ObservationEnvelope) -> String {
    let mut content = envelope.clone();
    content.observation_id.clear();
    format!(
        "sha256:{}",
        hex::encode(Sha256::digest(content.encode_to_vec()))
    )
}

fn content_id_from_wire(bytes: &[u8]) -> Result<String, ObservationError> {
    let mut content = Vec::with_capacity(bytes.len());
    let mut cursor = 0;
    let mut observation_id_fields = 0;

    while cursor < bytes.len() {
        let field_start = cursor;
        let (key, after_key) = decode_varint(bytes, cursor)?;
        cursor = after_key;
        let field_number = key >> 3;
        let wire_type = key & 0x07;
        if field_number == 0 {
            return Err(ObservationError::Decode(
                "protobuf field number zero".into(),
            ));
        }

        cursor = match wire_type {
            0 => decode_varint(bytes, cursor)?.1,
            1 => checked_advance(bytes, cursor, 8)?,
            2 => {
                let (length, after_length) = decode_varint(bytes, cursor)?;
                let length = usize::try_from(length)
                    .map_err(|_| ObservationError::Decode("protobuf field is too large".into()))?;
                checked_advance(bytes, after_length, length)?
            }
            5 => checked_advance(bytes, cursor, 4)?,
            _ => {
                return Err(ObservationError::Decode(format!(
                    "unsupported protobuf wire type {wire_type}"
                )));
            }
        };

        if field_number == 2 {
            if wire_type != 2 {
                return Err(ObservationError::Decode(
                    "observation id has the wrong protobuf wire type".into(),
                ));
            }
            observation_id_fields += 1;
        } else {
            content.extend_from_slice(&bytes[field_start..cursor]);
        }
    }

    if observation_id_fields != 1 {
        return Err(ObservationError::Decode(
            "observation must contain exactly one observation id".into(),
        ));
    }
    Ok(format!("sha256:{}", hex::encode(Sha256::digest(content))))
}

fn decode_varint(bytes: &[u8], mut cursor: usize) -> Result<(u64, usize), ObservationError> {
    let mut value = 0_u64;
    for shift in (0..70).step_by(7) {
        let byte = *bytes
            .get(cursor)
            .ok_or_else(|| ObservationError::Decode("truncated protobuf varint".into()))?;
        cursor += 1;
        if shift == 63 && byte > 1 {
            return Err(ObservationError::Decode("protobuf varint overflow".into()));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok((value, cursor));
        }
    }
    Err(ObservationError::Decode("protobuf varint overflow".into()))
}

fn checked_advance(bytes: &[u8], cursor: usize, length: usize) -> Result<usize, ObservationError> {
    let end = cursor
        .checked_add(length)
        .ok_or_else(|| ObservationError::Decode("protobuf field length overflow".into()))?;
    if end > bytes.len() {
        return Err(ObservationError::Decode("truncated protobuf field".into()));
    }
    Ok(end)
}

fn valid_content_id(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|digest| {
        digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
    })
}

fn valid_mpp_method(value: &str) -> bool {
    !value.is_empty() && value.bytes().all(|byte| byte.is_ascii_lowercase())
}

fn valid_http_method(value: &str) -> bool {
    matches!(
        value,
        "delete" | "get" | "head" | "options" | "patch" | "post" | "put" | "trace"
    )
}

fn required(value: String, field: &'static str) -> Result<String, ObservationError> {
    if value.trim().is_empty() {
        Err(ObservationError::Empty(field))
    } else {
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn future_schema_additive_field_replays_through_v1_reader() {
        let mut envelope = wire_v2::ObservationEnvelope {
            schema_version: SCHEMA_VERSION,
            observation_id: String::new(),
            provenance: Some(wire_v2::Provenance {
                source_id: "rpc-primary".into(),
                observed_at_unix_ms: 1_790_426_627_000,
                parser_version: "x402-adapter@2.0.0".into(),
            }),
            evidence: Some(wire_v2::EvidenceRef {
                algorithm: "sha256".into(),
                digest: hex::encode(Sha256::digest(b"HTTP 402 challenge")),
                media_type: "application/http".into(),
                byte_length: 18,
            }),
            protocol: Some(wire_v2::observation_envelope::Protocol::X402(
                wire_v2::X402Observation {
                    event_key: Some(wire_v2::X402EventKey {
                        payment_identifier: "pay_123456789012".into(),
                        request_fingerprint:
                            "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                                .into(),
                        scope: "merchant-1:/paid/weather".into(),
                    }),
                    asset: "USDC".into(),
                    amount_atomic: "1000".into(),
                },
            )),
            future_note: "new additive field".into(),
        };
        envelope.observation_id = format!(
            "sha256:{}",
            hex::encode(Sha256::digest(envelope.encode_to_vec()))
        );
        let encoded = envelope.encode_to_vec();

        let decoded_by_v1 = Observation::decode(&encoded).expect("v1 accepts additive v2 field");

        assert_eq!(decoded_by_v1.id(), envelope.observation_id);
        assert_eq!(decoded_by_v1.encode(), encoded);
    }
}
