use std::{collections::BTreeSet, fmt};

use agent_economy_adapter_api::ProtocolAdapter;
use agent_economy_contracts::{
    EvidenceRef, Observation, ObservationError, ProtocolObservation, Provenance, X402EventKey,
    X402Observation,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use sha2::{Digest, Sha256};
use thiserror::Error;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceKind {
    Runtime402,
    WellKnown,
    OpenApi,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EvidenceContext {
    kind: EvidenceKind,
    source_id: String,
    endpoint: String,
    observed_at_unix_ms: i64,
}

impl EvidenceContext {
    pub fn new(
        kind: EvidenceKind,
        source_id: impl Into<String>,
        endpoint: impl Into<String>,
        observed_at_unix_ms: i64,
    ) -> Result<Self, AdapterError> {
        let source_id = source_id.into();
        let endpoint = endpoint.into();
        if source_id.trim().is_empty() {
            return Err(AdapterError::InvalidContext("source id"));
        }
        if endpoint.trim().is_empty() {
            return Err(AdapterError::InvalidContext("endpoint"));
        }
        if observed_at_unix_ms <= 0 {
            return Err(AdapterError::InvalidContext("observation timestamp"));
        }
        Ok(Self {
            kind,
            source_id,
            endpoint,
            observed_at_unix_ms,
        })
    }
}

#[derive(Clone)]
pub struct EvidenceInput<'a> {
    context: EvidenceContext,
    evidence: &'a [u8],
}

impl<'a> EvidenceInput<'a> {
    pub fn new(context: EvidenceContext, evidence: &'a [u8]) -> Self {
        Self { context, evidence }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuarantineReason {
    MalformedRuntime,
    MalformedMetadata,
    UnsupportedVersion,
    UnsupportedScheme,
    InvalidTokenMetadata,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QuarantinedEvidence {
    evidence: EvidenceRef,
    reason: QuarantineReason,
}

impl QuarantinedEvidence {
    pub fn evidence(&self) -> EvidenceRef {
        self.evidence.clone()
    }

    pub fn reason(&self) -> QuarantineReason {
        self.reason
    }
}

#[derive(Clone, Debug, Default)]
pub struct DiscoveryResult {
    observations: Vec<Observation>,
    authoritative_observations: Vec<Observation>,
    quarantined: Vec<QuarantinedEvidence>,
}

impl DiscoveryResult {
    pub fn observations(&self) -> &[Observation] {
        &self.observations
    }

    pub fn authoritative_observations(&self) -> &[Observation] {
        &self.authoritative_observations
    }

    pub fn quarantined(&self) -> &[QuarantinedEvidence] {
        &self.quarantined
    }
}

#[derive(Clone, Debug)]
pub struct X402Discovery {
    parser_version: String,
}

impl X402Discovery {
    pub fn new(parser_version: impl Into<String>) -> Result<Self, AdapterError> {
        let parser_version = parser_version.into();
        if parser_version.trim().is_empty() {
            return Err(AdapterError::InvalidContext("parser version"));
        }
        Ok(Self { parser_version })
    }

    pub fn discover<'a>(
        &self,
        inputs: impl IntoIterator<Item = EvidenceInput<'a>>,
    ) -> DiscoveryResult {
        let mut result = DiscoveryResult::default();
        let mut parsed = Vec::new();
        let mut runtime_resources = std::collections::BTreeSet::new();
        for input in inputs {
            let kind = input.context.kind;
            if kind == EvidenceKind::Runtime402 {
                runtime_resources.insert(input.context.endpoint.clone());
            }
            let media_type = media_type(kind);
            let adapter = X402Adapter {
                context: input.context,
                parser_version: self.parser_version.clone(),
            };
            match adapter.observe(input.evidence) {
                Ok(observations) => {
                    parsed.extend(
                        observations
                            .into_iter()
                            .map(|observation| (kind, observation)),
                    );
                }
                Err(error) => result.quarantined.push(QuarantinedEvidence {
                    evidence: EvidenceRef::sha256(input.evidence, media_type)
                        .expect("adapter media types are non-empty"),
                    reason: quarantine_reason(&error, kind),
                }),
            }
        }
        result.authoritative_observations = parsed
            .iter()
            .filter(|(kind, observation)| {
                *kind == EvidenceKind::Runtime402
                    || !runtime_resources.contains(&observation_resource(observation))
            })
            .map(|(_, observation)| observation.clone())
            .collect();
        result.observations = parsed
            .into_iter()
            .map(|(_, observation)| observation)
            .collect();
        result
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum AdapterError {
    #[error("invalid x402 adapter context: {0}")]
    InvalidContext(&'static str),
    #[error("invalid runtime 402 response")]
    InvalidRuntimeResponse,
    #[error("runtime 402 response is missing a valid PAYMENT-REQUIRED header")]
    MissingPaymentRequired,
    #[error("invalid x402 JSON evidence")]
    InvalidJson,
    #[error("unsupported x402 protocol version")]
    UnsupportedVersion,
    #[error("x402 payment requirements are missing")]
    MissingAccepts,
    #[error("unsupported x402 payment scheme")]
    UnsupportedScheme,
    #[error("invalid x402 token metadata")]
    InvalidTokenMetadata,
    #[error("invalid x402 observation: {0}")]
    Observation(#[from] ObservationError),
}

#[derive(Clone, Debug)]
pub struct X402Adapter {
    context: EvidenceContext,
    parser_version: String,
}

impl X402Adapter {
    pub fn new(
        context: EvidenceContext,
        parser_version: impl Into<String>,
    ) -> Result<Self, AdapterError> {
        let parser_version = parser_version.into();
        if parser_version.trim().is_empty() {
            return Err(AdapterError::InvalidContext("parser version"));
        }
        Ok(Self {
            context,
            parser_version,
        })
    }
}

impl ProtocolAdapter for X402Adapter {
    type Error = AdapterError;

    fn observe(&self, evidence: &[u8]) -> Result<Vec<Observation>, Self::Error> {
        let (documents, request_fingerprint) = match self.context.kind {
            EvidenceKind::Runtime402 => {
                let runtime = runtime_evidence(evidence)?;
                let (document, authoritative_payload) =
                    if let Some(payment_required) = runtime.payment_required {
                        let document = parse_payment_required(&payment_required)?;
                        if document.x402_version != 2 {
                            return Err(AdapterError::UnsupportedVersion);
                        }
                        (document, payment_required)
                    } else {
                        let document = parse_payment_required(runtime.body)?;
                        if document.x402_version == 2 {
                            return Err(AdapterError::MissingPaymentRequired);
                        }
                        (document, runtime.body.to_vec())
                    };
                (
                    vec![(document, self.context.endpoint.clone())],
                    content_fingerprint(&authoritative_payload),
                )
            }
            EvidenceKind::WellKnown => (
                vec![(
                    parse_payment_required(evidence)?,
                    self.context.endpoint.clone(),
                )],
                content_fingerprint(evidence),
            ),
            EvidenceKind::OpenApi => (
                parse_openapi(evidence, &self.context.endpoint)?,
                content_fingerprint(evidence),
            ),
        };
        if !documents
            .iter()
            .any(|(document, _)| document.accepts.iter().any(|offer| offer.scheme == "exact"))
        {
            return Err(AdapterError::UnsupportedScheme);
        }
        let evidence_ref = EvidenceRef::sha256(evidence, media_type(self.context.kind))?;
        documents
            .into_iter()
            .flat_map(|(document, default_resource)| {
                document
                    .accepts
                    .into_iter()
                    .filter(|offer| offer.scheme == "exact")
                    .map(move |offer| (document.x402_version, default_resource.clone(), offer))
            })
            .enumerate()
            .map(
                |(offer_index, (protocol_version, default_resource, offer))| {
                    validate_token_metadata(&offer)?;
                    let resource = offer.resource.unwrap_or(default_resource);
                    let offer_index =
                        u64::try_from(offer_index).map_err(|_| AdapterError::InvalidJson)?;
                    let discovery_fingerprint =
                        discovery_fingerprint(&request_fingerprint, &resource, offer_index);
                    Observation::new(
                        Provenance::new(
                            self.context.source_id.clone(),
                            self.context.observed_at_unix_ms,
                            self.parser_version.clone(),
                        )?,
                        evidence_ref.clone(),
                        ProtocolObservation::X402(X402Observation::discovery(
                            X402EventKey::discovery(discovery_fingerprint, resource.clone())?,
                            offer.asset,
                            offer.amount,
                            protocol_version,
                            offer.scheme,
                            offer.network,
                            offer.pay_to,
                            resource,
                        )?),
                    )
                    .map_err(AdapterError::from)
                },
            )
            .collect()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPaymentRequired {
    x402_version: u32,
    accepts: Vec<RawPaymentOption>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct RawPaymentOption {
    scheme: String,
    network: String,
    amount: Option<String>,
    max_amount_required: Option<String>,
    asset: String,
    pay_to: String,
    resource: Option<String>,
    extra: Option<PaymentExtra>,
}

#[derive(Deserialize)]
struct PaymentExtra {
    name: Option<String>,
}

struct PaymentRequired {
    x402_version: u32,
    accepts: Vec<PaymentOption>,
}

struct PaymentOption {
    scheme: String,
    network: String,
    amount: String,
    asset: String,
    pay_to: String,
    resource: Option<String>,
    extra: Option<PaymentExtra>,
}

fn parse_payment_required(evidence: &[u8]) -> Result<PaymentRequired, AdapterError> {
    reject_duplicate_json_members(evidence)?;
    let document: RawPaymentRequired =
        serde_json::from_slice(evidence).map_err(|_| AdapterError::InvalidJson)?;
    if !matches!(document.x402_version, 1 | 2) {
        return Err(AdapterError::UnsupportedVersion);
    }
    if document.accepts.is_empty() {
        return Err(AdapterError::MissingAccepts);
    }
    let accepts = document
        .accepts
        .into_iter()
        .map(|offer| {
            let amount = match document.x402_version {
                1 if offer.amount.is_none() => offer.max_amount_required,
                2 if offer.max_amount_required.is_none() => offer.amount,
                _ => None,
            }
            .ok_or(AdapterError::InvalidJson)?;
            Ok(PaymentOption {
                scheme: offer.scheme,
                network: offer.network,
                amount,
                asset: offer.asset,
                pay_to: offer.pay_to,
                resource: offer.resource,
                extra: offer.extra,
            })
        })
        .collect::<Result<Vec<_>, AdapterError>>()?;
    Ok(PaymentRequired {
        x402_version: document.x402_version,
        accepts,
    })
}

fn parse_openapi(
    evidence: &[u8],
    endpoint: &str,
) -> Result<Vec<(PaymentRequired, String)>, AdapterError> {
    reject_duplicate_json_members(evidence)?;
    let value: serde_json::Value =
        serde_json::from_slice(evidence).map_err(|_| AdapterError::InvalidJson)?;
    let openapi = value
        .get("openapi")
        .and_then(serde_json::Value::as_str)
        .ok_or(AdapterError::InvalidJson)?;
    if !supported_openapi_version(openapi) {
        return Err(AdapterError::InvalidJson);
    }
    let paths = value
        .get("paths")
        .and_then(serde_json::Value::as_object)
        .ok_or(AdapterError::InvalidJson)?;
    let mut documents = Vec::new();
    for (path, item) in paths {
        let operations = item.as_object().ok_or(AdapterError::InvalidJson)?;
        for method in [
            "delete", "get", "head", "options", "patch", "post", "put", "trace",
        ] {
            let Some(operation) = operations.get(method) else {
                continue;
            };
            let Some(extension) = operation.get("x-x402") else {
                continue;
            };
            let encoded = serde_json::to_vec(extension).map_err(|_| AdapterError::InvalidJson)?;
            documents.push((
                parse_payment_required(&encoded)?,
                format!("{}{}", endpoint.trim_end_matches('/'), path),
            ));
        }
    }
    if documents.is_empty() {
        return Err(AdapterError::MissingAccepts);
    }
    Ok(documents)
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

fn reject_duplicate_json_members(evidence: &[u8]) -> Result<(), AdapterError> {
    let mut deserializer = serde_json::Deserializer::from_slice(evidence);
    RejectDuplicateMembers
        .deserialize(&mut deserializer)
        .map_err(|_| AdapterError::InvalidJson)?;
    deserializer.end().map_err(|_| AdapterError::InvalidJson)
}

struct RuntimeEvidence<'a> {
    body: &'a [u8],
    payment_required: Option<Vec<u8>>,
}

fn runtime_evidence(evidence: &[u8]) -> Result<RuntimeEvidence<'_>, AdapterError> {
    let (headers, body) = split_http(evidence).ok_or(AdapterError::InvalidRuntimeResponse)?;
    let headers = std::str::from_utf8(headers).map_err(|_| AdapterError::InvalidRuntimeResponse)?;
    let mut lines = headers.lines();
    let status = lines.next().ok_or(AdapterError::InvalidRuntimeResponse)?;
    let mut status_parts = status.splitn(3, ' ');
    let version = status_parts.next();
    let code = status_parts.next();
    let reason = status_parts.next();
    if !matches!(version, Some("HTTP/1.0" | "HTTP/1.1")) || code != Some("402") || reason.is_none()
    {
        return Err(AdapterError::InvalidRuntimeResponse);
    }
    let encoded = lines
        .filter_map(|line| line.split_once(':'))
        .filter(|(name, _)| name.eq_ignore_ascii_case("payment-required"))
        .map(|(_, value)| value.trim())
        .filter(|value| !value.is_empty())
        .collect::<Vec<_>>();
    if encoded.len() > 1 {
        return Err(AdapterError::MissingPaymentRequired);
    }
    let payment_required = encoded
        .first()
        .map(|encoded| {
            STANDARD
                .decode(encoded)
                .map_err(|_| AdapterError::MissingPaymentRequired)
        })
        .transpose()?;
    Ok(RuntimeEvidence {
        body,
        payment_required,
    })
}

fn content_fingerprint(content: &[u8]) -> String {
    format!("sha256:{}", hex::encode(Sha256::digest(content)))
}

fn discovery_fingerprint(request_fingerprint: &str, resource: &str, offer_index: u64) -> String {
    let mut digest = Sha256::new();
    digest.update(request_fingerprint.as_bytes());
    digest.update([0]);
    digest.update(resource.as_bytes());
    digest.update([0]);
    digest.update(offer_index.to_be_bytes());
    format!("sha256:{}", hex::encode(digest.finalize()))
}

fn supported_openapi_version(version: &str) -> bool {
    let mut components = version.split('.');
    matches!(
        (
            components.next(),
            components.next(),
            components.next(),
            components.next()
        ),
        (Some("3"), Some("0" | "1"), Some(patch), None)
            if !patch.is_empty()
                && (patch == "0" || !patch.starts_with('0'))
                && patch.bytes().all(|byte| byte.is_ascii_digit())
    )
}

fn split_http(evidence: &[u8]) -> Option<(&[u8], &[u8])> {
    evidence
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|index| (&evidence[..index], &evidence[index + 4..]))
        .or_else(|| {
            evidence
                .windows(2)
                .position(|window| window == b"\n\n")
                .map(|index| (&evidence[..index], &evidence[index + 2..]))
        })
}

fn observation_resource(observation: &Observation) -> String {
    match observation.protocol() {
        ProtocolObservation::X402(value) => value.resource().to_owned(),
        ProtocolObservation::Mpp(_) | ProtocolObservation::MppDiscovery(_) => {
            unreachable!("x402 adapter emitted an MPP observation")
        }
    }
}

fn media_type(kind: EvidenceKind) -> &'static str {
    match kind {
        EvidenceKind::Runtime402 => "application/http",
        EvidenceKind::WellKnown | EvidenceKind::OpenApi => "application/json",
    }
}

fn quarantine_reason(error: &AdapterError, kind: EvidenceKind) -> QuarantineReason {
    match error {
        AdapterError::UnsupportedVersion => QuarantineReason::UnsupportedVersion,
        AdapterError::UnsupportedScheme => QuarantineReason::UnsupportedScheme,
        AdapterError::InvalidTokenMetadata => QuarantineReason::InvalidTokenMetadata,
        AdapterError::InvalidRuntimeResponse
        | AdapterError::MissingPaymentRequired
        | AdapterError::InvalidContext(_)
        | AdapterError::InvalidJson
        | AdapterError::MissingAccepts
        | AdapterError::Observation(_) => match kind {
            EvidenceKind::Runtime402 => QuarantineReason::MalformedRuntime,
            EvidenceKind::WellKnown | EvidenceKind::OpenApi => QuarantineReason::MalformedMetadata,
        },
    }
}

fn validate_token_metadata(offer: &PaymentOption) -> Result<(), AdapterError> {
    const BASE_USDC: &str = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
    if offer.network == "eip155:8453"
        && offer.asset.eq_ignore_ascii_case(BASE_USDC)
        && offer.extra.as_ref().and_then(|extra| extra.name.as_deref()) != Some("USD Coin")
    {
        return Err(AdapterError::InvalidTokenMetadata);
    }
    Ok(())
}
