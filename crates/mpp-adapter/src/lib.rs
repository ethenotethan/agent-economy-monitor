use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
};

use agent_economy_adapter_api::ProtocolAdapter;
use agent_economy_contracts::{
    EvidenceRef, MppDiscoveryKey, MppDiscoveryObservation, Observation, ObservationError,
    ProtocolObservation, Provenance,
};
use serde::{
    Deserialize,
    de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor},
};
use serde_json::{Value, value::RawValue};
use thiserror::Error;

const HTTP_METHODS: [&str; 8] = [
    "delete", "get", "head", "options", "patch", "post", "put", "trace",
];

#[derive(Debug, Error, PartialEq, Eq)]
pub enum MppAdapterError {
    #[error("invalid OpenAPI JSON")]
    InvalidJson,
    #[error("unsupported OpenAPI version")]
    UnsupportedOpenApi,
    #[error("invalid OpenAPI document: {field}")]
    InvalidDocument { field: &'static str },
    #[error("invalid payable operation: {reason}")]
    InvalidOperation { reason: &'static str },
    #[error("invalid MPP offer {index}: {reason}")]
    InvalidOffer { index: usize, reason: &'static str },
    #[error(transparent)]
    Contract(#[from] ObservationError),
}

#[derive(Clone, Debug)]
pub struct MppDiscoveryAdapter {
    service_id: String,
    provenance: Provenance,
}

impl MppDiscoveryAdapter {
    pub fn new(
        service_id: impl Into<String>,
        source_id: impl Into<String>,
        observed_at_unix_ms: i64,
        parser_version: impl Into<String>,
    ) -> Result<Self, ObservationError> {
        let service_id = service_id.into();
        if service_id.trim().is_empty() {
            return Err(ObservationError::Empty("MPP discovery service id"));
        }
        Ok(Self {
            service_id,
            provenance: Provenance::new(source_id, observed_at_unix_ms, parser_version)?,
        })
    }
}

impl ProtocolAdapter for MppDiscoveryAdapter {
    type Error = MppAdapterError;

    fn observe(&self, evidence: &[u8]) -> Result<Vec<Observation>, Self::Error> {
        reject_duplicate_json_members(evidence)?;
        let document: OpenApiDocument =
            serde_json::from_slice(evidence).map_err(|_| MppAdapterError::InvalidJson)?;
        if !supported_openapi_version(&document.openapi) {
            return Err(MppAdapterError::UnsupportedOpenApi);
        }
        if document.info.title.trim().is_empty() {
            return Err(MppAdapterError::InvalidDocument {
                field: "info.title",
            });
        }
        if document.info.version.trim().is_empty() {
            return Err(MppAdapterError::InvalidDocument {
                field: "info.version",
            });
        }

        let evidence_ref = EvidenceRef::sha256(evidence, "application/vnd.oai.openapi+json")?;
        let mut observations = Vec::new();
        for (path, path_item) in document.paths {
            for method in HTTP_METHODS {
                let Some(raw_operation) = path_item.get(method) else {
                    continue;
                };
                let operation: Operation =
                    serde_json::from_str(raw_operation.get()).map_err(|_| {
                        MppAdapterError::InvalidOperation {
                            reason: "operation must be an object",
                        }
                    })?;
                let Some(payment_info) = operation.payment_info else {
                    continue;
                };
                if !operation.responses.contains_key("402") {
                    return Err(MppAdapterError::InvalidOperation {
                        reason: "payable operation must declare a 402 response",
                    });
                }
                let offers = parse_offers(payment_info.get())?;
                for (index, offer) in offers.into_iter().enumerate() {
                    let offer_index =
                        u32::try_from(index).map_err(|_| MppAdapterError::InvalidOperation {
                            reason: "too many payment offers",
                        })?;
                    let event_key = MppDiscoveryKey::new(
                        self.service_id.clone(),
                        method,
                        path.clone(),
                        offer_index,
                    )?;
                    observations.push(Observation::new(
                        self.provenance.clone(),
                        evidence_ref.clone(),
                        ProtocolObservation::MppDiscovery(MppDiscoveryObservation::new(
                            event_key,
                            document.openapi.clone(),
                            document.info.title.clone(),
                            document.info.version.clone(),
                            offer.intent,
                            offer.method,
                            offer.amount,
                            offer.currency,
                            offer.description,
                            offer.raw_json,
                        )?),
                    )?);
                }
            }
        }
        Ok(observations)
    }
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

fn reject_duplicate_json_members(evidence: &[u8]) -> Result<(), MppAdapterError> {
    let mut deserializer = serde_json::Deserializer::from_slice(evidence);
    RejectDuplicateMembers
        .deserialize(&mut deserializer)
        .map_err(|_| MppAdapterError::InvalidJson)?;
    deserializer.end().map_err(|_| MppAdapterError::InvalidJson)
}

#[derive(Deserialize)]
struct OpenApiDocument {
    openapi: String,
    info: OpenApiInfo,
    paths: BTreeMap<String, BTreeMap<String, Box<RawValue>>>,
}

#[derive(Deserialize)]
struct OpenApiInfo {
    title: String,
    version: String,
}

#[derive(Deserialize)]
struct Operation {
    #[serde(rename = "x-payment-info")]
    payment_info: Option<Box<RawValue>>,
    #[serde(default)]
    responses: BTreeMap<String, Value>,
}

struct Offer {
    intent: String,
    method: String,
    amount: Option<String>,
    currency: Option<String>,
    description: Option<String>,
    raw_json: Vec<u8>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MultipleOffers {
    offers: Vec<Box<RawValue>>,
}

fn parse_offers(raw: &str) -> Result<Vec<Offer>, MppAdapterError> {
    let value: Value =
        serde_json::from_str(raw).map_err(|_| MppAdapterError::InvalidOperation {
            reason: "payment metadata must be valid JSON",
        })?;
    let object = value.as_object().ok_or(MppAdapterError::InvalidOperation {
        reason: "payment metadata must be an object",
    })?;

    if object.contains_key("offers") {
        if object.len() != 1 {
            return Err(invalid_offer(0, "multi-offer metadata has unknown fields"));
        }
        let offers: MultipleOffers = serde_json::from_str(raw)
            .map_err(|_| invalid_offer(0, "offers must be a non-empty array"))?;
        if offers.offers.is_empty() {
            return Err(invalid_offer(0, "offers must be a non-empty array"));
        }
        offers
            .offers
            .iter()
            .enumerate()
            .map(|(index, offer)| parse_offer(offer.get(), index))
            .collect()
    } else {
        Ok(vec![parse_offer(raw, 0)?])
    }
}

fn parse_offer(raw: &str, index: usize) -> Result<Offer, MppAdapterError> {
    let value: Value =
        serde_json::from_str(raw).map_err(|_| invalid_offer(index, "offer must be valid JSON"))?;
    let object = value
        .as_object()
        .ok_or_else(|| invalid_offer(index, "offer must be an object"))?;
    const ALLOWED: [&str; 5] = ["amount", "currency", "description", "intent", "method"];
    if object.keys().any(|key| !ALLOWED.contains(&key.as_str())) {
        return Err(invalid_offer(index, "offer has unknown fields"));
    }

    let intent = required_string(object, "intent", index)?;
    if !matches!(intent.as_str(), "charge" | "session") {
        return Err(invalid_offer(index, "unsupported payment intent"));
    }
    let method = required_string(object, "method", index)?;
    if method.is_empty() || !method.bytes().all(|byte| byte.is_ascii_lowercase()) {
        return Err(invalid_offer(
            index,
            "method must contain lowercase ASCII letters",
        ));
    }
    let amount = match object.get("amount") {
        Some(Value::Null) => None,
        Some(Value::String(amount)) if canonical_amount(amount) => Some(amount.clone()),
        Some(_) => {
            return Err(invalid_offer(
                index,
                "amount must use canonical atomic units or null",
            ));
        }
        None => {
            return Err(invalid_offer(index, "amount is required"));
        }
    };

    Ok(Offer {
        intent,
        method,
        amount,
        currency: optional_string(object, "currency", index)?,
        description: optional_string(object, "description", index)?,
        raw_json: raw.as_bytes().to_vec(),
    })
}

fn required_string(
    object: &serde_json::Map<String, Value>,
    field: &'static str,
    index: usize,
) -> Result<String, MppAdapterError> {
    match object.get(field) {
        Some(Value::String(value)) if !value.is_empty() => Ok(value.clone()),
        _ => Err(invalid_offer(index, "required string is missing")),
    }
}

fn optional_string(
    object: &serde_json::Map<String, Value>,
    field: &'static str,
    index: usize,
) -> Result<Option<String>, MppAdapterError> {
    match object.get(field) {
        None => Ok(None),
        Some(Value::String(value)) => Ok(Some(value.clone())),
        Some(_) => Err(invalid_offer(index, "optional field must be a string")),
    }
}

fn canonical_amount(value: &str) -> bool {
    value == "0"
        || (!value.starts_with('0')
            && !value.is_empty()
            && value.bytes().all(|byte| byte.is_ascii_digit()))
}

fn supported_openapi_version(version: &str) -> bool {
    let mut components = version.split('.');
    matches!(
        (components.next(), components.next(), components.next(), components.next()),
        (Some("3"), Some("0" | "1"), Some(patch), None)
            if !patch.is_empty()
                && (patch == "0" || !patch.starts_with('0'))
                && patch.bytes().all(|byte| byte.is_ascii_digit())
    )
}

fn invalid_offer(index: usize, reason: &'static str) -> MppAdapterError {
    MppAdapterError::InvalidOffer { index, reason }
}
