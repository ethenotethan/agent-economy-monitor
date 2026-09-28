use agent_economy_adapter_api::ProtocolAdapter;
use agent_economy_contracts::{ProtocolObservation, X402Observation};
use agent_economy_x402_adapter::{
    EvidenceContext, EvidenceInput, EvidenceKind, QuarantineReason, X402Adapter, X402Discovery,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use static_assertions::assert_not_impl_any;

assert_not_impl_any!(EvidenceInput<'static>: std::fmt::Debug);

const RUNTIME_V2: &[u8] = include_bytes!("fixtures/runtime-v2.http");
const RUNTIME_V1: &[u8] = include_bytes!("fixtures/runtime-v1.http");
const WELL_KNOWN_V1: &[u8] = include_bytes!("fixtures/well-known-v1.json");
const OPENAPI_V2: &[u8] = include_bytes!("fixtures/openapi-v2.json");
const MALFORMED_RUNTIME: &[u8] = include_bytes!("fixtures/malformed-runtime.http");
const SIWX_ONLY: &[u8] = include_bytes!("fixtures/siwx-only.json");
const BASE_USDC_TICKER_NAME: &[u8] = include_bytes!("fixtures/base-usdc-ticker-name.json");

fn x402(observation: &agent_economy_contracts::Observation) -> X402Observation {
    match observation.protocol() {
        ProtocolObservation::X402(value) => value,
        ProtocolObservation::Mpp(_) | ProtocolObservation::MppDiscovery(_) => {
            panic!("expected x402 observation")
        }
    }
}

#[test]
fn runtime_v2_challenge_emits_hashed_versioned_observation() {
    let adapter = X402Adapter::new(
        EvidenceContext::new(
            EvidenceKind::Runtime402,
            "runtime-402:merchant.example/premium",
            "https://merchant.example/premium",
            1_790_426_627_000,
        )
        .expect("valid context"),
        "x402-adapter@1",
    )
    .expect("valid adapter");

    let observations = adapter.observe(RUNTIME_V2).expect("valid v2 challenge");

    assert_eq!(observations.len(), 1);
    let observed = &observations[0];
    let payload = x402(observed);
    assert_eq!(observed.evidence().byte_length(), RUNTIME_V2.len() as u64);
    assert_eq!(observed.evidence().media_type(), "application/http");
    assert_eq!(observed.provenance().parser_version(), "x402-adapter@1");
    assert_eq!(payload.protocol_version(), 2);
    assert_eq!(payload.scheme(), "exact");
    assert_eq!(payload.network(), "eip155:8453");
    assert_eq!(payload.amount_atomic(), "1000");
    assert_eq!(
        payload.pay_to(),
        "0x1111111111111111111111111111111111111111"
    );
    assert_eq!(payload.resource(), "https://merchant.example/premium");
}

#[test]
fn well_known_v1_metadata_emits_versioned_observation() {
    let adapter = X402Adapter::new(
        EvidenceContext::new(
            EvidenceKind::WellKnown,
            "well-known:merchant.example",
            "https://merchant.example/premium",
            1_790_426_627_000,
        )
        .expect("valid context"),
        "x402-adapter@1",
    )
    .expect("valid adapter");

    let observations = adapter.observe(WELL_KNOWN_V1).expect("valid v1 metadata");

    assert_eq!(observations.len(), 1);
    let payload = x402(&observations[0]);
    assert_eq!(payload.protocol_version(), 1);
    assert_eq!(payload.network(), "base");
    assert_eq!(payload.amount_atomic(), "900");
    assert_eq!(observations[0].evidence().media_type(), "application/json");
}

#[test]
fn runtime_v1_challenge_does_not_require_v2_payment_required_header() {
    let adapter = X402Adapter::new(
        EvidenceContext::new(
            EvidenceKind::Runtime402,
            "runtime-402:merchant.example/premium",
            "https://merchant.example/premium",
            1_790_426_627_000,
        )
        .expect("valid context"),
        "x402-adapter@1",
    )
    .expect("valid adapter");

    let observations = adapter.observe(RUNTIME_V1).expect("valid v1 challenge");

    assert_eq!(observations.len(), 1);
    assert_eq!(x402(&observations[0]).protocol_version(), 1);
}

#[test]
fn openapi_x402_extension_uses_operation_path_as_resource() {
    let adapter = X402Adapter::new(
        EvidenceContext::new(
            EvidenceKind::OpenApi,
            "openapi:merchant.example",
            "https://merchant.example",
            1_790_426_627_000,
        )
        .expect("valid context"),
        "x402-adapter@1",
    )
    .expect("valid adapter");

    let observations = adapter.observe(OPENAPI_V2).expect("valid OpenAPI metadata");

    assert_eq!(observations.len(), 1);
    let payload = x402(&observations[0]);
    assert_eq!(payload.protocol_version(), 2);
    assert_eq!(payload.resource(), "https://merchant.example/premium");
    assert_eq!(payload.amount_atomic(), "1100");
}

#[test]
fn openapi_without_supported_version_is_rejected() {
    let evidence = br#"{"paths":{"/premium":{"get":{"x-x402":{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"1100","asset":"USDC","payTo":"0x3333333333333333333333333333333333333333"}]}}}}}"#;

    assert!(adapter(EvidenceKind::OpenApi).observe(evidence).is_err());
}

#[test]
fn openapi_duplicate_members_are_rejected() {
    let evidence = br#"{"openapi":"3.0.0","openapi":"3.1.0","paths":{"/premium":{"get":{"x-x402":{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"1100","asset":"USDC","payTo":"0x3333333333333333333333333333333333333333"}]}}}}}"#;

    assert!(adapter(EvidenceKind::OpenApi).observe(evidence).is_err());
}

#[test]
fn malformed_openapi_version_prefix_is_rejected() {
    let evidence = br#"{"openapi":"3.0.evil","paths":{"/premium":{"get":{"x-x402":{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"1100","asset":"USDC","payTo":"0x3333333333333333333333333333333333333333"}]}}}}}"#;

    assert!(adapter(EvidenceKind::OpenApi).observe(evidence).is_err());
}

#[test]
fn malformed_runtime_response_is_quarantined_with_evidence_hash() {
    let context = EvidenceContext::new(
        EvidenceKind::Runtime402,
        "runtime-402:merchant.example/premium",
        "https://merchant.example/premium",
        1_790_426_627_000,
    )
    .expect("valid context");

    let result = X402Discovery::new("x402-adapter@1")
        .expect("valid discovery")
        .discover([EvidenceInput::new(context, MALFORMED_RUNTIME)]);

    assert!(result.observations().is_empty());
    assert_eq!(result.quarantined().len(), 1);
    assert_eq!(
        result.quarantined()[0].reason(),
        QuarantineReason::MalformedRuntime
    );
    assert_eq!(
        result.quarantined()[0].evidence().byte_length(),
        MALFORMED_RUNTIME.len() as u64
    );
    assert_eq!(result.quarantined()[0].evidence().algorithm(), "sha256");
}

#[test]
fn runtime_observation_is_authoritative_without_discarding_static_evidence() {
    let static_context = EvidenceContext::new(
        EvidenceKind::WellKnown,
        "well-known:merchant.example",
        "https://merchant.example/premium",
        1_790_426_626_000,
    )
    .expect("valid static context");
    let runtime_context = EvidenceContext::new(
        EvidenceKind::Runtime402,
        "runtime-402:merchant.example/premium",
        "https://merchant.example/premium",
        1_790_426_627_000,
    )
    .expect("valid runtime context");

    let result = X402Discovery::new("x402-adapter@1")
        .expect("valid discovery")
        .discover([
            EvidenceInput::new(static_context, WELL_KNOWN_V1),
            EvidenceInput::new(runtime_context, RUNTIME_V2),
        ]);

    assert_eq!(result.observations().len(), 2);
    assert_eq!(result.authoritative_observations().len(), 1);
    let authoritative = &result.authoritative_observations()[0];
    assert_eq!(
        authoritative.provenance().source_id(),
        "runtime-402:merchant.example/premium"
    );
    assert_eq!(x402(authoritative).amount_atomic(), "1000");
}

#[test]
fn siwx_only_metadata_is_quarantined_as_unsupported_scheme() {
    let context = EvidenceContext::new(
        EvidenceKind::WellKnown,
        "well-known:merchant.example",
        "https://merchant.example/premium",
        1_790_426_627_000,
    )
    .expect("valid context");

    let result = X402Discovery::new("x402-adapter@1")
        .expect("valid discovery")
        .discover([EvidenceInput::new(context, SIWX_ONLY)]);

    assert!(result.observations().is_empty());
    assert_eq!(result.quarantined().len(), 1);
    assert_eq!(
        result.quarantined()[0].reason(),
        QuarantineReason::UnsupportedScheme
    );
}

#[test]
fn base_usdc_ticker_as_eip712_name_is_quarantined() {
    let context = EvidenceContext::new(
        EvidenceKind::WellKnown,
        "well-known:merchant.example",
        "https://merchant.example/premium",
        1_790_426_627_000,
    )
    .expect("valid context");

    let result = X402Discovery::new("x402-adapter@1")
        .expect("valid discovery")
        .discover([EvidenceInput::new(context, BASE_USDC_TICKER_NAME)]);

    assert!(result.observations().is_empty());
    assert_eq!(result.quarantined().len(), 1);
    assert_eq!(
        result.quarantined()[0].reason(),
        QuarantineReason::InvalidTokenMetadata
    );
}

#[test]
fn runtime_v2_uses_payment_required_header_as_authoritative_payload() {
    let header_payload = br#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"777","asset":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","payTo":"0x1111111111111111111111111111111111111111","resource":"https://merchant.example/premium","extra":{"name":"USD Coin"}}]}"#;
    let body = r#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"1000","asset":"0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913","payTo":"0x1111111111111111111111111111111111111111","resource":"https://merchant.example/premium","extra":{"name":"USD Coin"}}]}"#;
    let evidence = format!(
        "HTTP/1.1 402 Payment Required\nContent-Type: application/json\nPAYMENT-REQUIRED: {}\n\n{body}",
        STANDARD.encode(header_payload)
    );

    let observations = adapter(EvidenceKind::Runtime402)
        .observe(evidence.as_bytes())
        .expect("valid v2 header");

    assert_eq!(x402(&observations[0]).amount_atomic(), "777");
}

#[test]
fn runtime_v2_header_does_not_depend_on_body_schema() {
    let header_payload = br#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"777","asset":"USDC","payTo":"0x1111111111111111111111111111111111111111"}]}"#;
    let evidence = format!(
        "HTTP/1.1 402 Payment Required\nPAYMENT-REQUIRED: {}\n\nnot-json",
        STANDARD.encode(header_payload)
    );

    let observations = adapter(EvidenceKind::Runtime402)
        .observe(evidence.as_bytes())
        .expect("valid v2 header is authoritative");

    assert_eq!(x402(&observations[0]).protocol_version(), 2);
    assert_eq!(x402(&observations[0]).amount_atomic(), "777");
}

#[test]
fn runtime_v2_event_key_does_not_depend_on_non_authoritative_body() {
    let header_payload = br#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"777","asset":"USDC","payTo":"0x1111111111111111111111111111111111111111"}]}"#;
    let header = STANDARD.encode(header_payload);
    let first = format!(
        "HTTP/1.1 402 Payment Required\nPAYMENT-REQUIRED: {header}\n\n{{\"error\":\"first\"}}"
    );
    let second = format!(
        "HTTP/1.1 402 Payment Required\nPAYMENT-REQUIRED: {header}\n\n{{\"error\":\"second\"}}"
    );

    let first = adapter(EvidenceKind::Runtime402)
        .observe(first.as_bytes())
        .expect("valid first v2 response");
    let second = adapter(EvidenceKind::Runtime402)
        .observe(second.as_bytes())
        .expect("valid second v2 response");

    assert_eq!(
        first[0].event_key().canonical_id(),
        second[0].event_key().canonical_id()
    );
    assert_ne!(first[0].evidence().digest(), second[0].evidence().digest());
}

#[test]
fn runtime_status_line_must_use_http_grammar() {
    for status in [
        "NOT-HTTP 402 Payment Required",
        "HTTP/1.1\t402 Payment Required",
        "HTTP/2 402",
    ] {
        let evidence = format!(
            "{status}\n\n{{\"x402Version\":1,\"accepts\":[{{\"scheme\":\"exact\",\"network\":\"base\",\"maxAmountRequired\":\"900\",\"asset\":\"USDC\",\"payTo\":\"0x2222222222222222222222222222222222222222\"}}]}}"
        );

        assert!(
            adapter(EvidenceKind::Runtime402)
                .observe(evidence.as_bytes())
                .is_err(),
            "accepted malformed status line: {status}"
        );
    }
}

#[test]
fn duplicate_json_members_are_quarantined_as_malformed_metadata() {
    let evidence = br#"{"x402Version":2,"x402Version":1,"accepts":[{"scheme":"exact","network":"base","maxAmountRequired":"900","asset":"USDC","payTo":"0x2222222222222222222222222222222222222222"}]}"#;

    let result = X402Discovery::new("x402-adapter@1")
        .expect("valid discovery")
        .discover([EvidenceInput::new(
            context(EvidenceKind::WellKnown),
            evidence,
        )]);

    assert_eq!(
        result.quarantined()[0].reason(),
        QuarantineReason::MalformedMetadata
    );
}

#[test]
fn v2_rejects_v1_max_amount_field() {
    let evidence = br#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","maxAmountRequired":"900","asset":"USDC","payTo":"0x2222222222222222222222222222222222222222"}]}"#;

    assert!(adapter(EvidenceKind::WellKnown).observe(evidence).is_err());
}

#[test]
fn malformed_runtime_prevents_static_metadata_from_becoming_authoritative() {
    let result = X402Discovery::new("x402-adapter@1")
        .expect("valid discovery")
        .discover([
            EvidenceInput::new(context(EvidenceKind::WellKnown), WELL_KNOWN_V1),
            EvidenceInput::new(context(EvidenceKind::Runtime402), MALFORMED_RUNTIME),
        ]);

    assert_eq!(result.observations().len(), 1);
    assert!(result.authoritative_observations().is_empty());
    assert_eq!(result.quarantined().len(), 1);
}

#[test]
fn mixed_exact_and_siwx_metadata_preserves_supported_offer() {
    let evidence = br#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"1000","asset":"USDC","payTo":"0x1111111111111111111111111111111111111111"},{"scheme":"siwx","network":"eip155:8453","amount":"1000","asset":"USDC","payTo":"0x1111111111111111111111111111111111111111"}]}"#;

    let observations = adapter(EvidenceKind::WellKnown)
        .observe(evidence)
        .expect("supported offer survives unrelated scheme");

    assert_eq!(observations.len(), 1);
    assert_eq!(x402(&observations[0]).scheme(), "exact");
}

#[test]
fn multiple_offers_for_one_resource_have_distinct_event_keys() {
    let evidence = br#"{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:1","amount":"1000","asset":"USDC","payTo":"0x1111111111111111111111111111111111111111"},{"scheme":"exact","network":"eip155:10","amount":"1000","asset":"USDC","payTo":"0x1111111111111111111111111111111111111111"}]}"#;

    let observations = adapter(EvidenceKind::WellKnown)
        .observe(evidence)
        .expect("valid offers");

    assert_eq!(observations.len(), 2);
    assert_ne!(
        observations[0].event_key().canonical_id(),
        observations[1].event_key().canonical_id()
    );
}

#[test]
fn offers_on_different_operations_have_distinct_event_keys() {
    let evidence = br#"{"openapi":"3.1.0","paths":{"/premium":{"get":{"x-x402":{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"1100","asset":"USDC","payTo":"0x3333333333333333333333333333333333333333"}]}},"post":{"x-x402":{"x402Version":2,"accepts":[{"scheme":"exact","network":"eip155:8453","amount":"1100","asset":"USDC","payTo":"0x3333333333333333333333333333333333333333"}]}}}}}"#;

    let observations = adapter(EvidenceKind::OpenApi)
        .observe(evidence)
        .expect("valid OpenAPI metadata");

    assert_eq!(observations.len(), 2);
    assert_ne!(
        observations[0].event_key().canonical_id(),
        observations[1].event_key().canonical_id()
    );
}

fn context(kind: EvidenceKind) -> EvidenceContext {
    EvidenceContext::new(
        kind,
        "discovery:merchant.example/premium",
        "https://merchant.example/premium",
        1_790_426_627_000,
    )
    .expect("valid context")
}

fn adapter(kind: EvidenceKind) -> X402Adapter {
    X402Adapter::new(context(kind), "x402-adapter@1").expect("valid adapter")
}
