use std::convert::Infallible;

use agent_economy_adapter_api::ProtocolAdapter;
use agent_economy_contracts::{
    EvidenceRef, Observation, ObservationError, ProtocolObservation, Provenance, X402EventKey,
    X402Observation,
};

struct X402Adapter;

impl ProtocolAdapter for X402Adapter {
    type Error = ObservationError;

    fn observe(&self, evidence: &[u8]) -> Result<Vec<Observation>, Self::Error> {
        Ok(vec![Observation::new(
            Provenance::new("runtime-402", 1_790_426_627_000, "test-adapter@1")?,
            EvidenceRef::sha256(evidence, "application/http")?,
            ProtocolObservation::X402(X402Observation::new(
                X402EventKey::payment_identifier(
                    "pay_123456789012",
                    "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "merchant-1:/paid/weather",
                )?,
                "USDC",
                "1000",
            )?),
        )?])
    }
}

fn collect<A: ProtocolAdapter>(adapter: &A) -> Result<Vec<Observation>, A::Error> {
    adapter.observe(b"HTTP 402 challenge")
}

#[test]
fn adapters_emit_immutable_observations() -> Result<(), Infallible> {
    let observations = collect(&X402Adapter).expect("adapter should parse fixture");

    assert_eq!(observations.len(), 1);
    assert!(observations[0].id().starts_with("sha256:"));
    Ok(())
}
