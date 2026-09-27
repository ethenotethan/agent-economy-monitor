use agent_economy_contracts::Observation;

/// Stateless boundary implemented by protocol parsers.
///
/// Adapters can emit immutable observations only. Canonical entities are
/// deliberately absent from this crate's dependency graph and API.
pub trait ProtocolAdapter {
    type Error;

    fn observe(&self, evidence: &[u8]) -> Result<Vec<Observation>, Self::Error>;
}
