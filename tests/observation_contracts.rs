use agent_economy_contracts::{
    CanonicalEvent, EvidenceRef, MppEventKey, MppObservation, Observation, ObservationError,
    ProtocolEventKey, ProtocolObservation, Provenance, X402EventKey, X402Observation,
};

fn observation(source_id: &str) -> Result<Observation, ObservationError> {
    Observation::new(
        Provenance::new(source_id, 1_790_426_627_000, "x402-adapter@1.0.0")?,
        EvidenceRef::sha256(b"HTTP 402 challenge", "application/http")?,
        ProtocolObservation::X402(X402Observation::new(x402_key()?, "USDC", "1000")?),
    )
}

fn x402_key() -> Result<X402EventKey, ObservationError> {
    X402EventKey::payment_identifier(
        "pay_123456789012",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "merchant-1:/paid/weather",
    )
}

#[test]
fn observation_id_is_content_addressed() -> Result<(), ObservationError> {
    let first = observation("rpc-primary")?;
    let replay = observation("rpc-primary")?;
    let distinct_source = observation("rpc-secondary")?;

    assert_eq!(first.id(), replay.id());
    assert!(first.id().starts_with("sha256:"));
    assert_ne!(first.id(), distinct_source.id());
    assert_eq!(first.encode(), replay.encode());

    Ok(())
}

#[test]
fn observation_construction_rejects_invalid_provenance() -> Result<(), ObservationError> {
    let result = Provenance::new("rpc-primary", 0, "x402-adapter@1.0.0");

    assert_eq!(
        result,
        Err(ObservationError::Invalid("observation timestamp"))
    );
    Ok(())
}

#[test]
fn protocol_observation_construction_rejects_invalid_amount() -> Result<(), ObservationError> {
    let result = X402Observation::new(x402_key()?, "USDC", "1.25");

    assert_eq!(result, Err(ObservationError::Invalid("x402 atomic amount")));
    Ok(())
}

#[test]
fn canonical_event_keys_are_protocol_defined() -> Result<(), ObservationError> {
    let x402 = ProtocolEventKey::X402(x402_key()?);
    let mpp = ProtocolEventKey::Mpp(MppEventKey::tempo("api.example.com", "challenge-7")?);

    assert_eq!(x402.canonical_id(), x402.canonical_id());
    assert!(x402.canonical_id().starts_with("event:x402:sha256:"));
    assert!(mpp.canonical_id().starts_with("event:mpp:sha256:"));
    assert_ne!(x402.canonical_id(), mpp.canonical_id());

    Ok(())
}

#[test]
fn mpp_event_keys_are_scoped_by_required_realm() -> Result<(), ObservationError> {
    let first = ProtocolEventKey::Mpp(MppEventKey::tempo("api.example.com", "challenge-7")?);
    let second = ProtocolEventKey::Mpp(MppEventKey::tempo("api.other.test", "challenge-7")?);

    assert_ne!(first.canonical_id(), second.canonical_id());
    Ok(())
}

#[test]
fn x402_event_key_requires_protocol_identifier_binding() {
    let result = X402EventKey::payment_identifier(
        "short",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "merchant-1:/paid/weather",
    );

    assert_eq!(
        result,
        Err(ObservationError::Invalid("x402 payment identifier"))
    );
}

#[test]
fn mpp_event_key_rejects_invalid_method_grammar() {
    assert_eq!(
        MppEventKey::new("api.example.com", "Tempo-1", "challenge-7"),
        Err(ObservationError::Invalid("mpp method"))
    );
}

#[test]
fn protobuf_replay_preserves_the_observation() -> Result<(), ObservationError> {
    let original = observation("rpc-primary")?;
    let replayed = Observation::decode(original.encode())?;

    assert_eq!(replayed.id(), original.id());
    assert_eq!(replayed.encode(), original.encode());

    Ok(())
}

#[test]
fn reducers_can_read_typed_observation_fields() -> Result<(), ObservationError> {
    let observed = observation("rpc-primary")?;

    assert_eq!(observed.provenance().source_id(), "rpc-primary");
    assert_eq!(observed.evidence().algorithm(), "sha256");
    assert_eq!(observed.event_key(), ProtocolEventKey::X402(x402_key()?));
    match observed.protocol() {
        ProtocolObservation::X402(payload) => {
            assert_eq!(payload.asset(), "USDC");
            assert_eq!(payload.amount_atomic(), "1000");
        }
        ProtocolObservation::Mpp(_) => panic!("expected x402 observation"),
    }
    Ok(())
}

#[test]
fn additive_unknown_fields_are_covered_by_the_content_address() -> Result<(), ObservationError> {
    let original = observation("rpc-primary")?;
    let mut tampered_wire = original.encode().to_vec();
    tampered_wire.extend_from_slice(&[0xa2, 0x06, 0x03, b'n', b'e', b'w']);

    assert_eq!(
        Observation::decode(&tampered_wire),
        Err(ObservationError::ContentHashMismatch)
    );
    Ok(())
}

#[test]
fn mpp_extension_replays_through_the_shared_envelope() -> Result<(), ObservationError> {
    let original = Observation::new(
        Provenance::new("tempo-rpc", 1_790_426_627_000, "mpp-adapter@1")?,
        EvidenceRef::sha256(b"mpp receipt", "application/cbor")?,
        ProtocolObservation::Mpp(MppObservation::new(
            MppEventKey::tempo("api.example.com", "challenge-7")?,
            "charge",
            "2500",
        )?),
    )?;

    assert_eq!(Observation::decode(original.encode())?.id(), original.id());
    match original.protocol() {
        ProtocolObservation::Mpp(payload) => assert_eq!(payload.intent(), "charge"),
        ProtocolObservation::X402(_) => panic!("expected mpp observation"),
    }
    Ok(())
}

#[test]
fn canonical_event_cites_supporting_observations() -> Result<(), ObservationError> {
    let supporting = observation("rpc-primary")?;
    let key = ProtocolEventKey::X402(x402_key()?);

    let event = CanonicalEvent::new(key, &[supporting])?;

    assert!(event.id().starts_with("event:x402:sha256:"));
    assert_eq!(event.supporting_observation_ids().len(), 1);
    Ok(())
}

#[test]
fn protobuf_replay_preserves_the_canonical_event() -> Result<(), ObservationError> {
    let supporting = observation("rpc-primary")?;
    let key = ProtocolEventKey::X402(x402_key()?);
    let original = CanonicalEvent::new(key, std::slice::from_ref(&supporting))?;
    let encoded = original.encode();

    let replayed = CanonicalEvent::decode(encoded, &[supporting])?;

    assert_eq!(replayed.id(), original.id());
    assert_eq!(
        replayed.supporting_observation_ids(),
        original.supporting_observation_ids()
    );
    assert_eq!(replayed.encode(), encoded);
    Ok(())
}

#[test]
fn canonical_event_decode_rejects_support_from_another_event() -> Result<(), ObservationError> {
    let x402 = observation("rpc-primary")?;
    let event = CanonicalEvent::new(
        ProtocolEventKey::X402(x402_key()?),
        std::slice::from_ref(&x402),
    )?;
    let mpp = Observation::new(
        Provenance::new("tempo-rpc", 1_790_426_627_000, "mpp-adapter@1")?,
        EvidenceRef::sha256(b"mpp receipt", "application/cbor")?,
        ProtocolObservation::Mpp(MppObservation::new(
            MppEventKey::tempo("api.example.com", "challenge-7")?,
            "charge",
            "2500",
        )?),
    )?;
    let mut forged = event.encode().to_vec();
    let support_start = forged
        .windows(x402.id().len())
        .position(|window| window == x402.id().as_bytes())
        .expect("canonical event contains its support id");
    forged[support_start..support_start + x402.id().len()].copy_from_slice(mpp.id().as_bytes());

    assert_eq!(
        CanonicalEvent::decode(&forged, &[mpp]),
        Err(ObservationError::MismatchedSupport)
    );
    Ok(())
}

#[test]
fn canonical_event_rejects_mismatched_support() -> Result<(), ObservationError> {
    let mpp = Observation::new(
        Provenance::new("tempo-rpc", 1_790_426_627_000, "mpp-adapter@1")?,
        EvidenceRef::sha256(b"mpp receipt", "application/cbor")?,
        ProtocolObservation::Mpp(MppObservation::new(
            MppEventKey::tempo("api.example.com", "challenge-7")?,
            "charge",
            "2500",
        )?),
    )?;
    let x402_key = ProtocolEventKey::X402(x402_key()?);

    assert!(CanonicalEvent::new(x402_key, &[mpp]).is_err());
    Ok(())
}
