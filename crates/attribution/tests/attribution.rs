use agent_economy_attribution::{
    AttributionEngine, AttributionError, AttributionLevel, AttributionMethod, PaymentRequirement,
    Settlement, SettlementFinality,
};

fn settlement(requirement_id: Option<&str>) -> Settlement {
    settlement_with_finality(requirement_id, SettlementFinality::Finalized)
}

fn settlement_with_finality(
    requirement_id: Option<&str>,
    finality: SettlementFinality,
) -> Settlement {
    Settlement::new(
        "settlement:base:0xabc",
        "x402",
        "base",
        "USDC",
        "1000",
        "0xmerchant",
        finality,
        requirement_id,
        ["evidence:settlement"],
    )
    .expect("valid settlement")
}

fn requirement(id: &str, service_id: &str, endpoint_id: &str) -> PaymentRequirement {
    PaymentRequirement::new(
        id,
        format!("option:{id}"),
        endpoint_id,
        service_id,
        "x402",
        "base",
        "USDC",
        "1000",
        "0xmerchant",
        [format!("evidence:{id}")],
    )
    .expect("valid payment requirement")
}

#[test]
fn explicit_requirement_match_is_verified_and_cites_both_sides() {
    let engine = AttributionEngine::new("attribution@1").expect("valid engine version");
    let result = engine
        .attribute(
            &settlement(Some("requirement:weather")),
            [requirement(
                "requirement:weather",
                "service:weather",
                "endpoint:forecast",
            )],
        )
        .expect("valid attribution inputs");

    assert_eq!(result.level(), AttributionLevel::Verified);
    assert_eq!(result.method(), AttributionMethod::ExplicitRequirement);
    assert_eq!(
        result.explicit_requirement_id(),
        Some("requirement:weather")
    );
    assert_eq!(result.candidates().len(), 1);
    let edge = &result.candidates()[0];
    assert_eq!(edge.requirement_id(), "requirement:weather");
    assert_eq!(edge.endpoint_id(), "endpoint:forecast");
    assert_eq!(edge.service_id(), "service:weather");
    assert_eq!(edge.confidence_bps(), 10_000);
    assert_eq!(
        edge.evidence_ids(),
        &["evidence:requirement:weather", "evidence:settlement"]
    );
    assert!(edge.id().starts_with("attribution:sha256:"));
}

#[test]
fn unique_exact_catalog_match_is_strong() {
    let result = AttributionEngine::new("attribution@1")
        .expect("valid engine version")
        .attribute(
            &settlement(None),
            [requirement(
                "requirement:weather",
                "service:weather",
                "endpoint:forecast",
            )],
        )
        .expect("valid attribution inputs");

    assert_eq!(result.level(), AttributionLevel::Strong);
    assert_eq!(result.candidates().len(), 1);
    assert_eq!(result.candidates()[0].confidence_bps(), 8_500);
}

#[test]
fn shared_recipient_keeps_all_exact_candidates_weak() {
    let result = AttributionEngine::new("attribution@1")
        .expect("valid engine version")
        .attribute(
            &settlement(None),
            [
                requirement(
                    "requirement:weather",
                    "service:weather",
                    "endpoint:forecast",
                ),
                requirement("requirement:maps", "service:maps", "endpoint:directions"),
            ],
        )
        .expect("valid attribution inputs");

    assert_eq!(result.level(), AttributionLevel::Weak);
    assert_eq!(result.method(), AttributionMethod::SharedExact);
    assert_eq!(result.candidates().len(), 2);
    assert!(
        result
            .candidates()
            .iter()
            .all(|candidate| candidate.confidence_bps() == 5_000)
    );
    assert_eq!(
        result
            .candidates()
            .iter()
            .map(|candidate| candidate.service_id())
            .collect::<Vec<_>>(),
        vec!["service:maps", "service:weather"]
    );
}

#[test]
fn replay_is_identical_across_catalog_input_order() {
    let engine = AttributionEngine::new("attribution@1").expect("valid engine version");
    let weather = requirement(
        "requirement:weather",
        "service:weather",
        "endpoint:forecast",
    );
    let maps = requirement("requirement:maps", "service:maps", "endpoint:directions");

    let forward = engine
        .attribute(&settlement(None), [weather.clone(), maps.clone()])
        .expect("forward replay");
    let reverse = engine
        .attribute(&settlement(None), [maps, weather])
        .expect("reverse replay");

    assert_eq!(forward.state_hash(), reverse.state_hash());
    assert_eq!(forward.input_snapshot_hash(), reverse.input_snapshot_hash());
    assert_eq!(forward.encode(), reverse.encode());
    assert_eq!(forward.engine_version(), "attribution@1");
}

#[test]
fn replay_encoding_has_stable_golden_vectors() {
    let result = AttributionEngine::new("attribution@1")
        .expect("valid engine version")
        .attribute(
            &settlement(None),
            [requirement(
                "requirement:weather",
                "service:weather",
                "endpoint:forecast",
            )],
        )
        .expect("valid attribution inputs");

    assert_eq!(
        result.input_snapshot_hash(),
        "3528c70ac97a99b18cb00274f4caaee398eb4c13d40b71879d9bbe3a960c0b65"
    );
    assert_eq!(
        result.state_hash(),
        "09f33b946816d5d17f7fc2d49ab90c567a5505d5d9a3f4eaeba1de96a1bc591b"
    );
    assert_eq!(
        result.candidates()[0].id(),
        "attribution:sha256:fd944928106a9ff25d29d135f1467c4c6d04aab4d5f6d133e6a8bc174ab5543c"
    );
}

#[test]
fn unmatched_settlement_is_unknown_but_keeps_settlement_evidence() {
    let result = AttributionEngine::new("attribution@1")
        .expect("valid engine version")
        .attribute(&settlement(None), [])
        .expect("valid attribution inputs");

    assert_eq!(result.level(), AttributionLevel::Unknown);
    assert!(result.candidates().is_empty());
    assert_eq!(result.evidence_ids(), &["evidence:settlement"]);
}

#[test]
fn duplicate_requirement_ids_are_rejected() {
    let error = AttributionEngine::new("attribution@1")
        .expect("valid engine version")
        .attribute(
            &settlement(None),
            [
                requirement("requirement:shared", "service:weather", "endpoint:forecast"),
                requirement("requirement:shared", "service:maps", "endpoint:directions"),
            ],
        )
        .expect_err("one requirement id cannot name two catalog subjects");

    assert_eq!(error, AttributionError::DuplicateRequirement);
}

#[test]
fn nonfinalized_settlements_are_rejected() {
    let error = AttributionEngine::new("attribution@1")
        .expect("valid engine version")
        .attribute(
            &settlement_with_finality(None, SettlementFinality::Confirmed),
            [requirement(
                "requirement:weather",
                "service:weather",
                "endpoint:forecast",
            )],
        )
        .expect_err("only finalized settlements are attributable");

    assert_eq!(error, AttributionError::SettlementNotFinalized);
}
