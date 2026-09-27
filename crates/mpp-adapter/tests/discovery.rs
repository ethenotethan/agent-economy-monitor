use agent_economy_adapter_api::ProtocolAdapter;
use agent_economy_contracts::{Observation, ProtocolObservation};
use agent_economy_mpp_adapter::{MppAdapterError, MppDiscoveryAdapter};

const OBSERVED_AT: i64 = 1_790_426_627_000;

fn adapter(service_id: &str, source_id: &str) -> MppDiscoveryAdapter {
    MppDiscoveryAdapter::new(service_id, source_id, OBSERVED_AT, "mpp-discovery@draft-01")
        .expect("fixture adapter configuration is valid")
}

#[test]
fn golden_openapi_versions_emit_replayable_mpp_discovery_observations() {
    let fixtures = [
        (
            "openapi-3.1-multi.json",
            "service:mpp-weather",
            "https://weather.example/openapi.json",
            2,
        ),
        (
            "openapi-3.0-single.json",
            "service:mpp-search",
            "https://search.example/openapi.json",
            1,
        ),
    ];

    for (fixture, service_id, source_id, expected_count) in fixtures {
        let evidence = std::fs::read(format!("tests/fixtures/{fixture}"))
            .expect("golden discovery fixture exists");
        let observed = adapter(service_id, source_id)
            .observe(&evidence)
            .expect("supported golden fixture parses");
        let deterministic_replay = adapter(service_id, source_id)
            .observe(&evidence)
            .expect("same evidence parses again");

        assert_eq!(observed.len(), expected_count);
        assert_eq!(deterministic_replay, observed);
        if fixture == "openapi-3.1-multi.json" {
            for observation in &observed {
                let ProtocolObservation::MppDiscovery(discovery) = observation.protocol() else {
                    panic!("expected an MPP discovery observation");
                };
                assert!(
                    !discovery
                        .raw_payment_info_json()
                        .windows(b"\"offers\"".len())
                        .any(|window| window == b"\"offers\"")
                );
            }
        }
        for observation in observed {
            let replayed = Observation::decode(observation.encode()).expect("observation replays");
            assert_eq!(replayed, observation);
            assert_eq!(replayed.provenance().source_id(), source_id);
            match replayed.protocol() {
                ProtocolObservation::MppDiscovery(discovery) => {
                    assert!(matches!(discovery.openapi_version(), "3.0.3" | "3.1.0"));
                    assert!(!discovery.service_title().is_empty());
                    assert!(discovery.raw_payment_info_json().starts_with(b"{"));
                    assert!(
                        discovery
                            .raw_payment_info_json()
                            .windows(b"\"method\"".len())
                            .any(|window| window == b"\"method\"")
                    );
                    assert!(
                        discovery
                            .event_key()
                            .canonical_id()
                            .starts_with("event:mpp:sha256:")
                    );
                }
                _ => panic!("expected an MPP discovery observation"),
            }
        }
    }
}

#[test]
fn malformed_payment_metadata_quarantines_the_whole_document_without_raw_data() {
    let evidence =
        std::fs::read("tests/fixtures/malformed-offer.json").expect("malformed fixture exists");

    let error = adapter("service:broken", "https://broken.example/openapi.json")
        .observe(&evidence)
        .expect_err("one malformed offer quarantines instead of partially emitting");

    assert!(matches!(error, MppAdapterError::InvalidOffer { .. }));
    let display = error.to_string();
    assert!(!display.contains("/mixed"));
    assert!(!display.contains("x-payment-info"));
    assert!(!display.contains("01"));
}

#[test]
fn malformed_openapi_version_is_quarantined() {
    let mut evidence = std::fs::read("tests/fixtures/openapi-3.1-multi.json")
        .expect("golden discovery fixture exists");
    let version = evidence
        .windows(b"3.1.0".len())
        .position(|window| window == b"3.1.0")
        .expect("fixture contains its version");
    evidence[version..version + b"3.1.0".len()].copy_from_slice(b"3.1.x");

    let error = adapter(
        "service:mpp-weather",
        "https://weather.example/openapi.json",
    )
    .observe(&evidence)
    .expect_err("malformed OpenAPI version is quarantined");

    assert_eq!(error, MppAdapterError::UnsupportedOpenApi);
}

#[test]
fn duplicate_json_members_are_quarantined_as_ambiguous_evidence() {
    let duplicate_offer = br#"{
      "openapi":"3.1.0",
      "info":{"title":"Duplicate offer","version":"1"},
      "paths":{"/paid":{"get":{
        "x-payment-info":{"intent":"charge","intent":"session","method":"tempo","amount":"1"},
        "responses":{"402":{"description":"Payment Required"}}
      }}}
    }"#;
    let duplicate_path = br#"{
      "openapi":"3.1.0",
      "info":{"title":"Duplicate path","version":"1"},
      "paths":{
        "/paid":{"get":{"responses":{"200":{"description":"Free"}}}},
        "/paid":{"get":{
          "x-payment-info":{"intent":"charge","method":"tempo","amount":"1"},
          "responses":{"402":{"description":"Payment Required"}}
        }}
      }
    }"#;

    for evidence in [duplicate_offer.as_slice(), duplicate_path.as_slice()] {
        let error = adapter(
            "service:duplicate",
            "https://duplicate.example/openapi.json",
        )
        .observe(evidence)
        .expect_err("duplicate JSON members are ambiguous");
        assert_eq!(error, MppAdapterError::InvalidJson);
    }
}
