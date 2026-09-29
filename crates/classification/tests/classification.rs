use agent_economy_classification::{
    Activity, AutonomySignal, BehavioralClassifier, ClaimStatus, FeatureEngine, FeatureMetric,
    LabelDefinition, LabelKind, Protocol, Rule, measure_drift,
};

fn activity(
    amount_atomic: u128,
    occurred_at: i64,
    protocol: Protocol,
    counterparty: &str,
    autonomy: AutonomySignal,
    evidence_id: &str,
) -> Activity {
    Activity::new(
        amount_atomic,
        occurred_at,
        protocol,
        counterparty,
        autonomy,
        evidence_id,
    )
    .expect("valid activity")
}

#[test]
fn computes_all_feature_families_deterministically() {
    let engine = FeatureEngine::new("behavior-features@1").expect("valid feature version");
    let first = activity(
        200,
        120,
        Protocol::Mpp,
        "service:maps",
        AutonomySignal::VerifiedAgent,
        "evidence:b",
    );
    let second = activity(
        100,
        60,
        Protocol::X402,
        "service:weather",
        AutonomySignal::Unknown,
        "evidence:a",
    );
    let third = activity(
        300,
        240,
        Protocol::X402,
        "service:weather",
        AutonomySignal::VerifiedAgent,
        "evidence:c",
    );

    let forward = engine
        .compute(
            "buyer:one",
            0,
            300,
            [first.clone(), second.clone(), third.clone()],
        )
        .expect("valid feature window");
    let reverse = engine
        .compute("buyer:one", 0, 300, [third, second, first])
        .expect("valid feature window");

    assert_eq!(forward, reverse);
    assert_eq!(forward.total_spend_atomic(), 600);
    assert_eq!(forward.payment_count(), 3);
    assert_eq!(forward.median_cadence_seconds(), Some(90));
    assert_eq!(forward.protocol_count(Protocol::X402), 2);
    assert_eq!(forward.protocol_count(Protocol::Mpp), 1);
    assert_eq!(forward.unique_counterparties(), 2);
    assert_eq!(forward.autonomous_count(), 2);
    assert_eq!(forward.autonomy_observed_count(), 2);
    assert_eq!(forward.feature_version(), "behavior-features@1");
    assert_eq!(forward.window(), (0, 300));
    assert_eq!(
        forward.evidence_ids(),
        &["evidence:a", "evidence:b", "evidence:c"]
    );
    assert_eq!(forward.input_snapshot_hash().len(), 64);
}

#[test]
fn emits_versioned_core_and_extension_labels_as_inferred_claims() {
    let snapshot = FeatureEngine::new("behavior-features@1")
        .expect("valid feature version")
        .compute(
            "buyer:one",
            0,
            300,
            [
                activity(
                    200,
                    60,
                    Protocol::X402,
                    "service:weather",
                    AutonomySignal::VerifiedAgent,
                    "evidence:a",
                ),
                activity(
                    400,
                    120,
                    Protocol::Mpp,
                    "service:maps",
                    AutonomySignal::VerifiedAgent,
                    "evidence:b",
                ),
                activity(
                    100,
                    240,
                    Protocol::X402,
                    "service:weather",
                    AutonomySignal::Unknown,
                    "evidence:c",
                ),
            ],
        )
        .expect("valid snapshot");
    let classifier = BehavioralClassifier::new(
        "buyer-classifier@1",
        [
            LabelDefinition::new(
                "core:high-frequency",
                1,
                LabelKind::Core,
                Rule::at_least(FeatureMetric::PaymentCount, 3),
            )
            .expect("valid core label"),
            LabelDefinition::new(
                "local:high-spend",
                7,
                LabelKind::Extension,
                Rule::at_least(FeatureMetric::TotalSpendAtomic, 500),
            )
            .expect("valid extension label"),
        ],
    )
    .expect("valid classifier");

    let result = classifier.classify(&snapshot);

    assert_eq!(result.classifier_version(), "buyer-classifier@1");
    assert_eq!(result.feature_version(), "behavior-features@1");
    assert_eq!(result.window(), (0, 300));
    assert_eq!(result.input_snapshot_hash(), snapshot.input_snapshot_hash());
    assert_eq!(result.label_set_hash().len(), 64);
    assert_eq!(result.state_hash().len(), 64);
    assert_eq!(result.claims().len(), 2);
    assert_eq!(result.claims()[0].label_id(), "core:high-frequency");
    assert_eq!(result.claims()[0].label_version(), 1);
    assert_eq!(result.claims()[0].label_kind(), LabelKind::Core);
    assert_eq!(result.claims()[0].status(), ClaimStatus::Inferred);
    assert_eq!(result.claims()[1].label_id(), "local:high-spend");
    assert_eq!(result.claims()[1].label_version(), 7);
    assert_eq!(result.claims()[1].label_kind(), LabelKind::Extension);
    assert_eq!(result.claims()[1].status(), ClaimStatus::Inferred);
}

#[test]
fn measures_label_and_input_drift_between_replays() {
    let engine = FeatureEngine::new("behavior-features@1").expect("valid feature version");
    let baseline_snapshot = engine
        .compute(
            "buyer:one",
            0,
            300,
            [activity(
                600,
                60,
                Protocol::X402,
                "service:weather",
                AutonomySignal::Unknown,
                "evidence:a",
            )],
        )
        .expect("baseline snapshot");
    let replay_snapshot = engine
        .compute(
            "buyer:one",
            0,
            300,
            [activity(
                400,
                60,
                Protocol::X402,
                "service:weather",
                AutonomySignal::Unknown,
                "evidence:b",
            )],
        )
        .expect("replay snapshot");
    let label = || {
        LabelDefinition::new(
            "local:high-spend",
            7,
            LabelKind::Extension,
            Rule::at_least(FeatureMetric::TotalSpendAtomic, 500),
        )
        .expect("valid label")
    };
    let classifier =
        BehavioralClassifier::new("buyer-classifier@1", [label()]).expect("valid classifier");
    let baseline = classifier.classify(&baseline_snapshot);
    let replay = classifier.classify(&replay_snapshot);

    let drift = measure_drift(&baseline, &replay).expect("comparable replay windows");

    assert!(drift.input_changed());
    assert!(drift.added_labels().is_empty());
    assert_eq!(drift.removed_labels(), &["local:high-spend@7"]);
    assert_eq!(drift.label_churn_count(), 1);
    assert_eq!(drift.baseline_state_hash(), baseline.state_hash());
    assert_eq!(drift.replay_state_hash(), replay.state_hash());
}

#[test]
fn cadence_handles_the_full_supported_timestamp_range_without_overflow() {
    let snapshot = FeatureEngine::new("behavior-features@1")
        .expect("valid feature version")
        .compute(
            "buyer:one",
            i64::MIN,
            i64::MAX,
            [
                activity(
                    1,
                    i64::MIN,
                    Protocol::X402,
                    "service:weather",
                    AutonomySignal::Unknown,
                    "evidence:a",
                ),
                activity(
                    1,
                    i64::MAX - 1,
                    Protocol::X402,
                    "service:weather",
                    AutonomySignal::Unknown,
                    "evidence:b",
                ),
            ],
        )
        .expect("full-range feature window");

    assert_eq!(snapshot.median_cadence_seconds(), Some(u64::MAX - 1));
}

#[test]
fn classifier_rejects_an_empty_label_set_that_cannot_be_persisted() {
    let error = BehavioralClassifier::new("buyer-classifier@1", [])
        .expect_err("classification runs must bind a complete label set");

    assert_eq!(
        error,
        agent_economy_classification::ClassificationError::EmptyLabelSet
    );
}
