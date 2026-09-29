use agent_economy_contracts::{
    EvidenceRef, Observation, ObservationError, ProtocolObservation, Provenance, X402EventKey,
    X402Observation,
};
use agent_economy_reducer::{
    EvmFinalityEvidence, ExecutionOutcome, FinalityEngine, FinalityStatus, FinalitySubject,
    ObservationRole, Reducer, ReducerError, SolanaCommitment, SolanaFinalityEvidence,
};

fn observation(source: &str, amount: &str) -> Result<Observation, ObservationError> {
    let key = X402EventKey::payment_identifier(
        "pay_123456789012",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "merchant-1:/paid/weather",
    )?;
    Observation::new(
        Provenance::new(source, 1_790_426_627_000, "x402-adapter@1")?,
        EvidenceRef::sha256(format!("{source}:{amount}").as_bytes(), "application/json")?,
        ProtocolObservation::X402(X402Observation::new(key, "USDC", amount)?),
    )
}

fn finality_subject() -> FinalitySubject {
    FinalitySubject::new(
        "event:x402:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "base",
        "0xtx-a",
    )
    .expect("valid finality subject")
}

#[test]
fn independent_observations_and_conflicts_remain_inspectable() -> Result<(), ObservationError> {
    let primary = observation("rpc-primary", "1000")?;
    let secondary = observation("rpc-secondary", "1000")?;
    let conflicting = observation("rpc-conflicting", "2000")?;

    let snapshot = Reducer::new("reducer@1")
        .reduce([conflicting.clone(), secondary.clone(), primary.clone()])
        .expect("valid observations reduce");
    let event = snapshot.events().first().expect("one canonical event");

    assert_eq!(snapshot.events().len(), 1);
    assert_eq!(event.id(), primary.event_key().canonical_id());
    assert_eq!(
        event.observations(),
        &[
            (primary.id().to_owned(), ObservationRole::Supporting),
            (secondary.id().to_owned(), ObservationRole::Supporting),
            (conflicting.id().to_owned(), ObservationRole::Conflicting),
        ]
    );
    assert_eq!(event.conflicts().len(), 1);
    assert_eq!(event.conflicts()[0].observation_ids(), &[conflicting.id()]);
    Ok(())
}

#[test]
fn replay_is_identical_across_input_order() -> Result<(), ObservationError> {
    let primary = observation("rpc-primary", "1000")?;
    let secondary = observation("rpc-secondary", "1000")?;
    let conflicting = observation("rpc-conflicting", "2000")?;
    let reducer = Reducer::new("reducer@1");

    let forward = reducer
        .reduce([primary.clone(), secondary.clone(), conflicting.clone()])
        .expect("forward replay");
    let reverse = reducer
        .reduce([conflicting, secondary, primary])
        .expect("reverse replay");

    assert_eq!(forward.state_hash(), reverse.state_hash());
    assert_eq!(forward.encode(), reverse.encode());
    Ok(())
}

#[test]
fn evm_finality_progression_preserves_reorg_history() {
    let engine = FinalityEngine::evm(5);
    let subject = finality_subject();
    let evidence = [
        EvmFinalityEvidence::new(
            subject.clone(),
            "rpc-primary",
            "provenance:rpc-primary",
            "evidence:rpc-primary",
            100,
            10,
            "0xaaa",
            "0xaaa",
            10,
            0,
            ExecutionOutcome::Succeeded,
        ),
        EvmFinalityEvidence::new(
            subject.clone(),
            "rpc-primary",
            "provenance:rpc-primary",
            "evidence:rpc-primary",
            200,
            10,
            "0xaaa",
            "0xaaa",
            15,
            0,
            ExecutionOutcome::Succeeded,
        ),
        EvmFinalityEvidence::new(
            subject.clone(),
            "rpc-primary",
            "provenance:rpc-primary",
            "evidence:rpc-primary",
            300,
            10,
            "0xaaa",
            "0xaaa",
            20,
            10,
            ExecutionOutcome::Succeeded,
        ),
        EvmFinalityEvidence::new(
            subject.clone(),
            "rpc-primary",
            "provenance:rpc-primary",
            "evidence:rpc-primary",
            400,
            10,
            "0xaaa",
            "0xbbb",
            21,
            10,
            ExecutionOutcome::Succeeded,
        ),
    ];

    let timeline = engine
        .reconcile_for(&subject, evidence)
        .expect("valid EVM finality evidence");

    assert_eq!(
        timeline
            .updates()
            .iter()
            .map(|update| update.status())
            .collect::<Vec<_>>(),
        vec![
            FinalityStatus::Observed,
            FinalityStatus::Confirmed,
            FinalityStatus::Finalized,
            FinalityStatus::Orphaned,
        ]
    );
    assert_eq!(timeline.current(), Some(FinalityStatus::Orphaned));
    assert!(timeline.conflicts().is_empty());
}

#[test]
fn evm_finality_replays_reinclusion_after_orphaning_in_any_input_order() {
    let engine = FinalityEngine::evm(5);
    let subject = finality_subject();
    let observed_in_block_a = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:observed-a",
        "evidence:observed-a",
        100,
        10,
        "0xaaa",
        "0xaaa",
        10,
        0,
        ExecutionOutcome::Succeeded,
    );
    let orphaned_from_block_a = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:orphaned-a",
        "evidence:orphaned-a",
        200,
        10,
        "0xaaa",
        "0xbbb",
        16,
        0,
        ExecutionOutcome::Succeeded,
    );
    let confirmed_in_block_b = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:confirmed-b",
        "evidence:confirmed-b",
        300,
        12,
        "0xccc",
        "0xccc",
        17,
        0,
        ExecutionOutcome::Succeeded,
    );

    let forward = engine
        .reconcile_for(
            &subject,
            [
                observed_in_block_a.clone(),
                orphaned_from_block_a.clone(),
                confirmed_in_block_b.clone(),
            ],
        )
        .expect("re-inclusion after orphaning is valid");
    let reverse = engine
        .reconcile_for(
            &subject,
            [
                confirmed_in_block_b,
                orphaned_from_block_a,
                observed_in_block_a,
            ],
        )
        .expect("replay order does not change re-inclusion");

    assert_eq!(forward, reverse);
    assert_eq!(
        forward
            .updates()
            .iter()
            .map(|update| update.status())
            .collect::<Vec<_>>(),
        vec![
            FinalityStatus::Observed,
            FinalityStatus::Orphaned,
            FinalityStatus::Confirmed,
        ]
    );
    assert_eq!(forward.current(), Some(FinalityStatus::Confirmed));
    assert!(forward.conflicts().is_empty());
}

#[test]
fn evm_delayed_orphan_for_old_inclusion_preserves_newer_reinclusion() {
    let engine = FinalityEngine::evm(5);
    let subject = finality_subject();
    let observed_in_block_a = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:observed-a",
        "evidence:observed-a",
        100,
        10,
        "0xaaa",
        "0xaaa",
        10,
        0,
        ExecutionOutcome::Succeeded,
    );
    let confirmed_in_block_b = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:confirmed-b",
        "evidence:confirmed-b",
        200,
        12,
        "0xccc",
        "0xccc",
        17,
        0,
        ExecutionOutcome::Succeeded,
    );
    let delayed_orphan_from_block_a = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:orphaned-a",
        "evidence:orphaned-a",
        300,
        10,
        "0xaaa",
        "0xbbb",
        18,
        0,
        ExecutionOutcome::Succeeded,
    );

    let forward = engine
        .reconcile_for(
            &subject,
            [
                observed_in_block_a.clone(),
                confirmed_in_block_b.clone(),
                delayed_orphan_from_block_a.clone(),
            ],
        )
        .expect("delayed orphan evidence remains valid history");
    let reverse = engine
        .reconcile_for(
            &subject,
            [
                delayed_orphan_from_block_a,
                confirmed_in_block_b,
                observed_in_block_a,
            ],
        )
        .expect("replay order does not change delayed-orphan handling");

    assert_eq!(forward, reverse);
    assert_eq!(
        forward
            .updates()
            .iter()
            .map(|update| update.status())
            .collect::<Vec<_>>(),
        vec![
            FinalityStatus::Observed,
            FinalityStatus::Confirmed,
            FinalityStatus::Orphaned,
        ]
    );
    assert_eq!(forward.current(), Some(FinalityStatus::Confirmed));
    assert!(forward.conflicts().is_empty());
}

#[test]
fn solana_finality_records_reverted_execution_and_conflicting_regression() {
    let engine = FinalityEngine::solana();
    let subject = finality_subject();
    let evidence = [
        SolanaFinalityEvidence::new(
            subject.clone(),
            "rpc-primary",
            "provenance:rpc-primary",
            "evidence:rpc-primary",
            100,
            42,
            "block-a",
            "block-a",
            SolanaCommitment::Finalized,
            ExecutionOutcome::Succeeded,
        ),
        SolanaFinalityEvidence::new(
            subject.clone(),
            "rpc-secondary",
            "provenance:rpc-secondary",
            "evidence:rpc-secondary",
            200,
            42,
            "block-a",
            "block-a",
            SolanaCommitment::Confirmed,
            ExecutionOutcome::Succeeded,
        ),
        SolanaFinalityEvidence::new(
            subject.clone(),
            "rpc-primary",
            "provenance:rpc-primary",
            "evidence:rpc-primary",
            300,
            42,
            "block-a",
            "block-a",
            SolanaCommitment::Finalized,
            ExecutionOutcome::Reverted,
        ),
    ];

    let timeline = engine
        .reconcile_for(&subject, evidence)
        .expect("valid Solana evidence");

    assert_eq!(
        timeline
            .updates()
            .iter()
            .map(|update| update.status())
            .collect::<Vec<_>>(),
        vec![FinalityStatus::Finalized, FinalityStatus::Reverted]
    );
    assert_eq!(timeline.conflicts().len(), 1);
    assert_eq!(
        timeline.conflicts()[0].update().source_id(),
        "rpc-secondary"
    );
    assert_eq!(
        timeline.conflicts()[0].update().status(),
        FinalityStatus::Confirmed
    );
}

#[test]
fn finality_replay_is_identical_when_provider_timestamps_tie() {
    let engine = FinalityEngine::evm(5);
    let subject = finality_subject();
    let confirmed = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:rpc-primary",
        "evidence:rpc-primary",
        100,
        10,
        "0xaaa",
        "0xaaa",
        15,
        0,
        ExecutionOutcome::Succeeded,
    );
    let finalized = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:rpc-primary",
        "evidence:rpc-primary",
        100,
        10,
        "0xaaa",
        "0xaaa",
        20,
        10,
        ExecutionOutcome::Succeeded,
    );

    let forward = engine
        .reconcile_for(&subject, [confirmed.clone(), finalized.clone()])
        .expect("forward replay");
    let reverse = engine
        .reconcile_for(&subject, [finalized, confirmed])
        .expect("reverse replay");

    assert_eq!(forward, reverse);
}

#[test]
fn finality_reconciliation_rejects_cross_event_evidence() {
    let expected = FinalitySubject::new(
        "event:x402:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "base",
        "0xtx-a",
    )
    .expect("valid expected subject");
    let other = FinalitySubject::new(
        "event:x402:sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "ethereum",
        "0xtx-b",
    )
    .expect("valid other subject");
    let evidence = EvmFinalityEvidence::new(
        other,
        "rpc-primary",
        "provenance:rpc-primary",
        "evidence:rpc-primary",
        100,
        10,
        "0xaaa",
        "0xaaa",
        15,
        0,
        ExecutionOutcome::Succeeded,
    );

    assert_eq!(
        FinalityEngine::evm(5).reconcile_for(&expected, [evidence]),
        Err(ReducerError::FinalitySubjectMismatch)
    );
}

#[test]
fn corroborating_finality_assertions_are_preserved() {
    let subject = finality_subject();
    let evidence = [
        EvmFinalityEvidence::new(
            subject.clone(),
            "rpc-primary",
            "provenance:rpc-primary",
            "evidence:rpc-primary",
            100,
            10,
            "0xaaa",
            "0xaaa",
            15,
            0,
            ExecutionOutcome::Succeeded,
        ),
        EvmFinalityEvidence::new(
            subject.clone(),
            "rpc-secondary",
            "provenance:rpc-secondary",
            "evidence:rpc-secondary",
            101,
            10,
            "0xaaa",
            "0xaaa",
            15,
            0,
            ExecutionOutcome::Succeeded,
        ),
    ];

    let timeline = FinalityEngine::evm(5)
        .reconcile_for(&subject, evidence)
        .expect("corroborating evidence");

    assert_eq!(timeline.updates().len(), 2);
    assert_eq!(timeline.current(), Some(FinalityStatus::Confirmed));
}

#[test]
fn finality_state_hash_commits_to_policy_basis() {
    let subject = finality_subject();
    let evidence = |latest_block, evidence_id| {
        EvmFinalityEvidence::new(
            subject.clone(),
            "rpc-primary",
            "provenance:rpc-primary",
            evidence_id,
            100,
            10,
            "0xaaa",
            "0xaaa",
            latest_block,
            0,
            ExecutionOutcome::Succeeded,
        )
    };

    let first = FinalityEngine::evm(5)
        .reconcile_for(&subject, [evidence(15, "evidence:first")])
        .expect("first replay");
    let changed = FinalityEngine::evm(5)
        .reconcile_for(&subject, [evidence(16, "evidence:first")])
        .expect("changed replay");
    let changed_evidence = FinalityEngine::evm(5)
        .reconcile_for(&subject, [evidence(15, "evidence:second")])
        .expect("changed evidence replay");

    assert_ne!(first.state_hash(), changed.state_hash());
    assert_ne!(first.encode(), changed.encode());
    assert_ne!(first.state_hash(), changed_evidence.state_hash());
}

#[test]
fn evm_finality_rejects_positions_that_postgres_cannot_persist() {
    let subject = finality_subject();
    let evidence = EvmFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:rpc-primary",
        "evidence:rpc-primary",
        100,
        i64::MAX as u64 + 1,
        "0xaaa",
        "0xaaa",
        i64::MAX as u64 + 1,
        0,
        ExecutionOutcome::Succeeded,
    );

    assert_eq!(
        FinalityEngine::evm(5).reconcile_for(&subject, [evidence]),
        Err(ReducerError::InvalidFinalityEvidence("block range"))
    );
}

#[test]
fn solana_finality_rejects_slots_that_postgres_cannot_persist() {
    let subject = finality_subject();
    let evidence = SolanaFinalityEvidence::new(
        subject.clone(),
        "rpc-primary",
        "provenance:rpc-primary",
        "evidence:rpc-primary",
        100,
        i64::MAX as u64 + 1,
        "block-a",
        "block-a",
        SolanaCommitment::Processed,
        ExecutionOutcome::Succeeded,
    );

    assert_eq!(
        FinalityEngine::solana().reconcile_for(&subject, [evidence]),
        Err(ReducerError::InvalidFinalityEvidence("slot"))
    );
}
