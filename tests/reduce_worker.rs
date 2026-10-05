use std::sync::{Arc, Mutex};

use agent_economy_contracts::{
    EvidenceRef, Observation, ProtocolObservation, Provenance, X402EventKey, X402Observation,
};
use agent_economy_monitor::{
    reduce::{
        ReductionBatch, ReductionCommitStore, ReductionError, ReductionHandler, ReductionInputStore,
    },
    worker::{LeasedJob, WorkerHandler, WorkerMode},
};
use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};

struct StaticInput(Vec<u8>);

#[async_trait]
impl ReductionInputStore for StaticInput {
    async fn load(&self, _job: &LeasedJob) -> Result<Vec<u8>, ReductionError> {
        Ok(self.0.clone())
    }
}

#[derive(Default)]
struct RecordingCommit(Mutex<Vec<ReductionBatch>>);

#[async_trait]
impl ReductionCommitStore for RecordingCommit {
    async fn commit(&self, _job: &LeasedJob, batch: ReductionBatch) -> Result<(), ReductionError> {
        self.0.lock().unwrap().push(batch);
        Ok(())
    }
}

fn observation(source: &str, amount: &str) -> Observation {
    let key = X402EventKey::payment_identifier(
        "pay_123456789012",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "merchant-1:/paid/weather",
    )
    .unwrap();
    Observation::new(
        Provenance::new(source, 1_790_426_627_000, "x402-adapter@1").unwrap(),
        EvidenceRef::sha256(format!("{source}:{amount}").as_bytes(), "application/json").unwrap(),
        ProtocolObservation::X402(X402Observation::new(key, "USDC", amount).unwrap()),
    )
    .unwrap()
}

fn manifest(observations: &[Observation]) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "reducer_version": "reducer@1",
        "chain_scope": "base",
        "start_height": 42,
        "end_height": 42,
        "observations": observations.iter().map(|observation| serde_json::json!({
            "source_id": observation.provenance().source_id(),
            "observation_id": observation.id(),
            "height": 42,
            "encoded_base64": STANDARD.encode(observation.encode()),
        })).collect::<Vec<_>>(),
        "finality": [],
        "settlements": []
    }))
    .unwrap()
}

fn full_manifest(observation: &Observation) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "reducer_version": "reducer@1",
        "attribution_version": "attribution@1",
        "chain_scope": "base",
        "start_height": 42,
        "end_height": 42,
        "observations": [{
            "source_id": observation.provenance().source_id(),
            "observation_id": observation.id(),
            "height": 42,
            "encoded_base64": STANDARD.encode(observation.encode()),
        }],
        "finality": [{
            "kind": "evm",
            "canonical_event_id": observation.event_key().canonical_id(),
            "transaction_id": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "source_id": observation.provenance().source_id(),
            "provenance_id": "provenance-1",
            "evidence_id": format!("sha256:{}", observation.evidence().digest()),
            "asserted_at_unix_ms": 1_790_426_628_000_i64,
            "block_number": 42,
            "block_hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "canonical_block_hash": "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "latest_block": 70,
            "finalized_block": 60,
            "execution": "succeeded",
            "confirmations": 12
        }],
        "settlements": [{
            "canonical_event_id": observation.event_key().canonical_id(),
            "transaction_id": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "settled_at_unix_ms": 1_790_426_628_000_i64,
            "source_id": observation.provenance().source_id(),
            "provenance_id": "provenance-1",
            "settlement": {
                "id": "settlement:x402:base:0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
                "protocol": "x402", "network": "base", "asset": "USDC",
                "amount_atomic": "1000",
                "pay_to": "0x1111111111111111111111111111111111111111",
                "finality": "finalized", "requirement_id": "requirement-1",
                "evidence_ids": [format!("sha256:{}", observation.evidence().digest())]
            },
            "requirements": [{
                "id": "requirement-1", "payment_option_id": "payment-option-1",
                "endpoint_id": "endpoint-1", "service_id": "service-1",
                "protocol": "x402", "network": "base", "asset": "USDC",
                "amount_atomic": "1000",
                "pay_to": "0x1111111111111111111111111111111111111111",
                "evidence_ids": [format!("sha256:{}", observation.evidence().digest())]
            }]
        }]
    }))
    .unwrap()
}

fn job(input: &[u8]) -> LeasedJob {
    LeasedJob {
        job_id: "00000000-0000-0000-0000-000000000049".into(),
        mode: WorkerMode::Reduce,
        job_kind: "canonical-observation-range-v1".into(),
        input_sha256: format!("{:x}", Sha256::digest(input)),
        attempt: 1,
        lease_owner: "reduce-test".into(),
        lease_token: "00000000-0000-0000-0000-000000000149".into(),
        collection_admission: None,
    }
}

#[tokio::test]
async fn reduce_handler_reconciles_immutable_observations_and_commits_one_batch() {
    let primary = observation("rpc-primary", "1000");
    let corroborating = observation("rpc-secondary", "1000");
    let conflicting = observation("rpc-conflicting", "2000");
    let input = manifest(&[conflicting, corroborating, primary]);
    let commit = Arc::new(RecordingCommit::default());
    let handler = ReductionHandler::new(Arc::new(StaticInput(input.clone())), commit.clone());

    let result = handler.process(&job(&input)).await.unwrap();

    let batches = commit.0.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].events().len(), 1);
    assert_eq!(batches[0].events()[0].supporting_observation_ids().len(), 2);
    assert_eq!(
        batches[0].events()[0].conflicting_observation_ids().len(),
        1
    );
    assert_eq!(result.output_sha256(), batches[0].output_sha256());
}

#[tokio::test]
async fn reduce_handler_fails_closed_on_wrong_mode_kind_or_manifest_digest() {
    let input = manifest(&[observation("rpc-primary", "1000")]);
    let commit = Arc::new(RecordingCommit::default());
    let handler = ReductionHandler::new(Arc::new(StaticInput(input.clone())), commit.clone());

    let mut wrong_kind = job(&input);
    wrong_kind.job_kind = "anything".into();
    assert_eq!(
        handler.process(&wrong_kind).await.unwrap_err().code(),
        "invalid_reduce_job"
    );

    let mut wrong_digest = job(&input);
    wrong_digest.input_sha256 = "0".repeat(64);
    assert_eq!(
        handler.process(&wrong_digest).await.unwrap_err().code(),
        "invalid_reduce_input"
    );
    assert!(commit.0.lock().unwrap().is_empty());
}

#[tokio::test]
async fn reduce_handler_runs_finality_and_attribution_engines() {
    let observation = observation("rpc-primary", "1000");
    let input = full_manifest(&observation);
    let commit = Arc::new(RecordingCommit::default());
    let handler = ReductionHandler::new(Arc::new(StaticInput(input.clone())), commit.clone());

    handler.process(&job(&input)).await.unwrap();

    let batches = commit.0.lock().unwrap();
    assert_eq!(batches[0].finality_updates().len(), 1);
    assert_eq!(batches[0].attributions().len(), 1);
    assert_eq!(batches[0].attributions()[0].candidate_count(), 1);
}

#[tokio::test]
async fn reduce_handler_preserves_rejected_finality_regressions_as_conflicts() {
    let observation = observation("rpc-primary", "1000");
    let mut manifest_json: serde_json::Value =
        serde_json::from_slice(&full_manifest(&observation)).unwrap();
    let mut regression = manifest_json["finality"][0].clone();
    regression["asserted_at_unix_ms"] = serde_json::json!(1_790_426_629_000_i64);
    regression["latest_block"] = serde_json::json!(42);
    regression["finalized_block"] = serde_json::json!(0);
    regression["evidence_id"] = serde_json::json!(
        "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    );
    manifest_json["finality"]
        .as_array_mut()
        .unwrap()
        .push(regression);
    let input = serde_json::to_vec(&manifest_json).unwrap();
    let commit = Arc::new(RecordingCommit::default());
    let handler = ReductionHandler::new(Arc::new(StaticInput(input.clone())), commit.clone());

    handler.process(&job(&input)).await.unwrap();

    let batches = commit.0.lock().unwrap();
    let finality = batches[0].finality_json();
    assert_eq!(finality.as_array().unwrap().len(), 2);
    assert_eq!(
        finality
            .as_array()
            .unwrap()
            .iter()
            .filter(|update| update["accepted"] == false)
            .count(),
        1
    );
    assert_eq!(finality[1]["current_status"], "finalized");
}
