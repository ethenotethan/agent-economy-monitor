use std::sync::{Arc, Mutex};

use agent_economy_monitor::{
    classify::{
        ClassificationBatch, ClassificationHandler, ClassificationStore, ClassificationWorkerError,
        derive_classification_batch,
    },
    worker::{LeasedJob, WorkerDispatcher, WorkerJobStore, WorkerMode, WorkerStoreError},
};
use async_trait::async_trait;
use sha2::{Digest, Sha256};

fn manifest() -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "buyer_handle_id": "buyer:base:one",
        "run_id": "classification:buyer:base:one",
        "run_version": 1,
        "classifier_version": "buyer-classifier@1",
        "feature_version": "behavior-features@1",
        "window_start_unix_seconds": 100,
        "window_end_unix_seconds": 400,
        "provenance_id": "00000000-0000-0000-0000-000000000050",
        "activities": [
            {
                "settlement_id": "settlement:one",
                "amount_atomic": "600",
                "occurred_at_unix_seconds": 120,
                "protocol": "x402",
                "counterparty": "service:weather",
                "autonomy": "unknown",
                "evidence_id": "evidence:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            }
        ],
        "labels": [
            {
                "id": "core:high-spend",
                "version": 1,
                "kind": "core",
                "metric": "total_spend_atomic",
                "threshold": "500",
                "confidence_bps": 8500,
                "definition_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
            }
        ]
    }))
    .unwrap()
}

#[test]
fn classification_batch_runs_existing_engine_and_preserves_threshold_confidence_and_lineage() {
    let input = manifest();
    let digest = format!("{:x}", Sha256::digest(&input));

    let batch = derive_classification_batch(&digest, &input).expect("valid classification batch");

    assert_eq!(batch.buyer_handle_id(), "buyer:base:one");
    assert_eq!(batch.feature_version(), "behavior-features@1");
    assert_eq!(batch.classifier_version(), "buyer-classifier@1");
    assert_eq!(batch.input_snapshot_hash().len(), 64);
    assert_eq!(batch.features_json()["total_spend_atomic"], "600");
    assert_eq!(batch.labels_json()[0]["threshold"], "500");
    assert_eq!(batch.claims_json()[0]["confidence_bps"], 8500);
    assert_eq!(
        batch.evidence_ids(),
        &["evidence:sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]
    );
    assert_eq!(batch.output_sha256().len(), 64);
}

struct MemoryStore {
    manifest: Vec<u8>,
    committed: Mutex<Vec<String>>,
}

#[async_trait]
impl ClassificationStore for MemoryStore {
    async fn load_input(&self, _lease: &LeasedJob) -> Result<Vec<u8>, ClassificationWorkerError> {
        Ok(self.manifest.clone())
    }

    async fn commit_batch(
        &self,
        _lease: &LeasedJob,
        batch: &ClassificationBatch,
    ) -> Result<Option<String>, ClassificationWorkerError> {
        self.committed
            .lock()
            .unwrap()
            .push(batch.output_sha256().to_owned());
        Ok(Some(batch.output_sha256().to_owned()))
    }
}

#[async_trait]
impl WorkerJobStore for MemoryStore {
    async fn claim(
        &self,
        _mode: WorkerMode,
        _lease_owner: &str,
        _lease_seconds: u64,
    ) -> Result<Option<LeasedJob>, WorkerStoreError> {
        Ok(Some(LeasedJob {
            job_id: "00000000-0000-0000-0000-000000000051".to_owned(),
            mode: WorkerMode::Classify,
            job_kind: "buyer-behavior-v1".to_owned(),
            input_sha256: format!("{:x}", Sha256::digest(&self.manifest)),
            attempt: 1,
            lease_owner: "classifier:test".to_owned(),
            lease_token: "00000000-0000-0000-0000-000000000052".to_owned(),
            collection_admission: None,
        }))
    }

    async fn renew(&self, _lease: &LeasedJob, _lease_seconds: u64) -> Result<(), WorkerStoreError> {
        Ok(())
    }

    async fn complete(
        &self,
        _lease: &LeasedJob,
        output_sha256: &str,
    ) -> Result<(), WorkerStoreError> {
        if self
            .committed
            .lock()
            .unwrap()
            .iter()
            .any(|hash| hash == output_sha256)
        {
            Ok(())
        } else {
            Err(WorkerStoreError::Conflict)
        }
    }

    async fn fail(
        &self,
        _lease: &LeasedJob,
        _error_code: &str,
        _retryable: bool,
    ) -> Result<(), WorkerStoreError> {
        Ok(())
    }
}

#[tokio::test]
async fn classify_worker_claims_runs_persists_and_completes_its_real_job_kind() {
    let store = Arc::new(MemoryStore {
        manifest: manifest(),
        committed: Mutex::new(Vec::new()),
    });
    let dispatcher = WorkerDispatcher::new(store.clone()).register(
        WorkerMode::Classify,
        Arc::new(ClassificationHandler::new(store.clone())),
    );

    let completed = dispatcher
        .run_once(WorkerMode::Classify, "classifier:test")
        .await
        .expect("classification completes");
    assert_eq!(completed.job_id, "00000000-0000-0000-0000-000000000051");
    assert_eq!(store.committed.lock().unwrap().len(), 1);
}
