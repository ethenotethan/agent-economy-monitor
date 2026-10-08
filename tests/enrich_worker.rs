use std::sync::{Arc, Mutex};

use agent_economy_monitor::{
    enrich::{
        ArchivedHistoryEvidence, EnrichmentBatch, EnrichmentEvidenceArchive, EnrichmentHandler,
        EnrichmentJobStore, EnrichmentTransport, EnrichmentWorkerError,
    },
    worker::{LeasedJob, WorkerHandler, WorkerMode},
};
use agent_economy_rpc_collector::{BuyerHistoryTarget, RawRpcResponse};
use async_trait::async_trait;
use sha2::{Digest, Sha256};

struct OnePageTransport;

#[async_trait]
impl EnrichmentTransport for OnePageTransport {
    async fn fetch(
        &mut self,
        _target: &BuyerHistoryTarget,
        cursor: Option<&str>,
    ) -> Result<RawRpcResponse, EnrichmentWorkerError> {
        assert_eq!(cursor, None);
        RawRpcResponse::try_new(
            200,
            br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","blockNum":"0x10","from":"0x1111111111111111111111111111111111111111"}]}}"#.to_vec(),
        )
        .map_err(|_| EnrichmentWorkerError::Unavailable)
    }
}

struct MemoryArchive;

#[async_trait]
impl EnrichmentEvidenceArchive for MemoryArchive {
    async fn archive(
        &self,
        _observed_date: &str,
        body: &[u8],
    ) -> Result<ArchivedHistoryEvidence, EnrichmentWorkerError> {
        let sha256 = format!("{:x}", Sha256::digest(body));
        Ok(ArchivedHistoryEvidence::new(
            format!(
                "evidence/alchemy-history/2026-10-07/sha256/{}/{}",
                &sha256[..2],
                sha256
            ),
            body.to_vec(),
        ))
    }
}

struct MemoryStore {
    input: Vec<u8>,
    committed: Mutex<Vec<EnrichmentBatch>>,
}

#[async_trait]
impl EnrichmentJobStore for MemoryStore {
    async fn load_input(&self, _job: &LeasedJob) -> Result<Vec<u8>, EnrichmentWorkerError> {
        Ok(self.input.clone())
    }

    async fn commit_batch(
        &self,
        _job: &LeasedJob,
        batch: &EnrichmentBatch,
    ) -> Result<bool, EnrichmentWorkerError> {
        self.committed.lock().unwrap().push(batch.clone());
        Ok(true)
    }
}

#[tokio::test]
async fn enrich_worker_archives_raw_history_before_committing_finalized_records() {
    let input = serde_json::to_vec(&serde_json::json!({
        "schema_version": 1,
        "namespace_id": "00000000-0000-0000-0000-000000000001",
        "buyer_handle_id": "buyer:base:one",
        "chain_scope": "base",
        "handle_value": "0x1111111111111111111111111111111111111111",
        "enrichment_mode": "automatic",
        "max_pages": 1,
        "request_budget": 1,
        "observed_date": "2026-10-07",
        "cursor_version": 0,
        "start_cursor": null,
        "classification_labels": [{
            "id": "core:active",
            "version": 1,
            "kind": "core",
            "metric": "payment_count",
            "threshold": "1",
            "confidence_bps": 8500,
            "definition_hash": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        }]
    }))
    .unwrap();
    let job = LeasedJob {
        job_id: "00000000-0000-0000-0000-000000000060".to_owned(),
        mode: WorkerMode::Enrich,
        job_kind: "buyer-public-history-v1".to_owned(),
        input_sha256: format!("{:x}", Sha256::digest(&input)),
        attempt: 1,
        lease_owner: "enricher:test".to_owned(),
        lease_token: "00000000-0000-0000-0000-000000000061".to_owned(),
        collection_admission: None,
    };
    let store = Arc::new(MemoryStore {
        input,
        committed: Mutex::new(Vec::new()),
    });
    let handler = EnrichmentHandler::new(OnePageTransport, Arc::new(MemoryArchive), store.clone());

    let result = handler.process(&job).await.expect("enrichment succeeds");
    let batches = store.committed.lock().unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].requests_used(), 1);
    assert_eq!(batches[0].evidence().len(), 1);
    assert_eq!(batches[0].records().len(), 1);
    assert_eq!(
        batches[0].records()[0].transaction_reference(),
        "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert!(batches[0].complete());
    assert_eq!(result.output_sha256(), batches[0].output_sha256());
}
