use std::{collections::VecDeque, sync::Arc};

use agent_economy_monitor::worker::{
    HandlerFailure, JobResult, LeasedJob, WorkerDispatchError, WorkerDispatcher, WorkerHandler,
    WorkerJobStore, WorkerMode, WorkerStoreError,
};
use async_trait::async_trait;
use tokio::sync::Mutex;

#[derive(Default)]
struct StoreState {
    queued: VecDeque<LeasedJob>,
    claims: usize,
    completed: Vec<(String, String, String)>,
    failed: Vec<(String, String, String, bool)>,
}

#[derive(Default)]
struct MemoryStore(Mutex<StoreState>);

#[async_trait]
impl WorkerJobStore for MemoryStore {
    async fn claim(
        &self,
        mode: WorkerMode,
        lease_owner: &str,
        _lease_seconds: u64,
    ) -> Result<Option<LeasedJob>, WorkerStoreError> {
        let mut state = self.0.lock().await;
        state.claims += 1;
        Ok(state.queued.pop_front().map(|mut job| {
            assert_eq!(job.mode, mode);
            job.lease_owner = lease_owner.into();
            job
        }))
    }

    async fn renew(&self, _job: &LeasedJob, _lease_seconds: u64) -> Result<(), WorkerStoreError> {
        Ok(())
    }

    async fn complete(&self, job: &LeasedJob, output_sha256: &str) -> Result<(), WorkerStoreError> {
        self.0.lock().await.completed.push((
            job.job_id.clone(),
            job.lease_token.clone(),
            output_sha256.into(),
        ));
        Ok(())
    }

    async fn fail(
        &self,
        job: &LeasedJob,
        error_code: &str,
        retryable: bool,
    ) -> Result<(), WorkerStoreError> {
        self.0.lock().await.failed.push((
            job.job_id.clone(),
            job.lease_token.clone(),
            error_code.into(),
            retryable,
        ));
        Ok(())
    }
}

struct DigestHandler;

#[async_trait]
impl WorkerHandler for DigestHandler {
    async fn process(&self, job: &LeasedJob) -> Result<JobResult, HandlerFailure> {
        assert_eq!(job.input_sha256, "a".repeat(64));
        JobResult::new(&"b".repeat(64))
    }
}

struct RetryHandler;

#[async_trait]
impl WorkerHandler for RetryHandler {
    async fn process(&self, _job: &LeasedJob) -> Result<JobResult, HandlerFailure> {
        Err(HandlerFailure::retryable("provider_unavailable").unwrap())
    }
}

fn leased_job(mode: WorkerMode) -> LeasedJob {
    LeasedJob {
        job_id: "00000000-0000-0000-0000-000000000047".into(),
        mode,
        job_kind: "bounded-test".into(),
        input_sha256: "a".repeat(64),
        attempt: 1,
        lease_owner: String::new(),
        lease_token: "00000000-0000-0000-0000-000000000147".into(),
        collection_admission: None,
    }
}

#[test]
fn process_modes_are_explicit_and_unknown_modes_fail_closed() {
    assert_eq!(WorkerMode::parse("collect").unwrap(), WorkerMode::Collect);
    assert_eq!(WorkerMode::parse("reduce").unwrap(), WorkerMode::Reduce);
    assert_eq!(WorkerMode::parse("classify").unwrap(), WorkerMode::Classify);
    assert_eq!(WorkerMode::parse("enrich").unwrap(), WorkerMode::Enrich);
    assert!(WorkerMode::parse("serve").is_err());
    assert!(WorkerMode::parse("anything-else").is_err());
}

#[tokio::test]
async fn missing_handler_fails_before_claiming_a_job() {
    let store = Arc::new(MemoryStore::default());
    store
        .0
        .lock()
        .await
        .queued
        .push_back(leased_job(WorkerMode::Reduce));
    let dispatcher = WorkerDispatcher::new(store.clone());

    let error = dispatcher
        .run_once(WorkerMode::Reduce, "worker-a")
        .await
        .unwrap_err();

    assert_eq!(
        error,
        WorkerDispatchError::MissingHandler(WorkerMode::Reduce)
    );
    assert_eq!(store.0.lock().await.claims, 0);
}

#[tokio::test]
async fn successful_handler_commits_the_exact_output_digest() {
    let store = Arc::new(MemoryStore::default());
    store
        .0
        .lock()
        .await
        .queued
        .push_back(leased_job(WorkerMode::Collect));
    let dispatcher =
        WorkerDispatcher::new(store.clone()).register(WorkerMode::Collect, Arc::new(DigestHandler));

    let result = dispatcher
        .run_once(WorkerMode::Collect, "worker-a")
        .await
        .unwrap();

    assert_eq!(result.job_id, "00000000-0000-0000-0000-000000000047");
    assert_eq!(result.output_sha256, "b".repeat(64));
    assert_eq!(
        store.0.lock().await.completed,
        vec![(
            result.job_id,
            "00000000-0000-0000-0000-000000000147".into(),
            "b".repeat(64)
        )]
    );
}

#[tokio::test]
async fn handler_failure_records_only_a_safe_code_and_returns_nonzero_semantics() {
    let store = Arc::new(MemoryStore::default());
    store
        .0
        .lock()
        .await
        .queued
        .push_back(leased_job(WorkerMode::Enrich));
    let dispatcher =
        WorkerDispatcher::new(store.clone()).register(WorkerMode::Enrich, Arc::new(RetryHandler));

    let error = dispatcher
        .run_once(WorkerMode::Enrich, "worker-a")
        .await
        .unwrap_err();

    assert_eq!(
        error,
        WorkerDispatchError::HandlerFailed("provider_unavailable".into())
    );
    assert_eq!(
        store.0.lock().await.failed,
        vec![(
            "00000000-0000-0000-0000-000000000047".into(),
            "00000000-0000-0000-0000-000000000147".into(),
            "provider_unavailable".into(),
            true
        )]
    );
}

#[tokio::test]
async fn an_empty_queue_is_not_reported_as_success() {
    let store = Arc::new(MemoryStore::default());
    let dispatcher =
        WorkerDispatcher::new(store).register(WorkerMode::Classify, Arc::new(DigestHandler));

    assert_eq!(
        dispatcher
            .run_once(WorkerMode::Classify, "worker-a")
            .await
            .unwrap_err(),
        WorkerDispatchError::NoJob(WorkerMode::Classify)
    );
}
