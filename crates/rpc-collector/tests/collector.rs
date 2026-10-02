use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
};

use agent_economy_rpc_collector::{
    AlchemyRequest, AlchemyTransport, Chain, CollectRequest, Collector, CollectorError,
    CursorCheckpoint, CursorStore, EvidenceArchive, RawRpcResponse, RetryDelay, RetryPolicy,
    RpcEndpoint, RpcEvidence, RpcTransport,
};

#[derive(Default)]
struct FakeTransport {
    responses: VecDeque<Result<RawRpcResponse, CollectorError>>,
    events: Arc<Mutex<Vec<String>>>,
}

impl RpcTransport for FakeTransport {
    async fn fetch_block(
        &mut self,
        chain: Chain,
        height: u64,
    ) -> Result<RawRpcResponse, CollectorError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("fetch:{chain}:{height}"));
        self.responses.pop_front().expect("response queued")
    }
}

#[derive(Default)]
struct FakeArchive {
    events: Arc<Mutex<Vec<String>>>,
    sequence: usize,
}

impl EvidenceArchive for FakeArchive {
    async fn archive(
        &mut self,
        observed_date: &str,
        evidence: &RpcEvidence,
    ) -> Result<String, CollectorError> {
        self.events.lock().unwrap().push(format!(
            "archive:{chain}:{observed_date}:{}",
            evidence.body().len(),
            chain = evidence.chain(),
        ));
        self.sequence += 1;
        Ok(format!(
            "evidence/{}/object-{}",
            evidence.chain(),
            self.sequence
        ))
    }
}

#[derive(Default)]
struct FakeCursorStore {
    checkpoint: Option<CursorCheckpoint>,
    events: Arc<Mutex<Vec<String>>>,
}

impl CursorStore for FakeCursorStore {
    async fn load(&mut self, chain: Chain) -> Result<Option<CursorCheckpoint>, CollectorError> {
        self.events.lock().unwrap().push(format!("load:{chain}"));
        Ok(self.checkpoint)
    }

    async fn compare_and_set(
        &mut self,
        chain: Chain,
        expected: CursorCheckpoint,
        next_height: u64,
    ) -> Result<CursorCheckpoint, CollectorError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("commit:{chain}:{next_height}"));
        if self.checkpoint != Some(expected) {
            return Err(CollectorError::CursorConflict);
        }
        let next = CursorCheckpoint::new(next_height, expected.version() + 1);
        self.checkpoint = Some(next);
        Ok(next)
    }

    async fn initialize(
        &mut self,
        chain: Chain,
        start_height: u64,
    ) -> Result<CursorCheckpoint, CollectorError> {
        self.events
            .lock()
            .unwrap()
            .push(format!("initialize:{chain}:{start_height}"));
        let checkpoint = self
            .checkpoint
            .get_or_insert(CursorCheckpoint::new(start_height, 0));
        Ok(*checkpoint)
    }
}

#[derive(Default)]
struct NoDelay {
    waits: Vec<u64>,
}

impl RetryDelay for NoDelay {
    async fn wait(&mut self, milliseconds: u64) {
        self.waits.push(milliseconds);
    }
}

fn response(status: u16, body: &str) -> Result<RawRpcResponse, CollectorError> {
    RawRpcResponse::try_new(status, body.as_bytes().to_vec())
}

fn evm_block(height: u64) -> String {
    format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"number":"0x{height:x}"}}}}"#)
}

fn solana_block(height: u64) -> String {
    format!(
        r#"{{"jsonrpc":"2.0","id":1,"result":{{"blockHeight":{height},"blockhash":"block-{height}","parentSlot":{},"previousBlockhash":"block-parent","transactions":[]}}}}"#,
        height.saturating_sub(1)
    )
}

fn collect_request(
    chain: Chain,
    start_height: u64,
    max_blocks: u64,
    request_budget: u64,
    observed_date: &str,
) -> CollectRequest {
    CollectRequest::try_new(
        chain,
        start_height,
        max_blocks,
        request_budget,
        observed_date,
    )
    .expect("bounded test request")
}

fn retry_policy(max_retries: u32, base_delay_milliseconds: u64) -> RetryPolicy {
    RetryPolicy::try_new(max_retries, base_delay_milliseconds).expect("bounded test retry policy")
}

#[test]
fn four_launch_chains_share_typed_alchemy_requests() {
    assert_eq!(
        Chain::ALL,
        [Chain::Ethereum, Chain::Base, Chain::Solana, Chain::Tempo]
    );

    for chain in [Chain::Ethereum, Chain::Base, Chain::Tempo] {
        let request = AlchemyRequest::block(chain, 42);
        assert_eq!(request.method(), "eth_getBlockByNumber");
        assert_eq!(request.requested_height(), 42);
    }
    let solana = AlchemyRequest::block(Chain::Solana, 42);
    assert_eq!(solana.method(), "getBlock");
    assert_eq!(solana.requested_height(), 42);
}

#[test]
fn rpc_endpoint_debug_never_exposes_credentials() {
    let endpoint = RpcEndpoint::parse("https://eth-mainnet.g.alchemy.com/v2/private-token")
        .expect("valid HTTPS endpoint");

    assert_eq!(format!("{endpoint:?}"), "RpcEndpoint([REDACTED])");
    assert!(!format!("{endpoint:?}").contains("private-token"));
}

#[test]
fn alchemy_endpoints_must_match_their_configured_chain() {
    let endpoints = [
        (
            Chain::Ethereum,
            RpcEndpoint::parse("https://eth-mainnet.g.alchemy.com/v2/ethereum-token").unwrap(),
        ),
        (
            Chain::Base,
            RpcEndpoint::parse("https://eth-mainnet.g.alchemy.com/v2/base-token").unwrap(),
        ),
        (
            Chain::Solana,
            RpcEndpoint::parse("https://solana-mainnet.g.alchemy.com/v2/solana-token").unwrap(),
        ),
        (
            Chain::Tempo,
            RpcEndpoint::parse("https://tempo-mainnet.g.alchemy.com/v2/tempo-token").unwrap(),
        ),
    ];

    assert!(matches!(
        AlchemyTransport::new(endpoints),
        Err(CollectorError::InvalidEndpoint)
    ));
}

#[test]
fn oversized_provider_response_is_rejected_before_allocation_contract_accepts_it() {
    let error = match RawRpcResponse::try_new(200, vec![0; 8 * 1024 * 1024 + 1]) {
        Ok(_) => panic!("provider responses are capped at eight MiB"),
        Err(error) => error,
    };

    assert_eq!(error, CollectorError::ResponseTooLarge);
}

#[test]
fn archived_rpc_evidence_replays_provider_provenance_and_exact_body() {
    let response = RawRpcResponse::try_new(429, b"provider bytes".to_vec())
        .unwrap()
        .with_retry_after(2_500);
    let evidence = RpcEvidence::from_response(Chain::Base, 88, 2, &response);
    let encoded = evidence.encode();
    let replayed = RpcEvidence::decode(&encoded).expect("evidence envelope replays");

    assert_eq!(replayed.provider(), "alchemy");
    assert_eq!(replayed.chain(), Chain::Base);
    assert_eq!(replayed.method(), "eth_getBlockByNumber");
    assert_eq!(replayed.requested_height(), 88);
    assert_eq!(replayed.http_status(), 429);
    assert_eq!(replayed.attempt(), 2);
    assert_eq!(replayed.retry_after_milliseconds(), Some(2_500));
    assert_eq!(replayed.body(), b"provider bytes");
}

#[test]
fn run_configuration_rejects_unbounded_caller_values() {
    assert_eq!(
        CollectRequest::try_new(Chain::Base, 1, u64::MAX, 1, "2026-09-28"),
        Err(CollectorError::InvalidConfiguration)
    );
    assert_eq!(
        CollectRequest::try_new(Chain::Base, 1, 1, u64::MAX, "2026-09-28"),
        Err(CollectorError::InvalidConfiguration)
    );
    assert_eq!(
        RetryPolicy::try_new(u32::MAX, 1),
        Err(CollectorError::InvalidConfiguration)
    );
    assert_eq!(
        RetryPolicy::try_new(1, u64::MAX),
        Err(CollectorError::InvalidConfiguration)
    );
}

#[tokio::test]
async fn response_is_archived_before_parsing_or_cursor_commit() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(200, "not-json")]),
        events: events.clone(),
    };
    let mut archive = FakeArchive {
        events: events.clone(),
        ..Default::default()
    };
    let mut cursors = FakeCursorStore {
        events: events.clone(),
        ..Default::default()
    };
    let mut delay = NoDelay::default();

    let error = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Ethereum, 10, 1, 1, "2026-09-28"))
        .await
        .expect_err("malformed archived evidence must not advance the cursor");

    assert!(matches!(error.cause(), CollectorError::InvalidResponse(_)));
    assert_eq!(
        *events.lock().unwrap(),
        [
            "load:ethereum",
            "initialize:ethereum:10",
            "fetch:ethereum:10",
            "archive:ethereum:2026-09-28:8",
        ]
    );
}

#[tokio::test]
async fn collector_resumes_and_advances_only_contiguous_archived_blocks() {
    let events = Arc::new(Mutex::new(Vec::new()));
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(200, &evm_block(20)), response(200, &evm_block(21))]),
        events: events.clone(),
    };
    let mut archive = FakeArchive {
        events: events.clone(),
        ..Default::default()
    };
    let mut cursors = FakeCursorStore {
        checkpoint: Some(CursorCheckpoint::new(20, 7)),
        events,
    };
    let mut delay = NoDelay::default();

    let report = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Ethereum, 1, 2, 2, "2026-09-28"))
        .await
        .expect("contiguous batch");

    assert_eq!(report.responses_archived(), 2);
    assert_eq!(report.requests_used(), 2);
    assert_eq!(report.requests_remaining(), 0);
    assert_eq!(report.next_height(), 22);
    assert!(report.gaps().is_empty());
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(22, 9)));
}

#[tokio::test]
async fn gap_is_reported_without_skipping_the_missing_height() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(200, &evm_block(102))]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore {
        checkpoint: Some(CursorCheckpoint::new(100, 3)),
        ..Default::default()
    };
    let mut delay = NoDelay::default();

    let report = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Ethereum, 1, 4, 4, "2026-09-28"))
        .await
        .expect("gap is an exposed collection result");

    assert_eq!(report.responses_archived(), 1);
    assert_eq!(report.next_height(), 100);
    assert_eq!(report.gaps()[0].expected_height(), 100);
    assert_eq!(report.gaps()[0].observed_height(), Some(102));
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(100, 3)));
}

#[tokio::test]
async fn solana_cursor_tracks_requested_slots_not_block_heights() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(200, &solana_block(88))]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore {
        checkpoint: Some(CursorCheckpoint::new(100, 3)),
        ..Default::default()
    };
    let mut delay = NoDelay::default();

    let report = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Solana, 1, 1, 1, "2026-09-28"))
        .await
        .expect("a returned Solana block proves the requested slot exists");

    assert!(report.gaps().is_empty());
    assert_eq!(report.next_height(), 101);
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(101, 4)));
}

#[tokio::test]
async fn retries_are_bounded_by_policy_and_request_budget() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([
            response(429, r#"{"error":"rate limited"}"#),
            response(503, r#"{"error":"unavailable"}"#),
            response(200, &evm_block(5)),
        ]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    let report = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .with_retry_policy(retry_policy(2, 25))
        .collect(collect_request(Chain::Base, 5, 1, 3, "2026-09-28"))
        .await
        .expect("third budgeted request succeeds");

    assert_eq!(report.requests_used(), 3);
    assert_eq!(report.retry_count(), 2);
    assert_eq!(report.responses_archived(), 3);
    assert_eq!(delay.waits, [25, 50]);
}

#[tokio::test]
async fn terminal_provider_failure_preserves_run_metrics() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([
            response(429, r#"{"error":"rate limited"}"#),
            response(503, r#"{"error":"unavailable"}"#),
        ]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    let error = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .with_retry_policy(retry_policy(5, 25))
        .collect(collect_request(Chain::Base, 5, 1, 2, "2026-09-28"))
        .await
        .expect_err("the terminal response remains a failed run");

    assert_eq!(error.cause(), &CollectorError::HttpStatus(503));
    let report = error.report().expect("failed runs expose their report");
    assert_eq!(report.requests_used(), 2);
    assert_eq!(report.retry_count(), 1);
    assert_eq!(report.responses_archived(), 2);
    assert_eq!(report.evidence_objects().len(), 2);
    assert!(report.budget_exhausted());
}

#[tokio::test]
async fn cursor_height_overflow_fails_without_wrapping_or_committing() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(200, &evm_block(u64::MAX))]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore {
        checkpoint: Some(CursorCheckpoint::new(u64::MAX, 4)),
        ..Default::default()
    };
    let mut delay = NoDelay::default();

    let error = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Tempo, 1, 1, 1, "2026-09-28"))
        .await
        .expect_err("cursor height must not wrap");

    assert_eq!(error.cause(), &CollectorError::InvalidHeight);
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(u64::MAX, 4)));
}

#[tokio::test]
async fn provider_retry_after_is_capped() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([
            Ok(
                RawRpcResponse::try_new(429, br#"{"error":"slow down"}"#.to_vec())
                    .unwrap()
                    .with_retry_after(u64::MAX),
            ),
            response(200, &evm_block(9)),
        ]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .with_retry_policy(retry_policy(1, 25))
        .collect(collect_request(Chain::Ethereum, 9, 1, 2, "2026-09-28"))
        .await
        .expect("bounded retry succeeds");

    assert_eq!(delay.waits, [60_000]);
}

#[tokio::test]
async fn transport_retry_backoff_is_capped() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([
            Err(CollectorError::Transport),
            Err(CollectorError::Transport),
            response(200, &evm_block(9)),
        ]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .with_retry_policy(retry_policy(2, 60_000))
        .collect(collect_request(Chain::Ethereum, 9, 1, 3, "2026-09-28"))
        .await
        .expect("bounded transport retry succeeds");

    assert_eq!(delay.waits, [60_000, 60_000]);
}

#[tokio::test]
async fn duplicate_json_members_are_archived_but_rejected() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(
            200,
            r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0xc","number":"0xd"}}"#,
        )]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    let error = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Base, 12, 1, 1, "2026-09-28"))
        .await
        .expect_err("ambiguous provider evidence must be quarantined");

    assert_eq!(error.cause(), &CollectorError::InvalidResponse("json"));
    assert_eq!(archive.sequence, 1);
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(12, 0)));
}

#[tokio::test]
async fn response_with_result_and_error_is_archived_but_rejected() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(
            200,
            r#"{"jsonrpc":"2.0","id":1,"result":{"number":"0xc"},"error":{"code":-1}}"#,
        )]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    let error = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Base, 12, 1, 1, "2026-09-28"))
        .await
        .expect_err("JSON-RPC responses cannot contain both result and error");

    assert_eq!(
        error.cause(),
        &CollectorError::InvalidResponse("result and error")
    );
    assert_eq!(archive.sequence, 1);
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(12, 0)));
}

#[tokio::test]
async fn mismatched_json_rpc_id_cannot_advance_cursor() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(
            200,
            r#"{"jsonrpc":"2.0","id":2,"result":{"number":"0xc"}}"#,
        )]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    let error = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Tempo, 12, 1, 1, "2026-09-28"))
        .await
        .expect_err("response correlation is mandatory");

    assert_eq!(
        error.cause(),
        &CollectorError::InvalidResponse("json-rpc id")
    );
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(12, 0)));
}

#[tokio::test]
async fn noncanonical_evm_quantities_cannot_advance_cursor() {
    for quantity in ["0x01", "0xA", "0x+1"] {
        let body = format!(r#"{{"jsonrpc":"2.0","id":1,"result":{{"number":"{quantity}"}}}}"#);
        let mut transport = FakeTransport {
            responses: VecDeque::from([response(200, &body)]),
            ..Default::default()
        };
        let mut archive = FakeArchive::default();
        let mut cursors = FakeCursorStore::default();
        let mut delay = NoDelay::default();

        let error = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
            .collect(collect_request(Chain::Ethereum, 1, 1, 1, "2026-09-28"))
            .await
            .expect_err("EVM quantities must use canonical lowercase minimal hex");

        assert_eq!(error.cause(), &CollectorError::InvalidResponse("number"));
        assert_eq!(archive.sequence, 1);
        assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(1, 0)));
    }
}

#[tokio::test]
async fn solana_skipped_slot_is_exposed_as_a_gap() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(
            200,
            r#"{"jsonrpc":"2.0","id":1,"error":{"code":-32007,"message":"slot skipped"}}"#,
        )]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    let report = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Solana, 44, 1, 1, "2026-09-28"))
        .await
        .expect("known skipped slots are gaps, not parser failures");

    assert_eq!(report.gaps()[0].expected_height(), 44);
    assert_eq!(report.gaps()[0].observed_height(), None);
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(44, 0)));
}

#[tokio::test]
async fn malformed_solana_block_cannot_advance_cursor() {
    let mut transport = FakeTransport {
        responses: VecDeque::from([response(200, r#"{"jsonrpc":"2.0","id":1,"result":{}}"#)]),
        ..Default::default()
    };
    let mut archive = FakeArchive::default();
    let mut cursors = FakeCursorStore::default();
    let mut delay = NoDelay::default();

    let error = Collector::new(&mut transport, &mut archive, &mut cursors, &mut delay)
        .collect(collect_request(Chain::Solana, 44, 1, 1, "2026-09-28"))
        .await
        .expect_err("an arbitrary object is not a valid Solana block");

    assert_eq!(
        error.cause(),
        &CollectorError::InvalidResponse("solana block")
    );
    assert_eq!(error.report().unwrap().responses_archived(), 1);
    assert_eq!(cursors.checkpoint, Some(CursorCheckpoint::new(44, 0)));
}

impl fmt::Display for FakeTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("fake")
    }
}
