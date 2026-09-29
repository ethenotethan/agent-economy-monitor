use std::collections::VecDeque;

use agent_economy_rpc_collector::{
    AlchemyHistoryRequest, AlchemyTransport, BuyerHistoryEnricher, BuyerHistoryTarget, Chain,
    CollectorError, EnrichmentCandidate, EnrichmentCheckpoint, EnrichmentMode, EnrichmentRequest,
    EnrichmentStateStore, FinalizedHistoryCache, FinalizedHistoryRecord, HistoryEvidenceArchive,
    HistoryFinality, HistoryPage, HistoryTransport, RawRpcResponse, prioritize_automatic,
};

fn target(chain: Chain) -> BuyerHistoryTarget {
    BuyerHistoryTarget::try_new(
        "00000000-0000-0000-0000-000000000001",
        format!("buyer:{}:alice", chain.as_str()),
        chain,
        "alice",
    )
    .expect("valid discovered buyer target")
}

#[test]
fn automatic_selection_prioritizes_only_discovered_buyers() {
    let candidates = vec![
        EnrichmentCandidate::new(target(Chain::Ethereum), 3, 10, false),
        EnrichmentCandidate::new(target(Chain::Base), 8, 2, true),
        EnrichmentCandidate::new(target(Chain::Solana), 8, 20, false),
    ];

    let selected = prioritize_automatic(candidates, 2).expect("bounded candidate selection");

    assert_eq!(selected.len(), 2);
    assert_eq!(selected[0].buyer_handle_id(), "buyer:base:alice");
    assert_eq!(selected[1].buyer_handle_id(), "buyer:solana:alice");
    assert_eq!(
        prioritize_automatic(Vec::new(), 1),
        Err(CollectorError::NoEnrichmentTargets)
    );
}

#[test]
fn alchemy_history_requests_always_filter_by_the_target_buyer() {
    fn assert_history_transport<T: HistoryTransport>() {}
    assert_history_transport::<AlchemyTransport>();

    for chain in [Chain::Ethereum, Chain::Base, Chain::Tempo] {
        let request = AlchemyHistoryRequest::try_new(&target(chain), None).unwrap();
        assert_eq!(request.method(), "alchemy_getAssetTransfers");
        assert_eq!(request.body()["params"][0]["fromAddress"], "alice");
        assert!(request.body()["params"][0].get("toAddress").is_none());
    }

    let solana = AlchemyHistoryRequest::try_new(&target(Chain::Solana), None).unwrap();
    assert_eq!(solana.method(), "getSignaturesForAddress");
    assert_eq!(solana.body()["params"][0], "alice");
    assert_eq!(solana.body()["params"][1]["commitment"], "finalized");
}

#[test]
fn alchemy_history_pages_decode_finality_and_opaque_cursors() {
    const EVM_TX: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SOLANA_SIGNATURE: &str =
        "1111111111111111111111111111111111111111111111111111111111111111";
    let base = target(Chain::Base);
    let evm = HistoryPage::decode_alchemy(
        &base,
        br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","blockNum":"0xa","from":"alice"}],"pageKey":"opaque-next"}}"#,
    )
    .unwrap();
    assert_eq!(evm.items().len(), 1);
    assert_eq!(evm.items()[0].transaction_reference(), EVM_TX);
    assert_eq!(evm.items()[0].block_reference(), "0xa");
    assert_eq!(evm.items()[0].finality(), HistoryFinality::Finalized);
    assert_eq!(evm.next_cursor(), Some("opaque-next"));

    let solana_target = target(Chain::Solana);
    let solana = HistoryPage::decode_alchemy(
        &solana_target,
        br#"{"jsonrpc":"2.0","id":1,"result":[{"signature":"1111111111111111111111111111111111111111111111111111111111111111","slot":42,"confirmationStatus":"confirmed"}]}"#,
    )
    .unwrap();
    assert_eq!(solana.items()[0].transaction_reference(), SOLANA_SIGNATURE);
    assert_eq!(solana.items()[0].finality(), HistoryFinality::Confirmed);
    assert_eq!(solana.next_cursor(), None);
}

#[test]
fn alchemy_history_rejects_unrelated_buyers_and_malformed_identifiers() {
    let base = target(Chain::Base);
    let unrelated = br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","blockNum":"0xa","from":"mallory"}]}}"#;
    assert_eq!(
        HistoryPage::decode_alchemy(&base, unrelated),
        Err(CollectorError::InvalidHistoryPage)
    );
    let malformed = br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[{"hash":"0xtx","blockNum":"10","from":"alice"}]}}"#;
    assert_eq!(
        HistoryPage::decode_alchemy(&base, malformed),
        Err(CollectorError::InvalidHistoryPage)
    );
}

#[test]
fn automatic_and_manual_modes_are_targeted_and_hard_bounded() {
    let automatic = EnrichmentRequest::try_new(
        target(Chain::Base),
        EnrichmentMode::Automatic,
        25,
        50,
        "2026-09-29",
    )
    .expect("maximum automatic request is bounded");
    assert_eq!(automatic.target().chain(), Chain::Base);
    assert_eq!(automatic.mode(), EnrichmentMode::Automatic);
    assert_eq!(automatic.max_pages(), 25);
    assert_eq!(automatic.request_budget(), 50);

    let manual = EnrichmentRequest::try_new(
        target(Chain::Solana),
        EnrichmentMode::ManualDeepScan,
        500,
        1_000,
        "2026-09-29",
    )
    .expect("maximum manual deep scan is bounded");
    assert_eq!(manual.mode(), EnrichmentMode::ManualDeepScan);

    assert_eq!(
        EnrichmentRequest::try_new(
            target(Chain::Base),
            EnrichmentMode::Automatic,
            26,
            50,
            "2026-09-29",
        ),
        Err(CollectorError::InvalidConfiguration)
    );
    assert_eq!(
        EnrichmentRequest::try_new(
            target(Chain::Base),
            EnrichmentMode::ManualDeepScan,
            500,
            1_001,
            "2026-09-29",
        ),
        Err(CollectorError::InvalidConfiguration)
    );
    assert_eq!(
        BuyerHistoryTarget::try_new("", "buyer:base:alice", Chain::Base, "alice"),
        Err(CollectorError::InvalidTarget)
    );
}

#[derive(Default)]
struct FakeHistoryTransport {
    responses: VecDeque<Result<RawRpcResponse, CollectorError>>,
    requests: Vec<(Chain, String, Option<String>)>,
}

impl HistoryTransport for FakeHistoryTransport {
    async fn fetch_history(
        &mut self,
        target: &BuyerHistoryTarget,
        cursor: Option<&str>,
    ) -> Result<RawRpcResponse, CollectorError> {
        self.requests.push((
            target.chain(),
            target.buyer_handle_id().to_owned(),
            cursor.map(str::to_owned),
        ));
        self.responses
            .pop_front()
            .unwrap_or(Err(CollectorError::Transport))
    }
}

#[derive(Default)]
struct FakeHistoryArchive {
    bodies: Vec<Vec<u8>>,
    statuses: Vec<u16>,
}

impl HistoryEvidenceArchive for FakeHistoryArchive {
    fn archive_history(
        &mut self,
        _target: &BuyerHistoryTarget,
        _observed_date: &str,
        http_status: u16,
        body: &[u8],
    ) -> Result<String, CollectorError> {
        self.statuses.push(http_status);
        self.bodies.push(body.to_vec());
        Ok(format!("evidence/history/{}", self.bodies.len()))
    }
}

#[derive(Default)]
struct FakeEnrichmentState {
    checkpoint: Option<EnrichmentCheckpoint>,
}

impl EnrichmentStateStore for FakeEnrichmentState {
    async fn load(
        &mut self,
        _target: &BuyerHistoryTarget,
    ) -> Result<Option<EnrichmentCheckpoint>, CollectorError> {
        Ok(self.checkpoint.clone())
    }

    async fn initialize(
        &mut self,
        _target: &BuyerHistoryTarget,
    ) -> Result<EnrichmentCheckpoint, CollectorError> {
        let checkpoint = self
            .checkpoint
            .get_or_insert_with(|| EnrichmentCheckpoint::new(None, 0, 0, false));
        Ok(checkpoint.clone())
    }

    async fn compare_and_set(
        &mut self,
        _target: &BuyerHistoryTarget,
        expected: &EnrichmentCheckpoint,
        next_cursor: Option<String>,
        complete: bool,
    ) -> Result<EnrichmentCheckpoint, CollectorError> {
        if self.checkpoint.as_ref() != Some(expected) {
            return Err(CollectorError::CursorConflict);
        }
        let next = EnrichmentCheckpoint::with_budget_state(
            next_cursor,
            expected.version() + 1,
            expected.requests_used_total(),
            expected.last_run_budget(),
            expected.last_run_requests_used(),
            complete,
        );
        self.checkpoint = Some(next.clone());
        Ok(next)
    }

    async fn record_request(
        &mut self,
        _target: &BuyerHistoryTarget,
        expected: &EnrichmentCheckpoint,
        request_budget: u64,
        run_requests_used: u64,
    ) -> Result<EnrichmentCheckpoint, CollectorError> {
        if self.checkpoint.as_ref() != Some(expected) {
            return Err(CollectorError::CursorConflict);
        }
        let next = EnrichmentCheckpoint::with_budget_state(
            expected.cursor().map(str::to_owned),
            expected.version() + 1,
            expected.requests_used_total() + 1,
            request_budget,
            run_requests_used,
            expected.complete(),
        );
        self.checkpoint = Some(next.clone());
        Ok(next)
    }
}

#[derive(Default)]
struct FakeFinalizedCache {
    records: Vec<FinalizedHistoryRecord>,
}

impl FinalizedHistoryCache for FakeFinalizedCache {
    async fn store_finalized(
        &mut self,
        record: FinalizedHistoryRecord,
    ) -> Result<(), CollectorError> {
        self.records.push(record);
        Ok(())
    }
}

#[tokio::test]
async fn enrichment_archives_pages_caches_only_finalized_history_and_persists_budget() {
    const TRANSACTION: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const PAGE_ONE: &[u8] = br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[{"hash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","blockNum":"0xa","from":"alice"}],"pageKey":"next"}}"#;
    const PAGE_TWO: &[u8] = br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[]}}"#;
    let mut transport = FakeHistoryTransport {
        responses: VecDeque::from([
            RawRpcResponse::try_new(200, PAGE_ONE.to_vec()),
            RawRpcResponse::try_new(200, PAGE_TWO.to_vec()),
        ]),
        ..Default::default()
    };
    let mut archive = FakeHistoryArchive::default();
    let mut state = FakeEnrichmentState::default();
    let mut cache = FakeFinalizedCache::default();
    let request = EnrichmentRequest::try_new(
        target(Chain::Base),
        EnrichmentMode::Automatic,
        2,
        2,
        "2026-09-29",
    )
    .unwrap();

    let report = BuyerHistoryEnricher::new(&mut transport, &mut archive, &mut state, &mut cache)
        .enrich(request)
        .await
        .expect("bounded enrichment completes");

    assert_eq!(
        transport.requests,
        [
            (Chain::Base, "buyer:base:alice".to_owned(), None),
            (
                Chain::Base,
                "buyer:base:alice".to_owned(),
                Some("next".to_owned())
            ),
        ]
    );
    assert_eq!(archive.bodies, [PAGE_ONE, PAGE_TWO]);
    assert_eq!(archive.statuses, [200, 200]);
    assert_eq!(cache.records.len(), 1);
    assert_eq!(cache.records[0].transaction_reference(), TRANSACTION);
    assert!(cache.records[0].protocol_attribution().is_none());
    assert_eq!(report.requests_used(), 2);
    assert_eq!(report.finalized_records_cached(), 1);
    assert_eq!(report.unfinalized_records_skipped(), 0);
    assert!(report.complete());
    let checkpoint = state.checkpoint.expect("cursor and budget state persisted");
    assert_eq!(checkpoint.requests_used_total(), 2);
    assert_eq!(checkpoint.last_run_budget(), 2);
    assert_eq!(checkpoint.last_run_requests_used(), 2);
    assert!(checkpoint.complete());
}

#[tokio::test]
async fn enrichment_rejects_a_repeated_opaque_cursor_without_advancing_state() {
    const REPEATED: &[u8] =
        br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[],"pageKey":"same"}}"#;
    let original = EnrichmentCheckpoint::new(Some("same".to_owned()), 4, 9, false);
    let mut transport = FakeHistoryTransport {
        responses: VecDeque::from([RawRpcResponse::try_new(200, REPEATED.to_vec())]),
        ..Default::default()
    };
    let mut archive = FakeHistoryArchive::default();
    let mut state = FakeEnrichmentState {
        checkpoint: Some(original.clone()),
    };
    let mut cache = FakeFinalizedCache::default();
    let request = EnrichmentRequest::try_new(
        target(Chain::Ethereum),
        EnrichmentMode::ManualDeepScan,
        2,
        2,
        "2026-09-29",
    )
    .unwrap();

    let error = BuyerHistoryEnricher::new(&mut transport, &mut archive, &mut state, &mut cache)
        .enrich(request)
        .await
        .expect_err("provider cursors must make progress");

    assert_eq!(error, CollectorError::InvalidHistoryPage);
    assert_eq!(archive.bodies, [REPEATED]);
    assert_eq!(
        state.checkpoint,
        Some(EnrichmentCheckpoint::with_budget_state(
            Some("same".to_owned()),
            5,
            10,
            2,
            1,
            false
        ))
    );
}

#[tokio::test]
async fn enrichment_archives_provider_response_before_validation() {
    const INVALID: &[u8] = br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":"invalid"}}"#;
    let mut transport = FakeHistoryTransport {
        responses: VecDeque::from([RawRpcResponse::try_new(200, INVALID.to_vec())]),
        ..Default::default()
    };
    let mut archive = FakeHistoryArchive::default();
    let mut state = FakeEnrichmentState::default();
    let mut cache = FakeFinalizedCache::default();
    let request = EnrichmentRequest::try_new(
        target(Chain::Base),
        EnrichmentMode::Automatic,
        1,
        1,
        "2026-09-29",
    )
    .unwrap();

    let error = BuyerHistoryEnricher::new(&mut transport, &mut archive, &mut state, &mut cache)
        .enrich(request)
        .await
        .expect_err("invalid provider response must not advance enrichment");

    assert_eq!(error, CollectorError::InvalidHistoryPage);
    assert_eq!(archive.bodies, [INVALID]);
    assert_eq!(archive.statuses, [200]);
    assert_eq!(
        state.checkpoint,
        Some(EnrichmentCheckpoint::with_budget_state(
            None, 1, 1, 1, 1, false
        ))
    );
}

#[tokio::test]
async fn enrichment_persists_hard_budget_for_transport_failures() {
    let mut transport = FakeHistoryTransport {
        responses: VecDeque::from([Err(CollectorError::Transport)]),
        ..Default::default()
    };
    let mut archive = FakeHistoryArchive::default();
    let mut state = FakeEnrichmentState::default();
    let mut cache = FakeFinalizedCache::default();
    let request = EnrichmentRequest::try_new(
        target(Chain::Ethereum),
        EnrichmentMode::Automatic,
        1,
        1,
        "2026-09-29",
    )
    .unwrap();

    let error = BuyerHistoryEnricher::new(&mut transport, &mut archive, &mut state, &mut cache)
        .enrich(request)
        .await
        .expect_err("transport failure must stop enrichment");

    assert_eq!(error, CollectorError::Transport);
    assert_eq!(transport.requests.len(), 1);
    assert_eq!(
        state.checkpoint,
        Some(EnrichmentCheckpoint::with_budget_state(
            None, 1, 1, 1, 1, false
        ))
    );
}

#[tokio::test]
async fn completed_enrichment_restarts_from_head_for_new_history() {
    const EMPTY_HEAD: &[u8] = br#"{"jsonrpc":"2.0","id":1,"result":{"transfers":[]}}"#;
    let mut transport = FakeHistoryTransport {
        responses: VecDeque::from([RawRpcResponse::try_new(200, EMPTY_HEAD.to_vec())]),
        ..Default::default()
    };
    let mut archive = FakeHistoryArchive::default();
    let mut state = FakeEnrichmentState {
        checkpoint: Some(EnrichmentCheckpoint::new(None, 7, 9, true)),
    };
    let mut cache = FakeFinalizedCache::default();
    let request = EnrichmentRequest::try_new(
        target(Chain::Base),
        EnrichmentMode::Automatic,
        1,
        1,
        "2026-09-29",
    )
    .unwrap();

    let report = BuyerHistoryEnricher::new(&mut transport, &mut archive, &mut state, &mut cache)
        .enrich(request)
        .await
        .unwrap();

    assert_eq!(
        transport.requests,
        [(Chain::Base, "buyer:base:alice".to_owned(), None)]
    );
    assert_eq!(report.requests_used(), 1);
    assert!(report.complete());
    assert_eq!(state.checkpoint.unwrap().requests_used_total(), 10);
}
