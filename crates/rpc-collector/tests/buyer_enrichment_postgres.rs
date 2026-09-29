use std::collections::VecDeque;

use agent_economy_rpc_collector::{
    BuyerHistoryEnricher, BuyerHistoryTarget, Chain, CollectorError, EnrichmentMode,
    EnrichmentRequest, EnrichmentStateStore, HistoryEvidenceArchive, HistoryTransport,
    POSTGRES_BUYER_ENRICHMENT_SCHEMA, PostgresEnrichmentStateStore, PostgresFinalizedHistoryCache,
    RawRpcResponse,
};

const KNOWLEDGE_SCHEMA: &str = include_str!("../../../migrations/0001_knowledge_graph.up.sql");
const ANALYTICS_SCHEMA: &str =
    include_str!("../../../migrations/0002_operational_analytics.up.sql");

struct FakeHistoryTransport {
    responses: VecDeque<RawRpcResponse>,
}

impl HistoryTransport for FakeHistoryTransport {
    async fn fetch_history(
        &mut self,
        _target: &BuyerHistoryTarget,
        _cursor: Option<&str>,
    ) -> Result<RawRpcResponse, CollectorError> {
        self.responses.pop_front().ok_or(CollectorError::Transport)
    }
}

struct FakeHistoryArchive {
    evidence_ids: VecDeque<String>,
}

impl HistoryEvidenceArchive for FakeHistoryArchive {
    fn archive_history(
        &mut self,
        _target: &BuyerHistoryTarget,
        _observed_date: &str,
        _http_status: u16,
        _body: &[u8],
    ) -> Result<String, CollectorError> {
        self.evidence_ids
            .pop_front()
            .ok_or(CollectorError::FinalizedHistoryStorage)
    }
}

#[tokio::test]
async fn postgres_enrichment_state_is_chain_scoped_and_finalized_cache_is_immutable() {
    let Ok(database_url) = std::env::var("TEST_DATABASE_URL") else {
        return;
    };
    let (setup, setup_connection) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to disposable PostgreSQL");
    tokio::spawn(async move {
        setup_connection
            .await
            .expect("setup connection remains healthy");
    });
    setup
        .batch_execute("DROP SCHEMA IF EXISTS agent_economy CASCADE")
        .await
        .unwrap();
    setup.batch_execute(KNOWLEDGE_SCHEMA).await.unwrap();
    setup.batch_execute(ANALYTICS_SCHEMA).await.unwrap();
    setup
        .batch_execute(POSTGRES_BUYER_ENRICHMENT_SCHEMA)
        .await
        .unwrap();
    setup
        .batch_execute(
            "INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
             VALUES ('00000000-0000-0000-0000-000000000001', 'tenant', 'test');
             INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
             VALUES
                ('00000000-0000-0000-0000-000000000001', 'evidence:seed',
                 repeat('a', 64), 'evidence://seed', 'application/json', 2, now()),
                ('00000000-0000-0000-0000-000000000001', 'evidence/history/1',
                 repeat('b', 64), 'evidence://history/1', 'application/json', 2, now()),
                ('00000000-0000-0000-0000-000000000001', 'evidence/history/2',
                 repeat('c', 64), 'evidence://history/2', 'application/json', 2, now());
             INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at, parser_version,
                 chain_scope, evidence_id)
             VALUES
                ('00000000-0000-0000-0000-000000000001',
                 '00000000-0000-0000-0000-000000000010', 'rpc-primary', now(),
                 'test@1', 'base', 'evidence:seed'),
                ('00000000-0000-0000-0000-000000000001',
                 '00000000-0000-0000-0000-000000000011', 'rpc-primary', now(),
                 'test@1', 'solana', 'evidence:seed');
             INSERT INTO agent_economy.buyer_handles
                (namespace_id, buyer_handle_id, handle_kind, chain_scope,
                 handle_value, provenance_id)
             VALUES
                ('00000000-0000-0000-0000-000000000001', 'buyer:base:alice',
                 'wallet', 'base', 'alice',
                 '00000000-0000-0000-0000-000000000010'),
                ('00000000-0000-0000-0000-000000000001', 'buyer:solana:alice',
                 'public_key', 'solana', 'alice',
                 '00000000-0000-0000-0000-000000000011');",
        )
        .await
        .unwrap();

    let (state_client, state_connection) =
        tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move { state_connection.await.unwrap() });
    let (cache_client, cache_connection) =
        tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
            .await
            .unwrap();
    tokio::spawn(async move { cache_connection.await.unwrap() });
    let mut state = PostgresEnrichmentStateStore::new(state_client);
    let mut cache = PostgresFinalizedHistoryCache::new(cache_client);
    let base = BuyerHistoryTarget::try_new(
        "00000000-0000-0000-0000-000000000001",
        "buyer:base:alice",
        Chain::Base,
        "alice",
    )
    .unwrap();
    let solana = BuyerHistoryTarget::try_new(
        "00000000-0000-0000-0000-000000000001",
        "buyer:solana:alice",
        Chain::Solana,
        "alice",
    )
    .unwrap();
    let mismatched = BuyerHistoryTarget::try_new(
        "00000000-0000-0000-0000-000000000001",
        "buyer:base:alice",
        Chain::Base,
        "mallory",
    )
    .unwrap();

    assert!(state.initialize(&mismatched).await.is_err());
    let base_initial = state.initialize(&base).await.unwrap();
    assert!(
        state
            .record_request(&mismatched, &base_initial, 10, 1)
            .await
            .is_err(),
        "a caller-selected handle value must remain bound at every CAS boundary"
    );
    let base_recorded = state
        .record_request(&base, &base_initial, 10, 1)
        .await
        .unwrap();
    let base_next = state
        .compare_and_set(&base, &base_recorded, Some("base-next".to_owned()), false)
        .await
        .unwrap();
    let solana_initial = state.initialize(&solana).await.unwrap();
    assert_eq!(state.load(&base).await.unwrap(), Some(base_next.clone()));
    assert_eq!(state.load(&solana).await.unwrap(), Some(solana_initial));

    const TRANSACTION: &str = "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    let response = |block: &str| {
        RawRpcResponse::try_new(
            200,
            format!(
                r#"{{"jsonrpc":"2.0","id":1,"result":{{"transfers":[{{"hash":"{TRANSACTION}","blockNum":"{block}","from":"alice"}}]}}}}"#
            )
            .into_bytes(),
        )
    };
    let mut transport = FakeHistoryTransport {
        responses: VecDeque::from([
            response("0xa").unwrap(),
            response("0xa").unwrap(),
            response("0xb").unwrap(),
        ]),
    };
    let mut archive = FakeHistoryArchive {
        evidence_ids: VecDeque::from([
            "evidence/history/1".to_owned(),
            "evidence/history/2".to_owned(),
            "evidence/history/2".to_owned(),
        ]),
    };
    for _ in 0..2 {
        BuyerHistoryEnricher::new(&mut transport, &mut archive, &mut state, &mut cache)
            .enrich(
                EnrichmentRequest::try_new(
                    base.clone(),
                    EnrichmentMode::Automatic,
                    1,
                    1,
                    "2026-09-29",
                )
                .unwrap(),
            )
            .await
            .unwrap();
    }
    let conflicting =
        BuyerHistoryEnricher::new(&mut transport, &mut archive, &mut state, &mut cache)
            .enrich(
                EnrichmentRequest::try_new(
                    base.clone(),
                    EnrichmentMode::Automatic,
                    1,
                    1,
                    "2026-09-29",
                )
                .unwrap(),
            )
            .await;
    assert_eq!(conflicting, Err(CollectorError::FinalizedHistoryConflict));
    let cached: i64 = setup
        .query_one(
            "SELECT count(*) FROM agent_economy.buyer_finalized_history",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(cached, 1);
    let evidence_links: i64 = setup
        .query_one(
            "SELECT count(*) FROM agent_economy.buyer_finalized_history_evidence",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(evidence_links, 2);
    let mutation = setup
        .execute(
            "UPDATE agent_economy.buyer_finalized_history
             SET block_reference = 'rewritten'",
            &[],
        )
        .await;
    assert!(mutation.is_err());
    let protocol_columns: i64 = setup
        .query_one(
            "SELECT count(*) FROM information_schema.columns
             WHERE table_schema = 'agent_economy'
               AND table_name = 'buyer_finalized_history'
               AND column_name = 'protocol'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(protocol_columns, 0);
}
