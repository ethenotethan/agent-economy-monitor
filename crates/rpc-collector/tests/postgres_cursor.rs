use agent_economy_rpc_collector::{
    Chain, CollectorError, CursorCheckpoint, CursorStore, PostgresCursorStore,
};

#[tokio::test]
async fn postgres_cursor_compare_and_set_rejects_stale_writer() {
    let Ok(database_url) = std::env::var("TEST_DATABASE_URL") else {
        return;
    };
    let (client_a, connection_a) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect to disposable PostgreSQL");
    tokio::spawn(async move {
        connection_a
            .await
            .expect("PostgreSQL connection remains healthy");
    });
    let (client_b, connection_b) = tokio_postgres::connect(&database_url, tokio_postgres::NoTls)
        .await
        .expect("connect a second independent PostgreSQL client");
    tokio::spawn(async move {
        connection_b
            .await
            .expect("second PostgreSQL connection remains healthy");
    });
    client_a
        .batch_execute(agent_economy_rpc_collector::POSTGRES_CURSOR_SCHEMA)
        .await
        .expect("cursor migration applies");
    client_a
        .execute(
            "DELETE FROM rpc_collection_cursors WHERE chain = $1",
            &[&Chain::Ethereum.as_str()],
        )
        .await
        .expect("disposable cursor starts clean");
    let mut cursors_a = PostgresCursorStore::new(client_a);
    let mut cursors_b = PostgresCursorStore::new(client_b);

    let (initialized_a, initialized_b) = tokio::join!(
        cursors_a.initialize(Chain::Ethereum, 700),
        cursors_b.initialize(Chain::Ethereum, 900)
    );
    let initialized_a = initialized_a.expect("first initialization completes");
    let initialized_b = initialized_b.expect("concurrent initialization completes");
    assert_eq!(initialized_a, initialized_b);
    assert!(matches!(initialized_a.next_height(), 700 | 900));

    let next_height = initialized_a.next_height() + 1;
    let (advanced_a, advanced_b) = tokio::join!(
        cursors_a.compare_and_set(Chain::Ethereum, initialized_a, next_height),
        cursors_b.compare_and_set(Chain::Ethereum, initialized_b, next_height)
    );
    let successes = usize::from(advanced_a.is_ok()) + usize::from(advanced_b.is_ok());
    let conflicts = usize::from(advanced_a == Err(CollectorError::CursorConflict))
        + usize::from(advanced_b == Err(CollectorError::CursorConflict));

    assert_eq!(successes, 1);
    assert_eq!(conflicts, 1);
    assert_eq!(
        cursors_a.load(Chain::Ethereum).await.unwrap(),
        Some(CursorCheckpoint::new(next_height, 1))
    );
}
