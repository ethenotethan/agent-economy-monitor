use std::sync::Arc;

use agent_economy_monitor::worker::{
    PostgresWorkerJobStore, WorkerJobStore, WorkerMode, WorkerStoreError,
};
use tokio_postgres::{Client, NoTls};

const NAMESPACE: &str = "00000000-0000-0000-0000-000000000047";
const MIGRATIONS_BEFORE_WORKER_JOBS: &[&str] = &[
    include_str!("../migrations/0001_knowledge_graph.up.sql"),
    include_str!("../migrations/0002_operational_analytics.up.sql"),
    include_str!("../migrations/0003_shadow_catalog.up.sql"),
    include_str!("../migrations/0004_buyer_enrichment.up.sql"),
    include_str!("../migrations/0005_reducer_finality.up.sql"),
    include_str!("../migrations/0006_settlement_attribution.up.sql"),
    include_str!("../migrations/0007_buyer_classification.up.sql"),
    include_str!("../migrations/0008_query_read_models.up.sql"),
    include_str!("../migrations/0009_cockpit_auth.up.sql"),
    include_str!("../migrations/0010_classification_promotions.up.sql"),
    include_str!("../migrations/0011_semantic_projection.up.sql"),
];
const WORKER_JOBS_MIGRATION: &str = include_str!("../migrations/0012_worker_jobs.up.sql");

async fn connect(database_url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

async fn connect_worker(database_url: &str) -> Client {
    let client = connect(database_url).await;
    client
        .batch_execute("SET ROLE agent_economy_worker")
        .await
        .unwrap();
    client
}

async fn insert_job(client: &Client, job_id: &str, mode: &str, max_attempts: i16) {
    client
        .execute(
            "INSERT INTO agent_economy.worker_jobs \
               (namespace_id, job_id, mode, job_kind, idempotency_key, input_sha256, max_attempts) \
             VALUES ($1::text::uuid, $2::text::uuid, $3, 'integration', $2, $4, $5)",
            &[&NAMESPACE, &job_id, &mode, &"a".repeat(64), &max_attempts],
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires a fresh AEM_WORKER_PRIVILEGE_TEST_DATABASE_URL with role creation authority"]
async fn migration_revokes_worker_table_privileges_inherited_from_owner_defaults() {
    let database_url = std::env::var("AEM_WORKER_PRIVILEGE_TEST_DATABASE_URL").unwrap();
    let owner = connect(&database_url).await;
    for migration in MIGRATIONS_BEFORE_WORKER_JOBS {
        owner.batch_execute(migration).await.unwrap();
    }

    owner
        .batch_execute(
            "CREATE ROLE agent_economy_worker NOLOGIN; \
             ALTER DEFAULT PRIVILEGES IN SCHEMA agent_economy \
             GRANT SELECT, UPDATE ON TABLES TO agent_economy_worker;",
        )
        .await
        .unwrap();
    owner.batch_execute(WORKER_JOBS_MIGRATION).await.unwrap();
    owner
        .execute(
            "INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key) \
             VALUES ($1::text::uuid, 'tenant', 'worker-privilege-test')",
            &[&NAMESPACE],
        )
        .await
        .unwrap();
    let job_id = "00000000-0000-0000-0000-000000000647";
    insert_job(&owner, job_id, "collect", 3).await;

    let worker = connect_worker(&database_url).await;
    let select_error = worker
        .query("SELECT status FROM agent_economy.worker_jobs", &[])
        .await
        .expect_err("the worker role must not read worker_jobs directly");
    assert_eq!(
        select_error.code(),
        Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE)
    );
    let update_error = worker
        .execute(
            "UPDATE agent_economy.worker_jobs SET status = 'cancelled', \
             last_error_code = 'bypass' WHERE job_id = $1::text::uuid",
            &[&job_id],
        )
        .await
        .expect_err("the worker role must not mutate worker_jobs directly");
    assert_eq!(
        update_error.code(),
        Some(&tokio_postgres::error::SqlState::INSUFFICIENT_PRIVILEGE)
    );

    let claimed = worker
        .query_one(
            "SELECT job_id::text FROM agent_economy.claim_worker_job( \
             $1::text::uuid, 'collect', 'worker-privilege-test', 60)",
            &[&NAMESPACE],
        )
        .await
        .expect("the worker role must retain function-only lease authority")
        .get::<_, String>(0);
    assert_eq!(claimed, job_id);
}

#[tokio::test]
#[ignore = "requires a fresh AEM_WORKER_TRANSITIVE_PRIVILEGE_TEST_DATABASE_URL with role creation authority"]
async fn migration_rejects_worker_role_with_inherited_table_authority() {
    let database_url = std::env::var("AEM_WORKER_TRANSITIVE_PRIVILEGE_TEST_DATABASE_URL").unwrap();
    let owner = connect(&database_url).await;
    for migration in MIGRATIONS_BEFORE_WORKER_JOBS {
        owner.batch_execute(migration).await.unwrap();
    }

    owner
        .batch_execute(
            "CREATE ROLE aem_rogue_defaults NOLOGIN; \
             CREATE ROLE agent_economy_worker NOLOGIN; \
             GRANT aem_rogue_defaults TO agent_economy_worker; \
             ALTER DEFAULT PRIVILEGES IN SCHEMA agent_economy \
             GRANT SELECT, UPDATE ON TABLES TO aem_rogue_defaults;",
        )
        .await
        .unwrap();

    let migration_error = owner
        .batch_execute(WORKER_JOBS_MIGRATION)
        .await
        .expect_err("unsafe inherited worker authority must fail the migration closed");
    assert!(
        migration_error
            .as_db_error()
            .is_some_and(|error| error.message().contains("must not inherit roles")),
        "unexpected migration failure: {migration_error}"
    );
    owner.batch_execute("ROLLBACK").await.unwrap();
    let worker_jobs_exists: bool = owner
        .query_one(
            "SELECT to_regclass('agent_economy.worker_jobs') IS NOT NULL",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(
        !worker_jobs_exists,
        "the failed migration transaction must not leave worker_jobs behind"
    );
}

#[tokio::test]
#[ignore = "requires AEM_WORKER_TEST_DATABASE_URL migrated through 0012"]
async fn postgres_leases_are_exclusive_reclaimable_bounded_and_cancellable() {
    let database_url = std::env::var("AEM_WORKER_TEST_DATABASE_URL").unwrap();
    let fixture = connect(&database_url).await;
    fixture
        .execute(
            "INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key) \
             VALUES ($1::text::uuid, 'tenant', 'worker-test') ON CONFLICT DO NOTHING",
            &[&NAMESPACE],
        )
        .await
        .unwrap();
    fixture
        .execute(
            "DELETE FROM agent_economy.worker_jobs WHERE namespace_id = $1::text::uuid",
            &[&NAMESPACE],
        )
        .await
        .unwrap();

    let first = Arc::new(PostgresWorkerJobStore::new(
        connect_worker(&database_url).await,
        NAMESPACE.into(),
    ));
    let second = Arc::new(PostgresWorkerJobStore::new(
        connect_worker(&database_url).await,
        NAMESPACE.into(),
    ));

    let reclaim_id = "00000000-0000-0000-0000-000000000147";
    insert_job(&fixture, reclaim_id, "collect", 3).await;
    let (left, right) = tokio::join!(
        first.claim(WorkerMode::Collect, "worker-a", 60),
        second.claim(WorkerMode::Collect, "worker-b", 60)
    );
    let (leased, empty_store) = match (left.unwrap(), right.unwrap()) {
        (Some(job), None) => (job, second.clone()),
        (None, Some(job)) => (job, first.clone()),
        result => panic!("one and only one worker must claim the live lease: {result:?}"),
    };

    fixture
        .execute(
            "UPDATE agent_economy.worker_jobs SET lease_expires_at = clock_timestamp() - interval '1 second' \
             WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid",
            &[&NAMESPACE, &reclaim_id],
        )
        .await
        .unwrap();
    let reclaimed = empty_store
        .claim(WorkerMode::Collect, "worker-c", 60)
        .await
        .unwrap()
        .expect("expired lease must be reclaimed once");
    assert_ne!(leased.lease_token, reclaimed.lease_token);
    assert_eq!(
        first.complete(&leased, &"b".repeat(64)).await,
        Err(WorkerStoreError::Conflict),
        "the expired lease token must not commit after reassignment"
    );
    empty_store
        .complete(&reclaimed, &"c".repeat(64))
        .await
        .unwrap();
    assert!(
        first
            .claim(WorkerMode::Collect, "worker-d", 60)
            .await
            .unwrap()
            .is_none()
    );

    let retry_id = "00000000-0000-0000-0000-000000000247";
    insert_job(&fixture, retry_id, "reduce", 2).await;
    let retry = first
        .claim(WorkerMode::Reduce, "worker-a", 60)
        .await
        .unwrap()
        .unwrap();
    first.fail(&retry, "temporary", true).await.unwrap();
    fixture
        .execute(
            "UPDATE agent_economy.worker_jobs SET scheduled_for = clock_timestamp() - interval '1 second' \
             WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid",
            &[&NAMESPACE, &retry_id],
        )
        .await
        .unwrap();
    let final_attempt = first
        .claim(WorkerMode::Reduce, "worker-a", 60)
        .await
        .unwrap()
        .unwrap();
    first
        .fail(&final_attempt, "still_temporary", true)
        .await
        .unwrap();
    let retry_status: String = fixture
        .query_one(
            "SELECT status FROM agent_economy.worker_jobs \
             WHERE namespace_id = $1::text::uuid AND job_id = $2::text::uuid",
            &[&NAMESPACE, &retry_id],
        )
        .await
        .unwrap()
        .get(0);
    assert_eq!(retry_status, "dead_letter");

    let poison_id = "00000000-0000-0000-0000-000000000347";
    insert_job(&fixture, poison_id, "classify", 5).await;
    let poison = first
        .claim(WorkerMode::Classify, "worker-a", 60)
        .await
        .unwrap()
        .unwrap();
    first.fail(&poison, "invalid_input", false).await.unwrap();

    let cancel_id = "00000000-0000-0000-0000-000000000447";
    insert_job(&fixture, cancel_id, "enrich", 3).await;
    let cancelled = first
        .claim(WorkerMode::Enrich, "worker-a", 2)
        .await
        .unwrap()
        .unwrap();
    first.renew(&cancelled, 60).await.unwrap();
    let mut foreign_lease = cancelled.clone();
    foreign_lease.lease_owner = "worker-b".into();
    foreign_lease.lease_token = "00000000-0000-0000-0000-000000000547".into();
    assert_eq!(
        second.cancel(&foreign_lease).await,
        Err(WorkerStoreError::Conflict),
        "a worker must not cancel another worker's live lease"
    );
    first.cancel(&cancelled).await.unwrap();
    assert_eq!(
        first.complete(&cancelled, &"d".repeat(64)).await,
        Err(WorkerStoreError::Conflict)
    );

    let statuses = fixture
        .query(
            "SELECT job_id::text, status FROM agent_economy.worker_jobs \
             WHERE namespace_id = $1::text::uuid ORDER BY job_id",
            &[&NAMESPACE],
        )
        .await
        .unwrap()
        .into_iter()
        .map(|row| (row.get::<_, String>(0), row.get::<_, String>(1)))
        .collect::<Vec<_>>();
    assert_eq!(
        statuses,
        vec![
            (reclaim_id.into(), "succeeded".into()),
            (retry_id.into(), "dead_letter".into()),
            (poison_id.into(), "dead_letter".into()),
            (cancel_id.into(), "cancelled".into()),
        ]
    );
}
