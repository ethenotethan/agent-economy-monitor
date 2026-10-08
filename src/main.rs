use std::{env, io, net::SocketAddr, path::PathBuf, sync::Arc};

use agent_economy_evidence_store::{
    FilesystemEvidenceStore, GcsEvidenceStore, GcsRetryPolicy, GoogleCloudStorageClient,
};
use agent_economy_monitor::{
    auth::{AuthState, PostgresAuthStore, protect_router},
    classification_promotion::{PostgresClassificationPromotionStore, PromotionRequest},
    classify::{ClassificationHandler, PostgresClassificationStore},
    cockpit::mount_cockpit,
    collect::{
        CollectionEvidenceStore, CollectionHandler, PostgresCollectionCommitStore,
        PostgresCollectionJobStore,
    },
    enrich::{EnrichmentHandler, EvidenceStoreHistoryArchive, PostgresEnrichmentStore},
    projection_gateway::{
        PostgresProjectionGatewayStore, ProjectionGatewayState, mount_projection_gateway,
    },
    projection_runtime::{ProjectionRuntimeConfig, run_projection_once},
    query::{PostgresQueryStore, api_router},
    reduce::{PostgresReductionStore, ReductionHandler},
    verify_evidence::{CollectionEvidenceReader, PostgresEvidenceVerifier},
    worker::{WorkerDispatcher, WorkerMode},
};
use agent_economy_rpc_collector::{
    AlchemyTransport, Chain, CollectorError, RawRpcResponse, RpcEndpoint, RpcTransport,
};
use axum::{Json, Router, routing::get};
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_postgres::NoTls;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Serialize)]
struct Status {
    service: &'static str,
    status: &'static str,
    version: &'static str,
}

enum CollectionRpcTransport {
    Alchemy(AlchemyTransport),
    #[cfg(debug_assertions)]
    Replay(PathBuf),
}

impl RpcTransport for CollectionRpcTransport {
    async fn fetch_block(
        &mut self,
        chain: Chain,
        height: u64,
    ) -> Result<RawRpcResponse, CollectorError> {
        match self {
            Self::Alchemy(transport) => transport.fetch_block(chain, height).await,
            #[cfg(debug_assertions)]
            Self::Replay(root) => {
                let bytes = tokio::fs::read(root.join(format!("{}-{height}.json", chain.as_str())))
                    .await
                    .map_err(|_| CollectorError::Transport)?;
                RawRpcResponse::try_new(200, bytes)
            }
        }
    }
}

async fn status() -> Json<Status> {
    Json(Status {
        service: "agent-economy-monitor",
        status: "ok",
        version: env!("CARGO_PKG_VERSION"),
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let mut arguments = env::args().skip(1);
    match arguments.next().as_deref() {
        Some("project-wiki") => {
            let config = ProjectionRuntimeConfig::from_env()
                .map_err(|message| io::Error::new(io::ErrorKind::InvalidInput, message))?;
            match run_projection_once(config).await? {
                Some(projection) => {
                    info!(
                        job_id = %projection.payload.job_id,
                        bundle_sha256 = %projection.bundle_sha256,
                        "published approved semantic projection"
                    );
                }
                None => info!("no semantic projection job available"),
            }
            Ok(())
        }
        Some("promote-classification") => {
            let request = PromotionRequest::from_args(arguments)?;
            let database_url = env::var("DATABASE_URL")?;
            let namespace_id = env::var("NAMESPACE_ID")?;
            let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
            tokio::spawn(async move {
                if connection.await.is_err() {
                    tracing::error!("PostgreSQL promotion connection closed unexpectedly");
                }
            });
            let store = PostgresClassificationPromotionStore::new(
                std::sync::Arc::new(client),
                namespace_id,
            )?;
            let promotion_sequence = store.promote(&request).await?;
            info!(promotion_sequence, "classification run promoted");
            Ok(())
        }
        Some("collect") => {
            reject_mixed_worker_authority("collect")?;
            run_collect_once().await
        }
        Some("verify-evidence") => {
            reject_mixed_worker_authority("verify-evidence")?;
            run_verify_evidence_once().await
        }
        Some("reduce") => {
            reject_mixed_worker_authority("reduce")?;
            run_reduce_once().await
        }
        Some("classify") => {
            reject_mixed_worker_authority("classify")?;
            run_classify_once().await
        }
        Some("enrich") => {
            reject_mixed_worker_authority("enrich")?;
            run_enrich_once().await
        }
        None | Some("serve") => serve().await,
        Some(command) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown command: {command}"),
        )
        .into()),
    }
}

fn reject_mixed_worker_authority(mode: &str) -> Result<(), io::Error> {
    if env::var_os("NAMESPACE_ID").is_some() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "collection runtimes derive namespace from session_user; NAMESPACE_ID is forbidden",
        ));
    }
    let collector_authority = [
        "COLLECTOR_DATABASE_URL",
        "COLLECTION_INPUT_ROOT",
        "EVIDENCE_WRITE_ROOT",
        "EVIDENCE_WRITE_BUCKET",
        "ALCHEMY_ETHEREUM_RPC_URL",
        "ALCHEMY_BASE_RPC_URL",
        "ALCHEMY_SOLANA_RPC_URL",
        "ALCHEMY_TEMPO_RPC_URL",
        "COLLECTION_RPC_REPLAY_ROOT",
    ];
    let verifier_authority = [
        "EVIDENCE_VERIFIER_DATABASE_URL",
        "EVIDENCE_READ_ROOT",
        "EVIDENCE_READ_BUCKET",
    ];
    let mixed = match mode {
        "collect" => verifier_authority
            .iter()
            .any(|name| env::var_os(name).is_some()),
        "verify-evidence" => collector_authority
            .iter()
            .any(|name| env::var_os(name).is_some()),
        "reduce" => collector_authority
            .iter()
            .chain(verifier_authority.iter())
            .any(|name| env::var_os(name).is_some()),
        "classify" => collector_authority
            .iter()
            .chain(verifier_authority.iter())
            .chain(["REDUCER_DATABASE_URL", "ENRICHER_DATABASE_URL"].iter())
            .any(|name| env::var_os(name).is_some()),
        "enrich" => [
            "COLLECTOR_DATABASE_URL",
            "COLLECTION_INPUT_ROOT",
            "COLLECTION_RPC_REPLAY_ROOT",
            "EVIDENCE_VERIFIER_DATABASE_URL",
            "EVIDENCE_READ_ROOT",
            "EVIDENCE_READ_BUCKET",
            "REDUCER_DATABASE_URL",
            "CLASSIFIER_DATABASE_URL",
        ]
        .iter()
        .any(|name| env::var_os(name).is_some()),
        _ => false,
    };
    if mixed {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "mixed collection and verification authority is forbidden",
        ));
    }
    Ok(())
}

async fn run_classify_once() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = required_collection_env("CLASSIFIER_DATABASE_URL")?;
    let lease_owner = env::var("CLASSIFY_LEASE_OWNER")
        .unwrap_or_else(|_| format!("agent-economy-classifier:{}", std::process::id()));
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::error!("PostgreSQL classifier connection closed unexpectedly");
        }
    });
    let session_user = client
        .query_one("SELECT session_user", &[])
        .await?
        .get::<_, String>(0);
    if session_user != "agent_economy_classifier_runtime" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "CLASSIFIER_DATABASE_URL must authenticate as agent_economy_classifier_runtime",
        )
        .into());
    }
    client
        .query_one("SELECT agent_economy.bound_classifier_namespace()", &[])
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsafe classifier database authority",
            )
        })?;
    let store = Arc::new(PostgresClassificationStore::new(client));
    let handler = Arc::new(ClassificationHandler::new(Arc::clone(&store)));
    let completed = WorkerDispatcher::new(store)
        .register(WorkerMode::Classify, handler)
        .run_once(WorkerMode::Classify, &lease_owner)
        .await?;
    info!(
        job_id = %completed.job_id,
        output_sha256 = %completed.output_sha256,
        "classification job completed"
    );
    Ok(())
}

async fn run_enrich_once() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = required_collection_env("ENRICHER_DATABASE_URL")?;
    let evidence_store: Arc<dyn CollectionEvidenceStore> = match (
        env::var("EVIDENCE_WRITE_ROOT").ok(),
        env::var("EVIDENCE_WRITE_BUCKET").ok(),
    ) {
        (Some(root), None) => Arc::new(FilesystemEvidenceStore::open(PathBuf::from(root))?),
        (None, Some(bucket)) => Arc::new(GcsEvidenceStore::new(
            GoogleCloudStorageClient::from_application_default_credentials().await?,
            &bucket,
            GcsRetryPolicy::new(3)?,
        )?),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "configure exactly one create-only evidence backend: EVIDENCE_WRITE_ROOT or EVIDENCE_WRITE_BUCKET",
            )
            .into());
        }
    };
    let transport = AlchemyTransport::new([
        (
            Chain::Ethereum,
            RpcEndpoint::parse(&required_collection_env("ALCHEMY_ETHEREUM_RPC_URL")?)?,
        ),
        (
            Chain::Base,
            RpcEndpoint::parse(&required_collection_env("ALCHEMY_BASE_RPC_URL")?)?,
        ),
        (
            Chain::Solana,
            RpcEndpoint::parse(&required_collection_env("ALCHEMY_SOLANA_RPC_URL")?)?,
        ),
        (
            Chain::Tempo,
            RpcEndpoint::parse(&required_collection_env("ALCHEMY_TEMPO_RPC_URL")?)?,
        ),
    ])?;
    let lease_owner = env::var("ENRICH_LEASE_OWNER")
        .unwrap_or_else(|_| format!("agent-economy-enricher:{}", std::process::id()));
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::error!("PostgreSQL enricher connection closed unexpectedly");
        }
    });
    let session_user = client
        .query_one("SELECT session_user", &[])
        .await?
        .get::<_, String>(0);
    if session_user != "agent_economy_enricher_runtime" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "ENRICHER_DATABASE_URL must authenticate as agent_economy_enricher_runtime",
        )
        .into());
    }
    client
        .query_one("SELECT agent_economy.bound_enricher_namespace()", &[])
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsafe enricher database authority",
            )
        })?;
    let store = Arc::new(PostgresEnrichmentStore::new(client));
    let archive = Arc::new(EvidenceStoreHistoryArchive::new(evidence_store));
    let handler = Arc::new(EnrichmentHandler::new(
        transport,
        archive,
        Arc::clone(&store),
    ));
    let completed = WorkerDispatcher::new(store)
        .register(WorkerMode::Enrich, handler)
        .run_once(WorkerMode::Enrich, &lease_owner)
        .await?;
    info!(
        job_id = %completed.job_id,
        output_sha256 = %completed.output_sha256,
        "buyer enrichment job completed"
    );
    Ok(())
}

async fn run_reduce_once() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = env::var("REDUCER_DATABASE_URL").map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "missing reducer configuration: REDUCER_DATABASE_URL",
        )
    })?;
    let lease_owner = env::var("REDUCE_LEASE_OWNER")
        .unwrap_or_else(|_| format!("agent-economy-reducer:{}", std::process::id()));
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::error!("PostgreSQL reducer connection closed unexpectedly");
        }
    });
    let session_user = client
        .query_one("SELECT session_user", &[])
        .await?
        .get::<_, String>(0);
    if session_user != "agent_economy_reducer_runtime" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "REDUCER_DATABASE_URL must authenticate as agent_economy_reducer_runtime",
        )
        .into());
    }
    client
        .query_one("SELECT agent_economy.bound_reducer_namespace()", &[])
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsafe reducer database authority",
            )
        })?;
    let store = Arc::new(PostgresReductionStore::new(client));
    let handler = Arc::new(ReductionHandler::new(store.clone(), store.clone()));
    let completed = WorkerDispatcher::new(store)
        .register(WorkerMode::Reduce, handler)
        .run_once(WorkerMode::Reduce, &lease_owner)
        .await?;
    info!(
        job_id = %completed.job_id,
        output_sha256 = %completed.output_sha256,
        "reduction job completed"
    );
    Ok(())
}

async fn run_verify_evidence_once() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = required_collection_env("EVIDENCE_VERIFIER_DATABASE_URL")?;
    let lease_owner = env::var("VERIFY_EVIDENCE_LEASE_OWNER")
        .unwrap_or_else(|_| format!("agent-economy-verifier:{}", std::process::id()));
    let evidence_store: Arc<dyn CollectionEvidenceReader> = match (
        env::var("EVIDENCE_READ_ROOT").ok(),
        env::var("EVIDENCE_READ_BUCKET").ok(),
    ) {
        (Some(root), None) => Arc::new(FilesystemEvidenceStore::open(PathBuf::from(root))?),
        (None, Some(bucket)) => Arc::new(GcsEvidenceStore::new(
            GoogleCloudStorageClient::from_application_default_credentials().await?,
            &bucket,
            GcsRetryPolicy::new(3)?,
        )?),
        _ => return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "configure exactly one read-only evidence backend: EVIDENCE_READ_ROOT or EVIDENCE_READ_BUCKET",
        ).into()),
    };
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::error!("PostgreSQL evidence verifier connection closed unexpectedly");
        }
    });
    let session_user = client
        .query_one("SELECT session_user", &[])
        .await?
        .get::<_, String>(0);
    if session_user != "agent_economy_evidence_verifier_runtime" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "EVIDENCE_VERIFIER_DATABASE_URL must authenticate as agent_economy_evidence_verifier_runtime",
        ).into());
    }
    client
        .query_one(
            "SELECT agent_economy.bound_collection_namespace('verify-evidence')",
            &[],
        )
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsafe evidence verifier database authority",
            )
        })?;
    let verifier = PostgresEvidenceVerifier::new(client, evidence_store, lease_owner);
    match verifier.run_once().await? {
        Some(pending_id) => {
            info!(%pending_id, "evidence batch independently verified and promoted")
        }
        None => info!("no pending evidence batch available"),
    }
    Ok(())
}

async fn run_collect_once() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = required_collection_env("COLLECTOR_DATABASE_URL")?;
    let input_root = PathBuf::from(required_collection_env("COLLECTION_INPUT_ROOT")?);
    let lease_owner = env::var("COLLECT_LEASE_OWNER")
        .unwrap_or_else(|_| format!("agent-economy-monitor:{}", std::process::id()));
    let evidence_store: Arc<dyn CollectionEvidenceStore> = match (
        env::var("EVIDENCE_WRITE_ROOT").ok(),
        env::var("EVIDENCE_WRITE_BUCKET").ok(),
    ) {
        (Some(root), None) => Arc::new(FilesystemEvidenceStore::open(PathBuf::from(root))?),
        (None, Some(bucket)) => Arc::new(GcsEvidenceStore::new(
            GoogleCloudStorageClient::from_application_default_credentials().await?,
            &bucket,
            GcsRetryPolicy::new(3)?,
        )?),
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "configure exactly one create-only evidence backend: EVIDENCE_WRITE_ROOT or EVIDENCE_WRITE_BUCKET",
            )
            .into());
        }
    };

    let (collector_client, collector_connection) =
        tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if collector_connection.await.is_err() {
            tracing::error!("PostgreSQL collector runtime connection closed unexpectedly");
        }
    });
    let current_user = collector_client
        .query_one("SELECT session_user", &[])
        .await?
        .get::<_, String>(0);
    if current_user != "agent_economy_collector_runtime" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "COLLECTOR_DATABASE_URL must authenticate as agent_economy_collector_runtime",
        )
        .into());
    }
    collector_client
        .query_one(
            "SELECT agent_economy.bound_collection_namespace('collect')",
            &[],
        )
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "unsafe collector database authority",
            )
        })?;

    let shared_client = Arc::new(tokio::sync::Mutex::new(collector_client));
    let jobs = Arc::new(PostgresCollectionJobStore::from_shared(Arc::clone(
        &shared_client,
    )));
    let commits = Arc::new(PostgresCollectionCommitStore::from_shared(shared_client));
    let rpc_transport = collection_rpc_transport()?;
    let handler = Arc::new(CollectionHandler::new(
        input_root,
        evidence_store,
        commits,
        rpc_transport,
    ));
    let completed = WorkerDispatcher::new(jobs)
        .register(WorkerMode::Collect, handler)
        .run_once(WorkerMode::Collect, &lease_owner)
        .await?;
    info!(
        job_id = %completed.job_id,
        output_sha256 = %completed.output_sha256,
        "collection job completed"
    );
    Ok(())
}

fn collection_rpc_transport() -> Result<CollectionRpcTransport, Box<dyn std::error::Error>> {
    #[cfg(debug_assertions)]
    if let Ok(root) = env::var("COLLECTION_RPC_REPLAY_ROOT") {
        return Ok(CollectionRpcTransport::Replay(PathBuf::from(root)));
    }
    Ok(CollectionRpcTransport::Alchemy(AlchemyTransport::new([
        (
            Chain::Ethereum,
            RpcEndpoint::parse(&required_collection_env("ALCHEMY_ETHEREUM_RPC_URL")?)?,
        ),
        (
            Chain::Base,
            RpcEndpoint::parse(&required_collection_env("ALCHEMY_BASE_RPC_URL")?)?,
        ),
        (
            Chain::Solana,
            RpcEndpoint::parse(&required_collection_env("ALCHEMY_SOLANA_RPC_URL")?)?,
        ),
        (
            Chain::Tempo,
            RpcEndpoint::parse(&required_collection_env("ALCHEMY_TEMPO_RPC_URL")?)?,
        ),
    ])?))
}

fn required_collection_env(name: &'static str) -> Result<String, io::Error> {
    env::var(name).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("missing collection configuration: {name}"),
        )
    })
}

async fn serve() -> Result<(), Box<dyn std::error::Error>> {
    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(address).await?;
    let database_url = env::var("DATABASE_URL")?;
    let namespace_id = env::var("NAMESPACE_ID")?;

    let (auth_client, auth_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if auth_connection.await.is_err() {
            tracing::error!("PostgreSQL authentication connection closed unexpectedly");
        }
    });
    let auth_store = std::sync::Arc::new(PostgresAuthStore::new(
        std::sync::Arc::new(auth_client),
        namespace_id.clone(),
    ));
    let auth = AuthState::from_env(auth_store)?;

    let (query_client, query_connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    query_client
        .batch_execute("SET ROLE agent_economy_dashboard_reader")
        .await?;
    tokio::spawn(async move {
        if query_connection.await.is_err() {
            tracing::error!("PostgreSQL query connection closed unexpectedly");
        }
    });
    let query_store = PostgresQueryStore::new(query_client, namespace_id.clone());
    let (gateway_client, gateway_connection) =
        tokio_postgres::connect(&database_url, NoTls).await?;
    gateway_client
        .batch_execute("SET ROLE agent_economy_projection_writer")
        .await?;
    tokio::spawn(async move {
        if gateway_connection.await.is_err() {
            tracing::error!("PostgreSQL projection gateway connection closed unexpectedly");
        }
    });
    let gateway_token = env::var("PROJECTION_GATEWAY_TOKEN")?;
    let lease_owner =
        env::var("PROJECTION_LEASE_OWNER").unwrap_or_else(|_| "agent-economy-monitor".into());
    let gateway = ProjectionGatewayState::new(
        std::sync::Arc::new(PostgresProjectionGatewayStore::new(
            gateway_client,
            namespace_id.clone(),
        )),
        &gateway_token,
        &lease_owner,
    );
    let app: Router =
        api_router(std::sync::Arc::new(query_store)).route("/api/v1/status", get(status));
    let app = mount_cockpit(app);
    let app = protect_router(app, auth).route("/healthz", get(status));
    let app = mount_projection_gateway(app, gateway);

    info!(%address, "agent economy monitor listening");
    axum::serve(listener, app).await?;
    Ok(())
}
