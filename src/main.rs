use std::{env, io, net::SocketAddr, path::PathBuf, sync::Arc};

use agent_economy_evidence_store::{
    FilesystemEvidenceStore, GcsEvidenceStore, GcsRetryPolicy, GoogleCloudStorageClient,
};
use agent_economy_monitor::{
    auth::{AuthState, PostgresAuthStore, protect_router},
    classification_promotion::{PostgresClassificationPromotionStore, PromotionRequest},
    cockpit::mount_cockpit,
    collect::{
        CollectionEvidenceStore, CollectionHandler, PostgresCollectionCommitStore,
        PostgresCollectionJobStore,
    },
    projection_gateway::{
        PostgresProjectionGatewayStore, ProjectionGatewayState, mount_projection_gateway,
    },
    projection_runtime::{ProjectionRuntimeConfig, run_projection_once},
    query::{PostgresQueryStore, api_router},
    worker::{WorkerDispatchError, WorkerDispatcher, WorkerMode},
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
        Some("collect") => run_collect_once().await,
        Some(command @ ("reduce" | "classify" | "enrich")) => {
            let mode = WorkerMode::parse(command)?;
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                WorkerDispatchError::MissingHandler(mode),
            )
            .into())
        }
        None | Some("serve") => serve().await,
        Some(command) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("unknown command: {command}"),
        )
        .into()),
    }
}

async fn run_collect_once() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = required_collection_env("COLLECTOR_DATABASE_URL")?;
    let namespace_id = required_collection_env("NAMESPACE_ID")?;
    let input_root = PathBuf::from(required_collection_env("COLLECTION_INPUT_ROOT")?);
    let lease_owner = env::var("COLLECT_LEASE_OWNER")
        .unwrap_or_else(|_| format!("agent-economy-monitor:{}", std::process::id()));
    let evidence_store: Arc<dyn CollectionEvidenceStore> = match (
        env::var("EVIDENCE_ROOT").ok(),
        env::var("EVIDENCE_BUCKET").ok(),
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
                "configure exactly one evidence backend: EVIDENCE_ROOT or EVIDENCE_BUCKET",
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
        .query_one("SELECT current_user", &[])
        .await?
        .get::<_, String>(0);
    if current_user != "agent_economy_collector_runtime" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "COLLECTOR_DATABASE_URL must authenticate as agent_economy_collector_runtime",
        )
        .into());
    }

    let shared_client = Arc::new(tokio::sync::Mutex::new(collector_client));
    let jobs = Arc::new(PostgresCollectionJobStore::from_shared(
        Arc::clone(&shared_client),
        namespace_id.clone(),
    ));
    let commits = Arc::new(PostgresCollectionCommitStore::from_shared(
        shared_client,
        namespace_id,
    ));
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
