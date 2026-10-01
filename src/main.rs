use std::{env, io, net::SocketAddr};

use agent_economy_monitor::{
    auth::{AuthState, PostgresAuthStore, protect_router},
    cockpit::mount_cockpit,
    projection_gateway::{
        PostgresProjectionGatewayStore, ProjectionGatewayState, mount_projection_gateway,
    },
    projection_runtime::{ProjectionRuntimeConfig, run_projection_once},
    query::{PostgresQueryStore, api_router},
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

    match env::args().nth(1).as_deref() {
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
        None | Some("serve") => serve().await,
        Some(_) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "usage: agent-economy-monitor [serve|project-wiki]",
        )
        .into()),
    }
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
