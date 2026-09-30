use std::{env, io, net::SocketAddr};

use agent_economy_monitor::projection_runtime::{ProjectionRuntimeConfig, run_projection_once};
use agent_economy_monitor::query::{PostgresQueryStore, api_router};
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
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    client
        .batch_execute("SET ROLE agent_economy_dashboard_reader")
        .await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::error!("PostgreSQL query connection closed unexpectedly");
        }
    });
    let query_store = PostgresQueryStore::new(client, namespace_id);
    let app: Router = api_router(std::sync::Arc::new(query_store))
        .route("/healthz", get(status))
        .route("/api/v1/status", get(status));

    info!(%address, "agent economy monitor listening");
    axum::serve(listener, app).await?;
    Ok(())
}
