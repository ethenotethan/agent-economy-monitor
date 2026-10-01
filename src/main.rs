use std::{env, net::SocketAddr};

use agent_economy_monitor::{
    auth::{AuthState, PostgresAuthStore, protect_router},
    classification_promotion::{PostgresClassificationPromotionStore, PromotionRequest},
    cockpit::mount_cockpit,
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

    let mut arguments = env::args().skip(1);
    if let Some(command) = arguments.next() {
        if command != "promote-classification" {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("unknown command: {command}"),
            )
            .into());
        }
        let request = PromotionRequest::from_args(arguments)?;
        let database_url = env::var("DATABASE_URL")?;
        let namespace_id = env::var("NAMESPACE_ID")?;
        let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
        tokio::spawn(async move {
            if connection.await.is_err() {
                tracing::error!("PostgreSQL promotion connection closed unexpectedly");
            }
        });
        let store =
            PostgresClassificationPromotionStore::new(std::sync::Arc::new(client), namespace_id)?;
        let promotion_sequence = store.promote(&request).await?;
        info!(promotion_sequence, "classification run promoted");
        return Ok(());
    }

    let port = env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(8080);
    let address = SocketAddr::from(([0, 0, 0, 0], port));
    let listener = TcpListener::bind(address).await?;
    let database_url = env::var("DATABASE_URL")?;
    let namespace_id = env::var("NAMESPACE_ID")?;
    let (client, connection) = tokio_postgres::connect(&database_url, NoTls).await?;
    tokio::spawn(async move {
        if connection.await.is_err() {
            tracing::error!("PostgreSQL query connection closed unexpectedly");
        }
    });
    let client = std::sync::Arc::new(client);
    let auth_store =
        std::sync::Arc::new(PostgresAuthStore::new(client.clone(), namespace_id.clone()));
    let auth = AuthState::from_env(auth_store)?;
    let query_store = PostgresQueryStore::from_shared(client, namespace_id);
    let app: Router =
        api_router(std::sync::Arc::new(query_store)).route("/api/v1/status", get(status));
    let app = mount_cockpit(app);
    let app = protect_router(app, auth).route("/healthz", get(status));

    info!(%address, "agent economy monitor listening");
    axum::serve(listener, app).await?;
    Ok(())
}
