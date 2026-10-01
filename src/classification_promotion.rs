use std::sync::Arc;

use tokio_postgres::Client;

const MAX_IDENTIFIER_LENGTH: usize = 256;
const PROMOTE_SQL: &str = "SELECT agent_economy.promote_buyer_classification_run($1::text::uuid, $2, $3, $4, $5, $6::text::uuid)";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromotionRequest {
    buyer_handle_id: String,
    run_id: String,
    run_version: i32,
    promotion_method: String,
    provenance_id: String,
}

impl PromotionRequest {
    pub fn from_args<I, S>(args: I) -> Result<Self, PromotionError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut args = args.into_iter().map(Into::into);
        let buyer_handle_id = args.next().ok_or(PromotionError::InvalidRequest)?;
        let run_id = args.next().ok_or(PromotionError::InvalidRequest)?;
        let run_version = args
            .next()
            .ok_or(PromotionError::InvalidRequest)?
            .parse()
            .map_err(|_| PromotionError::InvalidRequest)?;
        let promotion_method = args.next().ok_or(PromotionError::InvalidRequest)?;
        let provenance_id = args.next().ok_or(PromotionError::InvalidRequest)?;
        if args.next().is_some() {
            return Err(PromotionError::InvalidRequest);
        }
        Self::new(
            buyer_handle_id,
            run_id,
            run_version,
            promotion_method,
            provenance_id,
        )
    }

    pub fn new(
        buyer_handle_id: impl Into<String>,
        run_id: impl Into<String>,
        run_version: i32,
        promotion_method: impl Into<String>,
        provenance_id: impl Into<String>,
    ) -> Result<Self, PromotionError> {
        let request = Self {
            buyer_handle_id: buyer_handle_id.into(),
            run_id: run_id.into(),
            run_version,
            promotion_method: promotion_method.into(),
            provenance_id: provenance_id.into(),
        };
        if request.run_version <= 0
            || [
                request.buyer_handle_id.as_str(),
                request.run_id.as_str(),
                request.promotion_method.as_str(),
                request.provenance_id.as_str(),
            ]
            .iter()
            .any(|value| value.trim().is_empty() || value.len() > MAX_IDENTIFIER_LENGTH)
        {
            return Err(PromotionError::InvalidRequest);
        }
        Ok(request)
    }

    pub fn run_version(&self) -> i32 {
        self.run_version
    }
}

pub struct PostgresClassificationPromotionStore {
    client: Arc<Client>,
    namespace_id: String,
}

impl PostgresClassificationPromotionStore {
    pub fn new(client: Arc<Client>, namespace_id: String) -> Result<Self, PromotionError> {
        if namespace_id.trim().is_empty() || namespace_id.len() > MAX_IDENTIFIER_LENGTH {
            return Err(PromotionError::InvalidRequest);
        }
        Ok(Self {
            client,
            namespace_id,
        })
    }

    pub async fn promote(&self, request: &PromotionRequest) -> Result<i64, PromotionError> {
        let row = self
            .client
            .query_one(
                PROMOTE_SQL,
                &[
                    &self.namespace_id,
                    &request.buyer_handle_id,
                    &request.run_id,
                    &request.run_version,
                    &request.promotion_method,
                    &request.provenance_id,
                ],
            )
            .await
            .map_err(|_| PromotionError::Unavailable)?;
        row.try_get(0).map_err(|_| PromotionError::Unavailable)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PromotionError {
    InvalidRequest,
    Unavailable,
}

impl std::fmt::Display for PromotionError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InvalidRequest => "classification promotion request is invalid",
            Self::Unavailable => "classification promotion store is unavailable",
        })
    }
}

impl std::error::Error for PromotionError {}
