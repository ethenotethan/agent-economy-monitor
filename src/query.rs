use std::sync::Arc;

use async_trait::async_trait;
use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::{HeaderValue, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, Row, types::ToSql};

const API_VERSION: &str = "v1";
const DEFAULT_PAGE_SIZE: usize = 25;
const MAX_PAGE_SIZE: usize = 100;
const CACHE_CONTROL: &str = "private, max-age=30, stale-while-revalidate=120";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Fact {
    pub id: String,
    pub kind: String,
    pub label: String,
    pub value: Value,
    pub observed_at: String,
    pub provenance_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct DashboardPage {
    pub api_version: &'static str,
    pub items: Vec<Fact>,
    pub next_cursor: Option<String>,
}

impl DashboardPage {
    pub fn new(items: Vec<Fact>, next_cursor: Option<String>) -> Self {
        Self {
            api_version: API_VERSION,
            items,
            next_cursor,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GraphReadModel {
    pub root: Fact,
    pub nodes: Vec<Fact>,
    pub edges: Vec<Fact>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct EvidenceProvenanceBinding {
    pub evidence_id: String,
    pub provenance_ids: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ClassificationReadModel {
    pub claim_id: String,
    pub label: String,
    pub status: String,
    pub confidence: String,
    pub method: String,
    pub evidence_window_start: String,
    pub evidence_window_end: String,
    pub valid_to: Option<String>,
    pub is_stale: bool,
    pub provenance_ids: Vec<String>,
    pub supporting_evidence_ids: Vec<String>,
    pub conflicting_evidence_ids: Vec<String>,
    pub supporting_evidence: Vec<EvidenceProvenanceBinding>,
    pub conflicting_evidence: Vec<EvidenceProvenanceBinding>,
}

#[derive(Clone, Debug, Serialize, PartialEq)]
pub struct BuyerDossierReadModel {
    pub buyer: Fact,
    pub classifications: Vec<ClassificationReadModel>,
    pub timeline: DashboardPage,
    pub graph: GraphReadModel,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProvenanceReadModel {
    pub provenance_id: String,
    pub source_id: String,
    pub observed_at: String,
    pub parser_version: String,
    pub provider: Option<String>,
    pub chain_scope: Option<String>,
    pub block_reference: Option<String>,
    pub transaction_reference: Option<String>,
    pub finality: Option<String>,
    pub evidence_id: String,
    pub evidence_sha256: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct SystemReadModel {
    pub facts: Vec<Fact>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProjectionPage {
    pub page_path: String,
    pub stable_entity_id: String,
    pub generated_markdown: String,
    pub citations: Value,
    pub wikilinks: Vec<String>,
    pub model_id: String,
    pub model_sha256: String,
    pub prompt_sha256: String,
    pub snapshot_sha256: String,
    pub output_sha256: String,
    pub bundle_sha256: String,
    pub changeset_id: String,
    pub changeset_sha256: String,
    pub approved_by: String,
    pub approved_at: String,
    pub published_at: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ProjectionPageList {
    pub api_version: &'static str,
    pub items: Vec<ProjectionPage>,
    pub next_cursor: Option<String>,
}

impl ProjectionPageList {
    pub fn new(items: Vec<ProjectionPage>, next_cursor: Option<String>) -> Self {
        Self {
            api_version: API_VERSION,
            items,
            next_cursor,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
struct Versioned<T> {
    api_version: &'static str,
    data: T,
}

#[derive(Debug)]
pub enum QueryError {
    Invalid(String),
    NotFound,
    Unavailable,
}

impl IntoResponse for QueryError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            Self::Invalid(message) => (StatusCode::BAD_REQUEST, "invalid_query", message),
            Self::NotFound => (
                StatusCode::NOT_FOUND,
                "not_found",
                "resource not found".into(),
            ),
            Self::Unavailable => (
                StatusCode::SERVICE_UNAVAILABLE,
                "query_store_unavailable",
                "query store unavailable".into(),
            ),
        };
        (
            status,
            Json(json!({"error": {"code": code, "message": message}})),
        )
            .into_response()
    }
}

#[async_trait]
pub trait QueryStore: Send + Sync + 'static {
    async fn pulse(&self) -> Result<Vec<Fact>, QueryError>;
    async fn buyers(&self, after: Option<&str>, limit: usize) -> Result<DashboardPage, QueryError>;
    async fn buyer(&self, id: &str) -> Result<Option<Fact>, QueryError>;
    async fn buyer_dossier(&self, id: &str) -> Result<Option<BuyerDossierReadModel>, QueryError>;
    async fn buyer_timeline(
        &self,
        id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DashboardPage, QueryError>;
    async fn services(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DashboardPage, QueryError>;
    async fn service(&self, id: &str) -> Result<Option<Fact>, QueryError>;
    async fn graph(&self, kind: &str, id: &str, limit: usize)
    -> Result<GraphReadModel, QueryError>;
    async fn provenance(&self, id: &str) -> Result<Option<ProvenanceReadModel>, QueryError>;
    async fn search(
        &self,
        query: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DashboardPage, QueryError>;
    async fn investigations(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectionPageList, QueryError>;
    async fn system(&self) -> Result<SystemReadModel, QueryError>;
}

pub struct PostgresQueryStore {
    client: Arc<Client>,
    namespace_id: String,
}

impl PostgresQueryStore {
    pub fn new(client: Client, namespace_id: String) -> Self {
        Self::from_shared(Arc::new(client), namespace_id)
    }

    pub fn from_shared(client: Arc<Client>, namespace_id: String) -> Self {
        Self {
            client,
            namespace_id,
        }
    }

    async fn facts(
        &self,
        statement: &str,
        parameters: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Fact>, QueryError> {
        self.client
            .query(statement, parameters)
            .await
            .map_err(|_| QueryError::Unavailable)?
            .iter()
            .map(row_to_fact)
            .collect()
    }

    async fn fact_page(
        &self,
        kind: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        let fetch = i64::try_from(limit + 1).map_err(|_| QueryError::Unavailable)?;
        let sql = format!(
            "SELECT {FACT_COLUMNS} FROM agent_economy.dashboard_facts AS fact JOIN agent_economy.namespaces AS namespace USING (namespace_id) WHERE namespace.namespace_id = $1::text::uuid AND fact.kind = $2 AND ($3::text IS NULL OR fact.id > $3) ORDER BY fact.id LIMIT $4"
        );
        let items = self
            .facts(&sql, &[&self.namespace_id, &kind, &after, &fetch])
            .await?;
        Ok(page(items, limit))
    }

    async fn one_fact(&self, kind: &str, id: &str) -> Result<Option<Fact>, QueryError> {
        let sql = format!(
            "SELECT {FACT_COLUMNS} FROM agent_economy.dashboard_facts AS fact JOIN agent_economy.namespaces AS namespace USING (namespace_id) WHERE namespace.namespace_id = $1::text::uuid AND fact.kind = $2 AND fact.id = $3 LIMIT 1"
        );
        Ok(self
            .facts(&sql, &[&self.namespace_id, &kind, &id])
            .await?
            .into_iter()
            .next())
    }

    async fn buyer_classifications(
        &self,
        id: &str,
    ) -> Result<Vec<ClassificationReadModel>, QueryError> {
        let rows = self
            .client
            .query(BUYER_CLASSIFICATIONS_SQL, &[&self.namespace_id, &id])
            .await
            .map_err(|_| QueryError::Unavailable)?;
        rows.iter().map(row_to_classification).collect()
    }
}

fn row_to_fact(row: &Row) -> Result<Fact, QueryError> {
    let value: String = row.try_get("value").map_err(|_| QueryError::Unavailable)?;
    Ok(Fact {
        id: row.try_get("id").map_err(|_| QueryError::Unavailable)?,
        kind: row.try_get("kind").map_err(|_| QueryError::Unavailable)?,
        label: row.try_get("label").map_err(|_| QueryError::Unavailable)?,
        value: serde_json::from_str(&value).map_err(|_| QueryError::Unavailable)?,
        observed_at: row
            .try_get("observed_at")
            .map_err(|_| QueryError::Unavailable)?,
        provenance_ids: row
            .try_get("provenance_ids")
            .map_err(|_| QueryError::Unavailable)?,
    })
}

fn row_to_classification(row: &Row) -> Result<ClassificationReadModel, QueryError> {
    Ok(ClassificationReadModel {
        claim_id: row
            .try_get("claim_id")
            .map_err(|_| QueryError::Unavailable)?,
        label: row.try_get("label").map_err(|_| QueryError::Unavailable)?,
        status: row.try_get("status").map_err(|_| QueryError::Unavailable)?,
        confidence: row
            .try_get("confidence")
            .map_err(|_| QueryError::Unavailable)?,
        method: row.try_get("method").map_err(|_| QueryError::Unavailable)?,
        evidence_window_start: row
            .try_get("evidence_window_start")
            .map_err(|_| QueryError::Unavailable)?,
        evidence_window_end: row
            .try_get("evidence_window_end")
            .map_err(|_| QueryError::Unavailable)?,
        valid_to: row
            .try_get("valid_to")
            .map_err(|_| QueryError::Unavailable)?,
        is_stale: row
            .try_get("is_stale")
            .map_err(|_| QueryError::Unavailable)?,
        provenance_ids: vec![
            row.try_get("provenance_id")
                .map_err(|_| QueryError::Unavailable)?,
        ],
        supporting_evidence_ids: row
            .try_get("supporting_evidence_ids")
            .map_err(|_| QueryError::Unavailable)?,
        conflicting_evidence_ids: row
            .try_get("conflicting_evidence_ids")
            .map_err(|_| QueryError::Unavailable)?,
        supporting_evidence: evidence_bindings(row, "supporting_evidence")?,
        conflicting_evidence: evidence_bindings(row, "conflicting_evidence")?,
    })
}

fn evidence_bindings(
    row: &Row,
    column: &str,
) -> Result<Vec<EvidenceProvenanceBinding>, QueryError> {
    let encoded: String = row.try_get(column).map_err(|_| QueryError::Unavailable)?;
    serde_json::from_str(&encoded).map_err(|_| QueryError::Unavailable)
}

fn type_graph_edge(edge: &mut Fact) {
    let buyer_id = edge
        .value
        .get("buyer_handle_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let service_id = edge
        .value
        .get("service_id")
        .and_then(Value::as_str)
        .map(str::to_owned);
    if let (Some(buyer_id), Some(service_id), Some(value)) =
        (buyer_id, service_id, edge.value.as_object_mut())
    {
        value.insert("source".into(), json!({"kind": "buyer", "id": buyer_id}));
        value.insert(
            "target".into(),
            json!({"kind": "service", "id": service_id}),
        );
        let predicate = match value.get("level").and_then(Value::as_str) {
            Some("verified" | "strong") => "paid_for",
            _ => "candidate_for",
        };
        value.insert("predicate".into(), Value::String(predicate.into()));
        value.insert("direction".into(), Value::String("outbound".into()));
        value.insert(
            "attribution_method".into(),
            Value::String(edge.label.clone()),
        );
    }
}

fn page(mut items: Vec<Fact>, limit: usize) -> DashboardPage {
    let next_cursor = (items.len() > limit).then(|| items[limit - 1].id.clone());
    items.truncate(limit);
    DashboardPage::new(items, next_cursor)
}

fn encode_cursor(parts: &[&str]) -> String {
    serde_json::to_vec(parts)
        .expect("serializing string cursor parts cannot fail")
        .into_iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn decode_cursor(cursor: &str, expected_parts: usize) -> Result<Vec<String>, QueryError> {
    if !cursor.len().is_multiple_of(2) {
        return Err(QueryError::Invalid("cursor is malformed".into()));
    }
    let bytes = (0..cursor.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&cursor[offset..offset + 2], 16))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| QueryError::Invalid("cursor is malformed".into()))?;
    let parts: Vec<String> = serde_json::from_slice(&bytes)
        .map_err(|_| QueryError::Invalid("cursor is malformed".into()))?;
    if parts.len() != expected_parts {
        return Err(QueryError::Invalid("cursor is malformed".into()));
    }
    Ok(parts)
}

const FACT_COLUMNS: &str = "fact.id, fact.kind, fact.label, fact.value::text AS value, fact.observed_at::text AS observed_at, fact.provenance_ids";

const BUYER_CLASSIFICATIONS_SQL: &str = r#"
WITH current_run AS (
    SELECT current.namespace_id, current.run_id, current.run_version
    FROM agent_economy.current_buyer_classification_runs AS current
    WHERE current.namespace_id = $1::text::uuid
      AND current.buyer_handle_id = $2
)
SELECT
    claim.claim_id,
    claim.label,
    claim.status,
    claim.confidence::text,
    claim.method,
    claim.evidence_window_start::text,
    claim.evidence_window_end::text,
    claim.valid_to::text,
    (claim.valid_to IS NOT NULL AND claim.valid_to <= CURRENT_TIMESTAMP) AS is_stale,
    claim.provenance_id::text,
    coalesce(array_agg(evidence.evidence_id ORDER BY evidence.evidence_id)
        FILTER (WHERE evidence.evidence_role = 'supporting'), '{}')::text[]
        AS supporting_evidence_ids,
    coalesce(array_agg(evidence.evidence_id ORDER BY evidence.evidence_id)
        FILTER (WHERE evidence.evidence_role = 'conflicting'), '{}')::text[]
        AS conflicting_evidence_ids,
    coalesce((
        SELECT jsonb_agg(jsonb_build_object(
            'evidence_id', bound.evidence_id,
            'provenance_ids', coalesce((
                SELECT jsonb_agg(provenance.provenance_id::text ORDER BY provenance.provenance_id)
                FROM agent_economy.provenance_records AS provenance
                WHERE provenance.namespace_id = bound.namespace_id
                  AND provenance.evidence_id = bound.evidence_id
            ), '[]'::jsonb)
        ) ORDER BY bound.evidence_id)
        FROM agent_economy.classification_claim_evidence AS bound
        WHERE bound.namespace_id = claim.namespace_id
          AND bound.claim_id = claim.claim_id
          AND bound.claim_version = claim.version
          AND bound.evidence_role = 'supporting'
    ), '[]'::jsonb)::text AS supporting_evidence,
    coalesce((
        SELECT jsonb_agg(jsonb_build_object(
            'evidence_id', bound.evidence_id,
            'provenance_ids', coalesce((
                SELECT jsonb_agg(provenance.provenance_id::text ORDER BY provenance.provenance_id)
                FROM agent_economy.provenance_records AS provenance
                WHERE provenance.namespace_id = bound.namespace_id
                  AND provenance.evidence_id = bound.evidence_id
            ), '[]'::jsonb)
        ) ORDER BY bound.evidence_id)
        FROM agent_economy.classification_claim_evidence AS bound
        WHERE bound.namespace_id = claim.namespace_id
          AND bound.claim_id = claim.claim_id
          AND bound.claim_version = claim.version
          AND bound.evidence_role = 'conflicting'
    ), '[]'::jsonb)::text AS conflicting_evidence
FROM current_run AS current
JOIN agent_economy.classification_run_claims AS run_claim
  USING (namespace_id, run_id, run_version)
JOIN agent_economy.classification_claims AS claim
  ON claim.namespace_id = run_claim.namespace_id
 AND claim.claim_id = run_claim.claim_id
 AND claim.version = run_claim.claim_version
LEFT JOIN agent_economy.classification_claim_evidence AS evidence
  ON evidence.namespace_id = claim.namespace_id
 AND evidence.claim_id = claim.claim_id
 AND evidence.claim_version = claim.version
GROUP BY claim.namespace_id, claim.claim_id, claim.version, claim.label, claim.status,
         claim.confidence, claim.method, claim.evidence_window_start,
         claim.evidence_window_end, claim.valid_to, claim.provenance_id
ORDER BY claim.label, claim.claim_id
"#;

#[async_trait]
impl QueryStore for PostgresQueryStore {
    async fn pulse(&self) -> Result<Vec<Fact>, QueryError> {
        let sql = format!(
            "SELECT {FACT_COLUMNS} FROM agent_economy.dashboard_pulse AS fact JOIN agent_economy.namespaces AS namespace USING (namespace_id) WHERE namespace.namespace_id = $1::text::uuid ORDER BY fact.observed_at DESC, fact.id LIMIT 168"
        );
        self.facts(&sql, &[&self.namespace_id]).await
    }

    async fn buyers(&self, after: Option<&str>, limit: usize) -> Result<DashboardPage, QueryError> {
        self.fact_page("buyer", after, limit).await
    }

    async fn buyer(&self, id: &str) -> Result<Option<Fact>, QueryError> {
        self.one_fact("buyer", id).await
    }

    async fn buyer_dossier(&self, id: &str) -> Result<Option<BuyerDossierReadModel>, QueryError> {
        let Some(buyer) = self.buyer(id).await? else {
            return Ok(None);
        };
        Ok(Some(BuyerDossierReadModel {
            buyer,
            classifications: self.buyer_classifications(id).await?,
            timeline: self.buyer_timeline(id, None, DEFAULT_PAGE_SIZE).await?,
            graph: self.graph("buyer", id, DEFAULT_PAGE_SIZE).await?,
        }))
    }

    async fn buyer_timeline(
        &self,
        id: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        let fetch = i64::try_from(limit + 1).map_err(|_| QueryError::Unavailable)?;
        let cursor = after.map(|value| decode_cursor(value, 2)).transpose()?;
        let after_time = cursor.as_ref().map(|parts| parts[0].as_str());
        let after_id = cursor.as_ref().map(|parts| parts[1].as_str());
        let sql = format!(
            "SELECT {FACT_COLUMNS} FROM agent_economy.dashboard_facts AS fact JOIN agent_economy.namespaces AS namespace USING (namespace_id) WHERE namespace.namespace_id = $1::text::uuid AND fact.kind = 'settlement' AND fact.value->>'buyer_handle_id' = $2 AND EXISTS (SELECT 1 FROM agent_economy.settlements AS settlement JOIN agent_economy.current_event_finality AS finality ON finality.namespace_id = settlement.namespace_id AND finality.protocol = settlement.protocol AND finality.chain_scope = settlement.chain_scope AND finality.canonical_event_id = settlement.canonical_event_id WHERE settlement.namespace_id = fact.namespace_id AND settlement.chain_scope = fact.value->>'chain_scope' AND settlement.settlement_id = fact.value->>'settlement_id' AND finality.finality_status = 'finalized') AND ($3::text IS NULL OR ((fact.value->>'settled_at')::timestamptz, fact.id) < ($3::timestamptz, $4)) ORDER BY (fact.value->>'settled_at')::timestamptz DESC, fact.id DESC LIMIT $5"
        );
        let items = self
            .facts(
                &sql,
                &[&self.namespace_id, &id, &after_time, &after_id, &fetch],
            )
            .await?;
        let next_cursor = (items.len() > limit)
            .then(|| {
                let last = &items[limit - 1];
                last.value
                    .get("settled_at")
                    .and_then(Value::as_str)
                    .map(|settled_at| encode_cursor(&[settled_at, &last.id]))
            })
            .flatten();
        let mut items = items;
        items.truncate(limit);
        Ok(DashboardPage::new(items, next_cursor))
    }

    async fn services(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        self.fact_page("service", after, limit).await
    }

    async fn service(&self, id: &str) -> Result<Option<Fact>, QueryError> {
        self.one_fact("service", id).await
    }

    async fn graph(
        &self,
        kind: &str,
        id: &str,
        limit: usize,
    ) -> Result<GraphReadModel, QueryError> {
        if !matches!(kind, "buyer" | "service") {
            return Err(QueryError::Invalid(
                "graph kind must be buyer or service".into(),
            ));
        }
        let root = self.one_fact(kind, id).await?.ok_or(QueryError::NotFound)?;
        let fetch = i64::try_from(limit).map_err(|_| QueryError::Unavailable)?;
        let predicate = if kind == "buyer" {
            "settlement.buyer_handle_id = $2"
        } else {
            "candidate.service_id = $2"
        };
        let sql = format!(
            "WITH latest AS (SELECT DISTINCT ON (run.namespace_id, run.chain_scope, run.settlement_id) run.namespace_id, run.chain_scope, run.settlement_id, run.attribution_version, run.match_method, run.level FROM agent_economy.attribution_runs AS run JOIN agent_economy.attribution_run_seals AS seal USING (namespace_id, chain_scope, settlement_id, attribution_version) WHERE run.namespace_id = $1::text::uuid ORDER BY run.namespace_id, run.chain_scope, run.settlement_id, run.attribution_version DESC) SELECT concat_ws(':', candidate.chain_scope, candidate.settlement_id, candidate.attribution_version::text, candidate.candidate_id) AS id, 'attribution'::text AS kind, latest.match_method AS label, jsonb_build_object('settlement_id', candidate.settlement_id, 'chain_scope', candidate.chain_scope, 'buyer_handle_id', settlement.buyer_handle_id, 'service_id', candidate.service_id, 'endpoint_id', candidate.endpoint_id, 'payment_option_id', candidate.payment_option_id, 'confidence', candidate.confidence, 'level', latest.level)::text AS value, greatest(settlement_provenance.observed_at, requirement_provenance.observed_at)::text AS observed_at, ARRAY[settlement.provenance_id::text, requirement.provenance_id::text] AS provenance_ids FROM latest JOIN agent_economy.attribution_candidates AS candidate USING (namespace_id, chain_scope, settlement_id, attribution_version) JOIN agent_economy.settlements AS settlement ON settlement.namespace_id = candidate.namespace_id AND settlement.chain_scope = candidate.chain_scope AND settlement.settlement_id = candidate.settlement_id JOIN agent_economy.payment_requirements AS requirement ON requirement.namespace_id = candidate.namespace_id AND requirement.requirement_id = candidate.requirement_id JOIN agent_economy.provenance_records AS settlement_provenance ON settlement_provenance.namespace_id = settlement.namespace_id AND settlement_provenance.provenance_id = settlement.provenance_id JOIN agent_economy.provenance_records AS requirement_provenance ON requirement_provenance.namespace_id = requirement.namespace_id AND requirement_provenance.provenance_id = requirement.provenance_id WHERE {predicate} ORDER BY candidate.chain_scope, candidate.settlement_id, candidate.attribution_version, candidate.candidate_id LIMIT $3"
        );
        let mut edges = self.facts(&sql, &[&self.namespace_id, &id, &fetch]).await?;
        for edge in &mut edges {
            type_graph_edge(edge);
        }
        let mut nodes = Vec::new();
        for edge in &edges {
            let related = if kind == "buyer" {
                edge.value.get("service_id")
            } else {
                edge.value.get("buyer_handle_id")
            };
            if let Some(related_id) = related.and_then(Value::as_str) {
                let related_kind = if kind == "buyer" { "service" } else { "buyer" };
                if let Some(fact) = self.one_fact(related_kind, related_id).await?
                    && !nodes.iter().any(|node: &Fact| node.id == fact.id)
                {
                    nodes.push(fact);
                }
            }
        }
        Ok(GraphReadModel { root, nodes, edges })
    }

    async fn provenance(&self, id: &str) -> Result<Option<ProvenanceReadModel>, QueryError> {
        let row = self.client.query_opt("SELECT provenance.provenance_id::text, provenance.source_id, provenance.observed_at::text, provenance.parser_version, provenance.provider, provenance.chain_scope, provenance.block_reference, provenance.transaction_reference, provenance.finality, provenance.evidence_id, evidence.sha256 FROM agent_economy.provenance_records AS provenance JOIN agent_economy.evidence_objects AS evidence ON evidence.namespace_id = provenance.namespace_id AND evidence.evidence_id = provenance.evidence_id JOIN agent_economy.namespaces AS namespace ON namespace.namespace_id = provenance.namespace_id WHERE namespace.namespace_id = $1::text::uuid AND provenance.provenance_id = $2::text::uuid", &[&self.namespace_id, &id]).await.map_err(|_| QueryError::Unavailable)?;
        row.map(|row| {
            Ok(ProvenanceReadModel {
                provenance_id: row.try_get(0).map_err(|_| QueryError::Unavailable)?,
                source_id: row.try_get(1).map_err(|_| QueryError::Unavailable)?,
                observed_at: row.try_get(2).map_err(|_| QueryError::Unavailable)?,
                parser_version: row.try_get(3).map_err(|_| QueryError::Unavailable)?,
                provider: row.try_get(4).map_err(|_| QueryError::Unavailable)?,
                chain_scope: row.try_get(5).map_err(|_| QueryError::Unavailable)?,
                block_reference: row.try_get(6).map_err(|_| QueryError::Unavailable)?,
                transaction_reference: row.try_get(7).map_err(|_| QueryError::Unavailable)?,
                finality: row.try_get(8).map_err(|_| QueryError::Unavailable)?,
                evidence_id: row.try_get(9).map_err(|_| QueryError::Unavailable)?,
                evidence_sha256: row.try_get(10).map_err(|_| QueryError::Unavailable)?,
            })
        })
        .transpose()
    }

    async fn search(
        &self,
        query: &str,
        after: Option<&str>,
        limit: usize,
    ) -> Result<DashboardPage, QueryError> {
        let fetch = i64::try_from(limit + 1).map_err(|_| QueryError::Unavailable)?;
        let needle = format!("%{query}%");
        let cursor = after.map(|value| decode_cursor(value, 2)).transpose()?;
        let after_kind = cursor.as_ref().map(|parts| parts[0].as_str());
        let after_id = cursor.as_ref().map(|parts| parts[1].as_str());
        let sql = format!(
            "SELECT {FACT_COLUMNS} FROM agent_economy.dashboard_facts AS fact JOIN agent_economy.namespaces AS namespace USING (namespace_id) WHERE namespace.namespace_id = $1::text::uuid AND fact.kind IN ('buyer', 'service') AND (fact.id ILIKE $2 OR fact.label ILIKE $2) AND ($3::text IS NULL OR (fact.kind, fact.id) > ($3, $4)) ORDER BY fact.kind, fact.id LIMIT $5"
        );
        let items = self
            .facts(
                &sql,
                &[&self.namespace_id, &needle, &after_kind, &after_id, &fetch],
            )
            .await?;
        let next_cursor = (items.len() > limit).then(|| {
            let last = &items[limit - 1];
            encode_cursor(&[&last.kind, &last.id])
        });
        let mut items = items;
        items.truncate(limit);
        Ok(DashboardPage::new(items, next_cursor))
    }

    async fn investigations(
        &self,
        after: Option<&str>,
        limit: usize,
    ) -> Result<ProjectionPageList, QueryError> {
        let fetch = i64::try_from(limit + 1).map_err(|_| QueryError::Unavailable)?;
        let rows = self.client.query(
            "SELECT page_path, stable_entity_id, generated_markdown, citations::text, wikilinks, model_id, model_sha256, prompt_sha256, snapshot_sha256, output_sha256, bundle_sha256, changeset_id, changeset_sha256, approved_by, approved_at::text, published_at::text FROM (SELECT DISTINCT ON (page.namespace_id, page.page_path) page.* FROM agent_economy.projection_mirror_pages AS page WHERE page.namespace_id = $1::text::uuid ORDER BY page.namespace_id, page.page_path, page.published_at DESC, page.bundle_sha256 DESC) AS latest WHERE ($2::text IS NULL OR page_path > $2) ORDER BY page_path LIMIT $3",
            &[&self.namespace_id, &after, &fetch],
        ).await.map_err(|_| QueryError::Unavailable)?;
        let mut items = rows
            .iter()
            .map(row_to_projection_page)
            .collect::<Result<Vec<_>, _>>()?;
        let next_cursor = (items.len() > limit).then(|| items[limit - 1].page_path.clone());
        items.truncate(limit);
        Ok(ProjectionPageList::new(items, next_cursor))
    }

    async fn system(&self) -> Result<SystemReadModel, QueryError> {
        let sql = format!(
            "SELECT {FACT_COLUMNS} FROM agent_economy.dashboard_system AS fact JOIN agent_economy.namespaces AS namespace USING (namespace_id) WHERE namespace.namespace_id = $1::text::uuid ORDER BY fact.observed_at DESC, fact.id LIMIT 100"
        );
        Ok(SystemReadModel {
            facts: self.facts(&sql, &[&self.namespace_id]).await?,
        })
    }
}

fn row_to_projection_page(row: &Row) -> Result<ProjectionPage, QueryError> {
    let citations: String = row.try_get(3).map_err(|_| QueryError::Unavailable)?;
    Ok(ProjectionPage {
        page_path: row.try_get(0).map_err(|_| QueryError::Unavailable)?,
        stable_entity_id: row.try_get(1).map_err(|_| QueryError::Unavailable)?,
        generated_markdown: row.try_get(2).map_err(|_| QueryError::Unavailable)?,
        citations: serde_json::from_str(&citations).map_err(|_| QueryError::Unavailable)?,
        wikilinks: row.try_get(4).map_err(|_| QueryError::Unavailable)?,
        model_id: row.try_get(5).map_err(|_| QueryError::Unavailable)?,
        model_sha256: row.try_get(6).map_err(|_| QueryError::Unavailable)?,
        prompt_sha256: row.try_get(7).map_err(|_| QueryError::Unavailable)?,
        snapshot_sha256: row.try_get(8).map_err(|_| QueryError::Unavailable)?,
        output_sha256: row.try_get(9).map_err(|_| QueryError::Unavailable)?,
        bundle_sha256: row.try_get(10).map_err(|_| QueryError::Unavailable)?,
        changeset_id: row.try_get(11).map_err(|_| QueryError::Unavailable)?,
        changeset_sha256: row.try_get(12).map_err(|_| QueryError::Unavailable)?,
        approved_by: row.try_get(13).map_err(|_| QueryError::Unavailable)?,
        approved_at: row.try_get(14).map_err(|_| QueryError::Unavailable)?,
        published_at: row.try_get(15).map_err(|_| QueryError::Unavailable)?,
    })
}

type Store = Arc<dyn QueryStore>;

#[derive(Debug, Deserialize)]
struct PageQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

impl PageQuery {
    fn bounded(&self) -> Result<(Option<&str>, usize), QueryError> {
        let limit = self.limit.unwrap_or(DEFAULT_PAGE_SIZE);
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(QueryError::Invalid(format!(
                "limit must be between 1 and {MAX_PAGE_SIZE}"
            )));
        }
        Ok((self.cursor.as_deref(), limit))
    }
}

#[derive(Debug, Deserialize)]
struct SearchQuery {
    q: String,
    limit: Option<usize>,
    cursor: Option<String>,
}

pub fn api_router(store: Store) -> Router {
    Router::new()
        .route("/api/v1/openapi.json", get(openapi))
        .route("/api/v1/pulse", get(pulse))
        .route("/api/v1/buyers", get(buyers))
        .route("/api/v1/buyers/{id}", get(buyer))
        .route("/api/v1/buyers/{id}/dossier", get(buyer_dossier))
        .route("/api/v1/buyers/{id}/timeline", get(buyer_timeline))
        .route("/api/v1/services", get(services))
        .route("/api/v1/services/{id}", get(service))
        .route("/api/v1/graph/{kind}/{id}", get(graph))
        .route("/api/v1/provenance/{id}", get(provenance))
        .route("/api/v1/search", get(search))
        .route("/api/v1/investigations", get(investigations))
        .route("/api/v1/system", get(system))
        .with_state(store)
}

async fn openapi() -> Json<Value> {
    let response = || {
        json!({
            "200": {
                "description": "Versioned, provenance-bearing read model",
                "content": {"application/json": {"schema": {"type": "object"}}}
            },
            "400": {"description": "Invalid bounded query"},
            "404": {"description": "Resource not found"},
            "503": {"description": "Query store unavailable"}
        })
    };
    let path_parameter = |name: &str| json!({"name": name, "in": "path", "required": true, "schema": {"type": "string"}});
    let page_parameters = || {
        vec![
            json!({"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": MAX_PAGE_SIZE, "default": DEFAULT_PAGE_SIZE}}),
            json!({"name": "cursor", "in": "query", "schema": {"type": "string"}}),
        ]
    };
    let timeline_parameters = [vec![path_parameter("id")], page_parameters()].concat();
    let search_parameters = [
        vec![json!({"name": "q", "in": "query", "required": true, "schema": {"type": "string", "minLength": 1, "maxLength": 200}})],
        page_parameters(),
    ]
    .concat();
    Json(json!({
        "openapi": "3.1.0",
        "info": {"title": "Agent Economy Monitor Query API", "version": "1.0.0"},
        "paths": {
            "/api/v1/pulse": {"get": {"responses": response()}},
            "/api/v1/buyers": {"get": {"parameters": page_parameters(), "responses": response()}},
            "/api/v1/buyers/{id}": {"get": {"parameters": [path_parameter("id")], "responses": response()}},
            "/api/v1/buyers/{id}/dossier": {"get": {"parameters": [path_parameter("id")], "responses": response()}},
            "/api/v1/buyers/{id}/timeline": {"get": {"parameters": timeline_parameters, "responses": response()}},
            "/api/v1/services": {"get": {"parameters": page_parameters(), "responses": response()}},
            "/api/v1/services/{id}": {"get": {"parameters": [path_parameter("id")], "responses": response()}},
            "/api/v1/graph/{kind}/{id}": {"get": {"parameters": [path_parameter("kind"), path_parameter("id"), json!({"name": "limit", "in": "query", "schema": {"type": "integer", "minimum": 1, "maximum": MAX_PAGE_SIZE, "default": DEFAULT_PAGE_SIZE}})], "responses": response()}},
            "/api/v1/provenance/{id}": {"get": {"parameters": [path_parameter("id")], "responses": response()}},
            "/api/v1/search": {"get": {"parameters": search_parameters, "responses": response()}},
            "/api/v1/investigations": {"get": {"parameters": page_parameters(), "responses": response()}},
            "/api/v1/system": {"get": {"responses": response()}}
        },
        "components": {"schemas": {
            "Fact": {
                "type": "object",
                "required": ["id", "kind", "label", "value", "observed_at", "provenance_ids"],
                "properties": {
                    "id": {"type": "string"},
                    "kind": {"type": "string"},
                    "label": {"type": "string"},
                    "value": {},
                    "observed_at": {"type": "string", "format": "date-time"},
                    "provenance_ids": {"type": "array", "minItems": 1, "items": {"type": "string", "format": "uuid"}}
                }
            }
        }}
    }))
}

fn cacheable<T: Serialize>(value: &T) -> Result<Response, QueryError> {
    let body = serde_json::to_vec(value).map_err(|_| QueryError::Unavailable)?;
    let digest = Sha256::digest(&body);
    let etag = format!("\"{digest:x}\"");
    let mut response = (
        [
            (
                header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            ),
            (
                header::CACHE_CONTROL,
                HeaderValue::from_static(CACHE_CONTROL),
            ),
        ],
        body,
    )
        .into_response();
    response.headers_mut().insert(
        header::ETAG,
        HeaderValue::from_str(&etag).map_err(|_| QueryError::Unavailable)?,
    );
    Ok(response)
}

async fn pulse(State(store): State<Store>) -> Result<Response, QueryError> {
    cacheable(&Versioned {
        api_version: API_VERSION,
        data: store.pulse().await?,
    })
}

async fn buyers(
    State(store): State<Store>,
    Query(query): Query<PageQuery>,
) -> Result<Response, QueryError> {
    let (after, limit) = query.bounded()?;
    cacheable(&store.buyers(after, limit).await?)
}

async fn buyer(State(store): State<Store>, Path(id): Path<String>) -> Result<Response, QueryError> {
    let value = store.buyer(&id).await?.ok_or(QueryError::NotFound)?;
    cacheable(&Versioned {
        api_version: API_VERSION,
        data: value,
    })
}

async fn buyer_dossier(
    State(store): State<Store>,
    Path(id): Path<String>,
) -> Result<Response, QueryError> {
    let value = store
        .buyer_dossier(&id)
        .await?
        .ok_or(QueryError::NotFound)?;
    cacheable(&Versioned {
        api_version: API_VERSION,
        data: value,
    })
}

async fn buyer_timeline(
    State(store): State<Store>,
    Path(id): Path<String>,
    Query(query): Query<PageQuery>,
) -> Result<Response, QueryError> {
    let (after, limit) = query.bounded()?;
    cacheable(&store.buyer_timeline(&id, after, limit).await?)
}

async fn services(
    State(store): State<Store>,
    Query(query): Query<PageQuery>,
) -> Result<Response, QueryError> {
    let (after, limit) = query.bounded()?;
    cacheable(&store.services(after, limit).await?)
}

async fn service(
    State(store): State<Store>,
    Path(id): Path<String>,
) -> Result<Response, QueryError> {
    let value = store.service(&id).await?.ok_or(QueryError::NotFound)?;
    cacheable(&Versioned {
        api_version: API_VERSION,
        data: value,
    })
}

async fn graph(
    State(store): State<Store>,
    Path((kind, id)): Path<(String, String)>,
    Query(query): Query<PageQuery>,
) -> Result<Response, QueryError> {
    let (_, limit) = query.bounded()?;
    cacheable(&Versioned {
        api_version: API_VERSION,
        data: store.graph(&kind, &id, limit).await?,
    })
}

async fn provenance(
    State(store): State<Store>,
    Path(id): Path<String>,
) -> Result<Response, QueryError> {
    let value = store.provenance(&id).await?.ok_or(QueryError::NotFound)?;
    cacheable(&Versioned {
        api_version: API_VERSION,
        data: value,
    })
}

async fn search(
    State(store): State<Store>,
    Query(query): Query<SearchQuery>,
) -> Result<Response, QueryError> {
    let q = query.q.trim();
    if q.is_empty() || q.len() > 200 {
        return Err(QueryError::Invalid(
            "q must contain between 1 and 200 characters".into(),
        ));
    }
    let page = PageQuery {
        limit: query.limit,
        cursor: query.cursor,
    };
    let (after, limit) = page.bounded()?;
    cacheable(&store.search(q, after, limit).await?)
}

async fn investigations(
    State(store): State<Store>,
    Query(query): Query<PageQuery>,
) -> Result<Response, QueryError> {
    let (after, limit) = query.bounded()?;
    cacheable(&store.investigations(after, limit).await?)
}

async fn system(State(store): State<Store>) -> Result<Response, QueryError> {
    cacheable(&Versioned {
        api_version: API_VERSION,
        data: store.system().await?,
    })
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::{BUYER_CLASSIFICATIONS_SQL, Fact, decode_cursor, encode_cursor, type_graph_edge};

    #[test]
    fn buyer_classification_query_uses_explicit_promotion_authority() {
        assert!(BUYER_CLASSIFICATIONS_SQL.contains("current_buyer_classification_runs"));
        assert!(!BUYER_CLASSIFICATIONS_SQL.contains("run.created_at DESC"));
    }

    #[test]
    fn composite_cursor_round_trips_delimiter_characters() {
        let cursor = encode_cursor(&["2026-09-30T01:02:03Z", "base:settlement:abc"]);
        assert_eq!(
            decode_cursor(&cursor, 2).unwrap(),
            vec!["2026-09-30T01:02:03Z", "base:settlement:abc"]
        );
    }

    #[test]
    fn malformed_composite_cursor_is_rejected() {
        assert!(decode_cursor("not-hex", 2).is_err());
        assert!(decode_cursor(&encode_cursor(&["only-one"]), 2).is_err());
    }

    #[test]
    fn graph_edges_are_explicitly_typed_and_directed() {
        let mut edge = Fact {
            id: "edge:1".into(),
            kind: "attribution".into(),
            label: "explicit_requirement".into(),
            value: json!({
                "buyer_handle_id": "buyer:1",
                "service_id": "service:1",
                "confidence": "0.5000",
                "level": "weak"
            }),
            observed_at: "2026-09-30T00:00:00Z".into(),
            provenance_ids: vec!["provenance:1".into()],
        };

        type_graph_edge(&mut edge);

        assert_eq!(
            edge.value["source"],
            json!({"kind": "buyer", "id": "buyer:1"})
        );
        assert_eq!(
            edge.value["target"],
            json!({"kind": "service", "id": "service:1"})
        );
        assert_eq!(edge.value["predicate"], "candidate_for");
        assert_eq!(edge.value["direction"], "outbound");
        assert_eq!(edge.value["attribution_method"], "explicit_requirement");
    }
}
