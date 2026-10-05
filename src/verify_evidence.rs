use std::sync::Arc;

use agent_economy_evidence_store::{
    EvidenceObject, EvidenceStore, FilesystemEvidenceStore, GcsEvidenceStore, GcsObjectClient,
    ReadReceipt,
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::Mutex;
use tokio_postgres::Client;

use crate::collect::{ArchivedEvidence, CollectionError, verify_replayed_evidence};

#[async_trait]
pub trait CollectionEvidenceReader: Send + Sync {
    async fn read(&self, object: &EvidenceObject) -> Result<ReadReceipt, CollectionError>;
}

#[async_trait]
impl CollectionEvidenceReader for FilesystemEvidenceStore {
    async fn read(&self, object: &EvidenceObject) -> Result<ReadReceipt, CollectionError> {
        EvidenceStore::read_with_identity(self, object)
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)
    }
}

#[async_trait]
impl<C> CollectionEvidenceReader for GcsEvidenceStore<C>
where
    C: GcsObjectClient,
{
    async fn read(&self, object: &EvidenceObject) -> Result<ReadReceipt, CollectionError> {
        EvidenceStore::read_with_identity(self, object)
            .await
            .map_err(|_| CollectionError::EvidenceUnavailable)
    }
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PendingEvidence {
    evidence_id: String,
    sha256: String,
    storage_uri: String,
    storage_generation: Option<String>,
    media_type: String,
    byte_length: u64,
    height: u64,
}

struct PendingBatch {
    pending_id: String,
    verifier_token: String,
    batch_sha256: String,
    chain: String,
    source_id: String,
    observed_at_unix_ms: i64,
    start_height: u64,
    end_height: u64,
    evidence: Vec<PendingEvidence>,
}

pub struct PostgresEvidenceVerifier {
    client: Arc<Mutex<Client>>,
    evidence_store: Arc<dyn CollectionEvidenceReader>,
    lease_owner: String,
}

impl PostgresEvidenceVerifier {
    pub fn new(
        client: Client,
        evidence_store: Arc<dyn CollectionEvidenceReader>,
        lease_owner: String,
    ) -> Self {
        Self {
            client: Arc::new(Mutex::new(client)),
            evidence_store,
            lease_owner,
        }
    }

    pub async fn run_once(&self) -> Result<Option<String>, CollectionError> {
        let pending = self.claim().await?;
        let Some(pending) = pending else {
            return Ok(None);
        };
        match self.verify_and_promote(&pending).await {
            Ok(()) => Ok(Some(pending.pending_id)),
            Err(error) => {
                let retryable = matches!(
                    error,
                    CollectionError::EvidenceUnavailable | CollectionError::CommitUnavailable
                );
                let code = match error {
                    CollectionError::InvalidInput => "invalid_evidence_readback",
                    CollectionError::EvidenceUnavailable => "evidence_read_unavailable",
                    CollectionError::CommitUnavailable => "promotion_unavailable",
                };
                let _ = self.fail(&pending, code, retryable).await;
                Err(error)
            }
        }
    }

    async fn claim(&self) -> Result<Option<PendingBatch>, CollectionError> {
        let client = self.client.lock().await;
        let row = client
            .query_opt(
                "SELECT pending_id::text, verifier_token::text, batch_sha256, chain_scope, \
                 source_id, observed_at_unix_ms, start_height, end_height, evidence_json::text \
                 FROM agent_economy.claim_pending_collection_batch($1, 120::bigint)",
                &[&self.lease_owner],
            )
            .await
            .map_err(|_| CollectionError::CommitUnavailable)?;
        row.map(|row| {
            let start =
                u64::try_from(row.get::<_, i64>(6)).map_err(|_| CollectionError::InvalidInput)?;
            let end =
                u64::try_from(row.get::<_, i64>(7)).map_err(|_| CollectionError::InvalidInput)?;
            let evidence = serde_json::from_str(&row.get::<_, String>(8))
                .map_err(|_| CollectionError::InvalidInput)?;
            Ok(PendingBatch {
                pending_id: row.get(0),
                verifier_token: row.get(1),
                batch_sha256: row.get(2),
                chain: row.get(3),
                source_id: row.get(4),
                observed_at_unix_ms: row.get(5),
                start_height: start,
                end_height: end,
                evidence,
            })
        })
        .transpose()
    }

    async fn verify_and_promote(&self, pending: &PendingBatch) -> Result<(), CollectionError> {
        let mut verified = Vec::with_capacity(pending.evidence.len());
        for item in &pending.evidence {
            if item
                .storage_generation
                .as_deref()
                .is_some_and(str::is_empty)
            {
                return Err(CollectionError::InvalidInput);
            }
            let object = EvidenceObject::parse(&item.storage_uri)
                .map_err(|_| CollectionError::InvalidInput)?;
            if item.evidence_id != format!("evidence:sha256:{}", item.sha256)
                || object.sha256() != item.sha256
            {
                return Err(CollectionError::InvalidInput);
            }
            let readback = self.evidence_store.read(&object).await?;
            if readback.generation != item.storage_generation {
                return Err(CollectionError::InvalidInput);
            }
            if readback.bytes.len() as u64 != item.byte_length {
                return Err(CollectionError::InvalidInput);
            }
            verified.push(ArchivedEvidence::from_verified_readback(
                item.storage_uri.clone(),
                item.sha256.clone(),
                item.media_type.clone(),
                item.height,
                readback.bytes,
            )?);
        }
        let batch = verify_replayed_evidence(
            pending.chain.clone(),
            pending.source_id.clone(),
            pending.observed_at_unix_ms,
            pending.start_height,
            pending.end_height,
            verified,
        )?;
        if batch.evidence_manifest_sha256() != pending.batch_sha256 {
            return Err(CollectionError::InvalidInput);
        }
        let mut observations = batch
            .observations()
            .iter()
            .map(|item| {
                json!({
                    "observation_id": item.id(),
                    "protocol": item.protocol(),
                    "evidence_id": item.evidence_id(),
                    "observation_hash": item.observation_hash(),
                    "parser_version": item.observation().provenance().parser_version(),
                    "height": item.height(),
                })
            })
            .collect::<Vec<_>>();
        observations.sort_by_key(|item| {
            (
                item["height"].as_u64().unwrap_or_default(),
                item["protocol"].as_str().unwrap_or_default().to_owned(),
                item["observation_id"]
                    .as_str()
                    .unwrap_or_default()
                    .to_owned(),
            )
        });
        let observations_json =
            serde_json::to_string(&observations).map_err(|_| CollectionError::InvalidInput)?;
        let result = json!({
            "batch_sha256": pending.batch_sha256,
            "observations": observations,
            "parser_contract": "collection-readback-v1",
        });
        let result_sha256 = format!("{:x}", Sha256::digest(result.to_string().as_bytes()));
        let client = self.client.lock().await;
        let promoted = client
            .query_one(
                "SELECT agent_economy.promote_pending_collection_batch(\
             $1::text::uuid, $2, $3::text::uuid, $4::text::jsonb, $5)",
                &[
                    &pending.pending_id,
                    &self.lease_owner,
                    &pending.verifier_token,
                    &observations_json,
                    &result_sha256,
                ],
            )
            .await
            .map_err(|_| CollectionError::CommitUnavailable)?
            .get::<_, bool>(0);
        promoted
            .then_some(())
            .ok_or(CollectionError::CommitUnavailable)
    }

    async fn fail(
        &self,
        pending: &PendingBatch,
        error_code: &str,
        retryable: bool,
    ) -> Result<(), CollectionError> {
        let client = self.client.lock().await;
        client
            .query_one(
                "SELECT agent_economy.fail_pending_collection_batch(\
             $1::text::uuid, $2, $3::text::uuid, $4, $5)",
                &[
                    &pending.pending_id,
                    &self.lease_owner,
                    &pending.verifier_token,
                    &error_code,
                    &retryable,
                ],
            )
            .await
            .map_err(|_| CollectionError::CommitUnavailable)?;
        Ok(())
    }
}
