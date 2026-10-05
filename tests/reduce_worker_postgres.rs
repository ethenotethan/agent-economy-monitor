use std::process::Command;

use agent_economy_contracts::{
    EvidenceRef, Observation, ProtocolObservation, Provenance, X402EventKey, X402Observation,
};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};
use tokio_postgres::{Client, NoTls};

const REDUCER_PASSWORD: &str = "reducer_test_password";

fn runtime_database_url(database_url: &str) -> String {
    let (scheme, rest) = database_url
        .split_once("://")
        .expect("test database URL has a scheme");
    let (_, host_and_path) = rest
        .split_once('@')
        .expect("test database URL has explicit credentials");
    format!("{scheme}://agent_economy_reducer_runtime:{REDUCER_PASSWORD}@{host_and_path}")
}

async fn connect(database_url: &str) -> Client {
    let (client, connection) = tokio_postgres::connect(database_url, NoTls).await.unwrap();
    tokio::spawn(async move { connection.await.unwrap() });
    client
}

fn run_reduce(database_url: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_agent-economy-monitor"))
        .arg("reduce")
        .env_clear()
        .env("PATH", std::env::var_os("PATH").unwrap_or_default())
        .env("REDUCER_DATABASE_URL", database_url)
        .env("REDUCE_LEASE_OWNER", "reduce-postgres-test")
        .output()
        .unwrap()
}

fn fixture_observation() -> Observation {
    let key = X402EventKey::payment_identifier(
        "pay_123456789012",
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "merchant-1:/paid/weather",
    )
    .unwrap();
    Observation::new(
        Provenance::new("rpc-primary", 1_790_426_627_000, "x402-adapter@1").unwrap(),
        EvidenceRef::sha256(b"reduce-postgres-fixture", "application/json").unwrap(),
        ProtocolObservation::X402(X402Observation::new(key, "USDC", "1000").unwrap()),
    )
    .unwrap()
}

#[tokio::test]
async fn leased_reduce_retry_commits_canonical_state_and_provenance_atomically() {
    let database_url = std::env::var("AEM_COLLECT_TEST_DATABASE_URL")
        .expect("AEM_COLLECT_TEST_DATABASE_URL is required for reducer PostgreSQL tests");
    let admin = connect(&database_url).await;
    let run_key = u64::from(std::process::id());
    let namespace = format!("00000000-0000-0000-0000-{run_key:012}");
    let job = format!("00000000-0000-0000-0001-{run_key:012}");
    let attack_job = format!("00000000-0000-0000-0003-{run_key:012}");
    let provenance = format!("00000000-0000-0000-0002-{run_key:012}");
    admin
        .batch_execute(&format!(
            "ALTER ROLE agent_economy_reducer_runtime LOGIN PASSWORD '{REDUCER_PASSWORD}'; \
             DELETE FROM agent_economy.reducer_runtime_namespaces \
             WHERE login_name = 'agent_economy_reducer_runtime'; \
             INSERT INTO agent_economy.namespaces(namespace_id,namespace_kind,namespace_key) \
             VALUES ('{namespace}','tenant','reduce-worker-postgres-{run_key}'); \
             INSERT INTO agent_economy.reducer_runtime_namespaces(login_name,namespace_id,purpose) \
             VALUES ('agent_economy_reducer_runtime','{namespace}','reduce')"
        ))
        .await
        .unwrap();

    let observation = fixture_observation();
    let manifest = serde_json::json!({
        "schema_version": 1,
        "reducer_version": "reducer@1",
        "attribution_version": "attribution@1",
        "chain_scope": "base",
        "start_height": 42,
        "end_height": 42,
        "observations": [{
            "source_id": observation.provenance().source_id(),
            "observation_id": observation.id(),
            "height": 42,
            "encoded_base64": STANDARD.encode(observation.encode()),
        }],
        "finality": [{
            "kind": "evm",
            "canonical_event_id": observation.event_key().canonical_id(),
            "transaction_id": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "source_id": observation.provenance().source_id(),
            "provenance_id": provenance,
            "evidence_id": format!("sha256:{}", observation.evidence().digest()),
            "asserted_at_unix_ms": 1_790_426_628_000_i64,
            "block_number": 42,
            "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "canonical_block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "latest_block": 70,
            "finalized_block": 60,
            "execution": "succeeded",
            "confirmations": 12
        }],
        "settlements": [{
            "canonical_event_id": observation.event_key().canonical_id(),
            "transaction_id": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "settled_at_unix_ms": 1_790_426_628_000_i64,
            "source_id": observation.provenance().source_id(),
            "provenance_id": provenance,
            "settlement": {
                "id": format!("settlement:x402:base:{run_key}"),
                "protocol": "x402",
                "network": "base",
                "asset": "USDC",
                "amount_atomic": "1000",
                "pay_to": "0x1111111111111111111111111111111111111111",
                "finality": "finalized",
                "requirement_id": serde_json::Value::Null,
                "evidence_ids": [format!("sha256:{}", observation.evidence().digest())]
            },
            "requirements": []
        }]
    });
    let manifest_text = manifest.to_string();
    let digest = admin
        .query_one(
            "SELECT encode(sha256(convert_to($1::text::jsonb::text, 'UTF8')), 'hex')",
            &[&manifest_text],
        )
        .await
        .unwrap()
        .get::<_, String>(0);
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
             namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256) \
             VALUES ($1::text::uuid,$2::text::uuid,'reduce','canonical-observation-range-v1', \
             'reduce-postgres-49',$3)",
            &[&namespace, &job, &digest],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.reducer_job_inputs( \
             namespace_id,job_id,schema_version,reducer_version,attribution_version, \
             chain_scope,start_height,end_height,input_manifest) \
             VALUES ($1::text::uuid,$2::text::uuid,1,'reducer@1','attribution@1', \
             'base',42,42,$3::text::jsonb)",
            &[&namespace, &job, &manifest_text],
        )
        .await
        .unwrap();

    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
             namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256,scheduled_for) \
             VALUES ($1::text::uuid,$2::text::uuid,'reduce','canonical-observation-range-v1', \
             'reduce-postgres-attack',$3,clock_timestamp()-interval '1 minute')",
            &[&namespace, &attack_job, &digest],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.reducer_job_inputs( \
             namespace_id,job_id,schema_version,reducer_version,attribution_version, \
             chain_scope,start_height,end_height,input_manifest) \
             VALUES ($1::text::uuid,$2::text::uuid,1,'reducer@1','attribution@1', \
             'base',42,42,$3::text::jsonb)",
            &[&namespace, &attack_job, &manifest_text],
        )
        .await
        .unwrap();

    let reducer_url = runtime_database_url(&database_url);
    let restricted = connect(&reducer_url).await;
    let claimed = restricted
        .query_one(
            "SELECT job_id::text, lease_token::text \
             FROM agent_economy.claim_reducer_job('reduce-bypass-test',60)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(claimed.get::<_, String>(0), attack_job);
    let bypassed = restricted
        .query_one(
            "SELECT agent_economy.complete_reducer_job( \
             $1::text::uuid,'reduce-bypass-test',$2::text::uuid,$3)",
            &[&attack_job, &claimed.get::<_, String>(1), &"f".repeat(64)],
        )
        .await
        .unwrap()
        .get::<_, bool>(0);
    assert!(
        !bypassed,
        "reducer completion must require a committed receipt"
    );
    assert!(
        restricted
            .query_one(
                "SELECT agent_economy.fail_reducer_job( \
                 $1::text::uuid,'reduce-bypass-test',$2::text::uuid,'invalid_reduce_commit',false,0)",
                &[&attack_job, &claimed.get::<_, String>(1)],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
    let first = run_reduce(&reducer_url);
    assert!(
        !first.status.success(),
        "missing canonical input must fail closed"
    );
    let after_failure = admin
        .query_one(
            "SELECT \
             (SELECT status FROM agent_economy.worker_jobs WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid), \
             (SELECT count(*) FROM agent_economy.canonical_events WHERE namespace_id=$1::text::uuid), \
             (SELECT count(*) FROM agent_economy.reducer_checkpoints WHERE namespace_id=$1::text::uuid)",
            &[&namespace, &job],
        )
        .await
        .unwrap();
    assert_eq!(after_failure.get::<_, String>(0), "retryable");
    assert_eq!(after_failure.get::<_, i64>(1), 0);
    assert_eq!(after_failure.get::<_, i64>(2), 0);

    let encoded = observation.encode();
    let observation_hash = format!("{:x}", Sha256::digest(encoded));
    let evidence_id = format!("sha256:{}", observation.evidence().digest());
    admin
        .execute(
            "INSERT INTO agent_economy.evidence_objects( \
             namespace_id,evidence_id,sha256,storage_uri,media_type,byte_length,observed_at) \
             VALUES ($1::text::uuid,$2,$3,$4,'application/json',$5,to_timestamp($6::bigint/1000.0))",
            &[
                &namespace,
                &evidence_id,
                &observation.evidence().digest(),
                &format!("evidence://{}", observation.evidence().digest()),
                &i64::try_from(observation.evidence().byte_length()).unwrap(),
                &observation.provenance().observed_at_unix_ms(),
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.provenance_records( \
             namespace_id,provenance_id,source_id,observed_at,parser_version,provider, \
             chain_scope,block_reference,transaction_reference,evidence_id) \
             VALUES ($1::text::uuid,$2::text::uuid,$3,to_timestamp($4::bigint/1000.0), \
             $5,'fixture','base','42', \
             '0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb',$6)",
            &[
                &namespace,
                &provenance,
                &observation.provenance().source_id(),
                &observation.provenance().observed_at_unix_ms(),
                &observation.provenance().parser_version(),
                &evidence_id,
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.observations( \
             namespace_id,chain_scope,source_id,observation_id,observed_at,parser_version, \
             protocol,evidence_id,provenance_id,observation_hash) \
             VALUES ($1::text::uuid,'base',$2,$3,to_timestamp($4::bigint/1000.0),$5, \
             'x402',$6,$7::text::uuid,$8)",
            &[
                &namespace,
                &observation.provenance().source_id(),
                &observation.id(),
                &observation.provenance().observed_at_unix_ms(),
                &observation.provenance().parser_version(),
                &evidence_id,
                &provenance,
                &observation_hash,
            ],
        )
        .await
        .unwrap();
    admin
        .execute(
            "UPDATE agent_economy.worker_jobs SET scheduled_for=clock_timestamp() \
             WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid",
            &[&namespace, &job],
        )
        .await
        .unwrap();

    let initial_forge_claim = restricted
        .query_one(
            "SELECT job_id::text, lease_token::text \
             FROM agent_economy.claim_reducer_job('reduce-initial-forge',60)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(initial_forge_claim.get::<_, String>(0), job);
    let event_id = observation.event_key().canonical_id();
    let forged_events = serde_json::json!([{
        "protocol": "x402",
        "canonical_event_id": event_id,
        "event_at_unix_ms": observation.provenance().observed_at_unix_ms(),
        "observation_links": [{
            "source_id": observation.provenance().source_id(),
            "observation_id": observation.id(),
            "support_role": "supporting"
        }]
    }])
    .to_string();
    let forged_finality = serde_json::json!([{
        "canonical_event_id": event_id,
        "transaction_id": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "source_id": observation.provenance().source_id(),
        "provenance_id": provenance,
        "evidence_id": evidence_id,
        "asserted_at_unix_ms": 1_790_426_628_000_i64,
        "status": "finalized",
        "position": 42,
        "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "basis": {
            "kind": "evm",
            "block_number": 42,
            "block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "canonical_block_hash": "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "latest_block": 70,
            "finalized_block": 60,
            "execution": "succeeded"
        },
        "timeline_state_hash": "c".repeat(64),
        "accepted": true,
        "current_status": "finalized",
        "current": true
    }])
    .to_string();
    let forged_result = b"forged-initial-attribution";
    let forged_attribution = serde_json::json!([{
        "settlement_id": format!("settlement:x402:base:{run_key}"),
        "canonical_event_id": event_id,
        "transaction_id": "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        "settled_at_unix_ms": 1_790_426_628_000_i64,
        "source_id": observation.provenance().source_id(),
        "provenance_id": provenance,
        "protocol": "x402",
        "asset": "USDC",
        "amount_atomic": "9999",
        "level": "unknown",
        "method": "none",
        "explicit_requirement_id": serde_json::Value::Null,
        "engine_version": "attribution@1",
        "evidence_ids": [evidence_id],
        "input_snapshot_hash": "a".repeat(64),
        "state_hash": format!("{:x}", Sha256::digest(forged_result)),
        "result_encoded_base64": STANDARD.encode(forged_result),
        "requirements": [],
        "candidates": []
    }])
    .to_string();
    let initial_forge = restricted
        .query_one(
            "SELECT agent_economy.commit_reduction_batch( \
             $1::text::uuid,'reduce-initial-forge',$2::text::uuid,$3,$4,'reducer@1', \
             'base',42,42,$5::text::jsonb,$6::text::jsonb,$7::text::jsonb)",
            &[
                &job,
                &initial_forge_claim.get::<_, String>(1),
                &digest,
                &"d".repeat(64),
                &forged_events,
                &forged_finality,
                &forged_attribution,
            ],
        )
        .await;
    assert!(
        initial_forge.is_err(),
        "restricted runtime must not commit output fields that differ from the immutable manifest"
    );
    assert!(
        restricted
            .query_one(
                "SELECT agent_economy.fail_reducer_job( \
                 $1::text::uuid,'reduce-initial-forge',$2::text::uuid,'invalid_reduce_commit',true,0)",
                &[&job, &initial_forge_claim.get::<_, String>(1)],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );

    let second = run_reduce(&reducer_url);
    assert!(
        second.status.success(),
        "reducer retry failed: {}",
        String::from_utf8_lossy(&second.stderr)
    );
    let committed = admin
        .query_one(
            "SELECT \
             (SELECT status='succeeded' FROM agent_economy.worker_jobs WHERE namespace_id=$1::text::uuid AND job_id=$2::text::uuid), \
             (SELECT count(*) FROM agent_economy.canonical_events WHERE namespace_id=$1::text::uuid), \
             (SELECT count(*) FROM agent_economy.canonical_event_observations WHERE namespace_id=$1::text::uuid), \
             (SELECT next_height FROM agent_economy.reducer_checkpoints WHERE namespace_id=$1::text::uuid AND chain_scope='base'), \
             (SELECT count(*) FROM agent_economy.observations WHERE namespace_id=$1::text::uuid)",
            &[&namespace, &job],
        )
        .await
        .unwrap();
    assert!(committed.get::<_, bool>(0));
    assert_eq!(committed.get::<_, i64>(1), 1);
    assert_eq!(committed.get::<_, i64>(2), 1);
    assert_eq!(committed.get::<_, i64>(3), 43);
    assert_eq!(
        committed.get::<_, i64>(4),
        1,
        "source observation was mutated"
    );

    let replay_job = format!("00000000-0000-0000-0004-{run_key:012}");
    admin
        .execute(
            "INSERT INTO agent_economy.worker_jobs( \
             namespace_id,job_id,mode,job_kind,idempotency_key,input_sha256) \
             VALUES ($1::text::uuid,$2::text::uuid,'reduce','canonical-observation-range-v1', \
             'reduce-postgres-forged-replay',$3)",
            &[&namespace, &replay_job, &digest],
        )
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO agent_economy.reducer_job_inputs( \
             namespace_id,job_id,schema_version,reducer_version,attribution_version, \
             chain_scope,start_height,end_height,input_manifest) \
             VALUES ($1::text::uuid,$2::text::uuid,1,'reducer@1','attribution@1', \
             'base',42,42,$3::text::jsonb)",
            &[&namespace, &replay_job, &manifest_text],
        )
        .await
        .unwrap();
    let replay_claim = restricted
        .query_one(
            "SELECT job_id::text, lease_token::text \
             FROM agent_economy.claim_reducer_job('reduce-forged-replay',60)",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(replay_claim.get::<_, String>(0), replay_job);
    let receipt = admin
        .query_one(
            "SELECT output_sha256, events_json::text, finality_json::text, \
                    attributions_json::text \
             FROM agent_economy.reduction_range_receipts \
             WHERE namespace_id=$1::text::uuid AND chain_scope='base' \
               AND start_height=42 AND end_height=42",
            &[&namespace],
        )
        .await
        .unwrap();
    let receipt_finality = receipt.get::<_, String>(2);
    let receipt_attribution = receipt.get::<_, String>(3);
    let mut altered_attribution: serde_json::Value =
        serde_json::from_str(&receipt_attribution).unwrap();
    altered_attribution[0]["input_snapshot_hash"] = serde_json::json!("f".repeat(64));
    let altered_attribution = altered_attribution.to_string();
    let mut added_attribution: serde_json::Value =
        serde_json::from_str(&receipt_attribution).unwrap();
    let extra_attribution = added_attribution[0].clone();
    added_attribution
        .as_array_mut()
        .unwrap()
        .push(extra_attribution);
    let added_attribution = added_attribution.to_string();
    let mut altered_finality: serde_json::Value = serde_json::from_str(&receipt_finality).unwrap();
    altered_finality[0]["timeline_state_hash"] = serde_json::json!("e".repeat(64));
    let altered_finality = altered_finality.to_string();
    let mut added_finality: serde_json::Value = serde_json::from_str(&receipt_finality).unwrap();
    let extra_finality = added_finality[0].clone();
    added_finality.as_array_mut().unwrap().push(extra_finality);
    let added_finality = added_finality.to_string();
    let replay_attacks = [
        (
            "alter attribution",
            receipt_finality.as_str(),
            altered_attribution.as_str(),
        ),
        ("remove attribution", receipt_finality.as_str(), "[]"),
        (
            "add attribution",
            receipt_finality.as_str(),
            added_attribution.as_str(),
        ),
        (
            "alter finality",
            altered_finality.as_str(),
            receipt_attribution.as_str(),
        ),
        ("remove finality", "[]", receipt_attribution.as_str()),
        (
            "add finality",
            added_finality.as_str(),
            receipt_attribution.as_str(),
        ),
    ];
    for (attack, finality_json, attribution_json) in replay_attacks {
        let replay_commit = restricted
            .query_one(
                "SELECT agent_economy.commit_reduction_batch( \
            $1::text::uuid,'reduce-forged-replay',$2::text::uuid,$3,$4,'reducer@1', \
            'base',42,42,$5::text::jsonb,$6::text::jsonb,$7::text::jsonb)",
                &[
                    &replay_job,
                    &replay_claim.get::<_, String>(1),
                    &digest,
                    &receipt.get::<_, String>(0),
                    &receipt.get::<_, String>(1),
                    &finality_json,
                    &attribution_json,
                ],
            )
            .await;
        assert!(
            replay_commit.is_err(),
            "restricted replay must reject {attack} even when all digests and events match"
        );
    }
    let altered_settlement_count = admin
        .query_one(
            "SELECT count(*) FROM agent_economy.settlements \
             WHERE namespace_id=$1::text::uuid AND settlement_id=$2 \
               AND amount_atomic <> 1000",
            &[&namespace, &format!("settlement:x402:base:{run_key}")],
        )
        .await
        .unwrap()
        .get::<_, i64>(0);
    assert_eq!(altered_settlement_count, 0);

    assert!(
        !restricted
            .query_one(
                "SELECT has_table_privilege(session_user,'agent_economy.observations','UPDATE')",
                &[],
            )
            .await
            .unwrap()
            .get::<_, bool>(0)
    );
}
