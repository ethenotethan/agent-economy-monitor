use agent_economy_monitor::{
    collect::{ArchivedEvidence, verify_replayed_evidence},
    recovery::{RecoveryDrill, RecoveryError},
};
use agent_economy_rpc_collector::{Chain, RawRpcResponse, RpcEvidence};
use base64::{Engine as _, engine::general_purpose::STANDARD};
use sha2::{Digest, Sha256};

const OBSERVED_AT_UNIX_MS: i64 = 1_759_276_800_000;

fn chain(value: &str) -> Chain {
    match value {
        "ethereum" => Chain::Ethereum,
        "base" => Chain::Base,
        "solana" => Chain::Solana,
        "tempo" => Chain::Tempo,
        _ => unreachable!(),
    }
}

fn protocol_fixture(protocol: &str, amount: &str) -> (&'static str, String, Vec<u8>) {
    match protocol {
        "x402" => {
            let payload = serde_json::json!({
                "x402Version": 2,
                "accepts": [{
                    "scheme": "exact",
                    "network": "eip155:8453",
                    "amount": amount,
                    "asset": "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913",
                    "payTo": "0x1111111111111111111111111111111111111111",
                    "resource": "https://merchant.example/paid",
                    "extra": {"name": "USD Coin"}
                }]
            })
            .to_string();
            let runtime = format!(
                "HTTP/1.1 402 Payment Required\r\npayment-required: {}\r\n\r\n",
                STANDARD.encode(payload)
            );
            (
                "x402_runtime",
                "https://merchant.example/paid".to_owned(),
                runtime.into_bytes(),
            )
        }
        "mpp" => (
            "mpp_openapi",
            "service:recovery".to_owned(),
            serde_json::to_vec(&serde_json::json!({
                "openapi": "3.1.0",
                "info": {"title": "Recovery", "version": "1.0"},
                "paths": {"/paid": {"post": {
                    "x-payment-info": {
                        "intent": "charge",
                        "method": "tempo",
                        "amount": amount,
                        "currency": "USD"
                    },
                    "responses": {"402": {"description": "payment required"}}
                }}}
            }))
            .unwrap(),
        ),
        _ => unreachable!(),
    }
}

fn production_entry(
    chain_name: &str,
    protocol: &str,
    amount: &str,
    height: u64,
    generation: u64,
) -> serde_json::Value {
    let chain = chain(chain_name);
    let (kind, context, protocol_bytes) = protocol_fixture(protocol, amount);
    let mut result = if chain == Chain::Solana {
        serde_json::json!({
            "blockHeight": height,
            "blockhash": format!("block-{height}"),
            "parentSlot": height.saturating_sub(1),
            "previousBlockhash": "block-parent",
            "transactions": []
        })
    } else {
        serde_json::json!({"number": format!("0x{height:x}")})
    };
    result["agentEconomyProtocolEvidence"] = serde_json::json!([{
        "height": height,
        "kind": kind,
        "context": context,
        "evidence_base64": STANDARD.encode(protocol_bytes),
    }]);
    let body = serde_json::to_vec(&serde_json::json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": result,
    }))
    .unwrap();
    let response = RawRpcResponse::try_new(200, body).unwrap();
    let bytes = RpcEvidence::from_response(chain, height, 1, &response).encode();
    let digest = format!("{:x}", Sha256::digest(&bytes));
    let object_name = format!(
        "evidence/alchemy-{chain_name}/2025-10-01/sha256/{}/{}",
        &digest[..2],
        digest
    );
    let archived = ArchivedEvidence::from_verified_readback(
        object_name.clone(),
        digest,
        "application/vnd.agent-economy.rpc".to_owned(),
        height,
        bytes.clone(),
    )
    .unwrap();
    let batch = verify_replayed_evidence(
        chain_name.to_owned(),
        format!("alchemy-{chain_name}"),
        OBSERVED_AT_UNIX_MS,
        height,
        height,
        vec![archived],
    )
    .unwrap();
    assert_eq!(batch.observations().len(), 1);

    serde_json::json!({
        "object_name": object_name,
        "generation": generation,
        "buyer_handle_id": "buyer:recovery",
        "chain": chain_name,
        "source_id": format!("alchemy-{chain_name}"),
        "observed_at_unix_ms": OBSERVED_AT_UNIX_MS,
        "height": height,
        "observation_id": batch.observations()[0].id(),
        "payload_base64": STANDARD.encode(bytes),
    })
}

fn complete_manifest(amounts: &[String]) -> Vec<serde_json::Value> {
    let mut entries = Vec::new();
    let mut sequence = 0;
    for chain in ["ethereum", "base", "solana", "tempo"] {
        for protocol in ["x402", "mpp"] {
            entries.push(production_entry(
                chain,
                protocol,
                &amounts[sequence],
                sequence as u64 + 1,
                sequence as u64 + 10,
            ));
            sequence += 1;
        }
    }
    entries
}

fn materialized_manifest(entries: &[serde_json::Value], generation: u64) -> Vec<u8> {
    serde_json::to_vec(&serde_json::json!({
        "manifest_object": "recovery/manifests/buyer-recovery.json",
        "manifest_generation": generation,
        "entries": entries,
    }))
    .unwrap()
}

#[test]
fn rebuilds_a_buyer_dossier_from_production_rpc_evidence() {
    let amounts = (1..=8).map(|value| value.to_string()).collect::<Vec<_>>();
    let entries = complete_manifest(&amounts);
    let manifest = materialized_manifest(&entries, 77);

    let dossier = RecoveryDrill::rebuild_manifest("buyer:recovery", &manifest).unwrap();

    assert_eq!(dossier.buyer_handle_id(), "buyer:recovery");
    assert_eq!(dossier.event_count(), 8);
    assert_eq!(dossier.total_amount_atomic(), "36");
    assert_eq!(
        dossier.coverage(),
        &[
            "base:mpp",
            "base:x402",
            "ethereum:mpp",
            "ethereum:x402",
            "solana:mpp",
            "solana:x402",
            "tempo:mpp",
            "tempo:x402",
        ]
    );
    assert_eq!(dossier.evidence_objects().len(), 8);
    assert_eq!(dossier.provenance_ids().len(), 8);
    assert_eq!(
        dossier.manifest_object(),
        "recovery/manifests/buyer-recovery.json"
    );
    assert_eq!(dossier.manifest_generation(), 77);
    assert_eq!(dossier.digest().len(), 64);
}

#[test]
fn digest_is_permutation_invariant_and_binds_object_generation() {
    let amounts = vec!["1".to_owned(); 8];
    let entries = complete_manifest(&amounts);
    let mut reversed = entries.clone();
    reversed.reverse();
    let forward =
        RecoveryDrill::rebuild_manifest("buyer:recovery", &materialized_manifest(&entries, 77))
            .unwrap();
    let backward =
        RecoveryDrill::rebuild_manifest("buyer:recovery", &materialized_manifest(&reversed, 77))
            .unwrap();
    assert_eq!(forward.digest(), backward.digest());

    let mut other_generation = entries.clone();
    other_generation[0]["generation"] = serde_json::json!(999);
    let changed = RecoveryDrill::rebuild_manifest(
        "buyer:recovery",
        &materialized_manifest(&other_generation, 77),
    )
    .unwrap();
    assert_ne!(forward.digest(), changed.digest());

    let other_manifest_generation =
        RecoveryDrill::rebuild_manifest("buyer:recovery", &materialized_manifest(&entries, 78))
            .unwrap();
    assert_ne!(forward.digest(), other_manifest_generation.digest());
}

#[test]
fn rejects_non_production_payloads_and_supports_the_production_numeric_domain() {
    let mut malformed = complete_manifest(&vec!["1".to_owned(); 8]);
    let malformed_bytes = b"not-rpc-evidence";
    let malformed_digest = format!("{:x}", Sha256::digest(malformed_bytes));
    malformed[0]["object_name"] = serde_json::json!(format!(
        "evidence/alchemy-ethereum/2025-10-01/sha256/{}/{}",
        &malformed_digest[..2],
        malformed_digest
    ));
    malformed[0]["payload_base64"] = serde_json::json!(STANDARD.encode(malformed_bytes));
    assert_eq!(
        RecoveryDrill::rebuild_manifest("buyer:recovery", &materialized_manifest(&malformed, 77),)
            .unwrap_err(),
        RecoveryError::InvalidPayload
    );

    let mut amounts = vec!["0".to_owned(); 8];
    amounts[0] = u128::MAX.to_string();
    let dossier = RecoveryDrill::rebuild_manifest(
        "buyer:recovery",
        &materialized_manifest(&complete_manifest(&amounts), 77),
    )
    .unwrap();
    assert_eq!(dossier.total_amount_atomic(), u128::MAX.to_string());
}

#[test]
fn manifest_requires_nonzero_generation_and_bounded_input() {
    let mut entries = complete_manifest(&vec!["1".to_owned(); 8]);
    entries[0]["generation"] = serde_json::json!(0);
    assert_eq!(
        RecoveryDrill::rebuild_manifest("buyer:recovery", &materialized_manifest(&entries, 77),)
            .unwrap_err(),
        RecoveryError::InvalidManifest
    );
    let valid_entries = complete_manifest(&vec!["1".to_owned(); 8]);
    assert_eq!(
        RecoveryDrill::rebuild_manifest(
            "buyer:recovery",
            &materialized_manifest(&valid_entries, 0),
        )
        .unwrap_err(),
        RecoveryError::InvalidManifest
    );
    assert_eq!(
        RecoveryDrill::rebuild_manifest("buyer:recovery", &vec![b' '; 8 * 1024 * 1024 + 1])
            .unwrap_err(),
        RecoveryError::ManifestTooLarge
    );
}

#[test]
fn manifest_rejects_duplicate_json_members() {
    let entries = complete_manifest(&vec!["1".to_owned(); 8]);
    let manifest = String::from_utf8(materialized_manifest(&entries, 77)).unwrap();
    let ambiguous = manifest.replacen(
        "\"manifest_generation\":77",
        "\"manifest_generation\":77,\"manifest_generation\":78",
        1,
    );

    assert_eq!(
        RecoveryDrill::rebuild_manifest("buyer:recovery", ambiguous.as_bytes()).unwrap_err(),
        RecoveryError::InvalidManifest
    );
}
