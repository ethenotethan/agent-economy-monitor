use agent_economy_monitor::x402_history::{
    FacilitatorRegistry, HistoryError, parse_admitted_registry, plan_history_ranges,
    verify_facilitator_rpc_evidence,
};

const REGISTRY: &str = r#"{
  "schema_version":1,
  "registry_version":"x402scan-5732bb83c8c4290f413f24318a838e3fee4ed2bf",
  "entries":[
    {"facilitator_id":"coinbase","chain":"base","address":"0x8f5cb67b49555e614892b7233cfddebfb746e531","asset":"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913","start_unix_seconds":1765843200,"active":true},
    {"facilitator_id":"payai","chain":"solana","address":"2wKupLR9q6wXYppw8Gr2NvWxKBUqm4PPJKkQfoxHDBg4","asset":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v","start_unix_seconds":1751328000,"active":true}
  ],
  "unavailable":[
    {"chain":"ethereum","reason":"no-admitted-x402-facilitator-registry"},
    {"chain":"tempo","reason":"mpp-is-a-separate-protocol-series"}
  ]
}"#;

fn evidence_id(byte: char) -> String {
    format!("evidence:sha256:{}", byte.to_string().repeat(64))
}

#[test]
fn versioned_registry_preserves_supported_entries_and_explicit_chain_gaps() {
    let registry = FacilitatorRegistry::parse(REGISTRY.as_bytes()).unwrap();

    assert_eq!(
        registry.version(),
        "x402scan-5732bb83c8c4290f413f24318a838e3fee4ed2bf"
    );
    assert_eq!(
        registry.sha256(),
        "fdb7f26b78b7320c2fcc0cc0a581ea50a3cb9b3cbc11b616060f022d29cbac90"
    );
    let reformatted = FacilitatorRegistry::parse(format!("\n{REGISTRY}").as_bytes()).unwrap();
    assert_ne!(registry.sha256(), reformatted.sha256());
    assert_eq!(registry.entries_for_chain("base").len(), 1);
    assert_eq!(registry.entries_for_chain("solana").len(), 1);
    assert_eq!(
        registry.unavailable_reason("ethereum"),
        Some("no-admitted-x402-facilitator-registry")
    );
    assert_eq!(
        registry.unavailable_reason("tempo"),
        Some("mpp-is-a-separate-protocol-series")
    );
}

#[test]
fn admitted_registry_rejects_changed_bytes_even_when_version_is_unchanged() {
    let admitted = FacilitatorRegistry::parse(REGISTRY.as_bytes()).unwrap();
    assert!(
        parse_admitted_registry(REGISTRY.as_bytes(), admitted.version(), admitted.sha256()).is_ok()
    );

    let changed_bytes = format!("\n{REGISTRY}");
    assert!(matches!(
        parse_admitted_registry(
            changed_bytes.as_bytes(),
            admitted.version(),
            admitted.sha256(),
        ),
        Err(HistoryError::InvalidRegistry)
    ));
}

#[test]
fn evm_receipt_requires_registered_facilitator_finality_and_transfer_log() {
    let registry = FacilitatorRegistry::parse(REGISTRY.as_bytes()).unwrap();
    let response = br#"[
      {"jsonrpc":"2.0","id":"block","result":{"number":"0x25317c0","timestamp":"0x6940a101"}},
      {"jsonrpc":"2.0","id":"finalized","result":{"number":"0x25317c1"}},
      {"jsonrpc":"2.0","id":"receipts","result":[{
        "from":"0x8f5cb67b49555e614892b7233cfddebfb746e531",
        "status":"0x1",
        "transactionHash":"0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "blockNumber":"0x25317c0",
        "logs":[{
          "address":"0x833589fcd6edb6e08f4c7c32d4f71b54bda02913",
          "logIndex":"0x7",
          "topics":[
            "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
            "0x0000000000000000000000001111111111111111111111111111111111111111",
            "0x0000000000000000000000002222222222222222222222222222222222222222"
          ],
          "data":"0x00000000000000000000000000000000000000000000000000000000000f4240",
          "removed":false
        }]
      }]}
    ]"#;

    let payments =
        verify_facilitator_rpc_evidence(&registry, "base", &evidence_id('a'), 39_000_000, response)
            .unwrap();

    assert_eq!(payments.len(), 1);
    assert_eq!(
        payments[0].payer(),
        "0x1111111111111111111111111111111111111111"
    );
    assert_eq!(
        payments[0].recipient(),
        "0x2222222222222222222222222222222222222222"
    );
    assert_eq!(payments[0].amount_atomic(), "1000000");
    assert_eq!(payments[0].facilitator_id(), "coinbase");
    assert_eq!(payments[0].attribution_class(), "known_facilitator_onchain");

    let generic = response.to_vec();
    let generic = String::from_utf8(generic).unwrap().replace(
        "0x8f5cb67b49555e614892b7233cfddebfb746e531",
        "0x9999999999999999999999999999999999999999",
    );
    assert!(
        verify_facilitator_rpc_evidence(
            &registry,
            "base",
            &evidence_id('b'),
            39_000_000,
            generic.as_bytes(),
        )
        .unwrap()
        .is_empty()
    );

    let non_final = String::from_utf8(response.to_vec())
        .unwrap()
        .replace("0x25317c1", "0x25317bf");
    assert_eq!(
        verify_facilitator_rpc_evidence(
            &registry,
            "base",
            &evidence_id('c'),
            39_000_000,
            non_final.as_bytes(),
        ),
        Err(HistoryError::NonFinalSettlement)
    );

    let duplicate_member = String::from_utf8(response.to_vec()).unwrap().replace(
        "{\"jsonrpc\":\"2.0\",\"id\":\"block\",\"result\":",
        "{\"jsonrpc\":\"2.0\",\"id\":\"block\",\"result\":null,\"result\":",
    );
    assert_eq!(
        verify_facilitator_rpc_evidence(
            &registry,
            "base",
            &evidence_id('e'),
            39_000_000,
            duplicate_member.as_bytes(),
        ),
        Err(HistoryError::InvalidRpcEvidence)
    );
}

#[test]
fn solana_payment_requires_registered_facilitator_signature_and_finalized_transfer() {
    let registry = FacilitatorRegistry::parse(REGISTRY.as_bytes()).unwrap();
    let response = br#"{
      "jsonrpc":"2.0","id":"block","result":{
        "blockHeight":349999900,
        "blockTime":1763100000,
        "blockhash":"block-hash",
        "previousBlockhash":"previous-hash",
        "parentSlot":350000000,
        "transactions":[{
          "transaction":{
            "signatures":["signature-one"],
            "message":{"accountKeys":[
              {"pubkey":"2wKupLR9q6wXYppw8Gr2NvWxKBUqm4PPJKkQfoxHDBg4","signer":true},
              {"pubkey":"payer11111111111111111111111111111111111111","signer":true}
            ]}
          },
          "meta":{
            "err":null,
            "innerInstructions":[{"index":0,"instructions":[{
              "program":"spl-token",
              "parsed":{"type":"transferChecked","info":{
                "authority":"payer11111111111111111111111111111111111111",
                "destination":"recipient-token-account",
                "mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "tokenAmount":{"amount":"250000","decimals":6}
              }}
            }]}],
            "postTokenBalances":[{
              "accountIndex":2,
              "mint":"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
              "owner":"merchant111111111111111111111111111111111111"
            }]
          }
        }]
      }
    }"#;

    let payments = verify_facilitator_rpc_evidence(
        &registry,
        "solana",
        &evidence_id('d'),
        350_000_001,
        response,
    )
    .unwrap();

    assert_eq!(payments.len(), 1);
    assert_eq!(
        payments[0].payer(),
        "payer11111111111111111111111111111111111111"
    );
    assert_eq!(payments[0].amount_atomic(), "250000");
    assert_eq!(payments[0].facilitator_id(), "payai");

    let contradictory_envelope = String::from_utf8(response.to_vec()).unwrap().replace(
        "\"jsonrpc\":\"2.0\",\"id\":\"block\",\"result\":",
        "\"jsonrpc\":\"2.0\",\"id\":\"block\",\"error\":{\"code\":-32000,\"message\":\"failed\"},\"result\":",
    );
    assert_eq!(
        verify_facilitator_rpc_evidence(
            &registry,
            "solana",
            &evidence_id('e'),
            350_000_001,
            contradictory_envelope.as_bytes(),
        ),
        Err(HistoryError::InvalidRpcEvidence)
    );
}

#[test]
fn history_planner_covers_earliest_to_frozen_boundary_without_gaps_and_resumes() {
    assert_eq!(
        plan_history_ranges(10, 25, None, 6).unwrap(),
        vec![(10, 15), (16, 21), (22, 25)]
    );
    assert_eq!(
        plan_history_ranges(10, 25, Some(15), 6).unwrap(),
        vec![(16, 21), (22, 25)]
    );
    assert!(plan_history_ranges(10, 25, Some(25), 6).unwrap().is_empty());
}

#[test]
fn history_planner_rejects_invalid_or_noncontiguous_authority() {
    assert_eq!(
        plan_history_ranges(25, 10, None, 6),
        Err(HistoryError::InvalidInput)
    );
    assert_eq!(
        plan_history_ranges(10, 25, Some(9), 6),
        Err(HistoryError::InvalidInput)
    );
    assert_eq!(
        plan_history_ranges(10, 25, None, 0),
        Err(HistoryError::InvalidInput)
    );
}
