# x402 historical transaction sources

Status: bounded implementation decision for issue #81. This note describes source capabilities; it does not claim that an unmeasured chain is complete.

## Admission contract

A source is admissible only when it supplies or can be joined to all of the following:

- an immutable raw response archived before parsing;
- a stable chain coordinate and frozen upper boundary;
- explicit x402 attribution (`explicit_x402_receipt`, `x402_contract_event`, or a versioned known-facilitator signer registry), never a generic token transfer;
- an independent finalized-chain RPC receipt/transaction cross-check;
- documented pagination, retention, earliest available coordinate, and failure state.

`source-complete` means only that one admitted source was traversed from its measured earliest available coordinate through its frozen upper boundary with no gaps or conflicts. `chain-complete` remains false unless every admitted source needed for the chain is exhaustive. Private facilitator traffic and history outside provider retention are never inferred.

## Production source map

| Chain | Historical source | Attribution join | Boundary/pagination | Known limitation |
| --- | --- | --- | --- | --- |
| Ethereum | unavailable | none admitted | none | generic EIP-3009/Permit2/ERC-20 activity is ineligible |
| Base | archival RPC block/receipt replay | transaction sender is in the pinned x402scan facilitator registry and the receipt contains a matching USDC `Transfer` | admitted earliest block through a frozen finalized block, split into contiguous bounded jobs | unknown/self-hosted facilitators and provider retention remain visible gaps |
| Solana | archival RPC finalized-slot replay | a pinned x402scan facilitator is a transaction signer and a matching SPL `transferChecked` is finalized | admitted earliest slot through a frozen finalized slot, split into contiguous bounded jobs | unknown/self-hosted facilitators and provider retention remain visible gaps |
| Tempo | unavailable for x402 | none admitted | none | Tempo MPP is a separate protocol series and is never relabeled x402 |
| Newly discovered chains | same adapter contract with a dynamic `chain_scope` | explicit protocol evidence plus finalized RPC | source-defined coordinate recorded in the registry | never silently promoted to a supported or complete chain |

Base and Solana use the owner-approved `known_facilitator_onchain` evidence class pinned to x402scan source commit `5732bb83c8c4290f413f24318a838e3fee4ed2bf`. The repository registry is versioned and content-addressed. Admission freezes its digest, earliest coordinate, upper coordinate, range size, and tail interval before jobs are created. Ethereum and Tempo remain explicit unavailable rows rather than synthetic jobs.

The owner admission function creates the source before collection, so an honestly empty finalized range still commits coverage. The planner accepts only a complete contiguous earliest-to-frozen manifest set. Each manifest remains an owner-controlled, digest-named input file; the restricted collector can claim only the matching immutable job admission. After a frozen plan is complete, a later owner admission freezes the next tail boundary; no worker may move the boundary itself.

## Public index investigation

The public x402scan UI currently exposes network and transaction aggregates, but this spike did not find a documented, stable, complete export API with a reproducible frozen-boundary cursor. It is useful for discovery and cross-checking, not as the sole canonical corpus.

Naven documents `GET https://api.naven.network/x402scan/transactions` with recipient-sequence pagination and explicit `authorization`/`signature` fields. A live anonymous probe returned chain id `4663` records. Its earliest retained coordinate, cross-chain inventory, global traversal semantics, and completeness are not documented. Therefore it may be admitted only as a recipient-scoped corroborating source with `retention_limit = provider-retained-window-unknown`; it must not be labeled chain-complete.

No paid provider was enabled and no credentials were introduced.

## Reconciliation and restart rules

1. Freeze the source's upper coordinate.
2. Archive every fetched page before parse/verification.
3. Require contiguous coordinates from the durable checkpoint through the frozen upper boundary.
4. Verify facilitator signer membership and transfer fields against independently archived finalized RPC evidence.
5. Deduplicate by `(chain_scope, transaction_reference, settlement_position)` into canonical events and settlements while retaining every source observation link and failing conflicting payloads closed.
6. Advance the durable checkpoint only after the entire range verifies. A crash before commit replays the same pages idempotently.
7. Publish `partial`, `source-complete`, `stale`, or `failed` coverage with counts, gaps, lag, retention limits, and provenance.

## Failure semantics

Malformed pages, missing attribution, transfer/attestation mismatches, cursor gaps, divergent corroboration, source-boundary drift, and non-finalized RPC results fail closed. They produce immutable evidence and a failed/partial coverage receipt; they never create or advance canonical settlement state.
