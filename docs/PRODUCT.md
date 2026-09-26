# Product requirements

## Goal

Build a private, password-gated collaborative intelligence cockpit for navigating the agent economy across x402 and MPP. Researchers should be able to move from ecosystem-level activity to buyer dossiers, transaction timelines, relationship graphs, classifications, provenance, and cited investigations.

## Users

- Owner/admin operating the monitor
- Invited crypto researchers and trusted friends

V1 uses one shared password. Per-user roles and attribution are deferred.

## Cockpit

1. **Pulse** — live payments, volume, active buyers, protocol/chain split, anomalies.
2. **Buyers** — handles and reversible clusters, behavioral metrics, classifications, counterparties, timelines.
3. **Services** — endpoints, offers, pricing, accepted rails, runtime verification, settlement attribution.
4. **Graph** — buyer ↔ seller ↔ service ↔ protocol navigation.
5. **Investigations** — the isolated Agentic Commerce Intelligence semantic wiki and curated annotations.
6. **System** — ingestion lag, cursors, RPC budget, parser failures, quarantine, and replay status.

Every metric, relationship, and classification exposes a provenance drawer containing source, observation time, chain/block/transaction/finality, protocol version, provider, parser/classifier version, attribution method, confidence, evidence hash, supporting/conflicting observations, and LLM projection metadata where applicable.

## Initial coverage

- Chains: Ethereum, Base, Solana, Tempo
- Protocols: x402 and MPP
- Discovery: x402scan and AgentCash seeds; OpenAPI; well-known documents; runtime 402 challenges; registries; GitHub; submissions; observed recipients
- Transactions: attributable protocol activity plus on-demand full-history enrichment for discovered buyers
- Deferred: Bitcoin Lightning, indiscriminate full-chain ingestion, public access, per-user accounts

## Trust states

- Candidate/shadow
- Observed
- Verified
- Settled/finalized
- Inferred
- Disputed
- Stale

## Acceptance principles

- Same raw evidence and parser version reproduce the same canonical state.
- Multiple providers would remain separate observations even when they corroborate one event.
- Shared `payTo` addresses never force endpoint attribution.
- Buyer-handle clustering is reversible.
- Generated wiki text is visibly generated and cited.
- Human-authored investigations survive regeneration.
