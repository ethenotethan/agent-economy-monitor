# Agent Economy Monitor

An invite-only intelligence cockpit for discovering, indexing, classifying, and investigating x402 and MPP commerce activity.

[Explore the live, source-backed architecture System Map](https://ethenotethan.github.io/agent-economy-monitor/) generated from `architecture/model/model.json`.

## Product contract

The system separates three authorities:

1. **Deterministic evidence** — immutable observations, protocol parsing, chain finality, and reproducible features.
2. **Canonical knowledge** — services, buyer handles, reversible clusters, classifications, claims, and provenance.
3. **Semantic projection** — cited LLM-generated pages in the isolated `agentic-commerce` wiki.

LLM output may explain or suggest. It may not rewrite evidence, merge buyer identities, or promote canonical classifications.

## Initial scope

- Protocols: x402 and MPP
- Chains: Ethereum, Base, Solana, Tempo through one external RPC provider
- Discovery seeds: x402scan and AgentCash, followed by independent verification
- Product: private shared cockpit with Pulse, Buyers, Services, Graph, Investigations, and System views
- Implementation: Rust, Axum, Leptos, PostgreSQL, and Google Cloud Storage
- Runtime: one Rust image with a scale-to-zero Cloud Run service and bounded Cloud Run worker jobs

Bitcoin Lightning is an adapter slot but is not part of v1.

## Development

Start the lean local data plane with one command:

```bash
./scripts/dev-stack
```

The launcher generates a mode-`0600`, Git-ignored PostgreSQL credential file, starts
only PostgreSQL, waits for its real readiness check, and initializes the mode-`0700`
filesystem evidence root at `.local/evidence`. The named PostgreSQL volume and local
evidence survive ordinary `./scripts/dev-stack down` / `./scripts/dev-stack` restarts.
Use `status` or `logs` to inspect the stack; `reset` removes only the derived PostgreSQL
volume and deliberately retains immutable evidence.

The filesystem evidence adapter implements the production create-only storage contract:
objects are addressed by SHA-256 beneath source and observation-date prefixes, repeated
identical writes are idempotent, and every replay verifies the digest before returning
bytes. Production uses the same object names and semantics with Google Cloud Storage.

The external RPC collector uses one evidence-first contract for Ethereum, Base, Solana,
and Tempo. Alchemy responses are archived before validation, cursor updates use
PostgreSQL compare-and-set checkpoints, and every bounded run returns request-budget,
retry, evidence-object, and gap metrics. RPC endpoints are typed secrets with redacted
debug output; collectors never log endpoint URLs or JSON-RPC request bodies.

```bash
./scripts/verify
cargo run
curl http://127.0.0.1:8080/healthz
```

See [CONTEXT.md](CONTEXT.md), [docs/PRODUCT.md](docs/PRODUCT.md), and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
