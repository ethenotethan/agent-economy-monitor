# Agent Economy Monitor

An invite-only intelligence cockpit for discovering, indexing, classifying, and investigating x402 and MPP commerce activity.

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
- Implementation: Rust, Axum, Leptos, Redpanda, ClickHouse, PostgreSQL, S3-compatible evidence storage
- Runtime: one canonical Nomad service definition with internal worker modes

Bitcoin Lightning is an adapter slot but is not part of v1.

## Development

Start the complete local data plane with Docker Compose:

```bash
./scripts/dev-stack
```

The launcher creates a mode-`0600`, Git-ignored `.dev-stack.env` containing generated
local-only credentials, then waits for PostgreSQL, ClickHouse, Redpanda plus Schema
Registry, and S3-compatible evidence storage to become healthy. Named volumes preserve
data across ordinary `down`/`up` restarts. Use `./scripts/dev-stack status` to inspect
the stack, `./scripts/dev-stack down` to stop it, or `./scripts/dev-stack reset` to
explicitly remove its volumes and local data.

The default host endpoints are PostgreSQL `localhost:5432`, ClickHouse
`http://localhost:8123`, Kafka `localhost:19092`, Schema Registry
`http://localhost:18081`, and S3 `http://localhost:4566`. Ports and non-secret settings
can be overridden with the environment placeholders in `compose.yaml`.

```bash
./scripts/verify
cargo run
curl http://127.0.0.1:8080/healthz
```

See [CONTEXT.md](CONTEXT.md), [docs/PRODUCT.md](docs/PRODUCT.md), and [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).
