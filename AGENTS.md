# Agent instructions

Read `README.md`, `CONTEXT.md`, `docs/PRODUCT.md`, and `docs/ARCHITECTURE.md` before planning changes.

## Invariants

- Rust application code; do not introduce Node/Python application services.
- Deterministic evidence and canonical state are separate from LLM projections.
- Protocol adapters emit immutable observations; they do not mutate entities.
- Preserve raw evidence hashes, source provenance, parser versions, and replayability.
- Buyer clustering is reversible and confidence-weighted.
- Do not treat chain transfers as x402/MPP without explicit attribution evidence.
- No secrets, bearer headers, payment signatures, request bodies, or prompts in logs or fixtures.
- The `agentic-commerce` wiki is isolated from research and life wikis.
- Use test-driven development and keep each PR a bounded vertical slice.

## Verification

Run `./scripts/verify` before opening a PR. For dashboard changes, add browser-level tests once the Leptos surface exists.

## Factory contract

GitHub Issues own product intent and dependencies. Workers implement but never merge. Independent validation qualifies the exact PR head. The deterministic merge queue alone may merge a PR carrying the required factory labels and complete green checks.
