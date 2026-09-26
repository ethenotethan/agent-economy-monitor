# Agent Economy Monitor Context

Canonical language and resolved design decisions for the product factory.

## Language

**Observation**: One source's immutable statement that something was seen. Multiple observations may corroborate or contradict one canonical event.

**Canonical event**: A deterministically keyed protocol or chain event derived from observations.

**Buyer handle**: A chain-scoped wallet, public key, DID, or opaque facilitator identifier. It is not automatically a real-world identity.

**Buyer cluster**: A versioned, reversible grouping of buyer handles connected by evidence-backed attribution edges.

**Classification claim**: A versioned label targeting a buyer handle or cluster with method, evidence window, confidence, provenance, and status (`verified`, `inferred`, or `disputed`).

**Service**: A discoverable paid capability exposed through one or more endpoints and offers.

**Shadow index**: Unverified candidates inferred from discovery or payment activity. Shadow records never masquerade as canonical catalog entries.

**Evidence**: Content-addressed raw material supporting an observation, attribution, or classification.

**Semantic projection**: LLM-authored, cited narrative derived from bounded deterministic snapshots. It is not canonical evidence.

## Relationships

- A **source** produces **observations**.
- One or more **observations** support a **canonical event**.
- A **buyer handle** participates in events and may belong to a reversible **buyer cluster**.
- A **service** exposes **endpoints**, which expose **offers** and **payment options**.
- An **attribution claim** links a settlement to a payment requirement, endpoint, or service with explicit confidence.
- A **classification claim** describes a buyer handle or cluster and cites deterministic features.
- A **semantic projection** cites evidence and canonical entities through stable IDs.

## Resolved terminology ambiguities

- “All” means maximum discovery, not maximum trust: discover broadly into a shadow index and publish canonically only after verification.
- Buyer and seller are contextual roles, not permanent actor types.
- The system indexes commerce evidence; it is not a private purchase ledger and does not claim to observe fulfillment universally.
- A payer address does not prove human, agent, or organization identity.

## Implementation gates

- Raw evidence remains immutable and replayable.
- Protocol adapters are stateless and cannot write canonical entities directly.
- LLM output cannot promote claims or merge clusters.
- Every dashboard fact exposes provenance.
- Reprocessing uses side-by-side versioned projections and atomic promotion.
- Secrets never enter Git, raw evidence, logs, or wiki pages.
