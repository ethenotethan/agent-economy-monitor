# Lean Google Cloud runtime

This directory deploys one immutable `agent-economy-monitor` image in `asia-southeast1` by default:

- a Cloud Run service with zero minimum instances and two maximum instances;
- five bounded Cloud Run jobs (`collect`, `verify-evidence`, `reduce`, `classify`, and `enrich`) invoked by Cloud Scheduler;
- one zonal Cloud SQL for PostgreSQL 17 instance with automated backups, fourteen retained backups, and seven days of point-in-time recovery logs;
- one versioned Google Cloud Storage bucket with a locked 365-day retention policy;
- separate service, per-worker, and scheduler identities, error/CPU alerts, and a project-scoped monthly budget.

Pub/Sub, BigQuery, GKE, and an always-on broker are intentionally absent. PostgreSQL transactional leases remain the job-assignment authority. Cloud SQL and the evidence bucket are dependencies, not additional product identities.

## Before applying

1. Create a dedicated Google Cloud project and select a billing account.
2. Create the three cockpit secrets in `service_secret_ids`. In `worker_secret_ids`, provide a dedicated database URL for each mode (`COLLECTOR_DATABASE_URL`, `EVIDENCE_VERIFIER_DATABASE_URL`, `REDUCER_DATABASE_URL`, `CLASSIFIER_DATABASE_URL`, and `ENRICHER_DATABASE_URL`) and provide the four `ALCHEMY_*_RPC_URL` secrets only to `collect` and `enrich`. Add secret versions out of band; never put values in Terraform variables or state.
3. Build the repository `Dockerfile`, push it to Artifact Registry, and record the immutable `@sha256:` image reference.
4. Supply the project ID, billing account, namespace UUID, image digest, at least one Monitoring notification channel, and secret IDs in an untracked `.tfvars` file. Terraform resolves the project number from the project ID for budget scoping.
5. Run `terraform init`, `terraform validate`, and inspect `terraform plan` before applying.

The evidence retention lock is irreversible. Apply only in the final evidence project and only after the owner confirms the project and bucket names. `prevent_destroy` and Cloud SQL deletion protection make destructive changes fail closed. Before admitting a collect lease, upload its owner-approved `<input_sha256>.json` request to the private collection-input bucket. That bucket is mounted read-only at `/collection-inputs` only in the collect job.

A Cloud SQL URL should use the mounted `/cloudsql/<project>:<region>:<instance>` Unix socket and TLS is supplied by the Cloud SQL connection path. The application never logs those URLs. Each worker URL must authenticate as the mode-specific PostgreSQL runtime role enforced by the binary. Collect and enrich use a custom create-and-list role so content-addressed retries remain idempotent without granting either identity permission to read object bodies; the verifier can read but not create evidence. None can delete objects or administer retention. Reduce and classify receive no evidence or RPC authority, and no worker receives the cockpit namespace or password secrets.

## Recovery qualification

A release is not recovery-qualified until both checks below have fresh evidence:

1. Restore the newest automated Cloud SQL backup to a disposable recovery instance, run all migrations, and compare namespace row counts and the current backup/PITR settings. Delete the disposable instance after recording only non-secret counts and timestamps.
2. Export a bounded recovery manifest to the retained evidence bucket and run:

```bash
cargo build --locked --release
./scripts/recovery-drill "$EVIDENCE_BUCKET" \
  recovery/manifests/buyer-recovery.json "$MANIFEST_GENERATION" buyer:recovery
```

The generation-pinned manifest index binds each `object_name` and `generation` to its admitted buyer handle, chain/source coordinates, observation time, height, and production observation ID. The script downloads each exact retained binary RPC-evidence generation into a private temporary directory and base64-encodes it only for the bounded local handoff. The Rust drill re-verifies every SHA-256, replays the exact bytes through the production RPC envelope and protocol adapters, verifies the selected observation ID, derives protocol/event identity and amount from that observation, and binds the manifest object/generation plus every evidence object generation into the deterministic receipt. It rejects chain-scoped duplicate settlements and fails unless Ethereum, Base, Solana, and Tempo each contain both x402 and MPP evidence. The script first verifies that bucket versioning and the locked 365-day retention policy are active. It prints only the buyer handle, event count, and rebuilt dossier digest—not raw evidence.

## Observability and rollback

Cloud Run stderr errors from the service and jobs feed a five-minute alert. Cloud SQL CPU above 70% for fifteen minutes has a separate alert. Scheduler retries are bounded, and Cloud Run jobs run one task with three retries and a fifteen-minute timeout. PostgreSQL lease expiry provides record-level retry/reassignment; Cloud Run retries do not replace that authority.

Roll back application code by changing `image` to the prior digest and applying. Do not roll back or destroy retained evidence. Restore derived PostgreSQL state from a backup when possible; if derived dossier state is suspect, replay retained evidence side by side and promote only after comparison.

## Low-traffic cost envelope

The budget resource alerts at the intended USD 75 monthly infrastructure envelope; Google Cloud budgets do not hard-stop spending. Before every apply, refresh the estimate with the [Google Cloud Pricing Calculator](https://cloud.google.com/products/calculator) for the selected project and region. The bounded planning envelope is:

| Item | Monthly planning range |
| --- | ---: |
| Zonal shared-core Cloud SQL, 10 GiB SSD, backups/PITR | $20–40 |
| Scale-to-zero Cloud Run service and bounded jobs | $0–8 |
| 20 GiB Standard GCS evidence plus versions | $1–4 |
| Five Cloud Scheduler jobs | $0–1 |
| Secret Manager, Logging, and Monitoring at low traffic | $0–6 |
| Contingency | $10 |
| **Total** | **$31–69** |

Assumptions: 730 hours/month for Cloud SQL, low request volume, 20 GiB retained evidence, modest logs, no sustained CPU, and no cross-region transfer. External RPC, LLM, and internet-egress usage are excluded exactly as required by the product issue. Move beyond this tier only after the measured promotion gates in `docs/ARCHITECTURE.md` are met and the owner approves a new architecture issue.
