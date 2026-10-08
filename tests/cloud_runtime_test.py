import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
DEPLOY = ROOT / "deploy" / "google-cloud"


class LeanGoogleCloudRuntimeTest(unittest.TestCase):
    def test_docker_build_context_excludes_secret_and_state_files(self) -> None:
        dockerignore = (ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()

        for pattern in (
            ".env",
            ".env.*",
            ".dev-stack.env",
            "**/.terraform",
            "*.tfstate",
            "*.tfstate.*",
            "*.tfvars",
            "*.pem",
            "*.key",
        ):
            self.assertIn(pattern, dockerignore)

    def test_runtime_is_one_scale_to_zero_image_with_bounded_jobs(self) -> None:
        runtime = (DEPLOY / "runtime.tf").read_text(encoding="utf-8")
        dockerfile = (ROOT / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn('resource "google_cloud_run_v2_service" "cockpit"', runtime)
        self.assertIn("min_instance_count = 0", runtime)
        self.assertIn("max_instance_count = 2", runtime)
        self.assertIn('resource "google_cloud_run_v2_job" "worker"', runtime)
        self.assertIn(
            'for_each = toset(["collect", "verify-evidence", "reduce", "classify", "enrich"])',
            runtime,
        )
        self.assertIn("args    = [each.key]", runtime)
        self.assertGreaterEqual(runtime.count("var.image"), 2)
        self.assertNotIn("google_pubsub", runtime)
        self.assertNotIn("bigquery", runtime.lower())
        self.assertIn("FROM gcr.io/distroless/cc-debian12:nonroot", dockerfile)
        self.assertIn('ENTRYPOINT ["/agent-economy-monitor"]', dockerfile)

    def test_worker_jobs_receive_only_their_mode_authority(self) -> None:
        runtime = (DEPLOY / "runtime.tf").read_text(encoding="utf-8")
        variables = (DEPLOY / "variables.tf").read_text(encoding="utf-8")
        storage = (DEPLOY / "storage.tf").read_text(encoding="utf-8")
        worker = runtime.split(
            'resource "google_cloud_run_v2_job" "worker"', maxsplit=1
        )[1]

        self.assertIn(
            "service_account = google_service_account.worker[each.key].email", worker
        )
        self.assertIn("for_each = local.worker_secret_ids[each.key]", worker)
        self.assertNotIn('name  = "NAMESPACE_ID"', worker)
        self.assertIn("local.worker_plain_environment[each.key]", worker)

        for mode, names in {
            "collect": (
                "COLLECTOR_DATABASE_URL",
                "ALCHEMY_ETHEREUM_RPC_URL",
                "ALCHEMY_BASE_RPC_URL",
                "ALCHEMY_SOLANA_RPC_URL",
                "ALCHEMY_TEMPO_RPC_URL",
            ),
            "reduce": ("REDUCER_DATABASE_URL",),
            "verify-evidence": ("EVIDENCE_VERIFIER_DATABASE_URL",),
            "classify": ("CLASSIFIER_DATABASE_URL",),
            "enrich": (
                "ENRICHER_DATABASE_URL",
                "ALCHEMY_ETHEREUM_RPC_URL",
                "ALCHEMY_BASE_RPC_URL",
                "ALCHEMY_SOLANA_RPC_URL",
                "ALCHEMY_TEMPO_RPC_URL",
            ),
        }.items():
            self.assertIn(f"{mode} = toset([", variables)
            for name in names:
                self.assertIn(f'"{name}"', variables)

        self.assertIn('COLLECTION_INPUT_ROOT = "/collection-inputs"', runtime)
        self.assertIn("EVIDENCE_WRITE_BUCKET", runtime)
        self.assertIn('resource "google_storage_bucket" "collection_inputs"', storage)
        self.assertIn("roles/storage.objectViewer", storage)
        self.assertIn('"verify-evidence"', runtime)
        self.assertIn("EVIDENCE_READ_BUCKET", runtime)
        self.assertIn('resource "google_project_iam_custom_role" "collector_evidence_writer"', storage)
        self.assertIn('"storage.objects.create"', storage)
        self.assertIn('"storage.objects.list"', storage)
        self.assertIn(
            "role   = google_project_iam_custom_role.collector_evidence_writer.name",
            storage,
        )
        evidence_creator = storage.split(
            'resource "google_storage_bucket_iam_member" "runtime_evidence_creator"',
            maxsplit=1,
        )[1].split("resource ", maxsplit=1)[0]
        self.assertIn(
            "role   = google_project_iam_custom_role.collector_evidence_writer.name",
            evidence_creator,
        )
        evidence_reader = storage.split(
            'resource "google_storage_bucket_iam_member" "runtime_evidence_reader"',
            maxsplit=1,
        )[1].split("resource ", maxsplit=1)[0]
        self.assertNotIn('"collect"', evidence_reader)

    def test_durable_stores_enforce_backup_and_immutability_controls(self) -> None:
        storage = (DEPLOY / "storage.tf").read_text(encoding="utf-8")

        for fragment in (
            'resource "google_sql_database_instance" "postgres"',
            "backup_configuration {",
            "enabled                        = true",
            "point_in_time_recovery_enabled = true",
            "transaction_log_retention_days = 7",
            'database_version = "POSTGRES_17"',
            'resource "google_storage_bucket" "evidence"',
            "uniform_bucket_level_access = true",
            'public_access_prevention    = "enforced"',
            "versioning {",
            "enabled = true",
            "retention_policy {",
            "retention_period = 31536000",
            "is_locked        = true",
        ):
            self.assertIn(fragment, storage)
        self.assertIn("agentEconomyEvidenceWriter", storage)
        self.assertIn('"storage.objects.create"', storage)
        self.assertIn('"storage.objects.list"', storage)
        self.assertIn("roles/storage.objectViewer", storage)
        self.assertNotIn("roles/storage.objectAdmin", storage)

    def test_scheduler_observability_and_budget_are_explicit(self) -> None:
        operations = (DEPLOY / "operations.tf").read_text(encoding="utf-8")
        variables = (DEPLOY / "variables.tf").read_text(encoding="utf-8")
        main = (DEPLOY / "main.tf").read_text(encoding="utf-8")

        self.assertIn('resource "google_cloud_scheduler_job" "worker"', operations)
        self.assertIn('resource "google_logging_metric" "runtime_errors"', operations)
        self.assertIn('resource "google_monitoring_alert_policy" "runtime_errors"', operations)
        self.assertIn('resource "google_billing_budget" "monthly"', operations)
        self.assertIn("default     = 75", variables)
        self.assertIn("roles/run.invoker", operations)
        self.assertNotIn("roles/run.developer", operations)
        self.assertIn("billingbudgets.googleapis.com", main)
        self.assertIn('"iam.googleapis.com"', main)
        self.assertNotIn('default     = ""', variables)
        self.assertIn("length(var.notification_channels) > 0", variables)

    def test_recovery_drill_downloads_retained_evidence_and_runs_the_same_image(self) -> None:
        drill = (ROOT / "scripts" / "recovery-drill").read_text(encoding="utf-8")

        for fragment in (
            "gcloud storage buckets describe",
            "gcloud storage cp",
            "manifest_generation",
            '#$manifest_generation',
            'entry["generation"]',
            'materialized_entry["payload_base64"] = base64.b64encode(destination.read_bytes()).decode("ascii")',
            '"manifest_object": manifest_object',
            '"manifest_generation": int(manifest_generation)',
            '"entries": materialized',
            'result.get("manifest_object") != manifest_object',
            'result.get("manifest_generation") != int(manifest_generation)',
            '"buyer_handle_id"',
            '"observation_id"',
            "def reject_duplicate_members(pairs):",
            "object_pairs_hook=reject_duplicate_members",
            "subprocess.run",
            '"retentionPolicy", {}).get("isLocked")',
            '"versioning", {}).get("enabled")',
            'recovery-drill "$buyer_handle_id" "$manifest"',
            'trap cleanup EXIT',
        ):
            self.assertIn(fragment, drill)
        self.assertNotIn("set -x", drill)
        self.assertNotIn("destination.read_text", drill)


if __name__ == "__main__":
    unittest.main()
