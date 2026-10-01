import os
import subprocess
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0007_buyer_classification.up.sql"
DOWN = ROOT / "migrations" / "0007_buyer_classification.down.sql"
PROMOTION_UP = ROOT / "migrations" / "0010_classification_promotions.up.sql"
PROMOTION_DOWN = ROOT / "migrations" / "0010_classification_promotions.down.sql"
PREREQUISITES = sorted((ROOT / "migrations").glob("000[1-6]_*.up.sql"))


class BuyerClassificationMigrationContractTest(unittest.TestCase):
    def test_forward_migration_persists_replayable_versioned_classifications(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        for table in (
            "classification_label_definitions",
            "classification_runs",
            "classification_run_label_definitions",
            "classification_run_features",
            "classification_run_evidence",
            "classification_run_claims",
            "classification_run_seals",
            "classification_replay_drift",
        ):
            self.assertIn(f"CREATE TABLE agent_economy.{table}", up)
            self.assertIn(f"CREATE TRIGGER {table}_immutable", up)
        self.assertIn("label_kind text NOT NULL CHECK (label_kind IN ('core', 'extension'))", up)
        self.assertIn("classifier_version text NOT NULL", up)
        self.assertIn("feature_version text NOT NULL", up)
        self.assertIn("window_start timestamptz NOT NULL", up)
        self.assertIn("window_end timestamptz NOT NULL", up)
        self.assertIn("status IN ('verified', 'inferred', 'disputed')", up)
        self.assertIn("CONSTRAINT classification_runs_target_check CHECK", up)
        self.assertIn("cluster_version integer", up)
        self.assertIn(
            "FOREIGN KEY (namespace_id, cluster_id, cluster_version)", up
        )
        self.assertIn("validate_classification_run_seal", up)
        self.assertIn("classification run is sealed", up)
        self.assertIn("total_spend_atomic", up)
        self.assertIn("median_cadence_seconds", up)
        self.assertIn("x402_count", up)
        self.assertIn("unique_counterparties", up)
        self.assertIn("autonomy_observed_count", up)
        self.assertIn("classification drift subjects or windows do not match", up)
        self.assertIn(
            "baseline.cluster_version IS DISTINCT FROM replay.cluster_version", up
        )
        self.assertIn("label_churn_count", up)
        self.assertIn("pg_catalog.sha256(NEW.result_encoding)", up)
        self.assertIn("NEW.content_hash :=", up)
        self.assertIn("classification run must bind its complete label set", up)
        self.assertIn("classification feature values are internally inconsistent", up)
        self.assertTrue(up.rstrip().endswith("COMMIT;"))

    def test_down_migration_fails_closed_on_classification_history(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("cannot roll back buyer classification history", down)
        self.assertLess(down.index("LOCK TABLE"), down.index("IF EXISTS"))
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP TABLE"))
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))

    def test_promotion_migration_adds_append_only_explicit_current_authority(self):
        up = PROMOTION_UP.read_text(encoding="utf-8")
        down = PROMOTION_DOWN.read_text(encoding="utf-8")

        self.assertIn("CREATE TABLE agent_economy.classification_run_promotions", up)
        self.assertIn("promotion_sequence bigint NOT NULL", up)
        self.assertIn("promotion_method text NOT NULL", up)
        self.assertIn("provenance_id uuid NOT NULL", up)
        self.assertIn("validate_classification_run_promotion", up)
        self.assertIn("classification promotion must target a sealed run for the same buyer", up)
        self.assertIn("CREATE FUNCTION agent_economy.promote_buyer_classification_run", up)
        self.assertIn("migration-0010-earliest-sealed-series", up)
        self.assertIn("INSERT INTO agent_economy.classification_run_promotions", up)
        self.assertIn("CREATE VIEW agent_economy.current_buyer_classification_runs", up)
        self.assertIn("ORDER BY promotion.namespace_id, promotion.buyer_handle_id, promotion.promotion_sequence DESC", up)
        self.assertIn("CREATE TRIGGER classification_run_promotions_immutable", up)
        self.assertTrue(up.rstrip().endswith("COMMIT;"))

        self.assertIn("cannot roll back classification promotion history", down)
        self.assertIn("DROP FUNCTION agent_economy.promote_buyer_classification_run", down)
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


@unittest.skipUnless(
    os.environ.get("RUN_BUYER_CLASSIFICATION_LIVE") == "1",
    "set RUN_BUYER_CLASSIFICATION_LIVE=1 for PostgreSQL 17.6 migration qualification",
)
class BuyerClassificationMigrationLiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = f"aem-classification-{uuid.uuid4().hex[:12]}"
        subprocess.run(
            [
                "docker",
                "run",
                "--detach",
                "--rm",
                "--name",
                cls.container,
                "--tmpfs",
                "/var/lib/postgresql/data",
                "--env",
                "POSTGRES_HOST_AUTH_METHOD=trust",
                "postgres:17.6-alpine",
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            ready = subprocess.run(
                ["docker", "exec", cls.container, "pg_isready", "-U", "postgres"],
                capture_output=True,
                text=True,
            )
            logs = subprocess.run(
                ["docker", "logs", cls.container], capture_output=True, text=True
            )
            if ready.returncode == 0 and (logs.stdout + logs.stderr).count(
                "database system is ready to accept connections"
            ) >= 2:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("disposable PostgreSQL did not become ready")

    @classmethod
    def tearDownClass(cls):
        subprocess.run(
            ["docker", "rm", "--force", cls.container],
            capture_output=True,
            text=True,
        )

    def psql(self, sql, *, check=True):
        result = subprocess.run(
            [
                "docker",
                "exec",
                "--interactive",
                self.container,
                "psql",
                "-U",
                "postgres",
                "-v",
                "ON_ERROR_STOP=1",
                "-Atq",
            ],
            input=sql,
            check=False,
            capture_output=True,
            text=True,
        )
        if check and result.returncode != 0:
            self.fail(result.stderr)
        return result

    def setUp(self):
        self.psql("DROP SCHEMA IF EXISTS agent_economy CASCADE;")

    def test_upgrade_backfills_the_earliest_sealed_series(self):
        for migration in [*PREREQUISITES, UP]:
            self.psql(migration.read_text(encoding="utf-8"))
        namespace = "00000000-0000-0000-0000-000000000001"
        provenance = "00000000-0000-0000-0000-000000000010"
        features = (
            "('total_spend_atomic', '600'),"
            "('payment_count', '1'),"
            "('median_cadence_seconds', 'null'),"
            "('x402_count', '1'),"
            "('mpp_count', '0'),"
            "('unique_counterparties', '1'),"
            "('autonomous_count', '0'),"
            "('autonomy_observed_count', '0')"
        )
        self.psql(
            f"""
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'classification-upgrade-test');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type,
                 byte_length, observed_at)
            VALUES ('{namespace}', 'evidence:baseline', repeat('a', 64),
                    'evidence://baseline', 'application/json', 2,
                    '2026-09-01T00:00:00Z');
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at, parser_version,
                 evidence_id)
            VALUES ('{namespace}', '{provenance}', 'source:test',
                    '2026-09-01T00:00:00Z', 'parser@1', 'evidence:baseline');
            INSERT INTO agent_economy.buyer_handles
                (namespace_id, buyer_handle_id, handle_kind, chain_scope,
                 handle_value, provenance_id)
            VALUES ('{namespace}', 'buyer:one', 'wallet', 'base', '0x01', '{provenance}');
            INSERT INTO agent_economy.classification_label_definitions
                (namespace_id, label_id, label_version, label_kind,
                 rule_definition, definition_hash)
            VALUES ('{namespace}', 'local:high-spend', 7, 'extension',
                    '{{"metric":"total_spend_atomic","at_least":"500"}}',
                    repeat('c', 64));
            INSERT INTO agent_economy.classification_claims
                (namespace_id, claim_id, version, buyer_handle_id, label, method,
                 confidence, evidence_window_start, evidence_window_end,
                 valid_from, status, provenance_id)
            VALUES ('{namespace}', 'claim:high-spend', 1, 'buyer:one',
                    'local:high-spend', 'buyer-classifier@1', 1.0000,
                    '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z',
                    '2026-09-02T00:00:00Z', 'inferred', '{provenance}');
            INSERT INTO agent_economy.classification_runs
                (namespace_id, run_id, run_version, buyer_handle_id,
                 classifier_version, feature_version, label_set_hash,
                 input_snapshot_hash, window_start, window_end, created_at)
            VALUES
                ('{namespace}', 'run:baseline', 1, 'buyer:one',
                 'buyer-classifier@1', 'behavior-features@1', repeat('d', 64),
                 repeat('e', 64), '2026-09-01T00:00:00Z',
                 '2026-09-02T00:00:00Z', '2026-09-02T01:00:00Z'),
                ('{namespace}', 'run:later-replay', 1, 'buyer:one',
                 'buyer-classifier@1', 'behavior-features@1', repeat('d', 64),
                 repeat('f', 64), '2026-09-01T00:00:00Z',
                 '2026-09-02T00:00:00Z', '2026-09-03T01:00:00Z');
            INSERT INTO agent_economy.classification_run_features
                (namespace_id, run_id, run_version, feature_name, feature_value)
            SELECT '{namespace}', run_id, 1, feature_name, value::jsonb
            FROM (VALUES ('run:baseline'), ('run:later-replay')) AS run(run_id)
            CROSS JOIN LATERAL (VALUES {features}) AS feature(feature_name, value);
            INSERT INTO agent_economy.classification_run_label_definitions
                (namespace_id, run_id, run_version, label_id, label_version)
            VALUES
                ('{namespace}', 'run:baseline', 1, 'local:high-spend', 7),
                ('{namespace}', 'run:later-replay', 1, 'local:high-spend', 7);
            INSERT INTO agent_economy.classification_run_evidence
                (namespace_id, run_id, run_version, evidence_id, evidence_role)
            VALUES
                ('{namespace}', 'run:baseline', 1,
                 'evidence:baseline', 'supporting'),
                ('{namespace}', 'run:later-replay', 1,
                 'evidence:baseline', 'supporting');
            INSERT INTO agent_economy.classification_run_claims
                (namespace_id, run_id, run_version, claim_id, claim_version,
                 label_id, label_version, status)
            VALUES ('{namespace}', 'run:baseline', 1, 'claim:high-spend', 1,
                    'local:high-spend', 7, 'inferred');
            INSERT INTO agent_economy.classification_run_seals
                (namespace_id, run_id, run_version, result_encoding, state_hash)
            VALUES
                ('{namespace}', 'run:baseline', 1,
                 convert_to('baseline-result', 'UTF8'),
                 encode(sha256(convert_to('baseline-result', 'UTF8')), 'hex')),
                ('{namespace}', 'run:later-replay', 1,
                 convert_to('replay-result', 'UTF8'),
                 encode(sha256(convert_to('replay-result', 'UTF8')), 'hex'));
            """
        )

        self.psql(PROMOTION_UP.read_text(encoding="utf-8"))
        current = self.psql(
            "SELECT run_id || ':' || run_version || ':' || promotion_method "
            "FROM agent_economy.current_buyer_classification_runs;"
        ).stdout.strip()
        self.assertEqual(
            "run:baseline:1:migration-0010-earliest-sealed-series", current
        )

    def test_sealed_replays_measure_exact_label_drift(self):
        for migration in [*PREREQUISITES, UP, PROMOTION_UP]:
            self.psql(migration.read_text(encoding="utf-8"))
        namespace = "00000000-0000-0000-0000-000000000001"
        provenance = "00000000-0000-0000-0000-000000000010"
        features = (
            "('total_spend_atomic', '600'),"
            "('payment_count', '1'),"
            "('median_cadence_seconds', 'null'),"
            "('x402_count', '1'),"
            "('mpp_count', '0'),"
            "('unique_counterparties', '1'),"
            "('autonomous_count', '0'),"
            "('autonomy_observed_count', '0')"
        )
        replay_features = features.replace("'600'", "'400'", 1)
        self.psql(
            f"""
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'classification-test');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type,
                 byte_length, observed_at)
            VALUES
                ('{namespace}', 'evidence:baseline', repeat('a', 64),
                 'evidence://baseline', 'application/json', 2, '2026-09-01T00:00:00Z'),
                ('{namespace}', 'evidence:replay', repeat('b', 64),
                 'evidence://replay', 'application/json', 2, '2026-09-01T00:00:00Z');
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at, parser_version,
                 evidence_id)
            VALUES ('{namespace}', '{provenance}', 'source:test',
                    '2026-09-01T00:00:00Z', 'parser@1', 'evidence:baseline');
            INSERT INTO agent_economy.buyer_handles
                (namespace_id, buyer_handle_id, handle_kind, chain_scope,
                 handle_value, provenance_id)
            VALUES ('{namespace}', 'buyer:one', 'wallet', 'base', '0x01', '{provenance}');
            INSERT INTO agent_economy.classification_label_definitions
                (namespace_id, label_id, label_version, label_kind,
                 rule_definition, definition_hash)
            VALUES ('{namespace}', 'local:high-spend', 7, 'extension',
                    '{{"metric":"total_spend_atomic","at_least":"500"}}', repeat('c', 64));
            INSERT INTO agent_economy.classification_claims
                (namespace_id, claim_id, version, buyer_handle_id, label, method,
                 confidence, evidence_window_start, evidence_window_end,
                 valid_from, status, provenance_id)
            VALUES ('{namespace}', 'claim:high-spend', 1, 'buyer:one',
                    'local:high-spend', 'buyer-classifier@1', 1.0000,
                    '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z',
                    '2026-09-02T00:00:00Z', 'inferred', '{provenance}');
            INSERT INTO agent_economy.classification_runs
                (namespace_id, run_id, run_version, buyer_handle_id,
                 classifier_version, feature_version, label_set_hash,
                 input_snapshot_hash, window_start, window_end)
            VALUES
                ('{namespace}', 'run:baseline', 1, 'buyer:one',
                 'buyer-classifier@1', 'behavior-features@1', repeat('d', 64),
                 repeat('e', 64), '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z'),
                ('{namespace}', 'run:replay', 1, 'buyer:one',
                 'buyer-classifier@1', 'behavior-features@1', repeat('d', 64),
                 repeat('f', 64), '2026-09-01T00:00:00Z', '2026-09-02T00:00:00Z');
            INSERT INTO agent_economy.classification_run_features
                (namespace_id, run_id, run_version, feature_name, feature_value)
            SELECT '{namespace}', 'run:baseline', 1, feature_name, value::jsonb
            FROM (VALUES {features}) AS feature(feature_name, value);
            INSERT INTO agent_economy.classification_run_features
                (namespace_id, run_id, run_version, feature_name, feature_value)
            SELECT '{namespace}', 'run:replay', 1, feature_name, value::jsonb
            FROM (VALUES {replay_features}) AS feature(feature_name, value);
            INSERT INTO agent_economy.classification_run_label_definitions
                (namespace_id, run_id, run_version, label_id, label_version)
            VALUES
                ('{namespace}', 'run:baseline', 1, 'local:high-spend', 7),
                ('{namespace}', 'run:replay', 1, 'local:high-spend', 7);
            INSERT INTO agent_economy.classification_run_evidence
                (namespace_id, run_id, run_version, evidence_id, evidence_role)
            VALUES
                ('{namespace}', 'run:baseline', 1, 'evidence:baseline', 'supporting'),
                ('{namespace}', 'run:replay', 1, 'evidence:replay', 'supporting');
            INSERT INTO agent_economy.classification_run_claims
                (namespace_id, run_id, run_version, claim_id, claim_version,
                 label_id, label_version, status)
            VALUES ('{namespace}', 'run:baseline', 1, 'claim:high-spend', 1,
                    'local:high-spend', 7, 'inferred');
            """
        )
        timezone_hashes = []
        for timezone in ("UTC", "America/New_York"):
            timezone_hashes.append(
                self.psql(
                    f"""
                    SET TIME ZONE '{timezone}';
                    BEGIN;
                    INSERT INTO agent_economy.classification_run_seals
                        (namespace_id, run_id, run_version, result_encoding, state_hash)
                    VALUES ('{namespace}', 'run:baseline', 1,
                            convert_to('same-result', 'UTF8'),
                            encode(sha256(convert_to('same-result', 'UTF8')), 'hex'));
                    SELECT content_hash
                    FROM agent_economy.classification_run_seals
                    WHERE namespace_id = '{namespace}'
                      AND run_id = 'run:baseline'
                      AND run_version = 1;
                    ROLLBACK;
                    """
                ).stdout.strip()
            )
        self.assertEqual(timezone_hashes[0], timezone_hashes[1])
        self.psql(
            f"""
            INSERT INTO agent_economy.classification_run_seals
                (namespace_id, run_id, run_version, result_encoding, state_hash)
            VALUES
                ('{namespace}', 'run:baseline', 1, convert_to('same-result', 'UTF8'),
                 encode(sha256(convert_to('same-result', 'UTF8')), 'hex')),
                ('{namespace}', 'run:replay', 1, convert_to('same-result', 'UTF8'),
                 encode(sha256(convert_to('same-result', 'UTF8')), 'hex'));
            INSERT INTO agent_economy.classification_replay_drift
                (namespace_id, baseline_run_id, baseline_run_version,
                 replay_run_id, replay_run_version, baseline_content_hash,
                 replay_content_hash, input_changed, added_labels,
                 removed_labels, label_churn_count)
            SELECT '{namespace}', 'run:baseline', 1, 'run:replay', 1,
                   baseline.content_hash, replay.content_hash, true, '{{}}',
                   '{{local:high-spend@7}}', 1
            FROM agent_economy.classification_run_seals AS baseline
            CROSS JOIN agent_economy.classification_run_seals AS replay
            WHERE baseline.run_id = 'run:baseline' AND replay.run_id = 'run:replay';
            """
        )
        measured = self.psql(
            "SELECT input_changed || ':' || label_churn_count || ':' || removed_labels[1] "
            "FROM agent_economy.classification_replay_drift;"
        ).stdout.strip()
        self.assertEqual("true:1:local:high-spend@7", measured)
        bindings = self.psql(
            "SELECT count(DISTINCT state_hash) || ':' || count(DISTINCT content_hash) "
            "FROM agent_economy.classification_run_seals;"
        ).stdout.strip()
        self.assertEqual("1:2", bindings)
        self.psql(
            "SELECT agent_economy.promote_buyer_classification_run("
            f"'{namespace}', 'buyer:one', 'run:baseline', 1, "
            f"'operator-review', '{provenance}');"
        )
        current_before_explicit_replay_promotion = self.psql(
            "SELECT run_id FROM agent_economy.current_buyer_classification_runs;"
        ).stdout.strip()
        self.assertEqual("run:baseline", current_before_explicit_replay_promotion)
        self.psql(
            "SELECT agent_economy.promote_buyer_classification_run("
            f"'{namespace}', 'buyer:one', 'run:replay', 1, "
            f"'operator-review', '{provenance}');"
        )
        current_after_explicit_replay_promotion = self.psql(
            "SELECT run_id FROM agent_economy.current_buyer_classification_runs;"
        ).stdout.strip()
        self.assertEqual("run:replay", current_after_explicit_replay_promotion)
        late_feature = self.psql(
            f"""
            INSERT INTO agent_economy.classification_run_features
                (namespace_id, run_id, run_version, feature_name, feature_value)
            VALUES ('{namespace}', 'run:baseline', 1, 'mpp_count', '1');
            """,
            check=False,
        )
        self.assertNotEqual(0, late_feature.returncode)
        self.assertIn("classification run is sealed", late_feature.stderr)
        bad_drift = self.psql(
            f"""
            INSERT INTO agent_economy.classification_replay_drift
                (namespace_id, baseline_run_id, baseline_run_version,
                 replay_run_id, replay_run_version, baseline_content_hash,
                 replay_content_hash, input_changed, added_labels,
                 removed_labels, label_churn_count)
            SELECT '{namespace}', 'run:replay', 1, 'run:baseline', 1,
                   replay.content_hash, baseline.content_hash, true, '{{}}', '{{}}', 0
            FROM agent_economy.classification_run_seals AS baseline
            CROSS JOIN agent_economy.classification_run_seals AS replay
            WHERE baseline.run_id = 'run:baseline' AND replay.run_id = 'run:replay';
            """,
            check=False,
        )
        self.assertNotEqual(0, bad_drift.returncode)
        self.assertIn("classification drift does not match", bad_drift.stderr)
        self.psql(
            f"""
            INSERT INTO agent_economy.buyer_clusters
                (namespace_id, cluster_id, display_name, provenance_id)
            VALUES ('{namespace}', '00000000-0000-0000-0000-000000000020',
                    'cluster one', '{provenance}');
            INSERT INTO agent_economy.buyer_cluster_versions
                (namespace_id, cluster_id, version, status, method, confidence,
                 valid_from, provenance_id)
            VALUES ('{namespace}', '00000000-0000-0000-0000-000000000020', 1,
                    'accepted', 'test', 1.0000, '2026-09-01T00:00:00Z',
                    '{provenance}');
            """
        )
        unversioned_cluster_run = self.psql(
            f"""
            INSERT INTO agent_economy.classification_runs
                (namespace_id, run_id, run_version, cluster_id, cluster_version,
                 classifier_version, feature_version, label_set_hash,
                 input_snapshot_hash, window_start, window_end)
            VALUES ('{namespace}', 'run:unversioned-cluster', 1,
                    '00000000-0000-0000-0000-000000000020', NULL,
                    'buyer-classifier@1', 'behavior-features@1', repeat('d', 64),
                    repeat('e', 64), '2026-09-01T00:00:00Z',
                    '2026-09-02T00:00:00Z');
            """,
            check=False,
        )
        self.assertNotEqual(0, unversioned_cluster_run.returncode)
        self.assertIn("classification_runs_target_check", unversioned_cluster_run.stderr)
        self.psql(
            f"""
            INSERT INTO agent_economy.classification_runs
                (namespace_id, run_id, run_version, cluster_id, cluster_version,
                 classifier_version, feature_version, label_set_hash,
                 input_snapshot_hash, window_start, window_end)
            VALUES ('{namespace}', 'run:versioned-cluster', 1,
                    '00000000-0000-0000-0000-000000000020', 1,
                    'buyer-classifier@1', 'behavior-features@1', repeat('d', 64),
                    repeat('e', 64), '2026-09-01T00:00:00Z',
                    '2026-09-02T00:00:00Z');
            """
        )
        rollback = self.psql(DOWN.read_text(encoding="utf-8"), check=False)
        self.assertNotEqual(0, rollback.returncode)
        self.assertIn("cannot roll back buyer classification history", rollback.stderr)


if __name__ == "__main__":
    unittest.main()
