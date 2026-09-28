import os
import subprocess
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
KNOWLEDGE_UP = ROOT / "migrations" / "0001_knowledge_graph.up.sql"
OPERATIONAL_UP = ROOT / "migrations" / "0002_operational_analytics.up.sql"
UP = ROOT / "migrations" / "0003_shadow_catalog.up.sql"
DOWN = ROOT / "migrations" / "0003_shadow_catalog.down.sql"
EXPECTED_TABLES = {
    "catalog_candidates",
    "catalog_candidate_aliases",
    "catalog_verification_signals",
    "catalog_liveness_checks",
    "catalog_promotions",
}


class ShadowCatalogMigrationContractTest(unittest.TestCase):
    def test_migration_separates_shadow_candidates_from_verified_catalog(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        self.assertTrue(up.rstrip().endswith("COMMIT;"))
        self.assertEqual(
            EXPECTED_TABLES,
            {
                line.split("agent_economy.", 1)[1].split()[0]
                for line in up.splitlines()
                if line.startswith("CREATE TABLE agent_economy.")
            },
        )
        for fragment in (
            "candidate_id text NOT NULL CHECK (candidate_id ~ '^candidate:sha256:[0-9a-f]{64}$')",
            "alias_kind text NOT NULL CHECK (alias_kind IN ('x402scan', 'agentcash', 'discovery'))",
            "alias_source_id text NOT NULL CHECK (alias_source_id <> '')",
            "UNIQUE (namespace_id, alias_source_id, upstream_id)",
            "verification_kind text NOT NULL CHECK (verification_kind IN ('metadata', 'runtime', 'settlement'))",
            "provenance_records_observed_at_key",
            "FOREIGN KEY (namespace_id, provenance_id, observed_at)",
            "FOREIGN KEY (namespace_id, provenance_id, checked_at)",
            "FOREIGN KEY (namespace_id, provenance_id, promoted_at)",
            "CREATE TRIGGER catalog_candidate_aliases_source_policy",
            "CREATE TRIGGER catalog_promotions_two_signal_policy",
            "CREATE VIEW agent_economy.shadow_catalog",
            "CREATE VIEW agent_economy.verified_catalog",
        ):
            self.assertIn(fragment, up)
        for table in EXPECTED_TABLES:
            self.assertIn(f"CREATE TRIGGER {table}_immutable", up)
            self.assertIn(f"CREATE TRIGGER {table}_truncate_immutable", up)

    def test_down_migration_fails_closed_before_dropping_history(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("cannot roll back non-empty shadow catalog", down)
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP VIEW"))
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


@unittest.skipUnless(
    os.environ.get("RUN_SHADOW_CATALOG_LIVE") == "1",
    "set RUN_SHADOW_CATALOG_LIVE=1 for PostgreSQL 17.6 migration qualification",
)
class ShadowCatalogMigrationLiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = f"aem-shadow-catalog-{uuid.uuid4().hex[:12]}"
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
                ["docker", "logs", cls.container],
                capture_output=True,
                text=True,
            )
            if ready.returncode == 0 and (logs.stdout + logs.stderr).count(
                "database system is ready to accept connections"
            ) >= 2:
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("disposable PostgreSQL did not become ready")

        cls.psql(KNOWLEDGE_UP.read_text(encoding="utf-8"))
        cls.psql(OPERATIONAL_UP.read_text(encoding="utf-8"))
        cls.psql(UP.read_text(encoding="utf-8"))

    @classmethod
    def tearDownClass(cls):
        subprocess.run(
            ["docker", "rm", "--force", cls.container],
            capture_output=True,
            text=True,
        )

    @classmethod
    def psql(cls, sql, *, check=True):
        result = subprocess.run(
            [
                "docker",
                "exec",
                "--interactive",
                cls.container,
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
            raise AssertionError(result.stderr)
        return result

    def test_dead_endpoint_decays_without_deleting_liveness_history(self):
        namespace = "00000000-0000-0000-0000-000000000002"
        provenance = "00000000-0000-0000-0000-000000000020"
        check_provenance = [
            "00000000-0000-0000-0000-000000000021",
            "00000000-0000-0000-0000-000000000022",
            "00000000-0000-0000-0000-000000000023",
        ]
        candidate = "candidate:sha256:" + "d" * 64
        self.psql(
            f"""
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'decay');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type,
                 byte_length, observed_at)
            VALUES
                ('{namespace}', 'evidence:decay', repeat('d', 64),
                 'evidence://decay/source', 'application/json', 2,
                 '2026-09-01T00:00:00Z'),
                ('{namespace}', 'evidence:check-1', repeat('1', 64),
                 'evidence://decay/check-1', 'application/json', 2,
                 '2026-09-01T00:01:00Z'),
                ('{namespace}', 'evidence:check-2', repeat('2', 64),
                 'evidence://decay/check-2', 'application/json', 2,
                 '2026-09-01T00:02:00Z'),
                ('{namespace}', 'evidence:check-3', repeat('3', 64),
                 'evidence://decay/check-3', 'application/json', 2,
                 '2026-09-01T00:03:00Z');
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at,
                 parser_version, evidence_id)
            VALUES
                ('{namespace}', '{provenance}', 'runtime-probe',
                 '2026-09-01T00:00:00Z', 'probe@1', 'evidence:decay'),
                ('{namespace}', '{check_provenance[0]}', 'runtime-probe',
                 '2026-09-01T00:01:00Z', 'probe@1', 'evidence:check-1'),
                ('{namespace}', '{check_provenance[1]}', 'runtime-probe',
                 '2026-09-01T00:02:00Z', 'probe@1', 'evidence:check-2'),
                ('{namespace}', '{check_provenance[2]}', 'runtime-probe',
                 '2026-09-01T00:03:00Z', 'probe@1', 'evidence:check-3');
            INSERT INTO agent_economy.catalog_candidates
                (namespace_id, candidate_id, protocol, endpoint_uri, http_method,
                 display_name, discovered_at, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'x402', 'https://dead.example/pay',
                 'GET', 'Dead example', '2026-09-01T00:00:00Z', '{provenance}');
            INSERT INTO agent_economy.catalog_liveness_checks
                (namespace_id, candidate_id, check_id, checked_at, outcome,
                 provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'check:sha256:{'1' * 64}',
                 '2026-09-01T00:01:00Z', 'dead', '{check_provenance[0]}'),
                ('{namespace}', '{candidate}', 'check:sha256:{'2' * 64}',
                 '2026-09-01T00:02:00Z', 'dead', '{check_provenance[1]}'),
                ('{namespace}', '{candidate}', 'check:sha256:{'3' * 64}',
                 '2026-09-01T00:03:00Z', 'dead', '{check_provenance[2]}');
            """
        )

        state = self.psql(
            f"""
            SELECT health_status || ':' || liveness_check_count
            FROM agent_economy.shadow_catalog
            WHERE namespace_id = '{namespace}' AND candidate_id = '{candidate}';
            SELECT agent_economy.catalog_candidate_health(
                '{namespace}', '{candidate}', '2026-09-01T00:02:30Z');
            SELECT agent_economy.catalog_candidate_health(
                '{namespace}', '{candidate}', '2026-09-01T00:03:30Z');
            SELECT count(*) FROM agent_economy.catalog_liveness_checks
            WHERE namespace_id = '{namespace}' AND candidate_id = '{candidate}';
            """
        ).stdout.splitlines()
        self.assertEqual(["dead:3", "stale", "dead", "3"], state)

        delete_history = self.psql(
            f"""
            DELETE FROM agent_economy.catalog_liveness_checks
            WHERE namespace_id = '{namespace}' AND candidate_id = '{candidate}';
            """,
            check=False,
        )
        self.assertNotEqual(0, delete_history.returncode)
        self.assertIn("canonical evidence and knowledge rows are immutable", delete_history.stderr)

    def test_two_independent_signal_kinds_are_required_for_promotion(self):
        namespace = "00000000-0000-0000-0000-000000000001"
        first_provenance = "00000000-0000-0000-0000-000000000010"
        second_provenance = "00000000-0000-0000-0000-000000000011"
        agentcash_provenance = "00000000-0000-0000-0000-000000000012"
        promotion_provenance = "00000000-0000-0000-0000-000000000013"
        candidate = "candidate:sha256:" + "c" * 64
        self.psql(
            f"""
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'alpha');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type,
                 byte_length, observed_at)
            VALUES
                ('{namespace}', 'evidence:first', repeat('a', 64),
                 'evidence://alpha/first', 'application/json', 2,
                 '2026-09-01T00:00:00Z'),
                ('{namespace}', 'evidence:second', repeat('b', 64),
                 'evidence://alpha/second', 'application/json', 2,
                 '2026-09-01T00:01:00Z'),
                ('{namespace}', 'evidence:agentcash', repeat('e', 64),
                 'evidence://alpha/agentcash', 'application/json', 2,
                 '2026-09-01T00:00:30Z'),
                ('{namespace}', 'evidence:promotion', repeat('f', 64),
                 'evidence://alpha/promotion', 'application/json', 2,
                 '2026-09-01T00:02:00Z');
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at,
                 parser_version, evidence_id)
            VALUES
                ('{namespace}', '{first_provenance}', 'x402scan',
                 '2026-09-01T00:00:00Z', 'seed@1', 'evidence:first'),
                ('{namespace}', '{second_provenance}', 'runtime-probe',
                 '2026-09-01T00:01:00Z', 'x402-adapter@1', 'evidence:second'),
                ('{namespace}', '{agentcash_provenance}', 'agentcash',
                 '2026-09-01T00:00:30Z', 'seed@1', 'evidence:agentcash'),
                ('{namespace}', '{promotion_provenance}', 'catalog-reducer',
                 '2026-09-01T00:02:00Z', 'catalog@1', 'evidence:promotion');
            INSERT INTO agent_economy.catalog_candidates
                (namespace_id, candidate_id, protocol, endpoint_uri, http_method,
                 display_name, discovered_at, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'x402', 'https://api.example/pay',
                 'GET', 'Example', '2026-09-01T00:00:00Z', '{first_provenance}');
            INSERT INTO agent_economy.catalog_candidate_aliases
                (namespace_id, candidate_id, alias_kind, alias_source_id,
                 upstream_id, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'x402scan', 'x402scan', 'upstream-17',
                 '{first_provenance}');
            INSERT INTO agent_economy.services
                (namespace_id, service_id, display_name, trust_state, provenance_id)
            VALUES
                ('{namespace}', 'service:example', 'Example', 'observed',
                 '{second_provenance}');
            INSERT INTO agent_economy.endpoints
                (namespace_id, endpoint_id, service_id, http_method, endpoint_uri,
                 provenance_id)
            VALUES
                ('{namespace}', 'endpoint:example', 'service:example', 'GET',
                 'https://api.example/pay', '{second_provenance}');
            INSERT INTO agent_economy.catalog_verification_signals
                (namespace_id, candidate_id, signal_id, verification_kind,
                 verifier_scope, observed_at, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'signal:sha256:{'1' * 64}',
                 'metadata', 'well-known', '2026-09-01T00:00:00Z',
                 '{first_provenance}');
            """
        )

        direct_verified = self.psql(
            f"""
            INSERT INTO agent_economy.services
                (namespace_id, service_id, display_name, trust_state, provenance_id)
            VALUES
                ('{namespace}', 'service:bypass', 'Bypass', 'verified',
                 '{second_provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, direct_verified.returncode)
        self.assertIn("verified catalog state requires promotion", direct_verified.stderr)

        forged_alias = self.psql(
            f"""
            INSERT INTO agent_economy.catalog_candidate_aliases
                (namespace_id, candidate_id, alias_kind, alias_source_id,
                 upstream_id, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'agentcash', 'agentcash', 'listing-44',
                 '{second_provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, forged_alias.returncode)
        self.assertIn("alias source does not match provenance", forged_alias.stderr)
        self.psql(
            f"""
            INSERT INTO agent_economy.catalog_candidate_aliases
                (namespace_id, candidate_id, alias_kind, alias_source_id,
                 upstream_id, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'agentcash', 'agentcash', 'listing-44',
                 '{agentcash_provenance}');
            """
        )

        one_signal = self.psql(
            f"""
            INSERT INTO agent_economy.catalog_promotions
                (namespace_id, candidate_id, service_id, endpoint_id,
                 policy_version, promoted_at, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'service:example',
                 'endpoint:example', 'two-signal-v1', '2026-09-01T00:02:00Z',
                 '{promotion_provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, one_signal.returncode)
        self.assertIn("two independent verification signal kinds", one_signal.stderr)

        self.psql(
            f"""
            INSERT INTO agent_economy.catalog_verification_signals
                (namespace_id, candidate_id, signal_id, verification_kind,
                 verifier_scope, observed_at, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'signal:sha256:{'2' * 64}',
                 'runtime', 'https://api.example/pay', '2026-09-01T00:00:00Z',
                 '{first_provenance}');
            """
        )
        one_source = self.psql(
            f"""
            INSERT INTO agent_economy.catalog_promotions
                (namespace_id, candidate_id, service_id, endpoint_id,
                 policy_version, promoted_at, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'service:example',
                 'endpoint:example', 'two-signal-v1', '2026-09-01T00:02:00Z',
                 '{promotion_provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, one_source.returncode)
        self.assertIn("two independent verification signal kinds", one_source.stderr)

        self.psql(
            f"""
            INSERT INTO agent_economy.catalog_verification_signals
                (namespace_id, candidate_id, signal_id, verification_kind,
                 verifier_scope, observed_at, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'signal:sha256:{'3' * 64}',
                 'runtime', 'https://api.example/pay', '2026-09-01T00:01:00Z',
                 '{second_provenance}');
            INSERT INTO agent_economy.catalog_promotions
                (namespace_id, candidate_id, service_id, endpoint_id,
                 policy_version, promoted_at, provenance_id)
            VALUES
                ('{namespace}', '{candidate}', 'service:example',
                 'endpoint:example', 'two-signal-v1', '2026-09-01T00:02:00Z',
                 '{promotion_provenance}');
            """
        )

        catalogs = self.psql(
            f"""
            SELECT count(*) FROM agent_economy.shadow_catalog
            WHERE namespace_id = '{namespace}';
            SELECT count(*) FROM agent_economy.verified_catalog
            WHERE namespace_id = '{namespace}';
            SELECT trust_state FROM agent_economy.verified_catalog
            WHERE namespace_id = '{namespace}';
            SELECT trust_state FROM agent_economy.services
            WHERE namespace_id = '{namespace}' AND service_id = 'service:example';
            SELECT string_agg(alias_kind || ':' || upstream_id, ',' ORDER BY alias_kind)
            FROM agent_economy.catalog_candidate_aliases
            WHERE namespace_id = '{namespace}' AND candidate_id = '{candidate}';
            """
        ).stdout.splitlines()
        self.assertEqual(
            [
                "0",
                "1",
                "verified",
                "observed",
                "agentcash:listing-44,x402scan:upstream-17",
            ],
            catalogs,
        )


if __name__ == "__main__":
    unittest.main()
