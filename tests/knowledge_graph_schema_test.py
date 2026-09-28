import os
import subprocess
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0001_knowledge_graph.up.sql"
DOWN = ROOT / "migrations" / "0001_knowledge_graph.down.sql"
EXPECTED_TABLES = {
    "namespaces",
    "evidence_objects",
    "provenance_records",
    "services",
    "endpoints",
    "offers",
    "payment_options",
    "buyer_handles",
    "buyer_clusters",
    "buyer_cluster_versions",
    "buyer_cluster_memberships",
    "attribution_edges",
    "classification_claims",
    "classification_claim_evidence",
}


def table_definition(sql, table):
    return sql.split(f"CREATE TABLE agent_economy.{table} (", 1)[1].split("\n);", 1)[0]


class KnowledgeGraphMigrationContractTest(unittest.TestCase):
    def test_migration_models_the_bounded_canonical_graph(self):
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
            "PRIMARY KEY (namespace_id, service_id)",
            "PRIMARY KEY (namespace_id, endpoint_id)",
            "PRIMARY KEY (namespace_id, buyer_handle_id)",
            "PRIMARY KEY (namespace_id, cluster_id, version)",
            "membership_action text NOT NULL CHECK (membership_action IN ('add', 'remove'))",
            "confidence numeric(5, 4) NOT NULL CHECK (confidence >= 0 AND confidence <= 1)",
            "status text NOT NULL CHECK (status IN ('verified', 'inferred', 'disputed'))",
            "CHECK (valid_to IS NULL OR valid_to > valid_from)",
            "FOREIGN KEY (namespace_id, evidence_id)",
            "FOREIGN KEY (namespace_id, provenance_id)",
        ):
            self.assertIn(fragment, up)
        provenance_bearing = EXPECTED_TABLES - {
            "namespaces",
            "evidence_objects",
            "classification_claim_evidence",
        }
        for table in provenance_bearing:
            with self.subTest(table=table):
                self.assertIn("provenance_id uuid NOT NULL", table_definition(up, table))
        for table in EXPECTED_TABLES:
            self.assertIn(
                f"CREATE TRIGGER {table}_immutable",
                up,
            )
        for fragment in (
            "supersedes_version = version - 1",
            "status = 'reverted' AND reverts_version IS NOT NULL",
            "FOREIGN KEY (namespace_id, cluster_id, reverts_version)",
            "FOREIGN KEY (namespace_id, claim_id, supersedes_version)",
        ):
            self.assertIn(fragment, up)

    def test_down_migration_fails_closed_before_dropping_nonempty_graph(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("cannot roll back non-empty canonical knowledge graph", down)
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP SCHEMA"))
        self.assertNotIn("CASCADE", down)
        self.assertIn("DROP TABLE agent_economy.evidence_objects;", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


@unittest.skipUnless(
    os.environ.get("RUN_KNOWLEDGE_GRAPH_LIVE") == "1",
    "set RUN_KNOWLEDGE_GRAPH_LIVE=1 for PostgreSQL 17.6 migration qualification",
)
class KnowledgeGraphMigrationLiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = f"aem-knowledge-graph-{uuid.uuid4().hex[:12]}"
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
            # The image briefly starts a bootstrap postmaster before the final one.
            # Do not mistake that transient server for fixture readiness.
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
        return subprocess.run(
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
            check=check,
            capture_output=True,
            text=True,
        )

    def test_namespace_history_claims_and_rollback_safety(self):
        self.psql(UP.read_text(encoding="utf-8"))
        first = "00000000-0000-0000-0000-000000000001"
        second = "00000000-0000-0000-0000-000000000002"
        provenance = "00000000-0000-0000-0000-000000000010"
        second_provenance = "00000000-0000-0000-0000-000000000011"
        cluster = "00000000-0000-0000-0000-000000000020"
        self.psql(
            f"""
            INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key)
            VALUES ('{first}', 'tenant', 'alpha'), ('{second}', 'tenant', 'beta');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
            VALUES
                ('{first}', 'evidence:sha256:aa', repeat('a', 64), 'evidence://alpha/aa', 'application/json', 2, now()),
                ('{first}', 'evidence:sha256:cc', repeat('c', 64), 'evidence://alpha/cc', 'application/json', 2, now()),
                ('{second}', 'evidence:sha256:bb', repeat('b', 64), 'evidence://beta/bb', 'application/json', 2, now());
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at, parser_version, evidence_id)
            VALUES
                ('{first}', '{provenance}', 'source:test', now(), 'test-v1', 'evidence:sha256:aa'),
                ('{second}', '{second_provenance}', 'source:test', now(), 'test-v1', 'evidence:sha256:bb');
            INSERT INTO agent_economy.services
                (namespace_id, service_id, display_name, trust_state, provenance_id)
            VALUES
                ('{first}', 'service:shared', 'Alpha', 'verified', '{provenance}'),
                ('{second}', 'service:shared', 'Beta', 'candidate', '{second_provenance}');
            INSERT INTO agent_economy.buyer_handles
                (namespace_id, buyer_handle_id, handle_kind, chain_scope, handle_value, provenance_id)
            VALUES
                ('{first}', 'buyer:one', 'wallet', 'base', '0x01', '{provenance}');
            INSERT INTO agent_economy.buyer_clusters
                (namespace_id, cluster_id, display_name, provenance_id)
            VALUES ('{first}', '{cluster}', 'reversible cluster', '{provenance}');
            INSERT INTO agent_economy.buyer_cluster_versions
                (namespace_id, cluster_id, version, supersedes_version, reverts_version,
                 status, method, confidence, valid_from, provenance_id)
            VALUES
                ('{first}', '{cluster}', 1, NULL, NULL, 'accepted', 'shared-settlement', 0.8000, now() - interval '1 hour', '{provenance}');
            INSERT INTO agent_economy.buyer_cluster_memberships
                (namespace_id, cluster_id, cluster_version, buyer_handle_id, membership_action, method, confidence, provenance_id)
            VALUES
                ('{first}', '{cluster}', 1, 'buyer:one', 'add', 'shared-settlement', 0.8000, '{provenance}');
            INSERT INTO agent_economy.buyer_cluster_versions
                (namespace_id, cluster_id, version, supersedes_version, reverts_version,
                 status, method, confidence, valid_from, provenance_id)
            VALUES
                ('{first}', '{cluster}', 2, 1, 1, 'reverted', 'manual-reversal', 1.0000, now(), '{provenance}');
            INSERT INTO agent_economy.buyer_cluster_memberships
                (namespace_id, cluster_id, cluster_version, buyer_handle_id, membership_action, method, confidence, provenance_id)
            VALUES
                ('{first}', '{cluster}', 2, 'buyer:one', 'remove', 'manual-reversal', 1.0000, '{provenance}');
            INSERT INTO agent_economy.classification_claims
                (namespace_id, claim_id, version, buyer_handle_id, label, method, confidence,
                 evidence_window_start, evidence_window_end, valid_from, status, provenance_id)
            VALUES
                ('{first}', 'claim:buyer-kind', 1, 'buyer:one', 'agent', 'rule-v1', 0.7500,
                 now() - interval '1 day', now(), now(), 'inferred', '{provenance}');
            INSERT INTO agent_economy.classification_claim_evidence
                (namespace_id, claim_id, claim_version, evidence_id, evidence_role)
            VALUES
                ('{first}', 'claim:buyer-kind', 1, 'evidence:sha256:aa', 'supporting');
            """
        )

        history = self.psql(
            f"""
            SELECT string_agg(membership_action, ',' ORDER BY cluster_version)
            FROM agent_economy.buyer_cluster_memberships
            WHERE namespace_id = '{first}' AND cluster_id = '{cluster}';
            SELECT method || ':' || confidence || ':' || status
            FROM agent_economy.classification_claims
            WHERE namespace_id = '{first}' AND claim_id = 'claim:buyer-kind';
            """
        )
        self.assertEqual(["add,remove", "rule-v1:0.7500:inferred"], history.stdout.splitlines())

        duplicate = self.psql(
            f"""
            INSERT INTO agent_economy.services
                (namespace_id, service_id, display_name, trust_state, provenance_id)
            VALUES ('{first}', 'service:shared', 'Duplicate', 'candidate', '{provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, duplicate.returncode)
        invalid_confidence = self.psql(
            f"""
            INSERT INTO agent_economy.classification_claims
                (namespace_id, claim_id, version, buyer_handle_id, label, method, confidence,
                 evidence_window_start, evidence_window_end, valid_from, status, provenance_id)
            VALUES
                ('{first}', 'claim:invalid', 1, 'buyer:one', 'agent', 'rule-v1', 1.1000,
                 now() - interval '1 day', now(), now(), 'inferred', '{provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, invalid_confidence.returncode)

        missing_predecessor = self.psql(
            f"""
            INSERT INTO agent_economy.buyer_cluster_versions
                (namespace_id, cluster_id, version, supersedes_version, status, method,
                 confidence, valid_from, provenance_id)
            VALUES
                ('{first}', '{cluster}', 4, 3, 'accepted', 'invalid-gap', 1.0000, now(), '{provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, missing_predecessor.returncode)
        targetless_reversal = self.psql(
            f"""
            INSERT INTO agent_economy.buyer_cluster_versions
                (namespace_id, cluster_id, version, supersedes_version, status, method,
                 confidence, valid_from, provenance_id)
            VALUES
                ('{first}', '{cluster}', 3, 2, 'reverted', 'invalid-reversal', 1.0000, now(), '{provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, targetless_reversal.returncode)

        cross_namespace_provenance = self.psql(
            f"""
            INSERT INTO agent_economy.services
                (namespace_id, service_id, display_name, trust_state, provenance_id)
            VALUES ('{first}', 'service:crossed', 'Crossed', 'candidate', '{second_provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, cross_namespace_provenance.returncode)

        self.psql(
            f"""
            INSERT INTO agent_economy.buyer_handles
                (namespace_id, buyer_handle_id, handle_kind, chain_scope, handle_value, provenance_id)
            VALUES ('{first}', 'buyer:late', 'wallet', 'base', '0x02', '{provenance}');
            INSERT INTO agent_economy.classification_claims
                (namespace_id, claim_id, version, supersedes_version, buyer_handle_id,
                 label, method, confidence, evidence_window_start, evidence_window_end,
                 valid_from, status, provenance_id)
            VALUES
                ('{first}', 'claim:buyer-kind', 2, 1, 'buyer:one', 'agent', 'rule-v2',
                 0.8000, now() - interval '1 day', now(), now(), 'inferred', '{provenance}');
            """
        )
        late_membership = self.psql(
            f"""
            INSERT INTO agent_economy.buyer_cluster_memberships
                (namespace_id, cluster_id, cluster_version, buyer_handle_id,
                 membership_action, method, confidence, provenance_id)
            VALUES
                ('{first}', '{cluster}', 1, 'buyer:late', 'add', 'late-write', 1.0000, '{provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, late_membership.returncode)
        self.assertIn("cluster version is sealed", late_membership.stderr)
        late_claim_evidence = self.psql(
            f"""
            INSERT INTO agent_economy.classification_claim_evidence
                (namespace_id, claim_id, claim_version, evidence_id, evidence_role)
            VALUES
                ('{first}', 'claim:buyer-kind', 1, 'evidence:sha256:cc', 'supporting');
            """,
            check=False,
        )
        self.assertNotEqual(0, late_claim_evidence.returncode)
        self.assertIn("claim version is sealed", late_claim_evidence.stderr)

        immutable_evidence = self.psql(
            f"""
            UPDATE agent_economy.evidence_objects SET media_type = 'text/plain'
            WHERE namespace_id = '{first}' AND evidence_id = 'evidence:sha256:aa';
            """,
            check=False,
        )
        self.assertNotEqual(0, immutable_evidence.returncode)
        self.assertIn("canonical evidence and knowledge rows are immutable", immutable_evidence.stderr)
        immutable_provenance = self.psql(
            f"""
            DELETE FROM agent_economy.provenance_records
            WHERE namespace_id = '{first}' AND provenance_id = '{provenance}';
            """,
            check=False,
        )
        self.assertNotEqual(0, immutable_provenance.returncode)
        self.assertIn("canonical evidence and knowledge rows are immutable", immutable_provenance.stderr)
        immutable_binding = self.psql(
            f"""
            UPDATE agent_economy.services SET provenance_id = '{provenance}'
            WHERE namespace_id = '{first}' AND service_id = 'service:shared';
            """,
            check=False,
        )
        self.assertNotEqual(0, immutable_binding.returncode)
        immutable_claim = self.psql(
            f"""
            UPDATE agent_economy.classification_claims SET confidence = 0.1000
            WHERE namespace_id = '{first}' AND claim_id = 'claim:buyer-kind';
            """,
            check=False,
        )
        self.assertNotEqual(0, immutable_claim.returncode)
        immutable_claim_evidence = self.psql(
            f"""
            DELETE FROM agent_economy.classification_claim_evidence
            WHERE namespace_id = '{first}' AND claim_id = 'claim:buyer-kind';
            """,
            check=False,
        )
        self.assertNotEqual(0, immutable_claim_evidence.returncode)
        immutable_namespace = self.psql(
            f"""
            UPDATE agent_economy.namespaces SET namespace_key = 'renamed'
            WHERE namespace_id = '{first}';
            """,
            check=False,
        )
        self.assertNotEqual(0, immutable_namespace.returncode)

        rollback = self.psql(DOWN.read_text(encoding="utf-8"), check=False)
        self.assertNotEqual(0, rollback.returncode)
        self.assertIn("cannot roll back non-empty canonical knowledge graph", rollback.stderr)
        self.assertEqual(
            "agent_economy",
            self.psql("SELECT to_regnamespace('agent_economy');").stdout.strip(),
        )

        self.psql("DROP SCHEMA agent_economy CASCADE;")
        self.psql(UP.read_text(encoding="utf-8"))
        self.psql(DOWN.read_text(encoding="utf-8"))
        self.assertEqual("", self.psql("SELECT to_regnamespace('agent_economy');").stdout.strip())
        self.psql(UP.read_text(encoding="utf-8"))
        self.assertEqual(
            str(len(EXPECTED_TABLES)),
            self.psql(
                "SELECT count(*) FROM information_schema.tables "
                "WHERE table_schema = 'agent_economy' AND table_type = 'BASE TABLE';"
            ).stdout.strip(),
        )


if __name__ == "__main__":
    unittest.main()
