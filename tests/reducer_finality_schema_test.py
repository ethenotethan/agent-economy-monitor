import os
import subprocess
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0005_reducer_finality.up.sql"
DOWN = ROOT / "migrations" / "0005_reducer_finality.down.sql"
PREREQUISITES = [
    ROOT / "migrations" / "0001_knowledge_graph.up.sql",
    ROOT / "migrations" / "0002_operational_analytics.up.sql",
    ROOT / "migrations" / "0003_shadow_catalog.up.sql",
    ROOT / "migrations" / "0004_buyer_enrichment.up.sql",
]


class ReducerFinalityMigrationContractTest(unittest.TestCase):
    def test_forward_migration_persists_lossless_finality_assertions(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        self.assertIn("DROP CONSTRAINT event_finality_updates_finality_status_check", up)
        self.assertIn("'observed', 'confirmed', 'finalized', 'orphaned', 'reverted'", up)
        self.assertIn("CREATE TABLE agent_economy.event_finality_assertions", up)
        self.assertIn("accepted boolean NOT NULL", up)
        self.assertIn("asserted_status text NOT NULL", up)
        self.assertIn("current_status text NOT NULL", up)
        self.assertIn("transaction_id text NOT NULL", up)
        self.assertIn("basis_kind text NOT NULL", up)
        self.assertIn("position bigint NOT NULL", up)
        self.assertIn("block_hash text NOT NULL", up)
        self.assertIn("canonical_block_hash text NOT NULL", up)
        self.assertIn("confirmations_required bigint", up)
        self.assertIn("commitment text", up)
        self.assertIn("execution_outcome text NOT NULL", up)
        self.assertIn("finality_state_hash text NOT NULL", up)
        self.assertIn("provenance_id uuid NOT NULL", up)
        self.assertIn("validate_finality_assertion_provenance", up)
        self.assertIn("JOIN agent_economy.observations AS observation", up)
        self.assertIn(
            "JOIN agent_economy.canonical_event_observations AS event_observation", up
        )
        self.assertIn(
            "event_observation.canonical_event_id = NEW.canonical_event_id", up
        )
        self.assertIn("event_finality_assertions_immutable", up)
        self.assertIn("event_finality_assertions_truncate_immutable", up)
        self.assertIn("CREATE VIEW agent_economy.event_finality_conflicts", up)
        self.assertTrue(up.rstrip().endswith("COMMIT;"))

    def test_down_migration_fails_closed_on_new_finality_history(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("cannot roll back reducer finality history", down)
        self.assertIn("LOCK TABLE agent_economy.event_finality_assertions", down)
        self.assertLess(down.index("LOCK TABLE"), down.index("IF EXISTS"))
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP TABLE"))
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


@unittest.skipUnless(
    os.environ.get("RUN_REDUCER_FINALITY_LIVE") == "1",
    "set RUN_REDUCER_FINALITY_LIVE=1 for PostgreSQL 17.6 migration qualification",
)
class ReducerFinalityMigrationLiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = f"aem-reducer-finality-{uuid.uuid4().hex[:12]}"
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

    def test_lossless_assertion_subject_binding_and_rollback_guard(self):
        for migration in [*PREREQUISITES, UP]:
            self.psql(migration.read_text(encoding="utf-8"))
        namespace = "00000000-0000-0000-0000-000000000001"
        provenance = "00000000-0000-0000-0000-000000000010"
        event = "event:x402:sha256:" + "a" * 64
        other_event = "event:x402:sha256:" + "b" * 64
        observation = "sha256:" + "f" * 64
        self.psql(
            f"""
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'reducer-test');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
            VALUES ('{namespace}', 'evidence:test', repeat('b', 64),
                    'evidence://test', 'application/json', 2, now());
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at, parser_version,
                 chain_scope, transaction_reference, evidence_id)
            VALUES ('{namespace}', '{provenance}', 'rpc-primary', now(), 'chain@1',
                    'base', '0xtx-a', 'evidence:test');
            INSERT INTO agent_economy.canonical_events
                (namespace_id, protocol, canonical_event_id, chain_scope, event_at,
                 reducer_version, canonical_state_hash)
            VALUES ('{namespace}', 'x402', '{event}', 'base', now(), 'reducer@1', repeat('c', 64));
            INSERT INTO agent_economy.canonical_events
                (namespace_id, protocol, canonical_event_id, chain_scope, event_at,
                 reducer_version, canonical_state_hash)
            VALUES ('{namespace}', 'x402', '{other_event}', 'base', now(), 'reducer@1', repeat('d', 64));
            INSERT INTO agent_economy.observations
                (namespace_id, chain_scope, source_id, observation_id, observed_at,
                 parser_version, protocol, evidence_id, provenance_id, observation_hash)
            VALUES ('{namespace}', 'base', 'rpc-primary', '{observation}', now(),
                    'chain@1', 'x402', 'evidence:test', '{provenance}', repeat('e', 64));
            INSERT INTO agent_economy.canonical_event_observations
                (namespace_id, protocol, canonical_event_id, chain_scope, source_id,
                 observation_id, support_role)
            VALUES ('{namespace}', 'x402', '{event}', 'base', 'rpc-primary',
                    '{observation}', 'supporting');
            INSERT INTO agent_economy.event_finality_assertions
                (namespace_id, protocol, chain_scope, canonical_event_id, assertion_sequence,
                 accepted, asserted_status, current_status, asserted_at, source_id,
                 provenance_id, transaction_id, basis_kind, position, block_hash,
                 canonical_block_hash, latest_position, finalized_position,
                 confirmations_required, execution_outcome, finality_state_hash)
            VALUES ('{namespace}', 'x402', 'base', '{event}', 1, false, 'confirmed',
                    'finalized', now(), 'rpc-primary', '{provenance}', '0xtx-a', 'evm',
                    10, '0xaaa', '0xaaa', 15, 0, 5, 'succeeded', repeat('d', 64));
            """
        )
        conflicts = self.psql(
            "SELECT count(*) FROM agent_economy.event_finality_conflicts;"
        )
        self.assertEqual("1", conflicts.stdout.strip())
        mismatched = self.psql(
            f"""
            INSERT INTO agent_economy.event_finality_assertions
                (namespace_id, protocol, chain_scope, canonical_event_id, assertion_sequence,
                 accepted, asserted_status, current_status, asserted_at, source_id,
                 provenance_id, transaction_id, basis_kind, position, block_hash,
                 canonical_block_hash, latest_position, finalized_position,
                 confirmations_required, execution_outcome, finality_state_hash)
            VALUES ('{namespace}', 'x402', 'base', '{event}', 2, true, 'finalized',
                    'finalized', now(), 'rpc-primary', '{provenance}', '0xtx-wrong', 'evm',
                    10, '0xaaa', '0xaaa', 20, 10, 5, 'succeeded', repeat('e', 64));
            """,
            check=False,
        )
        self.assertNotEqual(0, mismatched.returncode)
        cross_event = self.psql(
            f"""
            INSERT INTO agent_economy.event_finality_assertions
                (namespace_id, protocol, chain_scope, canonical_event_id, assertion_sequence,
                 accepted, asserted_status, current_status, asserted_at, source_id,
                 provenance_id, transaction_id, basis_kind, position, block_hash,
                 canonical_block_hash, latest_position, finalized_position,
                 confirmations_required, execution_outcome, finality_state_hash)
            VALUES ('{namespace}', 'x402', 'base', '{other_event}', 1, true, 'confirmed',
                    'confirmed', now(), 'rpc-primary', '{provenance}', '0xtx-a', 'evm',
                    10, '0xaaa', '0xaaa', 15, 0, 5, 'succeeded', repeat('f', 64));
            """,
            check=False,
        )
        self.assertNotEqual(0, cross_event.returncode)
        rollback = self.psql(DOWN.read_text(encoding="utf-8"), check=False)
        self.assertNotEqual(0, rollback.returncode)


if __name__ == "__main__":
    unittest.main()
