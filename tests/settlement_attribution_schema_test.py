import os
import subprocess
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0006_settlement_attribution.up.sql"
DOWN = ROOT / "migrations" / "0006_settlement_attribution.down.sql"
PREREQUISITES = [
    ROOT / "migrations" / f"{number:04d}_{name}.up.sql"
    for number, name in [
        (1, "knowledge_graph"),
        (2, "operational_analytics"),
        (3, "shadow_catalog"),
        (4, "buyer_enrichment"),
        (5, "reducer_finality"),
    ]
]


class SettlementAttributionMigrationContractTest(unittest.TestCase):
    def test_forward_migration_persists_replayable_evidence_backed_candidates(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        self.assertIn("CREATE TABLE agent_economy.payment_requirements", up)
        self.assertIn("CREATE TABLE agent_economy.attribution_runs", up)
        self.assertIn("CREATE TABLE agent_economy.attribution_run_evidence", up)
        self.assertIn("CREATE TABLE agent_economy.attribution_run_requirements", up)
        self.assertIn("CREATE TABLE agent_economy.attribution_candidates", up)
        self.assertIn("CREATE TABLE agent_economy.attribution_candidate_evidence", up)
        self.assertIn("CREATE TABLE agent_economy.attribution_run_seals", up)
        self.assertIn("JOIN agent_economy.provenance_records AS provenance", up)
        self.assertIn("provenance.evidence_id = NEW.evidence_id", up)
        self.assertIn("level text NOT NULL CHECK (level IN ('verified', 'strong', 'weak', 'unknown'))", up)
        self.assertIn("attribution_version integer NOT NULL CHECK (attribution_version > 0)", up)
        self.assertIn("engine_version text NOT NULL", up)
        self.assertIn("input_snapshot_hash text NOT NULL", up)
        self.assertIn("state_hash text NOT NULL", up)
        self.assertIn("result_encoding bytea NOT NULL", up)
        self.assertIn("pg_catalog.sha256(NEW.result_encoding)", up)
        self.assertIn("validate_attribution_run_seal", up)
        self.assertIn("attribution run is sealed", up)
        self.assertIn("settlement_evidence_id text NOT NULL", up)
        self.assertIn("requirement_evidence_id text NOT NULL", up)
        self.assertIn("FOREIGN KEY (namespace_id, chain_scope, settlement_id)", up)
        self.assertIn(
            "REFERENCES agent_economy.attribution_run_requirements",
            up,
        )
        self.assertIn("attribution_candidate_evidence_immutable", up)
        self.assertIn("attribution_runs_serialize_insert", up)
        self.assertTrue(up.rstrip().endswith("COMMIT;"))

    def test_down_migration_fails_closed_on_attribution_history(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("cannot roll back settlement attribution history", down)
        self.assertLess(down.index("LOCK TABLE"), down.index("IF EXISTS"))
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP TABLE"))
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


@unittest.skipUnless(
    os.environ.get("RUN_SETTLEMENT_ATTRIBUTION_LIVE") == "1",
    "set RUN_SETTLEMENT_ATTRIBUTION_LIVE=1 for PostgreSQL 17.6 migration qualification",
)
class SettlementAttributionMigrationLiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = f"aem-attribution-{uuid.uuid4().hex[:12]}"
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

    def test_shared_recipient_candidates_and_evidence_are_preserved(self):
        for migration in [*PREREQUISITES, UP]:
            self.psql(migration.read_text(encoding="utf-8"))
        namespace = "00000000-0000-0000-0000-000000000001"
        provenance = "00000000-0000-0000-0000-000000000010"
        catalog_provenance = "00000000-0000-0000-0000-000000000011"
        event = "event:x402:sha256:" + "a" * 64
        self.psql(
            f"""
            INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'attribution-test');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
            VALUES
                ('{namespace}', 'evidence:settlement', repeat('a', 64), 'evidence://settlement', 'application/json', 2, now()),
                ('{namespace}', 'evidence:catalog', repeat('b', 64), 'evidence://catalog', 'application/json', 2, now());
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at, parser_version,
                 chain_scope, evidence_id)
            VALUES
                ('{namespace}', '{provenance}', 'rpc-primary', now(), 'test@1',
                 'base', 'evidence:settlement'),
                ('{namespace}', '{catalog_provenance}', 'catalog-primary', now(), 'test@1',
                 'base', 'evidence:catalog');
            INSERT INTO agent_economy.services
                (namespace_id, service_id, display_name, trust_state, provenance_id)
            VALUES
                ('{namespace}', 'service:weather', 'Weather', 'observed', '{catalog_provenance}'),
                ('{namespace}', 'service:maps', 'Maps', 'observed', '{catalog_provenance}');
            INSERT INTO agent_economy.endpoints
                (namespace_id, endpoint_id, service_id, http_method, endpoint_uri, provenance_id)
            VALUES
                ('{namespace}', 'endpoint:forecast', 'service:weather', 'GET', 'https://weather.test/forecast', '{catalog_provenance}'),
                ('{namespace}', 'endpoint:directions', 'service:maps', 'GET', 'https://maps.test/directions', '{catalog_provenance}');
            INSERT INTO agent_economy.offers
                (namespace_id, offer_id, endpoint_id, offer_version, valid_from, provenance_id)
            VALUES
                ('{namespace}', 'offer:weather', 'endpoint:forecast', 1, now(), '{catalog_provenance}'),
                ('{namespace}', 'offer:maps', 'endpoint:directions', 1, now(), '{catalog_provenance}');
            INSERT INTO agent_economy.payment_options
                (namespace_id, payment_option_id, offer_id, protocol, network, asset,
                 amount_atomic, pay_to, provenance_id)
            VALUES
                ('{namespace}', 'option:weather', 'offer:weather', 'x402', 'base', 'USDC', 1000, '0xmerchant', '{catalog_provenance}'),
                ('{namespace}', 'option:maps', 'offer:maps', 'x402', 'base', 'USDC', 1000, '0xmerchant', '{catalog_provenance}');
            INSERT INTO agent_economy.payment_requirements
                (namespace_id, requirement_id, payment_option_id, endpoint_id, service_id,
                 evidence_id, provenance_id)
            VALUES
                ('{namespace}', 'requirement:weather', 'option:weather', 'endpoint:forecast',
                 'service:weather', 'evidence:catalog', '{catalog_provenance}'),
                ('{namespace}', 'requirement:maps', 'option:maps', 'endpoint:directions',
                 'service:maps', 'evidence:catalog', '{catalog_provenance}');
            INSERT INTO agent_economy.canonical_events
                (namespace_id, protocol, canonical_event_id, chain_scope, event_at,
                 reducer_version, canonical_state_hash)
            VALUES ('{namespace}', 'x402', '{event}', 'base', now(), 'reducer@1', repeat('c', 64));
            INSERT INTO agent_economy.settlements
                (namespace_id, chain_scope, settlement_id, protocol, canonical_event_id,
                 source_id, asset, amount_atomic, settled_at, provenance_id)
            VALUES ('{namespace}', 'base', 'settlement:base:0xabc', 'x402', '{event}',
                    'rpc-primary', 'USDC', 1000, now(), '{provenance}');
            INSERT INTO agent_economy.attribution_runs
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 supersedes_version, engine_version, encoding_version, match_method,
                 input_snapshot_hash, level, settlement_evidence_id)
            VALUES ('{namespace}', 'base', 'settlement:base:0xabc', 1, NULL,
                    'attribution@1', 'aem-attribution-result-v1', 'shared_exact',
                    repeat('d', 64), 'weak', 'evidence:settlement');
            INSERT INTO agent_economy.attribution_run_evidence
                (namespace_id, chain_scope, settlement_id, attribution_version, evidence_id, evidence_role)
            VALUES
                ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                 'evidence:settlement', 'settlement'),
                ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                 'evidence:catalog', 'catalog_snapshot');
            INSERT INTO agent_economy.attribution_run_requirements
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 requirement_id, payment_option_id, endpoint_id, service_id,
                 requirement_evidence_id)
            VALUES
                ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                 'requirement:weather', 'option:weather', 'endpoint:forecast',
                 'service:weather', 'evidence:catalog'),
                ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                 'requirement:maps', 'option:maps', 'endpoint:directions',
                 'service:maps', 'evidence:catalog');
            INSERT INTO agent_economy.attribution_candidates
                (namespace_id, chain_scope, settlement_id, attribution_version, candidate_id,
                 requirement_id, payment_option_id, endpoint_id, service_id, confidence,
                 settlement_evidence_id, requirement_evidence_id)
            VALUES
                ('{namespace}', 'base', 'settlement:base:0xabc', 1, 'candidate:weather',
                 'requirement:weather', 'option:weather', 'endpoint:forecast', 'service:weather',
                 0.5000, 'evidence:settlement', 'evidence:catalog'),
                ('{namespace}', 'base', 'settlement:base:0xabc', 1, 'candidate:maps',
                 'requirement:maps', 'option:maps', 'endpoint:directions', 'service:maps',
                 0.5000, 'evidence:settlement', 'evidence:catalog');
            INSERT INTO agent_economy.attribution_candidate_evidence
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 candidate_id, evidence_id, evidence_role)
            VALUES
                ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                 'candidate:weather', 'evidence:settlement', 'settlement'),
                ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                 'candidate:weather', 'evidence:catalog', 'requirement'),
                ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                 'candidate:maps', 'evidence:settlement', 'settlement'),
                ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                 'candidate:maps', 'evidence:catalog', 'requirement');
            INSERT INTO agent_economy.attribution_run_seals
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 result_encoding, state_hash)
            VALUES ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                    convert_to('weak-result', 'UTF8'),
                    encode(sha256(convert_to('weak-result', 'UTF8')), 'hex'));
            """
        )
        result = self.psql(
            f"""
            SELECT r.level || ':' || count(DISTINCT c.candidate_id) || ':' || count(e.evidence_id)
            FROM agent_economy.attribution_runs r
            JOIN agent_economy.attribution_candidates c USING
                (namespace_id, chain_scope, settlement_id, attribution_version)
            JOIN agent_economy.attribution_candidate_evidence e USING
                (namespace_id, chain_scope, settlement_id, attribution_version, candidate_id)
            WHERE r.namespace_id = '{namespace}'
            GROUP BY r.level;
            """
        )
        self.assertEqual("weak:2:4", result.stdout.strip())
        late_append = self.psql(
            f"""
            INSERT INTO agent_economy.attribution_run_evidence
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 evidence_id, evidence_role)
            VALUES ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                    'evidence:catalog', 'supporting');
            """,
            check=False,
        )
        self.assertNotEqual(0, late_append.returncode)
        self.assertIn("attribution run is sealed", late_append.stderr)
        bad_hash = self.psql(
            f"""
            INSERT INTO agent_economy.attribution_run_seals
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 result_encoding, state_hash)
            VALUES ('{namespace}', 'base', 'settlement:base:0xabc', 1,
                    convert_to('tampered', 'UTF8'), repeat('0', 64));
            """,
            check=False,
        )
        self.assertNotEqual(0, bad_hash.returncode)
        self.assertIn("state hash does not match", bad_hash.stderr)
        self.psql(
            f"""
            INSERT INTO agent_economy.attribution_runs
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 supersedes_version, engine_version, encoding_version, match_method,
                 input_snapshot_hash, level, settlement_evidence_id)
            VALUES ('{namespace}', 'base', 'settlement:base:0xabc', 2, 1,
                    'attribution@1', 'aem-attribution-result-v1', 'shared_exact',
                    repeat('f', 64), 'weak', 'evidence:settlement');
            """
        )
        invalid_cardinality = self.psql(
            f"""
            INSERT INTO agent_economy.attribution_run_seals
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 result_encoding, state_hash)
            VALUES ('{namespace}', 'base', 'settlement:base:0xabc', 2,
                    convert_to('empty-weak', 'UTF8'),
                    encode(sha256(convert_to('empty-weak', 'UTF8')), 'hex'));
            """,
            check=False,
        )
        self.assertNotEqual(0, invalid_cardinality.returncode)
        self.assertIn("candidate count", invalid_cardinality.stderr)
        unsealed_successor = self.psql(
            f"""
            INSERT INTO agent_economy.attribution_runs
                (namespace_id, chain_scope, settlement_id, attribution_version,
                 supersedes_version, engine_version, encoding_version, match_method,
                 input_snapshot_hash, level, settlement_evidence_id)
            VALUES ('{namespace}', 'base', 'settlement:base:0xabc', 3, 2,
                    'attribution@1', 'aem-attribution-result-v1', 'none',
                    repeat('1', 64), 'unknown', 'evidence:settlement');
            """,
            check=False,
        )
        self.assertNotEqual(0, unsealed_successor.returncode)
        self.assertIn("predecessor must be sealed", unsealed_successor.stderr)
        mutation = self.psql(
            "UPDATE agent_economy.attribution_candidates SET confidence = 1.0;",
            check=False,
        )
        self.assertNotEqual(0, mutation.returncode)
        rollback = self.psql(DOWN.read_text(encoding="utf-8"), check=False)
        self.assertNotEqual(0, rollback.returncode)
