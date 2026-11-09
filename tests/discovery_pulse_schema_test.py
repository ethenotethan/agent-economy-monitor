import os
import pathlib
import subprocess
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0019_discovery_pulse.up.sql"
DOWN = ROOT / "migrations" / "0019_discovery_pulse.down.sql"
VERIFY = ROOT / "scripts" / "verify"


class DiscoveryPulseMigrationTest(unittest.TestCase):
    def test_verified_discovery_read_model_is_separate_and_provenance_bearing(self) -> None:
        sql = UP.read_text()
        self.assertIn("CREATE VIEW agent_economy.dashboard_discovery_activity AS", sql)
        self.assertIn("JOIN agent_economy.canonical_event_observations", sql)
        self.assertIn("JOIN agent_economy.canonical_events", sql)
        self.assertIn("CREATE TABLE agent_economy.discovery_source_registry", sql)
        self.assertIn("JOIN agent_economy.discovery_source_registry", sql)
        self.assertIn("'cdp-x402-bazaar-base', 'x402-adapter@1'", sql)
        self.assertIn("support_role = 'supporting'", sql)
        self.assertIn("'discovery_observation'::text AS kind", sql)
        for field in (
            "source_id",
            "chain_scope",
            "protocol",
            "parser_version",
            "observation_count",
            "evidence_ids",
        ):
            self.assertIn(f"'{field}'", sql)
        self.assertIn("array_agg(DISTINCT accepted.provenance_id::text", sql)
        self.assertIn("count(*) OVER", sql)
        self.assertIn("row_number() OVER", sql)
        self.assertIn("WHERE accepted.lineage_position <= 100", sql)
        self.assertIn("'lineage_limit', 100", sql)
        self.assertNotIn("agent_economy.settlements", sql)
        self.assertIn(
            "GRANT SELECT ON agent_economy.dashboard_discovery_activity "
            "TO agent_economy_dashboard_reader;",
            sql,
        )

    def test_down_migration_removes_discovery_read_model(self) -> None:
        self.assertEqual(
            DOWN.read_text().splitlines(),
            [
                "BEGIN;",
                "",
                "DROP VIEW agent_economy.dashboard_discovery_activity;",
                "DROP TABLE agent_economy.discovery_source_registry;",
                "",
                "COMMIT;",
            ],
        )

    def test_canonical_gate_runs_the_live_zero_settlement_fixture(self) -> None:
        verify = VERIFY.read_text()
        self.assertIn(
            'AEM_DISCOVERY_TEST_DATABASE_URL="$AEM_COLLECT_TEST_DATABASE_URL"',
            verify,
        )
        self.assertIn("-p 'discovery_pulse_schema_test.py' -v", verify)


@unittest.skipUnless(
    os.environ.get("AEM_DISCOVERY_TEST_DATABASE_URL"),
    "set AEM_DISCOVERY_TEST_DATABASE_URL for the zero-settlement regression fixture",
)
class DiscoveryPulseLiveTest(unittest.TestCase):
    def psql(self, sql: str) -> str:
        return subprocess.run(
            [
                "psql",
                os.environ["AEM_DISCOVERY_TEST_DATABASE_URL"],
                "-v",
                "ON_ERROR_STOP=1",
                "-At",
                "-c",
                sql,
            ],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    def test_verified_observations_remain_visible_without_settlements(self) -> None:
        namespace = "00000000-0000-0000-0000-000000000075"
        provenance = "00000000-0000-0000-0000-000000000175"
        history_provenance = "00000000-0000-0000-0000-000000000275"
        observation = "sha256:" + "b" * 64
        history_observation = "sha256:" + "f" * 64
        event = "event:x402:sha256:" + "c" * 64
        history_event = "event:x402:sha256:" + "9" * 64
        self.psql(f"""
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'issue-75-regression');
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type,
                 byte_length, observed_at)
            VALUES ('{namespace}', 'evidence:issue-75', repeat('a', 64),
                    'evidence://issue-75', 'application/json', 2,
                    '2026-11-01T00:00:00Z');
            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at,
                 parser_version, provider, chain_scope, evidence_id)
            VALUES ('{namespace}', '{provenance}', 'cdp-x402-bazaar-base',
                    '2026-11-01T00:00:00Z', 'x402-adapter@1', 'cdp-bazaar',
                    'base', 'evidence:issue-75');
            INSERT INTO agent_economy.observations
                (namespace_id, chain_scope, source_id, observation_id,
                 observed_at, parser_version, protocol, evidence_id,
                 provenance_id, observation_hash)
            VALUES ('{namespace}', 'base', 'cdp-x402-bazaar-base', '{observation}',
                    '2026-11-01T00:00:00Z', 'x402-adapter@1', 'x402',
                    'evidence:issue-75', '{provenance}', repeat('d', 64));
            INSERT INTO agent_economy.canonical_events
                (namespace_id, protocol, canonical_event_id, chain_scope,
                 event_at, reducer_version, canonical_state_hash)
            VALUES ('{namespace}', 'x402', '{event}', 'base',
                    '2026-11-01T00:00:00Z', 'reducer@1', repeat('e', 64));
            INSERT INTO agent_economy.canonical_event_observations
                (namespace_id, protocol, canonical_event_id, chain_scope,
                 source_id, observation_id, support_role)
            VALUES ('{namespace}', 'x402', '{event}', 'base',
                    'cdp-x402-bazaar-base', '{observation}', 'supporting');

            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at,
                 parser_version, provider, chain_scope, evidence_id)
            VALUES ('{namespace}', '{history_provenance}', 'alchemy-history',
                    '2026-11-01T00:00:00Z', 'buyer-history@1', 'alchemy',
                    'base', 'evidence:issue-75');
            INSERT INTO agent_economy.observations
                (namespace_id, chain_scope, source_id, observation_id,
                 observed_at, parser_version, protocol, evidence_id,
                 provenance_id, observation_hash)
            VALUES ('{namespace}', 'base', 'alchemy-history', '{history_observation}',
                    '2026-11-01T00:00:00Z', 'buyer-history@1', 'x402',
                    'evidence:issue-75', '{history_provenance}', repeat('1', 64));
            INSERT INTO agent_economy.canonical_events
                (namespace_id, protocol, canonical_event_id, chain_scope,
                 event_at, reducer_version, canonical_state_hash)
            VALUES ('{namespace}', 'x402', '{history_event}', 'base',
                    '2026-11-01T00:00:00Z', 'reducer@1', repeat('2', 64));
            INSERT INTO agent_economy.canonical_event_observations
                (namespace_id, protocol, canonical_event_id, chain_scope,
                 source_id, observation_id, support_role)
            VALUES ('{namespace}', 'x402', '{history_event}', 'base',
                    'alchemy-history', '{history_observation}', 'supporting');
        """)

        result = self.psql(f"""
            SELECT
                (SELECT count(*) FROM agent_economy.settlements
                 WHERE namespace_id = '{namespace}') || '|' ||
                kind || '|' || (value ->> 'observation_count') || '|' ||
                (value ->> 'source_id') || '|' || (value ->> 'chain_scope') || '|' ||
                (value ->> 'protocol') || '|' || (value ->> 'parser_version') || '|' ||
                (value -> 'evidence_ids' ->> 0) || '|' || provenance_ids[1]
            FROM agent_economy.dashboard_discovery_activity
            WHERE namespace_id = '{namespace}'
        """)
        self.assertEqual(
            result,
            "0|discovery_observation|1|cdp-x402-bazaar-base|base|x402|"
            f"x402-adapter@1|evidence:issue-75|{provenance}",
        )

    def test_discovery_lineage_is_bounded_but_total_count_remains_accurate(self) -> None:
        namespace = "00000000-0000-0000-0000-000000000076"
        self.psql(f"""
            INSERT INTO agent_economy.namespaces
                (namespace_id, namespace_kind, namespace_key)
            VALUES ('{namespace}', 'tenant', 'issue-75-cardinality-regression');

            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri, media_type,
                 byte_length, observed_at)
            SELECT '{namespace}',
                   'evidence:issue-75-cardinality:' || sequence,
                   md5('evidence-a-' || sequence) || md5('evidence-b-' || sequence),
                   'evidence://issue-75-cardinality/' || sequence,
                   'application/json', 2,
                   '2026-11-01T00:00:00Z'::timestamptz + sequence * interval '1 second'
            FROM generate_series(1, 120) AS sequence;

            INSERT INTO agent_economy.provenance_records
                (namespace_id, provenance_id, source_id, observed_at,
                 parser_version, provider, chain_scope, evidence_id)
            SELECT '{namespace}',
                   (substr(digest, 1, 8) || '-' || substr(digest, 9, 4) || '-' ||
                    substr(digest, 13, 4) || '-' || substr(digest, 17, 4) || '-' ||
                    substr(digest, 21, 12))::uuid,
                   'cdp-x402-bazaar-base',
                   '2026-11-01T00:00:00Z'::timestamptz + sequence * interval '1 second',
                   'x402-adapter@1', 'cdp-bazaar', 'base',
                   'evidence:issue-75-cardinality:' || sequence
            FROM (
                SELECT sequence, md5('provenance-' || sequence) AS digest
                FROM generate_series(1, 120) AS sequence
            ) AS generated;

            INSERT INTO agent_economy.observations
                (namespace_id, chain_scope, source_id, observation_id,
                 observed_at, parser_version, protocol, evidence_id,
                 provenance_id, observation_hash)
            SELECT '{namespace}', 'base', 'cdp-x402-bazaar-base',
                   'sha256:' || md5('observation-a-' || sequence) ||
                       md5('observation-b-' || sequence),
                   '2026-11-01T00:00:00Z'::timestamptz + sequence * interval '1 second',
                   'x402-adapter@1', 'x402',
                   'evidence:issue-75-cardinality:' || sequence,
                   (substr(digest, 1, 8) || '-' || substr(digest, 9, 4) || '-' ||
                    substr(digest, 13, 4) || '-' || substr(digest, 17, 4) || '-' ||
                    substr(digest, 21, 12))::uuid,
                   md5('observation-hash-a-' || sequence) ||
                       md5('observation-hash-b-' || sequence)
            FROM (
                SELECT sequence, md5('provenance-' || sequence) AS digest
                FROM generate_series(1, 120) AS sequence
            ) AS generated;

            INSERT INTO agent_economy.canonical_events
                (namespace_id, protocol, canonical_event_id, chain_scope,
                 event_at, reducer_version, canonical_state_hash)
            SELECT '{namespace}', 'x402',
                   'event:x402:sha256:' || md5('event-a-' || sequence) ||
                       md5('event-b-' || sequence),
                   'base',
                   '2026-11-01T00:00:00Z'::timestamptz + sequence * interval '1 second',
                   'reducer@1',
                   md5('state-a-' || sequence) || md5('state-b-' || sequence)
            FROM generate_series(1, 120) AS sequence;

            INSERT INTO agent_economy.canonical_event_observations
                (namespace_id, protocol, canonical_event_id, chain_scope,
                 source_id, observation_id, support_role)
            SELECT '{namespace}', 'x402',
                   'event:x402:sha256:' || md5('event-a-' || sequence) ||
                       md5('event-b-' || sequence),
                   'base', 'cdp-x402-bazaar-base',
                   'sha256:' || md5('observation-a-' || sequence) ||
                       md5('observation-b-' || sequence),
                   'supporting'
            FROM generate_series(1, 120) AS sequence;
        """)

        result = self.psql(f"""
            SELECT (value ->> 'observation_count') || '|' ||
                   jsonb_array_length(value -> 'evidence_ids') || '|' ||
                   cardinality(provenance_ids) || '|' ||
                   (value ->> 'lineage_limit') || '|' ||
                   (value ->> 'lineage_truncated')
            FROM agent_economy.dashboard_discovery_activity
            WHERE namespace_id = '{namespace}'
        """)
        self.assertEqual(result, "120|100|100|100|true")


if __name__ == "__main__":
    unittest.main()
