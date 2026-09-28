import os
import re
import subprocess
import time
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
KNOWLEDGE_UP = ROOT / "migrations" / "0001_knowledge_graph.up.sql"
UP = ROOT / "migrations" / "0002_operational_analytics.up.sql"
DOWN = ROOT / "migrations" / "0002_operational_analytics.down.sql"
EXPECTED_TABLES = {
    "collection_jobs",
    "observations",
    "canonical_events",
    "canonical_event_observations",
    "event_finality_updates",
    "settlements",
    "feature_windows",
    "analytics_daily_metrics",
}


def table_definition(sql, table):
    return sql.split(f"CREATE TABLE agent_economy.{table} (", 1)[1].split("\n)", 1)[0]


class OperationalAnalyticsMigrationContractTest(unittest.TestCase):
    def test_migration_models_partitioned_replayable_operations_and_analytics(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        self.assertTrue(up.rstrip().endswith("COMMIT;"))
        self.assertEqual(
            EXPECTED_TABLES,
            {
                line.split("agent_economy.", 1)[1].split()[0]
                for line in up.splitlines()
                if line.startswith("CREATE TABLE agent_economy.")
                and " PARTITION OF " not in line
            },
        )
        for fragment in (
            "PARTITION BY HASH (namespace_id, chain_scope, source_id)",
            "PARTITION BY HASH (namespace_id, protocol)",
            "PARTITION BY HASH (namespace_id, chain_scope)",
            "CREATE TABLE agent_economy.observations_p7 PARTITION OF",
            "CREATE TABLE agent_economy.canonical_events_p7 PARTITION OF",
            "CREATE TABLE agent_economy.settlements_p7 PARTITION OF",
            "CREATE MATERIALIZED VIEW agent_economy.pulse_hourly",
            "CREATE VIEW agent_economy.current_event_finality",
            "CREATE VIEW agent_economy.bigquery_projection_readiness",
            "p95_query_latency_ms > 2000",
            "canonical_event_count > 100000000",
            "analytics_cpu_percent > 30",
            "ROWS BETWEEN 13 PRECEDING AND CURRENT ROW",
            "provenance_records_source_chain_key",
            "provenance_records_evidence_source_chain_key",
            "buyer_handles_chain_key",
            "DO $partition_triggers$",
        ):
            self.assertIn(fragment, up)

        observations = table_definition(up, "observations")
        self.assertIn(
            "PRIMARY KEY (namespace_id, chain_scope, source_id, protocol, observation_id)",
            observations,
        )
        self.assertIn("observed_at timestamptz NOT NULL", observations)
        self.assertIn("parser_version text NOT NULL", observations)
        self.assertIn("observation_hash text NOT NULL", observations)

        events = table_definition(up, "canonical_events")
        self.assertIn(
            "PRIMARY KEY (namespace_id, protocol, chain_scope, canonical_event_id)", events
        )
        self.assertIn("canonical_state_hash text NOT NULL", events)
        self.assertIn("reducer_version text NOT NULL", events)

        for table in (
            "observations",
            "canonical_events",
            "canonical_event_observations",
            "event_finality_updates",
            "settlements",
            "feature_windows",
            "analytics_daily_metrics",
        ):
            with self.subTest(table=table):
                self.assertIn(f"CREATE TRIGGER {table}_immutable", up)
                self.assertIn(f"CREATE TRIGGER {table}_truncate_immutable", up)

        self.assertIn(
            "CREATE INDEX observations_chain_source_time_idx", up
        )
        self.assertIn(
            "CREATE INDEX canonical_events_protocol_time_idx", up
        )
        self.assertIn(
            "CREATE INDEX feature_windows_buyer_window_idx", up
        )

    def test_down_migration_fails_closed_before_dropping_nonempty_state(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("cannot roll back non-empty operational and analytics schema", down)
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP MATERIALIZED VIEW"))
        self.assertNotIn("CASCADE", down)
        self.assertIn("DROP TABLE agent_economy.observations;", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


@unittest.skipUnless(
    os.environ.get("RUN_OPERATIONAL_ANALYTICS_LIVE") == "1",
    "set RUN_OPERATIONAL_ANALYTICS_LIVE=1 for PostgreSQL 17.6 qualification",
)
class OperationalAnalyticsMigrationLiveTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.container = f"aem-operational-analytics-{uuid.uuid4().hex[:12]}"
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

    def test_replay_finality_queries_and_scale_thresholds(self):
        self.psql(KNOWLEDGE_UP.read_text(encoding="utf-8"))
        self.psql(UP.read_text(encoding="utf-8"))
        namespace = "00000000-0000-0000-0000-000000000001"
        provenance = "00000000-0000-0000-0000-000000000010"
        buyer = "buyer:base:0x01"
        event_id = "event:x402:sha256:" + "b" * 64
        observation_id = "sha256:" + "c" * 64
        settlement_id = "settlement:base:" + "d" * 64
        seed = f"""
        INSERT INTO agent_economy.namespaces (namespace_id, namespace_kind, namespace_key)
        VALUES ('{namespace}', 'tenant', 'alpha')
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.evidence_objects
            (namespace_id, evidence_id, sha256, storage_uri, media_type, byte_length, observed_at)
        VALUES
            ('{namespace}', 'evidence:sha256:aa', repeat('a', 64),
             'evidence://alpha/aa', 'application/json', 2, '2026-09-01T00:00:00Z')
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.provenance_records
            (namespace_id, provenance_id, source_id, observed_at, parser_version,
             chain_scope, evidence_id)
        VALUES
            ('{namespace}', '{provenance}', 'rpc-primary', '2026-09-01T00:00:00Z',
             'x402-adapter@1', 'base', 'evidence:sha256:aa')
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.buyer_handles
            (namespace_id, buyer_handle_id, handle_kind, chain_scope, handle_value, provenance_id)
        VALUES ('{namespace}', '{buyer}', 'wallet', 'base', '0x01', '{provenance}')
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.observations
            (namespace_id, chain_scope, source_id, observation_id, observed_at,
             parser_version, evidence_id, provenance_id, observation_hash, protocol)
        VALUES
            ('{namespace}', 'base', 'rpc-primary', '{observation_id}',
             '2026-09-01T00:00:00Z', 'x402-adapter@1', 'evidence:sha256:aa',
             '{provenance}', repeat('c', 64), 'x402')
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.canonical_events
            (namespace_id, protocol, canonical_event_id, chain_scope, event_at,
             reducer_version, canonical_state_hash)
        VALUES
            ('{namespace}', 'x402', '{event_id}', 'base', '2026-09-01T00:01:00Z',
             'reducer@1', repeat('b', 64))
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.canonical_event_observations
            (namespace_id, protocol, canonical_event_id, chain_scope, source_id,
             observation_id, support_role)
        VALUES
            ('{namespace}', 'x402', '{event_id}', 'base', 'rpc-primary',
             '{observation_id}', 'supporting')
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.event_finality_updates
            (namespace_id, protocol, chain_scope, canonical_event_id, finality_sequence,
             finality_status, asserted_at, source_id, provenance_id)
        VALUES
            ('{namespace}', 'x402', 'base', '{event_id}', 1, 'finalized',
             '2026-09-01T00:02:00Z', 'rpc-primary', '{provenance}')
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.settlements
            (namespace_id, chain_scope, settlement_id, protocol, canonical_event_id,
             source_id, buyer_handle_id, asset, amount_atomic, settled_at, provenance_id)
        VALUES
            ('{namespace}', 'base', '{settlement_id}', 'x402', '{event_id}',
             'rpc-primary', '{buyer}', 'USDC', 1000,
             '2026-09-01T00:01:00Z', '{provenance}')
        ON CONFLICT DO NOTHING;
        INSERT INTO agent_economy.feature_windows
            (namespace_id, buyer_handle_id, feature_name, window_start, window_end,
             feature_version, input_snapshot_hash, feature_value)
        VALUES
            ('{namespace}', '{buyer}', 'payment_count', '2026-09-01T00:00:00Z',
             '2026-09-02T00:00:00Z', 'feature@1', repeat('e', 64), '{{"count": 1}}')
        ON CONFLICT DO NOTHING;
        """
        self.psql(seed)
        before = self.psql(
            """
            SELECT json_build_array(
                (SELECT count(*) FROM agent_economy.observations),
                (SELECT count(*) FROM agent_economy.canonical_events),
                (SELECT count(*) FROM agent_economy.canonical_event_observations),
                (SELECT count(*) FROM agent_economy.settlements),
                (SELECT count(*) FROM agent_economy.feature_windows)
            );
            """
        ).stdout.strip()
        self.psql(seed)
        after = self.psql(
            """
            SELECT json_build_array(
                (SELECT count(*) FROM agent_economy.observations),
                (SELECT count(*) FROM agent_economy.canonical_events),
                (SELECT count(*) FROM agent_economy.canonical_event_observations),
                (SELECT count(*) FROM agent_economy.settlements),
                (SELECT count(*) FROM agent_economy.feature_windows)
            );
            """
        ).stdout.strip()
        self.assertEqual("[1, 1, 1, 1, 1]", before)
        self.assertEqual(before, after)

        self.psql("REFRESH MATERIALIZED VIEW agent_economy.pulse_hourly;")
        pulse = self.psql(
            f"""
            SELECT settlement_count || ':' || active_buyers || ':' || amount_atomic
            FROM agent_economy.pulse_hourly
            WHERE namespace_id = '{namespace}'
              AND hour = '2026-09-01T00:00:00Z'
              AND protocol = 'x402' AND chain_scope = 'base' AND asset = 'USDC';
            """
        ).stdout.strip()
        self.assertEqual("1:1:1000", pulse)
        pulse_plan = self.psql(
            f"""
            SET enable_seqscan = off;
            EXPLAIN (COSTS OFF)
            SELECT settlement_count, active_buyers, amount_atomic
            FROM agent_economy.pulse_hourly
            WHERE namespace_id = '{namespace}'
              AND hour >= '2026-09-01T00:00:00Z'
            ORDER BY hour DESC LIMIT 24;
            """
        ).stdout
        self.assertIn("pulse_hourly_identity_idx", pulse_plan)

        self.psql(
            f"""
            INSERT INTO agent_economy.observations
                (namespace_id, chain_scope, source_id, observation_id, observed_at,
                 parser_version, evidence_id, provenance_id, observation_hash, protocol)
            VALUES
                ('{namespace}', 'base', 'rpc-primary', 'sha256:{'f' * 64}',
                 '2026-09-01T00:00:01Z', 'mpp-adapter@1', 'evidence:sha256:aa',
                 '{provenance}', repeat('f', 64), 'mpp');
            """
        )
        mismatched_protocol = self.psql(
            f"""
            INSERT INTO agent_economy.canonical_event_observations
                (namespace_id, protocol, canonical_event_id, chain_scope, source_id,
                 observation_id, support_role)
            VALUES
                ('{namespace}', 'x402', '{event_id}', 'base', 'rpc-primary',
                 'sha256:{'f' * 64}', 'supporting');
            """,
            check=False,
        )
        self.assertNotEqual(0, mismatched_protocol.returncode)

        mismatched_provenance = self.psql(
            f"""
            INSERT INTO agent_economy.observations
                (namespace_id, chain_scope, source_id, observation_id, observed_at,
                 parser_version, evidence_id, provenance_id, observation_hash, protocol)
            VALUES
                ('{namespace}', 'ethereum', 'rpc-secondary', 'sha256:{'e' * 64}',
                 '2026-09-01T00:00:02Z', 'mpp-adapter@1', 'evidence:sha256:aa',
                 '{provenance}', repeat('e', 64), 'mpp');
            """,
            check=False,
        )
        self.assertNotEqual(0, mismatched_provenance.returncode)

        self.psql(
            f"""
            INSERT INTO agent_economy.evidence_objects
                (namespace_id, evidence_id, sha256, storage_uri,
                 media_type, byte_length, observed_at)
            VALUES
                ('{namespace}', 'evidence:sha256:22', repeat('2', 64),
                 'evidence://alpha/22', 'application/json', 2,
                 '2026-09-01T00:00:03Z');
            """
        )
        mismatched_evidence = self.psql(
            f"""
            INSERT INTO agent_economy.observations
                (namespace_id, chain_scope, source_id, observation_id, observed_at,
                 parser_version, evidence_id, provenance_id, observation_hash, protocol)
            VALUES
                ('{namespace}', 'base', 'rpc-primary', 'sha256:{'9' * 64}',
                 '2026-09-01T00:00:03Z', 'mpp-adapter@1', 'evidence:sha256:22',
                 '{provenance}', repeat('9', 64), 'mpp');
            """,
            check=False,
        )
        self.assertNotEqual(0, mismatched_evidence.returncode)

        mismatched_chain = self.psql(
            f"""
            INSERT INTO agent_economy.settlements
                (namespace_id, chain_scope, settlement_id, protocol, canonical_event_id,
                 source_id, asset, amount_atomic, settled_at, provenance_id)
            VALUES
                ('{namespace}', 'ethereum', 'settlement:ethereum:{'a' * 64}',
                 'x402', '{event_id}', 'rpc-primary', 'USDC', 1000,
                 '2026-09-01T00:01:00Z', '{provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, mismatched_chain.returncode)

        self.psql(
            f"""
            INSERT INTO agent_economy.buyer_handles
                (namespace_id, buyer_handle_id, handle_kind, chain_scope,
                 handle_value, provenance_id)
            VALUES
                ('{namespace}', 'buyer:ethereum:0x02', 'wallet', 'ethereum',
                 '0x02', '{provenance}');
            """
        )
        mismatched_buyer_chain = self.psql(
            f"""
            INSERT INTO agent_economy.settlements
                (namespace_id, chain_scope, settlement_id, protocol, canonical_event_id,
                 source_id, buyer_handle_id, asset, amount_atomic, settled_at, provenance_id)
            VALUES
                ('{namespace}', 'base', 'settlement:base:{'9' * 64}', 'x402',
                 '{event_id}', 'rpc-primary', 'buyer:ethereum:0x02', 'USDC', 1000,
                 '2026-09-01T00:01:00Z', '{provenance}');
            """,
            check=False,
        )
        self.assertNotEqual(0, mismatched_buyer_chain.returncode)

        self.psql(
            f"""
            INSERT INTO agent_economy.event_finality_updates
                (namespace_id, protocol, chain_scope, canonical_event_id, finality_sequence,
                 finality_status, asserted_at, source_id, provenance_id)
            VALUES
                ('{namespace}', 'x402', 'base', '{event_id}', 2, 'orphaned',
                 '2026-09-01T00:03:00Z', 'rpc-primary', '{provenance}');
            """
        )
        finality = self.psql(
            f"""
            SELECT finality_status || ':' || finality_sequence
            FROM agent_economy.current_event_finality
            WHERE namespace_id = '{namespace}' AND canonical_event_id = '{event_id}';
            SELECT count(*) FROM agent_economy.event_finality_updates
            WHERE namespace_id = '{namespace}' AND canonical_event_id = '{event_id}';
            """
        ).stdout.splitlines()
        self.assertEqual(["orphaned:2", "2"], finality)
        observation_plan = self.psql(
            f"""
            EXPLAIN (COSTS OFF)
            SELECT observation_id FROM agent_economy.observations
            WHERE namespace_id = '{namespace}' AND chain_scope = 'base'
              AND source_id = 'rpc-primary'
              AND observed_at >= '2026-09-01T00:00:00Z'
              AND observed_at < '2026-09-02T00:00:00Z';
            """
        ).stdout
        touched_partitions = set(re.findall(r"observations_p[0-7]", observation_plan))
        self.assertEqual(1, len(touched_partitions), observation_plan)

        feature_plan = self.psql(
            f"""
            SET enable_seqscan = off;
            EXPLAIN (COSTS OFF)
            SELECT feature_value FROM agent_economy.feature_windows
            WHERE namespace_id = '{namespace}' AND buyer_handle_id = '{buyer}'
              AND feature_name = 'payment_count'
              AND window_end > '2026-09-01T00:00:00Z'
            ORDER BY window_end DESC LIMIT 1;
            """
        ).stdout
        self.assertIn("feature_windows_buyer_window_idx", feature_plan)

        self.psql("REFRESH MATERIALIZED VIEW agent_economy.pulse_hourly;")
        self.assertEqual(
            "0",
            self.psql(
                f"SELECT count(*) FROM agent_economy.pulse_hourly "
                f"WHERE namespace_id = '{namespace}';"
            ).stdout.strip(),
        )

        self.psql(
            f"""
            INSERT INTO agent_economy.analytics_daily_metrics
                (namespace_id, measured_on, p95_query_latency_ms,
                 canonical_event_count, analytics_cpu_percent, materialized_views_tuned)
            SELECT '{namespace}', day::date, 2501,
                   CASE WHEN day::date = '2026-09-14'::date THEN 100000001 ELSE 1 END,
                   10, true
            FROM generate_series('2026-09-01'::date, '2026-09-14'::date, interval '1 day') day;
            """
        )
        readiness = self.psql(
            f"""
            SELECT latency_days || ':' || scale_days || ':' || ready
            FROM agent_economy.bigquery_projection_readiness
            WHERE namespace_id = '{namespace}'
            ORDER BY measured_on DESC LIMIT 1;
            """
        ).stdout.strip()
        self.assertEqual("14:1:true", readiness)
        truncate = self.psql(
            "TRUNCATE TABLE agent_economy.analytics_daily_metrics;",
            check=False,
        )
        self.assertNotEqual(0, truncate.returncode)
        self.assertIn("canonical evidence and knowledge rows are immutable", truncate.stderr)
        self.assertEqual(
            "14",
            self.psql(
                "SELECT count(*) FROM agent_economy.analytics_daily_metrics;"
            ).stdout.strip(),
        )
        partition_truncate = self.psql(
            "TRUNCATE TABLE agent_economy.settlements_p0;",
            check=False,
        )
        self.assertNotEqual(0, partition_truncate.returncode)
        self.assertIn(
            "canonical evidence and knowledge rows are immutable",
            partition_truncate.stderr,
        )

        rollback = self.psql(DOWN.read_text(encoding="utf-8"), check=False)
        self.assertNotEqual(0, rollback.returncode)
        self.assertIn(
            "cannot roll back non-empty operational and analytics schema", rollback.stderr
        )


if __name__ == "__main__":
    unittest.main()
