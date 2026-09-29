import pathlib
import re
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0008_query_read_models.up.sql"
DOWN = ROOT / "migrations" / "0008_query_read_models.down.sql"


class QueryReadModelsMigrationTest(unittest.TestCase):
    def test_read_models_are_purpose_built_and_provenance_bearing(self) -> None:
        sql = UP.read_text()
        for view in ("dashboard_facts", "dashboard_pulse", "dashboard_system"):
            self.assertIn(f"CREATE VIEW agent_economy.{view}", sql)
        self.assertGreaterEqual(len(re.findall(r"provenance_id", sql)), 8)
        self.assertNotIn("SECURITY DEFINER", sql)
        self.assertNotRegex(sql, r"(?i)create\s+(?:or\s+replace\s+)?function")
        self.assertIn("provenance_ids", sql)
        self.assertIn("array_agg(DISTINCT settlement.provenance_id::text", sql)
        self.assertIn("array_agg(DISTINCT observation.provenance_id::text", sql)
        pulse_group = sql.split("CREATE VIEW agent_economy.dashboard_pulse AS", 1)[1].split(
            "CREATE VIEW agent_economy.dashboard_system AS", 1
        )[0]
        system_group = sql.split("CREATE VIEW agent_economy.dashboard_system AS", 1)[1]
        self.assertNotRegex(pulse_group, r"GROUP BY[\s\S]*settlement\.provenance_id")
        self.assertNotRegex(system_group, r"GROUP BY[\s\S]*observation\.provenance_id")

    def test_graph_reads_the_versioned_attribution_projection(self) -> None:
        query = (ROOT / "src" / "query.rs").read_text()
        self.assertIn("attribution_candidates", query)
        self.assertIn("attribution_run_seals", query)
        self.assertIn("settlement.chain_scope = candidate.chain_scope", query)
        self.assertIn("settlement.settlement_id = candidate.settlement_id", query)
        self.assertNotIn("agent_economy.attribution_edges AS edge", query)

    def test_down_migration_removes_read_models_in_reverse_order(self) -> None:
        self.assertEqual(
            DOWN.read_text().splitlines(),
            [
                "BEGIN;",
                "",
                "DROP VIEW agent_economy.dashboard_system;",
                "DROP VIEW agent_economy.dashboard_pulse;",
                "DROP VIEW agent_economy.dashboard_facts;",
                "",
                "COMMIT;",
            ],
        )

    def test_runtime_wires_versioned_router_to_postgres(self) -> None:
        main = (ROOT / "src" / "main.rs").read_text()
        self.assertIn("PostgresQueryStore", main)
        self.assertIn("DATABASE_URL", main)
        self.assertIn("NAMESPACE_ID", main)
        self.assertIn("api_router", main)


if __name__ == "__main__":
    unittest.main()
