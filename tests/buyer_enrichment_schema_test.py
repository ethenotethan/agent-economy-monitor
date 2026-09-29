import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0004_buyer_enrichment.up.sql"
DOWN = ROOT / "migrations" / "0004_buyer_enrichment.down.sql"


class BuyerEnrichmentMigrationContractTest(unittest.TestCase):
    def test_migration_persists_chain_scoped_cursor_budget_and_finalized_cache(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        self.assertTrue(up.rstrip().endswith("COMMIT;"))
        self.assertIn("CREATE TABLE agent_economy.buyer_enrichment_cursors (", up)
        self.assertIn(
            "UNIQUE (namespace_id, buyer_handle_id, chain_scope, handle_value)", up
        )
        self.assertIn("PRIMARY KEY (namespace_id, buyer_handle_id, chain_scope)", up)
        self.assertIn("cursor text", up)
        self.assertIn("handle_value text NOT NULL", up)
        self.assertIn("version bigint NOT NULL DEFAULT 0", up)
        self.assertIn("requests_used_total bigint NOT NULL DEFAULT 0", up)
        self.assertIn("last_run_budget bigint NOT NULL DEFAULT 0", up)
        self.assertIn("last_run_requests_used bigint NOT NULL DEFAULT 0", up)
        self.assertIn("reservation_owner text", up)
        self.assertIn("reservation_expires_at timestamptz", up)
        self.assertIn("CREATE TABLE agent_economy.buyer_finalized_history (", up)
        self.assertIn("CREATE TABLE agent_economy.buyer_finalized_history_evidence (", up)
        self.assertIn("finality text NOT NULL DEFAULT 'finalized' CHECK (finality = 'finalized')", up)
        self.assertIn("evidence_id text NOT NULL", up)
        self.assertNotIn("protocol text", up)
        self.assertIn("CREATE TRIGGER buyer_finalized_history_immutable", up)
        self.assertIn("CREATE TRIGGER buyer_finalized_history_truncate_immutable", up)
        self.assertIn("CREATE TRIGGER buyer_finalized_history_evidence_immutable", up)

    def test_down_migration_fails_closed_before_dropping_cached_history(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("cannot roll back non-empty buyer enrichment state", down)
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP TABLE"))
        self.assertIn("LOCK TABLE agent_economy.buyer_finalized_history", down)
        self.assertIn("IN ACCESS EXCLUSIVE MODE", down)
        self.assertIn("DROP CONSTRAINT buyer_handles_enrichment_chain_key", down)
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


if __name__ == "__main__":
    unittest.main()
