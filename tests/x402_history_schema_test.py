import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0021_x402_history_backfill.up.sql"
DOWN = ROOT / "migrations" / "0021_x402_history_backfill.down.sql"


class X402HistoryBackfillMigrationContractTest(unittest.TestCase):
    def test_forward_migration_tracks_honest_source_coverage_and_immutable_observations(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        self.assertIn("CREATE TABLE agent_economy.x402_history_sources", up)
        self.assertIn("CREATE TABLE agent_economy.x402_history_ranges", up)
        self.assertIn("CREATE TABLE agent_economy.x402_payment_observations", up)
        self.assertIn("CREATE TABLE agent_economy.x402_settlement_observation_links", up)
        self.assertIn("earliest_available_coordinate", up)
        self.assertIn("frozen_upper_coordinate", up)
        self.assertIn("indexed_through_coordinate", up)
        self.assertIn("retention_limit", up)
        self.assertIn("pagination_semantics", up)
        self.assertIn("attribution_strength", up)
        self.assertIn("source-complete", up)
        self.assertIn("chain-complete", up)
        self.assertIn("partial", up)
        self.assertIn("stale", up)
        self.assertIn("failed", up)
        self.assertIn("unknown", up)
        self.assertIn("explicit_attribution_digest", up)
        self.assertIn("rpc_verification_evidence_id", up)
        self.assertIn("payer_handle_id", up)
        self.assertIn("UNIQUE (namespace_id, chain_scope, transaction_reference, settlement_position, source_id)", up)
        self.assertIn("x402_payment_observations_immutable", up)
        self.assertIn("x402_settlement_observation_links_immutable", up)
        self.assertIn("CREATE VIEW agent_economy.x402_history_coverage", up)
        self.assertIn("CREATE FUNCTION agent_economy.admit_x402_history_source", up)
        self.assertIn("CREATE FUNCTION agent_economy.plan_x402_history_jobs", up)
        self.assertIn("CREATE FUNCTION agent_economy.reconcile_x402_history_range", up)
        self.assertIn("INSERT INTO agent_economy.canonical_events", up)
        self.assertIn("INSERT INTO agent_economy.settlements", up)
        self.assertIn("INSERT INTO agent_economy.x402_settlement_observation_links", up)
        self.assertIn("min(receipt.start_height)", up)
        self.assertIn("max(receipt.end_height)", up)
        self.assertIn("bool_and(receipt.start_height", up)
        self.assertNotIn("pending.start_height, pending.end_height, pending.end_height", up)
        self.assertNotIn("recipient_match", up)
        self.assertTrue(up.rstrip().endswith("COMMIT;"))

    def test_down_migration_refuses_to_delete_backfill_evidence(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("cannot roll back x402 history with retained observations", down)
        self.assertLess(down.index("LOCK TABLE"), down.index("IF EXISTS"))
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


if __name__ == "__main__":
    unittest.main()
