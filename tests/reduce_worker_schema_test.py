import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0016_reduce_worker.up.sql"
DOWN = ROOT / "migrations" / "0016_reduce_worker.down.sql"


class ReduceWorkerMigrationContractTest(unittest.TestCase):
    def test_forward_migration_binds_inputs_runtime_and_atomic_canonical_commit(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        self.assertIn("CREATE TABLE agent_economy.reducer_job_inputs", up)
        self.assertIn("CREATE TABLE agent_economy.reducer_checkpoints", up)
        self.assertIn("finality_json jsonb NOT NULL", up)
        self.assertIn("attributions_json jsonb NOT NULL", up)
        self.assertIn("CREATE TABLE agent_economy.reducer_runtime_namespaces", up)
        self.assertIn("CREATE ROLE agent_economy_reducer_runtime LOGIN NOINHERIT", up)
        self.assertIn("CREATE FUNCTION agent_economy.claim_reducer_job", up)
        self.assertIn("mode = 'reduce'", up)
        self.assertIn("job_kind = 'canonical-observation-range-v1'", up)
        self.assertIn("CREATE FUNCTION agent_economy.load_reducer_job_input", up)
        self.assertIn("CREATE FUNCTION agent_economy.commit_reduction_batch", up)
        self.assertIn("INSERT INTO agent_economy.canonical_events", up)
        self.assertIn("INSERT INTO agent_economy.canonical_event_observations", up)
        self.assertIn("existing_receipt.finality_json <> p_finality_json", up)
        self.assertIn("existing_receipt.attributions_json <> p_attributions_json", up)
        self.assertIn("reduction attribution output is not bound", up)
        self.assertIn("UPDATE agent_economy.reducer_checkpoints", up)
        self.assertIn("REVOKE ALL ON agent_economy.observations", up)
        self.assertIn("GRANT EXECUTE ON FUNCTION agent_economy.commit_reduction_batch", up)
        self.assertTrue(up.rstrip().endswith("COMMIT;"))

    def test_down_migration_refuses_to_discard_reduction_history(self):
        down = DOWN.read_text(encoding="utf-8")
        self.assertIn("cannot roll back reducer worker history", down)
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


if __name__ == "__main__":
    unittest.main()
