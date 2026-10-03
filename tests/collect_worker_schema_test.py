import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0013_collect_worker.up.sql"
DOWN = ROOT / "migrations" / "0013_collect_worker.down.sql"
SPLIT_UP = ROOT / "migrations" / "0014_collection_custody_split.up.sql"
SPLIT_DOWN = ROOT / "migrations" / "0014_collection_custody_split.down.sql"


class CollectWorkerMigrationTest(unittest.TestCase):
    def test_cursor_and_atomic_commit_contract_are_owned_by_migration(self):
        up = UP.read_text()
        self.assertIn("CREATE TABLE agent_economy.collection_cursors", up)
        self.assertIn("CREATE TABLE agent_economy.collection_range_receipts", up)
        self.assertIn("CREATE FUNCTION agent_economy.commit_collection_batch", up)
        self.assertIn("FOR UPDATE", up)
        self.assertIn("agent_economy.evidence_objects", up)
        self.assertIn("agent_economy.provenance_records", up)
        self.assertIn("agent_economy.observations", up)
        self.assertIn("CREATE ROLE agent_economy_collector_runtime LOGIN NOINHERIT", up)
        self.assertIn(") TO agent_economy_collector_runtime;", up)
        self.assertNotIn(") TO agent_economy_worker;", up)
        self.assertNotIn("GRANT INSERT ON agent_economy.observations", up)
        self.assertNotIn("request_body", up.lower())
        self.assertNotIn("payment_signature", up.lower())

    def test_rollback_refuses_to_discard_live_cursor_state(self):
        down = DOWN.read_text()
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP TABLE"))
        self.assertNotIn("CASCADE", down)

    def test_security_definer_cross_binds_every_canonical_row(self):
        up = UP.read_text()
        self.assertIn("jsonb_object_keys(item)", up)
        self.assertIn("'evidence:sha256:' || (item ->> 'sha256')", up)
        self.assertIn("application/vnd.oai.openapi+json", up)
        self.assertIn("invalid collection evidence", up)
        self.assertIn("invalid collection observation", up)
        self.assertIn("contradictory collection evidence", up)
        self.assertIn("contradictory collection observation", up)
        self.assertIn("job.input_sha256 = p_input_sha256", up)
        self.assertIn("count(DISTINCT (item ->> 'height')::bigint)", up)
        self.assertIn("p_end_height - p_start_height + 1", up)
        self.assertIn("batch_sha256", up)
        self.assertIn("evidence_json jsonb", up)
        self.assertIn("observations_json jsonb", up)
        self.assertIn("existing_receipt.evidence_json <> p_evidence_json", up)
        self.assertIn("existing_receipt.observations_json <> p_observations_json", up)
        self.assertIn("collection replay differs from immutable receipt", up)
        self.assertIn("collection_chain_scope text", up)
        self.assertIn("collection_source_id text", up)
        self.assertIn("collection_start_height bigint", up)
        self.assertIn("collection_end_height bigint", up)
        self.assertIn("collection_acquisition_contract text", up)
        self.assertIn("collection_evidence_contract text", up)
        self.assertIn("job.collection_chain_scope = p_chain_scope", up)
        self.assertIn("job.collection_source_id = p_source_id", up)
        self.assertIn("job.collection_start_height = p_start_height", up)
        self.assertIn("job.collection_end_height = p_end_height", up)
        self.assertNotIn("'collection-manifest'", up)

    def test_dedicated_runtime_role_is_fail_closed(self):
        up = UP.read_text()
        self.assertIn("agent_economy_collector_runtime must be an unprivileged LOGIN role", up)
        self.assertIn("agent_economy_collector_runtime must not inherit roles", up)
        self.assertIn(
            "agent_economy_collector_runtime must not have members",
            up,
        )
        self.assertNotIn("GRANT agent_economy_collector_runtime TO CURRENT_USER", up)
        self.assertIn("CREATE FUNCTION agent_economy.claim_collection_job", up)
        self.assertIn("CREATE FUNCTION agent_economy.complete_collection_job", up)
        self.assertIn("CREATE FUNCTION agent_economy.fail_collection_job", up)
        self.assertIn("REVOKE ALL ON agent_economy.collection_range_receipts FROM PUBLIC", up)
        self.assertIn(
            "REVOKE ALL ON agent_economy.collection_range_receipts FROM agent_economy_worker",
            up,
        )

    def test_collection_custody_is_split_by_login_and_staged_handoff(self):
        up = SPLIT_UP.read_text()
        self.assertIn("CREATE TABLE agent_economy.collection_runtime_namespaces", up)
        self.assertIn("session_user", up)
        self.assertIn("CREATE TABLE agent_economy.pending_collection_batches", up)
        self.assertIn("CREATE FUNCTION agent_economy.stage_collection_batch", up)
        self.assertIn("CREATE FUNCTION agent_economy.claim_pending_collection_batch", up)
        self.assertIn("CREATE FUNCTION agent_economy.promote_pending_collection_batch", up)
        self.assertIn("DROP TABLE agent_economy.collection_evidence_attestations", up)
        self.assertIn("TO agent_economy_collector_runtime", up)
        self.assertIn("TO agent_economy_evidence_verifier_runtime", up)
        self.assertNotIn("GRANT agent_economy_collector_runtime TO", up)
        self.assertNotIn("GRANT agent_economy_evidence_verifier_runtime TO", up)

    def test_custody_split_rollback_refuses_live_pending_state(self):
        down = SPLIT_DOWN.read_text()
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP TABLE"))
        self.assertNotIn("CASCADE", down)


if __name__ == "__main__":
    unittest.main()
