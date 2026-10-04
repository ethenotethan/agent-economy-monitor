import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0013_collect_worker.up.sql"
DOWN = ROOT / "migrations" / "0013_collect_worker.down.sql"
SPLIT_UP = ROOT / "migrations" / "0014_collection_custody_split.up.sql"
SPLIT_DOWN = ROOT / "migrations" / "0014_collection_custody_split.down.sql"
BOUNDARY_UP = ROOT / "migrations" / "0015_collection_runtime_boundary.up.sql"
BOUNDARY_DOWN = ROOT / "migrations" / "0015_collection_runtime_boundary.down.sql"


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

    def test_runtime_boundary_rejects_role_object_acl_and_public_drift(self):
        up = BOUNDARY_UP.read_text()
        self.assertIn("rolcanlogin", up)
        self.assertIn("pg_auth_members", up)
        self.assertIn("must not own database objects", up)
        self.assertIn("pg_shdepend", up)
        self.assertIn("has_table_privilege", up)
        self.assertIn("has_any_column_privilege", up)
        self.assertIn("has_sequence_privilege", up)
        self.assertIn("has_type_privilege", up)
        self.assertIn("current_database()", up)
        self.assertIn("actual_routines <> expected_routines", up)
        self.assertIn("acl.grantee=0", up)
        self.assertIn("pg_default_acl", up)
        self.assertIn("dangerous ambient collection authority", up)
        self.assertIn("REVOKE ALL ON FUNCTION", up)
        self.assertIn("agent_economy.bound_collection_namespace(text)", up)

    def test_runtime_boundary_down_restores_predecessor_contract(self):
        down = BOUNDARY_DOWN.read_text()
        self.assertIn(
            "CREATE OR REPLACE FUNCTION agent_economy.bound_collection_namespace", down
        )
        self.assertIn("REVOKE EXECUTE ON FUNCTION", down)
        self.assertNotIn("CASCADE", down)

    def test_canonical_verify_requires_and_executes_split_custody(self):
        verify = (ROOT / "scripts" / "verify").read_text()
        postgres_test = (ROOT / "tests" / "collect_worker_postgres.rs").read_text()
        self.assertIn("AEM_COLLECT_TEST_DATABASE_URL:?", verify)
        self.assertIn("cargo test --test collect_worker_postgres", verify)
        self.assertNotIn("AEM_LEGACY_COLLECT_TEST_DATABASE_URL", postgres_test)
        self.assertIn('.arg("collect")', postgres_test)
        self.assertIn('.arg("verify-evidence")', postgres_test)
        self.assertIn('"EVIDENCE_WRITE_ROOT"', postgres_test)
        self.assertIn('"EVIDENCE_READ_ROOT"', postgres_test)


if __name__ == "__main__":
    unittest.main()
