import pathlib
import unittest

ROOT = pathlib.Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0011_semantic_projection.up.sql"
DOWN = ROOT / "migrations" / "0011_semantic_projection.down.sql"


class SemanticProjectionMigrationTest(unittest.TestCase):
    def test_projection_ledger_binds_snapshot_approval_and_read_only_mirror(self):
        sql = UP.read_text()
        for table in (
            "projection_snapshots",
            "projection_jobs",
            "projection_approvals",
            "projection_publications",
        ):
            self.assertIn(f"CREATE TABLE agent_economy.{table}", sql)

        self.assertIn("CREATE VIEW agent_economy.projection_mirror_pages", sql)
        self.assertIn("approved_payload jsonb NOT NULL", sql)
        self.assertIn("approved_payload_bytes bytea NOT NULL", sql)
        self.assertIn("encode(sha256(approved_payload_bytes), 'hex')", sql)
        self.assertIn(
            "approved_payload = convert_from(approved_payload_bytes, 'UTF8')::jsonb",
            sql,
        )
        self.assertIn("payload_sha256 text NOT NULL", sql)
        self.assertIn("bundle_sha256 text NOT NULL", sql)
        self.assertIn("bundle_bytes bytea NOT NULL", sql)
        self.assertIn("encode(sha256(bundle_bytes), 'hex')", sql)
        self.assertIn("candidate_sha256", sql)
        self.assertIn("page_revision_id", sql)
        self.assertIn("page_sha256", sql)
        self.assertIn("changeset_sha256", sql)
        self.assertIn("snapshot_sha256", sql)
        self.assertIn("snapshot_payload jsonb NOT NULL", sql)
        self.assertIn("model_sha256", sql)
        self.assertIn("prompt_sha256", sql)
        self.assertIn("output_sha256", sql)
        self.assertIn("wiki_id text NOT NULL DEFAULT 'agentic-commerce'", sql)
        self.assertIn("destination ~ '^gcs://agent-economy-projections/'", sql)
        self.assertIn("immutable_projection_row", sql)
        self.assertIn("validate_projection_approval", sql)
        self.assertIn("validate_projection_publication", sql)
        self.assertIn("protect_projection_job_identity", sql)
        self.assertIn("approved_payload ->> 'page_path'", sql)
        self.assertNotIn("job.page_path,", sql.split("CREATE VIEW", 1)[1])
        self.assertIn("CREATE ROLE agent_economy_projection_writer NOLOGIN", sql)
        self.assertIn("CREATE ROLE agent_economy_dashboard_reader NOLOGIN", sql)
        self.assertIn("agent_economy.projection_mirror_pages", sql)
        self.assertIn("TO agent_economy_dashboard_reader", sql)
        self.assertIn("REVOKE ALL ON agent_economy.projection_mirror_pages FROM PUBLIC", sql)
        self.assertIn("BEFORE UPDATE OR DELETE", sql)
        self.assertIn("FOREIGN KEY (namespace_id, snapshot_id, stable_entity_id, snapshot_sha256)", sql)
        self.assertIn("FOREIGN KEY (namespace_id, job_id, payload_sha256)", sql)
        self.assertIn("approved_payload ?& ARRAY[", sql)
        self.assertIn("IS DISTINCT FROM", sql)
        self.assertIn("|| approval.approved_payload_bytes", sql)
        self.assertIn("IN NEW.bundle_bytes", sql)
        self.assertIn("CREATE ROLE agent_economy_projection_approver NOLOGIN", sql)
        self.assertIn(
            "GRANT INSERT ON agent_economy.projection_approvals TO agent_economy_projection_approver",
            sql,
        )
        self.assertNotIn(
            "GRANT INSERT ON agent_economy.projection_approvals TO agent_economy_projection_writer",
            sql,
        )
        self.assertIn("dashboard_facts,", sql)
        self.assertIn("dashboard_pulse,", sql)
        self.assertIn("dashboard_system,", sql)
        for relation in (
            "namespaces",
            "attribution_runs",
            "attribution_run_seals",
            "attribution_candidates",
            "settlements",
            "payment_requirements",
            "provenance_records",
            "evidence_objects",
        ):
            self.assertIn(f"agent_economy.{relation}", sql)
        self.assertIn("GRANT agent_economy_dashboard_reader TO CURRENT_USER", sql)
        self.assertIn("GRANT agent_economy_projection_writer TO CURRENT_USER", sql)
        self.assertIn(
            "GRANT SELECT ON agent_economy.projection_publications TO agent_economy_projection_writer",
            sql,
        )

    def test_down_migration_fails_closed_before_removing_projection_history(self):
        sql = DOWN.read_text()
        self.assertIn("projection rollback blocked: projection history exists", sql)
        self.assertIn("DROP VIEW agent_economy.projection_mirror_pages", sql)
        positions = [
            sql.index("DROP VIEW agent_economy.projection_mirror_pages"),
            sql.index("DROP TABLE agent_economy.projection_publications"),
            sql.index("DROP TABLE agent_economy.projection_approvals"),
            sql.index("DROP TABLE agent_economy.projection_jobs"),
            sql.index("DROP TABLE agent_economy.projection_snapshots"),
        ]
        self.assertEqual(positions, sorted(positions))


if __name__ == "__main__":
    unittest.main()
