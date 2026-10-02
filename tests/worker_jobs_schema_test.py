import pathlib
import unittest


ROOT = pathlib.Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0012_worker_jobs.up.sql"
DOWN = ROOT / "migrations" / "0012_worker_jobs.down.sql"


class WorkerJobsMigrationContractTest(unittest.TestCase):
    def test_migration_defines_transactional_bounded_worker_leases(self):
        up = UP.read_text(encoding="utf-8")

        self.assertTrue(up.startswith("BEGIN;"))
        self.assertTrue(up.rstrip().endswith("COMMIT;"))
        for fragment in (
            "CREATE TABLE agent_economy.worker_jobs",
            "UNIQUE (namespace_id, idempotency_key)",
            "mode IN ('collect', 'reduce', 'classify', 'enrich')",
            "status IN ('pending', 'leased', 'retryable', 'succeeded', 'dead_letter', 'cancelled')",
            "input_sha256 text NOT NULL",
            "output_sha256 text",
            "max_attempts smallint NOT NULL",
            "lease_owner text",
            "lease_token uuid",
            "lease_expires_at timestamptz",
            "last_error_code text",
            "CREATE ROLE agent_economy_worker NOLOGIN",
            "CREATE FUNCTION agent_economy.claim_worker_job",
            "CREATE FUNCTION agent_economy.renew_worker_job_lease",
            "CREATE FUNCTION agent_economy.complete_worker_job",
            "CREATE FUNCTION agent_economy.fail_worker_job",
            "CREATE FUNCTION agent_economy.cancel_worker_job",
            "SECURITY DEFINER",
            "SET search_path = pg_catalog",
        ):
            with self.subTest(fragment=fragment):
                self.assertIn(fragment, up)
        self.assertNotIn("payload", up.lower())
        self.assertNotIn("request_body", up.lower())
        self.assertNotIn("GRANT UPDATE", up)
        self.assertNotIn("GRANT SELECT ON agent_economy.worker_jobs", up)
        self.assertIn(
            "REVOKE ALL ON agent_economy.worker_jobs FROM agent_economy_worker;",
            up,
        )
        self.assertIn("GRANT EXECUTE ON FUNCTION agent_economy.claim_worker_job", up)

    def test_down_migration_fails_closed_when_worker_history_exists(self):
        down = DOWN.read_text(encoding="utf-8")

        self.assertTrue(down.startswith("BEGIN;"))
        self.assertIn("worker job rollback blocked: worker history exists", down)
        self.assertLess(down.index("RAISE EXCEPTION"), down.index("DROP TABLE"))
        self.assertNotIn("CASCADE", down)
        self.assertTrue(down.rstrip().endswith("COMMIT;"))


if __name__ == "__main__":
    unittest.main()
