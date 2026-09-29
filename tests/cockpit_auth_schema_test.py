import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
UP = ROOT / "migrations" / "0009_cockpit_auth.up.sql"
DOWN = ROOT / "migrations" / "0009_cockpit_auth.down.sql"


class CockpitAuthMigrationContractTest(unittest.TestCase):
    def test_forward_migration_persists_only_secret_hashes_with_namespace_scope(self) -> None:
        sql = UP.read_text(encoding="utf-8")
        self.assertIn("CREATE TABLE agent_economy.auth_sessions", sql)
        self.assertIn("CREATE TABLE agent_economy.auth_login_limits", sql)
        self.assertIn("namespace_id uuid NOT NULL REFERENCES agent_economy.namespaces", sql)
        self.assertIn("token_hash text NOT NULL", sql)
        self.assertIn("csrf_hash text NOT NULL", sql)
        self.assertNotIn("password text", sql.lower())
        self.assertNotIn("session_token", sql.lower())
        self.assertIn("CHECK (expires_at > created_at)", sql)

    def test_reverse_migration_removes_auth_state(self) -> None:
        sql = DOWN.read_text(encoding="utf-8")
        self.assertIn("DROP TABLE agent_economy.auth_login_limits", sql)
        self.assertIn("DROP TABLE agent_economy.auth_sessions", sql)


if __name__ == "__main__":
    unittest.main()
