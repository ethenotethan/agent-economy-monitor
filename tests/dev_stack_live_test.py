import os
import socket
import subprocess
import tempfile
import unittest
import uuid
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(
    os.environ.get("RUN_DEV_STACK_LIVE") == "1",
    "set RUN_DEV_STACK_LIVE=1 for Docker restart qualification",
)
class DevStackLiveTest(unittest.TestCase):
    def test_postgresql_is_healthy_and_preserves_rows_across_down_up_restart(self):
        with socket.socket() as listener:
            listener.bind(("127.0.0.1", 0))
            port = listener.getsockname()[1]
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory).resolve()
            environment = os.environ.copy()
            environment.update(
                {
                    "COMPOSE_PROJECT_NAME": f"aem-test-{uuid.uuid4().hex[:12]}",
                    "DEV_STACK_ENV_FILE": str(temporary / "dev.env"),
                    "EVIDENCE_DIRECTORY": str(temporary / "evidence"),
                    "POSTGRES_PORT": str(port),
                }
            )

            def run(*command):
                return subprocess.run(
                    command,
                    cwd=ROOT,
                    env=environment,
                    check=True,
                    capture_output=True,
                    text=True,
                )

            try:
                run(str(ROOT / "scripts" / "dev-stack"))
                run(
                    "docker",
                    "compose",
                    "--env-file",
                    environment["DEV_STACK_ENV_FILE"],
                    "-f",
                    str(ROOT / "compose.yaml"),
                    "exec",
                    "-T",
                    "postgres",
                    "psql",
                    "-U",
                    "agent_economy",
                    "-d",
                    "agent_economy",
                    "-v",
                    "ON_ERROR_STOP=1",
                    "-c",
                    "CREATE TABLE restart_probe (value text PRIMARY KEY);",
                    "-c",
                    "INSERT INTO restart_probe VALUES ('survived');",
                )
                run(str(ROOT / "scripts" / "dev-stack"), "down")
                run(str(ROOT / "scripts" / "dev-stack"))
                replay = run(
                    "docker",
                    "compose",
                    "--env-file",
                    environment["DEV_STACK_ENV_FILE"],
                    "-f",
                    str(ROOT / "compose.yaml"),
                    "exec",
                    "-T",
                    "postgres",
                    "psql",
                    "-U",
                    "agent_economy",
                    "-d",
                    "agent_economy",
                    "-Atqc",
                    "SELECT value FROM restart_probe;",
                )
                services = run(
                    "docker",
                    "compose",
                    "--env-file",
                    environment["DEV_STACK_ENV_FILE"],
                    "-f",
                    str(ROOT / "compose.yaml"),
                    "config",
                    "--services",
                )

                self.assertEqual("survived", replay.stdout.strip())
                self.assertEqual("postgres", services.stdout.strip())
                self.assertTrue(environment["EVIDENCE_DIRECTORY"])
                self.assertTrue(Path(environment["EVIDENCE_DIRECTORY"]).is_dir())
            finally:
                subprocess.run(
                    [str(ROOT / "scripts" / "dev-stack"), "reset"],
                    cwd=ROOT,
                    env=environment,
                    capture_output=True,
                    text=True,
                )


if __name__ == "__main__":
    unittest.main()
