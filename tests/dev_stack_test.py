import json
import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DevStackLauncherTest(unittest.TestCase):
    def test_one_command_bootstraps_secrets_and_starts_stack(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            bin_directory = temporary_path / "bin"
            bin_directory.mkdir()
            docker_log = temporary_path / "docker.log"
            docker_stub = bin_directory / "docker"
            docker_stub.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$DOCKER_LOG\"\n",
                encoding="utf-8",
            )
            docker_stub.chmod(0o755)
            env_file = temporary_path / "dev-stack.env"
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(env_file),
                    "DOCKER_LOG": str(docker_log),
                    "PATH": f"{bin_directory}{os.pathsep}{environment['PATH']}",
                }
            )

            subprocess.run(
                [str(ROOT / "scripts" / "dev-stack")],
                cwd=ROOT,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertEqual(stat.S_IMODE(env_file.stat().st_mode), 0o600)
            values = dict(
                line.split("=", 1)
                for line in env_file.read_text(encoding="utf-8").splitlines()
            )
            for key in (
                "POSTGRES_PASSWORD",
                "CLICKHOUSE_PASSWORD",
                "AWS_ACCESS_KEY_ID",
                "AWS_SECRET_ACCESS_KEY",
            ):
                self.assertGreaterEqual(len(values[key]), 24)

            invocation = docker_log.read_text(encoding="utf-8")
            self.assertIn("compose version", invocation)
            self.assertIn(f"--env-file {env_file}", invocation)
            self.assertIn("up --detach --wait", invocation)

    def test_existing_credentials_are_restricted_before_use(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            bin_directory = temporary_path / "bin"
            bin_directory.mkdir()
            docker_stub = bin_directory / "docker"
            docker_stub.write_text(
                "#!/usr/bin/env python3\n"
                "import os\n"
                "import stat\n"
                "import sys\n"
                "if '--env-file' not in sys.argv:\n"
                "    sys.exit(0)\n"
                "mode = stat.S_IMODE(os.stat(os.environ['DEV_STACK_ENV_FILE']).st_mode)\n"
                "sys.exit(0 if mode == 0o600 else 1)\n",
                encoding="utf-8",
            )
            docker_stub.chmod(0o755)
            env_file = temporary_path / "dev-stack.env"
            env_file.write_text("POSTGRES_PASSWORD=existing\n", encoding="utf-8")
            env_file.chmod(0o644)
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(env_file),
                    "PATH": f"{bin_directory}{os.pathsep}{environment['PATH']}",
                }
            )

            subprocess.run(
                [str(ROOT / "scripts" / "dev-stack"), "status"],
                cwd=ROOT,
                env=environment,
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertEqual(stat.S_IMODE(env_file.stat().st_mode), 0o600)

    def test_secret_generation_failure_does_not_create_credentials(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            temporary_path = Path(temporary_directory)
            bin_directory = temporary_path / "bin"
            bin_directory.mkdir()
            for command, source in (
                ("docker", "#!/bin/sh\nexit 0\n"),
                ("openssl", "#!/bin/sh\nexit 1\n"),
            ):
                stub = bin_directory / command
                stub.write_text(source, encoding="utf-8")
                stub.chmod(0o755)
            env_file = temporary_path / "dev-stack.env"
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(env_file),
                    "PATH": f"{bin_directory}{os.pathsep}{environment['PATH']}",
                }
            )

            result = subprocess.run(
                [str(ROOT / "scripts" / "dev-stack")],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(result.returncode, 0)
            self.assertFalse(env_file.exists())


class DevStackComposeTest(unittest.TestCase):
    def test_dependencies_have_health_checks_and_persistent_storage(self):
        compose_file = ROOT / "compose.yaml"
        source = compose_file.read_text(encoding="utf-8")
        for placeholder in (
            "${POSTGRES_PASSWORD:?",
            "${CLICKHOUSE_PASSWORD:?",
            "${AWS_ACCESS_KEY_ID:?",
            "${AWS_SECRET_ACCESS_KEY:?",
        ):
            self.assertIn(placeholder, source)

        with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8") as env_file:
            env_file.write(
                "POSTGRES_PASSWORD=test-postgres-secret\n"
                "CLICKHOUSE_PASSWORD=test-clickhouse-secret\n"
                "AWS_ACCESS_KEY_ID=test-access-key\n"
                "AWS_SECRET_ACCESS_KEY=test-secret-key\n"
            )
            env_file.flush()
            result = subprocess.run(
                [
                    "docker",
                    "compose",
                    "--env-file",
                    env_file.name,
                    "-f",
                    str(compose_file),
                    "config",
                    "--format",
                    "json",
                ],
                cwd=ROOT,
                check=True,
                capture_output=True,
                text=True,
            )

        configuration = json.loads(result.stdout)
        services = configuration["services"]
        self.assertEqual(
            set(services), {"postgres", "clickhouse", "redpanda", "evidence"}
        )
        expected_volumes = {
            "postgres": "postgres-data",
            "clickhouse": "clickhouse-data",
            "redpanda": "redpanda-data",
            "evidence": "evidence-data",
        }
        for service_name, volume_name in expected_volumes.items():
            service = services[service_name]
            self.assertIn("healthcheck", service, service_name)
            self.assertTrue(service["healthcheck"]["test"], service_name)
            self.assertTrue(service["ports"], service_name)
            self.assertTrue(
                all(port.get("host_ip") == "127.0.0.1" for port in service["ports"]),
                service_name,
            )
            self.assertTrue(
                any(
                    volume.get("source") == volume_name
                    and volume.get("type") == "volume"
                    for volume in service["volumes"]
                ),
                service_name,
            )

        redpanda_command = " ".join(services["redpanda"]["command"])
        self.assertIn("--schema-registry-addr", redpanda_command)
        self.assertNotIn("--advertise-schema-registry-addr", redpanda_command)
        redpanda_healthcheck = " ".join(services["redpanda"]["healthcheck"]["test"])
        self.assertIn("-X brokers=127.0.0.1:9092", redpanda_healthcheck)
        self.assertIn("http://127.0.0.1:8081/subjects", redpanda_healthcheck)
        self.assertNotIn("--brokers", redpanda_healthcheck)

        evidence = services["evidence"]
        self.assertEqual(evidence["image"], "chrislusf/seaweedfs:4.47")
        evidence_command = " ".join(evidence["command"])
        self.assertIn("server", evidence_command)
        self.assertIn("-s3", evidence_command)
        self.assertTrue(
            any(volume.get("target") == "/data" for volume in evidence["volumes"])
        )
        evidence_healthcheck = " ".join(evidence["healthcheck"]["test"])
        self.assertIn("http://127.0.0.1:8333/", evidence_healthcheck)
        self.assertNotIn("9333", evidence_healthcheck)


class DevStackRepositoryGateTest(unittest.TestCase):
    def test_canonical_verify_runs_dev_stack_tests(self):
        verify_script = (ROOT / "scripts" / "verify").read_text(encoding="utf-8")
        self.assertIn("python3 -m unittest discover", verify_script)

    def test_readme_exposes_the_single_start_command(self):
        readme = (ROOT / "README.md").read_text(encoding="utf-8")
        self.assertIn("./scripts/dev-stack", readme)
        self.assertIn("./scripts/dev-stack reset", readme)

    def test_generated_credentials_are_git_ignored(self):
        result = subprocess.run(
            ["git", "check-ignore", ".dev-stack.env"],
            cwd=ROOT,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
