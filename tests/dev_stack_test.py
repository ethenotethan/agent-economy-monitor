import os
import stat
import subprocess
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class DevStackLauncherTest(unittest.TestCase):
    def test_one_command_initializes_evidence_and_starts_postgresql(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory).resolve()
            bin_directory = temporary / "bin"
            bin_directory.mkdir()
            docker_log = temporary / "docker.log"
            docker = bin_directory / "docker"
            docker.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$DOCKER_LOG\"\n",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            env_file = temporary / "dev-stack.env"
            evidence_directory = temporary / "evidence"
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(env_file),
                    "EVIDENCE_DIRECTORY": str(evidence_directory),
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

            self.assertTrue(evidence_directory.is_dir())
            self.assertEqual(0o700, stat.S_IMODE(evidence_directory.stat().st_mode))
            self.assertEqual(0o600, stat.S_IMODE(env_file.stat().st_mode))
            values = dict(
                line.split("=", 1)
                for line in env_file.read_text(encoding="utf-8").splitlines()
            )
            self.assertGreaterEqual(len(values["POSTGRES_PASSWORD"]), 24)
            invocation = docker_log.read_text(encoding="utf-8")
            self.assertIn("compose version", invocation)
            self.assertIn(f"--env-file {env_file}", invocation)
            self.assertIn("up --detach --wait", invocation)

    def test_existing_credentials_are_restricted_before_docker_uses_them(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory).resolve()
            bin_directory = temporary / "bin"
            bin_directory.mkdir()
            env_file = temporary / "dev-stack.env"
            env_file.write_text("POSTGRES_PASSWORD=existing\n", encoding="utf-8")
            env_file.chmod(0o644)
            docker = bin_directory / "docker"
            docker.write_text(
                "#!/bin/sh\n"
                "if [ \"$1\" = compose ] && [ \"$2\" != version ]; then\n"
                "  mode=$(stat -c '%a' \"$DEV_STACK_ENV_FILE\" 2>/dev/null || stat -f '%Lp' \"$DEV_STACK_ENV_FILE\")\n"
                "  [ \"$mode\" = 600 ] || exit 42\n"
                "fi\n",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(env_file),
                    "EVIDENCE_DIRECTORY": str(temporary / "evidence"),
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

            self.assertEqual(0o600, stat.S_IMODE(env_file.stat().st_mode))

    def test_ambient_postgres_password_cannot_override_generated_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory).resolve()
            bin_directory = temporary / "bin"
            bin_directory.mkdir()
            docker_log = temporary / "docker.log"
            docker = bin_directory / "docker"
            docker.write_text(
                "#!/bin/sh\n"
                "printf '%s:%s\\n' \"${POSTGRES_PASSWORD-unset}\" \"$*\" >> \"$DOCKER_LOG\"\n",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            env_file = temporary / "dev-stack.env"
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(env_file),
                    "EVIDENCE_DIRECTORY": str(temporary / "evidence"),
                    "DOCKER_LOG": str(docker_log),
                    "POSTGRES_PASSWORD": "ambient-must-not-win",
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

            invocations = docker_log.read_text(encoding="utf-8").splitlines()
            self.assertGreaterEqual(len(invocations), 2)
            self.assertTrue(all(line.startswith("unset:") for line in invocations))
            generated = env_file.read_text(encoding="utf-8").split("=", 1)[1].strip()
            self.assertNotEqual("ambient-must-not-win", generated)

    def test_existing_credential_symlink_is_rejected_before_docker_uses_it(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory).resolve()
            bin_directory = temporary / "bin"
            bin_directory.mkdir()
            docker_log = temporary / "docker.log"
            docker = bin_directory / "docker"
            docker.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$DOCKER_LOG\"\n",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            credential_target = temporary / "outside.env"
            credential_target.write_text("POSTGRES_PASSWORD=outside\n", encoding="utf-8")
            credential_target.chmod(0o644)
            env_file = temporary / "dev-stack.env"
            env_file.symlink_to(credential_target)
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(env_file),
                    "EVIDENCE_DIRECTORY": str(temporary / "evidence"),
                    "DOCKER_LOG": str(docker_log),
                    "PATH": f"{bin_directory}{os.pathsep}{environment['PATH']}",
                }
            )

            result = subprocess.run(
                [str(ROOT / "scripts" / "dev-stack"), "status"],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("credential path must not contain symlinks", result.stderr)
            self.assertFalse(docker_log.exists())
            self.assertEqual(0o644, stat.S_IMODE(credential_target.stat().st_mode))

    def test_existing_custom_evidence_directory_is_not_silently_repermissioned(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory).resolve()
            bin_directory = temporary / "bin"
            bin_directory.mkdir()
            docker = bin_directory / "docker"
            docker.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
            docker.chmod(0o755)
            env_file = temporary / "dev-stack.env"
            env_file.write_text("POSTGRES_PASSWORD=existing\n", encoding="utf-8")
            evidence_directory = temporary / "evidence"
            evidence_directory.mkdir(mode=0o755)
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(env_file),
                    "EVIDENCE_DIRECTORY": str(evidence_directory),
                    "PATH": f"{bin_directory}{os.pathsep}{environment['PATH']}",
                }
            )

            result = subprocess.run(
                [str(ROOT / "scripts" / "dev-stack"), "status"],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("must have mode 0700", result.stderr)
            self.assertEqual(0o755, stat.S_IMODE(evidence_directory.stat().st_mode))

    def test_symlinked_evidence_directory_ancestor_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            temporary = Path(directory).resolve()
            bin_directory = temporary / "bin"
            bin_directory.mkdir()
            docker_log = temporary / "docker.log"
            docker = bin_directory / "docker"
            docker.write_text(
                "#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$DOCKER_LOG\"\n",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            outside = temporary / "outside"
            outside.mkdir()
            linked_parent = temporary / "linked-parent"
            linked_parent.symlink_to(outside, target_is_directory=True)
            environment = os.environ.copy()
            environment.update(
                {
                    "DEV_STACK_ENV_FILE": str(temporary / "dev-stack.env"),
                    "EVIDENCE_DIRECTORY": str(linked_parent / "evidence"),
                    "DOCKER_LOG": str(docker_log),
                    "PATH": f"{bin_directory}{os.pathsep}{environment['PATH']}",
                }
            )

            result = subprocess.run(
                [str(ROOT / "scripts" / "dev-stack"), "status"],
                cwd=ROOT,
                env=environment,
                capture_output=True,
                text=True,
            )

            self.assertNotEqual(0, result.returncode)
            self.assertIn("path must not contain symlinks", result.stderr)
            self.assertFalse((outside / "evidence").exists())
            self.assertFalse(docker_log.exists())


class DevStackComposeTest(unittest.TestCase):
    def test_compose_runs_only_persistent_ready_postgresql(self):
        source = (ROOT / "compose.yaml").read_text(encoding="utf-8")

        self.assertIn("postgres:17.6-alpine", source)
        self.assertIn("${POSTGRES_PASSWORD:?", source)
        self.assertIn("pg_isready", source)
        self.assertIn("postgres-data:/var/lib/postgresql/data", source)
        self.assertIn('127.0.0.1:${POSTGRES_PORT:-5432}:5432', source)
        for removed_service in ("clickhouse", "redpanda", "seaweedfs", "minio"):
            self.assertNotIn(removed_service, source.lower())

    def test_generated_state_is_git_ignored(self):
        for path in (".dev-stack.env", ".local/evidence/example"):
            result = subprocess.run(
                ["git", "check-ignore", path],
                cwd=ROOT,
                capture_output=True,
                text=True,
            )
            self.assertEqual(0, result.returncode, path)

    def test_canonical_verify_runs_development_stack_tests(self):
        verify = (ROOT / "scripts" / "verify").read_text(encoding="utf-8")
        self.assertIn('PYTHON="${PYTHON:-/usr/bin/python3}"', verify)
        self.assertIn('"$PYTHON" -m unittest discover', verify)


if __name__ == "__main__":
    unittest.main()
