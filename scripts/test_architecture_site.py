from __future__ import annotations

import hashlib
import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
BUILD_SCRIPT = ROOT / "scripts" / "build_architecture_site.py"
MODEL = ROOT / "architecture" / "model" / "model.json"
WORKFLOW = ROOT / ".github" / "workflows" / "pages.yml"


def tree_digest(path: Path) -> str:
    digest = hashlib.sha256()
    for file in sorted(candidate for candidate in path.rglob("*") if candidate.is_file()):
        if file.name == ".architecture-pages-owned":
            continue
        digest.update(file.relative_to(path).as_posix().encode())
        digest.update(b"\0")
        digest.update(file.read_bytes())
        digest.update(b"\0")
    return digest.hexdigest()


class ArchitectureSiteTests(unittest.TestCase):
    maxDiff = None

    def build(self, output: Path) -> None:
        subprocess.run(
            ["python3", str(BUILD_SCRIPT), "--output", str(output)],
            cwd=ROOT,
            check=True,
            capture_output=True,
            text=True,
        )

    def test_build_is_deterministic_and_copies_canonical_model(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            resolved_directory = Path(directory).resolve()
            first = resolved_directory / "first"
            second = resolved_directory / "second"
            self.build(first)
            self.build(second)
            self.assertEqual(tree_digest(first), tree_digest(second))
            self.assertEqual(
                json.loads((first / "architecture.json").read_text()),
                json.loads(MODEL.read_text()),
            )
            self.assertEqual((first / "architecture.json").read_bytes(), MODEL.read_bytes())
            metadata = json.loads((first / "build-meta.json").read_text())
            self.assertEqual(metadata["source_revision"], json.loads(MODEL.read_text())["source_tree_sha256"])
            self.assertEqual(
                {path.name for path in first.iterdir()},
                {".architecture-pages-owned", ".nojekyll", "app.js", "architecture.json", "build-meta.json", "index.html", "styles.css"},
            )

    def test_build_rejects_repository_source_and_symlink_outputs(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "target"
            target.mkdir()
            link = Path(directory) / "linked-output"
            link.symlink_to(target, target_is_directory=True)
            repository_link = Path(directory) / "repository-link"
            repository_link.symlink_to(ROOT, target_is_directory=True)
            external = Path(directory) / "unowned-output"
            external.mkdir()
            sentinel = external / "sentinel"
            sentinel.write_text("preserve", encoding="utf-8")
            parent_target = Path(directory) / "parent-target"
            parent_target.mkdir()
            parent_sentinel = parent_target / "child"
            parent_sentinel.mkdir()
            (parent_sentinel / "sentinel").write_text("preserve", encoding="utf-8")
            parent_link = Path(directory) / "parent-link"
            parent_link.symlink_to(parent_target, target_is_directory=True)
            for output in (ROOT / "scripts", ROOT / "site", link, repository_link / "site", external, parent_link / "child"):
                result = subprocess.run(
                    ["python3", str(BUILD_SCRIPT), "--output", str(output)],
                    cwd=ROOT,
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0, output)
            self.assertTrue(target.is_dir())
            self.assertEqual(sentinel.read_text(encoding="utf-8"), "preserve")
            self.assertEqual((parent_sentinel / "sentinel").read_text(encoding="utf-8"), "preserve")

    def test_site_exposes_every_required_system_map_surface(self) -> None:
        model = json.loads(MODEL.read_text())
        self.assertGreaterEqual(len(model["interplay"]["nodes"]), 1)
        self.assertGreaterEqual(len(model["interplay"]["edges"]), 1)
        self.assertGreaterEqual(len(model["interplay"]["flows"]), 1)
        self.assertGreaterEqual(len(model["interplay"]["boundary_groups"]), 1)
        self.assertGreaterEqual(len(model["interplay"]["invariants"]), 1)
        self.assertGreaterEqual(len(model["stores"]["items"]), 1)
        self.assertGreaterEqual(len(model["ci"]["jobs"]), 1)
        self.assertGreaterEqual(len(model["extraction"]["passes"]), 1)

        html = (ROOT / "site" / "index.html").read_text()
        for section in (
            "system-map",
            "authority-model",
            "flows",
            "storage",
            "invariants",
            "coverage",
            "delivery",
            "provenance",
        ):
            self.assertIn(f'id="{section}"', html)
        self.assertIn('src="app.js"', html)
        self.assertIn('href="styles.css"', html)

        javascript = (ROOT / "site" / "app.js").read_text()
        for key in (
            "architecture.json",
            "interplay.nodes",
            "interplay.edges",
            "interplay.flows",
            "boundary_groups",
            "invariants",
            "extraction.passes",
            "ci.jobs",
        ):
            self.assertIn(key, javascript)

    def test_pages_workflow_has_least_privilege_deployment_contract(self) -> None:
        workflow = WORKFLOW.read_text()
        self.assertIn("contents: read", workflow)
        self.assertIn("pages: read", workflow)
        self.assertIn("    permissions:\n      pages: write\n      id-token: write", workflow)
        self.assertNotIn("\n  pages: write", workflow)
        self.assertIn("actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09", workflow)
        self.assertIn("actions/configure-pages@983d7736d9b0ae728b81ab479565c72886d7745b", workflow)
        self.assertIn("actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9", workflow)
        self.assertIn("actions/deploy-pages@368f82528645a54fb793d4d04e342629a3f51346", workflow)
        self.assertIn("python3 scripts/build_architecture.py --check", workflow)
        self.assertIn('python3 scripts/build_architecture_site.py --output _site --revision "$GITHUB_SHA"', workflow)
        self.assertIn("include-hidden-files: true", workflow)
        self.assertIn("if: github.ref == 'refs/heads/main'", workflow)
        self.assertNotIn("pull_request_target", workflow)

    def test_public_assets_do_not_contain_secret_shaped_values(self) -> None:
        forbidden = (
            re.compile(r"ghp_[A-Za-z0-9]{20,}"),
            re.compile(r"github_pat_[A-Za-z0-9_]{20,}"),
            re.compile(r"Bearer [A-Za-z0-9._-]{20,}"),
            re.compile(r"sk-[A-Za-z0-9]{20,}"),
        )
        for path in (ROOT / "site").iterdir():
            if path.is_file():
                content = path.read_text()
                for pattern in forbidden:
                    self.assertIsNone(pattern.search(content), f"{pattern.pattern!r} found in {path}")


if __name__ == "__main__":
    unittest.main()
