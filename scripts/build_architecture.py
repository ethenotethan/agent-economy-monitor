#!/usr/bin/env python3
"""Compile repository evidence into a deterministic hermes.architecture v1 model."""

from __future__ import annotations

import argparse
import fnmatch
import hashlib
import importlib.util
import json
import re
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = ROOT / "architecture" / "config.json"
MODEL_PATH = ROOT / "architecture" / "model" / "model.json"
CONTRACT_PATH = ROOT / "architecture" / "contract" / "architecture_contract.py"

RUST_DECLARATION_RE = re.compile(
    r"(?m)^\s*(?:pub(?:\([^)]*\))?\s+)?(?:(async)\s+)?"
    r"(struct|enum|trait|type|const|static|fn|mod)\s+([A-Za-z_][A-Za-z0-9_]*)"
)
RUST_ROUTE_RE = re.compile(
    r"\.\s*(?:r#)?route\s*\(\s*\"(?P<path>/[^\"]*)\"\s*,\s*"
    r"(?P<method>get|post|put|patch|delete|head|options|trace|any)"
    r"\(\s*(?P<handler>[A-Za-z_][A-Za-z0-9_:]*)\s*\)\s*\)"
)
RUST_ROUTE_CALL_RE = re.compile(r"\.\s*(?:r#)?route\s*\(")
RUST_UNMODELED_ROUTER_CALL_RE = re.compile(
    r"\.\s*(?:r#)?(?P<constructor>route_service|nest_service|fallback_service|nest|fallback|merge)\s*\("
)
RUST_ROUTER_TYPE_ALIAS_RE = re.compile(
    r"\btype\s+(?P<alias>[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*<[^;]*?>)?(?:\s+where\b[^;]*?)?\s*=\s*"
    r"(?P<target>(?:::)?(?:[A-Za-z_][A-Za-z0-9_]*\s*::\s*)*[A-Za-z_][A-Za-z0-9_]*)"
    r"(?:\s*<[^;\n]+>)?\s*;"
)
RUST_USE_RE = re.compile(r"\buse\b(?P<body>[^;]+);")
RUST_IMPORT_ALIAS_RE = re.compile(
    r"\b(?P<target>[A-Za-z_][A-Za-z0-9_]*)\s+as\s+"
    r"(?P<alias>[A-Za-z_][A-Za-z0-9_]*)\b"
)
RUST_ROUTER_UFCS_CALL_RE = re.compile(
    r"\b(?P<router_type>[A-Za-z_][A-Za-z0-9_]*)(?:\s*::<[^>\n]+>)?\s*::\s*"
    r"(?:r#)?(?P<constructor>route|route_service|nest_service|fallback_service|nest|fallback|merge)\b"
)
RUST_MACRO_METHOD_CALL_RE = re.compile(r"\.\s*\$(?P<method>[A-Za-z_][A-Za-z0-9_]*)\s*\(")


class ArchitectureError(RuntimeError):
    """A deterministic architecture gate failure."""


def load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ArchitectureError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ArchitectureError(f"{path} must contain a JSON object")
    return value


def serialized_json(value: Any) -> str:
    return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def line_for(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def rust_without_comments(text: str) -> str:
    """Replace Rust comments with spaces while preserving offsets and literals."""
    result = list(text)
    index = 0
    length = len(text)

    def mask(start: int, end: int) -> None:
        for position in range(start, end):
            if text[position] not in "\r\n":
                result[position] = " "

    while index < length:
        raw = re.match(r"(?:br|r)(?P<hashes>#{0,255})\"", text[index:])
        if raw:
            terminator = '"' + raw.group("hashes")
            end = text.find(terminator, index + raw.end())
            index = length if end < 0 else end + len(terminator)
            continue
        if text.startswith(('"', 'b"', 'c"'), index):
            quote = index + (1 if text[index] in "bc" else 0)
            index = quote + 1
            while index < length:
                if text[index] == "\\":
                    index += 2
                elif text[index] == '"':
                    index += 1
                    break
                else:
                    index += 1
            continue
        if text.startswith("//", index):
            end = text.find("\n", index + 2)
            end = length if end < 0 else end
            mask(index, end)
            index = end
            continue
        if text.startswith("/*", index):
            start = index
            depth = 1
            index += 2
            while index < length and depth:
                if text.startswith("/*", index):
                    depth += 1
                    index += 2
                elif text.startswith("*/", index):
                    depth -= 1
                    index += 2
                else:
                    index += 1
            mask(start, index)
            continue
        index += 1

    return "".join(result)


def rust_router_type_names(texts: list[str]) -> set[str]:
    """Return Router plus type aliases that resolve to it across Rust sources."""
    aliases = [
        (match.group("alias"), match.group("target").split("::")[-1].strip())
        for text in texts
        for match in RUST_ROUTER_TYPE_ALIAS_RE.finditer(text)
    ]
    aliases.extend(
        (alias.group("alias"), alias.group("target"))
        for text in texts
        for use in RUST_USE_RE.finditer(text)
        for alias in RUST_IMPORT_ALIAS_RE.finditer(use.group("body"))
    )
    router_types = {"Router"}
    while True:
        resolved = {alias for alias, target in aliases if target in router_types}
        if resolved <= router_types:
            return router_types
        router_types.update(resolved)


def evidence_for(root: Path, raw: dict[str, Any]) -> dict[str, Any]:
    relative = raw.get("path")
    if not isinstance(relative, str) or relative.startswith("/") or ".." in Path(relative).parts:
        raise ArchitectureError(f"invalid repository-relative origin path: {relative!r}")
    path = root / relative
    if not path.is_file():
        raise ArchitectureError(f"origin cites missing file: {relative}")
    text = path.read_text(encoding="utf-8")
    if "route" in raw:
        route_source = rust_without_comments(text)
        route = raw["route"]
        method = raw.get("method")
        if not isinstance(method, str) or not method:
            raise ArchitectureError(f"route origin {route!r} in {relative} needs method")
        matches = [
            match
            for match in RUST_ROUTE_RE.finditer(route_source)
            if match.group("path") == route and match.group("method") == method
        ]
        if len(matches) != 1:
            raise ArchitectureError(
                f"route origin {method.upper()} {route!r} in {relative} matched {len(matches)} times"
            )
        match = matches[0]
        return {
            "excerpt": text[match.start() : match.end()],
            "line": line_for(text, match.start()),
            "path": relative,
            "rule": "rust.http-route",
        }
    search = raw.get("search")
    if not isinstance(search, str) or not search:
        raise ArchitectureError(f"origin for {relative} needs search or route")
    offsets = [match.start() for match in re.finditer(re.escape(search), text)]
    if len(offsets) != 1:
        raise ArchitectureError(f"origin search {search!r} in {relative} matched {len(offsets)} times")
    line = line_for(text, offsets[0])
    excerpt = text.splitlines()[line - 1].strip()
    return {"excerpt": excerpt, "line": line, "path": relative, "rule": "declared.topology"}


def expanded_inventory(root: Path, patterns: list[str]) -> list[Path]:
    root_resolved = root.resolve(strict=True)
    ignored_root_names = {".git", ".worktrees", "__pycache__", "target"}

    def is_ignored(path: Path) -> bool:
        relative = path.relative_to(root)
        return bool(relative.parts) and relative.parts[0] in ignored_root_names

    paths: set[Path] = set()
    for pattern in patterns:
        paths.update(path for path in root.glob(pattern) if path.is_file())
    if not paths:
        raise ArchitectureError("inventory globs selected no files")
    symlinks = {
        candidate
        for candidate in root.rglob("*")
        if candidate.is_symlink() and not is_ignored(candidate)
    }
    for path in sorted(paths | symlinks):
        try:
            resolved = path.resolve(strict=True)
        except OSError as exc:
            raise ArchitectureError(f"inventory path cannot be resolved: {path}: {exc}") from exc
        if not resolved.is_relative_to(root_resolved):
            raise ArchitectureError(
                f"inventory path resolves outside repository root: {path.relative_to(root).as_posix()}"
            )
        relative = path.relative_to(root)
        ancestors = [root.joinpath(*relative.parts[:index]) for index in range(1, len(relative.parts) + 1)]
        if any(ancestor.is_symlink() for ancestor in ancestors):
            raise ArchitectureError(f"inventory rejects symlink path: {relative.as_posix()}")
    rust_sources = {
        path
        for path in root.rglob("*.rs")
        if path.is_file() and not is_ignored(path)
    }
    omitted_rust = sorted(
        (path.relative_to(root).as_posix() for path in rust_sources - paths)
    )
    if omitted_rust:
        raise ArchitectureError(
            "inventory configuration omits Rust source: " + ", ".join(omitted_rust)
        )
    generated_paths = {"architecture/model/model.json"}
    governed_sources = {
        path
        for path in root.rglob("*")
        if path.is_file()
        and path.name != ".git"
        and path.suffix != ".pyc"
        and not is_ignored(path)
        and path.relative_to(root).as_posix() not in generated_paths
    }
    omitted_sources = sorted(
        path.relative_to(root).as_posix() for path in governed_sources - paths
    )
    if omitted_sources:
        raise ArchitectureError(
            "inventory configuration omits source file: " + ", ".join(omitted_sources)
        )
    return sorted(paths, key=lambda path: path.relative_to(root).as_posix())


def component_for(relative: str, components: list[dict[str, Any]]) -> str:
    matches = [
        str(component["id"])
        for component in components
        if any(fnmatch.fnmatchcase(relative, pattern) for pattern in component.get("patterns", []))
    ]
    if not matches:
        raise ArchitectureError(f"unassigned source: {relative}")
    if len(matches) > 1:
        raise ArchitectureError(f"ambiguous source assignment for {relative}: {', '.join(matches)}")
    return matches[0]


def rust_declarations(text: str) -> list[dict[str, Any]]:
    declarations = []
    for match in RUST_DECLARATION_RE.finditer(text):
        kind = "async-fn" if match.group(1) and match.group(2) == "fn" else match.group(2)
        declarations.append(
            {
                "kind": kind,
                "line": line_for(text, match.start()),
                "name": match.group(3),
                "passes": ["rust.declaration"],
            }
        )
    return declarations


def parse_ci(root: Path) -> dict[str, Any]:
    relative = ".github/workflows/ci.yml"
    pages_relative = ".github/workflows/pages.yml"
    workflows = sorted(
        path.relative_to(root).as_posix()
        for path in (root / ".github" / "workflows").glob("*")
        if path.is_file() and path.suffix in {".yml", ".yaml"}
    )
    governed_workflows = {relative, pages_relative}
    unmapped_workflows = [workflow for workflow in workflows if workflow not in governed_workflows]
    if unmapped_workflows:
        raise ArchitectureError(
            "unmapped GitHub Actions workflow: " + ", ".join(unmapped_workflows)
        )
    if relative not in workflows:
        raise ArchitectureError(f"missing governed GitHub Actions workflow: {relative}")
    path = root / relative
    text = path.read_text(encoding="utf-8")
    pages_text = (
        (root / pages_relative).read_text(encoding="utf-8")
        if pages_relative in workflows
        else None
    )
    canonical = """name: CI

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

permissions:
  contents: read

jobs:
  architecture:
    name: architecture
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - name: Verify architecture contract
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          python3 -m unittest scripts.test_architecture -v
          python3 scripts/check_architecture_contract.py
          python3 scripts/build_architecture.py --check

  verify:
    name: verify
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17.6-alpine
        env:
          POSTGRES_DB: agent_economy_test
          POSTGRES_PASSWORD: postgres
          POSTGRES_USER: postgres
        ports:
          - 5432:5432
        options: >-
          --health-cmd "pg_isready -U postgres -d agent_economy_test"
          --health-interval 5s
          --health-timeout 5s
          --health-retries 20
    steps:
      - uses: actions/checkout@v5
      - uses: dtolnay/rust-toolchain@stable
        with:
          components: rustfmt, clippy
      - uses: Swatinem/rust-cache@v2
      - name: Install browser test runtime
        run: |
          python3 -m pip install --requirement requirements-dev.txt
          python3 -m playwright install --with-deps firefox
      - name: Verify
        env:
          GH_TOKEN: ${{ github.token }}
          TEST_DATABASE_URL: postgresql://postgres:postgres@127.0.0.1:5432/agent_economy_test
          AEM_COLLECT_TEST_DATABASE_URL: postgresql://postgres:postgres@127.0.0.1:5432/agent_economy_test
        run: ./scripts/verify
      - name: Qualify knowledge graph migration
        env:
          RUN_KNOWLEDGE_GRAPH_LIVE: "1"
        run: python3 tests/knowledge_graph_schema_test.py KnowledgeGraphMigrationLiveTest -v
      - name: Qualify operational and analytics migration
        env:
          RUN_OPERATIONAL_ANALYTICS_LIVE: "1"
        run: python3 tests/operational_analytics_schema_test.py OperationalAnalyticsMigrationLiveTest -v
"""
    # Accept one auditable workflow grammar instead of trying to security-parse
    # arbitrary YAML with regular expressions. Workflow changes must update this
    # compiler and its adversarial tests in the same reviewed change.
    if text != canonical:
        raise ArchitectureError(f"{relative} must match the canonical workflow grammar exactly")
    pages_canonical = """name: Architecture Pages

on:
  push:
    branches: [main]
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: pages
  cancel-in-progress: false

jobs:
  build:
    name: build-pages
    if: github.ref == 'refs/heads/main'
    runs-on: ubuntu-latest
    permissions:
      contents: read
      pages: read
    steps:
      - uses: actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09 # v5
      - uses: actions/configure-pages@983d7736d9b0ae728b81ab479565c72886d7745b # v5
      - name: Verify canonical architecture model
        run: python3 scripts/build_architecture.py --check
      - name: Build architecture showcase
        run: python3 scripts/build_architecture_site.py --output _site --revision "$GITHUB_SHA"
      - name: Verify architecture showcase
        run: python3 -m unittest scripts.test_architecture_site -v
      - uses: actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9 # v5
        with:
          path: _site
          include-hidden-files: true

  deploy:
    name: deploy-pages
    needs: build
    runs-on: ubuntu-latest
    permissions:
      pages: write
      id-token: write
    environment:
      name: github-pages
      url: ${{ steps.deployment.outputs.page_url }}
    steps:
      - name: Deploy to GitHub Pages
        id: deployment
        uses: actions/deploy-pages@368f82528645a54fb793d4d04e342629a3f51346 # v5
"""
    if pages_text is not None and pages_text != pages_canonical:
        raise ArchitectureError(
            f"{pages_relative} must match the canonical Pages workflow grammar exactly"
        )
    lines = text.splitlines()
    on_indexes = [
        index for index, line in enumerate(lines) if re.match(r"^on:\s*(?:#.*)?$", line)
    ]
    if len(on_indexes) != 1:
        raise ArchitectureError(f"{relative} must declare pull_request and push:main triggers")
    trigger_lines: list[str] = []
    for line in lines[on_indexes[0] + 1 :]:
        if line and not line[0].isspace() and not line.lstrip().startswith("#"):
            break
        trigger_lines.append(line)
    def event_block(event: str) -> list[str] | None:
        event_index = next(
            (
                index
                for index, line in enumerate(trigger_lines)
                if re.match(rf"^  {re.escape(event)}:\s*(?:\{{\}})?\s*(?:#.*)?$", line)
            ),
            None,
        )
        if event_index is None:
            return None
        block: list[str] = []
        for line in trigger_lines[event_index + 1 :]:
            if re.match(r"^  [A-Za-z0-9_-]+:", line):
                break
            block.append(line)
        return block

    def filter_values(block: list[str], key: str) -> list[str] | None:
        key_index = next(
            (
                index
                for index, line in enumerate(block)
                if re.match(rf"^    {re.escape(key)}:\s*", line)
            ),
            None,
        )
        if key_index is None:
            return None
        declaration = block[key_index].split(":", 1)[1].split("#", 1)[0].strip()
        if declaration.startswith("[") and declaration.endswith("]"):
            return [item.strip().strip("'\"") for item in declaration[1:-1].split(",") if item.strip()]
        values: list[str] = []
        for line in block[key_index + 1 :]:
            match = re.match(r"^      -\s+(.+?)\s*(?:#.*)?$", line)
            if not match:
                break
            values.append(match.group(1).strip().strip("'\""))
        return values

    pull_request_block = event_block("pull_request")
    push_block = event_block("push")
    restrictive_pull_request = {"types", "paths", "paths-ignore", "branches-ignore"}
    pull_request_controls = {
        key for key in restrictive_pull_request if pull_request_block is not None and filter_values(pull_request_block, key) is not None
    }
    pull_request_branches = (
        filter_values(pull_request_block, "branches") if pull_request_block is not None else None
    )
    push_controls = {
        key
        for key in {"branches-ignore", "paths", "paths-ignore"}
        if push_block is not None and filter_values(push_block, key) is not None
    }
    push_branches = filter_values(push_block, "branches") if push_block is not None else None
    has_pull_request = (
        pull_request_block is not None
        and not pull_request_controls
        and (pull_request_branches is None or "main" in pull_request_branches)
    )
    has_push_main = (
        push_block is not None
        and push_branches is not None
        and "main" in push_branches
        and "!main" not in push_branches
        and not push_controls
    )
    if not has_pull_request or not has_push_main:
        raise ArchitectureError(
            f"{relative} trigger policy must declare unrestricted pull_request coverage for main and normal push:main coverage"
        )
    if re.search(r"(?m)^\s+(?:-\s+)?shell:\s*.+$", text):
        raise ArchitectureError(f"{relative} uses a custom shell for required gates")
    workflow_name = "CI"
    jobs: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    in_jobs = False
    run_block_indent: int | None = None
    checkout_step_indent: int | None = None
    for number, line in enumerate(lines, start=1):
        if line.startswith("name:"):
            workflow_name = line.split(":", 1)[1].strip()
        if line == "jobs:":
            in_jobs = True
            continue
        if not in_jobs:
            continue
        control_match = re.match(
            r"^\s+(?:-\s+)?(if|continue-on-error):\s*(.+)$", line
        )
        if current is not None and control_match:
            current["unsafe_controls"].append(line.strip())
        if current is not None and checkout_step_indent is not None:
            indent = len(line) - len(line.lstrip())
            if line.strip() and indent <= checkout_step_indent:
                checkout_step_indent = None
            elif re.match(r"^\s+['\"]?ref['\"]?\s*:\s*.+$", line) or re.match(
                r"^\s+with:\s*\{[^}]*['\"]?ref['\"]?\s*:", line
            ):
                current["checkout_ref_overrides"].append(line.strip())
        if current is not None and run_block_indent is not None:
            indent = len(line) - len(line.lstrip())
            if line.strip() and indent > run_block_indent:
                current["commands"].append(line.strip())
                continue
            if not line.strip():
                continue
            run_block_indent = None
        match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if match:
            if current is not None:
                jobs.append(current)
            current = {
                "commands": [],
                "checkout_ref_overrides": [],
                "evidence": {"line": number, "path": relative},
                "id": match.group(1),
                "name": match.group(1),
                "needs": [],
                "step_count": 0,
                "unsafe_controls": [],
            }
            run_block_indent = None
            checkout_step_indent = None
            continue
        if current is None:
            continue
        if re.match(r"^      -\s+(?:uses|name|run):", line):
            current["step_count"] += 1
        name_match = re.match(r"^    name:\s*(.+)$", line)
        needs_match = re.match(r"^    needs:\s*(.+)$", line)
        run_match = re.match(r"^\s+(?:-\s+)?run:\s*(.+)$", line)
        runs_on_match = re.match(r"^    runs-on:\s*(.+)$", line)
        checkout_match = re.match(
            r"^(\s*)-\s+uses:\s*(['\"]?)actions/checkout@[^\s'\"]+\2\s*(?:#.*)?$",
            line,
        )
        if name_match:
            current["name"] = name_match.group(1).strip()
        elif needs_match:
            raw = needs_match.group(1).strip().strip("[]")
            current["needs"] = sorted(item.strip() for item in raw.split(",") if item.strip())
        elif run_match:
            command = run_match.group(1).strip()
            if command == "|":
                run_block_indent = len(line) - len(line.lstrip())
            else:
                current["commands"].append(command)
        elif runs_on_match:
            current["runs_on"] = runs_on_match.group(1).strip()
        if checkout_match:
            checkout_step_indent = len(checkout_match.group(1))
    if current is not None:
        jobs.append(current)
    if not jobs:
        raise ArchitectureError(f"{relative} declares no CI jobs")
    job_ids = {job["id"] for job in jobs}
    required = {"architecture", "verify"}
    missing = sorted(required - job_ids)
    if missing:
        raise ArchitectureError("CI is missing separately named jobs: " + ", ".join(missing))
    required_commands = {
        "architecture": [
            "python3 -m unittest scripts.test_architecture -v",
            "python3 scripts/check_architecture_contract.py",
            "python3 scripts/build_architecture.py --check",
        ],
        "verify": [
            "python3 -m pip install --requirement requirements-dev.txt",
            "python3 -m playwright install --with-deps firefox",
            "./scripts/verify",
            "python3 tests/knowledge_graph_schema_test.py KnowledgeGraphMigrationLiveTest -v",
            "python3 tests/operational_analytics_schema_test.py OperationalAnalyticsMigrationLiveTest -v",
        ],
    }
    by_id = {job["id"]: job for job in jobs}
    for job_id, expected_commands in required_commands.items():
        if by_id[job_id]["checkout_ref_overrides"]:
            raise ArchitectureError(
                f"CI job {job_id} has a checkout ref override: "
                + ", ".join(by_id[job_id]["checkout_ref_overrides"])
            )
        if by_id[job_id]["unsafe_controls"]:
            raise ArchitectureError(
                f"CI job {job_id} is conditional or non-blocking: "
                + ", ".join(by_id[job_id]["unsafe_controls"])
            )
        if by_id[job_id]["commands"] != expected_commands:
            raise ArchitectureError(
                f"CI job {job_id} does not run the exact gate commands; "
                f"expected={expected_commands!r}, actual={by_id[job_id]['commands']!r}"
            )
    normalized_jobs = []
    for job in sorted(jobs, key=lambda item: item["id"]):
        normalized_jobs.append(
            {
                "evidence": job["evidence"],
                "family": "architecture" if job["id"] == "architecture" else "behavior",
                "id": job["id"],
                "name": job["name"],
                "needs": job["needs"],
                "role": "gate",
                "runs_on": job.get("runs_on", ""),
                "scripts": sorted(set(job["commands"])),
                "step_count": job["step_count"],
                "workflow": "ci",
            }
        )
    pages_lines = pages_text.splitlines() if pages_text is not None else []
    pages_jobs = [
        {
            "evidence": {"line": pages_lines.index("  build:") + 1, "path": pages_relative},
            "family": "documentation",
            "id": "build-pages",
            "name": "build-pages",
            "needs": [],
            "role": "post-merge",
            "runs_on": "ubuntu-latest",
            "condition": "github.ref == 'refs/heads/main'",
            "steps": [
                {"uses": "actions/checkout@fbc6f3992d24b796d5a048ff273f7fcc4a7b6c09"},
                {"uses": "actions/configure-pages@983d7736d9b0ae728b81ab479565c72886d7745b"},
                {"run": "python3 scripts/build_architecture.py --check"},
                {"run": "python3 scripts/build_architecture_site.py --output _site --revision \"$GITHUB_SHA\""},
                {"run": "python3 -m unittest scripts.test_architecture_site -v"},
                {"uses": "actions/upload-pages-artifact@fc324d3547104276b827a68afc52ff2a11cc49c9"},
            ],
            "scripts": [
                "python3 -m unittest scripts.test_architecture_site -v",
                "python3 scripts/build_architecture.py --check",
                "python3 scripts/build_architecture_site.py --output _site --revision \"$GITHUB_SHA\"",
            ],
            "step_count": 6,
            "workflow": "architecture-pages",
        },
        {
            "evidence": {"line": pages_lines.index("  deploy:") + 1, "path": pages_relative},
            "family": "documentation",
            "id": "deploy-pages",
            "name": "deploy-pages",
            "needs": ["build-pages"],
            "role": "post-merge",
            "runs_on": "ubuntu-latest",
            "steps": [
                {"uses": "actions/deploy-pages@368f82528645a54fb793d4d04e342629a3f51346"},
            ],
            "scripts": [],
            "step_count": 1,
            "workflow": "architecture-pages",
        },
    ] if pages_text is not None else []
    all_jobs = normalized_jobs + pages_jobs
    pages_edges = [
        {"kind": "triggers", "source": "push-main", "target": "build-pages"},
        {"kind": "needs", "source": "build-pages", "target": "deploy-pages"},
    ] if pages_text is not None else []
    pages_triggers = [
        {
            "event": "push:main; workflow_dispatch (main only)",
            "id": "push-main",
            "workflows": ["architecture-pages"],
        },
    ] if pages_text is not None else []
    pages_workflows = [
        {
            "events": [
                "push:main",
                "workflow_dispatch (main only)",
            ],
            "family": "documentation",
            "id": "architecture-pages",
            "jobs": ["build-pages", "deploy-pages"],
            "label": "Architecture Pages",
            "name": "Architecture Pages",
            "path": pages_relative,
        },
    ] if pages_text is not None else []
    return {
        "architectural": {
            "command": "python3 scripts/build_architecture.py --check",
            "job": "architecture",
        },
        "edges": [
            {"kind": "triggers", "source": "pull-request", "target": "architecture"},
            {"kind": "triggers", "source": "pull-request", "target": "verify"},
            {"kind": "gates", "source": "architecture", "target": "merge"},
            {"kind": "gates", "source": "verify", "target": "merge"},
        ] + pages_edges,
        "jobs": all_jobs,
        "limitations": [],
        "merge": {"id": "merge", "inputs": ["architecture", "verify"], "label": "Deterministic merge queue"},
        "ratchets": [],
        "static_checks": [
            {
                "command": "python3 scripts/build_architecture.py --check",
                "evidence": {"line": next(job["evidence"]["line"] for job in jobs if job["id"] == "architecture"), "path": relative},
                "job": "architecture",
                "name": "Architecture model current and conforming",
                "scripts": ["scripts/build_architecture.py", "scripts/test_architecture.py"],
            },
            {
                "command": "./scripts/verify",
                "evidence": {"line": next(job["evidence"]["line"] for job in jobs if job["id"] == "verify"), "path": relative},
                "job": "verify",
                "name": "Repository behavior gate",
                "scripts": ["scripts/verify"],
            },
        ],
        "summary": {"gates": 2, "jobs": len(all_jobs), "ratchets": 0, "static_checks": 2, "workflows": len(workflows)},
        "triggers": [
            {"event": "pull_request", "id": "pull-request", "workflows": ["ci"]},
        ] + pages_triggers,
        "workflows": [
            {
                "events": ["pull_request", "push:main"],
                "family": "delivery",
                "id": "ci",
                "jobs": [job["id"] for job in normalized_jobs],
                "label": workflow_name,
                "name": workflow_name,
                "path": relative,
            },
        ] + pages_workflows,
    }


def load_contract(root: Path):
    path = root / "architecture" / "contract" / "architecture_contract.py"
    spec = importlib.util.spec_from_file_location("architecture_contract_v1", path)
    if spec is None or spec.loader is None:
        raise ArchitectureError("cannot load hermes.architecture contract")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_renderer_contract(model: dict[str, Any]) -> list[str]:
    """Fail closed on shapes read by the four native document-backed tabs."""
    problems: list[str] = []
    required_arrays = {
        "components": model.get("components"),
        "interplay.nodes": model.get("interplay", {}).get("nodes"),
        "interplay.edges": model.get("interplay", {}).get("edges"),
        "interplay.pages": model.get("interplay", {}).get("pages"),
        "interplay.flows": model.get("interplay", {}).get("flows"),
        "interplay.invariants": model.get("interplay", {}).get("invariants"),
        "interplay.boundary_groups": model.get("interplay", {}).get("boundary_groups"),
        "extraction.files": model.get("extraction", {}).get("files"),
        "extraction.passes": model.get("extraction", {}).get("passes"),
        "extraction.entities": model.get("extraction", {}).get("entities"),
        "ci.workflows": model.get("ci", {}).get("workflows"),
        "ci.jobs": model.get("ci", {}).get("jobs"),
        "ci.edges": model.get("ci", {}).get("edges"),
        "ci.ratchets": model.get("ci", {}).get("ratchets"),
        "ci.static_checks": model.get("ci", {}).get("static_checks"),
        "stores.items": model.get("stores", {}).get("items"),
        "externals.systems": model.get("externals", {}).get("systems"),
        "externals.groups": model.get("externals", {}).get("groups"),
        "externals.edges": model.get("externals", {}).get("edges"),
    }
    for field, value in required_arrays.items():
        if not isinstance(value, list):
            problems.append(f"renderer field {field} must be an array, never undefined or malformed")

    def reject_duplicate_ids(field: str) -> None:
        values = required_arrays[field]
        if not isinstance(values, list):
            return
        ids = [item.get("id") for item in values if isinstance(item, dict)]
        for duplicate_id in sorted(
            {item_id for item_id in ids if ids.count(item_id) > 1},
            key=str,
        ):
            problems.append(f"{field}: duplicate id {duplicate_id!r}")

    for field in (
        "components",
        "interplay.nodes",
        "interplay.pages",
        "interplay.flows",
        "interplay.invariants",
        "interplay.boundary_groups",
        "extraction.passes",
        "stores.items",
        "externals.systems",
        "externals.groups",
    ):
        reject_duplicate_ids(field)

    components = required_arrays["components"]
    nodes = required_arrays["interplay.nodes"]
    pages = required_arrays["interplay.pages"]
    flows = required_arrays["interplay.flows"]
    boundary_groups = required_arrays["interplay.boundary_groups"]
    stores = required_arrays["stores.items"]
    external_systems = required_arrays["externals.systems"]
    if isinstance(components, list) and isinstance(nodes, list) and isinstance(pages, list):
        component_ids = {
            item.get("id") for item in components if isinstance(item, dict)
        }
        node_ids = {item.get("id") for item in nodes if isinstance(item, dict)}
        page_ids = {item.get("id") for item in pages if isinstance(item, dict)}
        edge_keys = {
            (edge.get("source"), edge.get("target"), edge.get("relation"))
            for edge in (required_arrays["interplay.edges"] or [])
            if isinstance(edge, dict)
        }
        for node in nodes:
            if not isinstance(node, dict):
                continue
            page_id = node.get("page")
            if page_id is not None and page_id not in page_ids:
                problems.append(
                    f"interplay.nodes[{node.get('id')}]: page {page_id!r} is not declared"
                )
        for page in pages:
            if not isinstance(page, dict):
                continue
            for field in ("roots", "components"):
                references = page.get(field) or []
                duplicates = sorted(
                    {reference for reference in references if references.count(reference) > 1},
                    key=str,
                )
                for duplicate in duplicates:
                    problems.append(
                        f"interplay.pages[{page.get('id')}]: duplicate {field} reference {duplicate!r}"
                    )
            for root_id in page.get("roots") or []:
                if root_id not in node_ids:
                    problems.append(
                        f"interplay.pages[{page.get('id')}]: root {root_id!r} is not a node"
                    )
            for component_id in page.get("components") or []:
                if component_id not in component_ids:
                    problems.append(
                        f"interplay.pages[{page.get('id')}]: component {component_id!r} is not declared"
                    )
        if isinstance(flows, list):
            for flow in flows:
                if not isinstance(flow, dict):
                    continue
                if flow.get("page") not in (None, "") and flow.get("page") not in page_ids:
                    problems.append(
                        f"interplay.flows[{flow.get('id')}]: page {flow.get('page')!r} is not declared"
                    )
                for step in flow.get("steps") or []:
                    if not isinstance(step, dict):
                        continue
                    key = (step.get("from"), step.get("to"), step.get("relation"))
                    if key not in edge_keys:
                        problems.append(
                            f"interplay.flows[{flow.get('id')}]: step {key!r} is not a renderer-native edge"
                        )
        if isinstance(boundary_groups, list):
            for group in boundary_groups:
                if not isinstance(group, dict):
                    continue
                for member in group.get("members") or []:
                    if member not in node_ids:
                        problems.append(
                            f"interplay.boundary_groups[{group.get('id')}]: member {member!r} is not a node"
                        )
        for field, values in (("stores.items", stores), ("externals.systems", external_systems)):
            if not isinstance(values, list):
                continue
            for item in values:
                if isinstance(item, dict) and item.get("component") not in (None, "") and item.get("component") not in component_ids:
                    problems.append(
                        f"{field}[{item.get('id')}]: component {item.get('component')!r} is not declared"
                    )

    files = required_arrays["extraction.files"]
    static_checks = required_arrays["ci.static_checks"]
    extraction = model.get("extraction", {})
    families = extraction.get("families") if isinstance(extraction, dict) else None
    entities = required_arrays["extraction.entities"]
    passes = required_arrays["extraction.passes"]
    if not isinstance(families, dict):
        problems.append("extraction.families must be an object defining every provenance rule family")
    elif isinstance(entities, list) and isinstance(passes, list):
        families_by_rule: dict[str, list[str]] = defaultdict(list)
        for family, rules in families.items():
            if isinstance(rules, list):
                for rule in rules:
                    if isinstance(rule, str):
                        families_by_rule[rule].append(str(family))
        pass_ids = {item.get("id") for item in passes if isinstance(item, dict)}
        for rule in sorted(pass_ids, key=str):
            declared_families = families_by_rule.get(str(rule), [])
            if len(declared_families) != 1:
                problems.append(
                    f"extraction rule {rule!r} must belong to exactly one provenance family"
                )
        for entity in entities:
            if not isinstance(entity, dict):
                continue
            for origin in entity.get("origins") or []:
                if not isinstance(origin, dict):
                    continue
                rule = origin.get("rule")
                family = origin.get("family")
                declared_families = families_by_rule.get(str(rule), [])
                if declared_families != [family]:
                    problems.append(
                        f"extraction entity {entity.get('id')!r} origin family {family!r} is inconsistent with rule {rule!r} families {declared_families!r}"
                    )
    if isinstance(files, list) and isinstance(static_checks, list):
        file_paths = {
            item.get("path") for item in files if isinstance(item, dict)
        }
        for check in static_checks:
            if not isinstance(check, dict):
                continue
            for script in check.get("scripts") or []:
                if script not in file_paths:
                    problems.append(
                        f"ci.static_checks[{check.get('name')}]: script {script!r} is not an analysed file"
                    )
    return problems


def validate_model(model: dict[str, Any], root: Path = ROOT) -> list[str]:
    contract = load_contract(root)
    return contract.validate_document(model) + validate_renderer_contract(model)


def validate_config(config: dict[str, Any]) -> None:
    if config.get("schema_version") != "1.0.0":
        raise ArchitectureError("architecture/config.json must use schema_version 1.0.0")
    for collection in ("components", "layers", "nodes", "pages", "invariants", "flows", "stores", "externals", "boundary_groups"):
        values = config.get(collection)
        if not isinstance(values, list):
            raise ArchitectureError(f"config {collection} must be an array")
        ids = [item.get("id") for item in values if isinstance(item, dict)]
        if len(ids) != len(values) or None in ids or len(ids) != len(set(ids)):
            raise ArchitectureError(f"config {collection} IDs must be present and unique")
    node_ids = {item["id"] for item in config["nodes"]}
    pages = {item["id"] for item in config["pages"]}
    components = {item["id"] for item in config["components"]}
    forbidden_fallbacks = {"shared", "sharedcore"}
    normalized_component_ids = {
        re.sub(r"[^a-z0-9]", "", str(component_id).lower())
        for component_id in components
    }
    normalized_page_ids = {re.sub(r"[^a-z0-9]", "", str(page_id).lower()) for page_id in pages}
    normalized_node_ids = {
        re.sub(r"[^a-z0-9]", "", str(item["id"]).lower()) for item in config["nodes"]
    }
    normalized_assignments = {
        re.sub(r"[^a-z0-9]", "", str(item.get("page") or "").lower())
        for item in config["nodes"]
    }
    if (
        normalized_component_ids
        | normalized_page_ids
        | normalized_node_ids
        | normalized_assignments
    ) & forbidden_fallbacks:
        raise ArchitectureError("Shared-core fallback is forbidden")
    for node in config["nodes"]:
        page_id = node.get("page")
        if node["kind"] != "external" and page_id is None:
            raise ArchitectureError(f"internal node {node['id']} has no explicit System Map page")
        if page_id is not None and page_id not in pages:
            raise ArchitectureError(f"node {node['id']} has unknown page {page_id!r}")
        if node.get("component") is not None and node["component"] not in components:
            raise ArchitectureError(f"node {node['id']} has unknown component {node['component']}")
    for page in config["pages"]:
        roots = page.get("roots", [])
        page_components = page.get("components", [])
        if len(roots) != len(set(roots)):
            raise ArchitectureError(f"page {page['id']} has duplicate roots")
        if len(page_components) != len(set(page_components)):
            raise ArchitectureError(f"page {page['id']} has duplicate components")
        unknown_roots = sorted(set(roots) - node_ids)
        if unknown_roots:
            raise ArchitectureError(
                f"page {page['id']} has unknown roots: {', '.join(unknown_roots)}"
            )
        unknown_components = sorted(set(page.get("components", [])) - components)
        if unknown_components:
            raise ArchitectureError(
                f"page {page['id']} has unknown components: {', '.join(unknown_components)}"
            )
    edge_keys = {
        (edge.get("source"), edge.get("target"), edge.get("relation"))
        for edge in config.get("interplay_edges", [])
    }
    if len(edge_keys) != len(config.get("interplay_edges", [])):
        raise ArchitectureError("duplicate edge is forbidden")
    for edge in config.get("interplay_edges", []):
        if edge.get("source") not in node_ids or edge.get("target") not in node_ids:
            raise ArchitectureError(f"edge has invalid endpoint: {edge}")
    for flow in config["flows"]:
        if flow.get("page") not in pages:
            raise ArchitectureError(f"flow {flow['id']} has unknown page {flow.get('page')!r}")
        for step in flow.get("steps", []):
            key = (step.get("from"), step.get("to"), step.get("relation"))
            if key not in edge_keys:
                raise ArchitectureError(f"flow {flow['id']} step is not a renderer-native edge: {key}")
    grouped = [member for group in config["boundary_groups"] for member in group.get("members", [])]
    if len(grouped) != len(set(grouped)):
        raise ArchitectureError("boundary member is grouped more than once")
    unknown_members = sorted(set(grouped) - node_ids)
    if unknown_members:
        raise ArchitectureError("boundary groups name unknown nodes: " + ", ".join(unknown_members))
    boundary_required = {node["id"] for node in config["nodes"] if node["kind"] in {"external", "store"}}
    non_boundary = sorted(set(grouped) - boundary_required)
    if non_boundary:
        raise ArchitectureError(
            "boundary groups contain non-boundary nodes: " + ", ".join(non_boundary)
        )
    missing = sorted(boundary_required - set(grouped))
    if missing:
        raise ArchitectureError("external/store boundary members are ungrouped: " + ", ".join(missing))
    external_nodes = {
        node["id"].removeprefix("external:")
        for node in config["nodes"]
        if node["kind"] == "external"
    }
    external_systems = {item["id"] for item in config["externals"]}
    if external_nodes != external_systems:
        raise ArchitectureError(
            "external systems must explicitly type every external node; "
            f"nodes={sorted(external_nodes)!r}, systems={sorted(external_systems)!r}"
        )
    for collection in ("externals", "stores"):
        for item in config[collection]:
            component_id = item.get("component")
            if component_id is not None and component_id not in components:
                raise ArchitectureError(
                    f"{collection} item {item['id']} has unknown component {component_id!r}"
                )
    nodes_by_id = {node["id"]: node for node in config["nodes"]}
    for store in config["stores"]:
        node = nodes_by_id.get(store.get("node"))
        if node is None or node.get("kind") != "store":
            raise ArchitectureError(
                f"store {store['id']} has invalid store node {store.get('node')!r}"
            )


def compile_architecture(root: Path = ROOT) -> dict[str, Any]:
    config = load_json(root / "architecture" / "config.json")
    validate_config(config)
    inventory_paths = expanded_inventory(root, config["inventory_globs"])
    router_types = rust_router_type_names(
        [
            rust_without_comments(path.read_text(encoding="utf-8"))
            for path in inventory_paths
            if path.suffix == ".rs"
        ]
    )
    components = config["components"]
    files: list[dict[str, Any]] = []
    digest = hashlib.sha256()
    declarations_by_component: dict[str, list[str]] = defaultdict(list)
    files_by_component: dict[str, list[str]] = defaultdict(list)
    lines_by_component: Counter[str] = Counter()
    total_declarations = 0
    discovered_routes: set[tuple[str, str, str]] = set()
    for path in inventory_paths:
        relative = path.relative_to(root).as_posix()
        owner = component_for(relative, components)
        data = path.read_bytes()
        try:
            text = data.decode("utf-8")
        except UnicodeDecodeError as exc:
            raise ArchitectureError(f"inventory file is not UTF-8: {relative}") from exc
        digest.update(relative.encode("utf-8") + b"\0" + data + b"\0")
        declarations = rust_declarations(text) if path.suffix == ".rs" else []
        if path.suffix == ".rs":
            route_source = rust_without_comments(text)
            macro_method_calls = list(RUST_MACRO_METHOD_CALL_RE.finditer(route_source))
            if macro_method_calls:
                call = macro_method_calls[0]
                raise ArchitectureError(
                    "unsupported Axum macro method indirection: "
                    f"{relative} at line {line_for(text, call.start())}"
                )
            router_ufcs_calls = [
                call
                for call in RUST_ROUTER_UFCS_CALL_RE.finditer(route_source)
                if call.group("router_type") in router_types
            ]
            if router_ufcs_calls:
                call = router_ufcs_calls[0]
                raise ArchitectureError(
                    f"unsupported Axum {call.group('constructor')}: "
                    f"{relative} at line {line_for(text, call.start())}"
                )
            unmodeled_router_calls = list(RUST_UNMODELED_ROUTER_CALL_RE.finditer(route_source))
            if unmodeled_router_calls:
                call = unmodeled_router_calls[0]
                raise ArchitectureError(
                    f"unsupported Axum {call.group('constructor')}: "
                    f"{relative} at line {line_for(text, call.start())}"
                )
            route_matches = list(RUST_ROUTE_RE.finditer(route_source))
            supported_route_offsets = {match.start() for match in route_matches}
            unsupported_routes = [
                match
                for match in RUST_ROUTE_CALL_RE.finditer(route_source)
                if match.start() not in supported_route_offsets
            ]
            if unsupported_routes:
                route = unsupported_routes[0]
                raise ArchitectureError(
                    "unsupported Axum route: "
                    f"{relative} at line {line_for(text, route.start())}"
                )
            discovered_routes.update(
                (relative, match.group("path"), match.group("method"))
                for match in route_matches
            )
        total_declarations += len(declarations)
        declarations_by_component[owner].extend(item["name"] for item in declarations)
        files_by_component[owner].append(relative)
        lines_by_component[owner] += len(text.splitlines())
        files.append(
            {
                "citations": 0,
                "component": owner,
                "declaration_count": len(declarations),
                "declarations": declarations,
                "line_count": len(text.splitlines()),
                "mapped_declarations": 0,
                "passes": ["rust.declaration"] if path.suffix == ".rs" else ["declared.topology"],
                "path": relative,
                "semantic_citations": 0,
                "touched": False,
            }
        )

    nodes = []
    entities = []
    citation_count: Counter[str] = Counter()
    for raw in config["nodes"]:
        evidence = evidence_for(root, raw["origin"])
        citation_count[evidence["path"]] += 1
        node = {
            "component": raw.get("component"),
            "evidence": [{key: evidence[key] for key in ("excerpt", "line", "path")}],
            "id": raw["id"],
            "kind": raw["kind"],
            "label": raw["label"],
            "page": raw.get("page"),
        }
        nodes.append(node)
        entities.append(
            {
                "component": raw.get("component"),
                "id": raw["id"],
                "kind": raw["kind"],
                "label": raw["label"],
                "origins": [
                    {
                        "family": "mechanical" if raw["authority"] == "mechanical" else "specified",
                        "line": evidence["line"],
                        "path": evidence["path"],
                        "rule": evidence["rule"],
                        "via": evidence.get("excerpt", ""),
                    }
                ],
            }
        )
    declared_routes = {
        (raw["origin"]["path"], raw["origin"]["route"], raw["origin"].get("method"))
        for raw in config["nodes"]
        if "route" in raw["origin"]
    }
    undeclared_routes = sorted(discovered_routes - declared_routes)
    if undeclared_routes:
        raise ArchitectureError(
            "undeclared Axum route: "
            + ", ".join(
                f"{path}:{method.upper()} {route}" for path, route, method in undeclared_routes
            )
        )
    unimplemented_routes = sorted(declared_routes - discovered_routes)
    if unimplemented_routes:
        raise ArchitectureError(
            "declared Axum route is not implemented: "
            + ", ".join(
                f"{path}:{method.upper() if method else '<missing-method>'} {route}"
                for path, route, method in unimplemented_routes
            )
        )
    for record in files:
        citations = citation_count[record["path"]]
        record["citations"] = citations
        record["touched"] = citations > 0

    component_records = []
    for configured in components:
        component_id = configured["id"]
        owned_files = sorted(files_by_component[component_id])
        declarations = sorted(set(declarations_by_component[component_id]))
        component_records.append(
            {
                "declaration_count": len(declarations),
                "declarations": declarations,
                "description": configured["description"],
                "external": False,
                "file_count": len(owned_files),
                "files": owned_files,
                "id": component_id,
                "label": configured["label"],
                "layer": configured["layer"],
                "line_count": lines_by_component[component_id],
            }
        )

    ci = parse_ci(root)
    passes = [
        {"citations": sum(1 for entity in entities for origin in entity["origins"] if origin["rule"] == "declared.topology"), "class": "semantic", "description": "Human-authoritative topology with unique source citations.", "files": sum(record["touched"] for record in files), "id": "declared.topology"},
        {"citations": len(discovered_routes), "class": "mechanical", "description": "Literal Axum method and path routes extracted from Rust source.", "files": len({path for path, _route, _method in discovered_routes}), "id": "rust.http-route"},
        {"citations": total_declarations, "class": "mechanical", "description": "Rust item declarations extracted with source lines.", "files": sum(1 for record in files if record["path"].endswith(".rs")), "id": "rust.declaration"},
        {"citations": len(ci["jobs"]), "class": "mechanical", "description": "GitHub Actions workflow jobs and gate commands extracted from CI YAML.", "files": len(ci["workflows"]), "id": "ci.workflow"},
    ]
    flows = []
    for configured_flow in config["flows"]:
        flow = dict(configured_flow)
        flow["evidence"] = [
            {key: resolved[key] for key in ("excerpt", "line", "path")}
            for raw in configured_flow.get("evidence", [])
            for resolved in [evidence_for(root, raw)]
        ]
        flows.append(flow)
    model = {
        "ci": ci,
        "components": sorted(component_records, key=lambda item: item["id"]),
        "description": config["description"],
        "edges": [],
        "evidence_metadata": config["evidence_metadata"],
        "externals": {
            "edges": [],
            "groups": [{"id": group["id"], "label": group["label"]} for group in config["boundary_groups"]],
            "systems": sorted(config["externals"], key=lambda item: item["id"]),
        },
        "extraction": {
            "authority": "observed",
            "coverage": {
                "mechanically_extracted": ["rust.source-inventory", "rust.http-routes", "ci.workflow-topology"],
                "specified": ["pipeline.topology", "storage.authority", "external.boundaries", "system-map.pages", "system-flows"],
                "unresolved": sorted(config["unresolved"], key=lambda item: item["id"]),
            },
            "derivation": "Repository-relative UTF-8 inventory plus deterministic Rust/CI extraction and uniquely cited architecture policy.",
            "entities": sorted(entities, key=lambda item: item["id"]),
            "families": {"mechanical": ["ci.workflow", "rust.declaration", "rust.http-route"], "specified": ["declared.topology"]},
            "files": files,
            "passes": sorted(passes, key=lambda item: item["id"]),
            "summary": {
                "by_kind": dict(sorted(Counter(node["kind"] for node in nodes).items())),
                "citations": sum(citation_count.values()),
                "declarations": total_declarations,
                "entities": len(entities),
                "entities_with_origin": len(entities),
                "files": len(files),
                "mapped_declarations": 0,
                "passes": len(passes),
                "semantic_citations": 0,
                "touched_files": sum(record["touched"] for record in files),
                "untouched_files": sum(not record["touched"] for record in files),
            },
        },
        "interplay": {
            "boundary_groups": sorted(config["boundary_groups"], key=lambda item: item["id"]),
            "clusters": [],
            "edges": sorted(config["interplay_edges"], key=lambda item: (item["source"], item["target"], item["relation"])),
            "flows": sorted(flows, key=lambda item: item["id"]),
            "invariants": sorted(config["invariants"], key=lambda item: item["id"]),
            "launch": {},
            "machines": {},
            "nodes": sorted(nodes, key=lambda item: item["id"]),
            "pages": config["pages"],
            "triggers": [],
        },
        "inventory": {
            "declarations": total_declarations,
            "files": len(files),
            "lines": sum(record["line_count"] for record in files),
        },
        "layers": config["layers"],
        "repository": config["repository"],
        "schema_version": config["schema_version"],
        "source_tree_sha256": digest.hexdigest(),
        "stores": {
            "count": len(config["stores"]),
            "items": [
                {
                    **{key: value for key, value in store.items() if key != "node"},
                    "evidence": next(node["evidence"][0] for node in nodes if node["id"] == store["node"]),
                }
                for store in sorted(config["stores"], key=lambda item: item["id"])
            ],
        },
        "title": config["title"],
    }
    problems = validate_model(model, root)
    if problems:
        raise ArchitectureError("compiled model is non-conforming:\n  - " + "\n  - ".join(problems))
    return model


def check_model(expected: str, path: Path) -> None:
    actual = path.read_text(encoding="utf-8") if path.is_file() else None
    if actual != expected:
        raise ArchitectureError("architecture/model/model.json is stale; run python3 scripts/build_architecture.py")


def write_model(content: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as handle:
        handle.write(content)
        temporary = Path(handle.name)
    temporary.replace(path)
    print("wrote architecture/model/model.json")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="validate and fail if the checked-in model has drifted")
    args = parser.parse_args()
    try:
        content = serialized_json(compile_architecture(ROOT))
        if args.check:
            check_model(content, MODEL_PATH)
            print("architecture model is current and conforms to hermes.architecture v1.0")
        else:
            write_model(content, MODEL_PATH)
    except ArchitectureError as exc:
        print(f"architecture error: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
