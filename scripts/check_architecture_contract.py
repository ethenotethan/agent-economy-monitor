#!/usr/bin/env python3
"""Contract pin check: the vendored hermes.architecture contract is the one Harness ships.

`architecture/contract/architecture_contract.py` is vendored byte-for-byte from
Harness (`tui_gateway/architecture_contract.py`) and is the code the compiler
validates its own output with, the gateway validates documents with, and the
renderers decode against. Drift on either side would surface only when a
document is refused or a renderer meets a field it does not know. So the copy
is pinned: `pins.json` records the sha256 of the module and of its JSON Schema
export. The checker carries the Harness-approved sha256 values as its trust
anchor and requires `pins.json` to agree, so changing the vendored files and
locally repinning them cannot bless drift. This check fails when

  * the module or schema differs from the trusted digest;
  * `pins.json` differs from that trusted digest;
  * the schema export is not what the module itself exports (`schema_json()`);
  * the committed model (`architecture/model/model.json`) does not conform.

Changing the contract therefore requires updating the Harness-approved trust
anchor as part of the coordinated contract rollout; editing only this
repository's vendored files and `pins.json` is deliberately insufficient. The
constraint ratchet guards this script like every other `scripts/check-*.py`: it
may be replaced, not removed.

Usage:
    check-contract-pins.py
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, List

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_DIR = ROOT / "architecture" / "contract"
MODULE_PATH = CONTRACT_DIR / "architecture_contract.py"
SCHEMA_PATH = CONTRACT_DIR / "architecture-document-v1.schema.json"
PINS_PATH = CONTRACT_DIR / "pins.json"
MODEL_PATH = ROOT / "architecture" / "model" / "model.json"
TRUSTED_SHA256 = {
    "architecture-document-v1.schema.json": "b89978fcf4c8fed4f4f90a516c971d9c8950522040342ee853f64db3e2686541",
    "architecture_contract.py": "af9da370e8d5f627d2f022f37fa857b25aa33e7eb9b0068fadf6f2653a3c9e36",
}
TRUSTED_SOURCE = {
    "commit": "c284799ccef91c283127ae62df005b8b85e6e46b",
    "repository": "ethenotethan/harness",
    "files": {
        "architecture-document-v1.schema.json": "docs/api/architecture-document-v1.schema.json",
        "architecture_contract.py": "tui_gateway/architecture_contract.py",
    },
}


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def load_module(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location("architecture_contract_pinned", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def fetch_url(url: str) -> bytes:
    token = os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")
    if not token:
        try:
            completed = subprocess.run(
                ["gh", "auth", "token"],
                check=True,
                capture_output=True,
                text=True,
                timeout=10,
            )
            token = completed.stdout.strip()
        except (FileNotFoundError, subprocess.SubprocessError):
            token = None
    headers = {
        "Accept": "application/vnd.github.raw+json",
        "User-Agent": "agent-economy-monitor-architecture-check",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    request = urllib.request.Request(url, headers=headers)
    for attempt in range(3):
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return response.read()
        except urllib.error.HTTPError as exc:
            if exc.code != 429 or attempt == 2:
                raise
            retry_after = exc.headers.get("Retry-After") if exc.headers else None
            try:
                delay = float(retry_after) if retry_after is not None else float(2**attempt)
            except ValueError:
                delay = float(2**attempt)
            time.sleep(min(max(delay, 0.0), 60.0))

    raise AssertionError("unreachable retry loop")


def evaluate(root: Path = ROOT, fetch: Callable[[str], bytes] = fetch_url) -> List[str]:
    """Every way the vendored contract, its export, its pin or the model disagree."""
    contract_dir = root / "architecture" / "contract"
    module_path = contract_dir / "architecture_contract.py"
    schema_path = contract_dir / "architecture-document-v1.schema.json"
    pins_path = contract_dir / "pins.json"
    model_path = root / "architecture" / "model" / "model.json"
    problems: List[str] = []
    for path in (module_path, schema_path, pins_path):
        if not path.is_file():
            problems.append(f"{path.relative_to(root)}: missing")
    if problems:
        return problems
    try:
        pins = json.loads(pins_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        return [f"{pins_path.relative_to(root)}: not valid JSON ({exc})"]
    digests = pins.get("sha256") if isinstance(pins.get("sha256"), dict) else {}
    source = pins.get("source")
    if source != TRUSTED_SOURCE:
        problems.append(
            f"{pins_path.relative_to(root)}: source metadata does not name the exact trusted Harness repository, commit, and files"
        )
    for path in (module_path, schema_path):
        pinned = digests.get(path.name)
        actual = sha256(path)
        trusted = TRUSTED_SHA256[path.name]
        if pinned != trusted:
            problems.append(
                f"{pins_path.relative_to(root)}: {path.name} pin {str(pinned)[:12]}… "
                f"does not match the trusted contract digest {trusted[:12]}…"
            )
        if actual != trusted:
            problems.append(
                f"{path.relative_to(root)}: sha256 {actual[:12]}… does not match the trusted contract digest {trusted[:12]}… "
                "(vendor only the Harness-approved contract)"
            )
        upstream_path = TRUSTED_SOURCE["files"][path.name]
        upstream_url = (
            f"https://api.github.com/repos/{TRUSTED_SOURCE['repository']}/contents/"
            f"{upstream_path}?ref={TRUSTED_SOURCE['commit']}"
        )
        try:
            upstream = fetch(upstream_url)
        except Exception as exc:
            problems.append(
                f"{path.relative_to(root)}: cannot fetch immutable Harness source {upstream_url}: {exc}"
            )
        else:
            vendored = path.read_bytes()
            if vendored != upstream:
                problems.append(
                    f"{path.relative_to(root)}: vendored bytes differ from immutable Harness source {upstream_url}"
                )
    module = load_module(module_path)
    if schema_path.read_text(encoding="utf-8") != module.schema_json():
        problems.append(f"{schema_path.relative_to(root)}: is not the module's own schema export (run `python3 {module_path.relative_to(root)} --schema`)")
    if str(pins.get("version")) != str(module.CONTRACT_VERSION):
        problems.append(f"{pins_path.relative_to(root)}: version {pins.get('version')!r} is not the module's {module.CONTRACT_VERSION!r}")
    if not model_path.is_file():
        problems.append(f"{model_path.relative_to(root)}: missing (run make architecture)")
        return problems
    issues = module.validate_document(json.loads(model_path.read_text(encoding="utf-8")))
    for issue in issues[:20]:
        problems.append(f"{model_path.relative_to(root)}: {issue}")
    if len(issues) > 20:
        problems.append(f"{model_path.relative_to(root)}: +{len(issues) - 20} more problem(s)")
    return problems


def main() -> int:
    problems = evaluate()
    if not problems:
        module = load_module(MODULE_PATH)
        print(f"Contract pins: {module.CONTRACT_NAME} v{module.CONTRACT_VERSION} vendored copy matches its pins and the model conforms.")
        return 0
    print(f"Contract pins: {len(problems)} problem(s):")
    for problem in problems:
        print(f"  - {problem}")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
