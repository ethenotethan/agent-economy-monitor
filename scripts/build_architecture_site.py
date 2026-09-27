#!/usr/bin/env python3
"""Build the deterministic GitHub Pages architecture showcase."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE_SOURCE = ROOT / "site"
MODEL_SOURCE = ROOT / "architecture" / "model" / "model.json"
STATIC_FILES = ("index.html", "styles.css", "app.js")
OWNERSHIP_MARKER = ".architecture-pages-owned"


def ownership_token(output: Path) -> str:
    return hashlib.sha256(str(output).encode("utf-8")).hexdigest() + "\n"


def build(output: Path, revision: str | None = None) -> None:
    """Copy audited static assets and the canonical model into a clean directory."""
    expanded_output = output.expanduser()
    raw_output = Path(os.path.abspath(expanded_output if expanded_output.is_absolute() else ROOT / expanded_output))
    current = Path(raw_output.anchor)
    for part in raw_output.parts[1:]:
        current /= part
        if current.is_symlink():
            raise ValueError(f"output path component must not be a symlink: {current}")
    allowed_repository_outputs = {
        ROOT / "_site",
        ROOT / "target" / "architecture-pages",
    }
    output = raw_output.resolve()
    raw_is_in_repository = raw_output == ROOT or ROOT in raw_output.parents
    resolved_is_in_repository = output == ROOT or ROOT in output.parents
    if (raw_is_in_repository or resolved_is_in_repository) and not (
        raw_output in allowed_repository_outputs and output in allowed_repository_outputs
    ):
        raise ValueError("output inside the repository must be a designated build directory")

    if output.exists():
        marker = output / OWNERSHIP_MARKER
        if not marker.is_file() or marker.is_symlink() or marker.read_text(encoding="utf-8") != ownership_token(output):
            raise ValueError("existing output is not owned by the architecture Pages builder")
        shutil.rmtree(output)
    output.mkdir(parents=True)
    (output / OWNERSHIP_MARKER).write_text(ownership_token(output), encoding="utf-8")

    for name in STATIC_FILES:
        source = SITE_SOURCE / name
        if not source.is_file() or source.is_symlink():
            raise FileNotFoundError(f"required static asset is missing or unsafe: {source}")
        shutil.copyfile(source, output / name)

    if not MODEL_SOURCE.is_file() or MODEL_SOURCE.is_symlink():
        raise FileNotFoundError(f"canonical model is missing or unsafe: {MODEL_SOURCE}")
    model = json.loads(MODEL_SOURCE.read_text(encoding="utf-8"))
    source_revision = revision or model["source_tree_sha256"]
    if not isinstance(source_revision, str) or not source_revision or any(character not in "0123456789abcdef" for character in source_revision.lower()):
        raise ValueError("revision must be a hexadecimal commit or source-tree digest")
    shutil.copyfile(MODEL_SOURCE, output / "architecture.json")
    (output / "build-meta.json").write_text(
        json.dumps(
            {
                "repository": model["repository"],
                "schema_version": model["schema_version"],
                "source_revision": source_revision,
                "source_tree_sha256": model["source_tree_sha256"],
            },
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    (output / ".nojekyll").write_text("", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "target" / "architecture-pages")
    parser.add_argument("--revision")
    args = parser.parse_args()
    build(args.output, args.revision)
    print(f"built architecture showcase at {args.output.resolve()}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
