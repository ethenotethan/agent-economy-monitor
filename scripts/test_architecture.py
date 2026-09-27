#!/usr/bin/env python3
"""Tests for the repository-owned architecture compiler."""

from __future__ import annotations

import copy
import importlib.util
import json
import shutil
import subprocess
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "scripts" / "build_architecture.py"
CHECKER_PATH = ROOT / "scripts" / "check_architecture_contract.py"


def load_compiler():
    spec = importlib.util.spec_from_file_location("build_architecture", MODULE_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_contract_checker():
    spec = importlib.util.spec_from_file_location("check_architecture_contract", CHECKER_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ArchitectureCompilerTests(unittest.TestCase):
    @staticmethod
    def copy_repository(root: Path) -> None:
        shutil.copytree(
            ROOT,
            root,
            dirs_exist_ok=True,
            ignore=shutil.ignore_patterns(".git", ".worktrees", "__pycache__", "target"),
        )

    @staticmethod
    def upstream_contract_bytes(root: Path):
        source_root = root / "architecture" / "contract"
        upstream = {
            "docs/api/architecture-document-v1.schema.json": (source_root / "architecture-document-v1.schema.json").read_bytes(),
            "tui_gateway/architecture_contract.py": (source_root / "architecture_contract.py").read_bytes(),
        }

        def fetch(url: str) -> bytes:
            for path, content in upstream.items():
                if url.endswith("/" + path):
                    return content
            raise AssertionError(f"unexpected upstream URL: {url}")

        return fetch

    def test_contract_check_rejects_repository_local_repinning(self) -> None:
        checker = load_contract_checker()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / "architecture", root / "architecture")
            module_path = root / "architecture" / "contract" / "architecture_contract.py"
            module_path.write_text(
                module_path.read_text(encoding="utf-8") + "\n# locally repinned drift\n",
                encoding="utf-8",
            )
            pins_path = root / "architecture" / "contract" / "pins.json"
            pins = json.loads(pins_path.read_text(encoding="utf-8"))
            pins["sha256"][module_path.name] = checker.sha256(module_path)
            pins_path.write_text(json.dumps(pins, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            self.assertTrue(
                any("immutable Harness source" in problem for problem in checker.evaluate(root, self.upstream_contract_bytes(ROOT))),
                checker.evaluate(root, self.upstream_contract_bytes(ROOT)),
            )

    def test_contract_check_fails_closed_on_fetch_errors(self) -> None:
        checker = load_contract_checker()

        def unavailable(_url: str) -> bytes:
            raise OSError("offline")

        problems = checker.evaluate(ROOT, unavailable)
        self.assertTrue(any("cannot fetch immutable Harness source" in problem for problem in problems), problems)

    def test_contract_check_rejects_source_metadata_drift(self) -> None:
        checker = load_contract_checker()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            shutil.copytree(ROOT / "architecture", root / "architecture")
            pins_path = root / "architecture" / "contract" / "pins.json"
            pins = json.loads(pins_path.read_text(encoding="utf-8"))
            pins.setdefault("source", {})["commit"] = "0" * 40
            pins_path.write_text(json.dumps(pins, indent=2, sort_keys=True) + "\n", encoding="utf-8")

            problems = checker.evaluate(root, self.upstream_contract_bytes(ROOT))
            self.assertTrue(any("source metadata" in problem for problem in problems), problems)

    def test_compiles_a_conforming_system_map(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)

        self.assertEqual([], architecture.validate_model(model))
        self.assertTrue(model["interplay"]["nodes"])
        self.assertTrue(model["extraction"]["entities"])
        self.assertEqual(
            {"architecture", "verify"},
            {job["id"] for job in model["ci"]["jobs"]},
        )

    def test_ci_extraction_rejects_named_jobs_that_do_not_run_the_gates(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow = root / ".github" / "workflows" / "ci.yml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text(
                """name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
jobs:
  architecture:
    runs-on: ubuntu-latest
    steps:
      - run: echo architecture skipped
  verify:
    runs-on: ubuntu-latest
    steps:
      - run: echo behavior skipped
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                architecture.parse_ci(root)

    def test_ci_extraction_rejects_an_unmapped_workflow(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflows = root / ".github" / "workflows"
            workflows.mkdir(parents=True)
            shutil.copy2(ROOT / ".github" / "workflows" / "ci.yml", workflows / "ci.yml")
            (workflows / "deploy.yml").write_text(
                "name: Deploy\non:\n  workflow_dispatch:\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo deploy\n",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "unmapped GitHub Actions workflow"):
                architecture.parse_ci(root)

    def test_compiler_rejects_an_undeclared_axum_route(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status))\n        .route("/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "undeclared Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_undeclared_whitespace_separated_axum_route(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status))\n'
                    '        . route("/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "undeclared Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_with_a_comment_after_the_dot(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status))\n'
                    '        . /* architecture-bypass */ route("/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "undeclared Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_with_a_comment_before_the_parenthesis(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status))\n'
                    '        .route /* architecture-bypass */ ("/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "undeclared Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_undeclared_raw_identifier_axum_route(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status))\n'
                    '        .r#route("/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "undeclared Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_axum_macro_method_indirection(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    "#[tokio::main]",
                    """macro_rules! add_route {
    ($router:expr, $method:ident, $path:expr, $handler:expr) => {
        $router.$method($path, $handler)
    };
}

#[tokio::main]""",
                ).replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let app = add_route!(app, route, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                architecture.ArchitectureError,
                "unsupported Axum macro method indirection",
            ):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_ufcs_call(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let _app = Router::route(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_ufcs_call_through_a_type_alias(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8")
                .replace("use axum::{Json, Router, routing::get};", "use axum::{Json, Router, routing::get};\n\ntype AliasRouter = Router;")
                .replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let _app = AliasRouter::route(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_ufcs_call_through_an_absolute_type_alias(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8")
                .replace(
                    "use axum::{Json, Router, routing::get};",
                    "use axum::{Json, Router, routing::get};\n\n"
                    "type AliasRouter = ::axum::Router;",
                )
                .replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let app = AliasRouter::route(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            subprocess.run(
                ["cargo", "check", "--quiet"],
                cwd=root,
                check=True,
            )
            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_a_parenthesized_axum_route_function_item_call(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let app = (Router::route)(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            subprocess.run(
                ["cargo", "check", "--quiet"],
                cwd=root,
                check=True,
            )
            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_ufcs_call_through_a_generic_type_alias(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8")
                .replace(
                    "use axum::{Json, Router, routing::get};",
                    "use axum::{Json, Router, routing::get};\n\n"
                    "#[allow(type_alias_bounds)]\n"
                    "type AliasRouter<S: Clone = ()> = Router<S>;",
                )
                .replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let app = AliasRouter::route(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            subprocess.run(
                ["cargo", "check", "--quiet"],
                cwd=root,
                check=True,
            )
            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_ufcs_call_through_a_where_clause_alias(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8")
                .replace(
                    "use axum::{Json, Router, routing::get};",
                    "use axum::{Json, Router, routing::get};\n\n"
                    "#[allow(type_alias_bounds)]\n"
                    "type AliasRouter<S = ()> where S: Clone = Router<S>;",
                )
                .replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let app = AliasRouter::route(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            subprocess.run(
                ["cargo", "check", "--quiet"],
                cwd=root,
                check=True,
            )
            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_ufcs_call_through_an_import_alias(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8")
                .replace("use axum::{Json, Router, routing::get};", "use axum::{Json, Router as AliasRouter, routing::get};")
                .replace("Router::new()", "AliasRouter::new()")
                .replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let _app = AliasRouter::route(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_ufcs_call_through_a_cross_file_alias(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            (root / "src" / "router_alias.rs").write_text(
                "pub type AliasRouter = axum::Router;\n",
                encoding="utf-8",
            )
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8")
                .replace(
                    "use axum::{Json, Router, routing::get};",
                    "mod router_alias;\n\nuse axum::{Json, Router, routing::get};\n"
                    "use router_alias::AliasRouter;",
                )
                .replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let app = AliasRouter::route(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            subprocess.run(
                ["cargo", "check", "--quiet"],
                cwd=root,
                check=True,
            )
            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_ufcs_call_through_a_reexported_alias(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            (root / "src" / "router_alias.rs").write_text(
                "pub type AliasRouter = axum::Router;\n",
                encoding="utf-8",
            )
            (root / "src" / "router_reexport.rs").write_text(
                "pub use crate::router_alias::AliasRouter as ReexportedRouter;\n",
                encoding="utf-8",
            )
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8")
                .replace(
                    "use axum::{Json, Router, routing::get};",
                    "mod router_alias;\nmod router_reexport;\n\n"
                    "use axum::{Json, Router, routing::get};\n"
                    "use router_reexport::ReexportedRouter;",
                )
                .replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status));\n'
                    '    let app = ReexportedRouter::route(app, "/admin", get(status));',
                ),
                encoding="utf-8",
            )

            subprocess.run(
                ["cargo", "check", "--quiet"],
                cwd=root,
                check=True,
            )
            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_undeclared_supported_axum_routes(self) -> None:
        architecture = load_compiler()
        for method in ("post", "put", "patch", "delete", "head", "options", "trace", "any"):
            with self.subTest(method=method), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.copy_repository(root)
                main = root / "src" / "main.rs"
                main.write_text(
                    main.read_text(encoding="utf-8").replace(
                        '.route("/api/v1/status", get(status));',
                        '.route("/api/v1/status", get(status))\n'
                        f'        .route("/admin", {method}(status));',
                    ),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(architecture.ArchitectureError, "undeclared Axum route"):
                    architecture.compile_architecture(root)

    def test_compiler_binds_axum_route_method_and_path(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", post(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "route origin GET"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_unrecognized_axum_method_router(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status))\n'
                    '        .route("/admin", on(MethodFilter::POST, status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_a_nonliteral_axum_route_path(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status))\n'
                    "        .route(ADMIN_PATH, post(status));",
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_an_axum_route_service(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.copy_repository(root)
            main = root / "src" / "main.rs"
            main.write_text(
                main.read_text(encoding="utf-8").replace(
                    '.route("/api/v1/status", get(status));',
                    '.route("/api/v1/status", get(status))\n'
                    '        .route_service("/admin", get(status));',
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "unsupported Axum route_service"):
                architecture.compile_architecture(root)

    def test_compiler_rejects_unmodeled_axum_router_composition(self) -> None:
        architecture = load_compiler()
        calls = {
            "nest": '.nest("/admin", Router::new())',
            "nest_service": '.nest_service("/admin", get(status))',
            "fallback": ".fallback(status)",
            "fallback_service": ".fallback_service(get(status))",
            "merge": ".merge(Router::new())",
        }
        for constructor, call in calls.items():
            with self.subTest(constructor=constructor), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.copy_repository(root)
                main = root / "src" / "main.rs"
                main.write_text(
                    main.read_text(encoding="utf-8").replace(
                        '.route("/api/v1/status", get(status));',
                        f'.route("/api/v1/status", get(status))\n        {call};',
                    ),
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(
                    architecture.ArchitectureError,
                    f"unsupported Axum {constructor}",
                ):
                    architecture.compile_architecture(root)

    def test_model_validation_rejects_unknown_static_check_scripts(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)
        model["ci"]["static_checks"][0]["scripts"].append("scripts/not-present.py")

        self.assertTrue(
            any("ci.static_checks" in problem and "scripts/not-present.py" in problem for problem in architecture.validate_model(model)),
            architecture.validate_model(model),
        )

    def test_model_validation_rejects_unknown_page_components(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)
        model["interplay"]["pages"][0]["components"].append("missing-component")

        self.assertTrue(
            any("interplay.pages" in problem and "missing-component" in problem for problem in architecture.validate_model(model)),
            architecture.validate_model(model),
        )

    def test_model_validation_rejects_duplicate_extraction_passes(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)
        model["extraction"]["passes"].append(copy.deepcopy(model["extraction"]["passes"][0]))

        self.assertTrue(
            any("extraction.passes: duplicate id" in problem for problem in architecture.validate_model(model)),
            architecture.validate_model(model),
        )

    def test_model_validation_rejects_duplicate_pages(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)
        model["interplay"]["pages"].append(copy.deepcopy(model["interplay"]["pages"][0]))

        self.assertTrue(
            any("interplay.pages: duplicate id" in problem for problem in architecture.validate_model(model)),
            architecture.validate_model(model),
        )

    def test_model_validation_rejects_extraction_family_rule_mismatches(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)
        entity = next(item for item in model["extraction"]["entities"] if item["origins"])
        entity["origins"][0]["family"] = "mechanical" if entity["origins"][0]["family"] == "specified" else "specified"

        problems = architecture.validate_model(model)
        self.assertTrue(any("origin family" in problem for problem in problems), problems)

    def test_runtime_service_uses_the_family_declared_for_its_extraction_rule(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)
        runtime = next(entity for entity in model["extraction"]["entities"] if entity["id"] == "runtime-service")

        self.assertEqual("declared.topology", runtime["origins"][0]["rule"])
        self.assertEqual("specified", runtime["origins"][0]["family"])

    def test_renderer_contract_rejects_malformed_optional_collections(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)
        cases = [
            ("stores", "items", "stores.items"),
            ("externals", "systems", "externals.systems"),
            ("externals", "groups", "externals.groups"),
        ]
        for section, field, expected in cases:
            malformed = copy.deepcopy(model)
            malformed[section][field] = None
            self.assertTrue(
                any(expected in problem for problem in architecture.validate_renderer_contract(malformed)),
                expected,
            )

    def test_model_is_byte_deterministic_and_every_tab_has_renderer_data(self) -> None:
        architecture = load_compiler()
        first = architecture.compile_architecture(ROOT)
        second = architecture.compile_architecture(ROOT)
        self.assertEqual(architecture.serialized_json(first), architecture.serialized_json(second))
        json.loads(architecture.serialized_json(first))
        self.assertTrue(first["interplay"]["nodes"])
        self.assertTrue(first["extraction"]["files"])
        self.assertTrue(first["ci"]["jobs"])
        self.assertTrue(first["components"])

    def test_config_rejects_an_external_node_without_an_explicit_type(self) -> None:
        architecture = load_compiler()
        config = architecture.load_json(ROOT / "architecture" / "config.json")
        config["externals"] = [
            item for item in config["externals"] if item["id"] != "agentcash"
        ]

        with self.assertRaisesRegex(architecture.ArchitectureError, "external systems"):
            architecture.validate_config(config)

    def test_system_map_has_exact_provenance_boundaries_flows_and_gates(self) -> None:
        architecture = load_compiler()
        model = architecture.compile_architecture(ROOT)
        interplay = model["interplay"]
        nodes = {node["id"]: node for node in interplay["nodes"]}
        entities = {entity["id"]: entity for entity in model["extraction"]["entities"]}
        edge_keys = {
            (edge["source"], edge["target"], edge["relation"])
            for edge in interplay["edges"]
        }

        self.assertEqual(set(nodes), set(entities))
        self.assertNotIn("shared", {node.get("page") for node in nodes.values()})
        for node in nodes.values():
            self.assertTrue(node["evidence"], node["id"])
            if node["kind"] != "external":
                self.assertTrue(node["page"], node["id"])
        for flow in interplay["flows"]:
            for step in flow["steps"]:
                self.assertIn((step["from"], step["to"], step["relation"]), edge_keys)

        grouped = {
            member
            for group in interplay["boundary_groups"]
            for member in group["members"]
        }
        boundary_nodes = {
            node["id"] for node in nodes.values() if node["kind"] in {"external", "store"}
        }
        self.assertEqual(boundary_nodes, grouped)
        coverage = model["extraction"]["coverage"]
        self.assertTrue(coverage["specified"])
        self.assertTrue(coverage["mechanically_extracted"])
        self.assertTrue(coverage["unresolved"])
        self.assertEqual(
            {"architecture", "verify"},
            set(model["ci"]["merge"]["inputs"]),
        )

    def test_check_mode_detects_stale_bytes_without_mutating_them(self) -> None:
        architecture = load_compiler()
        expected = architecture.serialized_json(architecture.compile_architecture(ROOT))
        with tempfile.TemporaryDirectory() as directory:
            model_path = Path(directory) / "model.json"
            model_path.write_text("{}\n", encoding="utf-8")
            before = model_path.read_bytes()
            with self.assertRaisesRegex(architecture.ArchitectureError, "stale"):
                architecture.check_model(expected, model_path)
            self.assertEqual(before, model_path.read_bytes())

    def test_inventory_rejects_rust_source_omitted_by_configured_globs(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "tests").mkdir()
            (root / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
            (root / "tests" / "hidden.rs").write_text("#[test] fn hidden() {}\n", encoding="utf-8")

            with self.assertRaisesRegex(architecture.ArchitectureError, "omits Rust source"):
                architecture.expanded_inventory(root, ["src/**/*.rs"])

    def test_inventory_rejects_non_rust_source_omitted_by_configured_globs(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "scripts").mkdir()
            (root / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
            (root / "scripts" / "hidden.py").write_text("raise SystemExit(0)\n", encoding="utf-8")

            with self.assertRaisesRegex(architecture.ArchitectureError, "omits source file"):
                architecture.expanded_inventory(root, ["src/**/*.rs"])

    def test_inventory_ignores_generated_dev_stack_credentials(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            source = root / "compose.yaml"
            source.write_text("services: {}\n", encoding="utf-8")
            (root / ".dev-stack.env").write_text(
                "POSTGRES_PASSWORD=generated-local-secret\n", encoding="utf-8"
            )

            self.assertEqual(architecture.expanded_inventory(root, ["compose.yaml"]), [source])

    def test_inventory_rejects_root_configuration_and_manifests_when_omitted(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
            (root / "Cargo.lock").write_text("version = 4\n", encoding="utf-8")
            (root / ".env.example").write_text("PORT=8080\n", encoding="utf-8")

            with self.assertRaisesRegex(architecture.ArchitectureError, "Cargo.lock"):
                architecture.expanded_inventory(root, ["src/**/*.rs"])

    def test_inventory_rejects_source_in_an_unenumerated_directory(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            (root / "deploy").mkdir()
            (root / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
            (root / "deploy" / "runtime.yaml").write_text("service: hidden\n", encoding="utf-8")

            with self.assertRaisesRegex(architecture.ArchitectureError, "deploy/runtime.yaml"):
                architecture.expanded_inventory(root, ["src/**/*.rs"])

    def test_inventory_rejects_symlinked_files(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "src").mkdir()
            target = root / "src" / "real.rs"
            target.write_text("fn main() {}\n", encoding="utf-8")
            (root / "src" / "linked.rs").symlink_to(target)

            with self.assertRaisesRegex(architecture.ArchitectureError, "symlink"):
                architecture.expanded_inventory(root, ["src/*.rs"])

    def test_inventory_rejects_paths_resolving_outside_repository_root(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory, tempfile.TemporaryDirectory() as outside:
            root = Path(directory)
            external = Path(outside) / "external.rs"
            external.write_text("fn escaped() {}\n", encoding="utf-8")
            (root / "src").mkdir()
            (root / "src" / "escaped.rs").symlink_to(external)

            with self.assertRaisesRegex(architecture.ArchitectureError, "outside repository root"):
                architecture.expanded_inventory(root, ["src/*.rs"])

    def test_inventory_does_not_ignore_nested_build_or_cache_directories(self) -> None:
        architecture = load_compiler()
        for nested in ("src/target/hidden.rs", "src/__pycache__/hidden.py"):
            with self.subTest(nested=nested), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "src").mkdir()
                (root / "src" / "main.rs").write_text("fn main() {}\n", encoding="utf-8")
                hidden = root / nested
                hidden.parent.mkdir(parents=True)
                hidden.write_text("fn hidden() {}\n" if hidden.suffix == ".rs" else "hidden = True\n", encoding="utf-8")
                (root / "target").mkdir()
                (root / "target" / "ignored.rs").write_text("fn generated() {}\n", encoding="utf-8")

                with self.assertRaisesRegex(architecture.ArchitectureError, "hidden"):
                    architecture.expanded_inventory(root, ["src/main.rs"])

    def test_config_rejects_normalized_shared_core_component_spelling(self) -> None:
        architecture = load_compiler()
        config = architecture.load_json(ROOT / "architecture" / "config.json")
        config["components"].append(
            {
                "description": "Forbidden fallback",
                "id": "Shared_core",
                "label": "Fallback",
                "layer": "runtime",
                "patterns": [],
            }
        )

        with self.assertRaisesRegex(architecture.ArchitectureError, "Shared-core"):
            architecture.validate_config(config)

    def test_config_rejects_every_shared_core_page_spelling(self) -> None:
        architecture = load_compiler()
        config = architecture.load_json(ROOT / "architecture" / "config.json")
        config["pages"].append(
            {"components": [], "id": "shared-core", "label": "Fallback", "roots": []}
        )

        with self.assertRaisesRegex(architecture.ArchitectureError, "Shared-core"):
            architecture.validate_config(config)

        spaced = architecture.load_json(ROOT / "architecture" / "config.json")
        spaced["pages"].append(
            {"components": [], "id": "Shared core", "label": "Fallback", "roots": []}
        )
        with self.assertRaisesRegex(architecture.ArchitectureError, "Shared-core"):
            architecture.validate_config(spaced)

    def test_config_rejects_unresolved_page_roots_components_and_flow_pages(self) -> None:
        architecture = load_compiler()
        cases = []
        unknown_root = architecture.load_json(ROOT / "architecture" / "config.json")
        unknown_root["pages"][0]["roots"] = ["missing-node"]
        cases.append(unknown_root)
        unknown_component = architecture.load_json(ROOT / "architecture" / "config.json")
        unknown_component["pages"][0]["components"] = ["missing-component"]
        cases.append(unknown_component)
        unknown_flow_page = architecture.load_json(ROOT / "architecture" / "config.json")
        unknown_flow_page["flows"][0]["page"] = "missing-page"
        cases.append(unknown_flow_page)

        for config in cases:
            with self.assertRaisesRegex(architecture.ArchitectureError, "page"):
                architecture.validate_config(config)

    def test_config_rejects_external_store_reference_and_duplicate_page_entries(self) -> None:
        architecture = load_compiler()
        cases = []
        external_page = architecture.load_json(ROOT / "architecture" / "config.json")
        next(node for node in external_page["nodes"] if node["kind"] == "external")["page"] = "missing-page"
        cases.append(external_page)
        external_component = architecture.load_json(ROOT / "architecture" / "config.json")
        external_component["externals"][0]["component"] = "missing-component"
        cases.append(external_component)
        store_component = architecture.load_json(ROOT / "architecture" / "config.json")
        store_component["stores"][0]["component"] = "missing-component"
        cases.append(store_component)
        duplicate_roots = architecture.load_json(ROOT / "architecture" / "config.json")
        duplicate_roots["pages"][0]["roots"] *= 2
        cases.append(duplicate_roots)
        duplicate_components = architecture.load_json(ROOT / "architecture" / "config.json")
        duplicate_components["pages"][0]["components"] *= 2
        cases.append(duplicate_components)

        for config in cases:
            with self.assertRaises(architecture.ArchitectureError):
                architecture.validate_config(config)

    def test_renderer_contract_rejects_unresolved_renderer_native_references(self) -> None:
        architecture = load_compiler()
        mutations = [
            lambda model: model["interplay"]["nodes"][0].update(page="missing-page"),
            lambda model: model["interplay"]["pages"][0]["roots"].append("missing-node"),
            lambda model: model["interplay"]["flows"][0].update(page="missing-page"),
            lambda model: model["interplay"]["flows"][0]["steps"][0].update(relation="not-an-edge"),
            lambda model: model["interplay"]["boundary_groups"][0]["members"].append("missing-node"),
            lambda model: model["stores"]["items"][0].update(component="missing-component"),
            lambda model: model["externals"]["systems"][0].update(component="missing-component"),
            lambda model: model["extraction"].pop("families"),
        ]
        for mutate in mutations:
            model = architecture.compile_architecture(ROOT)
            mutate(model)
            self.assertTrue(architecture.validate_renderer_contract(model))

    def test_renderer_contract_rejects_duplicate_page_roots_and_components(self) -> None:
        architecture = load_compiler()
        for field in ("roots", "components"):
            model = architecture.compile_architecture(ROOT)
            model["interplay"]["pages"][0][field] *= 2
            problems = architecture.validate_renderer_contract(model)
            self.assertTrue(any(f"duplicate {field}" in problem for problem in problems), problems)

    def test_ci_rejects_workflows_without_pull_request_and_push_main_triggers(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow = root / ".github" / "workflows" / "ci.yml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text(
                """name: CI
on:
  workflow_dispatch:
jobs:
  architecture:
    runs-on: ubuntu-latest
    steps:
      - run: |
          python3 -m unittest scripts.test_architecture -v
          python3 scripts/check_architecture_contract.py
          python3 scripts/build_architecture.py --check
  verify:
    runs-on: ubuntu-latest
    steps:
      - run: ./scripts/verify
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                architecture.parse_ci(root)

    def test_ci_rejects_restrictive_pull_request_and_push_filters(self) -> None:
        architecture = load_compiler()
        trigger_cases = {
            "pull request wrong branch": "  push:\n    branches: [main]\n  pull_request:\n    branches: [not-main]\n",
            "pull request types": "  push:\n    branches: [main]\n  pull_request:\n    types: [opened]\n",
            "pull request paths": "  push:\n    branches: [main]\n  pull_request:\n    paths: [src/**]\n",
            "pull request paths ignore": "  push:\n    branches: [main]\n  pull_request:\n    paths-ignore: [docs/**]\n",
            "push wrong branch": "  push:\n    branches: [not-main]\n  pull_request:\n",
            "push branch ignore": "  push:\n    branches: [main]\n    branches-ignore: [main]\n  pull_request:\n",
            "push paths": "  push:\n    branches: [main]\n    paths: [src/**]\n  pull_request:\n",
            "push paths ignore": "  push:\n    branches: [main]\n    paths-ignore: [docs/**]\n  pull_request:\n",
        }
        jobs = """jobs:
  architecture:
    runs-on: ubuntu-latest
    steps:
      - run: |
          python3 -m unittest scripts.test_architecture -v
          python3 scripts/check_architecture_contract.py
          python3 scripts/build_architecture.py --check
  verify:
    runs-on: ubuntu-latest
    steps:
      - run: ./scripts/verify
"""
        for name, triggers in trigger_cases.items():
            with self.subTest(name=name), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                workflow = root / ".github" / "workflows" / "ci.yml"
                workflow.parent.mkdir(parents=True)
                workflow.write_text("name: CI\non:\n" + triggers + jobs, encoding="utf-8")

                with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                    architecture.parse_ci(root)

    def test_ci_rejects_checkout_ref_overrides_in_required_jobs(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow = root / ".github" / "workflows" / "ci.yml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text(
                """name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
jobs:
  architecture:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
        with:
          ref: main
      - run: |
          python3 -m unittest scripts.test_architecture -v
          python3 scripts/check_architecture_contract.py
          python3 scripts/build_architecture.py --check
  verify:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v5
      - run: ./scripts/verify
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                architecture.parse_ci(root)

    def test_ci_rejects_checkout_ref_overrides_for_quoted_uses_and_ref_keys(self) -> None:
        architecture = load_compiler()
        checkout_steps = [
            '      - uses: "actions/checkout@v5"\n        with:\n          ref: main\n',
            "      - uses: 'actions/checkout@v5'\n        with:\n          'ref': main\n",
            '      - uses: actions/checkout@v5\n        with: {"ref": main}\n',
            '      - "uses": actions/checkout@v5\n        with: {ref: main}\n',
            '      - {uses: actions/checkout@v5, with: {ref: main}}\n',
        ]
        for checkout_step in checkout_steps:
            with self.subTest(checkout_step=checkout_step), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                workflow = root / ".github" / "workflows" / "ci.yml"
                workflow.parent.mkdir(parents=True)
                workflow.write_text(
                    "name: CI\non:\n  push:\n    branches: [main]\n  pull_request:\njobs:\n  architecture:\n"
                    + "    runs-on: ubuntu-latest\n    steps:\n"
                    + checkout_step
                    + "      - run: |\n"
                    + "          python3 -m unittest scripts.test_architecture -v\n"
                    + "          python3 scripts/check_architecture_contract.py\n"
                    + "          python3 scripts/build_architecture.py --check\n"
                    + "  verify:\n    runs-on: ubuntu-latest\n    steps:\n"
                    + "      - run: ./scripts/verify\n",
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                    architecture.parse_ci(root)

    def test_ci_rejects_quoted_trigger_controls_and_execution_environment(self) -> None:
        architecture = load_compiler()
        canonical = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
        mutations = [
            canonical.replace(
                "  pull_request:\n    branches: [main]\n",
                '  pull_request:\n    "paths-ignore": ["**"]\n',
            ),
            canonical.replace(
                "  architecture:\n    name: architecture\n",
                "  architecture:\n    name: architecture\n    env: {BASH_ENV: ./attacker.sh}\n",
            ),
        ]
        for changed in mutations:
            with self.subTest(changed=changed), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                workflow = root / ".github" / "workflows" / "ci.yml"
                workflow.parent.mkdir(parents=True)
                workflow.write_text(changed, encoding="utf-8")
                with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                    architecture.parse_ci(root)

    def test_ci_rejects_required_commands_hidden_in_a_non_executing_branch(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow = root / ".github" / "workflows" / "ci.yml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text(
                """name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
jobs:
  architecture:
    runs-on: ubuntu-latest
    steps:
      - run: |
          if false; then
            python3 -m unittest scripts.test_architecture -v
            python3 scripts/check_architecture_contract.py
            python3 scripts/build_architecture.py --check
          fi
  verify:
    runs-on: ubuntu-latest
    steps:
      - run: ./scripts/verify
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                architecture.parse_ci(root)

    def test_ci_rejects_conditional_or_non_blocking_required_jobs(self) -> None:
        architecture = load_compiler()
        controls = ["    if: false\n", "      - continue-on-error: true\n"]
        for control in controls:
            with tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                workflow = root / ".github" / "workflows" / "ci.yml"
                workflow.parent.mkdir(parents=True)
                workflow.write_text(
                    "name: CI\non:\n  push:\n    branches: [main]\n  pull_request:\n    branches: [main]\njobs:\n  architecture:\n"
                    + control
                    + "    runs-on: ubuntu-latest\n    steps:\n"
                    + "      - run: |\n"
                    + "          python3 -m unittest scripts.test_architecture -v\n"
                    + "          python3 scripts/check_architecture_contract.py\n"
                    + "          python3 scripts/build_architecture.py --check\n"
                    + "  verify:\n    runs-on: ubuntu-latest\n    steps:\n"
                    + "      - run: ./scripts/verify\n",
                    encoding="utf-8",
                )

                with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                    architecture.parse_ci(root)

    def test_ci_rejects_a_custom_shell_for_required_gate_commands(self) -> None:
        architecture = load_compiler()
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            workflow = root / ".github" / "workflows" / "ci.yml"
            workflow.parent.mkdir(parents=True)
            workflow.write_text(
                """name: CI
on:
  push:
    branches: [main]
  pull_request:
    branches: [main]
jobs:
  architecture:
    runs-on: ubuntu-latest
    steps:
      - name: Architecture
        shell: bash -c 'true' -- {0}
        run: |
          python3 -m unittest scripts.test_architecture -v
          python3 scripts/check_architecture_contract.py
          python3 scripts/build_architecture.py --check
  verify:
    runs-on: ubuntu-latest
    steps:
      - run: ./scripts/verify
""",
                encoding="utf-8",
            )

            with self.assertRaisesRegex(architecture.ArchitectureError, "canonical workflow grammar"):
                architecture.parse_ci(root)

    def test_config_rejects_duplicate_edges_and_non_boundary_group_members(self) -> None:
        architecture = load_compiler()
        duplicate_edge = architecture.load_json(ROOT / "architecture" / "config.json")
        duplicate_edge["interplay_edges"].append(
            copy.deepcopy(duplicate_edge["interplay_edges"][0])
        )
        with self.assertRaisesRegex(architecture.ArchitectureError, "duplicate edge"):
            architecture.validate_config(duplicate_edge)

        non_boundary = architecture.load_json(ROOT / "architecture" / "config.json")
        non_boundary["boundary_groups"][0]["members"].append("runtime-service")
        with self.assertRaisesRegex(architecture.ArchitectureError, "non-boundary"):
            architecture.validate_config(non_boundary)


if __name__ == "__main__":
    unittest.main()
