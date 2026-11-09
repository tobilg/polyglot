import json
import os
import re
import subprocess
import tempfile
import textwrap
import unittest
from itertools import product
from pathlib import Path

from scripts import check_project_consistency as consistency


class ConsistencyCheckerTests(unittest.TestCase):
    def test_dialect_set_reports_alias_duplicates_and_missing_values(self):
        issues = consistency.compare_dialect_set(
            "test", ["postgres", "postgresql", "mysql"], {"postgresql", "generic"}
        )

        self.assertIn("test: duplicate dialects: postgresql", issues)
        self.assertIn("test: missing dialects: generic", issues)
        self.assertIn("test: unexpected dialects: mysql", issues)

    def test_marketing_copy_accepts_stable_wording(self):
        self.assertEqual(
            consistency.marketing_copy_issues(
                Path("README.md"), "Supports more than 30 SQL dialects."
            ),
            [],
        )

    def test_marketing_copy_rejects_exact_count(self):
        issues = consistency.marketing_copy_issues(
            Path("README.md"), "Supports 34 dialects."
        )

        self.assertEqual(len(issues), 2)
        self.assertIn("must use the phrase", issues[0])
        self.assertIn("exact dialect-count claim", issues[1])

    def test_version_check_reports_workspace_package_drift(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._write_version_fixture(root, package_version="0.5.0")
            metadata = self._metadata(root, package_version="0.6.0")

            issues = consistency.check_versions(root, metadata)

            self.assertTrue(any("packages/sdk/package.json" in issue for issue in issues))

    def test_version_check_reports_every_rust_dependency_format_without_writing(self):
        examples = (
            'polyglot-sql = { version = "0.5.0", default-features = false }\n'
            'polyglot-sql = {\n    version = "0.5.1",\n    features = ["generate"],\n}\n'
            "polyglot-sql={features=['transpile'], version='0.5.2'}\n"
            'polyglot-sql = "0.5.3"\n'
        )
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._write_version_fixture(root, package_version="0.6.0")
            readme = root / "crates/polyglot-sql/README.md"
            # Include an up-to-date single-line example, just like the original
            # bug: that must not hide stale versions in the remaining examples.
            text = readme.read_text(encoding="utf-8") + examples
            readme.write_text(text, encoding="utf-8")

            issues = consistency.check_versions(root, self._metadata(root, "0.6.0"))

            self.assertEqual(len(issues), 4)
            for patch in range(4):
                self.assertTrue(any(f"'0.5.{patch}'" in issue for issue in issues))
            self.assertTrue(all("crates/polyglot-sql/README.md:" in issue for issue in issues))
            self.assertEqual(readme.read_text(encoding="utf-8"), text)

    def test_version_reference_does_not_match_another_dependency(self):
        text = (
            'polyglot-sql = { path = "../core" }\n'
            'unrelated = { version = "0.5.0" }\n'
        )
        self.assertEqual(list(consistency.RUST_DEPENDENCY_VERSION.finditer(text)), [])

    def test_version_sync_updates_all_active_references_and_preserves_other_content(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._write_version_fixture(root, package_version="0.6.0")
            for path, _, _ in consistency.VERSION_REFERENCES:
                file = root / path
                file.write_text(
                    file.read_text(encoding="utf-8").replace("0.6.0", "0.5.0"),
                    encoding="utf-8",
                )
            readme = root / "crates/polyglot-sql/README.md"
            text = (
                '# Usage\n\n```toml\n'
                'polyglot-sql = { version = "0.5.0", default-features = false }\n'
                'polyglot-sql = {\n    version = "0.5.1",\n    features = ["generate"],\n}\n'
                "polyglot-sql={features=['transpile'], version='0.5.2'}\n"
                'polyglot-sql = "0.5.3"\n'
                'unrelated = { version = "0.5.0" }\n```\n'
            )
            readme.write_text(text, encoding="utf-8")
            historical = 'polyglot-sql = { version = "0.5.0" }\n'
            for path in ("CHANGELOG.md", "docs/current-benchmarks.md"):
                self._write(root / path, historical)

            changed = consistency.sync_version_references(root)

            self.assertEqual(set(changed), {path for path, _, _ in consistency.VERSION_REFERENCES})
            self.assertEqual(consistency.check_versions(root, self._metadata(root, "0.6.0")), [])
            expected = text.replace('version = "0.5.0",', 'version = "0.6.0",')
            for patch in range(1, 4):
                expected = expected.replace(f"0.5.{patch}", "0.6.0")
            self.assertEqual(readme.read_text(encoding="utf-8"), expected)
            for path in ("CHANGELOG.md", "docs/current-benchmarks.md"):
                self.assertEqual((root / path).read_text(encoding="utf-8"), historical)
            self.assertEqual(consistency.sync_version_references(root), [])

    def test_version_check_covers_catalog_readme_and_go_tag(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._write_version_fixture(root, package_version="0.6.0")
            for path in (
                "crates/polyglot-sql-function-catalogs/README.md",
                "packages/go/README.md",
            ):
                file = root / path
                file.write_text(
                    file.read_text(encoding="utf-8").replace("0.6.0", "0.5.0"),
                    encoding="utf-8",
                )

            issues = consistency.check_versions(root, self._metadata(root, "0.6.0"))

            self.assertEqual(len(issues), 2)
            self.assertTrue(any("function-catalogs/README.md" in issue for issue in issues))
            self.assertTrue(any("packages/go/README.md" in issue for issue in issues))

    def test_version_sync_checks_all_references_before_writing(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._write_version_fixture(root, package_version="0.6.0")
            stale = 'polyglot-sql = { version = "0.5.0" }\n'
            self._write(root / "README.md", stale)
            self._write(root / "packages/go/README.md", "No release tag example.\n")

            with self.assertRaisesRegex(ValueError, "missing Go release tag example"):
                consistency.sync_version_references(root)

            self.assertEqual((root / "README.md").read_text(encoding="utf-8"), stale)
            self.assertTrue(any(
                "missing Go release tag example" in issue
                for issue in consistency.check_version_references(root, "0.6.0")
            ))

    def test_version_check_reports_unversioned_publishable_path_dependency(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._write_version_fixture(root, package_version="0.6.0")
            metadata = self._metadata(
                root,
                package_version="0.6.0",
                dependencies=[
                    {
                        "name": "polyglot-sql-ast-derive",
                        "path": str(root / "crates/polyglot-sql-ast-derive"),
                        "req": "*",
                        "kind": None,
                    }
                ],
            )

            issues = consistency.check_versions(root, metadata)

            self.assertTrue(any("must specify version '0.6.0'" in issue for issue in issues))

    def test_version_check_reports_nonpublishable_path_dependency(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._write_version_fixture(root, package_version="0.6.0")
            metadata = self._metadata(
                root,
                package_version="0.6.0",
                dependencies=[
                    {
                        "name": "polyglot-sql-ast-derive",
                        "path": str(root / "crates/polyglot-sql-ast-derive"),
                        "req": "^0.6.0",
                        "kind": None,
                    }
                ],
            )
            derive_id = f"path+file://{root}/crates/polyglot-sql-ast-derive#0.6.0"
            metadata["workspace_members"].append(derive_id)
            metadata["packages"].append(
                {
                    "id": derive_id,
                    "name": "polyglot-sql-ast-derive",
                    "version": "0.6.0",
                    "manifest_path": str(
                        root / "crates/polyglot-sql-ast-derive/Cargo.toml"
                    ),
                    "dependencies": [],
                    "publish": [],
                }
            )

            issues = consistency.check_versions(root, metadata)

            self.assertTrue(any("depends on non-publishable package" in issue for issue in issues))

    def test_historical_versions_are_outside_active_copy_check(self):
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            self._write_marketing_fixture(root)
            (root / "CHANGELOG.md").write_text(
                "Version 0.1.0 supported 12 dialects.\n", encoding="utf-8"
            )

            self.assertEqual(consistency.check_marketing_copy(root), [])

    @staticmethod
    def _write(path: Path, content: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")

    @classmethod
    def _write_marketing_fixture(cls, root: Path) -> None:
        for path in consistency.MARKETING_PATHS:
            cls._write(root / path, "Supports more than 30 SQL dialects.\n")

    @classmethod
    def _write_version_fixture(cls, root: Path, package_version: str) -> None:
        cls._write(
            root / "Cargo.toml",
            '[workspace]\nmembers = []\n[workspace.package]\nversion = "0.6.0"\n',
        )
        cls._write(
            root / "packages/sdk/package.json",
            json.dumps({"name": "@polyglot-sql/sdk", "version": package_version}),
        )
        cls._write(root / "packages/go/types.go", 'const sdkVersion = "0.6.0"\n')
        cls._write(root / "packages/go/README.md", 'Example: `packages/go/v0.6.0`.\n')
        cls._write(
            root / "README.md", 'polyglot-sql = { version = "0.6.0" }\n'
        )
        cls._write(
            root / "crates/polyglot-sql/README.md",
            'polyglot-sql = { version = "0.6.0" }\n',
        )
        cls._write(
            root / "crates/polyglot-sql-function-catalogs/README.md",
            'polyglot-sql = { version = "0.6.0", features = ["function-catalog-clickhouse"] }\n',
        )
        cls._write(
            root / "examples/rust/Cargo.toml",
            '[dependencies]\npolyglot-sql = { version = "0.6.0", path = "../../crates/polyglot-sql" }\n',
        )

    @staticmethod
    def _metadata(
        root: Path, package_version: str, dependencies: list[dict] | None = None
    ) -> dict:
        package_id = f"path+file://{root}/crates/polyglot-sql#0.6.0"
        return {
            "workspace_members": [package_id],
            "packages": [
                {
                    "id": package_id,
                    "name": "polyglot-sql",
                    "version": package_version,
                    "manifest_path": str(root / "crates/polyglot-sql/Cargo.toml"),
                    "dependencies": dependencies or [],
                    "publish": None,
                }
            ],
        }


class RustCiConsistencyTests(unittest.TestCase):
    """Keep the parallel CI suites consistent with local verification."""

    @staticmethod
    def _make_plan(*targets: str) -> str:
        return subprocess.check_output(
            ["make", "--no-print-directory", "-n", *targets],
            cwd=consistency.PROJECT_ROOT,
            text=True,
        )

    def test_ci_suites_preserve_local_verification_commands(self):
        local = self._make_plan("test-rust-verify")
        ci = self._make_plan(
            "test-rust-ci-core", "test-rust-ci-release-fixtures",
            "test-rust-ci-bindings", "test-rust-ci-feature-gates",
        )
        def commands(plan: str) -> set[str]:
            return {line for line in plan.splitlines() if line.startswith("cargo ")}

        self.assertLessEqual(commands(local), commands(ci))
        for target in (
            "sqlglot_identity", "sqlglot_dialect_identity", "sqlglot_transpilation",
            "sqlglot_transpile", "sqlglot_parser", "sqlglot_pretty",
            "custom_dialect_tests", "custom_clickhouse_parser", "custom_clickhouse_coverage",
            "deep_nesting_regression",
        ):
            with self.subTest(target=target):
                self.assertEqual(len(re.findall(rf"--test {target}(?: |$)", ci, re.M)), 1)
        self.assertIn("cargo check --manifest-path examples/rust/Cargo.toml", ci)
        self.assertIn("cargo test -p polyglot-sql-wasm --lib -- --nocapture", ci)
        self.assertIn("cargo build -p polyglot-sql-ffi --profile ffi_release", ci)

    def test_release_suite_preserves_stack_and_profile(self):
        plan = self._make_plan("test-rust-ci-release-fixtures")
        self.assertIn("RUST_MIN_STACK=16777216", plan)
        commands = [line for line in plan.splitlines() if line.startswith("cargo ")]
        self.assertEqual(len(commands), 3)
        self.assertTrue(all("--release" in command for command in commands))

    def test_ci_gate_rejects_failed_skipped_cancelled_or_missing_results(self):
        workflow = (consistency.PROJECT_ROOT / ".github/workflows/ci.yml").read_text()
        # Extract this job's literal shell block without adding a YAML dependency
        # to the standard-library-only consistency checks. YAML syntax is linted
        # separately; execute the actual gate rather than a copy of its logic.
        match = re.search(r"(?ms)^  rust-test:\n(.*?)(?=^  [\w-]+:|\Z)", workflow)
        self.assertIsNotNone(match)
        job = match.group(1)
        self.assertIn("if: always()", job)
        self.assertIn("needs: [quality, rust-test-suite]", job)
        script = textwrap.dedent(job.split("        run: |\n", 1)[1])
        for quality, suites in product(("success", "failure", "skipped", "cancelled", ""), repeat=2):
            with self.subTest(quality=quality, suites=suites):
                result = subprocess.run(
                    ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", script],
                    env={**os.environ, "QUALITY_RESULT": quality, "SUITES_RESULT": suites},
                    capture_output=True,
                    text=True,
                )
                self.assertEqual(result.returncode == 0, quality == suites == "success")


if __name__ == "__main__":
    unittest.main()
