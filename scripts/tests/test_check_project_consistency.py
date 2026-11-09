import json
import copy
import io
import os
import re
import subprocess
import tempfile
import textwrap
import unittest
from unittest import mock
from itertools import product
from pathlib import Path

from scripts import check_project_consistency as consistency
from scripts import release_recovery


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


class ReleaseRecoveryTests(unittest.TestCase):
    def setUp(self):
        self.sha = "a" * 40
        self.run = {
            "id": 123, "event": "push", "path": ".github/workflows/ci.yml",
            "head_branch": "v0.14.1", "head_sha": self.sha,
            "status": "completed", "conclusion": "failure",
        }
        self.jobs = [
            {"name": name, "conclusion": "success"}
            for name in ("quality", "rust-test", *release_recovery.REUSED_ARTIFACTS.values())
        ]
        self.artifacts = [
            {
                "name": name, "id": index, "expired": False, "size_in_bytes": 1024,
                "workflow_run": {"id": 123, "head_sha": self.sha, "head_branch": "v0.14.1"},
            }
            for index, name in enumerate(release_recovery.REUSED_ARTIFACTS, 1)
        ]

    def validate(self, **overrides):
        args = dict(
            release_tag="v0.14.1", run_id="123", tag_sha=self.sha,
            run=self.run, jobs=self.jobs, artifacts=self.artifacts,
        )
        args.update(overrides)
        return release_recovery.validate_source(**args)

    def test_selects_only_the_required_artifacts_from_the_tagged_run(self):
        extras = [{"name": "dist", "id": 99}, {"name": "ffi-linux-aarch64", "id": 100}]
        self.assertEqual(
            self.validate(artifacts=self.artifacts + extras),
            {"source_sha": self.sha, "artifact_ids": "1,2,3,4,5"},
        )

    def test_rejects_invalid_dispatch_inputs(self):
        for tag, run_id in (
            ("main", "123"), ("v0.14.1\n", "123"), ("v0.14.1;echo bad", "123"),
            ("v0.14.1", "0"), ("v0.14.1", "../123"), ("v0.14.1", "123\n"),
        ):
            with self.subTest(tag=tag, run_id=run_id), self.assertRaises(ValueError):
                release_recovery.validate_inputs(tag, run_id)

    def test_rejects_wrong_commit_tag_workflow_or_incomplete_run(self):
        for key, value in (
            ("id", 456), ("event", "pull_request"), ("head_branch", "main"),
            ("path", ".github/workflows/unrelated.yml"), ("head_sha", "b" * 40),
            ("status", "in_progress"), ("conclusion", "cancelled"),
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                self.validate(run={**self.run, key: value})
        with self.assertRaises(ValueError):
            self.validate(tag_sha="b" * 40)

    def test_requires_successful_tests_and_artifact_producers(self):
        for index in range(len(self.jobs)):
            for conclusion in ("failure", "cancelled", "skipped", None):
                jobs = copy.deepcopy(self.jobs)
                jobs[index]["conclusion"] = conclusion
                with self.subTest(index=index, conclusion=conclusion), self.assertRaises(ValueError):
                    self.validate(jobs=jobs)
            with self.subTest(missing=index), self.assertRaises(ValueError):
                self.validate(jobs=self.jobs[:index] + self.jobs[index + 1:])

    def test_requires_every_artifact_exactly_once(self):
        for index in range(len(self.artifacts)):
            with self.subTest(missing=index), self.assertRaises(ValueError):
                self.validate(artifacts=self.artifacts[:index] + self.artifacts[index + 1:])
            with self.subTest(duplicate=index), self.assertRaises(ValueError):
                self.validate(artifacts=self.artifacts + [self.artifacts[index]])

    def test_rejects_expired_empty_or_mismatched_artifacts(self):
        for update in (
            {"expired": True}, {"size_in_bytes": 0}, {"id": 0},
            {"workflow_run": {"id": 456, "head_sha": self.sha, "head_branch": "v0.14.1"}},
            {"workflow_run": {"id": 123, "head_sha": "b" * 40, "head_branch": "v0.14.1"}},
            {"workflow_run": {"id": 123, "head_sha": self.sha, "head_branch": "main"}},
        ):
            artifacts = copy.deepcopy(self.artifacts)
            artifacts[0].update(update)
            with self.subTest(update=update), self.assertRaises(ValueError):
                self.validate(artifacts=artifacts)

    def test_reads_all_job_pages_and_preserves_the_latest_attempt_filter(self):
        first = [{"id": index} for index in range(100)]
        with mock.patch.object(
            release_recovery, "github_api", side_effect=[{"jobs": first}, {"jobs": [{"id": 100}]}]
        ) as api:
            items = release_recovery.github_items("runs/123/jobs?filter=latest", "jobs")
        self.assertEqual(len(items), 101)
        self.assertEqual(api.call_args_list, [
            mock.call("runs/123/jobs?filter=latest&per_page=100&page=1"),
            mock.call("runs/123/jobs?filter=latest&per_page=100&page=2"),
        ])


    def recovery_build_args(self):
        return dict(
            release_tag="v0.14.1", source_run_id="123", source_sha=self.sha,
            recovery_run_id="456",
            run={
                "id": 456, "event": "workflow_dispatch", "head_branch": "main",
                "head_sha": "b" * 40, "status": "completed", "conclusion": "failure",
                "path": ".github/workflows/recover-release.yml",
            },
            jobs=[{"name": name, "conclusion": "success"} for name in ("validate-source", "rebuild-arm64")],
            artifacts=[{
                "id": 42, "name": "recovered-ffi-linux-aarch64", "expired": False,
                "size_in_bytes": 1024,
                "workflow_run": {"id": 456, "head_sha": "b" * 40, "head_branch": "main"},
            }],
            source_log=f"2026-11-09T06:54:22.1731272Z Validated v0.14.1 at {self.sha} from run 123\n",
        )

    def test_reuses_build_from_newer_workflow_only_for_the_verified_release_source(self):
        self.assertEqual(release_recovery.validate_recovery_build(**self.recovery_build_args()), "42")

    def test_recovery_cli_captures_colored_logs_without_printing_them(self):
        recovery = self.recovery_build_args()
        recovery["jobs"][0]["id"] = 789
        prefix = "repos/tobilg/polyglot"
        responses = {
            f"{prefix}/commits/v0.14.1": {"sha": self.sha},
            f"{prefix}/actions/runs/123": self.run,
            f"{prefix}/actions/runs/123/jobs?filter=latest&per_page=100&page=1": {"jobs": self.jobs},
            f"{prefix}/actions/runs/123/artifacts?per_page=100&page=1": {"artifacts": self.artifacts},
            f"{prefix}/actions/runs/456": recovery["run"],
            f"{prefix}/actions/runs/456/jobs?filter=latest&per_page=100&page=1": {"jobs": recovery["jobs"]},
            f"{prefix}/actions/runs/456/artifacts?per_page=100&page=1": {"artifacts": recovery["artifacts"]},
        }

        def gh_api(command, *, text):
            self.assertTrue(text)
            self.assertEqual(command[:2], ["gh", "api"])
            endpoint = command[-1]
            if endpoint == f"{prefix}/actions/jobs/789/logs":
                # Reproduce the CLI's raw-log response guard seen on the runner.
                if "--allow-escape-sequences" not in command:
                    raise subprocess.CalledProcessError(
                        1, command, stderr="the response contains terminal escape sequences"
                    )
                return "2026-11-09T06:54:21.000Z \x1b[36mcheckout\x1b[0m\n" + recovery["source_log"]
            self.assertNotIn("--allow-escape-sequences", command)
            return json.dumps(responses[endpoint])

        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "outputs"
            stdout = io.StringIO()
            with (
                mock.patch.dict(os.environ, {"GITHUB_REPOSITORY": "tobilg/polyglot", "GITHUB_OUTPUT": str(output)}),
                mock.patch("sys.argv", ["release_recovery.py", "v0.14.1", "123", "--arm64-run-id", "456"]),
                mock.patch.object(release_recovery.subprocess, "check_output", side_effect=gh_api),
                mock.patch("sys.stdout", stdout),
            ):
                release_recovery.main()
            self.assertEqual(output.read_text(),
                             f"source_sha={self.sha}\nartifact_ids=1,2,3,4,5\narm64_artifact_id=42\n")
            self.assertNotIn("\x1b", stdout.getvalue())
            self.assertIn(f"Validated v0.14.1 at {self.sha} from run 123", stdout.getvalue())

    def test_rejects_a_recovery_build_for_another_release_source(self):
        args = self.recovery_build_args()
        for log in (
            "", args["source_log"].replace(self.sha, "c" * 40),
            args["source_log"].replace("v0.14.1", "v0.14.2"),
            args["source_log"].replace("run 123", "run 789"),
            args["source_log"].replace("Z Validated", "Z echo Validated"),
        ):
            with self.subTest(log=log), self.assertRaises(ValueError):
                release_recovery.validate_recovery_build(**{**args, "source_log": log})

    def test_rejects_unfinished_unrelated_or_unsuccessful_recovery_runs(self):
        for key, value in (
            ("id", 789), ("status", "in_progress"), ("event", "push"),
            ("path", ".github/workflows/ci.yml"), ("head_branch", "other"),
        ):
            args = self.recovery_build_args()
            args["run"][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):
                release_recovery.validate_recovery_build(**args)
        for index in (0, 1):
            for conclusion in ("failure", "cancelled", "skipped", None):
                args = self.recovery_build_args()
                args["jobs"][index]["conclusion"] = conclusion
                with self.subTest(index=index, conclusion=conclusion), self.assertRaises(ValueError):
                    release_recovery.validate_recovery_build(**args)

    def test_rejects_missing_expired_or_mismatched_recovery_artifacts(self):
        for change in (
            {"expired": True}, {"size_in_bytes": 0}, {"id": -1},
            {"workflow_run": {"id": 789, "head_sha": "b" * 40, "head_branch": "main"}},
            {"workflow_run": {"id": 456, "head_sha": "c" * 40, "head_branch": "main"}},
        ):
            args = self.recovery_build_args()
            args["artifacts"][0].update(change)
            with self.subTest(change=change), self.assertRaises(ValueError):
                release_recovery.validate_recovery_build(**args)
        for count in (0, 2):
            args = self.recovery_build_args()
            args["artifacts"] *= count
            with self.subTest(count=count), self.assertRaises(ValueError):
                release_recovery.validate_recovery_build(**args)

    def run_publication(self, state, fail_upload=False):
        # Execute the actual workflow shell block with a local gh stub. No API writes.
        workflow = (consistency.PROJECT_ROOT / ".github/workflows/recover-release.yml").read_text()
        block = workflow.split("      - name: Publish the complete release assets\n", 1)[1]
        script = textwrap.dedent(block.split("        run: |\n", 1)[1].split("\n      - name:", 1)[0])
        stub = r'''
gh() {
  printf '%s\n' "$*" >> "$CALLS_LOG"
  if [ "$1" = api ]; then
    case "$RELEASE_STATE" in
      missing) return 0 ;;
      error) return 13 ;;
      *) printf '%s\n' "$RELEASE_STATE" ;;
    esac
  elif [ "$2" = upload ] && [ "$FAIL_UPLOAD" = 1 ]; then
    return 17
  fi
  return 0
}
'''
        with tempfile.TemporaryDirectory() as directory:
            log = Path(directory) / "calls.log"
            result = subprocess.run(
                ["bash", "--noprofile", "--norc", "-eo", "pipefail", "-c", stub + script],
                env={**os.environ, "CALLS_LOG": str(log), "RELEASE_STATE": state,
                     "RELEASE_TAG": "v0.14.1", "GITHUB_REPOSITORY": "tobilg/polyglot",
                     "FAIL_UPLOAD": "1" if fail_upload else "0"},
                capture_output=True, text=True,
            )
            return result, log.read_text().splitlines()

    def test_release_creation_requires_existing_tag_without_historical_target(self):
        result, calls = self.run_publication("missing")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[1].startswith("release create v0.14.1 "))
        self.assertIn("--verify-tag", calls[1])
        self.assertNotIn("--target", calls[1])
        self.assertEqual(calls[1].count("dist/"), 8)

    def test_existing_published_release_only_receives_assets(self):
        result, calls = self.run_publication("false")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(len(calls), 2)
        self.assertTrue(calls[1].startswith("release upload v0.14.1 "))

    def test_existing_draft_is_published_only_after_successful_upload(self):
        result, calls = self.run_publication("true")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertTrue(calls[-1].startswith("release edit v0.14.1 "))
        self.assertIn("--draft=false", calls[-1])
        result, calls = self.run_publication("true", fail_upload=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertFalse(any(call.startswith("release edit ") for call in calls))

    def test_release_query_error_never_causes_release_creation(self):
        result, calls = self.run_publication("error")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(len(calls), 1)


if __name__ == "__main__":
    unittest.main()
