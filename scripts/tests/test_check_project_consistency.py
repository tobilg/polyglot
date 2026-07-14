import json
import tempfile
import unittest
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
        cls._write(
            root / "README.md", 'polyglot-sql = { version = "0.6.0" }\n'
        )
        cls._write(
            root / "crates/polyglot-sql/README.md",
            'polyglot-sql = { version = "0.6.0" }\n',
        )
        cls._write(
            root / "examples/rust/Cargo.toml",
            '[dependencies]\npolyglot-sql = { version = "0.6.0", path = "../../crates/polyglot-sql" }\n',
        )

    @staticmethod
    def _metadata(root: Path, package_version: str) -> dict:
        package_id = f"path+file://{root}/crates/polyglot-sql#0.6.0"
        return {
            "workspace_members": [package_id],
            "packages": [
                {
                    "id": package_id,
                    "name": "polyglot-sql",
                    "version": package_version,
                    "manifest_path": str(root / "crates/polyglot-sql/Cargo.toml"),
                    "dependencies": [],
                }
            ],
        }


if __name__ == "__main__":
    unittest.main()
