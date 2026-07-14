#!/usr/bin/env python3
"""Check version, dialect, and active-documentation consistency."""

from __future__ import annotations

import json
import re
import subprocess
import sys
import tomllib
from collections.abc import Iterable
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
MARKETING_PATHS = (
    Path("README.md"),
    Path("crates/polyglot-sql/README.md"),
    Path("crates/polyglot-sql/src/dialects/mod.rs"),
    Path("crates/polyglot-sql/src/expressions.rs"),
    Path("crates/polyglot-sql/src/generator.rs"),
    Path("crates/polyglot-sql/src/parser.rs"),
    Path("crates/polyglot-sql-python/README.md"),
    Path("crates/polyglot-sql-python/pyproject.toml"),
    Path("crates/polyglot-sql-python/python/polyglot_sql/__init__.py"),
    Path("packages/documentation/README.md"),
    Path("packages/documentation/postbuild.mjs"),
    Path("packages/playground/index.html"),
    Path("packages/sdk/README.md"),
)
MARKETING_DIALECT_PHRASE = "more than 30 sql dialects"


def load_toml(path: Path) -> dict:
    with path.open("rb") as source:
        return tomllib.load(source)


def read_text(root: Path, relative_path: str | Path) -> str:
    return (root / relative_path).read_text(encoding="utf-8")


def cargo_metadata(root: Path) -> dict:
    result = subprocess.run(
        ["cargo", "metadata", "--no-deps", "--format-version", "1"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    )
    return json.loads(result.stdout)


def workspace_version(root: Path) -> str:
    return load_toml(root / "Cargo.toml")["workspace"]["package"]["version"]


def _version_requirement_matches(requirement: str, version: str) -> bool:
    return requirement == "*" or requirement.lstrip("^=~") == version


def check_versions(root: Path, metadata: dict | None = None) -> list[str]:
    issues: list[str] = []
    expected = workspace_version(root)
    metadata = metadata or cargo_metadata(root)
    workspace_members = set(metadata["workspace_members"])
    workspace_packages = {
        package["name"]: package
        for package in metadata["packages"]
        if package["id"] in workspace_members
    }

    for package in workspace_packages.values():
        if package["version"] != expected:
            issues.append(
                f"{package['manifest_path']}: package version {package['version']!r} "
                f"does not match workspace version {expected!r}"
            )
        for dependency in package["dependencies"]:
            if not (
                dependency["name"].startswith("polyglot-sql")
                and dependency.get("path")
            ):
                continue

            requirement = dependency["req"]
            if not _version_requirement_matches(requirement, expected):
                issues.append(
                    f"{package['manifest_path']}: dependency {dependency['name']!r} uses "
                    f"{requirement!r}, expected {expected!r}"
                )

            package_is_publishable = package.get("publish") != []
            dependency_is_runtime = dependency.get("kind") != "dev"
            if not (package_is_publishable and dependency_is_runtime):
                continue

            if requirement == "*":
                issues.append(
                    f"{package['manifest_path']}: publishable package dependency "
                    f"{dependency['name']!r} must specify version {expected!r}"
                )

            dependency_package = workspace_packages.get(dependency["name"])
            if dependency_package and dependency_package.get("publish") == []:
                issues.append(
                    f"{package['manifest_path']}: publishable package depends on "
                    f"non-publishable package {dependency['name']!r}"
                )

    for package_path in sorted((root / "packages").glob("*/package.json")):
        package = json.loads(package_path.read_text(encoding="utf-8"))
        if package.get("version") != expected:
            issues.append(
                f"{package_path}: package version {package.get('version')!r} "
                f"does not match workspace version {expected!r}"
            )

    go_types_path = root / "packages/go/types.go"
    go_types = go_types_path.read_text(encoding="utf-8")
    go_match = re.search(r'const sdkVersion = "([^"]+)"', go_types)
    if not go_match or go_match.group(1) != expected:
        actual = go_match.group(1) if go_match else "missing"
        issues.append(
            f"{go_types_path}: sdkVersion {actual!r} does not match workspace version {expected!r}"
        )

    for readme_path in (root / "README.md", root / "crates/polyglot-sql/README.md"):
        readme = readme_path.read_text(encoding="utf-8")
        readme_versions = re.findall(r'polyglot-sql = \{ version = "([^"]+)"', readme)
        if not readme_versions:
            issues.append(f"{readme_path}: missing versioned Rust dependency example")
        for actual in readme_versions:
            if actual != expected:
                issues.append(
                    f"{readme_path}: Rust dependency example version {actual!r} "
                    f"does not match workspace version {expected!r}"
                )

    example_path = root / "examples/rust/Cargo.toml"
    example = load_toml(example_path)
    dependency = example.get("dependencies", {}).get("polyglot-sql")
    if not isinstance(dependency, dict):
        issues.append(f"{example_path}: polyglot-sql must be a versioned local path dependency")
    else:
        if dependency.get("version") != expected:
            issues.append(
                f"{example_path}: polyglot-sql version {dependency.get('version')!r} "
                f"does not match workspace version {expected!r}"
            )
        if dependency.get("path") != "../../crates/polyglot-sql":
            issues.append(
                f"{example_path}: polyglot-sql must use path '../../crates/polyglot-sql'"
            )

    return issues


def _extract_braced_block(text: str, marker: str) -> str:
    marker_index = text.find(marker)
    if marker_index < 0:
        raise ValueError(f"missing marker {marker!r}")
    start = text.find("{", marker_index + len(marker))
    if start < 0:
        raise ValueError(f"missing opening brace after {marker!r}")
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return text[start + 1 : index]
    raise ValueError(f"missing closing brace after {marker!r}")


def _extract_assignment_array(text: str, marker_pattern: str) -> str:
    marker = re.search(marker_pattern, text, re.DOTALL)
    if not marker:
        raise ValueError(f"missing array matching {marker_pattern!r}")
    start = text.find("[", marker.end())
    if start < 0:
        raise ValueError(f"missing opening bracket after {marker_pattern!r}")
    depth = 0
    for index in range(start, len(text)):
        if text[index] == "[":
            depth += 1
        elif text[index] == "]":
            depth -= 1
            if depth == 0:
                return text[start + 1 : index]
    raise ValueError(f"missing closing bracket after {marker_pattern!r}")


def normalize_dialect_name(name: str) -> str:
    return "postgresql" if name == "postgres" else name


def compare_dialect_set(
    label: str, names: Iterable[str], expected: set[str]
) -> list[str]:
    normalized = [normalize_dialect_name(name) for name in names]
    actual = set(normalized)
    issues: list[str] = []
    if len(actual) != len(normalized):
        duplicates = sorted({name for name in normalized if normalized.count(name) > 1})
        issues.append(f"{label}: duplicate dialects: {', '.join(duplicates)}")
    missing = sorted(expected - actual)
    extra = sorted(actual - expected)
    if missing:
        issues.append(f"{label}: missing dialects: {', '.join(missing)}")
    if extra:
        issues.append(f"{label}: unexpected dialects: {', '.join(extra)}")
    return issues


def check_dialects(root: Path) -> list[str]:
    issues: list[str] = []
    core_manifest = load_toml(root / "crates/polyglot-sql/Cargo.toml")
    concrete = {
        feature.removeprefix("dialect-")
        for feature in core_manifest["features"]["all-dialects"]
        if feature.startswith("dialect-")
    }
    expected = concrete | {"generic"}

    dialect_source = read_text(root, "crates/polyglot-sql/src/dialects/mod.rs")
    enum_block = _extract_braced_block(dialect_source, "pub enum DialectType")
    enum_variants = set(re.findall(r"^\s*([A-Za-z][A-Za-z0-9_]*)\s*,", enum_block, re.MULTILINE))
    display_pairs = dict(
        re.findall(
            r'DialectType::([A-Za-z][A-Za-z0-9_]*)\s*=>\s*write!\(f,\s*"([^"]+)"\)',
            dialect_source,
        )
    )
    if enum_variants != set(display_pairs):
        missing_display = sorted(enum_variants - set(display_pairs))
        extra_display = sorted(set(display_pairs) - enum_variants)
        if missing_display:
            issues.append(f"DialectType Display: missing variants: {', '.join(missing_display)}")
        if extra_display:
            issues.append(f"DialectType Display: unexpected variants: {', '.join(extra_display)}")
    issues.extend(compare_dialect_set("DialectType", display_pairs.values(), expected))

    ffi_source = read_text(root, "crates/polyglot-sql-ffi/src/dialects.rs")
    ffi_block = _extract_assignment_array(ffi_source, r"const\s+DIALECTS\s*:[^=]+=")
    ffi_variants = re.findall(r"DialectType::([A-Za-z][A-Za-z0-9_]*)", ffi_block)
    unknown_ffi = sorted(set(ffi_variants) - set(display_pairs))
    if unknown_ffi:
        issues.append(f"FFI DIALECTS: unknown variants: {', '.join(unknown_ffi)}")
    issues.extend(
        compare_dialect_set(
            "FFI DIALECTS",
            (display_pairs[name] for name in ffi_variants if name in display_pairs),
            expected,
        )
    )

    python_source = read_text(root, "crates/polyglot-sql-python/src/dialects.rs")
    python_block = _extract_assignment_array(
        python_source, r"const\s+DIALECT_NAMES\s*:[^=]+="
    )
    issues.extend(
        compare_dialect_set(
            "Python DIALECT_NAMES", re.findall(r'"([^"]+)"', python_block), expected
        )
    )

    sdk_source = read_text(root, "packages/sdk/src/index.ts")
    sdk_block = _extract_braced_block(sdk_source, "export enum Dialect")
    issues.extend(
        compare_dialect_set(
            "TypeScript Dialect", re.findall(r"=\s*'([^']+)'", sdk_block), expected
        )
    )

    wasm_source = read_text(root, "crates/polyglot-sql-wasm/src/lib.rs")
    wasm_block = _extract_braced_block(wasm_source, "fn get_dialects_internal")
    wasm_names = re.findall(r'(?:vec!\[|push\()"([^"]+)"', wasm_block)
    issues.extend(compare_dialect_set("WASM get_dialects", wasm_names, expected))

    wasm_manifest = load_toml(root / "crates/polyglot-sql-wasm/Cargo.toml")
    wasm_features = {
        feature.removeprefix("dialect-")
        for feature in wasm_manifest["features"]["all-dialects"]
        if feature.startswith("dialect-")
    }
    issues.extend(compare_dialect_set("WASM all-dialects feature", wasm_features, concrete))

    return issues


def marketing_copy_issues(path: Path, text: str) -> list[str]:
    issues: list[str] = []
    lower = text.lower()
    if MARKETING_DIALECT_PHRASE not in lower:
        issues.append(f"{path}: must use the phrase 'more than 30 SQL dialects'")

    numeric_claim = re.compile(r"\b\d+\+?\s+(?:database\s+|sql\s+)?dialects\b", re.IGNORECASE)
    for match in numeric_claim.finditer(text):
        prefix = text[max(0, match.start() - len("more than ")) : match.start()].lower()
        if prefix != "more than ":
            issues.append(f"{path}: exact dialect-count claim {match.group(0)!r} is not allowed")
    return issues


def check_marketing_copy(root: Path) -> list[str]:
    issues: list[str] = []
    for relative_path in MARKETING_PATHS:
        issues.extend(marketing_copy_issues(relative_path, read_text(root, relative_path)))
    return issues


def run_checks(root: Path = PROJECT_ROOT) -> list[str]:
    issues: list[str] = []
    for check in (check_versions, check_dialects, check_marketing_copy):
        try:
            issues.extend(check(root))
        except (KeyError, OSError, ValueError, subprocess.CalledProcessError) as error:
            issues.append(f"{check.__name__}: {error}")
    return issues


def main() -> int:
    issues = run_checks()
    if issues:
        print("Project consistency checks failed:", file=sys.stderr)
        for issue in issues:
            print(f"  - {issue}", file=sys.stderr)
        return 1
    print("Project versions, dialect metadata, and active documentation are consistent.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
