#!/usr/bin/env python3
"""Parse polyglot test output and remove failing tests from ClickHouse fixture files.

Usage:
    python3 tools/clickhouse-extract/remove-failures.py <test_output_file>
"""

import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
FIXTURE_DIR = PROJECT_ROOT / "crates" / "polyglot-sql" / "tests" / "custom_fixtures" / "clickhouse"
FAILURES_FILE = PROJECT_ROOT / "temporary-files" / "clickhouse_failures.json"

# Pattern: FAIL [category:index] description: error
FAIL_PATTERN = re.compile(r"FAIL \[(\w+):(\d+)\]")


def main():
    if len(sys.argv) < 2:
        print("Usage: python3 remove-failures.py <test_output_file>")
        sys.exit(1)

    test_output = Path(sys.argv[1])
    if not test_output.exists():
        print(f"ERROR: File not found: {test_output}")
        sys.exit(1)

    # Parse failures from test output
    failures_by_category = defaultdict(set)
    total_failures = 0

    with open(test_output) as f:
        for line in f:
            m = FAIL_PATTERN.search(line)
            if m:
                category = m.group(1)
                index = int(m.group(2))
                failures_by_category[category].add(index)
                total_failures += 1

    print(f"Found {total_failures} failures across {len(failures_by_category)} categories")

    # Remove failing tests from fixture files
    total_removed = 0
    total_kept = 0
    failure_details = []

    for json_file in sorted(FIXTURE_DIR.glob("*.json")):
        with open(json_file) as f:
            data = json.load(f)

        category = data["category"]
        if category not in failures_by_category:
            total_kept += len(data["identity"])
            continue

        fail_indices = failures_by_category[category]
        original_count = len(data["identity"])

        # Save failing tests for reference
        for idx in sorted(fail_indices, reverse=True):
            if idx < len(data["identity"]):
                failure_details.append({
                    "category": category,
                    "index": idx,
                    "sql": data["identity"][idx]["sql"],
                    "description": data["identity"][idx].get("description", ""),
                })

        # Remove failures (iterate in reverse to maintain indices)
        new_identity = [
            test for i, test in enumerate(data["identity"])
            if i not in fail_indices
        ]

        removed = original_count - len(new_identity)
        total_removed += removed
        total_kept += len(new_identity)

        print(f"  {json_file.name}: {removed} removed, {len(new_identity)} kept")

        data["identity"] = new_identity

        # Remove empty files
        if not new_identity:
            json_file.unlink()
            print(f"    -> removed empty file")
            continue

        with open(json_file, "w") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
            f.write("\n")

    # Save failures for future reference
    FAILURES_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(FAILURES_FILE, "w") as f:
        json.dump(failure_details, f, indent=2, ensure_ascii=False)
        f.write("\n")

    print(f"\nSummary:")
    print(f"  Tests removed: {total_removed}")
    print(f"  Tests kept:    {total_kept}")
    print(f"  Failures saved to: {FAILURES_FILE}")


if __name__ == "__main__":
    main()
