#!/usr/bin/env python3
"""Extract ClickHouse SQL tests from the official ClickHouse test suite
and generate custom dialect fixture JSON files for polyglot-sql.

Pipeline:
  .sql files → split into statements → filter → normalize → deduplicate
  → categorize → write JSON fixtures

The resulting fixtures are identity tests: polyglot parses the SQL as ClickHouse
and must reproduce it unchanged. Statements that polyglot can't handle yet are
removed in a second pass via remove-failures.py after running cargo test.

Usage:
    uv run python3 tools/clickhouse-extract/extract-clickhouse-tests.py [options]

Options:
    --max-length N          Skip statements longer than N chars (default: 300)
    --max-per-file N        Max tests per output JSON file (default: 1000)
    --output-dir DIR        Output directory (default: crates/polyglot-sql/tests/custom_fixtures/clickhouse)
    --verbose               Print detailed progress
"""

import argparse
import json
import re
import sys
from pathlib import Path
from collections import defaultdict

# ─── Configuration ──────────────────────────────────────────────────────────

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
CLICKHOUSE_TESTS_DIR = PROJECT_ROOT / "external-projects" / "clickhouse" / "tests" / "queries" / "0_stateless"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "crates" / "polyglot-sql" / "tests" / "custom_fixtures" / "clickhouse"

# ClickHouse directive patterns to strip
DIRECTIVE_PATTERNS = [
    re.compile(r"--\s*\{.*?\}", re.DOTALL),  # -- { serverError ... }
    re.compile(r"--\s*Tags:.*$", re.MULTILINE),
    re.compile(r"--\s*\{\s*echo(On|Off)\s*\}", re.IGNORECASE),
]

# Runtime settings/format suffixes to strip
SETTINGS_PATTERN = re.compile(r"\s+SETTINGS\s+\w+\s*=\s*[^;]*$", re.IGNORECASE)
FORMAT_PATTERN = re.compile(r"\s+FORMAT\s+\w+(\s+\w+)*\s*$", re.IGNORECASE)

# INSERT ... VALUES / INSERT ... FORMAT patterns (data rows, not SQL structure)
INSERT_VALUES_PATTERN = re.compile(r"^\s*INSERT\s+INTO\s+.*?\bVALUES\b", re.IGNORECASE | re.DOTALL)
INSERT_FORMAT_PATTERN = re.compile(r"^\s*INSERT\s+INTO\s+.*?\bFORMAT\b", re.IGNORECASE | re.DOTALL)


# ─── Statement Splitting ────────────────────────────────────────────────────

def split_statements(content: str) -> list[str]:
    """Split SQL content into individual statements, handling strings and comments."""
    statements = []
    current = []
    in_single_quote = False
    in_double_quote = False
    in_backtick = False
    in_line_comment = False
    in_block_comment = False
    paren_depth = 0
    i = 0
    n = len(content)

    while i < n:
        c = content[i]

        # Line comment
        if not in_single_quote and not in_double_quote and not in_backtick and not in_block_comment:
            if c == '-' and i + 1 < n and content[i + 1] == '-':
                in_line_comment = True
                i += 2
                continue

        if in_line_comment:
            if c == '\n':
                in_line_comment = False
                current.append(' ')
            i += 1
            continue

        # Block comment
        if not in_single_quote and not in_double_quote and not in_backtick:
            if c == '/' and i + 1 < n and content[i + 1] == '*':
                in_block_comment = True
                i += 2
                continue

        if in_block_comment:
            if c == '*' and i + 1 < n and content[i + 1] == '/':
                in_block_comment = False
                i += 2
                current.append(' ')
                continue
            i += 1
            continue

        # String literals
        if c == "'" and not in_double_quote and not in_backtick:
            if in_single_quote:
                # Check for escaped quote
                if i + 1 < n and content[i + 1] == "'":
                    current.append("''")
                    i += 2
                    continue
                in_single_quote = False
            else:
                in_single_quote = True
            current.append(c)
            i += 1
            continue

        if c == '"' and not in_single_quote and not in_backtick:
            in_double_quote = not in_double_quote
            current.append(c)
            i += 1
            continue

        if c == '`' and not in_single_quote and not in_double_quote:
            in_backtick = not in_backtick
            current.append(c)
            i += 1
            continue

        # Parentheses
        if not in_single_quote and not in_double_quote and not in_backtick:
            if c == '(':
                paren_depth += 1
            elif c == ')':
                paren_depth = max(0, paren_depth - 1)

        # Statement separator
        if c == ';' and paren_depth == 0 and not in_single_quote and not in_double_quote and not in_backtick:
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
            current = []
            i += 1
            continue

        # Newlines become spaces
        if c == '\n':
            current.append(' ')
        else:
            current.append(c)
        i += 1

    # Last statement (no trailing semicolon)
    stmt = ''.join(current).strip()
    if stmt:
        statements.append(stmt)

    return statements


# ─── Filtering ──────────────────────────────────────────────────────────────

def normalize_statement(sql: str) -> str:
    """Normalize a SQL statement for consistency."""
    # Collapse whitespace
    sql = re.sub(r'\s+', ' ', sql).strip()
    # Strip trailing semicolons
    sql = sql.rstrip(';').strip()
    # Strip SETTINGS clauses
    sql = SETTINGS_PATTERN.sub('', sql).strip()
    # Strip FORMAT suffixes
    sql = FORMAT_PATTERN.sub('', sql).strip()
    return sql


def should_keep(sql: str, max_length: int) -> bool:
    """Determine if a normalized SQL statement should be kept.

    Only filters out things that aren't testable SQL structure:
    - Empty / too long / too deeply nested (practical limits)
    - INSERT...VALUES / INSERT...FORMAT (data rows, not SQL)
    """
    if not sql:
        return False

    if len(sql) > max_length:
        return False

    # INSERT with literal data rows — not SQL structure worth testing
    if INSERT_VALUES_PATTERN.match(sql):
        return False
    if INSERT_FORMAT_PATTERN.match(sql):
        return False

    # Deeply nested parens can cause stack overflow in the parser
    depth = 0
    for c in sql:
        if c == '(':
            depth += 1
            if depth > 15:
                return False
        elif c == ')':
            depth -= 1

    return True


# ─── Categorization ─────────────────────────────────────────────────────────

def categorize(sql: str) -> str:
    """Categorize a SQL statement into a fixture category."""
    upper = sql.upper().strip()

    if upper.startswith("EXPLAIN"):
        return "explain"
    if upper.startswith("SET"):
        return "set"
    if upper.startswith(("CREATE", "ALTER", "DROP", "TRUNCATE", "RENAME")):
        return "ddl"
    if upper.startswith(("INSERT", "UPDATE", "DELETE", "MERGE")):
        return "dml"
    if upper.startswith(("SHOW", "DESCRIBE", "DESC", "EXISTS")):
        return "show"
    if upper.startswith(("GRANT", "REVOKE")):
        return "privileges"
    if upper.startswith(("SYSTEM", "OPTIMIZE", "CHECK", "KILL")):
        return "system"
    if upper.startswith(("ATTACH", "DETACH")):
        return "attach"
    if upper.startswith(("BACKUP", "RESTORE")):
        return "backup"
    if upper.startswith("USE"):
        return "use"
    if upper.startswith(("WATCH", "EXCHANGE")):
        return "other"

    # SELECT-based categorization
    if upper.startswith(("SELECT", "WITH")):
        # Window functions
        if re.search(r'\bOVER\s*\(', sql, re.IGNORECASE):
            return "window"
        # Array operations
        if re.search(r'\bARRAY\s+JOIN\b', sql, re.IGNORECASE) or \
           re.search(r'\barray\w*\s*\(', sql):
            return "array_operations"
        # Type operations (CAST, ::, type constructors)
        if re.search(r'\bCAST\s*\(', sql, re.IGNORECASE) or \
           re.search(r'::', sql) or \
           re.search(r'\b(Tuple|Array|Map|Nullable|LowCardinality|Enum\d*)\s*\(', sql):
            return "types"
        # Function-only tests (SELECT func(...) without FROM)
        if not re.search(r'\bFROM\b', sql, re.IGNORECASE):
            return "functions"
        return "select"

    return "other"


# ─── Main Pipeline ──────────────────────────────────────────────────────────

def extract_from_file(filepath: Path, max_length: int) -> list[dict]:
    """Extract SQL statements from a single .sql file."""
    try:
        content = filepath.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return []

    # Strip ClickHouse test directives from content
    cleaned = content
    for pattern in DIRECTIVE_PATTERNS:
        cleaned = pattern.sub('', cleaned)

    raw_stmts = split_statements(cleaned)
    results = []

    for stmt in raw_stmts:
        normalized = normalize_statement(stmt)
        if not should_keep(normalized, max_length):
            continue

        results.append({
            "sql": normalized,
            "source": filepath.name,
        })

    return results


def main():
    parser = argparse.ArgumentParser(description="Extract ClickHouse SQL tests")
    parser.add_argument("--max-length", type=int, default=300)
    parser.add_argument("--max-per-file", type=int, default=1000)
    parser.add_argument("--output-dir", type=str, default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--verbose", action="store_true")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)

    # Check that ClickHouse tests exist
    if not CLICKHOUSE_TESTS_DIR.exists():
        print(f"ERROR: ClickHouse test directory not found: {CLICKHOUSE_TESTS_DIR}")
        print("Run 'make setup-clickhouse-tests' first.")
        sys.exit(1)

    # Find all .sql files
    sql_files = sorted(CLICKHOUSE_TESTS_DIR.glob("*.sql"))
    print(f"Found {len(sql_files)} .sql files in {CLICKHOUSE_TESTS_DIR}")

    # Phase 1: Extract statements from all files
    print("\n--- Phase 1: Extracting statements ---")
    all_stmts = []
    files_processed = 0
    for filepath in sql_files:
        stmts = extract_from_file(filepath, args.max_length)
        all_stmts.extend(stmts)
        files_processed += 1
        if args.verbose and files_processed % 500 == 0:
            print(f"  Processed {files_processed}/{len(sql_files)} files, {len(all_stmts)} statements so far")

    print(f"  Extracted {len(all_stmts)} statements from {files_processed} files")

    # Phase 2: Deduplicate
    print("\n--- Phase 2: Deduplicating ---")
    seen = set()
    unique_stmts = []
    for stmt in all_stmts:
        key = stmt["sql"]
        if key not in seen:
            seen.add(key)
            unique_stmts.append(stmt)

    print(f"  {len(unique_stmts)} unique statements (removed {len(all_stmts) - len(unique_stmts)} duplicates)")

    # Phase 3: Categorize
    print("\n--- Phase 3: Categorizing ---")
    validated = unique_stmts
    categories = defaultdict(list)
    for stmt in validated:
        cat = categorize(stmt["sql"])
        categories[cat].append(stmt)

    for cat, stmts in sorted(categories.items()):
        print(f"  {cat}: {len(stmts)}")

    # Phase 4: Write fixture files
    print(f"\n--- Phase 4: Writing fixtures to {output_dir} ---")
    output_dir.mkdir(parents=True, exist_ok=True)

    # Clean existing files first
    for f in output_dir.glob("*.json"):
        f.unlink()

    total_written = 0
    for cat, stmts in sorted(categories.items()):
        # Split into chunks if needed
        if len(stmts) > args.max_per_file:
            chunks = [stmts[i:i + args.max_per_file] for i in range(0, len(stmts), args.max_per_file)]
        else:
            chunks = [stmts]

        for chunk_idx, chunk in enumerate(chunks):
            if len(chunks) > 1:
                filename = f"{cat}_{chunk_idx + 1:02d}.json"
            else:
                filename = f"{cat}.json"

            fixture = {
                "dialect": "clickhouse",
                "category": cat if len(chunks) == 1 else f"{cat}_{chunk_idx + 1:02d}",
                "identity": [],
                "transpilation": [],
            }

            for stmt in chunk:
                fixture["identity"].append({
                    "sql": stmt["sql"],
                    "description": f"from {stmt['source']}",
                })

            filepath = output_dir / filename
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(fixture, f, indent=2, ensure_ascii=False)
                f.write("\n")

            total_written += len(chunk)
            print(f"  {filename}: {len(chunk)} tests")

    # Summary
    print(f"\n{'='*60}")
    print(f"SUMMARY")
    print(f"{'='*60}")
    print(f"  SQL files scanned:       {len(sql_files)}")
    print(f"  Statements extracted:    {len(all_stmts)}")
    print(f"  After deduplication:     {len(unique_stmts)}")
    print(f"  Total tests written:     {total_written}")
    print(f"  Output directory:        {output_dir}")
    print(f"{'='*60}")
    print(f"\nNext step: run polyglot tests and remove failures:")
    print(f"  RUST_MIN_STACK=16777216 cargo test --test custom_dialect_tests -p polyglot-sql --release -- test_custom_dialect_identity_all --nocapture 2>&1 > temporary-files/clickhouse_test_output.txt")
    print(f"  uv run python3 tools/clickhouse-extract/remove-failures.py temporary-files/clickhouse_test_output.txt")


if __name__ == "__main__":
    main()
