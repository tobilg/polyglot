#!/usr/bin/env python3
"""Generate the ClickHouse function catalog Rust source from clickhouse-functions.jsonl.

Reads the JSONL file (extracted from ClickHouse system.functions) and produces a Rust source file
containing a static FunctionCatalog with all known ClickHouse functions and their arity
constraints, parsed from the `syntax` field.
§
Usage:
    python3 tools/clickhouse-extract/generate-function-catalog.py
"""

import json
import re
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
JSONL_PATH = PROJECT_ROOT / "tools" / "clickhouse-extract" / "clickhouse-functions.jsonl"
OUTPUT_PATH = PROJECT_ROOT / "crates" / "polyglot-sql" / "src" / "validation" / "clickhouse.rs"

# Header indices in JSONL (line 1 is the header)
NAME = 0
IS_AGGREGATE = 1
CASE_INSENSITIVE = 2
ALIAS_TO = 3
SYNTAX = 7


def parse_arity_from_syntax(syntax: str) -> tuple[int, int | None]:
    """Extract (min_args, max_args) from a syntax string like 'func(a, b[, c])'.

    Returns (min_args, max_args) where max_args=None means variadic.
    """
    # Find the argument list inside parens
    # Use the LAST opening paren that has a matching close, to handle nested syntax
    m = re.search(r'\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*$', syntax)
    if not m:
        # No parens or unparseable — could be a 0-arg function like "now()"
        if "()" in syntax:
            return 0, 0
        return 0, None  # permissive fallback

    inner = m.group(1).strip()
    if not inner:
        return 0, 0

    variadic = "..." in inner

    # Split top-level by comma, respecting brackets and parens
    depth_square = 0
    depth_paren = 0
    parts: list[str] = []
    current: list[str] = []

    for ch in inner:
        if ch == "[":
            depth_square += 1
            current.append(ch)
        elif ch == "]":
            depth_square -= 1
            current.append(ch)
        elif ch == "(":
            depth_paren += 1
            current.append(ch)
        elif ch == ")":
            depth_paren -= 1
            current.append(ch)
        elif ch == "," and depth_square == 0 and depth_paren == 0:
            parts.append("".join(current).strip())
            current = []
        else:
            current.append(ch)

    if current:
        last = "".join(current).strip()
        if last:
            parts.append(last)

    required = 0
    optional = 0

    for part in parts:
        part = part.strip()
        if not part or part == "...":
            continue
        # Check if this part is optional (wrapped in brackets or starts with [)
        if part.startswith("["):
            # Count args inside optional bracket group
            # e.g. "[, s3, ...]" or "[, N]"
            inner_opt = part.strip("[]").strip().lstrip(",").strip()
            if inner_opt and inner_opt != "...":
                # Could contain multiple optional args separated by commas
                opt_parts = [p.strip() for p in inner_opt.split(",") if p.strip() and p.strip() != "..."]
                optional += max(1, len(opt_parts))
        else:
            required += 1

    min_args = required
    max_args = None if variadic else required + optional
    return min_args, max_args


def main():
    if not JSONL_PATH.exists():
        print(f"ERROR: {JSONL_PATH} not found")
        sys.exit(1)

    # Read all functions
    functions: list[dict] = []
    aliases: list[tuple[str, str]] = []  # (alias_name, target_name)

    with open(JSONL_PATH) as f:
        header = json.loads(f.readline())  # column names
        types = json.loads(f.readline())   # column types

        for line in f:
            row = json.loads(line)
            name = row[NAME]
            is_agg = row[IS_AGGREGATE] == 1
            alias_to = row[ALIAS_TO]
            syntax = row[SYNTAX].strip()

            if alias_to:
                aliases.append((name, alias_to))
                continue

            min_args, max_args = parse_arity_from_syntax(syntax)

            functions.append({
                "name": name,
                "is_aggregate": is_agg,
                "min_args": min_args,
                "max_args": max_args,
            })

    # Build a lookup for alias resolution
    func_by_name = {f["name"].upper(): f for f in functions}

    # Generate Rust source
    lines: list[str] = []
    lines.append("//! ClickHouse function validation.")
    lines.append("//!")
    lines.append("//! Contains the function catalog (auto-generated from clickhouse-functions.jsonl)")
    lines.append("//! and ClickHouse-specific validation logic like aggregate combinator handling.")
    lines.append("")
    lines.append("use super::{FunctionCatalog, FunctionSignature};")
    lines.append("use std::sync::LazyLock;")
    lines.append("")
    lines.append("/// ClickHouse aggregate function combinator suffixes.")
    lines.append("///")
    lines.append('/// Any aggregate function can be combined with these suffixes to create derived')
    lines.append('/// functions (e.g., `sum` + `If` = `sumIf`, `count` + `State` = `countState`).')
    lines.append("/// See: https://clickhouse.com/docs/en/sql-reference/aggregate-functions/combinators")
    lines.append('pub const COMBINATORS: &[&str] = &[')
    for c in ["If", "Array", "Map", "State", "Merge", "MergeState",
              "ForEach", "Distinct", "OrDefault", "OrNull", "Resample",
              "ArgMin", "ArgMax"]:
        lines.append(f'    "{c}",')
    lines.append("];")
    lines.append("")
    lines.append("/// The global ClickHouse function catalog, initialized once on first access.")
    lines.append("///")
    lines.append(f"/// Auto-generated from: tools/clickhouse-extract/clickhouse-functions.jsonl ({len(functions)} functions, {len(aliases)} aliases)")
    lines.append("///")
    lines.append("/// To regenerate:")
    lines.append("///   python3 tools/clickhouse-extract/generate-function-catalog.py")
    lines.append("pub static CATALOG: LazyLock<FunctionCatalog> = LazyLock::new(build_catalog);")
    lines.append("")
    lines.append("fn build_catalog() -> FunctionCatalog {")
    lines.append(f"    let mut cat = FunctionCatalog::with_capacity({len(functions) + len(aliases)});")
    lines.append("    cat.set_combinators(COMBINATORS);")
    lines.append("")

    # Sort by name for readability
    functions.sort(key=lambda f: f["name"].lower())

    for func in functions:
        name = func["name"]
        min_a = func["min_args"]
        max_a = func["max_args"]

        if max_a is None:
            lines.append(f'    cat.register(FunctionSignature::variadic("{name}", {min_a}));')
        elif min_a == max_a:
            lines.append(f'    cat.register(FunctionSignature::fixed("{name}", {min_a}));')
        else:
            lines.append(f'    cat.register(FunctionSignature::new("{name}", {min_a}, Some({max_a})));')

    # Add aliases
    if aliases:
        lines.append("")
        lines.append("    // Aliases")
        aliases.sort(key=lambda a: a[0].lower())
        for alias_name, target_name in aliases:
            target = func_by_name.get(target_name.upper())
            if target:
                min_a = target["min_args"]
                max_a = target["max_args"]
                if max_a is None:
                    lines.append(f'    cat.register(FunctionSignature::variadic("{alias_name}", {min_a}));')
                elif min_a == max_a:
                    lines.append(f'    cat.register(FunctionSignature::fixed("{alias_name}", {min_a}));')
                else:
                    lines.append(f'    cat.register(FunctionSignature::new("{alias_name}", {min_a}, Some({max_a})));')
            else:
                # Target not found — register permissively
                lines.append(f'    cat.register(FunctionSignature::variadic("{alias_name}", 0)); // alias of unknown: {target_name}')

    lines.append("")
    lines.append("    cat")
    lines.append("}")
    lines.append("")

    # Write output
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text("\n".join(lines))

    print(f"Generated {OUTPUT_PATH}")
    print(f"  {len(functions)} functions + {len(aliases)} aliases = {len(functions) + len(aliases)} entries")


if __name__ == "__main__":
    main()
