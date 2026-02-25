#!/usr/bin/env python3
"""
Extract ClickHouse function metadata and generate Rust catalog code.

Uses `chdb` to run:
  select * from system.functions

Usage:
  uv run --with chdb python crates/polyglot-sql-function-catalogs/tools/clickhouse/extract_functions.py

NOTE:
  In local verification (ClickHouse 25.8.2.1 via chdb), `system.functions` contained many
  rows with empty or non-parseable `syntax` values. This extractor derives arity primarily
  from `syntax`, and falls back to `FunctionSignature::variadic(0)` when arity cannot be
  determined safely. Because of that, generated output can be less specific than a curated
  catalog for some functions.
"""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[4]
    default_out = repo_root / "crates/polyglot-sql-function-catalogs/src/clickhouse.rs"

    parser = argparse.ArgumentParser(
        description="Generate ClickHouse function catalog Rust source from system.functions.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_out,
        help=f"Rust output path (default: {default_out})",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="select * from system.functions",
        help="ClickHouse query to extract functions (default: select * from system.functions)",
    )
    parser.add_argument(
        "--include-unknown-alias-targets",
        action="store_true",
        help="Include aliases whose alias_to target cannot be resolved (fallback variadic(0)).",
    )
    return parser.parse_args()


def rust_str(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def render_signature(min_arity: int, max_arity: int | None) -> str:
    if max_arity is None:
        return f"FunctionSignature::variadic({min_arity})"
    if min_arity == max_arity:
        return f"FunctionSignature::exact({min_arity})"
    return f"FunctionSignature::range({min_arity}, {max_arity})"


def _count_optional_args(bracket_str: str) -> int:
    """
    Count optional arguments in bracket groups, e.g.:
      [, N]
      [, precision[, time_zone]]
    """
    comma_count = bracket_str.count(",")
    inner = bracket_str.strip("[]").strip()
    if comma_count == 0 and inner and inner != "...":
        return 1
    return comma_count


def parse_arity_from_syntax(syntax: str) -> tuple[int, int | None]:
    """
    Parse (min_arity, max_arity) from a ClickHouse syntax string.

    max_arity=None means variadic.
    """
    text = syntax.strip()
    if not text:
        return 0, None

    if "()" in text and text.endswith(")"):
        # still continue through parser to handle nested constructs safely
        pass

    # Match argument list from the trailing call pattern.
    match = re.search(r"\(([^()]*(?:\([^()]*\)[^()]*)*)\)\s*$", text)
    if not match:
        # No parseable call signature; keep permissive.
        return 0, None

    inner = match.group(1).strip()
    if not inner:
        return 0, 0

    variadic = "..." in inner

    parts: list[str] = []
    current: list[str] = []
    depth_square = 0
    depth_paren = 0
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
            token = "".join(current).strip()
            if token:
                parts.append(token)
            current = []
        else:
            current.append(ch)
    token = "".join(current).strip()
    if token:
        parts.append(token)

    required = 0
    optional = 0
    for part in parts:
        if not part or part == "...":
            continue
        if part.startswith("["):
            optional += _count_optional_args(part)
            continue
        if "[" in part:
            required += 1
            optional += _count_optional_args(part[part.index("[") :])
            continue
        required += 1

    min_arity = required
    max_arity = None if variadic else required + optional
    return min_arity, max_arity


def generate_rows(rows: Iterable[tuple[str, list[tuple[int, int | None]]]]) -> list[str]:
    lines: list[str] = []
    for function_name, signatures in rows:
        rendered = ", ".join(render_signature(min_a, max_a) for min_a, max_a in signatures)
        lines.append(
            f"    catalog.register(d, {rust_str(function_name)}, vec![{rendered}]);"
        )
    return lines


def main() -> int:
    args = parse_args()

    # Imported lazily so script help/inspection works without installed deps.
    import chdb  # type: ignore

    sql = f"{args.query.strip()} format JSONEachRow"
    result = chdb.query(sql)
    rows = [
        json.loads(line)
        for line in result.bytes().decode("utf-8").splitlines()
        if line.strip()
    ]
    if not rows:
        raise RuntimeError("system.functions query returned no rows")

    required_cols = ("name", "alias_to", "syntax")
    sample = rows[0]
    missing = [col for col in required_cols if col not in sample]
    if missing:
        raise RuntimeError(
            f"system.functions output missing required columns: {', '.join(missing)}"
        )

    signatures_by_name: dict[str, set[tuple[int, int | None]]] = {}
    aliases: list[tuple[str, str]] = []
    dropped_unknown_aliases = 0

    for row in rows:
        name = str(row.get("name", "")).strip()
        if not name:
            continue
        alias_to = str(row.get("alias_to", "") or "").strip()

        if alias_to:
            aliases.append((name, alias_to))
            continue

        syntax = str(row.get("syntax", "") or "").strip()
        min_a, max_a = parse_arity_from_syntax(syntax)
        signatures_by_name.setdefault(name, set()).add((min_a, max_a))

    # Resolve aliases from non-alias function signatures.
    lookup_by_upper = {name.upper(): sigs for name, sigs in signatures_by_name.items()}
    for alias_name, target_name in aliases:
        target_sigs = signatures_by_name.get(target_name)
        if target_sigs is None:
            target_sigs = lookup_by_upper.get(target_name.upper())
        if target_sigs is None:
            if args.include_unknown_alias_targets:
                signatures_by_name.setdefault(alias_name, set()).add((0, None))
            else:
                dropped_unknown_aliases += 1
            continue
        signatures_by_name.setdefault(alias_name, set()).update(target_sigs)

    sorted_entries: list[tuple[str, list[tuple[int, int | None]]]] = []
    for function_name in sorted(signatures_by_name, key=lambda n: (n.lower(), n)):
        signatures = sorted(
            signatures_by_name[function_name],
            key=lambda sig: (sig[0], 10**9 if sig[1] is None else sig[1]),
        )
        sorted_entries.append((function_name, signatures))

    body_lines = generate_rows(sorted_entries)

    output = "\n".join(
        [
            "use crate::{CatalogSink, FunctionNameCase, FunctionSignature};",
            "",
            "/// Register the ClickHouse function catalog.",
            "///",
            "/// This file is generated from system.functions via",
            "/// tools/clickhouse/extract_functions.py.",
            "pub(crate) fn register<S: CatalogSink>(catalog: &mut S) {",
            '    let d = "clickhouse";',
            "    catalog.set_dialect_name_case(d, FunctionNameCase::Insensitive);",
            *body_lines,
            "}",
            "",
        ]
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(output, encoding="utf-8")

    signature_count = sum(len(sigs) for _, sigs in sorted_entries)
    print(
        f"Generated {args.output} with {len(sorted_entries)} functions and "
        f"{signature_count} signatures "
        f"(source rows={len(rows)}, aliases={len(aliases)}, dropped_unknown_aliases={dropped_unknown_aliases})."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
