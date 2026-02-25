#!/usr/bin/env python3
"""
Extract DuckDB function metadata and generate Rust catalog code.

Usage:
  uv run --with duckdb python crates/polyglot-sql-function-catalogs/tools/duckdb/extract_functions.py
"""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Iterable


def parse_args() -> argparse.Namespace:
    repo_root = Path(__file__).resolve().parents[4]
    default_out = repo_root / "crates/polyglot-sql-function-catalogs/src/duckdb.rs"

    parser = argparse.ArgumentParser(
        description="Generate DuckDB function catalog Rust source from duckdb_functions().",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=default_out,
        help=f"Rust output path (default: {default_out})",
    )
    parser.add_argument(
        "--database",
        type=str,
        default=":memory:",
        help="DuckDB database path (default: :memory:)",
    )
    parser.add_argument(
        "--exclude-internal",
        action="store_true",
        help="Exclude internal DuckDB functions when an 'internal' column is present.",
    )
    parser.add_argument(
        "--function-type",
        action="append",
        default=[],
        help="Filter function types (repeatable), e.g. --function-type scalar --function-type aggregate.",
    )
    return parser.parse_args()


def as_bool(value: object) -> bool:
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, (list, tuple, set, dict)):
        return len(value) > 0
    text = str(value).strip().lower()
    if text in ("", "0", "false", "none", "null", "[]"):
        return False
    return True


def list_len(value: object) -> int:
    if value is None:
        return 0
    if isinstance(value, (list, tuple)):
        return len(value)
    text = str(value).strip()
    if not text:
        return 0
    if text.startswith("[") and text.endswith("]"):
        inner = text[1:-1].strip()
        if not inner:
            return 0
        return len([x for x in inner.split(",") if x.strip()])
    return 0


def rust_str(text: str) -> str:
    return '"' + text.replace("\\", "\\\\").replace('"', '\\"') + '"'


def render_signature(min_arity: int, max_arity: int | None) -> str:
    if max_arity is None:
        return f"FunctionSignature::variadic({min_arity})"
    if min_arity == max_arity:
        return f"FunctionSignature::exact({min_arity})"
    return f"FunctionSignature::range({min_arity}, {max_arity})"


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

    # Imported lazily so this script can be inspected without duckdb installed.
    import duckdb  # type: ignore

    con = duckdb.connect(database=args.database)
    rel = con.execute("SELECT * FROM duckdb_functions()")
    columns = [desc[0].lower() for desc in rel.description]
    index = {name: i for i, name in enumerate(columns)}
    rows = rel.fetchall()

    if "function_name" not in index:
        raise RuntimeError("duckdb_functions() result did not contain 'function_name'")

    function_type_filter = {value.lower() for value in args.function_type}

    signatures_by_name: dict[str, set[tuple[int, int | None]]] = {}

    for row in rows:
        name_obj = row[index["function_name"]]
        if name_obj is None:
            continue
        name = str(name_obj).strip()
        if not name:
            continue

        if function_type_filter and "function_type" in index:
            function_type = str(row[index["function_type"]]).lower()
            if function_type not in function_type_filter:
                continue

        if args.exclude_internal and "internal" in index:
            if as_bool(row[index["internal"]]):
                continue

        arity = 0
        if "parameter_types" in index:
            arity = list_len(row[index["parameter_types"]])
        elif "parameters" in index:
            arity = list_len(row[index["parameters"]])

        variadic = False
        if "varargs" in index:
            variadic = as_bool(row[index["varargs"]])

        signature = (arity, None) if variadic else (arity, arity)
        signatures_by_name.setdefault(name, set()).add(signature)

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
            "/// Register the DuckDB function catalog.",
            "///",
            "/// This file is generated from duckdb_functions() metadata via",
            "/// tools/duckdb/extract_functions.py.",
            "pub(crate) fn register<S: CatalogSink>(catalog: &mut S) {",
            '    let d = "duckdb";',
            "    catalog.set_dialect_name_case(d, FunctionNameCase::Insensitive);",
            *body_lines,
            "}",
            "",
        ]
    )

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(output, encoding="utf-8")

    print(
        f"Generated {args.output} with {len(sorted_entries)} functions and "
        f"{sum(len(sigs) for _, sigs in sorted_entries)} signatures."
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
