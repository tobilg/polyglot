"""Polyglot SQL — Rust-powered SQL transpiler for 32+ dialects."""

from polyglot_sql._polyglot_sql import (
    GenerateError,
    ParseError,
    PolyglotError,
    TranspileError,
    ValidationError,
    ValidationErrorInfo,
    ValidationResult,
    dialects,
    diff,
    format_sql,
    generate,
    lineage,
    optimize,
    parse,
    parse_one,
    source_tables,
    transpile,
    validate,
    version as _version,
)

format = format_sql
__version__ = _version()

__all__ = [
    "transpile",
    "parse",
    "parse_one",
    "generate",
    "format_sql",
    "format",
    "validate",
    "optimize",
    "lineage",
    "source_tables",
    "diff",
    "dialects",
    "PolyglotError",
    "ParseError",
    "GenerateError",
    "TranspileError",
    "ValidationError",
    "ValidationResult",
    "ValidationErrorInfo",
    "__version__",
]
