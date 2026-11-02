"""Output fingerprints for before/after compatibility checks, not timings.

Use the same script with each native Python build. Diff edit ordering is
unspecified, so only that operation's outer list is sorted before comparison.
All other array ordering, error classes/messages, spans, and types are retained.
"""
import hashlib
import json

import polyglot_sql as sql
from polyglot_sql import _polyglot_sql as native

SCHEMA = {"tables": [{"name": "t", "columns": [
    {"name": "x", "type": "INT"}, {"name": "flag", "type": "BOOLEAN"},
]}]}
QUERIES = [
    "SELECT x + 1 AS a, x + 2 AS b FROM t",
    "SELECT x + 1 AS a, a + 2 AS b FROM t",
    "WITH c AS (SELECT x + 1 AS a, flag FROM t) SELECT a + 1 AS b, b + 2 AS c FROM c",
    "WITH c AS (SELECT flag AS a FROM t) SELECT a + 1 AS b FROM c",
    "SELECT a.x AS x FROM t a JOIN t b ON a.x = b.x",
    "SELECT CASE WHEN flag THEN x ELSE 0 END AS x FROM t",
    "SELECT CAST(x AS DECIMAL(12,2)) AS x FROM t",
    "SELECT mystery(x) AS a, a + 1 AS b FROM t",
    "SELECT SUM(x) OVER (ORDER BY x) AS x FROM t",
    "SELECT x FROM t UNION ALL SELECT x + 1 FROM t",
    "SELECT x FROM t INTERSECT SELECT x FROM t",
    "SELECT *, x AS x FROM t",
    "SELECT x AS a, x AS a FROM t",
    "SELECT 'été', '😀', x AS x FROM t /* π */ WHERE x > 1",
    "SELECT DISTINCT ON (x) x FROM t ORDER BY x DESC NULLS LAST",
    "SELECT x FROM t ORDER BY x FETCH FIRST 2 ROWS WITH TIES",
    "SELECT NULL, TRUE, NOT flag, x::TEXT FROM t",
    "SELECT COUNT(*), SUM(x) FROM t",
    "SELECT x LIKE 'a%' FROM t",
    "SELECT café, δelta, 'été\n😀', \"naïve\" FROM données",
    "SELECT 'it''s été', $$été\n😀$$, $tag$δ$tag$, 1.25e-2 /* π */",
    "SELECT 中文(δ), café AS x FROM données WHERE café > 1",
    "SELECT flag AS x FROM t WHERE flag",
    "SELECT x FROM t GROUP BY x HAVING flag",
    "SELECT x FROM t QUALIFY flag",
    "SELECT t.x FROM t JOIN t s ON t.flag",
    "SELECT (SELECT x FROM t WHERE flag) AS x",
    "SELECT CAST(flag AS BOOLEAN) AS x FROM t",
]


def plain(value):
    if isinstance(value, sql.Expression):
        return value.to_dict()
    if isinstance(value, sql.ValidationResult):
        return {"valid": value.valid, "errors": [{
            key: getattr(error, key) for key in
            ("code", "message", "severity", "line", "col", "start", "end")
        } for error in value.errors]}
    return value


def record(operation, case, function, sort_edits=False):
    try:
        value = plain(function())
        if sort_edits:
            value.sort(key=lambda edit: json.dumps(edit, sort_keys=True))
        result = {"value": value}
    except Exception as error:
        result = {"error_type": type(error).__name__, "error": str(error)}
    encoded = json.dumps(result, sort_keys=True, ensure_ascii=True).encode()
    print(json.dumps({"operation": operation, "case": case,
                      "success": "value" in result, "sha256": hashlib.sha256(encoded).hexdigest()}))


for index, query in enumerate(QUERIES):
    record("parse", index, lambda: sql.parse_one(query, read="postgresql"))
    record("tokenize", index, lambda: sql.tokenize(query, "postgresql"))
    record("format", index, lambda: sql.format_sql(query, "postgresql"))
    record("annotate", index, lambda: native.annotate_types(query, SCHEMA, "postgresql"))
    record("lineage", index, lambda: sql.lineage("x", query, "postgresql"))
    record("openlineage", index, lambda: sql.openlineage_column_lineage(query, {
        "dialect": "postgresql", "producer": "performance-review",
        "datasetNamespace": "review", "outputDataset": {"namespace": "review", "name": "out"},
    }))
    for dialect in ("postgresql", "duckdb", "snowflake", "bigquery", "tsql"):
        record("validate_" + dialect, index,
               lambda: sql.validate_with_schema(query, SCHEMA, dialect, check_types=True))
    for dialect in sql.dialects():
        record("transpile_" + dialect, index,
               lambda: sql.transpile(query, read="postgresql", write=dialect))
    for target in QUERIES[:8]:
        record("diff", [index, QUERIES.index(target)],
               lambda: sql.diff(query, target, "postgresql"), sort_edits=True)
