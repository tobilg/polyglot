# Set-operation output typing

`analyze_query` and scoped type annotation resolve `UNION`, `INTERSECT`, and
`EXCEPT` outputs from both operands. SQL result typing also considers the right
operand of `INTERSECT` and `EXCEPT`, even though its lineage role is `filter`.

For example, in Snowflake:

```sql
SELECT CAST(amount AS INTEGER) AS amount FROM orders
UNION ALL
SELECT CAST(amount AS FLOAT) AS amount FROM orders
```

The combined projection has `typeHint: "FLOAT"` and no `castType`. Each branch
retains its own explicit cast. Reversing the branches gives the same type.

## Resolution and propagation

- Resolve the actual query tree bottom-up. Each positional column is combined
  independently; aliases do not identify positional columns across branches.
- Use the shared set-operation layout for `BY NAME`, `CORRESPONDING`, and their
  supported modifiers. Missing name-aligned columns contribute SQL NULL.
- Distinguish SQL NULL, untyped string literals, known types, and unresolved
  expressions. An unknown function or column cannot be treated as NULL or
  ignored just because the other operand has a parameterized type.
- Propagate resolved columns through CTEs, derived tables, explicit column
  aliases, single-column scalar subqueries, and `ARRAY(query)` constructors.
  Multi-column relations cannot supply a scalar subquery type.
- Preserve a combined `castType` only if both aligned outputs have a consensus
  explicit cast whose semantic target matches the resolved type. Synthetic NULL
  padding and passthrough columns do not supply an explicit cast.
- Keep layout, lineage, nullability, source SQL, and scalar arithmetic coercion
  separate from set-operation result typing. No new API options are required.

Policies are immutable and selected explicitly for every dialect enum variant.
Query-analysis caching is local to one annotated tree. Resolution has a depth
bound and leaves uncertain metadata unresolved; it does not validate whether
every SQL expression is executable on a particular engine.

## Dialect rules and evidence

The policy covers all 34 named dialects and Generic. It is intentionally a
partial type system: unsupported combinations return no hint, rather than
claiming a complete implementation of an engine's implicit-cast rules.

| Dialects | Implemented policy and limits |
| --- | --- |
| Snowflake | Numeric widening, FLOAT/DOUBLE equivalence, explicit decimal precision and scale, text and temporal combinations. Named unions retain the existing NULL-padding layout. Session-dependent timestamp conversions stay unresolved. |
| PostgreSQL, Materialize, RisingWave, CockroachDB | Numeric categories, untyped string versus typed text, pairwise all-NULL resolution, and compatible nested types. Different numeric typmods resolve to unconstrained NUMERIC. Custom/domain coercions are unresolved. |
| MySQL, TiDB, SingleStore | Numeric widening and numeric/text union results. Mixed signed/unsigned combinations without an explicit rule and mode-sensitive conversions stay unresolved. |
| BigQuery | INT64/FLOAT64 and NUMERIC/BIGNUMERIC supertypes, all-NULL INT64, validated ISO temporal literals, and strict ARRAY/STRUCT element compatibility. Typed STRING does not get literal-only temporal coercion. |
| DuckDB | Combination casting, signed/unsigned widening, decimal shape merging, recursive arrays/maps, and STRUCT field union by name. |
| SQLite | Identical known types can be retained. Mixed compound-query types and mixed NULL/typed outputs are unresolved because result affinity is indeterminate. |
| Hive | Numeric, string, and temporal groups remain distinct; compatible nested types resolve recursively. |
| Spark | Compatible same-group coercions, with unresolved results where FLOAT widening depends on ANSI mode. No ANSI mode is assumed. |
| Databricks | Numeric precedence skips FLOAT when combining it with another numeric type; compatible nested types resolve recursively. |
| Trino, Presto, Athena, Dune | Numeric widening, explicit decimal shapes, character lengths, and compatible nested types. Numeric/text implicit coercion is not inferred. Decimal overflow is conservative. |
| Redshift | Numeric and explicit decimal shape merging; overflow is unresolved. |
| T-SQL, Fabric | Numeric precedence, decimal precision/scale merging with the 38-digit cap, and character lengths. Unsupported cross-category conversions are unresolved. |
| Oracle | NUMBER, BINARY_FLOAT, BINARY_DOUBLE precedence and Oracle character-length rules. Cross-category conversions are not inferred. |
| ClickHouse | Numeric representability, Nullable propagation, and compatible nested types. Int64/Float64 and Int64/UInt64 do not silently become lossy common types. |
| Teradata | Compatible known operands retain the first SELECT's type, as specified by the engine. |
| DataFusion | Numeric and decimal coercion, string/numeric union output, recursive nested types, and STRUCT matching by field name. Unsupported Arrow-specific types are unresolved. |
| Doris, StarRocks, Exasol, Vertica, Generic | Same-category scalar widening and explicit decimal shapes. Engine-specific complex and cross-category coercions remain unresolved. |
| Drill, Dremio, Druid, Solr, Tableau | Conservative scalar rules; mixed floating, decimal, temporal, and complex combinations without a supported rule remain unresolved. Druid DECIMAL/REAL normalize to DOUBLE. |

Engine documentation and source used to distinguish these rules:

- [Snowflake set operators](https://docs.snowflake.com/en/sql-reference/operators-query)
  and [numeric types](https://docs.snowflake.com/en/sql-reference/data-types-numeric).
- [PostgreSQL UNION type resolution](https://www.postgresql.org/docs/18/typeconv-union-case.html).
- [MySQL set-operation result columns](https://dev.mysql.com/doc/refman/8.4/en/set-operations.html).
- [BigQuery conversion and supertyping](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/conversion_rules).
- [DuckDB combination casting](https://duckdb.org/docs/stable/sql/data_types/typecasting).
- [SQLite compound-view affinity](https://www.sqlite.org/datatype3.html#column_affinity_for_compound_views).
- [Hive UNION groups](https://hive.apache.org/docs/latest/language/languagemanual-union/).
- [Spark ANSI compliance](https://spark.apache.org/docs/latest/sql-ref-ansi-compliance.html)
  and [Databricks type rules](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatype-rules).
- [Trino conversion](https://trino.io/docs/current/functions/conversion.html).
- [Redshift set operators](https://docs.aws.amazon.com/redshift/latest/dg/r_UNION.html).
- [T-SQL precision, scale, and length](https://learn.microsoft.com/en-us/sql/t-sql/data-types/precision-scale-and-length-transact-sql).
- [Oracle set-operator type rules](https://docs.oracle.com/en/database/oracle/oracle-database/26/sqlrf/The-UNION-ALL-INTERSECT-MINUS-Operators.html).
- [ClickHouse UNION](https://clickhouse.com/docs/sql-reference/statements/select/union)
  and [least-supertype implementation](https://github.com/ClickHouse/ClickHouse/blob/master/src/DataTypes/getLeastSupertype.cpp).
- [Teradata SELECT order and result type](https://docs.teradata.com/r/Enterprise_IntelliFlex_VMware/SQL-Data-Manipulation-Language/Set-Operators/UNION-Operator/Examples/Example-Effect-of-the-Order-of-SELECT-Statements-on-Data-Type).
- [DataFusion union coercion](https://github.com/apache/datafusion/blob/main/datafusion/expr-common/src/type_coercion/binary.rs)
  and [STRUCT field matching](https://datafusion.apache.org/user-guide/sql/struct_coercion.html).
- [Druid SQL type mapping](https://druid.apache.org/docs/latest/querying/sql-data-types/).

Recursive CTEs bind their anchor before typing a self-referencing arm. Stable
types propagate; a changing type remains unresolved rather than assuming that
regular UNION widening is valid for recursion. Unexpanded stars, mismatched
positional widths, ambiguous names, unknown nested members, unsupported type
parameters, and engine-version/settings-dependent rules remain conservative.

## SQLGlot comparison and tests

The reference checkout is SQLGlot 30.14.0. The adjacent audit covered its scoped
source selection, set-operation resolution, scalar subqueries, BigQuery array
and row typing, name-alignment resolver, and available dialect type overrides.

The implementation does not inherit SQLGlot's early return for parameterized
types, alias-keyed nested-union accumulation, or first-SELECT scalar/row
selection. Its mutable inherited coercion sets also do not serve as shared
runtime policies here.

The existing SQLGlot union annotation test and its three simple BigQuery
`ARRAY(SELECT ... UNION ALL ...)` text fixtures were executed. They pass; no
fixture exclusion is needed. More demanding alias and parameter cases are
covered by native regressions in the existing Rust query-analysis and type
annotation tests, plus existing Python and FFI test files.
