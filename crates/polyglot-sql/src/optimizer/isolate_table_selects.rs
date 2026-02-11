//! Isolate Table Selects Optimization Pass
//!
//! This module wraps plain table references in subqueries (`SELECT * FROM table`)
//! when multiple tables are present in a scope. This normalization is needed for
//! other optimizations (like merge_subqueries) to work correctly, since they
//! expect each source in a multi-table query to be a subquery rather than a bare
//! table reference.
//!
//! Ported from sqlglot's optimizer/isolate_table_selects.py

use crate::dialects::DialectType;
use crate::expressions::*;
use crate::schema::Schema;

/// Error type for the isolate_table_selects pass
#[derive(Debug, Clone, thiserror::Error)]
pub enum IsolateTableSelectsError {
    #[error("Tables require an alias: {0}")]
    MissingAlias(String),
}

/// Wrap plain table references in subqueries when multiple sources are present.
///
/// When a SELECT has multiple sources (FROM + JOINs, or multiple FROM tables),
/// each bare `Table` reference is replaced with:
///
/// ```sql
/// (SELECT * FROM table AS alias) AS alias
/// ```
///
/// This makes every source a subquery, which simplifies downstream
/// optimizations such as `merge_subqueries`.
///
/// # Arguments
///
/// * `expression` - The SQL expression tree to transform
/// * `schema` - Optional schema for looking up column names (used to skip
///   tables whose columns are unknown, matching the Python behavior)
/// * `_dialect` - Optional dialect (reserved for future use)
///
/// # Returns
///
/// The transformed expression with isolated table selects
pub fn isolate_table_selects(
    expression: Expression,
    schema: Option<&dyn Schema>,
    _dialect: Option<DialectType>,
) -> Expression {
    match expression {
        Expression::Select(select) => {
            let transformed = isolate_select(*select, schema);
            Expression::Select(Box::new(transformed))
        }
        Expression::Union(mut union) => {
            union.left = isolate_table_selects(union.left, schema, _dialect);
            union.right = isolate_table_selects(union.right, schema, _dialect);
            Expression::Union(union)
        }
        Expression::Intersect(mut intersect) => {
            intersect.left = isolate_table_selects(intersect.left, schema, _dialect);
            intersect.right = isolate_table_selects(intersect.right, schema, _dialect);
            Expression::Intersect(intersect)
        }
        Expression::Except(mut except) => {
            except.left = isolate_table_selects(except.left, schema, _dialect);
            except.right = isolate_table_selects(except.right, schema, _dialect);
            Expression::Except(except)
        }
        other => other,
    }
}

/// Process a single SELECT statement, wrapping bare table references in
/// subqueries when multiple sources are present.
fn isolate_select(mut select: Select, schema: Option<&dyn Schema>) -> Select {
    // First, recursively process CTEs
    if let Some(ref mut with) = select.with {
        for cte in &mut with.ctes {
            cte.this = isolate_table_selects(cte.this.clone(), schema, None);
        }
    }

    // Recursively process subqueries in FROM and JOINs
    if let Some(ref mut from) = select.from {
        for expr in &mut from.expressions {
            if let Expression::Subquery(ref mut sq) = expr {
                sq.this = isolate_table_selects(sq.this.clone(), schema, None);
            }
        }
    }
    for join in &mut select.joins {
        if let Expression::Subquery(ref mut sq) = join.this {
            sq.this = isolate_table_selects(sq.this.clone(), schema, None);
        }
    }

    // Count the total number of sources (FROM expressions + JOINs)
    let source_count = count_sources(&select);

    // Only isolate when there are multiple sources
    if source_count <= 1 {
        return select;
    }

    // Wrap bare table references in FROM clause
    if let Some(ref mut from) = select.from {
        from.expressions = from
            .expressions
            .drain(..)
            .map(|expr| maybe_wrap_table(expr, schema))
            .collect();
    }

    // Wrap bare table references in JOINs
    for join in &mut select.joins {
        join.this = maybe_wrap_table(join.this.clone(), schema);
    }

    select
}

/// Count the total number of source tables/subqueries in a SELECT.
///
/// This counts FROM expressions plus JOINs.
fn count_sources(select: &Select) -> usize {
    let from_count = select
        .from
        .as_ref()
        .map(|f| f.expressions.len())
        .unwrap_or(0);
    let join_count = select.joins.len();
    from_count + join_count
}

/// If the expression is a bare `Table` reference that should be isolated,
/// wrap it in a `(SELECT * FROM table AS alias) AS alias` subquery.
///
/// A table is wrapped when:
/// - It is an `Expression::Table` (not already a subquery)
/// - It has an alias (required by the Python reference)
/// - If a schema is provided, the table must have known columns in the schema
///
/// If no schema is provided, all aliased tables are wrapped (simplified mode).
fn maybe_wrap_table(expression: Expression, schema: Option<&dyn Schema>) -> Expression {
    match expression {
        Expression::Table(ref table) => {
            // If a schema is provided, check that the table has known columns.
            // If we cannot find columns for the table, skip wrapping it (matching
            // the Python behavior where `schema.column_names(source)` must be truthy).
            if let Some(s) = schema {
                let table_name = full_table_name(table);
                if s.column_names(&table_name).unwrap_or_default().is_empty() {
                    return expression;
                }
            }

            // The table must have an alias; if it does not, we leave it as-is.
            // The Python version raises an OptimizeError here, but in practice
            // earlier passes (qualify_tables) ensure aliases are present.
            let alias_name = match &table.alias {
                Some(alias) if !alias.name.is_empty() => alias.name.clone(),
                _ => return expression,
            };

            wrap_table_in_subquery(table.clone(), &alias_name)
        }
        _ => expression,
    }
}

/// Build `(SELECT * FROM table_ref AS alias) AS alias` from a table reference.
///
/// The inner table reference keeps the original alias so that
/// `FROM t AS t` becomes `(SELECT * FROM t AS t) AS t`.
fn wrap_table_in_subquery(table: TableRef, alias_name: &str) -> Expression {
    // Build: SELECT * FROM <table>
    let inner_select = Select::new()
        .column(Expression::Star(Star {
            table: None,
            except: None,
            replace: None,
            rename: None,
            trailing_comments: Vec::new(),
        }))
        .from(Expression::Table(table));

    // Wrap the SELECT in a Subquery with the original alias
    Expression::Subquery(Box::new(Subquery {
        this: Expression::Select(Box::new(inner_select)),
        alias: Some(Identifier::new(alias_name)),
        column_aliases: Vec::new(),
        order_by: None,
        limit: None,
        offset: None,
        distribute_by: None,
        sort_by: None,
        cluster_by: None,
        lateral: false,
        modifiers_inside: false,
        trailing_comments: Vec::new(),
    }))
}

/// Construct the fully qualified table name from a `TableRef`.
///
/// Produces `catalog.schema.name` or `schema.name` or just `name`
/// depending on which parts are present.
fn full_table_name(table: &TableRef) -> String {
    let mut parts = Vec::new();
    if let Some(ref catalog) = table.catalog {
        parts.push(catalog.name.as_str());
    }
    if let Some(ref schema) = table.schema {
        parts.push(schema.name.as_str());
    }
    parts.push(&table.name.name);
    parts.join(".")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::Generator;
    use crate::parser::Parser;
    use crate::schema::MappingSchema;

    /// Helper: parse SQL into an Expression
    fn parse(sql: &str) -> Expression {
        Parser::parse_sql(sql).expect("Failed to parse")[0].clone()
    }

    /// Helper: generate SQL from an Expression
    fn gen(expr: &Expression) -> String {
        Generator::new().generate(expr).unwrap()
    }

    // ---------------------------------------------------------------
    // Basic: single source should NOT be wrapped
    // ---------------------------------------------------------------

    #[test]
    fn test_single_table_unchanged() {
        let sql = "SELECT * FROM t AS t";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        // Should remain a plain table, not wrapped in a subquery
        assert!(
            !output.contains("(SELECT"),
            "Single table should not be wrapped: {output}"
        );
    }

    #[test]
    fn test_single_subquery_unchanged() {
        let sql = "SELECT * FROM (SELECT 1) AS t";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        // Still just one source, no additional wrapping expected
        assert_eq!(
            output.matches("(SELECT").count(),
            1,
            "Single subquery source should not gain extra wrapping: {output}"
        );
    }

    // ---------------------------------------------------------------
    // Multiple sources: tables should be wrapped
    // ---------------------------------------------------------------

    #[test]
    fn test_two_tables_joined() {
        let sql = "SELECT * FROM a AS a JOIN b AS b ON a.id = b.id";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        // Both tables should now be subqueries
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "FROM table should be wrapped: {output}"
        );
        assert!(
            output.contains("(SELECT * FROM b AS b) AS b"),
            "JOIN table should be wrapped: {output}"
        );
    }

    #[test]
    fn test_table_with_join_subquery() {
        // If one source is already a subquery and the other is a table,
        // only the bare table should be wrapped.
        let sql = "SELECT * FROM a AS a JOIN (SELECT * FROM b) AS b ON a.id = b.id";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        // `a` should be wrapped
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "Bare table should be wrapped: {output}"
        );
        // `b` is already a subquery, so it should appear once
        // (no double-wrapping)
        assert_eq!(
            output.matches("(SELECT * FROM b)").count(),
            1,
            "Already-subquery source should not be double-wrapped: {output}"
        );
    }

    #[test]
    fn test_no_alias_not_wrapped() {
        // Tables without aliases are left alone (in real pipelines,
        // qualify_tables runs first and assigns aliases).
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        // Without aliases, tables should not be wrapped
        assert!(
            !output.contains("(SELECT * FROM a"),
            "Table without alias should not be wrapped: {output}"
        );
    }

    // ---------------------------------------------------------------
    // Schema-aware mode: only wrap tables with known columns
    // ---------------------------------------------------------------

    #[test]
    fn test_schema_known_table_wrapped() {
        let mut schema = MappingSchema::new();
        schema
            .add_table(
                "a",
                &[("id".to_string(), DataType::Int { length: None, integer_spelling: false })],
                None,
            )
            .unwrap();
        schema
            .add_table(
                "b",
                &[("id".to_string(), DataType::Int { length: None, integer_spelling: false })],
                None,
            )
            .unwrap();

        let sql = "SELECT * FROM a AS a JOIN b AS b ON a.id = b.id";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, Some(&schema), None);
        let output = gen(&result);
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "Known table 'a' should be wrapped: {output}"
        );
        assert!(
            output.contains("(SELECT * FROM b AS b) AS b"),
            "Known table 'b' should be wrapped: {output}"
        );
    }

    #[test]
    fn test_schema_unknown_table_not_wrapped() {
        let mut schema = MappingSchema::new();
        // Only 'a' is in the schema; 'b' is unknown
        schema
            .add_table(
                "a",
                &[("id".to_string(), DataType::Int { length: None, integer_spelling: false })],
                None,
            )
            .unwrap();

        let sql = "SELECT * FROM a AS a JOIN b AS b ON a.id = b.id";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, Some(&schema), None);
        let output = gen(&result);
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "Known table 'a' should be wrapped: {output}"
        );
        // 'b' is not in schema, so it should remain a plain table
        assert!(
            !output.contains("(SELECT * FROM b AS b) AS b"),
            "Unknown table 'b' should NOT be wrapped: {output}"
        );
    }

    // ---------------------------------------------------------------
    // Recursive: CTEs and nested subqueries
    // ---------------------------------------------------------------

    #[test]
    fn test_cte_inner_query_processed() {
        let sql = "WITH cte AS (SELECT * FROM x AS x JOIN y AS y ON x.id = y.id) SELECT * FROM cte AS c";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        // Inside the CTE, x and y should be wrapped
        assert!(
            output.contains("(SELECT * FROM x AS x) AS x"),
            "CTE inner table 'x' should be wrapped: {output}"
        );
        assert!(
            output.contains("(SELECT * FROM y AS y) AS y"),
            "CTE inner table 'y' should be wrapped: {output}"
        );
    }

    #[test]
    fn test_nested_subquery_processed() {
        let sql = "SELECT * FROM (SELECT * FROM a AS a JOIN b AS b ON a.id = b.id) AS sub";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        // The inner SELECT has two sources; they should be wrapped
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "Nested inner table 'a' should be wrapped: {output}"
        );
    }

    // ---------------------------------------------------------------
    // Set operations: UNION, INTERSECT, EXCEPT
    // ---------------------------------------------------------------

    #[test]
    fn test_union_both_sides_processed() {
        let sql = "SELECT * FROM a AS a JOIN b AS b ON a.id = b.id UNION ALL SELECT * FROM c AS c";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        // Left side has two sources - should be wrapped
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "UNION left side should be processed: {output}"
        );
        // Right side has only one source - should NOT be wrapped
        assert!(
            !output.contains("(SELECT * FROM c AS c) AS c"),
            "UNION right side (single source) should not be wrapped: {output}"
        );
    }

    // ---------------------------------------------------------------
    // Edge cases
    // ---------------------------------------------------------------

    #[test]
    fn test_cross_join() {
        let sql = "SELECT * FROM a AS a CROSS JOIN b AS b";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "CROSS JOIN table 'a' should be wrapped: {output}"
        );
        assert!(
            output.contains("(SELECT * FROM b AS b) AS b"),
            "CROSS JOIN table 'b' should be wrapped: {output}"
        );
    }

    #[test]
    fn test_multiple_from_tables() {
        // Comma-separated FROM (implicit cross join)
        let sql = "SELECT * FROM a AS a, b AS b";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "Comma-join table 'a' should be wrapped: {output}"
        );
        assert!(
            output.contains("(SELECT * FROM b AS b) AS b"),
            "Comma-join table 'b' should be wrapped: {output}"
        );
    }

    #[test]
    fn test_three_way_join() {
        let sql = "SELECT * FROM a AS a JOIN b AS b ON a.id = b.id JOIN c AS c ON b.id = c.id";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        assert!(
            output.contains("(SELECT * FROM a AS a) AS a"),
            "Three-way join: 'a' should be wrapped: {output}"
        );
        assert!(
            output.contains("(SELECT * FROM b AS b) AS b"),
            "Three-way join: 'b' should be wrapped: {output}"
        );
        assert!(
            output.contains("(SELECT * FROM c AS c) AS c"),
            "Three-way join: 'c' should be wrapped: {output}"
        );
    }

    #[test]
    fn test_qualified_table_name_with_schema() {
        let mut schema = MappingSchema::new();
        schema
            .add_table(
                "mydb.a",
                &[("id".to_string(), DataType::Int { length: None, integer_spelling: false })],
                None,
            )
            .unwrap();
        schema
            .add_table(
                "mydb.b",
                &[("id".to_string(), DataType::Int { length: None, integer_spelling: false })],
                None,
            )
            .unwrap();

        let sql = "SELECT * FROM mydb.a AS a JOIN mydb.b AS b ON a.id = b.id";
        let expr = parse(sql);
        let result = isolate_table_selects(expr, Some(&schema), None);
        let output = gen(&result);
        assert!(
            output.contains("(SELECT * FROM mydb.a AS a) AS a"),
            "Qualified table 'mydb.a' should be wrapped: {output}"
        );
        assert!(
            output.contains("(SELECT * FROM mydb.b AS b) AS b"),
            "Qualified table 'mydb.b' should be wrapped: {output}"
        );
    }

    #[test]
    fn test_non_select_expression_unchanged() {
        // Non-SELECT expressions (e.g., INSERT, CREATE) pass through unchanged
        let sql = "INSERT INTO t VALUES (1)";
        let expr = parse(sql);
        let original = gen(&expr);
        let result = isolate_table_selects(expr, None, None);
        let output = gen(&result);
        assert_eq!(original, output, "Non-SELECT should be unchanged");
    }
}
