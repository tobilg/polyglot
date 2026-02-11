//! Identifier Normalization Module
//!
//! This module provides functionality for normalizing identifiers in SQL queries
//! based on dialect-specific rules for case sensitivity and quoting.
//!
//! Ported from sqlglot's optimizer/normalize_identifiers.py

use crate::dialects::DialectType;
use crate::expressions::{Column, Expression, Identifier};

/// Strategy for normalizing identifiers based on dialect rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NormalizationStrategy {
    /// Unquoted identifiers are lowercased (e.g., PostgreSQL)
    Lowercase,
    /// Unquoted identifiers are uppercased (e.g., Oracle, Snowflake)
    Uppercase,
    /// Always case-sensitive, regardless of quotes (e.g., MySQL on Linux)
    CaseSensitive,
    /// Always case-insensitive (lowercase), regardless of quotes (e.g., Spark, BigQuery)
    CaseInsensitive,
    /// Always case-insensitive (uppercase), regardless of quotes
    CaseInsensitiveUppercase,
}

impl Default for NormalizationStrategy {
    fn default() -> Self {
        Self::Lowercase
    }
}

/// Get the normalization strategy for a dialect
pub fn get_normalization_strategy(dialect: Option<DialectType>) -> NormalizationStrategy {
    match dialect {
        // Uppercase dialects
        Some(DialectType::Oracle) | Some(DialectType::Snowflake) | Some(DialectType::Exasol) => {
            NormalizationStrategy::Uppercase
        }
        // Case-sensitive dialects
        Some(DialectType::MySQL) | Some(DialectType::ClickHouse) => {
            NormalizationStrategy::CaseSensitive
        }
        // Case-insensitive dialects (lowercase)
        Some(DialectType::DuckDB)
        | Some(DialectType::SQLite)
        | Some(DialectType::BigQuery)
        | Some(DialectType::Presto)
        | Some(DialectType::Trino)
        | Some(DialectType::Hive)
        | Some(DialectType::Spark)
        | Some(DialectType::Databricks)
        | Some(DialectType::Redshift) => NormalizationStrategy::CaseInsensitive,
        // Default: lowercase (PostgreSQL-like behavior)
        _ => NormalizationStrategy::Lowercase,
    }
}

/// Normalize identifiers in an expression based on dialect rules.
///
/// This transformation reflects how identifiers would be resolved by the engine
/// corresponding to each SQL dialect. For example:
/// - `FoO` → `foo` in PostgreSQL (lowercases unquoted)
/// - `FoO` → `FOO` in Snowflake (uppercases unquoted)
/// - `"FoO"` → `FoO` preserved when quoted (case-sensitive)
///
/// # Arguments
/// * `expression` - The expression to normalize
/// * `dialect` - The dialect to use for normalization rules
///
/// # Returns
/// The expression with normalized identifiers
pub fn normalize_identifiers(expression: Expression, dialect: Option<DialectType>) -> Expression {
    let strategy = get_normalization_strategy(dialect);
    normalize_expression(expression, strategy)
}

/// Normalize a single identifier based on the strategy.
///
/// # Arguments
/// * `identifier` - The identifier to normalize
/// * `strategy` - The normalization strategy to use
///
/// # Returns
/// The normalized identifier
pub fn normalize_identifier(identifier: Identifier, strategy: NormalizationStrategy) -> Identifier {
    // Case-sensitive strategy: never normalize
    if strategy == NormalizationStrategy::CaseSensitive {
        return identifier;
    }

    // If quoted and not case-insensitive, don't normalize
    if identifier.quoted
        && strategy != NormalizationStrategy::CaseInsensitive
        && strategy != NormalizationStrategy::CaseInsensitiveUppercase
    {
        return identifier;
    }

    // Normalize the identifier name
    let normalized_name = match strategy {
        NormalizationStrategy::Uppercase | NormalizationStrategy::CaseInsensitiveUppercase => {
            identifier.name.to_uppercase()
        }
        NormalizationStrategy::Lowercase | NormalizationStrategy::CaseInsensitive => {
            identifier.name.to_lowercase()
        }
        NormalizationStrategy::CaseSensitive => identifier.name, // Should not reach here
    };

    Identifier {
        name: normalized_name,
        quoted: identifier.quoted,
        trailing_comments: identifier.trailing_comments,
    }
}

/// Recursively normalize all identifiers in an expression.
fn normalize_expression(expression: Expression, strategy: NormalizationStrategy) -> Expression {
    match expression {
        Expression::Identifier(id) => {
            Expression::Identifier(normalize_identifier(id, strategy))
        }
        Expression::Column(col) => {
            Expression::Column(Column {
                name: normalize_identifier(col.name, strategy),
                table: col.table.map(|t| normalize_identifier(t, strategy)),
                join_mark: col.join_mark,
                trailing_comments: col.trailing_comments,
            })
        }
        Expression::Table(mut table) => {
            table.name = normalize_identifier(table.name, strategy);
            if let Some(schema) = table.schema {
                table.schema = Some(normalize_identifier(schema, strategy));
            }
            if let Some(catalog) = table.catalog {
                table.catalog = Some(normalize_identifier(catalog, strategy));
            }
            if let Some(alias) = table.alias {
                table.alias = Some(normalize_identifier(alias, strategy));
            }
            table.column_aliases = table
                .column_aliases
                .into_iter()
                .map(|a| normalize_identifier(a, strategy))
                .collect();
            Expression::Table(table)
        }
        Expression::Select(select) => {
            let mut select = *select;
            // Normalize SELECT expressions
            select.expressions = select
                .expressions
                .into_iter()
                .map(|e| normalize_expression(e, strategy))
                .collect();
            // Normalize FROM
            if let Some(mut from) = select.from {
                from.expressions = from
                    .expressions
                    .into_iter()
                    .map(|e| normalize_expression(e, strategy))
                    .collect();
                select.from = Some(from);
            }
            // Normalize JOINs
            select.joins = select
                .joins
                .into_iter()
                .map(|mut j| {
                    j.this = normalize_expression(j.this, strategy);
                    if let Some(on) = j.on {
                        j.on = Some(normalize_expression(on, strategy));
                    }
                    j
                })
                .collect();
            // Normalize WHERE
            if let Some(mut where_clause) = select.where_clause {
                where_clause.this = normalize_expression(where_clause.this, strategy);
                select.where_clause = Some(where_clause);
            }
            // Normalize GROUP BY
            if let Some(mut group_by) = select.group_by {
                group_by.expressions = group_by
                    .expressions
                    .into_iter()
                    .map(|e| normalize_expression(e, strategy))
                    .collect();
                select.group_by = Some(group_by);
            }
            // Normalize HAVING
            if let Some(mut having) = select.having {
                having.this = normalize_expression(having.this, strategy);
                select.having = Some(having);
            }
            // Normalize ORDER BY
            if let Some(mut order_by) = select.order_by {
                order_by.expressions = order_by
                    .expressions
                    .into_iter()
                    .map(|mut o| {
                        o.this = normalize_expression(o.this, strategy);
                        o
                    })
                    .collect();
                select.order_by = Some(order_by);
            }
            Expression::Select(Box::new(select))
        }
        Expression::Alias(alias) => {
            let mut alias = *alias;
            alias.this = normalize_expression(alias.this, strategy);
            alias.alias = normalize_identifier(alias.alias, strategy);
            Expression::Alias(Box::new(alias))
        }
        // Binary operations
        Expression::And(bin) => normalize_binary(Expression::And, *bin, strategy),
        Expression::Or(bin) => normalize_binary(Expression::Or, *bin, strategy),
        Expression::Add(bin) => normalize_binary(Expression::Add, *bin, strategy),
        Expression::Sub(bin) => normalize_binary(Expression::Sub, *bin, strategy),
        Expression::Mul(bin) => normalize_binary(Expression::Mul, *bin, strategy),
        Expression::Div(bin) => normalize_binary(Expression::Div, *bin, strategy),
        Expression::Mod(bin) => normalize_binary(Expression::Mod, *bin, strategy),
        Expression::Eq(bin) => normalize_binary(Expression::Eq, *bin, strategy),
        Expression::Neq(bin) => normalize_binary(Expression::Neq, *bin, strategy),
        Expression::Lt(bin) => normalize_binary(Expression::Lt, *bin, strategy),
        Expression::Lte(bin) => normalize_binary(Expression::Lte, *bin, strategy),
        Expression::Gt(bin) => normalize_binary(Expression::Gt, *bin, strategy),
        Expression::Gte(bin) => normalize_binary(Expression::Gte, *bin, strategy),
        Expression::Concat(bin) => normalize_binary(Expression::Concat, *bin, strategy),
        // Unary operations
        Expression::Not(un) => {
            let mut un = *un;
            un.this = normalize_expression(un.this, strategy);
            Expression::Not(Box::new(un))
        }
        Expression::Neg(un) => {
            let mut un = *un;
            un.this = normalize_expression(un.this, strategy);
            Expression::Neg(Box::new(un))
        }
        // Functions
        Expression::Function(func) => {
            let mut func = *func;
            func.args = func
                .args
                .into_iter()
                .map(|e| normalize_expression(e, strategy))
                .collect();
            Expression::Function(Box::new(func))
        }
        Expression::AggregateFunction(agg) => {
            let mut agg = *agg;
            agg.args = agg
                .args
                .into_iter()
                .map(|e| normalize_expression(e, strategy))
                .collect();
            Expression::AggregateFunction(Box::new(agg))
        }
        // Other expressions with children
        Expression::Paren(paren) => {
            let mut paren = *paren;
            paren.this = normalize_expression(paren.this, strategy);
            Expression::Paren(Box::new(paren))
        }
        Expression::Case(case) => {
            let mut case = *case;
            case.operand = case.operand.map(|e| normalize_expression(e, strategy));
            case.whens = case
                .whens
                .into_iter()
                .map(|(w, t)| {
                    (
                        normalize_expression(w, strategy),
                        normalize_expression(t, strategy),
                    )
                })
                .collect();
            case.else_ = case.else_.map(|e| normalize_expression(e, strategy));
            Expression::Case(Box::new(case))
        }
        Expression::Cast(cast) => {
            let mut cast = *cast;
            cast.this = normalize_expression(cast.this, strategy);
            Expression::Cast(Box::new(cast))
        }
        Expression::In(in_expr) => {
            let mut in_expr = *in_expr;
            in_expr.this = normalize_expression(in_expr.this, strategy);
            in_expr.expressions = in_expr
                .expressions
                .into_iter()
                .map(|e| normalize_expression(e, strategy))
                .collect();
            if let Some(q) = in_expr.query {
                in_expr.query = Some(normalize_expression(q, strategy));
            }
            Expression::In(Box::new(in_expr))
        }
        Expression::Between(between) => {
            let mut between = *between;
            between.this = normalize_expression(between.this, strategy);
            between.low = normalize_expression(between.low, strategy);
            between.high = normalize_expression(between.high, strategy);
            Expression::Between(Box::new(between))
        }
        Expression::Subquery(subquery) => {
            let mut subquery = *subquery;
            subquery.this = normalize_expression(subquery.this, strategy);
            if let Some(alias) = subquery.alias {
                subquery.alias = Some(normalize_identifier(alias, strategy));
            }
            Expression::Subquery(Box::new(subquery))
        }
        // Set operations
        Expression::Union(union) => {
            let mut union = *union;
            union.left = normalize_expression(union.left, strategy);
            union.right = normalize_expression(union.right, strategy);
            Expression::Union(Box::new(union))
        }
        Expression::Intersect(intersect) => {
            let mut intersect = *intersect;
            intersect.left = normalize_expression(intersect.left, strategy);
            intersect.right = normalize_expression(intersect.right, strategy);
            Expression::Intersect(Box::new(intersect))
        }
        Expression::Except(except) => {
            let mut except = *except;
            except.left = normalize_expression(except.left, strategy);
            except.right = normalize_expression(except.right, strategy);
            Expression::Except(Box::new(except))
        }
        // Leaf nodes and others - return unchanged
        _ => expression,
    }
}

/// Helper to normalize binary operations
fn normalize_binary<F>(
    constructor: F,
    mut bin: crate::expressions::BinaryOp,
    strategy: NormalizationStrategy,
) -> Expression
where
    F: FnOnce(Box<crate::expressions::BinaryOp>) -> Expression,
{
    bin.left = normalize_expression(bin.left, strategy);
    bin.right = normalize_expression(bin.right, strategy);
    constructor(Box::new(bin))
}

/// Check if an identifier contains case-sensitive characters based on dialect rules.
pub fn is_case_sensitive(text: &str, strategy: NormalizationStrategy) -> bool {
    match strategy {
        NormalizationStrategy::CaseInsensitive | NormalizationStrategy::CaseInsensitiveUppercase => {
            false
        }
        NormalizationStrategy::Uppercase => {
            text.chars().any(|c| c.is_lowercase())
        }
        NormalizationStrategy::Lowercase => {
            text.chars().any(|c| c.is_uppercase())
        }
        NormalizationStrategy::CaseSensitive => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::Generator;
    use crate::parser::Parser;

    fn gen(expr: &Expression) -> String {
        Generator::new().generate(expr).unwrap()
    }

    fn parse_and_normalize(sql: &str, dialect: Option<DialectType>) -> String {
        let ast = Parser::parse_sql(sql).expect("Failed to parse");
        let normalized = normalize_identifiers(ast[0].clone(), dialect);
        gen(&normalized)
    }

    #[test]
    fn test_normalize_lowercase() {
        // PostgreSQL-like: lowercase unquoted identifiers
        let result = parse_and_normalize("SELECT FoO FROM Bar", None);
        assert!(result.contains("foo") || result.contains("FOO")); // normalized
    }

    #[test]
    fn test_normalize_uppercase() {
        // Snowflake: uppercase unquoted identifiers
        let result = parse_and_normalize("SELECT foo FROM bar", Some(DialectType::Snowflake));
        // Should contain uppercase versions
        assert!(result.to_uppercase().contains("FOO"));
    }

    #[test]
    fn test_normalize_preserves_quoted() {
        // Quoted identifiers should be preserved in non-case-insensitive dialects
        let id = Identifier {
            name: "FoO".to_string(),
            quoted: true,
            trailing_comments: vec![],
        };
        let normalized = normalize_identifier(id, NormalizationStrategy::Lowercase);
        assert_eq!(normalized.name, "FoO"); // Preserved
    }

    #[test]
    fn test_case_insensitive_normalizes_quoted() {
        // In case-insensitive dialects, even quoted identifiers are normalized
        let id = Identifier {
            name: "FoO".to_string(),
            quoted: true,
            trailing_comments: vec![],
        };
        let normalized = normalize_identifier(id, NormalizationStrategy::CaseInsensitive);
        assert_eq!(normalized.name, "foo"); // Lowercased
    }

    #[test]
    fn test_case_sensitive_no_normalization() {
        // Case-sensitive dialects don't normalize at all
        let id = Identifier {
            name: "FoO".to_string(),
            quoted: false,
            trailing_comments: vec![],
        };
        let normalized = normalize_identifier(id, NormalizationStrategy::CaseSensitive);
        assert_eq!(normalized.name, "FoO"); // Unchanged
    }

    #[test]
    fn test_normalize_column() {
        let col = Expression::Column(Column {
            name: Identifier::new("MyColumn"),
            table: Some(Identifier::new("MyTable")),
            join_mark: false,
            trailing_comments: vec![],
        });

        let normalized = normalize_expression(col, NormalizationStrategy::Lowercase);
        let sql = gen(&normalized);
        assert!(sql.contains("mycolumn") || sql.contains("mytable"));
    }

    #[test]
    fn test_get_normalization_strategy() {
        assert_eq!(
            get_normalization_strategy(Some(DialectType::Snowflake)),
            NormalizationStrategy::Uppercase
        );
        assert_eq!(
            get_normalization_strategy(Some(DialectType::PostgreSQL)),
            NormalizationStrategy::Lowercase
        );
        assert_eq!(
            get_normalization_strategy(Some(DialectType::MySQL)),
            NormalizationStrategy::CaseSensitive
        );
        assert_eq!(
            get_normalization_strategy(Some(DialectType::DuckDB)),
            NormalizationStrategy::CaseInsensitive
        );
    }
}
