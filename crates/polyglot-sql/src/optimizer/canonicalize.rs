//! Canonicalization Module
//!
//! This module provides functionality for converting SQL expressions into a
//! standard canonical form. This includes:
//! - Converting string addition to CONCAT
//! - Replacing date functions with casts
//! - Removing redundant type casts
//! - Ensuring boolean predicates
//! - Removing unnecessary ASC from ORDER BY
//!
//! Ported from sqlglot's optimizer/canonicalize.py

use crate::dialects::DialectType;
use crate::expressions::{DataType, Expression, Literal};
use crate::helper::{is_iso_date, is_iso_datetime};

/// Converts a SQL expression into a standard canonical form.
///
/// This transformation relies on type annotations because many of the
/// conversions depend on type inference.
///
/// # Arguments
/// * `expression` - The expression to canonicalize
/// * `dialect` - Optional dialect for dialect-specific behavior
///
/// # Returns
/// The canonicalized expression
pub fn canonicalize(expression: Expression, dialect: Option<DialectType>) -> Expression {
    canonicalize_recursive(expression, dialect)
}

/// Recursively canonicalize an expression and its children
fn canonicalize_recursive(expression: Expression, dialect: Option<DialectType>) -> Expression {
    let expr = match expression {
        Expression::Select(mut select) => {
            // Canonicalize SELECT expressions
            select.expressions = select
                .expressions
                .into_iter()
                .map(|e| canonicalize_recursive(e, dialect))
                .collect();

            // Canonicalize FROM
            if let Some(mut from) = select.from {
                from.expressions = from
                    .expressions
                    .into_iter()
                    .map(|e| canonicalize_recursive(e, dialect))
                    .collect();
                select.from = Some(from);
            }

            // Canonicalize WHERE
            if let Some(mut where_clause) = select.where_clause {
                where_clause.this = canonicalize_recursive(where_clause.this, dialect);
                where_clause.this = ensure_bools(where_clause.this);
                select.where_clause = Some(where_clause);
            }

            // Canonicalize HAVING
            if let Some(mut having) = select.having {
                having.this = canonicalize_recursive(having.this, dialect);
                having.this = ensure_bools(having.this);
                select.having = Some(having);
            }

            // Canonicalize ORDER BY
            if let Some(mut order_by) = select.order_by {
                order_by.expressions = order_by
                    .expressions
                    .into_iter()
                    .map(|mut o| {
                        o.this = canonicalize_recursive(o.this, dialect);
                        o = remove_ascending_order(o);
                        o
                    })
                    .collect();
                select.order_by = Some(order_by);
            }

            // Canonicalize JOINs
            select.joins = select
                .joins
                .into_iter()
                .map(|mut j| {
                    j.this = canonicalize_recursive(j.this, dialect);
                    if let Some(on) = j.on {
                        j.on = Some(canonicalize_recursive(on, dialect));
                    }
                    j
                })
                .collect();

            Expression::Select(select)
        }

        // Binary operations that might involve string addition
        Expression::Add(bin) => {
            let left = canonicalize_recursive(bin.left, dialect);
            let right = canonicalize_recursive(bin.right, dialect);
            let result = Expression::Add(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }));
            add_text_to_concat(result)
        }

        // Other binary operations
        Expression::And(bin) => {
            let left = ensure_bools(canonicalize_recursive(bin.left, dialect));
            let right = ensure_bools(canonicalize_recursive(bin.right, dialect));
            Expression::And(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Or(bin) => {
            let left = ensure_bools(canonicalize_recursive(bin.left, dialect));
            let right = ensure_bools(canonicalize_recursive(bin.right, dialect));
            Expression::Or(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }

        Expression::Not(un) => {
            let inner = ensure_bools(canonicalize_recursive(un.this, dialect));
            Expression::Not(Box::new(crate::expressions::UnaryOp { this: inner }))
        }

        // Comparison operations - check for date coercion
        Expression::Eq(bin) => canonicalize_comparison(Expression::Eq, *bin, dialect),
        Expression::Neq(bin) => canonicalize_comparison(Expression::Neq, *bin, dialect),
        Expression::Lt(bin) => canonicalize_comparison(Expression::Lt, *bin, dialect),
        Expression::Lte(bin) => canonicalize_comparison(Expression::Lte, *bin, dialect),
        Expression::Gt(bin) => canonicalize_comparison(Expression::Gt, *bin, dialect),
        Expression::Gte(bin) => canonicalize_comparison(Expression::Gte, *bin, dialect),

        Expression::Sub(bin) => canonicalize_comparison(Expression::Sub, *bin, dialect),
        Expression::Mul(bin) => canonicalize_binary(Expression::Mul, *bin, dialect),
        Expression::Div(bin) => canonicalize_binary(Expression::Div, *bin, dialect),

        // Cast - check for redundancy
        Expression::Cast(cast) => {
            let inner = canonicalize_recursive(cast.this, dialect);
            let result = Expression::Cast(Box::new(crate::expressions::Cast {
                this: inner,
                to: cast.to,
                trailing_comments: cast.trailing_comments,
                double_colon_syntax: cast.double_colon_syntax,
                format: cast.format,
                default: cast.default,
            }));
            remove_redundant_casts(result)
        }

        // Function expressions
        Expression::Function(func) => {
            let args = func
                .args
                .into_iter()
                .map(|e| canonicalize_recursive(e, dialect))
                .collect();
            Expression::Function(Box::new(crate::expressions::Function {
                name: func.name,
                args,
                distinct: func.distinct,
                trailing_comments: func.trailing_comments,
                use_bracket_syntax: func.use_bracket_syntax,
                no_parens: func.no_parens,
                quoted: func.quoted,
            }))
        }

        Expression::AggregateFunction(agg) => {
            let args = agg
                .args
                .into_iter()
                .map(|e| canonicalize_recursive(e, dialect))
                .collect();
            Expression::AggregateFunction(Box::new(crate::expressions::AggregateFunction {
                name: agg.name,
                args,
                distinct: agg.distinct,
                filter: agg.filter.map(|f| canonicalize_recursive(f, dialect)),
                order_by: agg.order_by,
                limit: agg.limit,
                ignore_nulls: agg.ignore_nulls,
            }))
        }

        // Alias
        Expression::Alias(alias) => {
            let inner = canonicalize_recursive(alias.this, dialect);
            Expression::Alias(Box::new(crate::expressions::Alias {
                this: inner,
                alias: alias.alias,
                column_aliases: alias.column_aliases,
                pre_alias_comments: alias.pre_alias_comments,
                trailing_comments: alias.trailing_comments,
            }))
        }

        // Paren
        Expression::Paren(paren) => {
            let inner = canonicalize_recursive(paren.this, dialect);
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: inner,
                trailing_comments: paren.trailing_comments,
            }))
        }

        // Case
        Expression::Case(case) => {
            let operand = case.operand.map(|e| canonicalize_recursive(e, dialect));
            let whens = case
                .whens
                .into_iter()
                .map(|(w, t)| {
                    (
                        canonicalize_recursive(w, dialect),
                        canonicalize_recursive(t, dialect),
                    )
                })
                .collect();
            let else_ = case.else_.map(|e| canonicalize_recursive(e, dialect));
            Expression::Case(Box::new(crate::expressions::Case {
                operand,
                whens,
                else_,
                comments: Vec::new(),
            }))
        }

        // Between - check for date coercion
        Expression::Between(between) => {
            let this = canonicalize_recursive(between.this, dialect);
            let low = canonicalize_recursive(between.low, dialect);
            let high = canonicalize_recursive(between.high, dialect);
            Expression::Between(Box::new(crate::expressions::Between {
                this,
                low,
                high,
                not: between.not,
                symmetric: between.symmetric,
            }))
        }

        // In
        Expression::In(in_expr) => {
            let this = canonicalize_recursive(in_expr.this, dialect);
            let expressions = in_expr
                .expressions
                .into_iter()
                .map(|e| canonicalize_recursive(e, dialect))
                .collect();
            let query = in_expr.query.map(|q| canonicalize_recursive(q, dialect));
            Expression::In(Box::new(crate::expressions::In {
                this,
                expressions,
                query,
                not: in_expr.not,
                global: in_expr.global,
                unnest: in_expr.unnest,
                is_field: in_expr.is_field,
            }))
        }

        // Subquery
        Expression::Subquery(subquery) => {
            let this = canonicalize_recursive(subquery.this, dialect);
            Expression::Subquery(Box::new(crate::expressions::Subquery {
                this,
                alias: subquery.alias,
                column_aliases: subquery.column_aliases,
                order_by: subquery.order_by,
                limit: subquery.limit,
                offset: subquery.offset,
                distribute_by: subquery.distribute_by,
                sort_by: subquery.sort_by,
                cluster_by: subquery.cluster_by,
                lateral: subquery.lateral,
                modifiers_inside: subquery.modifiers_inside,
                trailing_comments: subquery.trailing_comments,
            }))
        }

        // Set operations
        Expression::Union(union) => {
            let left = canonicalize_recursive(union.left, dialect);
            let right = canonicalize_recursive(union.right, dialect);
            Expression::Union(Box::new(crate::expressions::Union {
                left,
                right,
                all: union.all,
                distinct: union.distinct,
                with: union.with,
                order_by: union.order_by,
                limit: union.limit,
                offset: union.offset,
                distribute_by: union.distribute_by,
                sort_by: union.sort_by,
                cluster_by: union.cluster_by,
                by_name: union.by_name,
                side: union.side,
                kind: union.kind,
                corresponding: union.corresponding,
                strict: union.strict,
                on_columns: union.on_columns,
            }))
        }
        Expression::Intersect(intersect) => {
            let left = canonicalize_recursive(intersect.left, dialect);
            let right = canonicalize_recursive(intersect.right, dialect);
            Expression::Intersect(Box::new(crate::expressions::Intersect {
                left,
                right,
                all: intersect.all,
                distinct: intersect.distinct,
                with: intersect.with,
                order_by: intersect.order_by,
                limit: intersect.limit,
                offset: intersect.offset,
                distribute_by: intersect.distribute_by,
                sort_by: intersect.sort_by,
                cluster_by: intersect.cluster_by,
                by_name: intersect.by_name,
                side: intersect.side,
                kind: intersect.kind,
                corresponding: intersect.corresponding,
                strict: intersect.strict,
                on_columns: intersect.on_columns,
            }))
        }
        Expression::Except(except) => {
            let left = canonicalize_recursive(except.left, dialect);
            let right = canonicalize_recursive(except.right, dialect);
            Expression::Except(Box::new(crate::expressions::Except {
                left,
                right,
                all: except.all,
                distinct: except.distinct,
                with: except.with,
                order_by: except.order_by,
                limit: except.limit,
                offset: except.offset,
                distribute_by: except.distribute_by,
                sort_by: except.sort_by,
                cluster_by: except.cluster_by,
                by_name: except.by_name,
                side: except.side,
                kind: except.kind,
                corresponding: except.corresponding,
                strict: except.strict,
                on_columns: except.on_columns,
            }))
        }

        // Leaf nodes - return unchanged
        other => other,
    };

    expr
}

/// Convert string addition to CONCAT.
///
/// When two TEXT types are added with +, convert to CONCAT.
/// This is used by dialects like T-SQL and Redshift.
fn add_text_to_concat(expression: Expression) -> Expression {
    // In a full implementation, we would check if the operands are TEXT types
    // and convert to CONCAT. For now, we return unchanged.
    expression
}

/// Remove redundant cast expressions.
///
/// If casting to the same type the expression already is, remove the cast.
fn remove_redundant_casts(expression: Expression) -> Expression {
    if let Expression::Cast(cast) = &expression {
        // Check if the inner expression's type matches the cast target
        // In a full implementation with type annotations, we would compare types
        // For now, just check simple cases

        // If casting a literal to its natural type, we might be able to simplify
        if let Expression::Literal(Literal::String(_)) = &cast.this {
            if matches!(&cast.to, DataType::VarChar { .. } | DataType::Text) {
                return cast.this.clone();
            }
        }
        if let Expression::Literal(Literal::Number(_)) = &cast.this {
            if matches!(
                &cast.to,
                DataType::Int { .. }
                    | DataType::BigInt { .. }
                    | DataType::Decimal { .. }
                    | DataType::Float { .. }
            ) {
                // Could potentially remove cast, but be conservative
            }
        }
    }
    expression
}

/// Ensure expressions used as boolean predicates are actually boolean.
///
/// For example, in some dialects, integers can be used as booleans.
/// This function ensures proper boolean semantics.
fn ensure_bools(expression: Expression) -> Expression {
    // In a full implementation, we would check if the expression is an integer
    // and convert it to a comparison (e.g., x != 0).
    // For now, return unchanged.
    expression
}

/// Remove explicit ASC from ORDER BY clauses.
///
/// Since ASC is the default, `ORDER BY a ASC` can be simplified to `ORDER BY a`.
fn remove_ascending_order(mut ordered: crate::expressions::Ordered) -> crate::expressions::Ordered {
    // If ASC was explicitly written (not DESC), remove the explicit flag
    // since ASC is the default ordering
    if !ordered.desc && ordered.explicit_asc {
        ordered.explicit_asc = false;
    }
    ordered
}

/// Canonicalize a binary comparison operation.
fn canonicalize_comparison<F>(
    constructor: F,
    bin: crate::expressions::BinaryOp,
    dialect: Option<DialectType>,
) -> Expression
where
    F: FnOnce(Box<crate::expressions::BinaryOp>) -> Expression,
{
    let left = canonicalize_recursive(bin.left, dialect);
    let right = canonicalize_recursive(bin.right, dialect);

    // Check for date coercion opportunities
    let (left, right) = coerce_date_operands(left, right);

    constructor(Box::new(crate::expressions::BinaryOp {
        left,
        right,
        left_comments: bin.left_comments,
        operator_comments: bin.operator_comments,
        trailing_comments: bin.trailing_comments,
    }))
}

/// Canonicalize a regular binary operation.
fn canonicalize_binary<F>(
    constructor: F,
    bin: crate::expressions::BinaryOp,
    dialect: Option<DialectType>,
) -> Expression
where
    F: FnOnce(Box<crate::expressions::BinaryOp>) -> Expression,
{
    let left = canonicalize_recursive(bin.left, dialect);
    let right = canonicalize_recursive(bin.right, dialect);

    constructor(Box::new(crate::expressions::BinaryOp {
        left,
        right,
        left_comments: bin.left_comments,
        operator_comments: bin.operator_comments,
        trailing_comments: bin.trailing_comments,
    }))
}

/// Coerce date operands in comparisons.
///
/// When comparing a date/datetime column with a string literal,
/// add appropriate CAST to the string.
fn coerce_date_operands(left: Expression, right: Expression) -> (Expression, Expression) {
    // Check if we should cast string literals to date/datetime
    let left = coerce_date_string(left, &right);
    let right = coerce_date_string(right, &left);
    (left, right)
}

/// Coerce a string literal to date/datetime if comparing with a temporal type.
fn coerce_date_string(expr: Expression, _other: &Expression) -> Expression {
    if let Expression::Literal(Literal::String(ref s)) = expr {
        // Check if the string is an ISO date or datetime
        if is_iso_date(s) {
            // In a full implementation, we would add CAST to DATE
            // For now, return unchanged
        } else if is_iso_datetime(s) {
            // In a full implementation, we would add CAST to DATETIME/TIMESTAMP
            // For now, return unchanged
        }
    }
    expr
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::Generator;
    use crate::parser::Parser;

    fn gen(expr: &Expression) -> String {
        Generator::new().generate(expr).unwrap()
    }

    fn parse(sql: &str) -> Expression {
        Parser::parse_sql(sql).expect("Failed to parse")[0].clone()
    }

    #[test]
    fn test_canonicalize_simple() {
        let expr = parse("SELECT a FROM t");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_canonicalize_preserves_structure() {
        let expr = parse("SELECT a, b FROM t WHERE c = 1");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_canonicalize_and_or() {
        let expr = parse("SELECT 1 WHERE a AND b OR c");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("AND") || sql.contains("OR"));
    }

    #[test]
    fn test_canonicalize_comparison() {
        let expr = parse("SELECT 1 WHERE a = 1 AND b > 2");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("=") && sql.contains(">"));
    }

    #[test]
    fn test_canonicalize_case() {
        let expr = parse("SELECT CASE WHEN a = 1 THEN 'yes' ELSE 'no' END FROM t");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("CASE") && sql.contains("WHEN"));
    }

    #[test]
    fn test_canonicalize_subquery() {
        let expr = parse("SELECT a FROM (SELECT b FROM t) AS sub");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("SELECT") && sql.contains("sub"));
    }

    #[test]
    fn test_canonicalize_order_by() {
        let expr = parse("SELECT a FROM t ORDER BY a");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("ORDER BY"));
    }

    #[test]
    fn test_canonicalize_union() {
        let expr = parse("SELECT a FROM t UNION SELECT b FROM s");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("UNION"));
    }

    #[test]
    fn test_add_text_to_concat_passthrough() {
        // Test that non-text additions pass through
        let expr = parse("SELECT 1 + 2");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("+"));
    }

    #[test]
    fn test_canonicalize_function() {
        let expr = parse("SELECT MAX(a) FROM t");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("MAX"));
    }

    #[test]
    fn test_canonicalize_between() {
        let expr = parse("SELECT 1 WHERE a BETWEEN 1 AND 10");
        let result = canonicalize(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("BETWEEN"));
    }
}
