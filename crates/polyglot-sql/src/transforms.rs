//! SQL AST Transforms
//!
//! This module provides functions to transform SQL ASTs for dialect compatibility.
//! These transforms are used during transpilation to convert dialect-specific features
//! to forms that are supported by the target dialect.
//!
//! Based on the Python implementation in `sqlglot/transforms.py`.

use crate::dialects::transform_recursive;
use crate::dialects::{Dialect, DialectType};
use crate::error::Result;
use crate::expressions::{
    Alias, BinaryOp, BooleanLiteral, Cast, DataType, Exists, Expression, From, Function,
    Identifier, Join, JoinKind, Lateral, LateralView, Literal, NamedArgSeparator, NamedArgument,
    Over, Select, StructField, Subquery, UnaryFunc, UnnestFunc, Where,
};
use std::cell::RefCell;

/// Apply a chain of transforms to an expression
///
/// # Arguments
/// * `expr` - The expression to transform
/// * `transforms` - A list of transform functions to apply in order
///
/// # Returns
/// The transformed expression
pub fn preprocess<F>(expr: Expression, transforms: &[F]) -> Result<Expression>
where
    F: Fn(Expression) -> Result<Expression>,
{
    let mut result = expr;
    for transform in transforms {
        result = transform(result)?;
    }
    Ok(result)
}

/// Convert UNNEST to EXPLODE (for Spark/Hive compatibility)
///
/// UNNEST is standard SQL but Spark uses EXPLODE instead.
pub fn unnest_to_explode(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Unnest(unnest) => {
            Ok(Expression::Explode(Box::new(UnaryFunc::new(unnest.this))))
        }
        _ => Ok(expr),
    }
}

/// Convert CROSS JOIN UNNEST to LATERAL VIEW EXPLODE/INLINE for Spark/Hive/Databricks.
///
/// This is a SELECT-level structural transformation that:
/// 1. Converts UNNEST in FROM clause to INLINE/EXPLODE
/// 2. Converts CROSS JOIN (LATERAL) UNNEST to LATERAL VIEW entries
/// 3. For single-arg UNNEST: uses EXPLODE
/// 4. For multi-arg UNNEST: uses INLINE(ARRAYS_ZIP(...))
///
/// Based on Python sqlglot's `unnest_to_explode` transform in transforms.py (lines 290-391).
pub fn unnest_to_explode_select(expr: Expression) -> Result<Expression> {
    transform_recursive(expr, &unnest_to_explode_select_inner)
}

/// Helper to determine the UDTF function for an UNNEST expression.
/// Single-arg UNNEST → EXPLODE, multi-arg → INLINE
fn make_udtf_expr(unnest: &UnnestFunc) -> Expression {
    let has_multi_expr = !unnest.expressions.is_empty();
    if has_multi_expr {
        // Multi-arg: INLINE(ARRAYS_ZIP(arg1, arg2, ...))
        let mut all_args = vec![unnest.this.clone()];
        all_args.extend(unnest.expressions.iter().cloned());
        let arrays_zip =
            Expression::Function(Box::new(Function::new("ARRAYS_ZIP".to_string(), all_args)));
        Expression::Function(Box::new(Function::new(
            "INLINE".to_string(),
            vec![arrays_zip],
        )))
    } else {
        // Single-arg: EXPLODE(arg)
        Expression::Explode(Box::new(UnaryFunc::new(unnest.this.clone())))
    }
}

fn unnest_to_explode_select_inner(expr: Expression) -> Result<Expression> {
    let Expression::Select(mut select) = expr else {
        return Ok(expr);
    };

    // Process FROM clause: UNNEST items need conversion
    if let Some(ref mut from) = select.from {
        if from.expressions.len() >= 1 {
            let mut new_from_exprs = Vec::new();
            let mut new_lateral_views = Vec::new();
            let first_is_unnest = is_unnest_expr(&from.expressions[0]);

            for (idx, from_item) in from.expressions.drain(..).enumerate() {
                if idx == 0 && first_is_unnest {
                    // UNNEST is the first (and possibly only) item in FROM
                    // Replace it with INLINE/EXPLODE, keeping alias
                    let replaced = replace_from_unnest(from_item);
                    new_from_exprs.push(replaced);
                } else if idx > 0 && is_unnest_expr(&from_item) {
                    // Additional UNNEST items in FROM (comma-joined) → LATERAL VIEW
                    let (alias_name, column_aliases, unnest_func) = extract_unnest_info(from_item);
                    let udtf = make_udtf_expr(&unnest_func);
                    new_lateral_views.push(LateralView {
                        this: udtf,
                        table_alias: alias_name,
                        column_aliases,
                        outer: false,
                    });
                } else {
                    new_from_exprs.push(from_item);
                }
            }

            from.expressions = new_from_exprs;
            // Append lateral views for comma-joined UNNESTs
            select.lateral_views.extend(new_lateral_views);
        }
    }

    // Process joins: CROSS JOIN (LATERAL) UNNEST → LATERAL VIEW
    let mut remaining_joins = Vec::new();
    for join in select.joins.drain(..) {
        if matches!(join.kind, JoinKind::Cross | JoinKind::Inner) {
            let (is_unnest, is_lateral) = check_join_unnest(&join.this);
            if is_unnest {
                // Extract UNNEST info from join, handling Lateral wrapper
                let (lateral_alias, lateral_col_aliases, join_expr) = if is_lateral {
                    if let Expression::Lateral(lat) = join.this {
                        // Extract alias from Lateral struct
                        let alias = lat.alias.map(|s| Identifier::new(&s));
                        let col_aliases: Vec<Identifier> = lat
                            .column_aliases
                            .iter()
                            .map(|s| Identifier::new(s))
                            .collect();
                        (alias, col_aliases, *lat.this)
                    } else {
                        (None, Vec::new(), join.this)
                    }
                } else {
                    (None, Vec::new(), join.this)
                };

                let (alias_name, column_aliases, unnest_func) = extract_unnest_info(join_expr);

                // Prefer Lateral's alias over UNNEST's alias
                let final_alias = lateral_alias.or(alias_name);
                let final_col_aliases = if !lateral_col_aliases.is_empty() {
                    lateral_col_aliases
                } else {
                    column_aliases
                };

                // Use "unnest" as default alias if none provided (for single-arg case)
                let table_alias = final_alias.or_else(|| Some(Identifier::new("unnest")));
                let col_aliases = if final_col_aliases.is_empty() {
                    vec![Identifier::new("unnest")]
                } else {
                    final_col_aliases
                };

                let udtf = make_udtf_expr(&unnest_func);
                select.lateral_views.push(LateralView {
                    this: udtf,
                    table_alias,
                    column_aliases: col_aliases,
                    outer: false,
                });
            } else {
                remaining_joins.push(join);
            }
        } else {
            remaining_joins.push(join);
        }
    }
    select.joins = remaining_joins;

    Ok(Expression::Select(select))
}

/// Check if an expression is or wraps an UNNEST
fn is_unnest_expr(expr: &Expression) -> bool {
    match expr {
        Expression::Unnest(_) => true,
        Expression::Alias(a) => matches!(a.this, Expression::Unnest(_)),
        _ => false,
    }
}

/// Check if a join's expression is an UNNEST (possibly wrapped in Lateral)
fn check_join_unnest(expr: &Expression) -> (bool, bool) {
    match expr {
        Expression::Unnest(_) => (true, false),
        Expression::Alias(a) => {
            if matches!(a.this, Expression::Unnest(_)) {
                (true, false)
            } else {
                (false, false)
            }
        }
        Expression::Lateral(lat) => match &*lat.this {
            Expression::Unnest(_) => (true, true),
            Expression::Alias(a) => {
                if matches!(a.this, Expression::Unnest(_)) {
                    (true, true)
                } else {
                    (false, true)
                }
            }
            _ => (false, true),
        },
        _ => (false, false),
    }
}

/// Replace an UNNEST in FROM with INLINE/EXPLODE, preserving alias structure
fn replace_from_unnest(from_item: Expression) -> Expression {
    match from_item {
        Expression::Alias(mut a) => {
            if let Expression::Unnest(unnest) = a.this {
                a.this = make_udtf_expr(&unnest);
            }
            Expression::Alias(a)
        }
        Expression::Unnest(unnest) => make_udtf_expr(&unnest),
        other => other,
    }
}

/// Extract alias info and UnnestFunc from an expression (possibly wrapped in Alias)
fn extract_unnest_info(expr: Expression) -> (Option<Identifier>, Vec<Identifier>, UnnestFunc) {
    match expr {
        Expression::Alias(a) => {
            if let Expression::Unnest(unnest) = a.this {
                (Some(a.alias), a.column_aliases, *unnest)
            } else {
                // Should not happen if we already checked is_unnest_expr
                (
                    Some(a.alias),
                    a.column_aliases,
                    UnnestFunc {
                        this: a.this,
                        expressions: Vec::new(),
                        with_ordinality: false,
                        alias: None,
                        offset_alias: None,
                    },
                )
            }
        }
        Expression::Unnest(unnest) => {
            let alias = unnest.alias.clone();
            (alias, Vec::new(), *unnest)
        }
        _ => (
            None,
            Vec::new(),
            UnnestFunc {
                this: expr,
                expressions: Vec::new(),
                with_ordinality: false,
                alias: None,
                offset_alias: None,
            },
        ),
    }
}

/// Convert EXPLODE to UNNEST (for standard SQL compatibility)
pub fn explode_to_unnest(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Explode(explode) => Ok(Expression::Unnest(Box::new(UnnestFunc {
            this: explode.this,
            expressions: Vec::new(),
            with_ordinality: false,
            alias: None,
            offset_alias: None,
        }))),
        _ => Ok(expr),
    }
}

/// Replace boolean literals for dialects that don't support them
///
/// Converts TRUE/FALSE to 1/0 for dialects like older MySQL versions
pub fn replace_bool_with_int(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Boolean(b) => {
            let value = if b.value { "1" } else { "0" };
            Ok(Expression::Literal(Literal::Number(value.to_string())))
        }
        _ => Ok(expr),
    }
}

/// Replace integer literals for dialects that prefer boolean
///
/// Converts 1/0 to TRUE/FALSE
pub fn replace_int_with_bool(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Literal(Literal::Number(n)) if n == "1" || n == "0" => {
            Ok(Expression::Boolean(BooleanLiteral { value: n == "1" }))
        }
        _ => Ok(expr),
    }
}

/// Remove precision from parameterized types
///
/// Some dialects don't support precision parameters on certain types.
/// This transform removes them, e.g., VARCHAR(255) → VARCHAR, DECIMAL(10,2) → DECIMAL
pub fn remove_precision_parameterized_types(expr: Expression) -> Result<Expression> {
    Ok(strip_type_params_recursive(expr))
}

/// Recursively strip type parameters from DataType values in an expression
fn strip_type_params_recursive(expr: Expression) -> Expression {
    match expr {
        // Handle Cast expressions - strip precision from target type
        Expression::Cast(mut cast) => {
            cast.to = strip_data_type_params(cast.to);
            // Also recursively process the expression being cast
            cast.this = strip_type_params_recursive(cast.this);
            Expression::Cast(cast)
        }
        // Handle TryCast expressions (uses same Cast struct)
        Expression::TryCast(mut try_cast) => {
            try_cast.to = strip_data_type_params(try_cast.to);
            try_cast.this = strip_type_params_recursive(try_cast.this);
            Expression::TryCast(try_cast)
        }
        // Handle SafeCast expressions (uses same Cast struct)
        Expression::SafeCast(mut safe_cast) => {
            safe_cast.to = strip_data_type_params(safe_cast.to);
            safe_cast.this = strip_type_params_recursive(safe_cast.this);
            Expression::SafeCast(safe_cast)
        }
        // For now, pass through other expressions
        // A full implementation would recursively visit all nodes
        _ => expr,
    }
}

/// Strip precision/scale/length parameters from a DataType
fn strip_data_type_params(dt: DataType) -> DataType {
    match dt {
        // Numeric types with precision/scale
        DataType::Decimal { .. } => DataType::Decimal {
            precision: None,
            scale: None,
        },
        DataType::TinyInt { .. } => DataType::TinyInt { length: None },
        DataType::SmallInt { .. } => DataType::SmallInt { length: None },
        DataType::Int { .. } => DataType::Int {
            length: None,
            integer_spelling: false,
        },
        DataType::BigInt { .. } => DataType::BigInt { length: None },

        // String types with length
        DataType::Char { .. } => DataType::Char { length: None },
        DataType::VarChar { .. } => DataType::VarChar {
            length: None,
            parenthesized_length: false,
        },

        // Binary types with length
        DataType::Binary { .. } => DataType::Binary { length: None },
        DataType::VarBinary { .. } => DataType::VarBinary { length: None },

        // Bit types with length
        DataType::Bit { .. } => DataType::Bit { length: None },
        DataType::VarBit { .. } => DataType::VarBit { length: None },

        // Time types with precision
        DataType::Time { .. } => DataType::Time {
            precision: None,
            timezone: false,
        },
        DataType::Timestamp { timezone, .. } => DataType::Timestamp {
            precision: None,
            timezone,
        },

        // Array - recursively strip element type
        DataType::Array {
            element_type,
            dimension,
        } => DataType::Array {
            element_type: Box::new(strip_data_type_params(*element_type)),
            dimension,
        },

        // Map - recursively strip key and value types
        DataType::Map {
            key_type,
            value_type,
        } => DataType::Map {
            key_type: Box::new(strip_data_type_params(*key_type)),
            value_type: Box::new(strip_data_type_params(*value_type)),
        },

        // Struct - recursively strip field types
        DataType::Struct { fields, nested } => DataType::Struct {
            fields: fields
                .into_iter()
                .map(|f| {
                    StructField::with_options(
                        f.name,
                        strip_data_type_params(f.data_type),
                        f.options,
                    )
                })
                .collect(),
            nested,
        },

        // Vector - strip dimension
        DataType::Vector { element_type, .. } => DataType::Vector {
            element_type: element_type.map(|et| Box::new(strip_data_type_params(*et))),
            dimension: None,
        },

        // Object - recursively strip field types
        DataType::Object { fields, modifier } => DataType::Object {
            fields: fields
                .into_iter()
                .map(|(name, ty, not_null)| (name, strip_data_type_params(ty), not_null))
                .collect(),
            modifier,
        },

        // Other types pass through unchanged
        other => other,
    }
}

/// Eliminate QUALIFY clause by converting to a subquery with WHERE filter
///
/// QUALIFY is supported by Snowflake, BigQuery, and DuckDB but not by most other dialects.
///
/// Converts:
/// ```sql
/// SELECT * FROM t QUALIFY ROW_NUMBER() OVER (...) = 1
/// ```
/// To:
/// ```sql
/// SELECT * FROM (SELECT *, ROW_NUMBER() OVER (...) AS _w FROM t) _t WHERE _w = 1
/// ```
///
/// Reference: `transforms.py:194-255`
pub fn eliminate_qualify(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            if let Some(qualify) = select.qualify.take() {
                // Python sqlglot approach:
                // 1. Extract the window function from the qualify condition
                // 2. Add it as _w alias to the inner select
                // 3. Replace the window function reference with _w in the outer WHERE
                // 4. Keep original select expressions in the outer query

                let qualify_filter = qualify.this;
                let window_alias_name = "_w".to_string();
                let window_alias_ident = Identifier::new(window_alias_name.clone());

                // Try to extract window function from comparison
                // Pattern: WINDOW_FUNC = value -> inner adds WINDOW_FUNC AS _w, outer WHERE _w = value
                let (window_expr, outer_where) =
                    extract_window_from_condition(qualify_filter.clone(), &window_alias_ident);

                if let Some(win_expr) = window_expr {
                    // Add window function as _w alias to inner select
                    let window_alias_expr =
                        Expression::Alias(Box::new(crate::expressions::Alias {
                            this: win_expr,
                            alias: window_alias_ident.clone(),
                            column_aliases: vec![],
                            pre_alias_comments: vec![],
                            trailing_comments: vec![],
                        }));

                    // For the outer SELECT, replace aliased expressions with just the alias reference
                    // e.g., `1 AS other_id` in inner -> `other_id` in outer
                    // Non-aliased expressions (columns, identifiers) stay as-is
                    let outer_exprs: Vec<Expression> = select
                        .expressions
                        .iter()
                        .map(|expr| {
                            if let Expression::Alias(a) = expr {
                                // Replace with just the alias identifier as a column reference
                                Expression::Column(crate::expressions::Column {
                                    name: a.alias.clone(),
                                    table: None,
                                    join_mark: false,
                                    trailing_comments: vec![],
                                    span: None,
                                })
                            } else {
                                expr.clone()
                            }
                        })
                        .collect();
                    select.expressions.push(window_alias_expr);

                    // Create the inner subquery
                    let inner_select = Expression::Select(select);
                    let subquery = Subquery {
                        this: inner_select,
                        alias: Some(Identifier::new("_t".to_string())),
                        column_aliases: vec![],
                        order_by: None,
                        limit: None,
                        offset: None,
                        distribute_by: None,
                        sort_by: None,
                        cluster_by: None,
                        lateral: false,
                        modifiers_inside: false,
                        trailing_comments: vec![],
                    };

                    // Create the outer SELECT with alias-resolved expressions and WHERE _w <op> value
                    let outer_select = Select {
                        expressions: outer_exprs,
                        from: Some(From {
                            expressions: vec![Expression::Subquery(Box::new(subquery))],
                        }),
                        where_clause: Some(Where { this: outer_where }),
                        ..Select::new()
                    };

                    return Ok(Expression::Select(Box::new(outer_select)));
                } else {
                    // Fallback: if we can't extract a window function, use old approach
                    let qualify_alias = Expression::Alias(Box::new(crate::expressions::Alias {
                        this: qualify_filter.clone(),
                        alias: window_alias_ident.clone(),
                        column_aliases: vec![],
                        pre_alias_comments: vec![],
                        trailing_comments: vec![],
                    }));

                    let original_exprs = select.expressions.clone();
                    select.expressions.push(qualify_alias);

                    let inner_select = Expression::Select(select);
                    let subquery = Subquery {
                        this: inner_select,
                        alias: Some(Identifier::new("_t".to_string())),
                        column_aliases: vec![],
                        order_by: None,
                        limit: None,
                        offset: None,
                        distribute_by: None,
                        sort_by: None,
                        cluster_by: None,
                        lateral: false,
                        modifiers_inside: false,
                        trailing_comments: vec![],
                    };

                    let outer_select = Select {
                        expressions: original_exprs,
                        from: Some(From {
                            expressions: vec![Expression::Subquery(Box::new(subquery))],
                        }),
                        where_clause: Some(Where {
                            this: Expression::Column(crate::expressions::Column {
                                name: window_alias_ident,
                                table: None,
                                join_mark: false,
                                trailing_comments: vec![],
                                span: None,
                            }),
                        }),
                        ..Select::new()
                    };

                    return Ok(Expression::Select(Box::new(outer_select)));
                }
            }
            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Extract a window function from a qualify condition.
/// Returns (window_expression, rewritten_condition) if found.
/// The rewritten condition replaces the window function with a column reference to the alias.
fn extract_window_from_condition(
    condition: Expression,
    alias: &Identifier,
) -> (Option<Expression>, Expression) {
    let alias_col = Expression::Column(crate::expressions::Column {
        name: alias.clone(),
        table: None,
        join_mark: false,
        trailing_comments: vec![],
        span: None,
    });

    // Check if condition is a simple comparison with a window function on one side
    match condition {
        // WINDOW_FUNC = value
        Expression::Eq(ref op) => {
            if is_window_expr(&op.left) {
                (
                    Some(op.left.clone()),
                    Expression::Eq(Box::new(BinaryOp {
                        left: alias_col,
                        right: op.right.clone(),
                        ..(**op).clone()
                    })),
                )
            } else if is_window_expr(&op.right) {
                (
                    Some(op.right.clone()),
                    Expression::Eq(Box::new(BinaryOp {
                        left: op.left.clone(),
                        right: alias_col,
                        ..(**op).clone()
                    })),
                )
            } else {
                (None, condition)
            }
        }
        Expression::Neq(ref op) => {
            if is_window_expr(&op.left) {
                (
                    Some(op.left.clone()),
                    Expression::Neq(Box::new(BinaryOp {
                        left: alias_col,
                        right: op.right.clone(),
                        ..(**op).clone()
                    })),
                )
            } else if is_window_expr(&op.right) {
                (
                    Some(op.right.clone()),
                    Expression::Neq(Box::new(BinaryOp {
                        left: op.left.clone(),
                        right: alias_col,
                        ..(**op).clone()
                    })),
                )
            } else {
                (None, condition)
            }
        }
        Expression::Lt(ref op) => {
            if is_window_expr(&op.left) {
                (
                    Some(op.left.clone()),
                    Expression::Lt(Box::new(BinaryOp {
                        left: alias_col,
                        right: op.right.clone(),
                        ..(**op).clone()
                    })),
                )
            } else if is_window_expr(&op.right) {
                (
                    Some(op.right.clone()),
                    Expression::Lt(Box::new(BinaryOp {
                        left: op.left.clone(),
                        right: alias_col,
                        ..(**op).clone()
                    })),
                )
            } else {
                (None, condition)
            }
        }
        Expression::Lte(ref op) => {
            if is_window_expr(&op.left) {
                (
                    Some(op.left.clone()),
                    Expression::Lte(Box::new(BinaryOp {
                        left: alias_col,
                        right: op.right.clone(),
                        ..(**op).clone()
                    })),
                )
            } else if is_window_expr(&op.right) {
                (
                    Some(op.right.clone()),
                    Expression::Lte(Box::new(BinaryOp {
                        left: op.left.clone(),
                        right: alias_col,
                        ..(**op).clone()
                    })),
                )
            } else {
                (None, condition)
            }
        }
        Expression::Gt(ref op) => {
            if is_window_expr(&op.left) {
                (
                    Some(op.left.clone()),
                    Expression::Gt(Box::new(BinaryOp {
                        left: alias_col,
                        right: op.right.clone(),
                        ..(**op).clone()
                    })),
                )
            } else if is_window_expr(&op.right) {
                (
                    Some(op.right.clone()),
                    Expression::Gt(Box::new(BinaryOp {
                        left: op.left.clone(),
                        right: alias_col,
                        ..(**op).clone()
                    })),
                )
            } else {
                (None, condition)
            }
        }
        Expression::Gte(ref op) => {
            if is_window_expr(&op.left) {
                (
                    Some(op.left.clone()),
                    Expression::Gte(Box::new(BinaryOp {
                        left: alias_col,
                        right: op.right.clone(),
                        ..(**op).clone()
                    })),
                )
            } else if is_window_expr(&op.right) {
                (
                    Some(op.right.clone()),
                    Expression::Gte(Box::new(BinaryOp {
                        left: op.left.clone(),
                        right: alias_col,
                        ..(**op).clone()
                    })),
                )
            } else {
                (None, condition)
            }
        }
        // If the condition is just a window function (bare QUALIFY expression)
        _ if is_window_expr(&condition) => (Some(condition), alias_col),
        // Can't extract window function
        _ => (None, condition),
    }
}

/// Check if an expression is a window function
fn is_window_expr(expr: &Expression) -> bool {
    matches!(expr, Expression::Window(_) | Expression::WindowFunction(_))
}

/// Eliminate DISTINCT ON clause by converting to a subquery with ROW_NUMBER
///
/// DISTINCT ON is PostgreSQL-specific. For dialects that don't support it,
/// this converts it to a subquery with a ROW_NUMBER() window function.
///
/// Converts:
/// ```sql
/// SELECT DISTINCT ON (a) a, b FROM t ORDER BY a, b
/// ```
/// To:
/// ```sql
/// SELECT a, b FROM (
///     SELECT a, b, ROW_NUMBER() OVER (PARTITION BY a ORDER BY a, b) AS _row_number
///     FROM t
/// ) _t WHERE _row_number = 1
/// ```
///
/// Reference: `transforms.py:138-191`
pub fn eliminate_distinct_on(expr: Expression) -> Result<Expression> {
    eliminate_distinct_on_for_dialect(expr, None)
}

/// Eliminate DISTINCT ON with dialect-specific NULL ordering behavior.
///
/// For dialects where NULLs don't sort first by default in DESC ordering,
/// we need to add explicit NULL ordering to preserve DISTINCT ON semantics.
pub fn eliminate_distinct_on_for_dialect(
    expr: Expression,
    target: Option<DialectType>,
) -> Result<Expression> {
    use crate::expressions::Case;

    // PostgreSQL and DuckDB support DISTINCT ON natively - skip elimination
    if matches!(
        target,
        Some(DialectType::PostgreSQL) | Some(DialectType::DuckDB)
    ) {
        return Ok(expr);
    }

    // Determine NULL ordering mode based on target dialect
    // Oracle/Redshift/Snowflake: NULLS FIRST is default for DESC -> no change needed
    // BigQuery/Spark/Presto/Hive/etc: need explicit NULLS FIRST
    // MySQL/StarRocks/TSQL: no NULLS FIRST syntax -> use CASE WHEN IS NULL
    enum NullsMode {
        None,       // Default NULLS FIRST behavior (Oracle, Redshift, Snowflake)
        NullsFirst, // Add explicit NULLS FIRST (BigQuery, Spark, Presto, Hive, etc.)
        CaseExpr,   // Use CASE WHEN IS NULL for NULLS FIRST simulation (MySQL, StarRocks, TSQL)
    }

    let nulls_mode = match target {
        Some(DialectType::MySQL)
        | Some(DialectType::StarRocks)
        | Some(DialectType::SingleStore)
        | Some(DialectType::TSQL)
        | Some(DialectType::Fabric) => NullsMode::CaseExpr,
        Some(DialectType::Oracle) | Some(DialectType::Redshift) | Some(DialectType::Snowflake) => {
            NullsMode::None
        }
        // All other dialects that don't support DISTINCT ON: use NULLS FIRST
        _ => NullsMode::NullsFirst,
    };

    match expr {
        Expression::Select(mut select) => {
            if let Some(distinct_cols) = select.distinct_on.take() {
                if !distinct_cols.is_empty() {
                    // Create ROW_NUMBER() OVER (PARTITION BY distinct_cols ORDER BY ...)
                    let row_number_alias = Identifier::new("_row_number".to_string());

                    // Get order_by expressions, or use distinct_cols as default order
                    let order_exprs = if let Some(ref order_by) = select.order_by {
                        let mut exprs = order_by.expressions.clone();
                        // Add NULL ordering based on target dialect
                        match nulls_mode {
                            NullsMode::NullsFirst => {
                                for ord in &mut exprs {
                                    if ord.desc && ord.nulls_first.is_none() {
                                        ord.nulls_first = Some(true);
                                    }
                                }
                            }
                            NullsMode::CaseExpr => {
                                // For each DESC column without explicit nulls ordering,
                                // prepend: CASE WHEN col IS NULL THEN 1 ELSE 0 END DESC
                                let mut new_exprs = Vec::new();
                                for ord in &exprs {
                                    if ord.desc && ord.nulls_first.is_none() {
                                        // Add CASE WHEN col IS NULL THEN 1 ELSE 0 END DESC
                                        let null_check = Expression::Case(Box::new(Case {
                                            operand: None,
                                            whens: vec![(
                                                Expression::IsNull(Box::new(
                                                    crate::expressions::IsNull {
                                                        this: ord.this.clone(),
                                                        not: false,
                                                        postfix_form: false,
                                                    },
                                                )),
                                                Expression::Literal(Literal::Number(
                                                    "1".to_string(),
                                                )),
                                            )],
                                            else_: Some(Expression::Literal(Literal::Number(
                                                "0".to_string(),
                                            ))),
                                            comments: Vec::new(),
                                        }));
                                        new_exprs.push(crate::expressions::Ordered {
                                            this: null_check,
                                            desc: true,
                                            nulls_first: None,
                                            explicit_asc: false,
                                            with_fill: None,
                                        });
                                    }
                                    new_exprs.push(ord.clone());
                                }
                                exprs = new_exprs;
                            }
                            NullsMode::None => {}
                        }
                        exprs
                    } else {
                        distinct_cols
                            .iter()
                            .map(|e| crate::expressions::Ordered {
                                this: e.clone(),
                                desc: false,
                                nulls_first: None,
                                explicit_asc: false,
                                with_fill: None,
                            })
                            .collect()
                    };

                    // Create window function: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
                    let row_number_func =
                        Expression::WindowFunction(Box::new(crate::expressions::WindowFunction {
                            this: Expression::RowNumber(crate::expressions::RowNumber),
                            over: Over {
                                partition_by: distinct_cols,
                                order_by: order_exprs,
                                frame: None,
                                window_name: None,
                                alias: None,
                            },
                            keep: None,
                        }));

                    // Build aliased inner expressions and outer column references
                    // Inner: SELECT a AS a, b AS b, ROW_NUMBER() OVER (...) AS _row_number
                    // Outer: SELECT a, b FROM (...)
                    let mut inner_aliased_exprs = Vec::new();
                    let mut outer_select_exprs = Vec::new();
                    for orig_expr in &select.expressions {
                        match orig_expr {
                            Expression::Alias(alias) => {
                                // Already aliased - keep as-is in inner, reference alias in outer
                                inner_aliased_exprs.push(orig_expr.clone());
                                outer_select_exprs.push(Expression::Column(
                                    crate::expressions::Column {
                                        name: alias.alias.clone(),
                                        table: None,
                                        join_mark: false,
                                        trailing_comments: vec![],
                                        span: None,
                                    },
                                ));
                            }
                            Expression::Column(col) => {
                                // Wrap in alias: a AS a in inner, just a in outer
                                inner_aliased_exprs.push(Expression::Alias(Box::new(
                                    crate::expressions::Alias {
                                        this: orig_expr.clone(),
                                        alias: col.name.clone(),
                                        column_aliases: vec![],
                                        pre_alias_comments: vec![],
                                        trailing_comments: vec![],
                                    },
                                )));
                                outer_select_exprs.push(Expression::Column(
                                    crate::expressions::Column {
                                        name: col.name.clone(),
                                        table: None,
                                        join_mark: false,
                                        trailing_comments: vec![],
                                        span: None,
                                    },
                                ));
                            }
                            _ => {
                                // Complex expression without alias - include as-is in both
                                inner_aliased_exprs.push(orig_expr.clone());
                                outer_select_exprs.push(orig_expr.clone());
                            }
                        }
                    }

                    // Add ROW_NUMBER as aliased expression to inner select list
                    let row_number_alias_expr =
                        Expression::Alias(Box::new(crate::expressions::Alias {
                            this: row_number_func,
                            alias: row_number_alias.clone(),
                            column_aliases: vec![],
                            pre_alias_comments: vec![],
                            trailing_comments: vec![],
                        }));
                    inner_aliased_exprs.push(row_number_alias_expr);

                    // Replace inner select's expressions with aliased versions
                    select.expressions = inner_aliased_exprs;

                    // Remove ORDER BY from inner query (it's now in the window function)
                    let _inner_order_by = select.order_by.take();

                    // Clear DISTINCT from inner select (DISTINCT ON is replaced by ROW_NUMBER)
                    select.distinct = false;

                    // Create inner subquery
                    let inner_select = Expression::Select(select);
                    let subquery = Subquery {
                        this: inner_select,
                        alias: Some(Identifier::new("_t".to_string())),
                        column_aliases: vec![],
                        order_by: None,
                        limit: None,
                        offset: None,
                        distribute_by: None,
                        sort_by: None,
                        cluster_by: None,
                        lateral: false,
                        modifiers_inside: false,
                        trailing_comments: vec![],
                    };

                    // Create outer SELECT with WHERE _row_number = 1
                    // No ORDER BY on outer query
                    let outer_select = Select {
                        expressions: outer_select_exprs,
                        from: Some(From {
                            expressions: vec![Expression::Subquery(Box::new(subquery))],
                        }),
                        where_clause: Some(Where {
                            this: Expression::Eq(Box::new(BinaryOp {
                                left: Expression::Column(crate::expressions::Column {
                                    name: row_number_alias,
                                    table: None,
                                    join_mark: false,
                                    trailing_comments: vec![],
                                    span: None,
                                }),
                                right: Expression::Literal(Literal::Number("1".to_string())),
                                left_comments: vec![],
                                operator_comments: vec![],
                                trailing_comments: vec![],
                            })),
                        }),
                        ..Select::new()
                    };

                    return Ok(Expression::Select(Box::new(outer_select)));
                }
            }
            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Convert SEMI and ANTI joins into equivalent forms that use EXISTS instead.
///
/// For dialects that don't support SEMI/ANTI join syntax, this converts:
/// - `SELECT * FROM a SEMI JOIN b ON a.x = b.x` → `SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE a.x = b.x)`
/// - `SELECT * FROM a ANTI JOIN b ON a.x = b.x` → `SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE a.x = b.x)`
///
/// Reference: `transforms.py:607-621`
pub fn eliminate_semi_and_anti_joins(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            let mut new_joins = Vec::new();
            let mut extra_where_conditions = Vec::new();

            for join in select.joins.drain(..) {
                match join.kind {
                    JoinKind::Semi | JoinKind::LeftSemi => {
                        if let Some(on_condition) = join.on {
                            // Create: EXISTS (SELECT 1 FROM join_table WHERE on_condition)
                            let subquery_select = Select {
                                expressions: vec![Expression::Literal(Literal::Number(
                                    "1".to_string(),
                                ))],
                                from: Some(From {
                                    expressions: vec![join.this],
                                }),
                                where_clause: Some(Where { this: on_condition }),
                                ..Select::new()
                            };

                            let exists = Expression::Exists(Box::new(Exists {
                                this: Expression::Subquery(Box::new(Subquery {
                                    this: Expression::Select(Box::new(subquery_select)),
                                    alias: None,
                                    column_aliases: vec![],
                                    order_by: None,
                                    limit: None,
                                    offset: None,
                                    distribute_by: None,
                                    sort_by: None,
                                    cluster_by: None,
                                    lateral: false,
                                    modifiers_inside: false,
                                    trailing_comments: vec![],
                                })),
                                not: false,
                            }));

                            extra_where_conditions.push(exists);
                        }
                    }
                    JoinKind::Anti | JoinKind::LeftAnti => {
                        if let Some(on_condition) = join.on {
                            // Create: NOT EXISTS (SELECT 1 FROM join_table WHERE on_condition)
                            let subquery_select = Select {
                                expressions: vec![Expression::Literal(Literal::Number(
                                    "1".to_string(),
                                ))],
                                from: Some(From {
                                    expressions: vec![join.this],
                                }),
                                where_clause: Some(Where { this: on_condition }),
                                ..Select::new()
                            };

                            // Use Exists with not: true for NOT EXISTS
                            let not_exists = Expression::Exists(Box::new(Exists {
                                this: Expression::Subquery(Box::new(Subquery {
                                    this: Expression::Select(Box::new(subquery_select)),
                                    alias: None,
                                    column_aliases: vec![],
                                    order_by: None,
                                    limit: None,
                                    offset: None,
                                    distribute_by: None,
                                    sort_by: None,
                                    cluster_by: None,
                                    lateral: false,
                                    modifiers_inside: false,
                                    trailing_comments: vec![],
                                })),
                                not: true,
                            }));

                            extra_where_conditions.push(not_exists);
                        }
                    }
                    _ => {
                        // Keep other join types as-is
                        new_joins.push(join);
                    }
                }
            }

            select.joins = new_joins;

            // Add EXISTS conditions to WHERE clause
            if !extra_where_conditions.is_empty() {
                let combined = extra_where_conditions
                    .into_iter()
                    .reduce(|acc, cond| {
                        Expression::And(Box::new(BinaryOp {
                            left: acc,
                            right: cond,
                            left_comments: vec![],
                            operator_comments: vec![],
                            trailing_comments: vec![],
                        }))
                    })
                    .unwrap();

                select.where_clause = match select.where_clause {
                    Some(Where { this: existing }) => Some(Where {
                        this: Expression::And(Box::new(BinaryOp {
                            left: existing,
                            right: combined,
                            left_comments: vec![],
                            operator_comments: vec![],
                            trailing_comments: vec![],
                        })),
                    }),
                    None => Some(Where { this: combined }),
                };
            }

            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Convert FULL OUTER JOIN to a UNION of LEFT and RIGHT OUTER joins.
///
/// For dialects that don't support FULL OUTER JOIN, this converts:
/// ```sql
/// SELECT * FROM a FULL OUTER JOIN b ON a.x = b.x
/// ```
/// To:
/// ```sql
/// SELECT * FROM a LEFT OUTER JOIN b ON a.x = b.x
/// UNION ALL
/// SELECT * FROM a RIGHT OUTER JOIN b ON a.x = b.x
/// WHERE NOT EXISTS (SELECT 1 FROM a WHERE a.x = b.x)
/// ```
///
/// Note: This transformation currently only works for queries with a single FULL OUTER join.
///
/// Reference: `transforms.py:624-661`
pub fn eliminate_full_outer_join(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            // Find FULL OUTER joins
            let full_outer_join_idx = select.joins.iter().position(|j| j.kind == JoinKind::Full);

            if let Some(idx) = full_outer_join_idx {
                // We only handle queries with a single FULL OUTER join
                let full_join_count = select
                    .joins
                    .iter()
                    .filter(|j| j.kind == JoinKind::Full)
                    .count();
                if full_join_count != 1 {
                    return Ok(Expression::Select(select));
                }

                // Clone the query for the right side of the UNION
                let mut right_select = select.clone();

                // Get the join condition from the FULL OUTER join
                let full_join = &select.joins[idx];
                let join_condition = full_join.on.clone();

                // Left side: convert FULL to LEFT
                select.joins[idx].kind = JoinKind::Left;

                // Right side: convert FULL to RIGHT and add NOT EXISTS condition
                right_select.joins[idx].kind = JoinKind::Right;

                // Build NOT EXISTS for the right side to exclude rows that matched
                if let (Some(ref from), Some(ref join_cond)) = (&select.from, &join_condition) {
                    if !from.expressions.is_empty() {
                        let anti_subquery = Expression::Select(Box::new(Select {
                            expressions: vec![Expression::Literal(Literal::Number(
                                "1".to_string(),
                            ))],
                            from: Some(from.clone()),
                            where_clause: Some(Where {
                                this: join_cond.clone(),
                            }),
                            ..Select::new()
                        }));

                        let not_exists = Expression::Not(Box::new(crate::expressions::UnaryOp {
                            this: Expression::Exists(Box::new(Exists {
                                this: Expression::Subquery(Box::new(Subquery {
                                    this: anti_subquery,
                                    alias: None,
                                    column_aliases: vec![],
                                    order_by: None,
                                    limit: None,
                                    offset: None,
                                    distribute_by: None,
                                    sort_by: None,
                                    cluster_by: None,
                                    lateral: false,
                                    modifiers_inside: false,
                                    trailing_comments: vec![],
                                })),
                                not: false,
                            })),
                        }));

                        // Add NOT EXISTS to the WHERE clause
                        right_select.where_clause = Some(Where {
                            this: match right_select.where_clause {
                                Some(w) => Expression::And(Box::new(BinaryOp {
                                    left: w.this,
                                    right: not_exists,
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                })),
                                None => not_exists,
                            },
                        });
                    }
                }

                // Remove WITH clause from right side (CTEs should only be on left)
                right_select.with = None;

                // Remove ORDER BY from left side (will be applied after UNION)
                let order_by = select.order_by.take();

                // Create UNION ALL of left and right
                let union = crate::expressions::Union {
                    left: Expression::Select(select),
                    right: Expression::Select(right_select),
                    all: true, // UNION ALL
                    distinct: false,
                    with: None,
                    order_by,
                    limit: None,
                    offset: None,
                    distribute_by: None,
                    sort_by: None,
                    cluster_by: None,
                    by_name: false,
                    side: None,
                    kind: None,
                    corresponding: false,
                    strict: false,
                    on_columns: Vec::new(),
                };

                return Ok(Expression::Union(Box::new(union)));
            }

            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Move CTEs to the top level of the query.
///
/// Some dialects (e.g., Hive, T-SQL, Spark prior to version 3) only allow CTEs to be
/// defined at the top-level, so for example queries like:
///
/// ```sql
/// SELECT * FROM (WITH t(c) AS (SELECT 1) SELECT * FROM t) AS subq
/// ```
///
/// are invalid in those dialects. This transformation moves all CTEs to the top level.
///
/// Reference: `transforms.py:664-700`
pub fn move_ctes_to_top_level(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            // Phase 1: Collect CTEs from nested subqueries (not inside CTE definitions)
            let mut collected_ctes: Vec<crate::expressions::Cte> = Vec::new();
            let mut has_recursive = false;

            collect_nested_ctes(
                &Expression::Select(select.clone()),
                &mut collected_ctes,
                &mut has_recursive,
                true,
            );

            // Phase 2: Flatten CTEs nested inside top-level CTE definitions
            // This handles: WITH c AS (WITH b AS (...) SELECT ...) -> WITH b AS (...), c AS (SELECT ...)
            let mut cte_body_collected: Vec<(String, Vec<crate::expressions::Cte>)> = Vec::new();
            if let Some(ref with) = select.with {
                for cte in &with.ctes {
                    let mut body_ctes: Vec<crate::expressions::Cte> = Vec::new();
                    collect_ctes_from_cte_body(&cte.this, &mut body_ctes, &mut has_recursive);
                    if !body_ctes.is_empty() {
                        cte_body_collected.push((cte.alias.name.clone(), body_ctes));
                    }
                }
            }

            let has_subquery_ctes = !collected_ctes.is_empty();
            let has_body_ctes = !cte_body_collected.is_empty();

            if has_subquery_ctes || has_body_ctes {
                // Strip WITH clauses from inner subqueries
                strip_nested_with_clauses(&mut select, true);

                // Strip WITH clauses from CTE body definitions
                if has_body_ctes {
                    if let Some(ref mut with) = select.with {
                        for cte in with.ctes.iter_mut() {
                            strip_with_from_cte_body(&mut cte.this);
                        }
                    }
                }

                let top_with = select.with.get_or_insert_with(|| crate::expressions::With {
                    ctes: Vec::new(),
                    recursive: false,
                    leading_comments: vec![],
                    search: None,
                });

                if has_recursive {
                    top_with.recursive = true;
                }

                // Insert body CTEs before their parent CTE (Python sqlglot behavior)
                if has_body_ctes {
                    let mut new_ctes: Vec<crate::expressions::Cte> = Vec::new();
                    for mut cte in top_with.ctes.drain(..) {
                        // Check if this CTE has nested CTEs to insert before it
                        if let Some(pos) = cte_body_collected
                            .iter()
                            .position(|(name, _)| *name == cte.alias.name)
                        {
                            let (_, mut nested) = cte_body_collected.remove(pos);
                            // Strip WITH from each nested CTE's body too
                            for nested_cte in nested.iter_mut() {
                                strip_with_from_cte_body(&mut nested_cte.this);
                            }
                            new_ctes.extend(nested);
                        }
                        // Also strip WITH from the parent CTE's body
                        strip_with_from_cte_body(&mut cte.this);
                        new_ctes.push(cte);
                    }
                    top_with.ctes = new_ctes;
                }

                // Append collected subquery CTEs after existing ones
                top_with.ctes.extend(collected_ctes);
            }

            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Recursively collect CTEs from within CTE body expressions (for deep nesting)
fn collect_ctes_from_cte_body(
    expr: &Expression,
    collected: &mut Vec<crate::expressions::Cte>,
    has_recursive: &mut bool,
) {
    if let Expression::Select(select) = expr {
        if let Some(ref with) = select.with {
            if with.recursive {
                *has_recursive = true;
            }
            for cte in &with.ctes {
                // Recursively collect from this CTE's body first (depth-first)
                collect_ctes_from_cte_body(&cte.this, collected, has_recursive);
                // Then add this CTE itself
                collected.push(cte.clone());
            }
        }
    }
}

/// Strip WITH clauses from CTE body expressions
fn strip_with_from_cte_body(expr: &mut Expression) {
    if let Expression::Select(ref mut select) = expr {
        select.with = None;
    }
}

/// Strip WITH clauses from nested subqueries (after hoisting to top level)
fn strip_nested_with_clauses(select: &mut Select, _is_top_level: bool) {
    // Strip WITH from FROM subqueries
    if let Some(ref mut from) = select.from {
        for expr in from.expressions.iter_mut() {
            strip_with_from_expr(expr);
        }
    }
    // Strip from JOINs
    for join in select.joins.iter_mut() {
        strip_with_from_expr(&mut join.this);
    }
    // Strip from select expressions
    for expr in select.expressions.iter_mut() {
        strip_with_from_expr(expr);
    }
    // Strip from WHERE
    if let Some(ref mut w) = select.where_clause {
        strip_with_from_expr(&mut w.this);
    }
}

fn strip_with_from_expr(expr: &mut Expression) {
    match expr {
        Expression::Subquery(ref mut subquery) => {
            strip_with_from_inner_query(&mut subquery.this);
        }
        Expression::Alias(ref mut alias) => {
            strip_with_from_expr(&mut alias.this);
        }
        Expression::Select(ref mut select) => {
            // Strip WITH from this SELECT (it's nested)
            select.with = None;
            // Recurse into its subqueries
            strip_nested_with_clauses(select, false);
        }
        _ => {}
    }
}

fn strip_with_from_inner_query(expr: &mut Expression) {
    if let Expression::Select(ref mut select) = expr {
        select.with = None;
        strip_nested_with_clauses(select, false);
    }
}

/// Helper to recursively collect CTEs from nested subqueries
fn collect_nested_ctes(
    expr: &Expression,
    collected: &mut Vec<crate::expressions::Cte>,
    has_recursive: &mut bool,
    is_top_level: bool,
) {
    match expr {
        Expression::Select(select) => {
            // If this is not the top level and has a WITH clause, collect its CTEs
            if !is_top_level {
                if let Some(ref with) = select.with {
                    if with.recursive {
                        *has_recursive = true;
                    }
                    collected.extend(with.ctes.clone());
                }
            }

            // Recurse into FROM clause
            if let Some(ref from) = select.from {
                for expr in &from.expressions {
                    collect_nested_ctes(expr, collected, has_recursive, false);
                }
            }

            // Recurse into JOINs
            for join in &select.joins {
                collect_nested_ctes(&join.this, collected, has_recursive, false);
            }

            // Recurse into select expressions (for subqueries in SELECT)
            for sel_expr in &select.expressions {
                collect_nested_ctes(sel_expr, collected, has_recursive, false);
            }

            // Recurse into WHERE
            if let Some(ref where_clause) = select.where_clause {
                collect_nested_ctes(&where_clause.this, collected, has_recursive, false);
            }
        }
        Expression::Subquery(subquery) => {
            // Process the inner query
            collect_nested_ctes(&subquery.this, collected, has_recursive, false);
        }
        Expression::Alias(alias) => {
            collect_nested_ctes(&alias.this, collected, has_recursive, false);
        }
        // Add more expression types as needed
        _ => {}
    }
}

/// Inline window definitions from WINDOW clause.
///
/// Some dialects don't support named windows. This transform inlines them:
///
/// ```sql
/// SELECT SUM(a) OVER w FROM t WINDOW w AS (PARTITION BY b)
/// ```
///
/// To:
///
/// ```sql
/// SELECT SUM(a) OVER (PARTITION BY b) FROM t
/// ```
///
/// Reference: `transforms.py:975-1003`
pub fn eliminate_window_clause(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            if let Some(named_windows) = select.windows.take() {
                // Build a map of window name -> window spec
                let window_map: std::collections::HashMap<String, &Over> = named_windows
                    .iter()
                    .map(|nw| (nw.name.name.to_lowercase(), &nw.spec))
                    .collect();

                // Inline window references in the select expressions
                select.expressions = select
                    .expressions
                    .into_iter()
                    .map(|e| inline_window_refs(e, &window_map))
                    .collect();
            }
            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Helper function to inline window references in an expression
fn inline_window_refs(
    expr: Expression,
    window_map: &std::collections::HashMap<String, &Over>,
) -> Expression {
    match expr {
        Expression::WindowFunction(mut wf) => {
            // Check if this window references a named window
            if let Some(ref name) = wf.over.window_name {
                let key = name.name.to_lowercase();
                if let Some(named_spec) = window_map.get(&key) {
                    // Inherit properties from the named window
                    if wf.over.partition_by.is_empty() && !named_spec.partition_by.is_empty() {
                        wf.over.partition_by = named_spec.partition_by.clone();
                    }
                    if wf.over.order_by.is_empty() && !named_spec.order_by.is_empty() {
                        wf.over.order_by = named_spec.order_by.clone();
                    }
                    if wf.over.frame.is_none() && named_spec.frame.is_some() {
                        wf.over.frame = named_spec.frame.clone();
                    }
                    // Clear the window name reference
                    wf.over.window_name = None;
                }
            }
            Expression::WindowFunction(wf)
        }
        Expression::Alias(mut alias) => {
            // Recurse into aliased expressions
            alias.this = inline_window_refs(alias.this, window_map);
            Expression::Alias(alias)
        }
        // For a complete implementation, we would need to recursively visit all expressions
        // that can contain window functions (CASE, subqueries, etc.)
        other => other,
    }
}

/// Eliminate Oracle-style (+) join marks by converting to standard JOINs.
///
/// Oracle uses (+) syntax for outer joins:
/// ```sql
/// SELECT * FROM a, b WHERE a.x = b.x(+)
/// ```
///
/// This is converted to standard LEFT OUTER JOIN:
/// ```sql
/// SELECT * FROM a LEFT OUTER JOIN b ON a.x = b.x
/// ```
///
/// Reference: `transforms.py:828-945`
pub fn eliminate_join_marks(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            // Check if there are any join marks in the WHERE clause
            let has_join_marks = select
                .where_clause
                .as_ref()
                .map_or(false, |w| contains_join_mark(&w.this));

            if !has_join_marks {
                return Ok(Expression::Select(select));
            }

            // Collect tables from FROM clause
            let from_tables: Vec<String> = select
                .from
                .as_ref()
                .map(|f| {
                    f.expressions
                        .iter()
                        .filter_map(|e| get_table_name(e))
                        .collect()
                })
                .unwrap_or_default();

            // Extract join conditions and their marked tables from WHERE
            let mut join_conditions: std::collections::HashMap<String, Vec<Expression>> =
                std::collections::HashMap::new();
            let mut remaining_conditions: Vec<Expression> = Vec::new();

            if let Some(ref where_clause) = select.where_clause {
                extract_join_mark_conditions(
                    &where_clause.this,
                    &mut join_conditions,
                    &mut remaining_conditions,
                );
            }

            // Build new JOINs for each marked table
            let mut new_joins = select.joins.clone();
            for (table_name, conditions) in join_conditions {
                // Find if this table is in FROM or existing JOINs
                let table_in_from = from_tables.contains(&table_name);

                if table_in_from && !conditions.is_empty() {
                    // Create LEFT JOIN with combined conditions
                    let combined_condition = conditions.into_iter().reduce(|a, b| {
                        Expression::And(Box::new(BinaryOp {
                            left: a,
                            right: b,
                            left_comments: vec![],
                            operator_comments: vec![],
                            trailing_comments: vec![],
                        }))
                    });

                    // Find the table in FROM and move it to a JOIN
                    if let Some(ref mut from) = select.from {
                        if let Some(pos) = from
                            .expressions
                            .iter()
                            .position(|e| get_table_name(e).map_or(false, |n| n == table_name))
                        {
                            if from.expressions.len() > 1 {
                                let join_table = from.expressions.remove(pos);
                                new_joins.push(crate::expressions::Join {
                                    this: join_table,
                                    kind: JoinKind::Left,
                                    on: combined_condition,
                                    using: vec![],
                                    use_inner_keyword: false,
                                    use_outer_keyword: true,
                                    deferred_condition: false,
                                    join_hint: None,
                                    match_condition: None,
                                    pivots: Vec::new(),
                                    comments: Vec::new(),
                                    nesting_group: 0,
                                    directed: false,
                                });
                            }
                        }
                    }
                }
            }

            select.joins = new_joins;

            // Update WHERE with remaining conditions
            if remaining_conditions.is_empty() {
                select.where_clause = None;
            } else {
                let combined = remaining_conditions.into_iter().reduce(|a, b| {
                    Expression::And(Box::new(BinaryOp {
                        left: a,
                        right: b,
                        left_comments: vec![],
                        operator_comments: vec![],
                        trailing_comments: vec![],
                    }))
                });
                select.where_clause = combined.map(|c| Where { this: c });
            }

            // Clear join marks from all columns
            clear_join_marks(&mut Expression::Select(select.clone()));

            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Check if an expression contains any columns with join marks
fn contains_join_mark(expr: &Expression) -> bool {
    match expr {
        Expression::Column(col) => col.join_mark,
        Expression::And(op) | Expression::Or(op) => {
            contains_join_mark(&op.left) || contains_join_mark(&op.right)
        }
        Expression::Eq(op)
        | Expression::Neq(op)
        | Expression::Lt(op)
        | Expression::Lte(op)
        | Expression::Gt(op)
        | Expression::Gte(op) => contains_join_mark(&op.left) || contains_join_mark(&op.right),
        Expression::Not(op) => contains_join_mark(&op.this),
        _ => false,
    }
}

/// Get table name from a table expression
fn get_table_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Table(t) => Some(t.name.name.clone()),
        Expression::Alias(a) => Some(a.alias.name.clone()),
        _ => None,
    }
}

/// Extract join mark conditions from WHERE clause
fn extract_join_mark_conditions(
    expr: &Expression,
    join_conditions: &mut std::collections::HashMap<String, Vec<Expression>>,
    remaining: &mut Vec<Expression>,
) {
    match expr {
        Expression::And(op) => {
            extract_join_mark_conditions(&op.left, join_conditions, remaining);
            extract_join_mark_conditions(&op.right, join_conditions, remaining);
        }
        _ => {
            if let Some(table) = get_join_mark_table(expr) {
                join_conditions
                    .entry(table)
                    .or_insert_with(Vec::new)
                    .push(expr.clone());
            } else {
                remaining.push(expr.clone());
            }
        }
    }
}

/// Get the table name of a column with join mark in an expression
fn get_join_mark_table(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Eq(op)
        | Expression::Neq(op)
        | Expression::Lt(op)
        | Expression::Lte(op)
        | Expression::Gt(op)
        | Expression::Gte(op) => {
            // Check both sides for join mark columns
            if let Expression::Column(col) = &op.left {
                if col.join_mark {
                    return col.table.as_ref().map(|t| t.name.clone());
                }
            }
            if let Expression::Column(col) = &op.right {
                if col.join_mark {
                    return col.table.as_ref().map(|t| t.name.clone());
                }
            }
            None
        }
        _ => None,
    }
}

/// Clear join marks from all columns in an expression
fn clear_join_marks(expr: &mut Expression) {
    match expr {
        Expression::Column(col) => col.join_mark = false,
        Expression::Select(select) => {
            if let Some(ref mut w) = select.where_clause {
                clear_join_marks(&mut w.this);
            }
            for sel_expr in &mut select.expressions {
                clear_join_marks(sel_expr);
            }
        }
        Expression::And(op) | Expression::Or(op) => {
            clear_join_marks(&mut op.left);
            clear_join_marks(&mut op.right);
        }
        Expression::Eq(op)
        | Expression::Neq(op)
        | Expression::Lt(op)
        | Expression::Lte(op)
        | Expression::Gt(op)
        | Expression::Gte(op) => {
            clear_join_marks(&mut op.left);
            clear_join_marks(&mut op.right);
        }
        _ => {}
    }
}

/// Add column names to recursive CTE definitions.
///
/// Uses projection output names in recursive CTE definitions to define the CTEs' columns.
/// This is required by some dialects that need explicit column names in recursive CTEs.
///
/// Reference: `transforms.py:576-592`
pub fn add_recursive_cte_column_names(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            if let Some(ref mut with) = select.with {
                if with.recursive {
                    let mut counter = 0;
                    for cte in &mut with.ctes {
                        if cte.columns.is_empty() {
                            // Try to get column names from the CTE's SELECT
                            if let Expression::Select(ref cte_select) = cte.this {
                                let names: Vec<Identifier> = cte_select
                                    .expressions
                                    .iter()
                                    .map(|e| match e {
                                        Expression::Alias(a) => a.alias.clone(),
                                        Expression::Column(c) => c.name.clone(),
                                        _ => {
                                            counter += 1;
                                            Identifier::new(format!("_c_{}", counter))
                                        }
                                    })
                                    .collect();
                                cte.columns = names;
                            }
                        }
                    }
                }
            }
            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Convert epoch string in CAST to timestamp literal.
///
/// Replaces `CAST('epoch' AS TIMESTAMP)` with `CAST('1970-01-01 00:00:00' AS TIMESTAMP)`
/// for dialects that don't support the 'epoch' keyword.
///
/// Reference: `transforms.py:595-604`
pub fn epoch_cast_to_ts(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Cast(mut cast) => {
            if let Expression::Literal(Literal::String(ref s)) = cast.this {
                if s.to_lowercase() == "epoch" {
                    if is_temporal_type(&cast.to) {
                        cast.this =
                            Expression::Literal(Literal::String("1970-01-01 00:00:00".to_string()));
                    }
                }
            }
            Ok(Expression::Cast(cast))
        }
        Expression::TryCast(mut try_cast) => {
            if let Expression::Literal(Literal::String(ref s)) = try_cast.this {
                if s.to_lowercase() == "epoch" {
                    if is_temporal_type(&try_cast.to) {
                        try_cast.this =
                            Expression::Literal(Literal::String("1970-01-01 00:00:00".to_string()));
                    }
                }
            }
            Ok(Expression::TryCast(try_cast))
        }
        other => Ok(other),
    }
}

/// Check if a DataType is a temporal type (DATE, TIMESTAMP, etc.)
fn is_temporal_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Date | DataType::Timestamp { .. } | DataType::Time { .. }
    )
}

/// Ensure boolean values in conditions.
///
/// Converts numeric values used in conditions into explicit boolean expressions.
/// For dialects that require explicit booleans in WHERE clauses.
///
/// Converts:
/// ```sql
/// WHERE column
/// ```
/// To:
/// ```sql
/// WHERE column <> 0
/// ```
///
/// And:
/// ```sql
/// WHERE 1
/// ```
/// To:
/// ```sql
/// WHERE 1 <> 0
/// ```
///
/// Reference: `transforms.py:703-721`
pub fn ensure_bools(expr: Expression) -> Result<Expression> {
    // First, recursively process Case WHEN conditions throughout the expression tree
    let expr = ensure_bools_in_case(expr);
    match expr {
        Expression::Select(mut select) => {
            // Transform WHERE clause condition
            if let Some(ref mut where_clause) = select.where_clause {
                where_clause.this = ensure_bool_condition(where_clause.this.clone());
            }
            // Transform HAVING clause condition
            if let Some(ref mut having) = select.having {
                having.this = ensure_bool_condition(having.this.clone());
            }
            Ok(Expression::Select(select))
        }
        // Top-level AND/OR/NOT expressions also need ensure_bools processing
        Expression::And(_) | Expression::Or(_) | Expression::Not(_) => {
            Ok(ensure_bool_condition(expr))
        }
        other => Ok(other),
    }
}

/// Recursively walk the expression tree to find Case expressions and apply
/// ensure_bool_condition to their WHEN conditions. This ensures that
/// `CASE WHEN TRUE` becomes `CASE WHEN (1 = 1)` etc.
fn ensure_bools_in_case(expr: Expression) -> Expression {
    match expr {
        Expression::Case(mut case) => {
            case.whens = case
                .whens
                .into_iter()
                .map(|(condition, result)| {
                    let new_condition = ensure_bool_condition(ensure_bools_in_case(condition));
                    let new_result = ensure_bools_in_case(result);
                    (new_condition, new_result)
                })
                .collect();
            if let Some(else_expr) = case.else_ {
                case.else_ = Some(ensure_bools_in_case(else_expr));
            }
            Expression::Case(Box::new(*case))
        }
        Expression::Select(mut select) => {
            // Recursively process expressions in the SELECT list
            select.expressions = select
                .expressions
                .into_iter()
                .map(ensure_bools_in_case)
                .collect();
            // Process WHERE/HAVING are handled by ensure_bools main function
            Expression::Select(select)
        }
        Expression::Alias(mut alias) => {
            alias.this = ensure_bools_in_case(alias.this);
            Expression::Alias(alias)
        }
        Expression::Paren(mut paren) => {
            paren.this = ensure_bools_in_case(paren.this);
            Expression::Paren(paren)
        }
        other => other,
    }
}

/// Helper to check if an expression is inherently boolean (returns a boolean value).
/// Inherently boolean expressions include comparisons, predicates, logical operators, etc.
fn is_boolean_expression(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::Eq(_)
            | Expression::Neq(_)
            | Expression::Lt(_)
            | Expression::Lte(_)
            | Expression::Gt(_)
            | Expression::Gte(_)
            | Expression::Is(_)
            | Expression::IsNull(_)
            | Expression::IsTrue(_)
            | Expression::IsFalse(_)
            | Expression::Like(_)
            | Expression::ILike(_)
            | Expression::SimilarTo(_)
            | Expression::Glob(_)
            | Expression::RegexpLike(_)
            | Expression::In(_)
            | Expression::Between(_)
            | Expression::Exists(_)
            | Expression::And(_)
            | Expression::Or(_)
            | Expression::Not(_)
            | Expression::Any(_)
            | Expression::All(_)
            | Expression::EqualNull(_)
    )
}

/// Helper to wrap a non-boolean expression with `<> 0`
fn wrap_neq_zero(expr: Expression) -> Expression {
    Expression::Neq(Box::new(BinaryOp {
        left: expr,
        right: Expression::Literal(Literal::Number("0".to_string())),
        left_comments: vec![],
        operator_comments: vec![],
        trailing_comments: vec![],
    }))
}

/// Helper to convert a condition expression to ensure it's boolean.
///
/// In TSQL, conditions in WHERE/HAVING must be boolean expressions.
/// Non-boolean expressions (columns, literals, casts, function calls, etc.)
/// are wrapped with `<> 0`. Boolean literals are converted to `(1 = 1)` or `(1 = 0)`.
fn ensure_bool_condition(expr: Expression) -> Expression {
    match expr {
        // For AND/OR, recursively process children
        Expression::And(op) => {
            let new_op = BinaryOp {
                left: ensure_bool_condition(op.left.clone()),
                right: ensure_bool_condition(op.right.clone()),
                left_comments: op.left_comments.clone(),
                operator_comments: op.operator_comments.clone(),
                trailing_comments: op.trailing_comments.clone(),
            };
            Expression::And(Box::new(new_op))
        }
        Expression::Or(op) => {
            let new_op = BinaryOp {
                left: ensure_bool_condition(op.left.clone()),
                right: ensure_bool_condition(op.right.clone()),
                left_comments: op.left_comments.clone(),
                operator_comments: op.operator_comments.clone(),
                trailing_comments: op.trailing_comments.clone(),
            };
            Expression::Or(Box::new(new_op))
        }
        // For NOT, recursively process the inner expression
        Expression::Not(op) => Expression::Not(Box::new(crate::expressions::UnaryOp {
            this: ensure_bool_condition(op.this.clone()),
        })),
        // For Paren, recurse into inner expression
        Expression::Paren(paren) => Expression::Paren(Box::new(crate::expressions::Paren {
            this: ensure_bool_condition(paren.this.clone()),
            trailing_comments: paren.trailing_comments.clone(),
        })),
        // Boolean literals: true -> (1 = 1), false -> (1 = 0)
        Expression::Boolean(BooleanLiteral { value: true }) => {
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: Expression::Eq(Box::new(BinaryOp {
                    left: Expression::Literal(Literal::Number("1".to_string())),
                    right: Expression::Literal(Literal::Number("1".to_string())),
                    left_comments: vec![],
                    operator_comments: vec![],
                    trailing_comments: vec![],
                })),
                trailing_comments: vec![],
            }))
        }
        Expression::Boolean(BooleanLiteral { value: false }) => {
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: Expression::Eq(Box::new(BinaryOp {
                    left: Expression::Literal(Literal::Number("1".to_string())),
                    right: Expression::Literal(Literal::Number("0".to_string())),
                    left_comments: vec![],
                    operator_comments: vec![],
                    trailing_comments: vec![],
                })),
                trailing_comments: vec![],
            }))
        }
        // Already boolean expressions pass through unchanged
        ref e if is_boolean_expression(e) => expr,
        // Everything else (Column, Identifier, Cast, Literal::Number, function calls, etc.)
        // gets wrapped with <> 0
        _ => wrap_neq_zero(expr),
    }
}

/// Remove table qualifiers from column references.
///
/// Converts `table.column` to just `column` throughout the expression tree.
///
/// Reference: `transforms.py:724-730`
pub fn unqualify_columns(expr: Expression) -> Result<Expression> {
    Ok(unqualify_columns_recursive(expr))
}

/// Recursively remove table qualifiers from column references
fn unqualify_columns_recursive(expr: Expression) -> Expression {
    match expr {
        Expression::Column(mut col) => {
            col.table = None;
            Expression::Column(col)
        }
        Expression::Select(mut select) => {
            select.expressions = select
                .expressions
                .into_iter()
                .map(unqualify_columns_recursive)
                .collect();
            if let Some(ref mut where_clause) = select.where_clause {
                where_clause.this = unqualify_columns_recursive(where_clause.this.clone());
            }
            if let Some(ref mut having) = select.having {
                having.this = unqualify_columns_recursive(having.this.clone());
            }
            if let Some(ref mut group_by) = select.group_by {
                group_by.expressions = group_by
                    .expressions
                    .iter()
                    .cloned()
                    .map(unqualify_columns_recursive)
                    .collect();
            }
            if let Some(ref mut order_by) = select.order_by {
                order_by.expressions = order_by
                    .expressions
                    .iter()
                    .map(|o| crate::expressions::Ordered {
                        this: unqualify_columns_recursive(o.this.clone()),
                        desc: o.desc,
                        nulls_first: o.nulls_first,
                        explicit_asc: o.explicit_asc,
                        with_fill: o.with_fill.clone(),
                    })
                    .collect();
            }
            for join in &mut select.joins {
                if let Some(ref mut on) = join.on {
                    *on = unqualify_columns_recursive(on.clone());
                }
            }
            Expression::Select(select)
        }
        Expression::Alias(mut alias) => {
            alias.this = unqualify_columns_recursive(alias.this);
            Expression::Alias(alias)
        }
        // Binary operations
        Expression::And(op) => Expression::And(Box::new(unqualify_binary_op(*op))),
        Expression::Or(op) => Expression::Or(Box::new(unqualify_binary_op(*op))),
        Expression::Eq(op) => Expression::Eq(Box::new(unqualify_binary_op(*op))),
        Expression::Neq(op) => Expression::Neq(Box::new(unqualify_binary_op(*op))),
        Expression::Lt(op) => Expression::Lt(Box::new(unqualify_binary_op(*op))),
        Expression::Lte(op) => Expression::Lte(Box::new(unqualify_binary_op(*op))),
        Expression::Gt(op) => Expression::Gt(Box::new(unqualify_binary_op(*op))),
        Expression::Gte(op) => Expression::Gte(Box::new(unqualify_binary_op(*op))),
        Expression::Add(op) => Expression::Add(Box::new(unqualify_binary_op(*op))),
        Expression::Sub(op) => Expression::Sub(Box::new(unqualify_binary_op(*op))),
        Expression::Mul(op) => Expression::Mul(Box::new(unqualify_binary_op(*op))),
        Expression::Div(op) => Expression::Div(Box::new(unqualify_binary_op(*op))),
        // Functions
        Expression::Function(mut func) => {
            func.args = func
                .args
                .into_iter()
                .map(unqualify_columns_recursive)
                .collect();
            Expression::Function(func)
        }
        Expression::AggregateFunction(mut func) => {
            func.args = func
                .args
                .into_iter()
                .map(unqualify_columns_recursive)
                .collect();
            Expression::AggregateFunction(func)
        }
        Expression::Case(mut case) => {
            case.whens = case
                .whens
                .into_iter()
                .map(|(cond, result)| {
                    (
                        unqualify_columns_recursive(cond),
                        unqualify_columns_recursive(result),
                    )
                })
                .collect();
            if let Some(else_expr) = case.else_ {
                case.else_ = Some(unqualify_columns_recursive(else_expr));
            }
            Expression::Case(case)
        }
        // Other expressions pass through unchanged
        other => other,
    }
}

/// Helper to unqualify columns in a binary operation
fn unqualify_binary_op(mut op: BinaryOp) -> BinaryOp {
    op.left = unqualify_columns_recursive(op.left);
    op.right = unqualify_columns_recursive(op.right);
    op
}

/// Convert UNNEST(GENERATE_DATE_ARRAY(...)) to recursive CTE.
///
/// For dialects that don't support GENERATE_DATE_ARRAY, this converts:
/// ```sql
/// SELECT * FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-01-31', INTERVAL 1 DAY)) AS d(date_value)
/// ```
/// To a recursive CTE:
/// ```sql
/// WITH RECURSIVE _generated_dates(date_value) AS (
///     SELECT CAST('2024-01-01' AS DATE) AS date_value
///     UNION ALL
///     SELECT CAST(DATE_ADD(date_value, 1, DAY) AS DATE)
///     FROM _generated_dates
///     WHERE CAST(DATE_ADD(date_value, 1, DAY) AS DATE) <= CAST('2024-01-31' AS DATE)
/// )
/// SELECT date_value FROM _generated_dates
/// ```
///
/// Reference: `transforms.py:68-122`
pub fn unnest_generate_date_array_using_recursive_cte(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            let mut cte_count = 0;
            let mut new_ctes: Vec<crate::expressions::Cte> = Vec::new();

            // Process existing CTE bodies first (to handle CTE-wrapped GENERATE_DATE_ARRAY)
            if let Some(ref mut with) = select.with {
                for cte in &mut with.ctes {
                    process_expression_for_gda(&mut cte.this, &mut cte_count, &mut new_ctes);
                }
            }

            // Process FROM clause
            if let Some(ref mut from) = select.from {
                for table_expr in &mut from.expressions {
                    if let Some((cte, replacement)) =
                        try_convert_generate_date_array(table_expr, &mut cte_count)
                    {
                        new_ctes.push(cte);
                        *table_expr = replacement;
                    }
                }
            }

            // Process JOINs
            for join in &mut select.joins {
                if let Some((cte, replacement)) =
                    try_convert_generate_date_array(&join.this, &mut cte_count)
                {
                    new_ctes.push(cte);
                    join.this = replacement;
                }
            }

            // Add collected CTEs to the WITH clause
            if !new_ctes.is_empty() {
                let with_clause = select.with.get_or_insert_with(|| crate::expressions::With {
                    ctes: Vec::new(),
                    recursive: true, // Recursive CTEs
                    leading_comments: vec![],
                    search: None,
                });
                with_clause.recursive = true;

                // Prepend new CTEs before existing ones
                let mut all_ctes = new_ctes;
                all_ctes.append(&mut with_clause.ctes);
                with_clause.ctes = all_ctes;
            }

            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Recursively process an expression tree to find and convert UNNEST(GENERATE_DATE_ARRAY)
/// inside CTE bodies, subqueries, etc.
fn process_expression_for_gda(
    expr: &mut Expression,
    cte_count: &mut usize,
    new_ctes: &mut Vec<crate::expressions::Cte>,
) {
    match expr {
        Expression::Select(ref mut select) => {
            // Process FROM clause
            if let Some(ref mut from) = select.from {
                for table_expr in &mut from.expressions {
                    if let Some((cte, replacement)) =
                        try_convert_generate_date_array(table_expr, cte_count)
                    {
                        new_ctes.push(cte);
                        *table_expr = replacement;
                    }
                }
            }
            // Process JOINs
            for join in &mut select.joins {
                if let Some((cte, replacement)) =
                    try_convert_generate_date_array(&join.this, cte_count)
                {
                    new_ctes.push(cte);
                    join.this = replacement;
                }
            }
        }
        Expression::Union(ref mut u) => {
            process_expression_for_gda(&mut u.left, cte_count, new_ctes);
            process_expression_for_gda(&mut u.right, cte_count, new_ctes);
        }
        Expression::Subquery(ref mut sq) => {
            process_expression_for_gda(&mut sq.this, cte_count, new_ctes);
        }
        _ => {}
    }
}

/// Try to convert an UNNEST(GENERATE_DATE_ARRAY(...)) to a recursive CTE reference.
/// `column_name_override` allows the caller to specify a custom column name (from alias).
fn try_convert_generate_date_array(
    expr: &Expression,
    cte_count: &mut usize,
) -> Option<(crate::expressions::Cte, Expression)> {
    try_convert_generate_date_array_with_name(expr, cte_count, None)
}

fn try_convert_generate_date_array_with_name(
    expr: &Expression,
    cte_count: &mut usize,
    column_name_override: Option<&str>,
) -> Option<(crate::expressions::Cte, Expression)> {
    // Helper: extract (start, end, step) from GENERATE_DATE_ARRAY/GenerateSeries variants
    fn extract_gda_args(
        inner: &Expression,
    ) -> Option<(&Expression, &Expression, Option<&Expression>)> {
        match inner {
            Expression::GenerateDateArray(gda) => {
                let start = gda.start.as_ref()?;
                let end = gda.end.as_ref()?;
                let step = gda.step.as_deref();
                Some((start, end, step))
            }
            Expression::GenerateSeries(gs) => {
                let start = gs.start.as_deref()?;
                let end = gs.end.as_deref()?;
                let step = gs.step.as_deref();
                Some((start, end, step))
            }
            Expression::Function(f) if f.name.eq_ignore_ascii_case("GENERATE_DATE_ARRAY") => {
                if f.args.len() >= 2 {
                    let start = &f.args[0];
                    let end = &f.args[1];
                    let step = f.args.get(2);
                    Some((start, end, step))
                } else {
                    None
                }
            }
            _ => None,
        }
    }

    // Look for UNNEST containing GENERATE_DATE_ARRAY
    if let Expression::Unnest(unnest) = expr {
        if let Some((start, end, step_opt)) = extract_gda_args(&unnest.this) {
            let start = start;
            let end = end;
            let step: Option<&Expression> = step_opt;

            // Generate CTE name
            let cte_name = if *cte_count == 0 {
                "_generated_dates".to_string()
            } else {
                format!("_generated_dates_{}", cte_count)
            };
            *cte_count += 1;

            let column_name =
                Identifier::new(column_name_override.unwrap_or("date_value").to_string());

            // Helper: wrap expression in CAST(... AS DATE) unless already a date literal or CAST to DATE
            let cast_to_date = |expr: &Expression| -> Expression {
                match expr {
                    Expression::Literal(Literal::Date(_)) => {
                        // DATE '...' -> convert to CAST('...' AS DATE) to match expected output
                        if let Expression::Literal(Literal::Date(d)) = expr {
                            Expression::Cast(Box::new(Cast {
                                this: Expression::Literal(Literal::String(d.clone())),
                                to: DataType::Date,
                                trailing_comments: vec![],
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                            }))
                        } else {
                            unreachable!()
                        }
                    }
                    Expression::Cast(c) if matches!(c.to, DataType::Date) => expr.clone(),
                    _ => Expression::Cast(Box::new(Cast {
                        this: expr.clone(),
                        to: DataType::Date,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    })),
                }
            };

            // Build base case: SELECT CAST(start AS DATE) AS date_value
            let base_select = Select {
                expressions: vec![Expression::Alias(Box::new(crate::expressions::Alias {
                    this: cast_to_date(start),
                    alias: column_name.clone(),
                    column_aliases: vec![],
                    pre_alias_comments: vec![],
                    trailing_comments: vec![],
                }))],
                ..Select::new()
            };

            // Normalize interval: convert String("1") -> Number("1") so it generates without quotes
            let normalize_interval = |expr: &Expression| -> Expression {
                if let Expression::Interval(ref iv) = expr {
                    let mut iv_clone = iv.as_ref().clone();
                    if let Some(Expression::Literal(Literal::String(ref s))) = iv_clone.this {
                        // Convert numeric strings to Number literals for unquoted output
                        if s.parse::<f64>().is_ok() {
                            iv_clone.this = Some(Expression::Literal(Literal::Number(s.clone())));
                        }
                    }
                    Expression::Interval(Box::new(iv_clone))
                } else {
                    expr.clone()
                }
            };

            // Build recursive case: DateAdd(date_value, count, unit) from CTE where result <= end
            // Extract interval unit and count from step expression
            let normalized_step = step.map(|s| normalize_interval(s)).unwrap_or_else(|| {
                Expression::Interval(Box::new(crate::expressions::Interval {
                    this: Some(Expression::Literal(Literal::Number("1".to_string()))),
                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                        unit: crate::expressions::IntervalUnit::Day,
                        use_plural: false,
                    }),
                }))
            });

            // Extract unit and count from interval expression to build DateAddFunc
            let (add_unit, add_count) = extract_interval_unit_and_count(&normalized_step);

            let date_add_expr = Expression::DateAdd(Box::new(crate::expressions::DateAddFunc {
                this: Expression::Column(crate::expressions::Column {
                    name: column_name.clone(),
                    table: None,
                    join_mark: false,
                    trailing_comments: vec![],
                    span: None,
                }),
                interval: add_count,
                unit: add_unit,
            }));

            let cast_date_add = Expression::Cast(Box::new(Cast {
                this: date_add_expr.clone(),
                to: DataType::Date,
                trailing_comments: vec![],
                double_colon_syntax: false,
                format: None,
                default: None,
            }));

            let recursive_select = Select {
                expressions: vec![cast_date_add.clone()],
                from: Some(From {
                    expressions: vec![Expression::Table(crate::expressions::TableRef::new(
                        &cte_name,
                    ))],
                }),
                where_clause: Some(Where {
                    this: Expression::Lte(Box::new(BinaryOp {
                        left: cast_date_add,
                        right: cast_to_date(end),
                        left_comments: vec![],
                        operator_comments: vec![],
                        trailing_comments: vec![],
                    })),
                }),
                ..Select::new()
            };

            // Build UNION ALL of base and recursive
            let union = crate::expressions::Union {
                left: Expression::Select(Box::new(base_select)),
                right: Expression::Select(Box::new(recursive_select)),
                all: true, // UNION ALL
                distinct: false,
                with: None,
                order_by: None,
                limit: None,
                offset: None,
                distribute_by: None,
                sort_by: None,
                cluster_by: None,
                by_name: false,
                side: None,
                kind: None,
                corresponding: false,
                strict: false,
                on_columns: Vec::new(),
            };

            // Create CTE
            let cte = crate::expressions::Cte {
                this: Expression::Union(Box::new(union)),
                alias: Identifier::new(cte_name.clone()),
                columns: vec![column_name.clone()],
                materialized: None,
                key_expressions: Vec::new(),
                alias_first: true,
                comments: Vec::new(),
            };

            // Create replacement: SELECT date_value FROM cte_name
            let replacement_select = Select {
                expressions: vec![Expression::Column(crate::expressions::Column {
                    name: column_name,
                    table: None,
                    join_mark: false,
                    trailing_comments: vec![],
                    span: None,
                })],
                from: Some(From {
                    expressions: vec![Expression::Table(crate::expressions::TableRef::new(
                        &cte_name,
                    ))],
                }),
                ..Select::new()
            };

            let replacement = Expression::Subquery(Box::new(Subquery {
                this: Expression::Select(Box::new(replacement_select)),
                alias: Some(Identifier::new(cte_name)),
                column_aliases: vec![],
                order_by: None,
                limit: None,
                offset: None,
                distribute_by: None,
                sort_by: None,
                cluster_by: None,
                lateral: false,
                modifiers_inside: false,
                trailing_comments: vec![],
            }));

            return Some((cte, replacement));
        }
    }

    // Also check for aliased UNNEST like UNNEST(...) AS _q(date_week)
    if let Expression::Alias(alias) = expr {
        // Extract column name from alias column_aliases if present
        let col_name = alias.column_aliases.first().map(|id| id.name.as_str());
        if let Some((cte, replacement)) =
            try_convert_generate_date_array_with_name(&alias.this, cte_count, col_name)
        {
            // If we extracted a column name from the alias, don't preserve the outer alias
            // since the CTE now uses that column name directly
            if col_name.is_some() {
                return Some((cte, replacement));
            }
            let new_alias = Expression::Alias(Box::new(crate::expressions::Alias {
                this: replacement,
                alias: alias.alias.clone(),
                column_aliases: alias.column_aliases.clone(),
                pre_alias_comments: alias.pre_alias_comments.clone(),
                trailing_comments: alias.trailing_comments.clone(),
            }));
            return Some((cte, new_alias));
        }
    }

    None
}

/// Extract interval unit and count from an interval expression.
/// Handles both structured intervals (with separate unit field) and
/// string-encoded intervals like `INTERVAL '1 WEEK'` where unit is None
/// and the value contains both count and unit.
fn extract_interval_unit_and_count(
    expr: &Expression,
) -> (crate::expressions::IntervalUnit, Expression) {
    use crate::expressions::{IntervalUnit, IntervalUnitSpec, Literal};

    if let Expression::Interval(ref iv) = expr {
        // First try: structured unit field
        if let Some(ref unit_spec) = iv.unit {
            if let IntervalUnitSpec::Simple { unit, .. } = unit_spec {
                let count = match &iv.this {
                    Some(e) => e.clone(),
                    None => Expression::Literal(Literal::Number("1".to_string())),
                };
                return (unit.clone(), count);
            }
        }

        // Second try: parse from string value like "1 WEEK" or "1"
        if let Some(ref val_expr) = iv.this {
            match val_expr {
                Expression::Literal(Literal::String(s))
                | Expression::Literal(Literal::Number(s)) => {
                    // Try to parse "count unit" format like "1 WEEK", "1 MONTH"
                    let parts: Vec<&str> = s.trim().splitn(2, char::is_whitespace).collect();
                    if parts.len() == 2 {
                        let count_str = parts[0].trim();
                        let unit_str = parts[1].trim().to_uppercase();
                        let unit = match unit_str.as_str() {
                            "YEAR" | "YEARS" => IntervalUnit::Year,
                            "QUARTER" | "QUARTERS" => IntervalUnit::Quarter,
                            "MONTH" | "MONTHS" => IntervalUnit::Month,
                            "WEEK" | "WEEKS" => IntervalUnit::Week,
                            "DAY" | "DAYS" => IntervalUnit::Day,
                            "HOUR" | "HOURS" => IntervalUnit::Hour,
                            "MINUTE" | "MINUTES" => IntervalUnit::Minute,
                            "SECOND" | "SECONDS" => IntervalUnit::Second,
                            "MILLISECOND" | "MILLISECONDS" => IntervalUnit::Millisecond,
                            "MICROSECOND" | "MICROSECONDS" => IntervalUnit::Microsecond,
                            _ => IntervalUnit::Day,
                        };
                        return (
                            unit,
                            Expression::Literal(Literal::Number(count_str.to_string())),
                        );
                    }
                    // Just a number with no unit - default to Day
                    if s.parse::<f64>().is_ok() {
                        return (
                            IntervalUnit::Day,
                            Expression::Literal(Literal::Number(s.clone())),
                        );
                    }
                }
                _ => {}
            }
        }

        // Fallback
        (
            IntervalUnit::Day,
            Expression::Literal(Literal::Number("1".to_string())),
        )
    } else {
        (
            IntervalUnit::Day,
            Expression::Literal(Literal::Number("1".to_string())),
        )
    }
}

/// Convert ILIKE to LOWER(x) LIKE LOWER(y).
///
/// For dialects that don't support ILIKE (case-insensitive LIKE), this converts:
/// ```sql
/// SELECT * FROM t WHERE x ILIKE '%pattern%'
/// ```
/// To:
/// ```sql
/// SELECT * FROM t WHERE LOWER(x) LIKE LOWER('%pattern%')
/// ```
///
/// Reference: `generator.py:no_ilike_sql()`
pub fn no_ilike_sql(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::ILike(ilike) => {
            // Create LOWER(left) LIKE LOWER(right)
            let lower_left = Expression::Function(Box::new(crate::expressions::Function {
                name: "LOWER".to_string(),
                args: vec![ilike.left],
                distinct: false,
                trailing_comments: vec![],
                use_bracket_syntax: false,
                no_parens: false,
                quoted: false,
                span: None,
            }));

            let lower_right = Expression::Function(Box::new(crate::expressions::Function {
                name: "LOWER".to_string(),
                args: vec![ilike.right],
                distinct: false,
                trailing_comments: vec![],
                use_bracket_syntax: false,
                no_parens: false,
                quoted: false,
                span: None,
            }));

            Ok(Expression::Like(Box::new(crate::expressions::LikeOp {
                left: lower_left,
                right: lower_right,
                escape: ilike.escape,
                quantifier: ilike.quantifier,
            })))
        }
        other => Ok(other),
    }
}

/// Convert TryCast to Cast.
///
/// For dialects that don't support TRY_CAST (safe cast that returns NULL on error),
/// this converts TRY_CAST to regular CAST. Note: This may cause runtime errors
/// for invalid casts that TRY_CAST would handle gracefully.
///
/// Reference: `generator.py:no_trycast_sql()`
pub fn no_trycast_sql(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::TryCast(try_cast) => Ok(Expression::Cast(try_cast)),
        other => Ok(other),
    }
}

/// Convert SafeCast to Cast.
///
/// For dialects that don't support SAFE_CAST (BigQuery's safe cast syntax),
/// this converts SAFE_CAST to regular CAST.
pub fn no_safe_cast_sql(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::SafeCast(safe_cast) => Ok(Expression::Cast(safe_cast)),
        other => Ok(other),
    }
}

/// Convert COMMENT ON statements to inline comments.
///
/// For dialects that don't support COMMENT ON syntax, this can transform
/// comment statements into inline comments or skip them entirely.
///
/// Reference: `generator.py:no_comment_column_constraint_sql()`
pub fn no_comment_column_constraint(expr: Expression) -> Result<Expression> {
    // For now, just pass through - comment handling is done in generator
    Ok(expr)
}

/// Convert TABLE GENERATE_SERIES to UNNEST(GENERATE_SERIES(...)).
///
/// Some dialects use GENERATE_SERIES as a table-valued function, while others
/// prefer the UNNEST syntax. This converts:
/// ```sql
/// SELECT * FROM GENERATE_SERIES(1, 10) AS t(n)
/// ```
/// To:
/// ```sql
/// SELECT * FROM UNNEST(GENERATE_SERIES(1, 10)) AS _u(n)
/// ```
///
/// Reference: `transforms.py:125-135`
pub fn unnest_generate_series(expr: Expression) -> Result<Expression> {
    // Convert TABLE GENERATE_SERIES to UNNEST(GENERATE_SERIES(...))
    // This handles the case where GENERATE_SERIES is used as a table-valued function
    match expr {
        Expression::Table(ref table) => {
            // Check if the table name matches GENERATE_SERIES pattern
            // In practice, this would be Expression::GenerateSeries wrapped in a Table context
            if table.name.name.to_uppercase() == "GENERATE_SERIES" {
                // Create UNNEST wrapper
                let unnest = Expression::Unnest(Box::new(UnnestFunc {
                    this: expr.clone(),
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                }));

                // If there's an alias, wrap in alias
                return Ok(Expression::Alias(Box::new(crate::expressions::Alias {
                    this: unnest,
                    alias: Identifier::new("_u".to_string()),
                    column_aliases: vec![],
                    pre_alias_comments: vec![],
                    trailing_comments: vec![],
                })));
            }
            Ok(expr)
        }
        Expression::GenerateSeries(gs) => {
            // Wrap GenerateSeries directly in UNNEST
            let unnest = Expression::Unnest(Box::new(UnnestFunc {
                this: Expression::GenerateSeries(gs),
                expressions: Vec::new(),
                with_ordinality: false,
                alias: None,
                offset_alias: None,
            }));
            Ok(unnest)
        }
        other => Ok(other),
    }
}

/// Convert UNNEST(GENERATE_SERIES(start, end, step)) to a subquery for PostgreSQL.
///
/// PostgreSQL's GENERATE_SERIES returns rows directly, so UNNEST wrapping is unnecessary.
/// Instead, convert to:
/// ```sql
/// (SELECT CAST(value AS DATE) FROM GENERATE_SERIES(start, end, step) AS _t(value)) AS _unnested_generate_series
/// ```
///
/// This handles the case where GENERATE_DATE_ARRAY was converted to GENERATE_SERIES
/// during cross-dialect normalization, but the original had UNNEST wrapping.
pub fn unwrap_unnest_generate_series_for_postgres(expr: Expression) -> Result<Expression> {
    use crate::dialects::transform_recursive;
    transform_recursive(expr, &unwrap_unnest_generate_series_single)
}

fn unwrap_unnest_generate_series_single(expr: Expression) -> Result<Expression> {
    use crate::expressions::*;
    // Match UNNEST(GENERATE_SERIES(...)) patterns in FROM clauses
    match expr {
        Expression::Select(mut select) => {
            // Process FROM clause
            if let Some(ref mut from) = select.from {
                for table_expr in &mut from.expressions {
                    if let Some(replacement) = try_unwrap_unnest_gen_series(table_expr) {
                        *table_expr = replacement;
                    }
                }
            }
            // Process JOINs
            for join in &mut select.joins {
                if let Some(replacement) = try_unwrap_unnest_gen_series(&join.this) {
                    join.this = replacement;
                }
            }
            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Try to convert an UNNEST(GENERATE_SERIES(...)) to a PostgreSQL subquery.
/// Returns the replacement expression if applicable.
fn try_unwrap_unnest_gen_series(expr: &Expression) -> Option<Expression> {
    use crate::expressions::*;

    // Match Unnest containing GenerateSeries
    let gen_series = match expr {
        Expression::Unnest(unnest) => {
            if let Expression::GenerateSeries(ref gs) = unnest.this {
                Some(gs.as_ref().clone())
            } else {
                None
            }
        }
        Expression::Alias(alias) => {
            if let Expression::Unnest(ref unnest) = alias.this {
                if let Expression::GenerateSeries(ref gs) = unnest.this {
                    Some(gs.as_ref().clone())
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    };

    let gs = gen_series?;

    // Build: (SELECT CAST(value AS DATE) FROM GENERATE_SERIES(start, end, step) AS _t(value)) AS _unnested_generate_series
    let value_col = Expression::Column(Column {
        name: Identifier::new("value".to_string()),
        table: None,
        join_mark: false,
        trailing_comments: vec![],
        span: None,
    });

    let cast_value = Expression::Cast(Box::new(Cast {
        this: value_col,
        to: DataType::Date,
        trailing_comments: vec![],
        double_colon_syntax: false,
        format: None,
        default: None,
    }));

    let gen_series_expr = Expression::GenerateSeries(Box::new(gs));

    // GENERATE_SERIES(...) AS _t(value)
    let gen_series_aliased = Expression::Alias(Box::new(Alias {
        this: gen_series_expr,
        alias: Identifier::new("_t".to_string()),
        column_aliases: vec![Identifier::new("value".to_string())],
        pre_alias_comments: vec![],
        trailing_comments: vec![],
    }));

    let mut inner_select = Select::new();
    inner_select.expressions = vec![cast_value];
    inner_select.from = Some(From {
        expressions: vec![gen_series_aliased],
    });

    let inner_select_expr = Expression::Select(Box::new(inner_select));

    let subquery = Expression::Subquery(Box::new(Subquery {
        this: inner_select_expr,
        alias: None,
        column_aliases: vec![],
        order_by: None,
        limit: None,
        offset: None,
        distribute_by: None,
        sort_by: None,
        cluster_by: None,
        lateral: false,
        modifiers_inside: false,
        trailing_comments: vec![],
    }));

    // Wrap in alias AS _unnested_generate_series
    Some(Expression::Alias(Box::new(Alias {
        this: subquery,
        alias: Identifier::new("_unnested_generate_series".to_string()),
        column_aliases: vec![],
        pre_alias_comments: vec![],
        trailing_comments: vec![],
    })))
}

/// Expand BETWEEN expressions in DELETE statements to >= AND <=
///
/// Some dialects (like StarRocks) don't support BETWEEN in DELETE statements
/// or prefer the expanded form. This transforms:
///   `DELETE FROM t WHERE a BETWEEN b AND c`
/// to:
///   `DELETE FROM t WHERE a >= b AND a <= c`
pub fn expand_between_in_delete(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Delete(mut delete) => {
            // If there's a WHERE clause, expand any BETWEEN expressions in it
            if let Some(ref mut where_clause) = delete.where_clause {
                where_clause.this = expand_between_recursive(where_clause.this.clone());
            }
            Ok(Expression::Delete(delete))
        }
        other => Ok(other),
    }
}

/// Recursively expand BETWEEN expressions to >= AND <=
fn expand_between_recursive(expr: Expression) -> Expression {
    match expr {
        // Expand: a BETWEEN b AND c -> a >= b AND a <= c
        // Expand: a NOT BETWEEN b AND c -> a < b OR a > c
        Expression::Between(between) => {
            let this = expand_between_recursive(between.this.clone());
            let low = expand_between_recursive(between.low);
            let high = expand_between_recursive(between.high);

            if between.not {
                // NOT BETWEEN: a < b OR a > c
                Expression::Or(Box::new(BinaryOp::new(
                    Expression::Lt(Box::new(BinaryOp::new(this.clone(), low))),
                    Expression::Gt(Box::new(BinaryOp::new(this, high))),
                )))
            } else {
                // BETWEEN: a >= b AND a <= c
                Expression::And(Box::new(BinaryOp::new(
                    Expression::Gte(Box::new(BinaryOp::new(this.clone(), low))),
                    Expression::Lte(Box::new(BinaryOp::new(this, high))),
                )))
            }
        }

        // Recursively process AND/OR expressions
        Expression::And(mut op) => {
            op.left = expand_between_recursive(op.left);
            op.right = expand_between_recursive(op.right);
            Expression::And(op)
        }
        Expression::Or(mut op) => {
            op.left = expand_between_recursive(op.left);
            op.right = expand_between_recursive(op.right);
            Expression::Or(op)
        }
        Expression::Not(mut op) => {
            op.this = expand_between_recursive(op.this);
            Expression::Not(op)
        }

        // Recursively process parenthesized expressions
        Expression::Paren(mut paren) => {
            paren.this = expand_between_recursive(paren.this);
            Expression::Paren(paren)
        }

        // Pass through everything else unchanged
        other => other,
    }
}

/// Push down CTE column names into SELECT expressions.
///
/// BigQuery doesn't support column names when defining a CTE, e.g.:
/// `WITH vartab(v) AS (SELECT ...)` is not valid.
/// Instead, it expects: `WITH vartab AS (SELECT ... AS v)`.
///
/// This transform removes the CTE column aliases and adds them as
/// aliases on the SELECT expressions.
pub fn pushdown_cte_column_names(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            if let Some(ref mut with) = select.with {
                for cte in &mut with.ctes {
                    if !cte.columns.is_empty() {
                        // Check if the CTE body is a star query - if so, just strip column names
                        let is_star = matches!(&cte.this, Expression::Select(s) if
                            s.expressions.len() == 1 && matches!(&s.expressions[0], Expression::Star(_)));

                        if is_star {
                            // Can't push down column names for star queries, just remove them
                            cte.columns.clear();
                            continue;
                        }

                        // Extract column names
                        let column_names: Vec<Identifier> = cte.columns.drain(..).collect();

                        // Push column names down into the SELECT expressions
                        if let Expression::Select(ref mut inner_select) = cte.this {
                            let new_exprs: Vec<Expression> = inner_select
                                .expressions
                                .drain(..)
                                .zip(
                                    column_names
                                        .into_iter()
                                        .chain(std::iter::repeat_with(|| Identifier::new(""))),
                                )
                                .map(|(expr, col_name)| {
                                    if col_name.name.is_empty() {
                                        return expr;
                                    }
                                    // If already aliased, replace the alias
                                    match expr {
                                        Expression::Alias(mut a) => {
                                            a.alias = col_name;
                                            Expression::Alias(a)
                                        }
                                        other => {
                                            Expression::Alias(Box::new(crate::expressions::Alias {
                                                this: other,
                                                alias: col_name,
                                                column_aliases: Vec::new(),
                                                pre_alias_comments: Vec::new(),
                                                trailing_comments: Vec::new(),
                                            }))
                                        }
                                    }
                                })
                                .collect();
                            inner_select.expressions = new_exprs;
                        }
                    }
                }
            }
            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Simplify nested parentheses around VALUES in FROM clause.
/// Converts `FROM ((VALUES (1)))` to `FROM (VALUES (1))` by stripping redundant wrapping.
/// Handles various nesting patterns: Subquery(Paren(Values)), Paren(Paren(Values)), etc.
pub fn simplify_nested_paren_values(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            if let Some(ref mut from) = select.from {
                for from_item in from.expressions.iter_mut() {
                    simplify_paren_values_in_from(from_item);
                }
            }
            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

fn simplify_paren_values_in_from(expr: &mut Expression) {
    // Check various patterns and build replacement if needed
    let replacement = match expr {
        // Subquery(Paren(Values)) -> Subquery with Values directly
        Expression::Subquery(ref subquery) => {
            if let Expression::Paren(ref paren) = subquery.this {
                if matches!(&paren.this, Expression::Values(_)) {
                    let mut new_sub = subquery.as_ref().clone();
                    new_sub.this = paren.this.clone();
                    Some(Expression::Subquery(Box::new(new_sub)))
                } else {
                    None
                }
            } else {
                None
            }
        }
        // Paren(Subquery(Values)) -> Subquery(Values) - strip the Paren wrapper
        // Paren(Paren(Values)) -> Paren(Values) - strip one layer
        Expression::Paren(ref outer_paren) => {
            if let Expression::Subquery(ref subquery) = outer_paren.this {
                // Paren(Subquery(Values)) -> Subquery(Values) - strip outer Paren
                if matches!(&subquery.this, Expression::Values(_)) {
                    Some(outer_paren.this.clone())
                }
                // Paren(Subquery(Paren(Values))) -> Subquery(Values)
                else if let Expression::Paren(ref paren) = subquery.this {
                    if matches!(&paren.this, Expression::Values(_)) {
                        let mut new_sub = subquery.as_ref().clone();
                        new_sub.this = paren.this.clone();
                        Some(Expression::Subquery(Box::new(new_sub)))
                    } else {
                        None
                    }
                } else {
                    None
                }
            } else if let Expression::Paren(ref inner_paren) = outer_paren.this {
                if matches!(&inner_paren.this, Expression::Values(_)) {
                    Some(outer_paren.this.clone())
                } else {
                    None
                }
            } else {
                None
            }
        }
        _ => None,
    };
    if let Some(new_expr) = replacement {
        *expr = new_expr;
    }
}

/// Add auto-generated table aliases (like `_t0`) for POSEXPLODE/EXPLODE in FROM clause
/// when the alias has column_aliases but no alias name.
/// This is needed for Spark target: `FROM POSEXPLODE(x) AS (a, b)` -> `FROM POSEXPLODE(x) AS _t0(a, b)`
pub fn add_auto_table_alias(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(mut select) => {
            // Process FROM expressions
            if let Some(ref mut from) = select.from {
                let mut counter = 0usize;
                for from_item in from.expressions.iter_mut() {
                    add_auto_alias_to_from_item(from_item, &mut counter);
                }
            }
            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

fn add_auto_alias_to_from_item(expr: &mut Expression, counter: &mut usize) {
    use crate::expressions::Identifier;

    match expr {
        Expression::Alias(ref mut alias) => {
            // If the alias name is empty and there are column_aliases, add auto-generated name
            if alias.alias.name.is_empty() && !alias.column_aliases.is_empty() {
                alias.alias = Identifier::new(format!("_t{}", counter));
                *counter += 1;
            }
        }
        _ => {}
    }
}

/// Convert BigQuery-style UNNEST aliases to column-alias format for DuckDB/Presto/Spark.
///
/// BigQuery uses: `UNNEST(arr) AS x` where x is a column alias.
/// DuckDB/Presto/Spark need: `UNNEST(arr) AS _t0(x)` where _t0 is a table alias and x is the column alias.
///
/// Propagate struct field names from the first named struct in an array to subsequent unnamed structs.
///
/// In BigQuery, `[STRUCT('Alice' AS name, 85 AS score), STRUCT('Bob', 92)]` means the second struct
/// should inherit field names from the first: `[STRUCT('Alice' AS name, 85 AS score), STRUCT('Bob' AS name, 92 AS score)]`.
pub fn propagate_struct_field_names(expr: Expression) -> Result<Expression> {
    use crate::dialects::transform_recursive;
    transform_recursive(expr, &propagate_struct_names_in_expr)
}

fn propagate_struct_names_in_expr(expr: Expression) -> Result<Expression> {
    use crate::expressions::{Alias, ArrayConstructor, Function, Identifier};

    /// Helper to propagate struct field names within an array of expressions
    fn propagate_in_elements(elements: &[Expression]) -> Option<Vec<Expression>> {
        if elements.len() <= 1 {
            return None;
        }
        // Check if first element is a named STRUCT function
        if let Some(Expression::Function(ref first_struct)) = elements.first() {
            if first_struct.name.eq_ignore_ascii_case("STRUCT") {
                // Extract field names from first struct
                let field_names: Vec<Option<String>> = first_struct
                    .args
                    .iter()
                    .map(|arg| {
                        if let Expression::Alias(a) = arg {
                            Some(a.alias.name.clone())
                        } else {
                            None
                        }
                    })
                    .collect();

                // Only propagate if first struct has at least one named field
                if field_names.iter().any(|n| n.is_some()) {
                    let mut new_elements = Vec::with_capacity(elements.len());
                    new_elements.push(elements[0].clone());

                    for elem in &elements[1..] {
                        if let Expression::Function(ref s) = elem {
                            if s.name.eq_ignore_ascii_case("STRUCT")
                                && s.args.len() == field_names.len()
                            {
                                // Check if this struct has NO names (all unnamed)
                                let all_unnamed =
                                    s.args.iter().all(|a| !matches!(a, Expression::Alias(_)));
                                if all_unnamed {
                                    // Apply names from first struct
                                    let new_args: Vec<Expression> = s
                                        .args
                                        .iter()
                                        .zip(field_names.iter())
                                        .map(|(val, name)| {
                                            if let Some(n) = name {
                                                Expression::Alias(Box::new(Alias::new(
                                                    val.clone(),
                                                    Identifier::new(n.clone()),
                                                )))
                                            } else {
                                                val.clone()
                                            }
                                        })
                                        .collect();
                                    new_elements.push(Expression::Function(Box::new(
                                        Function::new("STRUCT".to_string(), new_args),
                                    )));
                                    continue;
                                }
                            }
                        }
                        new_elements.push(elem.clone());
                    }

                    return Some(new_elements);
                }
            }
        }
        None
    }

    // Look for Array expressions containing STRUCT function calls
    if let Expression::Array(ref arr) = expr {
        if let Some(new_elements) = propagate_in_elements(&arr.expressions) {
            return Ok(Expression::Array(Box::new(crate::expressions::Array {
                expressions: new_elements,
            })));
        }
    }

    // Also handle ArrayFunc (ArrayConstructor) - bracket notation [STRUCT(...), ...]
    if let Expression::ArrayFunc(ref arr) = expr {
        if let Some(new_elements) = propagate_in_elements(&arr.expressions) {
            return Ok(Expression::ArrayFunc(Box::new(ArrayConstructor {
                expressions: new_elements,
                bracket_notation: arr.bracket_notation,
                use_list_keyword: arr.use_list_keyword,
            })));
        }
    }

    Ok(expr)
}

/// This walks the entire expression tree to find SELECT statements and converts UNNEST aliases
/// in their FROM clauses and JOINs.
pub fn unnest_alias_to_column_alias(expr: Expression) -> Result<Expression> {
    use crate::dialects::transform_recursive;
    transform_recursive(expr, &unnest_alias_transform_single_select)
}

/// Move UNNEST items from FROM clause to CROSS JOINs without changing alias format.
/// Used for BigQuery -> BigQuery/Redshift where we want CROSS JOIN but not _t0(col) aliases.
pub fn unnest_from_to_cross_join(expr: Expression) -> Result<Expression> {
    use crate::dialects::transform_recursive;
    transform_recursive(expr, &unnest_from_to_cross_join_single_select)
}

fn unnest_from_to_cross_join_single_select(expr: Expression) -> Result<Expression> {
    if let Expression::Select(mut select) = expr {
        if let Some(ref mut from) = select.from {
            if from.expressions.len() > 1 {
                let mut new_from_exprs = Vec::new();
                let mut new_cross_joins = Vec::new();

                for (idx, from_item) in from.expressions.drain(..).enumerate() {
                    if idx == 0 {
                        new_from_exprs.push(from_item);
                    } else {
                        let is_unnest = match &from_item {
                            Expression::Unnest(_) => true,
                            Expression::Alias(a) => matches!(a.this, Expression::Unnest(_)),
                            _ => false,
                        };

                        if is_unnest {
                            new_cross_joins.push(crate::expressions::Join {
                                this: from_item,
                                on: None,
                                using: Vec::new(),
                                kind: JoinKind::Cross,
                                use_inner_keyword: false,
                                use_outer_keyword: false,
                                deferred_condition: false,
                                join_hint: None,
                                match_condition: None,
                                pivots: Vec::new(),
                                comments: Vec::new(),
                                nesting_group: 0,
                                directed: false,
                            });
                        } else {
                            new_from_exprs.push(from_item);
                        }
                    }
                }

                from.expressions = new_from_exprs;
                new_cross_joins.append(&mut select.joins);
                select.joins = new_cross_joins;
            }
        }

        Ok(Expression::Select(select))
    } else {
        Ok(expr)
    }
}

/// Wrap UNNEST function aliases in JOIN items from `AS name` to `AS _u(name)`
/// Used for PostgreSQL → Presto/Trino transpilation where GENERATE_SERIES is
/// converted to UNNEST(SEQUENCE) and the alias needs the column-alias format.
pub fn wrap_unnest_join_aliases(expr: Expression) -> Result<Expression> {
    use crate::dialects::transform_recursive;
    transform_recursive(expr, &wrap_unnest_join_aliases_single)
}

fn wrap_unnest_join_aliases_single(expr: Expression) -> Result<Expression> {
    if let Expression::Select(mut select) = expr {
        // Process JOIN items
        for join in &mut select.joins {
            wrap_unnest_alias_in_join_item(&mut join.this);
        }
        Ok(Expression::Select(select))
    } else {
        Ok(expr)
    }
}

/// If a join item is an Alias wrapping an UNNEST function, convert alias to _u(alias_name) format
fn wrap_unnest_alias_in_join_item(expr: &mut Expression) {
    use crate::expressions::Identifier;
    if let Expression::Alias(alias) = expr {
        // Check if the inner expression is a function call to UNNEST
        let is_unnest = match &alias.this {
            Expression::Function(f) => f.name.eq_ignore_ascii_case("UNNEST"),
            _ => false,
        };

        if is_unnest && alias.column_aliases.is_empty() {
            // Simple alias like `AS s` -> wrap to `AS _u(s)`
            let original_alias_name = alias.alias.name.clone();
            alias.alias = Identifier {
                name: "_u".to_string(),
                quoted: false,
                trailing_comments: Vec::new(),
                span: None,
            };
            alias.column_aliases = vec![Identifier {
                name: original_alias_name,
                quoted: false,
                trailing_comments: Vec::new(),
                span: None,
            }];
        }
    }
}

fn unnest_alias_transform_single_select(expr: Expression) -> Result<Expression> {
    if let Expression::Select(mut select) = expr {
        let mut counter = 0usize;

        // Process FROM expressions: convert aliases AND move UNNEST items to CROSS JOIN
        if let Some(ref mut from) = select.from {
            // First pass: convert aliases in-place
            for from_item in from.expressions.iter_mut() {
                convert_unnest_alias_in_from(from_item, &mut counter);
            }

            // Second pass: move UNNEST items from FROM to CROSS JOINs
            if from.expressions.len() > 1 {
                let mut new_from_exprs = Vec::new();
                let mut new_cross_joins = Vec::new();

                for (idx, from_item) in from.expressions.drain(..).enumerate() {
                    if idx == 0 {
                        // First expression always stays in FROM
                        new_from_exprs.push(from_item);
                    } else {
                        // Check if this is UNNEST or Alias(UNNEST)
                        let is_unnest = match &from_item {
                            Expression::Unnest(_) => true,
                            Expression::Alias(a) => matches!(a.this, Expression::Unnest(_)),
                            _ => false,
                        };

                        if is_unnest {
                            // Convert to CROSS JOIN
                            new_cross_joins.push(crate::expressions::Join {
                                this: from_item,
                                on: None,
                                using: Vec::new(),
                                kind: JoinKind::Cross,
                                use_inner_keyword: false,
                                use_outer_keyword: false,
                                deferred_condition: false,
                                join_hint: None,
                                match_condition: None,
                                pivots: Vec::new(),
                                comments: Vec::new(),
                                nesting_group: 0,
                                directed: false,
                            });
                        } else {
                            // Keep non-UNNEST items in FROM
                            new_from_exprs.push(from_item);
                        }
                    }
                }

                from.expressions = new_from_exprs;
                // Prepend cross joins before existing joins
                new_cross_joins.append(&mut select.joins);
                select.joins = new_cross_joins;
            }
        }

        // Process JOINs (existing joins that may have UNNEST aliases)
        for join in select.joins.iter_mut() {
            convert_unnest_alias_in_from(&mut join.this, &mut counter);
        }

        Ok(Expression::Select(select))
    } else {
        Ok(expr)
    }
}

fn convert_unnest_alias_in_from(expr: &mut Expression, counter: &mut usize) {
    use crate::expressions::Identifier;

    if let Expression::Alias(ref mut alias) = expr {
        // Check if the inner expression is UNNEST (or EXPLODE)
        let is_unnest = matches!(&alias.this, Expression::Unnest(_))
            || matches!(&alias.this, Expression::Function(f) if f.name.eq_ignore_ascii_case("EXPLODE"));

        if is_unnest && alias.column_aliases.is_empty() {
            // Convert: UNNEST(arr) AS x -> UNNEST(arr) AS _tN(x)
            let col_alias = alias.alias.clone();
            alias.column_aliases = vec![col_alias];
            alias.alias = Identifier::new(format!("_t{}", counter));
            *counter += 1;
        }
    }
}

/// Expand POSEXPLODE in SELECT expressions for DuckDB.
///
/// Converts `SELECT POSEXPLODE(x)` to `SELECT GENERATE_SUBSCRIPTS(x, 1) - 1 AS pos, UNNEST(x) AS col`
/// Handles both aliased and unaliased forms:
/// - `SELECT POSEXPLODE(x) AS (a, b)` -> `SELECT GENERATE_SUBSCRIPTS(x, 1) - 1 AS a, UNNEST(x) AS b`
/// - `SELECT * FROM POSEXPLODE(x) AS (a, b)` -> `SELECT * FROM (SELECT GENERATE_SUBSCRIPTS(x, 1) - 1 AS a, UNNEST(x) AS b)`
pub fn expand_posexplode_duckdb(expr: Expression) -> Result<Expression> {
    use crate::expressions::{Alias, Function};

    match expr {
        Expression::Select(mut select) => {
            // Check if any SELECT expression is a POSEXPLODE function
            let mut new_expressions = Vec::new();
            let mut changed = false;

            for sel_expr in select.expressions.drain(..) {
                // Check for POSEXPLODE(x) AS (a, b) - aliased form
                if let Expression::Alias(ref alias_box) = sel_expr {
                    if let Expression::Function(ref func) = alias_box.this {
                        if func.name.eq_ignore_ascii_case("POSEXPLODE") && func.args.len() == 1 {
                            let arg = func.args[0].clone();
                            // Get alias names: default pos, col
                            let (pos_name, col_name) = if alias_box.column_aliases.len() == 2 {
                                (
                                    alias_box.column_aliases[0].name.clone(),
                                    alias_box.column_aliases[1].name.clone(),
                                )
                            } else if !alias_box.alias.is_empty() {
                                // Single alias like AS x - use as col name, "pos" for position
                                ("pos".to_string(), alias_box.alias.name.clone())
                            } else {
                                ("pos".to_string(), "col".to_string())
                            };

                            // GENERATE_SUBSCRIPTS(x, 1) - 1 AS pos_name
                            let gen_subscripts = Expression::Function(Box::new(Function::new(
                                "GENERATE_SUBSCRIPTS".to_string(),
                                vec![
                                    arg.clone(),
                                    Expression::Literal(Literal::Number("1".to_string())),
                                ],
                            )));
                            let sub_one = Expression::Sub(Box::new(BinaryOp::new(
                                gen_subscripts,
                                Expression::Literal(Literal::Number("1".to_string())),
                            )));
                            let pos_alias = Expression::Alias(Box::new(Alias {
                                this: sub_one,
                                alias: Identifier::new(pos_name),
                                column_aliases: Vec::new(),
                                pre_alias_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                            }));

                            // UNNEST(x) AS col_name
                            let unnest = Expression::Unnest(Box::new(UnnestFunc {
                                this: arg,
                                expressions: Vec::new(),
                                with_ordinality: false,
                                alias: None,
                                offset_alias: None,
                            }));
                            let col_alias = Expression::Alias(Box::new(Alias {
                                this: unnest,
                                alias: Identifier::new(col_name),
                                column_aliases: Vec::new(),
                                pre_alias_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                            }));

                            new_expressions.push(pos_alias);
                            new_expressions.push(col_alias);
                            changed = true;
                            continue;
                        }
                    }
                }

                // Check for bare POSEXPLODE(x) - unaliased form
                if let Expression::Function(ref func) = sel_expr {
                    if func.name.eq_ignore_ascii_case("POSEXPLODE") && func.args.len() == 1 {
                        let arg = func.args[0].clone();
                        let pos_name = "pos";
                        let col_name = "col";

                        // GENERATE_SUBSCRIPTS(x, 1) - 1 AS pos
                        let gen_subscripts = Expression::Function(Box::new(Function::new(
                            "GENERATE_SUBSCRIPTS".to_string(),
                            vec![
                                arg.clone(),
                                Expression::Literal(Literal::Number("1".to_string())),
                            ],
                        )));
                        let sub_one = Expression::Sub(Box::new(BinaryOp::new(
                            gen_subscripts,
                            Expression::Literal(Literal::Number("1".to_string())),
                        )));
                        let pos_alias = Expression::Alias(Box::new(Alias {
                            this: sub_one,
                            alias: Identifier::new(pos_name),
                            column_aliases: Vec::new(),
                            pre_alias_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                        }));

                        // UNNEST(x) AS col
                        let unnest = Expression::Unnest(Box::new(UnnestFunc {
                            this: arg,
                            expressions: Vec::new(),
                            with_ordinality: false,
                            alias: None,
                            offset_alias: None,
                        }));
                        let col_alias = Expression::Alias(Box::new(Alias {
                            this: unnest,
                            alias: Identifier::new(col_name),
                            column_aliases: Vec::new(),
                            pre_alias_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                        }));

                        new_expressions.push(pos_alias);
                        new_expressions.push(col_alias);
                        changed = true;
                        continue;
                    }
                }

                // Not a POSEXPLODE, keep as-is
                new_expressions.push(sel_expr);
            }

            if changed {
                select.expressions = new_expressions;
            } else {
                select.expressions = new_expressions;
            }

            // Also handle POSEXPLODE in FROM clause:
            // SELECT * FROM POSEXPLODE(x) AS (a, b) -> SELECT * FROM (SELECT ...)
            if let Some(ref mut from) = select.from {
                expand_posexplode_in_from_duckdb(from)?;
            }

            Ok(Expression::Select(select))
        }
        other => Ok(other),
    }
}

/// Helper to expand POSEXPLODE in FROM clause for DuckDB
fn expand_posexplode_in_from_duckdb(from: &mut From) -> Result<()> {
    use crate::expressions::{Alias, Function};

    let mut new_expressions = Vec::new();
    let mut _changed = false;

    for table_expr in from.expressions.drain(..) {
        // Check for POSEXPLODE(x) AS (a, b) in FROM
        if let Expression::Alias(ref alias_box) = table_expr {
            if let Expression::Function(ref func) = alias_box.this {
                if func.name.eq_ignore_ascii_case("POSEXPLODE") && func.args.len() == 1 {
                    let arg = func.args[0].clone();
                    let (pos_name, col_name) = if alias_box.column_aliases.len() == 2 {
                        (
                            alias_box.column_aliases[0].name.clone(),
                            alias_box.column_aliases[1].name.clone(),
                        )
                    } else {
                        ("pos".to_string(), "col".to_string())
                    };

                    // Create subquery: (SELECT GENERATE_SUBSCRIPTS(x, 1) - 1 AS a, UNNEST(x) AS b)
                    let gen_subscripts = Expression::Function(Box::new(Function::new(
                        "GENERATE_SUBSCRIPTS".to_string(),
                        vec![
                            arg.clone(),
                            Expression::Literal(Literal::Number("1".to_string())),
                        ],
                    )));
                    let sub_one = Expression::Sub(Box::new(BinaryOp::new(
                        gen_subscripts,
                        Expression::Literal(Literal::Number("1".to_string())),
                    )));
                    let pos_alias = Expression::Alias(Box::new(Alias {
                        this: sub_one,
                        alias: Identifier::new(&pos_name),
                        column_aliases: Vec::new(),
                        pre_alias_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    }));
                    let unnest = Expression::Unnest(Box::new(UnnestFunc {
                        this: arg,
                        expressions: Vec::new(),
                        with_ordinality: false,
                        alias: None,
                        offset_alias: None,
                    }));
                    let col_alias = Expression::Alias(Box::new(Alias {
                        this: unnest,
                        alias: Identifier::new(&col_name),
                        column_aliases: Vec::new(),
                        pre_alias_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    }));

                    let mut inner_select = Select::new();
                    inner_select.expressions = vec![pos_alias, col_alias];

                    let subquery = Expression::Subquery(Box::new(Subquery {
                        this: Expression::Select(Box::new(inner_select)),
                        alias: None,
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
                    }));
                    new_expressions.push(subquery);
                    _changed = true;
                    continue;
                }
            }
        }

        // Also check for bare POSEXPLODE(x) in FROM (no alias)
        if let Expression::Function(ref func) = table_expr {
            if func.name.eq_ignore_ascii_case("POSEXPLODE") && func.args.len() == 1 {
                let arg = func.args[0].clone();

                // Create subquery: (SELECT GENERATE_SUBSCRIPTS(x, 1) - 1 AS pos, UNNEST(x) AS col)
                let gen_subscripts = Expression::Function(Box::new(Function::new(
                    "GENERATE_SUBSCRIPTS".to_string(),
                    vec![
                        arg.clone(),
                        Expression::Literal(Literal::Number("1".to_string())),
                    ],
                )));
                let sub_one = Expression::Sub(Box::new(BinaryOp::new(
                    gen_subscripts,
                    Expression::Literal(Literal::Number("1".to_string())),
                )));
                let pos_alias = Expression::Alias(Box::new(Alias {
                    this: sub_one,
                    alias: Identifier::new("pos"),
                    column_aliases: Vec::new(),
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));
                let unnest = Expression::Unnest(Box::new(UnnestFunc {
                    this: arg,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                }));
                let col_alias = Expression::Alias(Box::new(Alias {
                    this: unnest,
                    alias: Identifier::new("col"),
                    column_aliases: Vec::new(),
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));

                let mut inner_select = Select::new();
                inner_select.expressions = vec![pos_alias, col_alias];

                let subquery = Expression::Subquery(Box::new(Subquery {
                    this: Expression::Select(Box::new(inner_select)),
                    alias: None,
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
                }));
                new_expressions.push(subquery);
                _changed = true;
                continue;
            }
        }

        new_expressions.push(table_expr);
    }

    from.expressions = new_expressions;
    Ok(())
}

/// Convert EXPLODE/POSEXPLODE in SELECT projections into CROSS JOIN UNNEST patterns.
///
/// This implements the `explode_projection_to_unnest` transform from Python sqlglot.
/// It restructures queries like:
///   `SELECT EXPLODE(x) FROM tbl`
/// into:
///   `SELECT IF(pos = pos_2, col, NULL) AS col FROM tbl CROSS JOIN UNNEST(...) AS pos CROSS JOIN UNNEST(x) AS col WITH OFFSET AS pos_2 WHERE ...`
///
/// The transform handles:
/// - EXPLODE(x) and POSEXPLODE(x) functions
/// - Name collision avoidance (_u, _u_2, ... and col, col_2, ...)
/// - Multiple EXPLODE/POSEXPLODE in one SELECT
/// - Queries with or without FROM clause
/// - Presto (index_offset=1) and BigQuery (index_offset=0) variants
pub fn explode_projection_to_unnest(expr: Expression, target: DialectType) -> Result<Expression> {
    match expr {
        Expression::Select(select) => explode_projection_to_unnest_impl(*select, target),
        other => Ok(other),
    }
}

/// Snowflake-specific rewrite to mirror Python sqlglot's explode_projection_to_unnest behavior
/// when FLATTEN appears in a nested LATERAL within a SELECT projection.
///
/// This intentionally rewrites:
/// - `LATERAL FLATTEN(INPUT => x) alias`
/// into:
/// - `LATERAL IFF(_u.pos = _u_2.pos_2, _u_2.entity, NULL) AS alias(SEQ, KEY, PATH, INDEX, VALUE, THIS)`
/// and appends CROSS JOIN TABLE(FLATTEN(...)) range/entity joins plus alignment predicates
/// to the containing SELECT.
pub fn snowflake_flatten_projection_to_unnest(expr: Expression) -> Result<Expression> {
    match expr {
        Expression::Select(select) => snowflake_flatten_projection_to_unnest_impl(*select),
        other => Ok(other),
    }
}

fn snowflake_flatten_projection_to_unnest_impl(mut select: Select) -> Result<Expression> {
    let mut flattened_inputs: Vec<Expression> = Vec::new();
    let mut new_selects: Vec<Expression> = Vec::with_capacity(select.expressions.len());

    for sel_expr in select.expressions.into_iter() {
        let found_input: RefCell<Option<Expression>> = RefCell::new(None);

        let rewritten = transform_recursive(sel_expr, &|e| {
            if let Expression::Lateral(lat) = e {
                if let Some(input_expr) = extract_flatten_input(&lat) {
                    if found_input.borrow().is_none() {
                        *found_input.borrow_mut() = Some(input_expr);
                    }
                    return Ok(Expression::Lateral(Box::new(rewrite_flatten_lateral(*lat))));
                }
                return Ok(Expression::Lateral(lat));
            }
            Ok(e)
        })?;

        if let Some(input) = found_input.into_inner() {
            flattened_inputs.push(input);
        }
        new_selects.push(rewritten);
    }

    if flattened_inputs.is_empty() {
        select.expressions = new_selects;
        return Ok(Expression::Select(Box::new(select)));
    }

    select.expressions = new_selects;

    for (idx, input_expr) in flattened_inputs.into_iter().enumerate() {
        // Match sqlglot naming: first pair is _u/_u_2 with pos/pos_2 and entity.
        let is_first = idx == 0;
        let series_alias = if is_first {
            "pos".to_string()
        } else {
            format!("pos_{}", idx + 1)
        };
        let series_source_alias = if is_first {
            "_u".to_string()
        } else {
            format!("_u_{}", idx * 2 + 1)
        };
        let unnest_source_alias = if is_first {
            "_u_2".to_string()
        } else {
            format!("_u_{}", idx * 2 + 2)
        };
        let pos2_alias = if is_first {
            "pos_2".to_string()
        } else {
            format!("{}_2", series_alias)
        };
        let entity_alias = if is_first {
            "entity".to_string()
        } else {
            format!("entity_{}", idx + 1)
        };

        let array_size_call = Expression::Function(Box::new(Function::new(
            "ARRAY_SIZE".to_string(),
            vec![Expression::NamedArgument(Box::new(NamedArgument {
                name: Identifier::new("INPUT"),
                value: input_expr.clone(),
                separator: NamedArgSeparator::DArrow,
            }))],
        )));

        let greatest = Expression::Function(Box::new(Function::new(
            "GREATEST".to_string(),
            vec![array_size_call.clone()],
        )));

        let series_end = Expression::Add(Box::new(BinaryOp::new(
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: Expression::Sub(Box::new(BinaryOp::new(
                    greatest,
                    Expression::Literal(Literal::Number("1".to_string())),
                ))),
                trailing_comments: Vec::new(),
            })),
            Expression::Literal(Literal::Number("1".to_string())),
        )));

        let series_range = Expression::Function(Box::new(Function::new(
            "ARRAY_GENERATE_RANGE".to_string(),
            vec![
                Expression::Literal(Literal::Number("0".to_string())),
                series_end,
            ],
        )));

        let series_flatten = Expression::Function(Box::new(Function::new(
            "FLATTEN".to_string(),
            vec![Expression::NamedArgument(Box::new(NamedArgument {
                name: Identifier::new("INPUT"),
                value: series_range,
                separator: NamedArgSeparator::DArrow,
            }))],
        )));

        let series_table = Expression::Function(Box::new(Function::new(
            "TABLE".to_string(),
            vec![series_flatten],
        )));

        let series_alias_expr = Expression::Alias(Box::new(Alias {
            this: series_table,
            alias: Identifier::new(series_source_alias.clone()),
            column_aliases: vec![
                Identifier::new("seq"),
                Identifier::new("key"),
                Identifier::new("path"),
                Identifier::new("index"),
                Identifier::new(series_alias.clone()),
                Identifier::new("this"),
            ],
            pre_alias_comments: Vec::new(),
            trailing_comments: Vec::new(),
        }));

        select.joins.push(Join {
            this: series_alias_expr,
            on: None,
            using: Vec::new(),
            kind: JoinKind::Cross,
            use_inner_keyword: false,
            use_outer_keyword: false,
            deferred_condition: false,
            join_hint: None,
            match_condition: None,
            pivots: Vec::new(),
            comments: Vec::new(),
            nesting_group: 0,
            directed: false,
        });

        let entity_flatten = Expression::Function(Box::new(Function::new(
            "FLATTEN".to_string(),
            vec![Expression::NamedArgument(Box::new(NamedArgument {
                name: Identifier::new("INPUT"),
                value: input_expr.clone(),
                separator: NamedArgSeparator::DArrow,
            }))],
        )));

        let entity_table = Expression::Function(Box::new(Function::new(
            "TABLE".to_string(),
            vec![entity_flatten],
        )));

        let entity_alias_expr = Expression::Alias(Box::new(Alias {
            this: entity_table,
            alias: Identifier::new(unnest_source_alias.clone()),
            column_aliases: vec![
                Identifier::new("seq"),
                Identifier::new("key"),
                Identifier::new("path"),
                Identifier::new(pos2_alias.clone()),
                Identifier::new(entity_alias.clone()),
                Identifier::new("this"),
            ],
            pre_alias_comments: Vec::new(),
            trailing_comments: Vec::new(),
        }));

        select.joins.push(Join {
            this: entity_alias_expr,
            on: None,
            using: Vec::new(),
            kind: JoinKind::Cross,
            use_inner_keyword: false,
            use_outer_keyword: false,
            deferred_condition: false,
            join_hint: None,
            match_condition: None,
            pivots: Vec::new(),
            comments: Vec::new(),
            nesting_group: 0,
            directed: false,
        });

        let pos_col =
            Expression::qualified_column(series_source_alias.clone(), series_alias.clone());
        let pos2_col =
            Expression::qualified_column(unnest_source_alias.clone(), pos2_alias.clone());

        let eq = Expression::Eq(Box::new(BinaryOp::new(pos_col.clone(), pos2_col.clone())));
        let size_minus_1 = Expression::Paren(Box::new(crate::expressions::Paren {
            this: Expression::Sub(Box::new(BinaryOp::new(
                array_size_call,
                Expression::Literal(Literal::Number("1".to_string())),
            ))),
            trailing_comments: Vec::new(),
        }));
        let gt = Expression::Gt(Box::new(BinaryOp::new(pos_col, size_minus_1.clone())));
        let pos2_eq_size = Expression::Eq(Box::new(BinaryOp::new(pos2_col, size_minus_1)));
        let and_cond = Expression::And(Box::new(BinaryOp::new(gt, pos2_eq_size)));
        let or_cond = Expression::Or(Box::new(BinaryOp::new(
            eq,
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: and_cond,
                trailing_comments: Vec::new(),
            })),
        )));

        select.where_clause = Some(match select.where_clause.take() {
            Some(existing) => Where {
                this: Expression::And(Box::new(BinaryOp::new(existing.this, or_cond))),
            },
            None => Where { this: or_cond },
        });
    }

    Ok(Expression::Select(Box::new(select)))
}

fn extract_flatten_input(lat: &Lateral) -> Option<Expression> {
    let Expression::Function(f) = lat.this.as_ref() else {
        return None;
    };
    if !f.name.eq_ignore_ascii_case("FLATTEN") {
        return None;
    }

    for arg in &f.args {
        if let Expression::NamedArgument(na) = arg {
            if na.name.name.eq_ignore_ascii_case("INPUT") {
                return Some(na.value.clone());
            }
        }
    }
    f.args.first().cloned()
}

fn rewrite_flatten_lateral(mut lat: Lateral) -> Lateral {
    let cond = Expression::Eq(Box::new(BinaryOp::new(
        Expression::qualified_column("_u", "pos"),
        Expression::qualified_column("_u_2", "pos_2"),
    )));
    let true_expr = Expression::qualified_column("_u_2", "entity");
    let iff_expr = Expression::Function(Box::new(Function::new(
        "IFF".to_string(),
        vec![cond, true_expr, Expression::Null(crate::expressions::Null)],
    )));

    lat.this = Box::new(iff_expr);
    if lat.column_aliases.is_empty() {
        lat.column_aliases = vec![
            "SEQ".to_string(),
            "KEY".to_string(),
            "PATH".to_string(),
            "INDEX".to_string(),
            "VALUE".to_string(),
            "THIS".to_string(),
        ];
    }
    lat
}

/// Info about an EXPLODE/POSEXPLODE found in a SELECT projection
struct ExplodeInfo {
    /// The argument to EXPLODE/POSEXPLODE (the array expression)
    arg_sql: String,
    /// The alias for the exploded column
    explode_alias: String,
    /// The alias for the position column
    pos_alias: String,
    /// Source alias for this unnest (e.g., _u_2)
    unnest_source_alias: String,
}

fn explode_projection_to_unnest_impl(select: Select, target: DialectType) -> Result<Expression> {
    let is_presto = matches!(
        target,
        DialectType::Presto | DialectType::Trino | DialectType::Athena
    );
    let is_bigquery = matches!(target, DialectType::BigQuery);

    if !is_presto && !is_bigquery {
        return Ok(Expression::Select(Box::new(select)));
    }

    // Check if any SELECT projection contains EXPLODE or POSEXPLODE
    let has_explode = select.expressions.iter().any(|e| expr_contains_explode(e));
    if !has_explode {
        return Ok(Expression::Select(Box::new(select)));
    }

    // Collect taken names from existing SELECT expressions and FROM sources
    let mut taken_select_names = std::collections::HashSet::new();
    let mut taken_source_names = std::collections::HashSet::new();

    // Collect names from existing SELECT expressions (output names)
    for sel in &select.expressions {
        if let Some(name) = get_output_name(sel) {
            taken_select_names.insert(name);
        }
    }

    // Also add the explode arg name if it's a column reference
    for sel in &select.expressions {
        let explode_expr = find_explode_in_expr(sel);
        if let Some(arg) = explode_expr {
            if let Some(name) = get_output_name(&arg) {
                taken_select_names.insert(name);
            }
        }
    }

    // Collect source names from FROM clause
    if let Some(ref from) = select.from {
        for from_expr in &from.expressions {
            collect_source_names(from_expr, &mut taken_source_names);
        }
    }
    // Also collect from JOINs
    for join in &select.joins {
        collect_source_names(&join.this, &mut taken_source_names);
    }

    // Generate series alias
    let series_alias = new_name(&mut taken_select_names, "pos");

    // Generate series source alias
    let series_source_alias = new_name(&mut taken_source_names, "_u");

    // Get the target dialect for generating expression SQL
    let target_dialect = Dialect::get(target);

    // Process each SELECT expression, collecting explode info
    let mut explode_infos: Vec<ExplodeInfo> = Vec::new();
    let mut new_projections: Vec<String> = Vec::new();

    for (_idx, sel_expr) in select.expressions.iter().enumerate() {
        let explode_data = extract_explode_data(sel_expr);

        if let Some((is_posexplode, arg_expr, explicit_alias, explicit_pos_alias)) = explode_data {
            // Generate the argument SQL in target dialect
            let arg_sql = target_dialect
                .generate(&arg_expr)
                .unwrap_or_else(|_| "NULL".to_string());

            let unnest_source_alias = new_name(&mut taken_source_names, "_u");

            let explode_alias = if let Some(ref ea) = explicit_alias {
                // Use the explicit alias directly (it was explicitly specified by the user)
                // Remove from taken_select_names first to avoid false collision with itself
                taken_select_names.remove(ea.as_str());
                // Now check for collision with other names
                let name = new_name(&mut taken_select_names, ea);
                name
            } else {
                new_name(&mut taken_select_names, "col")
            };

            let pos_alias = if let Some(ref pa) = explicit_pos_alias {
                // Use the explicit pos alias directly
                taken_select_names.remove(pa.as_str());
                let name = new_name(&mut taken_select_names, pa);
                name
            } else {
                new_name(&mut taken_select_names, "pos")
            };

            // Build the IF projection
            if is_presto {
                // Presto: IF(_u.pos = _u_2.pos_2, _u_2.col) AS col
                let if_col = format!(
                    "IF({}.{} = {}.{}, {}.{}) AS {}",
                    series_source_alias,
                    series_alias,
                    unnest_source_alias,
                    pos_alias,
                    unnest_source_alias,
                    explode_alias,
                    explode_alias
                );
                new_projections.push(if_col);

                // For POSEXPLODE, also add the position projection
                if is_posexplode {
                    let if_pos = format!(
                        "IF({}.{} = {}.{}, {}.{}) AS {}",
                        series_source_alias,
                        series_alias,
                        unnest_source_alias,
                        pos_alias,
                        unnest_source_alias,
                        pos_alias,
                        pos_alias
                    );
                    new_projections.push(if_pos);
                }
            } else {
                // BigQuery: IF(pos = pos_2, col, NULL) AS col
                let if_col = format!(
                    "IF({} = {}, {}, NULL) AS {}",
                    series_alias, pos_alias, explode_alias, explode_alias
                );
                new_projections.push(if_col);

                // For POSEXPLODE, also add the position projection
                if is_posexplode {
                    let if_pos = format!(
                        "IF({} = {}, {}, NULL) AS {}",
                        series_alias, pos_alias, pos_alias, pos_alias
                    );
                    new_projections.push(if_pos);
                }
            }

            explode_infos.push(ExplodeInfo {
                arg_sql,
                explode_alias,
                pos_alias,
                unnest_source_alias,
            });
        } else {
            // Not an EXPLODE expression, generate as-is
            let sel_sql = target_dialect
                .generate(sel_expr)
                .unwrap_or_else(|_| "*".to_string());
            new_projections.push(sel_sql);
        }
    }

    if explode_infos.is_empty() {
        return Ok(Expression::Select(Box::new(select)));
    }

    // Build the FROM clause
    let mut from_parts: Vec<String> = Vec::new();

    // Existing FROM sources
    if let Some(ref from) = select.from {
        for from_expr in &from.expressions {
            let from_sql = target_dialect.generate(from_expr).unwrap_or_default();
            from_parts.push(from_sql);
        }
    }

    // Build the size expressions for the series generator
    let size_exprs: Vec<String> = explode_infos
        .iter()
        .map(|info| {
            if is_presto {
                format!("CARDINALITY({})", info.arg_sql)
            } else {
                format!("ARRAY_LENGTH({})", info.arg_sql)
            }
        })
        .collect();

    let greatest_arg = if size_exprs.len() == 1 {
        size_exprs[0].clone()
    } else {
        format!("GREATEST({})", size_exprs.join(", "))
    };

    // Build the series source
    // greatest_arg is already "GREATEST(...)" when multiple, or "CARDINALITY(x)" / "ARRAY_LENGTH(x)" when single
    let series_sql = if is_presto {
        // SEQUENCE(1, GREATEST(CARDINALITY(x))) for single, SEQUENCE(1, GREATEST(C(a), C(b))) for multiple
        if size_exprs.len() == 1 {
            format!(
                "UNNEST(SEQUENCE(1, GREATEST({}))) AS {}({})",
                greatest_arg, series_source_alias, series_alias
            )
        } else {
            // greatest_arg already has GREATEST(...) wrapper
            format!(
                "UNNEST(SEQUENCE(1, {})) AS {}({})",
                greatest_arg, series_source_alias, series_alias
            )
        }
    } else {
        // GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(x)) - 1) for single
        if size_exprs.len() == 1 {
            format!(
                "UNNEST(GENERATE_ARRAY(0, GREATEST({}) - 1)) AS {}",
                greatest_arg, series_alias
            )
        } else {
            // greatest_arg already has GREATEST(...) wrapper
            format!(
                "UNNEST(GENERATE_ARRAY(0, {} - 1)) AS {}",
                greatest_arg, series_alias
            )
        }
    };

    // Build CROSS JOIN UNNEST clauses
    // Always use Presto-style (WITH ORDINALITY) for the SQL string to parse,
    // then convert to BigQuery-style AST after parsing if needed
    let mut cross_joins: Vec<String> = Vec::new();

    for info in &explode_infos {
        // Always use WITH ORDINALITY syntax (which our parser handles)
        cross_joins.push(format!(
            "CROSS JOIN UNNEST({}) WITH ORDINALITY AS {}({}, {})",
            info.arg_sql, info.unnest_source_alias, info.explode_alias, info.pos_alias
        ));
    }

    // Build WHERE clause
    let mut where_conditions: Vec<String> = Vec::new();

    for info in &explode_infos {
        let size_expr = if is_presto {
            format!("CARDINALITY({})", info.arg_sql)
        } else {
            format!("ARRAY_LENGTH({})", info.arg_sql)
        };

        let cond = if is_presto {
            format!(
                "{series_src}.{series_al} = {unnest_src}.{pos_al} OR ({series_src}.{series_al} > {size} AND {unnest_src}.{pos_al} = {size})",
                series_src = series_source_alias,
                series_al = series_alias,
                unnest_src = info.unnest_source_alias,
                pos_al = info.pos_alias,
                size = size_expr
            )
        } else {
            format!(
                "{series_al} = {pos_al} OR ({series_al} > ({size} - 1) AND {pos_al} = ({size} - 1))",
                series_al = series_alias,
                pos_al = info.pos_alias,
                size = size_expr
            )
        };

        where_conditions.push(cond);
    }

    // Combine WHERE conditions with AND (wrapped in parens if multiple)
    let where_sql = if where_conditions.len() == 1 {
        where_conditions[0].clone()
    } else {
        where_conditions
            .iter()
            .map(|c| format!("({})", c))
            .collect::<Vec<_>>()
            .join(" AND ")
    };

    // Build the complete SQL
    let select_part = new_projections.join(", ");

    // FROM part: if there was no original FROM, the series becomes the FROM source
    let from_and_joins = if from_parts.is_empty() {
        // No original FROM: series is the FROM source, everything else is CROSS JOIN
        format!("FROM {} {}", series_sql, cross_joins.join(" "))
    } else {
        format!(
            "FROM {} {} {}",
            from_parts.join(", "),
            format!("CROSS JOIN {}", series_sql),
            cross_joins.join(" ")
        )
    };

    let full_sql = format!(
        "SELECT {} {} WHERE {}",
        select_part, from_and_joins, where_sql
    );

    // Parse the constructed SQL using the Generic dialect (which handles all SQL syntax)
    // We use Generic instead of the target dialect to avoid parser limitations
    let generic_dialect = Dialect::get(DialectType::Generic);
    let parsed = generic_dialect.parse(&full_sql);
    match parsed {
        Ok(mut stmts) if !stmts.is_empty() => {
            let mut result = stmts.remove(0);

            // For BigQuery, convert Presto-style UNNEST AST to BigQuery-style
            // Presto: Alias(Unnest(with_ordinality=true), alias=_u_N, column_aliases=[col, pos])
            // BigQuery: Unnest(with_ordinality=true, alias=col, offset_alias=pos) [no outer Alias]
            if is_bigquery {
                convert_unnest_presto_to_bigquery(&mut result);
            }

            Ok(result)
        }
        _ => {
            // If parsing fails, return the original expression unchanged
            Ok(Expression::Select(Box::new(select)))
        }
    }
}

/// Convert Presto-style UNNEST WITH ORDINALITY to BigQuery-style UNNEST WITH OFFSET in the AST.
/// Presto: Alias(Unnest(with_ordinality=true), alias=_u_N, column_aliases=[col, pos_N])
/// BigQuery: Unnest(with_ordinality=true, alias=col, offset_alias=pos_N)
fn convert_unnest_presto_to_bigquery(expr: &mut Expression) {
    match expr {
        Expression::Select(ref mut select) => {
            // Convert in FROM clause
            if let Some(ref mut from) = select.from {
                for from_item in from.expressions.iter_mut() {
                    convert_unnest_presto_to_bigquery(from_item);
                }
            }
            // Convert in JOINs
            for join in select.joins.iter_mut() {
                convert_unnest_presto_to_bigquery(&mut join.this);
            }
        }
        Expression::Alias(ref alias) => {
            // Check if this is Alias(Unnest(with_ordinality=true), ..., column_aliases=[col, pos])
            if let Expression::Unnest(ref unnest) = alias.this {
                if unnest.with_ordinality && alias.column_aliases.len() >= 2 {
                    let col_alias = alias.column_aliases[0].clone();
                    let pos_alias = alias.column_aliases[1].clone();
                    let mut new_unnest = unnest.as_ref().clone();
                    new_unnest.alias = Some(col_alias);
                    new_unnest.offset_alias = Some(pos_alias);
                    // Replace the Alias(Unnest) with just Unnest
                    *expr = Expression::Unnest(Box::new(new_unnest));
                }
            }
        }
        _ => {}
    }
}

/// Find a new name that doesn't conflict with existing names.
/// Tries `base`, then `base_2`, `base_3`, etc.
fn new_name(names: &mut std::collections::HashSet<String>, base: &str) -> String {
    if !names.contains(base) {
        names.insert(base.to_string());
        return base.to_string();
    }
    let mut i = 2;
    loop {
        let candidate = format!("{}_{}", base, i);
        if !names.contains(&candidate) {
            names.insert(candidate.clone());
            return candidate;
        }
        i += 1;
    }
}

/// Check if an expression contains EXPLODE or POSEXPLODE
fn expr_contains_explode(expr: &Expression) -> bool {
    match expr {
        Expression::Explode(_) => true,
        Expression::ExplodeOuter(_) => true,
        Expression::Function(f) => {
            let name = f.name.to_uppercase();
            name == "POSEXPLODE" || name == "POSEXPLODE_OUTER"
        }
        Expression::Alias(a) => expr_contains_explode(&a.this),
        _ => false,
    }
}

/// Find the EXPLODE/POSEXPLODE expression within a select item, return the arg
fn find_explode_in_expr(expr: &Expression) -> Option<Expression> {
    match expr {
        Expression::Explode(uf) => Some(uf.this.clone()),
        Expression::ExplodeOuter(uf) => Some(uf.this.clone()),
        Expression::Function(f) => {
            let name = f.name.to_uppercase();
            if (name == "POSEXPLODE" || name == "POSEXPLODE_OUTER") && !f.args.is_empty() {
                Some(f.args[0].clone())
            } else {
                None
            }
        }
        Expression::Alias(a) => find_explode_in_expr(&a.this),
        _ => None,
    }
}

/// Extract explode data from a SELECT expression.
/// Returns (is_posexplode, arg_expression, explicit_col_alias, explicit_pos_alias)
fn extract_explode_data(
    expr: &Expression,
) -> Option<(bool, Expression, Option<String>, Option<String>)> {
    match expr {
        // Bare EXPLODE(x) without alias
        Expression::Explode(uf) => Some((false, uf.this.clone(), None, None)),
        Expression::ExplodeOuter(uf) => Some((false, uf.this.clone(), None, None)),
        // Bare POSEXPLODE(x) without alias
        Expression::Function(f) => {
            let name = f.name.to_uppercase();
            if (name == "POSEXPLODE" || name == "POSEXPLODE_OUTER") && !f.args.is_empty() {
                Some((true, f.args[0].clone(), None, None))
            } else {
                None
            }
        }
        // Aliased: EXPLODE(x) AS col, or POSEXPLODE(x) AS (a, b)
        Expression::Alias(a) => {
            match &a.this {
                Expression::Explode(uf) => {
                    let alias = if !a.alias.is_empty() {
                        Some(a.alias.name.clone())
                    } else {
                        None
                    };
                    Some((false, uf.this.clone(), alias, None))
                }
                Expression::ExplodeOuter(uf) => {
                    let alias = if !a.alias.is_empty() {
                        Some(a.alias.name.clone())
                    } else {
                        None
                    };
                    Some((false, uf.this.clone(), alias, None))
                }
                Expression::Function(f) => {
                    let name = f.name.to_uppercase();
                    if (name == "POSEXPLODE" || name == "POSEXPLODE_OUTER") && !f.args.is_empty() {
                        // Check for column aliases: AS (a, b)
                        if a.column_aliases.len() == 2 {
                            let pos_alias = a.column_aliases[0].name.clone();
                            let col_alias = a.column_aliases[1].name.clone();
                            Some((true, f.args[0].clone(), Some(col_alias), Some(pos_alias)))
                        } else if !a.alias.is_empty() {
                            // Single alias: AS x
                            Some((true, f.args[0].clone(), Some(a.alias.name.clone()), None))
                        } else {
                            Some((true, f.args[0].clone(), None, None))
                        }
                    } else {
                        None
                    }
                }
                _ => None,
            }
        }
        _ => None,
    }
}

/// Get the output name of a SELECT expression
fn get_output_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Alias(a) => {
            if !a.alias.is_empty() {
                Some(a.alias.name.clone())
            } else {
                None
            }
        }
        Expression::Column(c) => Some(c.name.name.clone()),
        Expression::Identifier(id) => Some(id.name.clone()),
        _ => None,
    }
}

/// Collect source names from a FROM/JOIN expression
fn collect_source_names(expr: &Expression, names: &mut std::collections::HashSet<String>) {
    match expr {
        Expression::Alias(a) => {
            if !a.alias.is_empty() {
                names.insert(a.alias.name.clone());
            }
        }
        Expression::Subquery(s) => {
            if let Some(ref alias) = s.alias {
                names.insert(alias.name.clone());
            }
        }
        Expression::Table(t) => {
            if let Some(ref alias) = t.alias {
                names.insert(alias.name.clone());
            } else {
                names.insert(t.name.name.clone());
            }
        }
        Expression::Column(c) => {
            names.insert(c.name.name.clone());
        }
        Expression::Identifier(id) => {
            names.insert(id.name.clone());
        }
        _ => {}
    }
}

/// Strip UNNEST wrapping from column reference arguments for Redshift target.
/// BigQuery UNNEST(column_ref) -> Redshift: just column_ref
pub fn strip_unnest_column_refs(expr: Expression) -> Result<Expression> {
    use crate::dialects::transform_recursive;
    transform_recursive(expr, &strip_unnest_column_refs_single)
}

fn strip_unnest_column_refs_single(expr: Expression) -> Result<Expression> {
    if let Expression::Select(mut select) = expr {
        // Process JOINs (UNNEST items have been moved to joins by unnest_from_to_cross_join)
        for join in select.joins.iter_mut() {
            strip_unnest_from_expr(&mut join.this);
        }
        // Process FROM items too
        if let Some(ref mut from) = select.from {
            for from_item in from.expressions.iter_mut() {
                strip_unnest_from_expr(from_item);
            }
        }
        Ok(Expression::Select(select))
    } else {
        Ok(expr)
    }
}

/// If expr is Alias(UNNEST(column_ref), alias) where UNNEST arg is a column/dot path,
/// replace with Alias(column_ref, alias) to strip the UNNEST.
fn strip_unnest_from_expr(expr: &mut Expression) {
    if let Expression::Alias(ref mut alias) = expr {
        if let Expression::Unnest(ref unnest) = alias.this {
            let is_column_ref = matches!(&unnest.this, Expression::Column(_) | Expression::Dot(_));
            if is_column_ref {
                // Replace UNNEST(col_ref) with just col_ref
                let inner = unnest.this.clone();
                alias.this = inner;
            }
        }
    }
}

/// Wrap DuckDB UNNEST of struct arrays in (SELECT UNNEST(..., max_depth => 2)) subquery.
/// BigQuery UNNEST of struct arrays needs this wrapping for DuckDB to properly expand struct fields.
pub fn wrap_duckdb_unnest_struct(expr: Expression) -> Result<Expression> {
    use crate::dialects::transform_recursive;
    transform_recursive(expr, &wrap_duckdb_unnest_struct_single)
}

fn wrap_duckdb_unnest_struct_single(expr: Expression) -> Result<Expression> {
    if let Expression::Select(mut select) = expr {
        // Process FROM items
        if let Some(ref mut from) = select.from {
            for from_item in from.expressions.iter_mut() {
                try_wrap_unnest_in_subquery(from_item);
            }
        }

        // Process JOINs
        for join in select.joins.iter_mut() {
            try_wrap_unnest_in_subquery(&mut join.this);
        }

        Ok(Expression::Select(select))
    } else {
        Ok(expr)
    }
}

/// Check if an expression contains struct array elements that need DuckDB UNNEST wrapping.
fn is_struct_array_unnest_arg(expr: &Expression) -> bool {
    match expr {
        // Array literal containing struct elements
        Expression::Array(arr) => arr
            .expressions
            .iter()
            .any(|e| matches!(e, Expression::Struct(_))),
        Expression::ArrayFunc(arr) => arr
            .expressions
            .iter()
            .any(|e| matches!(e, Expression::Struct(_))),
        // CAST to struct array type, e.g. CAST([] AS STRUCT(x BIGINT)[])
        Expression::Cast(c) => {
            matches!(&c.to, DataType::Array { element_type, .. } if matches!(**element_type, DataType::Struct { .. }))
        }
        _ => false,
    }
}

/// Try to wrap an UNNEST expression in a (SELECT UNNEST(..., max_depth => 2)) subquery.
/// Handles both bare UNNEST and Alias(UNNEST).
fn try_wrap_unnest_in_subquery(expr: &mut Expression) {
    // Check for Alias wrapping UNNEST
    if let Expression::Alias(ref alias) = expr {
        if let Expression::Unnest(ref unnest) = alias.this {
            if is_struct_array_unnest_arg(&unnest.this) {
                let unnest_clone = (**unnest).clone();
                let alias_name = alias.alias.clone();
                let new_expr = make_unnest_subquery(unnest_clone, Some(alias_name));
                *expr = new_expr;
                return;
            }
        }
    }

    // Check for bare UNNEST
    if let Expression::Unnest(ref unnest) = expr {
        if is_struct_array_unnest_arg(&unnest.this) {
            let unnest_clone = (**unnest).clone();
            let new_expr = make_unnest_subquery(unnest_clone, None);
            *expr = new_expr;
        }
    }
}

/// Create (SELECT UNNEST(arg, max_depth => 2)) [AS alias] subquery.
fn make_unnest_subquery(unnest: UnnestFunc, alias: Option<Identifier>) -> Expression {
    // Build UNNEST function call with max_depth => 2 named argument
    let max_depth_arg = Expression::NamedArgument(Box::new(NamedArgument {
        name: Identifier::new("max_depth".to_string()),
        value: Expression::Literal(Literal::Number("2".to_string())),
        separator: NamedArgSeparator::DArrow,
    }));

    let mut unnest_args = vec![unnest.this];
    unnest_args.extend(unnest.expressions);
    unnest_args.push(max_depth_arg);

    let unnest_func =
        Expression::Function(Box::new(Function::new("UNNEST".to_string(), unnest_args)));

    // Build SELECT UNNEST(...)
    let mut inner_select = Select::new();
    inner_select.expressions = vec![unnest_func];
    let inner_select = Expression::Select(Box::new(inner_select));

    // Wrap in subquery
    let subquery = Subquery {
        this: inner_select,
        alias,
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
    };

    Expression::Subquery(Box::new(subquery))
}

/// Wrap UNION with ORDER BY/LIMIT in a subquery.
///
/// Some dialects (ClickHouse, TSQL) don't support ORDER BY/LIMIT directly on UNION.
/// This transform converts:
///   SELECT ... UNION SELECT ... ORDER BY x LIMIT n
/// to:
///   SELECT * FROM (SELECT ... UNION SELECT ...) AS _l_0 ORDER BY x LIMIT n
///
/// NOTE: Our parser may place ORDER BY/LIMIT on the right-hand SELECT rather than
/// the Union (unlike Python sqlglot). This function handles both cases by checking
/// the right-hand SELECT for trailing ORDER BY/LIMIT and moving them to the Union.
pub fn no_limit_order_by_union(expr: Expression) -> Result<Expression> {
    use crate::expressions::{Limit as LimitClause, Offset as OffsetClause, OrderBy, Star};

    match expr {
        Expression::Union(mut u) => {
            // Check if ORDER BY/LIMIT are on the rightmost Select instead of the Union
            // (our parser may attach them to the right SELECT)
            if u.order_by.is_none() && u.limit.is_none() && u.offset.is_none() {
                // Find the rightmost Select and check for ORDER BY/LIMIT
                if let Expression::Select(ref mut right_select) = u.right {
                    if right_select.order_by.is_some()
                        || right_select.limit.is_some()
                        || right_select.offset.is_some()
                    {
                        // Move ORDER BY/LIMIT from right Select to Union
                        u.order_by = right_select.order_by.take();
                        u.limit = right_select.limit.take().map(|l| Box::new(l.this));
                        u.offset = right_select.offset.take().map(|o| Box::new(o.this));
                    }
                }
            }

            let has_order_or_limit =
                u.order_by.is_some() || u.limit.is_some() || u.offset.is_some();
            if has_order_or_limit {
                // Extract ORDER BY, LIMIT, OFFSET from the Union
                let order_by: Option<OrderBy> = u.order_by.take();
                let union_limit: Option<Box<Expression>> = u.limit.take();
                let union_offset: Option<Box<Expression>> = u.offset.take();

                // Convert Union's limit (Box<Expression>) to Select's limit (Limit struct)
                let select_limit: Option<LimitClause> = union_limit.map(|l| LimitClause {
                    this: *l,
                    percent: false,
                    comments: Vec::new(),
                });

                // Convert Union's offset (Box<Expression>) to Select's offset (Offset struct)
                let select_offset: Option<OffsetClause> = union_offset.map(|o| OffsetClause {
                    this: *o,
                    rows: None,
                });

                // Create a subquery from the Union
                let subquery = Subquery {
                    this: Expression::Union(u),
                    alias: Some(Identifier::new("_l_0")),
                    column_aliases: Vec::new(),
                    lateral: false,
                    modifiers_inside: false,
                    order_by: None,
                    limit: None,
                    offset: None,
                    distribute_by: None,
                    sort_by: None,
                    cluster_by: None,
                    trailing_comments: Vec::new(),
                };

                // Build SELECT * FROM (UNION) AS _l_0 ORDER BY ... LIMIT ...
                let mut select = Select::default();
                select.expressions = vec![Expression::Star(Star {
                    table: None,
                    except: None,
                    replace: None,
                    rename: None,
                    trailing_comments: Vec::new(),
                    span: None,
                })];
                select.from = Some(From {
                    expressions: vec![Expression::Subquery(Box::new(subquery))],
                });
                select.order_by = order_by;
                select.limit = select_limit;
                select.offset = select_offset;

                Ok(Expression::Select(Box::new(select)))
            } else {
                Ok(Expression::Union(u))
            }
        }
        _ => Ok(expr),
    }
}

/// Expand LIKE ANY / ILIKE ANY to OR chains.
///
/// For dialects that don't support quantifiers on LIKE/ILIKE (e.g. DuckDB),
/// expand `x LIKE ANY (('a', 'b'))` to `x LIKE 'a' OR x LIKE 'b'`.
pub fn expand_like_any(expr: Expression) -> Result<Expression> {
    use crate::expressions::{BinaryOp, LikeOp};

    fn unwrap_parens(e: &Expression) -> &Expression {
        match e {
            Expression::Paren(p) => unwrap_parens(&p.this),
            _ => e,
        }
    }

    fn extract_tuple_values(e: &Expression) -> Option<Vec<Expression>> {
        let inner = unwrap_parens(e);
        match inner {
            Expression::Tuple(t) => Some(t.expressions.clone()),
            _ => None,
        }
    }

    transform_recursive(expr, &|e| {
        match e {
            Expression::Like(ref op) if op.quantifier.as_deref() == Some("ANY") => {
                if let Some(values) = extract_tuple_values(&op.right) {
                    if values.is_empty() {
                        return Ok(e);
                    }
                    // Build: left LIKE val1 OR left LIKE val2 OR ...
                    let mut result: Option<Expression> = None;
                    for val in values {
                        let like = Expression::Like(Box::new(LikeOp {
                            left: op.left.clone(),
                            right: val,
                            escape: op.escape.clone(),
                            quantifier: None,
                        }));
                        result = Some(match result {
                            None => like,
                            Some(prev) => Expression::Or(Box::new(BinaryOp::new(prev, like))),
                        });
                    }
                    Ok(result.unwrap_or(e))
                } else {
                    Ok(e)
                }
            }
            Expression::ILike(ref op) if op.quantifier.as_deref() == Some("ANY") => {
                if let Some(values) = extract_tuple_values(&op.right) {
                    if values.is_empty() {
                        return Ok(e);
                    }
                    let mut result: Option<Expression> = None;
                    for val in values {
                        let ilike = Expression::ILike(Box::new(LikeOp {
                            left: op.left.clone(),
                            right: val,
                            escape: op.escape.clone(),
                            quantifier: None,
                        }));
                        result = Some(match result {
                            None => ilike,
                            Some(prev) => Expression::Or(Box::new(BinaryOp::new(prev, ilike))),
                        });
                    }
                    Ok(result.unwrap_or(e))
                } else {
                    Ok(e)
                }
            }
            _ => Ok(e),
        }
    })
}

/// Ensures all unaliased column outputs in subqueries and CTEs get self-aliases.
///
/// This is needed for TSQL which requires derived table outputs to be aliased.
/// For example: `SELECT c FROM t` inside a subquery becomes `SELECT c AS c FROM t`.
///
/// Mirrors Python sqlglot's `qualify_derived_table_outputs` function which is applied
/// as a TRANSFORMS preprocessor for Subquery and CTE expressions in the TSQL dialect.
pub fn qualify_derived_table_outputs(expr: Expression) -> Result<Expression> {
    use crate::expressions::Alias;

    fn add_self_aliases_to_select(select: &mut Select) {
        let new_expressions: Vec<Expression> = select
            .expressions
            .iter()
            .map(|e| {
                match e {
                    // Column reference without alias -> add self-alias
                    Expression::Column(col) => {
                        let alias_name = col.name.clone();
                        Expression::Alias(Box::new(Alias {
                            this: e.clone(),
                            alias: alias_name,
                            column_aliases: Vec::new(),
                            pre_alias_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                        }))
                    }
                    // Already aliased or star or other -> keep as is
                    _ => e.clone(),
                }
            })
            .collect();
        select.expressions = new_expressions;
    }

    fn walk_and_qualify(expr: &mut Expression) {
        match expr {
            Expression::Select(ref mut select) => {
                // Qualify subqueries in FROM
                if let Some(ref mut from) = select.from {
                    for e in from.expressions.iter_mut() {
                        qualify_subquery_expr(e);
                        walk_and_qualify(e);
                    }
                }
                // Qualify subqueries in JOINs
                for join in select.joins.iter_mut() {
                    qualify_subquery_expr(&mut join.this);
                    walk_and_qualify(&mut join.this);
                }
                // Recurse into expressions (for correlated subqueries etc.)
                for e in select.expressions.iter_mut() {
                    walk_and_qualify(e);
                }
                // Recurse into WHERE
                if let Some(ref mut w) = select.where_clause {
                    walk_and_qualify(&mut w.this);
                }
            }
            Expression::Subquery(ref mut subquery) => {
                walk_and_qualify(&mut subquery.this);
            }
            Expression::Union(ref mut u) => {
                walk_and_qualify(&mut u.left);
                walk_and_qualify(&mut u.right);
            }
            Expression::Intersect(ref mut i) => {
                walk_and_qualify(&mut i.left);
                walk_and_qualify(&mut i.right);
            }
            Expression::Except(ref mut e) => {
                walk_and_qualify(&mut e.left);
                walk_and_qualify(&mut e.right);
            }
            Expression::Cte(ref mut cte) => {
                walk_and_qualify(&mut cte.this);
            }
            _ => {}
        }
    }

    fn qualify_subquery_expr(expr: &mut Expression) {
        match expr {
            Expression::Subquery(ref mut subquery) => {
                // Only qualify if the subquery has a table alias but no column aliases
                if subquery.alias.is_some() && subquery.column_aliases.is_empty() {
                    if let Expression::Select(ref mut inner_select) = subquery.this {
                        // Check the inner select doesn't use *
                        let has_star = inner_select
                            .expressions
                            .iter()
                            .any(|e| matches!(e, Expression::Star(_)));
                        if !has_star {
                            add_self_aliases_to_select(inner_select);
                        }
                    }
                }
                // Recurse into the subquery's inner query
                walk_and_qualify(&mut subquery.this);
            }
            Expression::Alias(ref mut alias) => {
                qualify_subquery_expr(&mut alias.this);
            }
            _ => {}
        }
    }

    let mut result = expr;
    walk_and_qualify(&mut result);

    // Also qualify CTE inner queries at the top level
    if let Expression::Select(ref mut select) = result {
        if let Some(ref mut with) = select.with {
            for cte in with.ctes.iter_mut() {
                // CTE with column names -> no need to qualify
                if cte.columns.is_empty() {
                    // Walk into the CTE's inner query for nested subqueries
                    walk_and_qualify(&mut cte.this);
                }
            }
        }
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::{Dialect, DialectType};
    use crate::expressions::Column;

    fn gen(expr: &Expression) -> String {
        let dialect = Dialect::get(DialectType::Generic);
        dialect.generate(expr).unwrap()
    }

    #[test]
    fn test_preprocess() {
        let expr = Expression::Boolean(BooleanLiteral { value: true });
        let result = preprocess(expr, &[replace_bool_with_int]).unwrap();
        assert!(matches!(result, Expression::Literal(Literal::Number(_))));
    }

    #[test]
    fn test_preprocess_chain() {
        // Test chaining multiple transforms using function pointers
        let expr = Expression::Boolean(BooleanLiteral { value: true });
        // Create array of function pointers (all same type)
        let transforms: Vec<fn(Expression) -> Result<Expression>> =
            vec![replace_bool_with_int, replace_int_with_bool];
        let result = preprocess(expr, &transforms).unwrap();
        // After replace_bool_with_int: 1
        // After replace_int_with_bool: true
        if let Expression::Boolean(b) = result {
            assert!(b.value);
        } else {
            panic!("Expected boolean literal");
        }
    }

    #[test]
    fn test_unnest_to_explode() {
        let unnest = Expression::Unnest(Box::new(UnnestFunc {
            this: Expression::Column(Column {
                name: Identifier::new("arr".to_string()),
                table: None,
                join_mark: false,
                trailing_comments: vec![],
                span: None,
            }),
            expressions: Vec::new(),
            with_ordinality: false,
            alias: None,
            offset_alias: None,
        }));

        let result = unnest_to_explode(unnest).unwrap();
        assert!(matches!(result, Expression::Explode(_)));
    }

    #[test]
    fn test_explode_to_unnest() {
        let explode = Expression::Explode(Box::new(UnaryFunc {
            this: Expression::Column(Column {
                name: Identifier::new("arr".to_string()),
                table: None,
                join_mark: false,
                trailing_comments: vec![],
                span: None,
            }),
            original_name: None,
        }));

        let result = explode_to_unnest(explode).unwrap();
        assert!(matches!(result, Expression::Unnest(_)));
    }

    #[test]
    fn test_replace_bool_with_int() {
        let true_expr = Expression::Boolean(BooleanLiteral { value: true });
        let result = replace_bool_with_int(true_expr).unwrap();
        if let Expression::Literal(Literal::Number(n)) = result {
            assert_eq!(n, "1");
        } else {
            panic!("Expected number literal");
        }

        let false_expr = Expression::Boolean(BooleanLiteral { value: false });
        let result = replace_bool_with_int(false_expr).unwrap();
        if let Expression::Literal(Literal::Number(n)) = result {
            assert_eq!(n, "0");
        } else {
            panic!("Expected number literal");
        }
    }

    #[test]
    fn test_replace_int_with_bool() {
        let one_expr = Expression::Literal(Literal::Number("1".to_string()));
        let result = replace_int_with_bool(one_expr).unwrap();
        if let Expression::Boolean(b) = result {
            assert!(b.value);
        } else {
            panic!("Expected boolean true");
        }

        let zero_expr = Expression::Literal(Literal::Number("0".to_string()));
        let result = replace_int_with_bool(zero_expr).unwrap();
        if let Expression::Boolean(b) = result {
            assert!(!b.value);
        } else {
            panic!("Expected boolean false");
        }

        // Test that other numbers are not converted
        let two_expr = Expression::Literal(Literal::Number("2".to_string()));
        let result = replace_int_with_bool(two_expr).unwrap();
        assert!(matches!(result, Expression::Literal(Literal::Number(_))));
    }

    #[test]
    fn test_strip_data_type_params() {
        // Test Decimal
        let decimal = DataType::Decimal {
            precision: Some(10),
            scale: Some(2),
        };
        let stripped = strip_data_type_params(decimal);
        assert_eq!(
            stripped,
            DataType::Decimal {
                precision: None,
                scale: None
            }
        );

        // Test VarChar
        let varchar = DataType::VarChar {
            length: Some(255),
            parenthesized_length: false,
        };
        let stripped = strip_data_type_params(varchar);
        assert_eq!(
            stripped,
            DataType::VarChar {
                length: None,
                parenthesized_length: false
            }
        );

        // Test Char
        let char_type = DataType::Char { length: Some(10) };
        let stripped = strip_data_type_params(char_type);
        assert_eq!(stripped, DataType::Char { length: None });

        // Test Timestamp (preserve timezone)
        let timestamp = DataType::Timestamp {
            precision: Some(6),
            timezone: true,
        };
        let stripped = strip_data_type_params(timestamp);
        assert_eq!(
            stripped,
            DataType::Timestamp {
                precision: None,
                timezone: true
            }
        );

        // Test Array (recursive)
        let array = DataType::Array {
            element_type: Box::new(DataType::VarChar {
                length: Some(100),
                parenthesized_length: false,
            }),
            dimension: None,
        };
        let stripped = strip_data_type_params(array);
        assert_eq!(
            stripped,
            DataType::Array {
                element_type: Box::new(DataType::VarChar {
                    length: None,
                    parenthesized_length: false
                }),
                dimension: None,
            }
        );

        // Test types without params are unchanged
        let text = DataType::Text;
        let stripped = strip_data_type_params(text);
        assert_eq!(stripped, DataType::Text);
    }

    #[test]
    fn test_remove_precision_parameterized_types_cast() {
        // Create a CAST(1 AS DECIMAL(10, 2)) expression
        let cast_expr = Expression::Cast(Box::new(Cast {
            this: Expression::Literal(Literal::Number("1".to_string())),
            to: DataType::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
        }));

        let result = remove_precision_parameterized_types(cast_expr).unwrap();
        if let Expression::Cast(cast) = result {
            assert_eq!(
                cast.to,
                DataType::Decimal {
                    precision: None,
                    scale: None
                }
            );
        } else {
            panic!("Expected Cast expression");
        }
    }

    #[test]
    fn test_remove_precision_parameterized_types_varchar() {
        // Create a CAST('hello' AS VARCHAR(10)) expression
        let cast_expr = Expression::Cast(Box::new(Cast {
            this: Expression::Literal(Literal::String("hello".to_string())),
            to: DataType::VarChar {
                length: Some(10),
                parenthesized_length: false,
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
        }));

        let result = remove_precision_parameterized_types(cast_expr).unwrap();
        if let Expression::Cast(cast) = result {
            assert_eq!(
                cast.to,
                DataType::VarChar {
                    length: None,
                    parenthesized_length: false
                }
            );
        } else {
            panic!("Expected Cast expression");
        }
    }

    #[test]
    fn test_remove_precision_direct_cast() {
        // Test transform on a direct Cast expression (not nested in Select)
        // The current implementation handles top-level Cast expressions;
        // a full implementation would need recursive AST traversal
        let cast = Expression::Cast(Box::new(Cast {
            this: Expression::Literal(Literal::Number("1".to_string())),
            to: DataType::Decimal {
                precision: Some(10),
                scale: Some(2),
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
        }));

        let transformed = remove_precision_parameterized_types(cast).unwrap();
        let generated = gen(&transformed);

        // Should now be DECIMAL without precision
        assert!(generated.contains("DECIMAL"));
        assert!(!generated.contains("(10"));
    }

    #[test]
    fn test_epoch_cast_to_ts() {
        // Test CAST('epoch' AS TIMESTAMP) → CAST('1970-01-01 00:00:00' AS TIMESTAMP)
        let cast_expr = Expression::Cast(Box::new(Cast {
            this: Expression::Literal(Literal::String("epoch".to_string())),
            to: DataType::Timestamp {
                precision: None,
                timezone: false,
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
        }));

        let result = epoch_cast_to_ts(cast_expr).unwrap();
        if let Expression::Cast(cast) = result {
            if let Expression::Literal(Literal::String(s)) = cast.this {
                assert_eq!(s, "1970-01-01 00:00:00");
            } else {
                panic!("Expected string literal");
            }
        } else {
            panic!("Expected Cast expression");
        }
    }

    #[test]
    fn test_epoch_cast_to_ts_preserves_non_epoch() {
        // Test that non-epoch strings are preserved
        let cast_expr = Expression::Cast(Box::new(Cast {
            this: Expression::Literal(Literal::String("2024-01-15".to_string())),
            to: DataType::Timestamp {
                precision: None,
                timezone: false,
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
        }));

        let result = epoch_cast_to_ts(cast_expr).unwrap();
        if let Expression::Cast(cast) = result {
            if let Expression::Literal(Literal::String(s)) = cast.this {
                assert_eq!(s, "2024-01-15");
            } else {
                panic!("Expected string literal");
            }
        } else {
            panic!("Expected Cast expression");
        }
    }

    #[test]
    fn test_unqualify_columns() {
        // Test that table qualifiers are removed
        let col = Expression::Column(Column {
            name: Identifier::new("id".to_string()),
            table: Some(Identifier::new("users".to_string())),
            join_mark: false,
            trailing_comments: vec![],
            span: None,
        });

        let result = unqualify_columns(col).unwrap();
        if let Expression::Column(c) = result {
            assert!(c.table.is_none());
            assert_eq!(c.name.name, "id");
        } else {
            panic!("Expected Column expression");
        }
    }

    #[test]
    fn test_is_temporal_type() {
        assert!(is_temporal_type(&DataType::Date));
        assert!(is_temporal_type(&DataType::Timestamp {
            precision: None,
            timezone: false
        }));
        assert!(is_temporal_type(&DataType::Time {
            precision: None,
            timezone: false
        }));
        assert!(!is_temporal_type(&DataType::Int {
            length: None,
            integer_spelling: false
        }));
        assert!(!is_temporal_type(&DataType::VarChar {
            length: None,
            parenthesized_length: false
        }));
    }

    #[test]
    fn test_eliminate_semi_join_basic() {
        use crate::expressions::{Join, TableRef};

        // Test that semi joins are converted to EXISTS
        let select = Expression::Select(Box::new(Select {
            expressions: vec![Expression::Column(Column {
                name: Identifier::new("a".to_string()),
                table: None,
                join_mark: false,
                trailing_comments: vec![],
                span: None,
            })],
            from: Some(From {
                expressions: vec![Expression::Table(TableRef::new("t1"))],
            }),
            joins: vec![Join {
                this: Expression::Table(TableRef::new("t2")),
                kind: JoinKind::Semi,
                on: Some(Expression::Eq(Box::new(BinaryOp {
                    left: Expression::Column(Column {
                        name: Identifier::new("x".to_string()),
                        table: None,
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                    }),
                    right: Expression::Column(Column {
                        name: Identifier::new("y".to_string()),
                        table: None,
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                    }),
                    left_comments: vec![],
                    operator_comments: vec![],
                    trailing_comments: vec![],
                }))),
                using: vec![],
                use_inner_keyword: false,
                use_outer_keyword: false,
                deferred_condition: false,
                join_hint: None,
                match_condition: None,
                pivots: Vec::new(),
                comments: Vec::new(),
                nesting_group: 0,
                directed: false,
            }],
            ..Select::new()
        }));

        let result = eliminate_semi_and_anti_joins(select).unwrap();
        if let Expression::Select(s) = result {
            // Semi join should be removed
            assert!(s.joins.is_empty());
            // WHERE clause should have EXISTS
            assert!(s.where_clause.is_some());
        } else {
            panic!("Expected Select expression");
        }
    }

    #[test]
    fn test_no_ilike_sql() {
        use crate::expressions::LikeOp;

        // Test ILIKE conversion to LOWER+LIKE
        let ilike_expr = Expression::ILike(Box::new(LikeOp {
            left: Expression::Column(Column {
                name: Identifier::new("name".to_string()),
                table: None,
                join_mark: false,
                trailing_comments: vec![],
                span: None,
            }),
            right: Expression::Literal(Literal::String("%test%".to_string())),
            escape: None,
            quantifier: None,
        }));

        let result = no_ilike_sql(ilike_expr).unwrap();
        if let Expression::Like(like) = result {
            // Left should be LOWER(name)
            if let Expression::Function(f) = &like.left {
                assert_eq!(f.name, "LOWER");
            } else {
                panic!("Expected LOWER function on left");
            }
            // Right should be LOWER('%test%')
            if let Expression::Function(f) = &like.right {
                assert_eq!(f.name, "LOWER");
            } else {
                panic!("Expected LOWER function on right");
            }
        } else {
            panic!("Expected Like expression");
        }
    }

    #[test]
    fn test_no_trycast_sql() {
        // Test TryCast conversion to Cast
        let trycast_expr = Expression::TryCast(Box::new(Cast {
            this: Expression::Literal(Literal::String("123".to_string())),
            to: DataType::Int {
                length: None,
                integer_spelling: false,
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
        }));

        let result = no_trycast_sql(trycast_expr).unwrap();
        assert!(matches!(result, Expression::Cast(_)));
    }

    #[test]
    fn test_no_safe_cast_sql() {
        // Test SafeCast conversion to Cast
        let safe_cast_expr = Expression::SafeCast(Box::new(Cast {
            this: Expression::Literal(Literal::String("123".to_string())),
            to: DataType::Int {
                length: None,
                integer_spelling: false,
            },
            trailing_comments: vec![],
            double_colon_syntax: false,
            format: None,
            default: None,
        }));

        let result = no_safe_cast_sql(safe_cast_expr).unwrap();
        assert!(matches!(result, Expression::Cast(_)));
    }

    #[test]
    fn test_explode_to_unnest_presto() {
        let spark = Dialect::get(DialectType::Spark);
        let result = spark
            .transpile_to("SELECT EXPLODE(x) FROM tbl", DialectType::Presto)
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT IF(_u.pos = _u_2.pos_2, _u_2.col) AS col FROM tbl CROSS JOIN UNNEST(SEQUENCE(1, GREATEST(CARDINALITY(x)))) AS _u(pos) CROSS JOIN UNNEST(x) WITH ORDINALITY AS _u_2(col, pos_2) WHERE _u.pos = _u_2.pos_2 OR (_u.pos > CARDINALITY(x) AND _u_2.pos_2 = CARDINALITY(x))"
        );
    }

    #[test]
    fn test_explode_to_unnest_bigquery() {
        let spark = Dialect::get(DialectType::Spark);
        let result = spark
            .transpile_to("SELECT EXPLODE(x) FROM tbl", DialectType::BigQuery)
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT IF(pos = pos_2, col, NULL) AS col FROM tbl CROSS JOIN UNNEST(GENERATE_ARRAY(0, GREATEST(ARRAY_LENGTH(x)) - 1)) AS pos CROSS JOIN UNNEST(x) AS col WITH OFFSET AS pos_2 WHERE pos = pos_2 OR (pos > (ARRAY_LENGTH(x) - 1) AND pos_2 = (ARRAY_LENGTH(x) - 1))"
        );
    }
}
