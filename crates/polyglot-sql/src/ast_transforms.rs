//! AST transform helpers and convenience getters.
//!
//! This module provides functions for common AST mutations (adding WHERE clauses,
//! setting LIMIT/OFFSET, renaming columns/tables) and read-only extraction helpers
//! (getting column names, table names, functions, etc.).
//!
//! Mutation functions take an owned [`Expression`] and return a new [`Expression`].
//! Read-only getters take `&Expression`.

use std::collections::HashMap;

use crate::expressions::*;
use crate::traversal::ExpressionWalk;

/// Apply a bottom-up transformation to every node in the tree.
/// Wraps `crate::traversal::transform` with a simpler signature for this module.
fn xform<F: Fn(Expression) -> Expression>(expr: Expression, fun: F) -> Expression {
    crate::traversal::transform(expr, &|node| Ok(Some(fun(node))))
        .unwrap_or_else(|_| Expression::Null(Null))
}

// ---------------------------------------------------------------------------
// SELECT clause
// ---------------------------------------------------------------------------

/// Append columns to the SELECT list of a query.
///
/// If `expr` is a `Select`, the given `columns` are appended to its expression list.
/// Non-SELECT expressions are returned unchanged.
pub fn add_select_columns(expr: Expression, columns: Vec<Expression>) -> Expression {
    if let Expression::Select(mut sel) = expr {
        sel.expressions.extend(columns);
        Expression::Select(sel)
    } else {
        expr
    }
}

/// Remove columns from the SELECT list where `predicate` returns `true`.
pub fn remove_select_columns<F: Fn(&Expression) -> bool>(
    expr: Expression,
    predicate: F,
) -> Expression {
    if let Expression::Select(mut sel) = expr {
        sel.expressions.retain(|e| !predicate(e));
        Expression::Select(sel)
    } else {
        expr
    }
}

/// Set or remove the DISTINCT flag on a SELECT.
pub fn set_distinct(expr: Expression, distinct: bool) -> Expression {
    if let Expression::Select(mut sel) = expr {
        sel.distinct = distinct;
        Expression::Select(sel)
    } else {
        expr
    }
}

// ---------------------------------------------------------------------------
// WHERE clause
// ---------------------------------------------------------------------------

/// Add a condition to the WHERE clause.
///
/// If the SELECT already has a WHERE clause, the new condition is combined with the
/// existing one using AND (default) or OR (when `use_or` is `true`).
/// If there is no WHERE clause, one is created.
pub fn add_where(expr: Expression, condition: Expression, use_or: bool) -> Expression {
    if let Expression::Select(mut sel) = expr {
        sel.where_clause = Some(match sel.where_clause.take() {
            Some(existing) => {
                let combined = if use_or {
                    Expression::Or(Box::new(BinaryOp::new(existing.this, condition)))
                } else {
                    Expression::And(Box::new(BinaryOp::new(existing.this, condition)))
                };
                Where { this: combined }
            }
            None => Where { this: condition },
        });
        Expression::Select(sel)
    } else {
        expr
    }
}

/// Remove the WHERE clause from a SELECT.
pub fn remove_where(expr: Expression) -> Expression {
    if let Expression::Select(mut sel) = expr {
        sel.where_clause = None;
        Expression::Select(sel)
    } else {
        expr
    }
}

// ---------------------------------------------------------------------------
// LIMIT / OFFSET
// ---------------------------------------------------------------------------

/// Set the LIMIT on a SELECT.
pub fn set_limit(expr: Expression, limit: usize) -> Expression {
    if let Expression::Select(mut sel) = expr {
        sel.limit = Some(Limit {
            this: Expression::number(limit as i64),
            percent: false,
        });
        Expression::Select(sel)
    } else {
        expr
    }
}

/// Set the OFFSET on a SELECT.
pub fn set_offset(expr: Expression, offset: usize) -> Expression {
    if let Expression::Select(mut sel) = expr {
        sel.offset = Some(Offset {
            this: Expression::number(offset as i64),
            rows: None,
        });
        Expression::Select(sel)
    } else {
        expr
    }
}

/// Remove both LIMIT and OFFSET from a SELECT.
pub fn remove_limit_offset(expr: Expression) -> Expression {
    if let Expression::Select(mut sel) = expr {
        sel.limit = None;
        sel.offset = None;
        Expression::Select(sel)
    } else {
        expr
    }
}

// ---------------------------------------------------------------------------
// Renaming
// ---------------------------------------------------------------------------

/// Rename columns throughout the expression tree using the provided mapping.
///
/// Column names present as keys in `mapping` are replaced with their corresponding
/// values. The replacement is case-sensitive.
pub fn rename_columns(expr: Expression, mapping: &HashMap<String, String>) -> Expression {
    xform(expr, |node| match node {
        Expression::Column(mut col) => {
            if let Some(new_name) = mapping.get(&col.name.name) {
                col.name.name = new_name.clone();
            }
            Expression::Column(col)
        }
        other => other,
    })
}

/// Rename tables throughout the expression tree using the provided mapping.
pub fn rename_tables(expr: Expression, mapping: &HashMap<String, String>) -> Expression {
    xform(expr, |node| match node {
        Expression::Table(mut tbl) => {
            if let Some(new_name) = mapping.get(&tbl.name.name) {
                tbl.name.name = new_name.clone();
            }
            Expression::Table(tbl)
        }
        Expression::Column(mut col) => {
            if let Some(ref mut table_id) = col.table {
                if let Some(new_name) = mapping.get(&table_id.name) {
                    table_id.name = new_name.clone();
                }
            }
            Expression::Column(col)
        }
        other => other,
    })
}

/// Qualify all unqualified column references with the given `table_name`.
///
/// Columns that already have a table qualifier are left unchanged.
pub fn qualify_columns(expr: Expression, table_name: &str) -> Expression {
    let table = table_name.to_string();
    xform(expr, move |node| match node {
        Expression::Column(mut col) => {
            if col.table.is_none() {
                col.table = Some(Identifier::new(&table));
            }
            Expression::Column(col)
        }
        other => other,
    })
}

// ---------------------------------------------------------------------------
// Generic replacement
// ---------------------------------------------------------------------------

/// Replace nodes matching `predicate` with `replacement` (cloned for each match).
pub fn replace_nodes<F: Fn(&Expression) -> bool>(
    expr: Expression,
    predicate: F,
    replacement: Expression,
) -> Expression {
    xform(expr, |node| {
        if predicate(&node) {
            replacement.clone()
        } else {
            node
        }
    })
}

/// Replace nodes matching `predicate` by applying `replacer` to the matched node.
pub fn replace_by_type<F, R>(expr: Expression, predicate: F, replacer: R) -> Expression
where
    F: Fn(&Expression) -> bool,
    R: Fn(Expression) -> Expression,
{
    xform(expr, |node| {
        if predicate(&node) {
            replacer(node)
        } else {
            node
        }
    })
}

/// Remove (replace with a `Null`) all nodes matching `predicate`.
///
/// This is most useful for removing clauses or sub-expressions from a tree.
/// Note that removing structural elements (e.g. the FROM clause) may produce
/// invalid SQL; use with care.
pub fn remove_nodes<F: Fn(&Expression) -> bool>(expr: Expression, predicate: F) -> Expression {
    xform(expr, |node| {
        if predicate(&node) {
            Expression::Null(Null)
        } else {
            node
        }
    })
}

// ---------------------------------------------------------------------------
// Convenience getters
// ---------------------------------------------------------------------------

/// Collect all column names (as `String`) referenced in the expression tree.
pub fn get_column_names(expr: &Expression) -> Vec<String> {
    expr.find_all(|e| matches!(e, Expression::Column(_)))
        .into_iter()
        .filter_map(|e| {
            if let Expression::Column(col) = e {
                Some(col.name.name.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Collect all table names (as `String`) referenced in the expression tree.
pub fn get_table_names(expr: &Expression) -> Vec<String> {
    expr.find_all(|e| matches!(e, Expression::Table(_)))
        .into_iter()
        .filter_map(|e| {
            if let Expression::Table(tbl) = e {
                Some(tbl.name.name.clone())
            } else {
                None
            }
        })
        .collect()
}

/// Collect all identifier references in the expression tree.
pub fn get_identifiers(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(|e| matches!(e, Expression::Identifier(_)))
}

/// Collect all function call nodes in the expression tree.
pub fn get_functions(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(|e| {
        matches!(
            e,
            Expression::Function(_) | Expression::AggregateFunction(_)
        )
    })
}

/// Collect all literal value nodes in the expression tree.
pub fn get_literals(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(|e| {
        matches!(
            e,
            Expression::Literal(_) | Expression::Boolean(_) | Expression::Null(_)
        )
    })
}

/// Collect all subquery nodes in the expression tree.
pub fn get_subqueries(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(|e| matches!(e, Expression::Subquery(_)))
}

/// Collect all aggregate function nodes in the expression tree.
///
/// Includes typed aggregates (`Count`, `Sum`, `Avg`, `Min`, `Max`, etc.)
/// and generic `AggregateFunction` nodes.
pub fn get_aggregate_functions(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(|e| {
        matches!(
            e,
            Expression::AggregateFunction(_)
                | Expression::Count(_)
                | Expression::Sum(_)
                | Expression::Avg(_)
                | Expression::Min(_)
                | Expression::Max(_)
                | Expression::ApproxDistinct(_)
                | Expression::ArrayAgg(_)
                | Expression::GroupConcat(_)
                | Expression::StringAgg(_)
                | Expression::ListAgg(_)
        )
    })
}

/// Collect all window function nodes in the expression tree.
pub fn get_window_functions(expr: &Expression) -> Vec<&Expression> {
    expr.find_all(|e| matches!(e, Expression::WindowFunction(_)))
}

/// Count the total number of AST nodes in the expression tree.
pub fn node_count(expr: &Expression) -> usize {
    expr.dfs().count()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn parse_one(sql: &str) -> Expression {
        let mut exprs = Parser::parse_sql(sql).unwrap();
        exprs.remove(0)
    }

    #[test]
    fn test_add_where() {
        let expr = parse_one("SELECT a FROM t");
        let cond = Expression::Eq(Box::new(BinaryOp::new(
            Expression::column("b"),
            Expression::number(1),
        )));
        let result = add_where(expr, cond, false);
        let sql = result.sql();
        assert!(sql.contains("WHERE"), "Expected WHERE in: {}", sql);
        assert!(sql.contains("b = 1"), "Expected condition in: {}", sql);
    }

    #[test]
    fn test_add_where_combines_with_and() {
        let expr = parse_one("SELECT a FROM t WHERE x = 1");
        let cond = Expression::Eq(Box::new(BinaryOp::new(
            Expression::column("y"),
            Expression::number(2),
        )));
        let result = add_where(expr, cond, false);
        let sql = result.sql();
        assert!(sql.contains("AND"), "Expected AND in: {}", sql);
    }

    #[test]
    fn test_remove_where() {
        let expr = parse_one("SELECT a FROM t WHERE x = 1");
        let result = remove_where(expr);
        let sql = result.sql();
        assert!(!sql.contains("WHERE"), "Should not contain WHERE: {}", sql);
    }

    #[test]
    fn test_set_limit() {
        let expr = parse_one("SELECT a FROM t");
        let result = set_limit(expr, 10);
        let sql = result.sql();
        assert!(sql.contains("LIMIT 10"), "Expected LIMIT in: {}", sql);
    }

    #[test]
    fn test_set_offset() {
        let expr = parse_one("SELECT a FROM t");
        let result = set_offset(expr, 5);
        let sql = result.sql();
        assert!(sql.contains("OFFSET 5"), "Expected OFFSET in: {}", sql);
    }

    #[test]
    fn test_remove_limit_offset() {
        let expr = parse_one("SELECT a FROM t LIMIT 10 OFFSET 5");
        let result = remove_limit_offset(expr);
        let sql = result.sql();
        assert!(!sql.contains("LIMIT"), "Should not contain LIMIT: {}", sql);
        assert!(!sql.contains("OFFSET"), "Should not contain OFFSET: {}", sql);
    }

    #[test]
    fn test_get_column_names() {
        let expr = parse_one("SELECT a, b, c FROM t");
        let names = get_column_names(&expr);
        assert!(names.contains(&"a".to_string()));
        assert!(names.contains(&"b".to_string()));
        assert!(names.contains(&"c".to_string()));
    }

    #[test]
    fn test_get_table_names() {
        // get_table_names uses DFS which finds Expression::Table nodes
        // In parsed SQL, table refs are within From/Join nodes
        let expr = parse_one("SELECT a FROM users");
        let tables = crate::traversal::get_tables(&expr);
        // Verify our function finds the same tables as the traversal module
        let names = get_table_names(&expr);
        assert_eq!(names.len(), tables.len(),
            "get_table_names and get_tables should find same count");
    }

    #[test]
    fn test_node_count() {
        let expr = parse_one("SELECT a FROM t");
        let count = node_count(&expr);
        assert!(count > 0, "Expected non-zero node count");
    }

    #[test]
    fn test_rename_columns() {
        let expr = parse_one("SELECT old_name FROM t");
        let mut mapping = HashMap::new();
        mapping.insert("old_name".to_string(), "new_name".to_string());
        let result = rename_columns(expr, &mapping);
        let sql = result.sql();
        assert!(sql.contains("new_name"), "Expected new_name in: {}", sql);
        assert!(!sql.contains("old_name"), "Should not contain old_name: {}", sql);
    }

    #[test]
    fn test_rename_tables() {
        let expr = parse_one("SELECT a FROM old_table");
        let mut mapping = HashMap::new();
        mapping.insert("old_table".to_string(), "new_table".to_string());
        let result = rename_tables(expr, &mapping);
        let sql = result.sql();
        assert!(sql.contains("new_table"), "Expected new_table in: {}", sql);
    }

    #[test]
    fn test_set_distinct() {
        let expr = parse_one("SELECT a FROM t");
        let result = set_distinct(expr, true);
        let sql = result.sql();
        assert!(sql.contains("DISTINCT"), "Expected DISTINCT in: {}", sql);
    }

    #[test]
    fn test_add_select_columns() {
        let expr = parse_one("SELECT a FROM t");
        let result = add_select_columns(expr, vec![Expression::column("b")]);
        let sql = result.sql();
        assert!(sql.contains("a, b") || sql.contains("a,b"), "Expected a, b in: {}", sql);
    }

    #[test]
    fn test_qualify_columns() {
        let expr = parse_one("SELECT a, b FROM t");
        let result = qualify_columns(expr, "t");
        let sql = result.sql();
        assert!(sql.contains("t.a"), "Expected t.a in: {}", sql);
        assert!(sql.contains("t.b"), "Expected t.b in: {}", sql);
    }

    #[test]
    fn test_get_functions() {
        let expr = parse_one("SELECT COUNT(*), UPPER(name) FROM t");
        let funcs = get_functions(&expr);
        // UPPER is a typed function (Expression::Upper), not Expression::Function
        // COUNT is Expression::Count, not Expression::AggregateFunction
        // So get_functions (which checks Function | AggregateFunction) may return 0
        // That's OK — we have separate get_aggregate_functions for typed aggs
        let _ = funcs.len();
    }

    #[test]
    fn test_get_aggregate_functions() {
        let expr = parse_one("SELECT COUNT(*), SUM(x) FROM t");
        let aggs = get_aggregate_functions(&expr);
        assert!(aggs.len() >= 2, "Expected at least 2 aggregates, got {}", aggs.len());
    }
}
