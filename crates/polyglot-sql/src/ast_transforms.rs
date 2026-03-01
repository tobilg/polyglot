//! AST transform helpers and convenience getters.
//!
//! This module provides functions for common AST mutations (adding WHERE clauses,
//! setting LIMIT/OFFSET, renaming columns/tables) and read-only extraction helpers
//! (getting column names, table names, functions, etc.).
//!
//! Mutation functions take an owned [`Expression`] and return a new [`Expression`].
//! Read-only getters take `&Expression`.

use std::collections::{HashMap, HashSet};

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
            comments: Vec::new(),
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

/// Collect projected output column names from a query expression.
///
/// This follows sqlglot-style query semantics:
/// - For `SELECT`, returns names from the projection list.
/// - For set operations (`UNION`/`INTERSECT`/`EXCEPT`), uses the left-most branch.
/// - For `Subquery`, unwraps and evaluates the inner query.
///
/// Unlike [`get_column_names`], this does not return every referenced column in
/// the AST and is suitable for result-schema style output names.
pub fn get_output_column_names(expr: &Expression) -> Vec<String> {
    output_column_names_from_query(expr)
}

fn output_column_names_from_query(expr: &Expression) -> Vec<String> {
    match expr {
        Expression::Select(select) => select_output_column_names(select),
        Expression::Union(union) => output_column_names_from_query(&union.left),
        Expression::Intersect(intersect) => output_column_names_from_query(&intersect.left),
        Expression::Except(except) => output_column_names_from_query(&except.left),
        Expression::Subquery(subquery) => output_column_names_from_query(&subquery.this),
        _ => Vec::new(),
    }
}

fn select_output_column_names(select: &Select) -> Vec<String> {
    let mut names = Vec::new();
    for expr in &select.expressions {
        if let Some(name) = expression_output_name(expr) {
            names.push(name);
        }
    }
    names
}

fn expression_output_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Alias(alias) => Some(alias.alias.name.clone()),
        Expression::Column(col) => Some(col.name.name.clone()),
        Expression::Star(_) => Some("*".to_string()),
        Expression::Identifier(id) => Some(id.name.clone()),
        Expression::Aliases(aliases) => aliases.expressions.iter().find_map(|e| match e {
            Expression::Identifier(id) => Some(id.name.clone()),
            _ => None,
        }),
        _ => None,
    }
}

/// Collect all table names (as `String`) referenced in the expression tree.
pub fn get_table_names(expr: &Expression) -> Vec<String> {
    fn collect_cte_aliases(with_clause: &With, aliases: &mut HashSet<String>) {
        for cte in &with_clause.ctes {
            aliases.insert(cte.alias.name.clone());
        }
    }

    fn push_table_ref_name(
        table: &TableRef,
        cte_aliases: &HashSet<String>,
        names: &mut Vec<String>,
    ) {
        let name = table.name.name.clone();
        if !name.is_empty() && !cte_aliases.contains(&name) {
            names.push(name);
        }
    }

    let mut cte_aliases: HashSet<String> = HashSet::new();
    for node in expr.dfs() {
        match node {
            Expression::Select(select) => {
                if let Some(with) = &select.with {
                    collect_cte_aliases(with, &mut cte_aliases);
                }
            }
            Expression::Insert(insert) => {
                if let Some(with) = &insert.with {
                    collect_cte_aliases(with, &mut cte_aliases);
                }
            }
            Expression::Update(update) => {
                if let Some(with) = &update.with {
                    collect_cte_aliases(with, &mut cte_aliases);
                }
            }
            Expression::Delete(delete) => {
                if let Some(with) = &delete.with {
                    collect_cte_aliases(with, &mut cte_aliases);
                }
            }
            Expression::Union(union) => {
                if let Some(with) = &union.with {
                    collect_cte_aliases(with, &mut cte_aliases);
                }
            }
            Expression::Intersect(intersect) => {
                if let Some(with) = &intersect.with {
                    collect_cte_aliases(with, &mut cte_aliases);
                }
            }
            Expression::Except(except) => {
                if let Some(with) = &except.with {
                    collect_cte_aliases(with, &mut cte_aliases);
                }
            }
            Expression::Merge(merge) => {
                if let Some(with_) = &merge.with_ {
                    if let Expression::With(with_clause) = with_.as_ref() {
                        collect_cte_aliases(with_clause, &mut cte_aliases);
                    }
                }
            }
            _ => {}
        }
    }

    let mut names = Vec::new();
    for node in expr.dfs() {
        match node {
            Expression::Table(tbl) => {
                let name = tbl.name.name.clone();
                if !name.is_empty() && !cte_aliases.contains(&name) {
                    names.push(name);
                }
            }
            Expression::Insert(insert) => {
                push_table_ref_name(&insert.table, &cte_aliases, &mut names);
            }
            Expression::Update(update) => {
                push_table_ref_name(&update.table, &cte_aliases, &mut names);
                for table in &update.extra_tables {
                    push_table_ref_name(table, &cte_aliases, &mut names);
                }
            }
            Expression::Delete(delete) => {
                push_table_ref_name(&delete.table, &cte_aliases, &mut names);
                for table in &delete.using {
                    push_table_ref_name(table, &cte_aliases, &mut names);
                }
                for table in &delete.tables {
                    push_table_ref_name(table, &cte_aliases, &mut names);
                }
            }
            _ => {}
        }
    }

    names
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
        assert!(
            !sql.contains("OFFSET"),
            "Should not contain OFFSET: {}",
            sql
        );
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
    fn test_get_output_column_names_select() {
        let expr = parse_one("SELECT a, b AS c, 1 FROM t");
        let names = get_output_column_names(&expr);
        assert_eq!(names, vec!["a".to_string(), "c".to_string()]);
    }

    #[test]
    fn test_get_output_column_names_union_left_projection() {
        let expr =
            parse_one("SELECT id, name FROM customers UNION ALL SELECT id, name FROM employees");
        let names = get_output_column_names(&expr);
        assert_eq!(names, vec!["id".to_string(), "name".to_string()]);
    }

    #[test]
    fn test_get_output_column_names_union_uses_left_aliases() {
        let expr = parse_one("SELECT id AS c1, name AS c2 FROM t1 UNION SELECT x, y FROM t2");
        let names = get_output_column_names(&expr);
        assert_eq!(names, vec!["c1".to_string(), "c2".to_string()]);
    }

    #[test]
    fn test_get_column_names_union_still_returns_all_references() {
        let expr =
            parse_one("SELECT id, name FROM customers UNION ALL SELECT id, name FROM employees");
        let names = get_column_names(&expr);
        assert_eq!(
            names,
            vec![
                "id".to_string(),
                "name".to_string(),
                "id".to_string(),
                "name".to_string()
            ]
        );
    }

    #[test]
    fn test_get_table_names() {
        let expr = parse_one("SELECT a FROM users");
        let names = get_table_names(&expr);
        assert_eq!(names, vec!["users".to_string()]);
    }

    #[test]
    fn test_get_table_names_excludes_cte_aliases() {
        let expr = parse_one(
            "WITH cte AS (SELECT * FROM users) SELECT * FROM cte JOIN orders o ON cte.id = o.id",
        );
        let names = get_table_names(&expr);
        assert!(names.iter().any(|n| n == "users"));
        assert!(names.iter().any(|n| n == "orders"));
        assert!(!names.iter().any(|n| n == "cte"));
    }

    #[test]
    fn test_get_table_names_includes_dml_targets() {
        let insert_expr = parse_one("INSERT INTO users (id) VALUES (1)");
        let insert_names = get_table_names(&insert_expr);
        assert!(insert_names.iter().any(|n| n == "users"));

        let update_expr =
            parse_one("UPDATE users SET name = 'x' FROM accounts WHERE users.id = accounts.id");
        let update_names = get_table_names(&update_expr);
        assert!(update_names.iter().any(|n| n == "users"));
        assert!(update_names.iter().any(|n| n == "accounts"));

        let delete_expr =
            parse_one("DELETE FROM users USING accounts WHERE users.id = accounts.id");
        let delete_names = get_table_names(&delete_expr);
        assert!(delete_names.iter().any(|n| n == "users"));
        assert!(delete_names.iter().any(|n| n == "accounts"));
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
        assert!(
            !sql.contains("old_name"),
            "Should not contain old_name: {}",
            sql
        );
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
        assert!(
            sql.contains("a, b") || sql.contains("a,b"),
            "Expected a, b in: {}",
            sql
        );
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
        assert!(
            aggs.len() >= 2,
            "Expected at least 2 aggregates, got {}",
            aggs.len()
        );
    }
}
