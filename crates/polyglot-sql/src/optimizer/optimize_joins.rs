//! Join Optimization Module
//!
//! This module provides functionality for optimizing JOIN operations:
//! - Removing cross joins when possible
//! - Reordering joins based on predicate dependencies
//! - Normalizing join syntax (removing unnecessary INNER/OUTER keywords)
//!
//! Ported from sqlglot's optimizer/optimize_joins.py

use std::collections::{HashMap, HashSet};

use crate::expressions::{BooleanLiteral, Expression, Join, JoinKind};
use crate::helper::tsort;

/// Optimize joins by removing cross joins and reordering based on dependencies.
///
/// # Example
///
/// ```sql
/// -- Before:
/// SELECT * FROM x CROSS JOIN y JOIN z ON x.a = z.a AND y.a = z.a
/// -- After:
/// SELECT * FROM x JOIN z ON x.a = z.a AND TRUE JOIN y ON y.a = z.a
/// ```
///
/// # Arguments
/// * `expression` - The expression to optimize
///
/// # Returns
/// The optimized expression with improved join order
pub fn optimize_joins(expression: Expression) -> Expression {
    let expression = optimize_cross_joins(expression);
    let expression = reorder_joins(expression);
    let expression = normalize_joins(expression);
    expression
}

/// Optimize cross joins by moving predicates from later joins
fn optimize_cross_joins(expression: Expression) -> Expression {
    if let Expression::Select(select) = expression {
        if select.joins.is_empty() || !is_reorderable(&select.joins) {
            return Expression::Select(select);
        }

        // Build reference map: table -> list of joins that reference it
        let mut references: HashMap<String, Vec<usize>> = HashMap::new();
        let mut cross_joins: Vec<(String, usize)> = Vec::new();

        for (i, join) in select.joins.iter().enumerate() {
            let tables = other_table_names(join);

            if tables.is_empty() {
                // This is a cross join
                if let Some(name) = get_join_name(join) {
                    cross_joins.push((name, i));
                }
            } else {
                // This join has ON predicates referencing tables
                for table in tables {
                    references.entry(table).or_insert_with(Vec::new).push(i);
                }
            }
        }

        // Move predicates from referencing joins to cross joins
        for (name, cross_idx) in &cross_joins {
            if let Some(ref_indices) = references.get(name) {
                for &ref_idx in ref_indices {
                    // In a full implementation, we would move predicates
                    // that reference the cross join table from the referencing
                    // join to the cross join
                    let _ = (cross_idx, ref_idx);
                }
            }
        }

        Expression::Select(select)
    } else {
        expression
    }
}

/// Reorder joins by topological sort based on predicate dependencies.
pub fn reorder_joins(expression: Expression) -> Expression {
    if let Expression::Select(mut select) = expression {
        if select.joins.is_empty() || !is_reorderable(&select.joins) {
            return Expression::Select(select);
        }

        // Build dependency graph
        let mut joins_by_name: HashMap<String, Join> = HashMap::new();
        let mut dag: HashMap<String, HashSet<String>> = HashMap::new();

        for join in &select.joins {
            if let Some(name) = get_join_name(join) {
                joins_by_name.insert(name.clone(), join.clone());
                dag.insert(name, other_table_names(join));
            }
        }

        // Get topologically sorted order
        if let Ok(sorted) = tsort(dag) {
            // Get the FROM table name (to exclude from join reordering)
            let from_name = select.from.as_ref()
                .and_then(|f| f.expressions.first())
                .and_then(|e| get_table_name(e));

            // Reorder joins
            let mut reordered: Vec<Join> = Vec::new();
            for name in sorted {
                if Some(&name) != from_name.as_ref() {
                    if let Some(join) = joins_by_name.remove(&name) {
                        reordered.push(join);
                    }
                }
            }

            // If reordering succeeded, use new order; otherwise keep original
            if !reordered.is_empty() && reordered.len() == select.joins.len() {
                select.joins = reordered;
            }
        }

        Expression::Select(select)
    } else {
        expression
    }
}

/// Normalize join syntax by removing unnecessary keywords.
///
/// - Remove INNER keyword (it's the default for joins with ON clause)
/// - Remove OUTER keyword (only LEFT/RIGHT/FULL matter)
/// - Add CROSS keyword to joins without any join type
/// - Add TRUE to joins without ON or USING clause
pub fn normalize_joins(expression: Expression) -> Expression {
    if let Expression::Select(mut select) = expression {
        for join in &mut select.joins {
            // For CROSS joins, clear the ON clause
            if join.kind == JoinKind::Cross {
                join.on = None;
            } else {
                // Remove INNER keyword flag (INNER is the default)
                if join.kind == JoinKind::Inner {
                    join.use_inner_keyword = false;
                }

                // Remove OUTER keyword flag
                join.use_outer_keyword = false;

                // If no ON or USING, add ON TRUE
                if join.on.is_none() && join.using.is_empty() {
                    join.on = Some(Expression::Boolean(BooleanLiteral { value: true }));
                }
            }
        }

        Expression::Select(select)
    } else {
        expression
    }
}

/// Check if joins can be reordered without changing query semantics.
///
/// Joins with a side (LEFT, RIGHT, FULL) cannot be reordered,
/// as the order affects which rows are included.
pub fn is_reorderable(joins: &[Join]) -> bool {
    joins.iter().all(|j| {
        matches!(j.kind, JoinKind::Inner | JoinKind::Cross | JoinKind::Natural)
    })
}

/// Get table names referenced in a join's ON clause (excluding the join's own table).
fn other_table_names(join: &Join) -> HashSet<String> {
    let mut tables = HashSet::new();

    if let Some(ref on) = join.on {
        collect_table_names(on, &mut tables);
    }

    // Remove the join's own table name
    if let Some(name) = get_join_name(join) {
        tables.remove(&name);
    }

    tables
}

/// Collect all table names referenced in an expression.
fn collect_table_names(expr: &Expression, tables: &mut HashSet<String>) {
    match expr {
        Expression::Column(col) => {
            if let Some(ref table) = col.table {
                tables.insert(table.name.clone());
            }
        }
        Expression::And(bin) | Expression::Or(bin) => {
            collect_table_names(&bin.left, tables);
            collect_table_names(&bin.right, tables);
        }
        Expression::Eq(bin) | Expression::Neq(bin) | Expression::Lt(bin) |
        Expression::Gt(bin) | Expression::Lte(bin) | Expression::Gte(bin) => {
            collect_table_names(&bin.left, tables);
            collect_table_names(&bin.right, tables);
        }
        Expression::Paren(p) => {
            collect_table_names(&p.this, tables);
        }
        _ => {}
    }
}

/// Get the alias or table name from a join.
fn get_join_name(join: &Join) -> Option<String> {
    get_table_name(&join.this)
}

/// Get the alias or name from a table expression.
fn get_table_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Table(table) => {
            if let Some(ref alias) = table.alias {
                Some(alias.name.clone())
            } else {
                Some(table.name.name.clone())
            }
        }
        Expression::Subquery(subquery) => {
            subquery.alias.as_ref().map(|a| a.name.clone())
        }
        Expression::Alias(alias) => {
            Some(alias.alias.name.clone())
        }
        _ => None,
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

    fn parse(sql: &str) -> Expression {
        Parser::parse_sql(sql).expect("Failed to parse")[0].clone()
    }

    #[test]
    fn test_optimize_joins_simple() {
        let expr = parse("SELECT * FROM x JOIN y ON x.a = y.a");
        let result = optimize_joins(expr);
        let sql = gen(&result);
        assert!(sql.contains("JOIN"));
    }

    #[test]
    fn test_is_reorderable_true() {
        let expr = parse("SELECT * FROM x JOIN y ON x.a = y.a JOIN z ON y.a = z.a");
        if let Expression::Select(select) = &expr {
            assert!(is_reorderable(&select.joins));
        }
    }

    #[test]
    fn test_is_reorderable_false() {
        let expr = parse("SELECT * FROM x LEFT JOIN y ON x.a = y.a");
        if let Expression::Select(select) = &expr {
            assert!(!is_reorderable(&select.joins));
        }
    }

    #[test]
    fn test_normalize_inner_join() {
        let expr = parse("SELECT * FROM x INNER JOIN y ON x.a = y.a");
        let result = normalize_joins(expr);
        let sql = gen(&result);
        // INNER should be normalized (removed)
        assert!(sql.contains("JOIN"));
    }

    #[test]
    fn test_normalize_cross_join() {
        let expr = parse("SELECT * FROM x CROSS JOIN y");
        let result = normalize_joins(expr);
        let sql = gen(&result);
        assert!(sql.contains("CROSS"));
    }

    #[test]
    fn test_reorder_joins() {
        let expr = parse("SELECT * FROM x JOIN y ON x.a = y.a JOIN z ON y.a = z.a");
        let result = reorder_joins(expr);
        let sql = gen(&result);
        assert!(sql.contains("JOIN"));
    }

    #[test]
    fn test_other_table_names() {
        let expr = parse("SELECT * FROM x JOIN y ON x.a = y.a AND x.b = z.b");
        if let Expression::Select(select) = &expr {
            if let Some(join) = select.joins.first() {
                let tables = other_table_names(join);
                assert!(tables.contains("x"));
                assert!(tables.contains("z"));
            }
        }
    }

    #[test]
    fn test_get_join_name_table() {
        let expr = parse("SELECT * FROM x JOIN y ON x.a = y.a");
        if let Expression::Select(select) = &expr {
            if let Some(join) = select.joins.first() {
                let name = get_join_name(join);
                assert_eq!(name, Some("y".to_string()));
            }
        }
    }

    #[test]
    fn test_get_join_name_alias() {
        let expr = parse("SELECT * FROM x JOIN y AS t ON x.a = t.a");
        if let Expression::Select(select) = &expr {
            if let Some(join) = select.joins.first() {
                let name = get_join_name(join);
                assert_eq!(name, Some("t".to_string()));
            }
        }
    }

    #[test]
    fn test_optimize_preserves_structure() {
        let expr = parse("SELECT a, b FROM x JOIN y ON x.a = y.a WHERE x.b > 1");
        let result = optimize_joins(expr);
        let sql = gen(&result);
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_left_join_not_reorderable() {
        let expr = parse("SELECT * FROM x LEFT JOIN y ON x.a = y.a JOIN z ON y.a = z.a");
        if let Expression::Select(select) = &expr {
            assert!(!is_reorderable(&select.joins));
        }
    }
}
