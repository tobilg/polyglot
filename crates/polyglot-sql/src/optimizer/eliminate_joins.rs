//! Join Elimination Module
//!
//! This module removes unused joins from SQL queries. A join can be eliminated
//! when no columns from the joined table are referenced outside the ON clause.
//!
//! Ported from sqlglot's optimizer/eliminate_joins.py

use crate::expressions::*;
use crate::scope::traverse_scope;
use crate::scope::ColumnRef;
use std::collections::HashMap;

/// Remove unused joins from an expression.
///
/// A LEFT JOIN can be eliminated when no columns from the joined table are
/// referenced in the SELECT list, WHERE clause, GROUP BY, HAVING, ORDER BY,
/// or any other part of the query outside the JOIN's own ON clause.
///
/// Semi and anti joins are never eliminated because they affect the result
/// set cardinality even when no columns are selected from them.
///
/// If the scope contains unqualified columns, we conservatively skip
/// elimination since we cannot determine which source an unqualified
/// column belongs to.
///
/// # Example
///
/// ```sql
/// -- Before:
/// SELECT x.a FROM x LEFT JOIN y ON x.b = y.b
/// -- After:
/// SELECT x.a FROM x
/// ```
///
/// # Arguments
/// * `expression` - The expression to optimize
///
/// # Returns
/// The optimized expression with unnecessary joins removed
pub fn eliminate_joins(expression: Expression) -> Expression {
    let scopes = traverse_scope(&expression);

    // Collect (source_alias, join_index) pairs to remove across all scopes.
    // We gather them first and then apply removals so that scope analysis
    // (which borrows the expression immutably) is finished before we mutate.
    let mut removals: Vec<JoinRemoval> = Vec::new();

    for mut scope in scopes {
        // If there are unqualified columns we cannot safely determine which
        // source they belong to, so skip this scope.
        if !scope.unqualified_columns().is_empty() {
            continue;
        }

        let select = match &scope.expression {
            Expression::Select(s) => s.clone(),
            _ => continue,
        };

        let joins = &select.joins;
        if joins.is_empty() {
            continue;
        }

        // Iterate joins in reverse order (like the Python implementation)
        // so that index-based removal is stable.
        for (idx, join) in joins.iter().enumerate().rev() {
            if is_semi_or_anti_join(join) {
                continue;
            }

            let alias = join_alias_or_name(join);
            let alias = match alias {
                Some(a) => a,
                None => continue,
            };

            if should_eliminate_join(&mut scope, &select, idx, join, &alias) {
                removals.push(JoinRemoval {
                    select_id: select_identity(&select),
                    join_index: idx,
                    source_alias: alias,
                });
            }
        }
    }

    if removals.is_empty() {
        return expression;
    }

    apply_removals(expression, &removals)
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Describes a join that should be removed.
struct JoinRemoval {
    /// An identity key for the Select node that owns this join.
    select_id: SelectIdentity,
    /// The index of the join in the Select's joins vec.
    join_index: usize,
    /// The alias (or name) of the joined source so we can also remove it
    /// from scope bookkeeping.
    #[allow(dead_code)]
    source_alias: String,
}

/// A lightweight identity for a Select node so we can match it when
/// walking the cloned tree. We use a combination of the number of
/// select-list expressions and the number of joins since that is
/// sufficient for the simple cases we handle and avoids needing
/// pointer identity across a clone.
#[derive(Debug, Clone, PartialEq, Eq)]
struct SelectIdentity {
    num_expressions: usize,
    num_joins: usize,
    /// First select expression as generated text (for disambiguation).
    first_expr_debug: String,
}

fn select_identity(select: &Select) -> SelectIdentity {
    SelectIdentity {
        num_expressions: select.expressions.len(),
        num_joins: select.joins.len(),
        first_expr_debug: select
            .expressions
            .first()
            .map(|e| format!("{:?}", e))
            .unwrap_or_default(),
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Returns `true` if the join is a SEMI or ANTI join (any directional
/// variant). These joins affect result cardinality even when no columns
/// are selected, so they must not be eliminated.
fn is_semi_or_anti_join(join: &Join) -> bool {
    matches!(
        join.kind,
        JoinKind::Semi
            | JoinKind::Anti
            | JoinKind::LeftSemi
            | JoinKind::LeftAnti
            | JoinKind::RightSemi
            | JoinKind::RightAnti
    )
}

/// Extract the alias or table name from a join's source expression.
fn join_alias_or_name(join: &Join) -> Option<String> {
    get_table_alias_or_name(&join.this)
}

/// Get alias or name from a table/subquery expression.
fn get_table_alias_or_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Table(table) => {
            if let Some(ref alias) = table.alias {
                Some(alias.name.clone())
            } else {
                Some(table.name.name.clone())
            }
        }
        Expression::Subquery(subquery) => subquery.alias.as_ref().map(|a| a.name.clone()),
        _ => None,
    }
}

/// Determine whether a join should be eliminated.
///
/// A join is eliminable when:
/// 1. It is a LEFT JOIN, AND
/// 2. No columns from the joined source appear outside the ON clause
///
/// The scope's `source_columns` includes JOIN conditions, so this check
/// explicitly subtracts the current join's own ON / MATCH_CONDITION references
/// and verifies whether any remaining references to the joined source exist.
fn should_eliminate_join(
    scope: &mut crate::scope::Scope,
    _select: &Select,
    _join_index: usize,
    join: &Join,
    alias: &str,
) -> bool {
    // Only LEFT JOINs can be safely eliminated in the general case.
    // (INNER JOINs can filter rows, RIGHT/FULL JOINs can introduce NULLs
    // on the left side, CROSS JOINs affect cardinality.)
    if join.kind != JoinKind::Left {
        return false;
    }

    // Check whether any columns from this source are referenced outside this join's
    // own join condition (ON / MATCH_CONDITION). With `scope.columns()` now
    // including all JOIN conditions, we subtract this join's own condition refs.
    let source_cols = scope.source_columns(alias);
    if source_cols.is_empty() {
        return true;
    }

    let mut source_counts: HashMap<(String, String), usize> = HashMap::new();
    for col in &source_cols {
        if let Some(table) = &col.table {
            *source_counts
                .entry((table.clone(), col.name.clone()))
                .or_insert(0) += 1;
        }
    }

    if let Some(on) = &join.on {
        subtract_columns_from_counts(alias, on, &mut source_counts);
    }
    if let Some(match_condition) = &join.match_condition {
        subtract_columns_from_counts(alias, match_condition, &mut source_counts);
    }

    !source_counts.values().any(|&count| count > 0)
}

fn subtract_columns_from_counts(
    alias: &str,
    expr: &Expression,
    counts: &mut HashMap<(String, String), usize>,
) {
    let mut cols: Vec<ColumnRef> = Vec::new();
    collect_columns_in_expression(expr, &mut cols);

    for col in cols {
        if col.table.as_deref() != Some(alias) {
            continue;
        }
        let key = (alias.to_string(), col.name);
        if let Some(value) = counts.get_mut(&key) {
            if *value > 0 {
                *value -= 1;
            }
        }
    }
}

fn collect_columns_in_expression(expr: &Expression, columns: &mut Vec<ColumnRef>) {
    match expr {
        Expression::Column(col) => {
            columns.push(ColumnRef {
                table: col.table.as_ref().map(|t| t.name.clone()),
                name: col.name.name.clone(),
            });
        }
        Expression::Select(select) => {
            for e in &select.expressions {
                collect_columns_in_expression(e, columns);
            }
            if let Some(from) = &select.from {
                for e in &from.expressions {
                    collect_columns_in_expression(e, columns);
                }
            }
            for join in &select.joins {
                collect_columns_in_expression(&join.this, columns);
                if let Some(on) = &join.on {
                    collect_columns_in_expression(on, columns);
                }
                if let Some(match_condition) = &join.match_condition {
                    collect_columns_in_expression(match_condition, columns);
                }
            }
            if let Some(where_clause) = &select.where_clause {
                collect_columns_in_expression(&where_clause.this, columns);
            }
            if let Some(group_by) = &select.group_by {
                for e in &group_by.expressions {
                    collect_columns_in_expression(e, columns);
                }
            }
            if let Some(having) = &select.having {
                collect_columns_in_expression(&having.this, columns);
            }
            if let Some(order_by) = &select.order_by {
                for o in &order_by.expressions {
                    collect_columns_in_expression(&o.this, columns);
                }
            }
            if let Some(qualify) = &select.qualify {
                collect_columns_in_expression(&qualify.this, columns);
            }
            if let Some(limit) = &select.limit {
                collect_columns_in_expression(&limit.this, columns);
            }
            if let Some(offset) = &select.offset {
                collect_columns_in_expression(&offset.this, columns);
            }
        }
        Expression::Alias(alias) => {
            collect_columns_in_expression(&alias.this, columns);
        }
        Expression::Function(func) => {
            for arg in &func.args {
                collect_columns_in_expression(arg, columns);
            }
        }
        Expression::AggregateFunction(agg) => {
            for arg in &agg.args {
                collect_columns_in_expression(arg, columns);
            }
        }
        Expression::And(bin)
        | Expression::Or(bin)
        | Expression::Eq(bin)
        | Expression::Neq(bin)
        | Expression::Lt(bin)
        | Expression::Lte(bin)
        | Expression::Gt(bin)
        | Expression::Gte(bin)
        | Expression::Add(bin)
        | Expression::Sub(bin)
        | Expression::Mul(bin)
        | Expression::Div(bin)
        | Expression::Mod(bin)
        | Expression::BitwiseAnd(bin)
        | Expression::BitwiseOr(bin)
        | Expression::BitwiseXor(bin)
        | Expression::Concat(bin) => {
            collect_columns_in_expression(&bin.left, columns);
            collect_columns_in_expression(&bin.right, columns);
        }
        Expression::Like(like) | Expression::ILike(like) => {
            collect_columns_in_expression(&like.left, columns);
            collect_columns_in_expression(&like.right, columns);
            if let Some(escape) = &like.escape {
                collect_columns_in_expression(escape, columns);
            }
        }
        Expression::Not(unary) | Expression::Neg(unary) | Expression::BitwiseNot(unary) => {
            collect_columns_in_expression(&unary.this, columns);
        }
        Expression::Case(case) => {
            if let Some(operand) = &case.operand {
                collect_columns_in_expression(operand, columns);
            }
            for (when_expr, then_expr) in &case.whens {
                collect_columns_in_expression(when_expr, columns);
                collect_columns_in_expression(then_expr, columns);
            }
            if let Some(else_) = &case.else_ {
                collect_columns_in_expression(else_, columns);
            }
        }
        Expression::Cast(cast) => {
            collect_columns_in_expression(&cast.this, columns);
        }
        Expression::In(in_expr) => {
            collect_columns_in_expression(&in_expr.this, columns);
            for e in &in_expr.expressions {
                collect_columns_in_expression(e, columns);
            }
            if let Some(query) = &in_expr.query {
                collect_columns_in_expression(query, columns);
            }
        }
        Expression::Between(between) => {
            collect_columns_in_expression(&between.this, columns);
            collect_columns_in_expression(&between.low, columns);
            collect_columns_in_expression(&between.high, columns);
        }
        Expression::Exists(exists) => {
            collect_columns_in_expression(&exists.this, columns);
        }
        Expression::Subquery(subquery) => {
            collect_columns_in_expression(&subquery.this, columns);
        }
        Expression::WindowFunction(wf) => {
            collect_columns_in_expression(&wf.this, columns);
            for p in &wf.over.partition_by {
                collect_columns_in_expression(p, columns);
            }
            for o in &wf.over.order_by {
                collect_columns_in_expression(&o.this, columns);
            }
            if let Some(frame) = &wf.over.frame {
                collect_columns_from_window_bound(&frame.start, columns);
                if let Some(end) = &frame.end {
                    collect_columns_from_window_bound(end, columns);
                }
            }
        }
        Expression::Ordered(ord) => {
            collect_columns_in_expression(&ord.this, columns);
        }
        Expression::Paren(paren) => {
            collect_columns_in_expression(&paren.this, columns);
        }
        Expression::Join(join) => {
            collect_columns_in_expression(&join.this, columns);
            if let Some(on) = &join.on {
                collect_columns_in_expression(on, columns);
            }
            if let Some(match_condition) = &join.match_condition {
                collect_columns_in_expression(match_condition, columns);
            }
        }
        _ => {}
    }
}

fn collect_columns_from_window_bound(bound: &WindowFrameBound, columns: &mut Vec<ColumnRef>) {
    match bound {
        WindowFrameBound::Preceding(expr)
        | WindowFrameBound::Following(expr)
        | WindowFrameBound::Value(expr) => collect_columns_in_expression(expr, columns),
        WindowFrameBound::CurrentRow
        | WindowFrameBound::UnboundedPreceding
        | WindowFrameBound::UnboundedFollowing
        | WindowFrameBound::BarePreceding
        | WindowFrameBound::BareFollowing => {}
    }
}

/// Walk the expression tree, find matching Select nodes, and remove the
/// indicated joins.
fn apply_removals(expression: Expression, removals: &[JoinRemoval]) -> Expression {
    match expression {
        Expression::Select(select) => {
            let id = select_identity(&select);

            // Collect join indices to drop for this Select.
            let mut indices_to_drop: Vec<usize> = removals
                .iter()
                .filter(|r| r.select_id == id)
                .map(|r| r.join_index)
                .collect();
            indices_to_drop.sort_unstable();
            indices_to_drop.dedup();

            let mut new_select = select.clone();

            // Remove joins (iterate in reverse to keep indices valid).
            for &idx in indices_to_drop.iter().rev() {
                if idx < new_select.joins.len() {
                    new_select.joins.remove(idx);
                }
            }

            // Recursively process subqueries in other parts of the Select
            new_select.expressions = new_select
                .expressions
                .into_iter()
                .map(|e| apply_removals(e, removals))
                .collect();

            if let Some(ref mut from) = new_select.from {
                from.expressions = from
                    .expressions
                    .clone()
                    .into_iter()
                    .map(|e| apply_removals(e, removals))
                    .collect();
            }

            if let Some(ref mut w) = new_select.where_clause {
                w.this = apply_removals(w.this.clone(), removals);
            }

            // Process remaining joins' subqueries
            new_select.joins = new_select
                .joins
                .into_iter()
                .map(|mut j| {
                    j.this = apply_removals(j.this, removals);
                    if let Some(on) = j.on {
                        j.on = Some(apply_removals(on, removals));
                    }
                    j
                })
                .collect();

            if let Some(ref mut with) = new_select.with {
                with.ctes = with
                    .ctes
                    .iter()
                    .map(|cte| {
                        let mut new_cte = cte.clone();
                        new_cte.this = apply_removals(new_cte.this, removals);
                        new_cte
                    })
                    .collect();
            }

            Expression::Select(new_select)
        }
        Expression::Subquery(mut subquery) => {
            subquery.this = apply_removals(subquery.this, removals);
            Expression::Subquery(subquery)
        }
        Expression::Union(mut union) => {
            union.left = apply_removals(union.left, removals);
            union.right = apply_removals(union.right, removals);
            Expression::Union(union)
        }
        Expression::Intersect(mut intersect) => {
            intersect.left = apply_removals(intersect.left, removals);
            intersect.right = apply_removals(intersect.right, removals);
            Expression::Intersect(intersect)
        }
        Expression::Except(mut except) => {
            except.left = apply_removals(except.left, removals);
            except.right = apply_removals(except.right, removals);
            Expression::Except(except)
        }
        other => other,
    }
}

// ===========================================================================
// Tests
// ===========================================================================

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

    // -----------------------------------------------------------------------
    // LEFT JOIN where no columns from the joined table are used => removed
    // -----------------------------------------------------------------------

    #[test]
    fn test_eliminate_unused_left_join() {
        let expr = parse("SELECT x.a FROM x LEFT JOIN y ON x.b = y.b");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        // The LEFT JOIN to y should be removed because no columns from y
        // appear in the SELECT list (or WHERE, GROUP BY, etc.).
        assert!(
            !sql.contains("JOIN"),
            "Expected JOIN to be eliminated, got: {}",
            sql
        );
        assert!(
            sql.contains("SELECT x.a FROM x"),
            "Expected simple select, got: {}",
            sql
        );
    }

    // -----------------------------------------------------------------------
    // LEFT JOIN where columns from the joined table ARE used => kept
    // -----------------------------------------------------------------------

    #[test]
    fn test_keep_used_left_join() {
        let expr = parse("SELECT x.a, y.c FROM x LEFT JOIN y ON x.b = y.b");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        // The LEFT JOIN should be preserved because y.c is in the SELECT list.
        assert!(
            sql.contains("JOIN"),
            "Expected JOIN to be preserved, got: {}",
            sql
        );
    }

    // -----------------------------------------------------------------------
    // INNER JOIN where no columns are used => NOT removed (INNER affects rows)
    // -----------------------------------------------------------------------

    #[test]
    fn test_inner_join_not_eliminated() {
        let expr = parse("SELECT x.a FROM x JOIN y ON x.b = y.b");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        // INNER JOINs can filter rows, so they should not be removed even
        // when no columns from the inner source are selected.
        assert!(
            sql.contains("JOIN"),
            "Expected INNER JOIN to be preserved, got: {}",
            sql
        );
    }

    // -----------------------------------------------------------------------
    // LEFT JOIN with column in WHERE => kept
    // -----------------------------------------------------------------------

    #[test]
    fn test_keep_left_join_column_in_where() {
        let expr = parse("SELECT x.a FROM x LEFT JOIN y ON x.b = y.b WHERE y.c > 1");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        assert!(
            sql.contains("JOIN"),
            "Expected JOIN to be preserved (column in WHERE), got: {}",
            sql
        );
    }

    // -----------------------------------------------------------------------
    // Multiple joins: only the unused one is removed
    // -----------------------------------------------------------------------

    #[test]
    fn test_eliminate_one_of_multiple_joins() {
        let expr =
            parse("SELECT x.a, z.d FROM x LEFT JOIN y ON x.b = y.b LEFT JOIN z ON x.c = z.c");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        // y is unused (no y.* columns outside ON), z is used (z.d in SELECT).
        // So the JOIN to y should be removed but the JOIN to z kept.
        assert!(
            sql.contains("JOIN"),
            "Expected at least one JOIN to remain, got: {}",
            sql
        );
        assert!(
            !sql.contains("JOIN y"),
            "Expected JOIN y to be removed, got: {}",
            sql
        );
        assert!(sql.contains("z"), "Expected z to remain, got: {}", sql);
    }

    // -----------------------------------------------------------------------
    // No joins at all => expression unchanged
    // -----------------------------------------------------------------------

    #[test]
    fn test_no_joins_unchanged() {
        let expr = parse("SELECT a FROM x");
        let original_sql = gen(&expr);
        let result = eliminate_joins(expr);
        let result_sql = gen(&result);

        assert_eq!(original_sql, result_sql);
    }

    // -----------------------------------------------------------------------
    // CROSS JOIN => not eliminated (affects cardinality)
    // -----------------------------------------------------------------------

    #[test]
    fn test_cross_join_not_eliminated() {
        let expr = parse("SELECT x.a FROM x CROSS JOIN y");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        assert!(
            sql.contains("CROSS JOIN"),
            "Expected CROSS JOIN to be preserved, got: {}",
            sql
        );
    }

    // -----------------------------------------------------------------------
    // Unqualified columns => skip elimination (conservative)
    // -----------------------------------------------------------------------

    #[test]
    fn test_skip_with_unqualified_columns() {
        // 'a' is unqualified -- we cannot be sure it doesn't come from y
        let expr = parse("SELECT a FROM x LEFT JOIN y ON x.b = y.b");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        // Because 'a' is unqualified the pass should conservatively keep the join.
        assert!(
            sql.contains("JOIN"),
            "Expected JOIN to be preserved (unqualified columns), got: {}",
            sql
        );
    }

    // -----------------------------------------------------------------------
    // LEFT JOIN column used in GROUP BY => kept
    // -----------------------------------------------------------------------

    #[test]
    fn test_keep_left_join_column_in_group_by() {
        let expr = parse("SELECT x.a, COUNT(*) FROM x LEFT JOIN y ON x.b = y.b GROUP BY y.c");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        assert!(
            sql.contains("JOIN"),
            "Expected JOIN to be preserved (column in GROUP BY), got: {}",
            sql
        );
    }

    // -----------------------------------------------------------------------
    // LEFT JOIN column used in ORDER BY => kept
    // -----------------------------------------------------------------------

    #[test]
    fn test_keep_left_join_column_in_order_by() {
        let expr = parse("SELECT x.a FROM x LEFT JOIN y ON x.b = y.b ORDER BY y.c");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        assert!(
            sql.contains("JOIN"),
            "Expected JOIN to be preserved (column in ORDER BY), got: {}",
            sql
        );
    }

    #[test]
    fn test_keep_left_join_used_in_other_join_condition() {
        let expr =
            parse("SELECT x.a FROM x LEFT JOIN y ON x.y_id = y.id LEFT JOIN z ON y.id = z.y_id");
        let result = eliminate_joins(expr);
        let sql = gen(&result);

        assert!(
            sql.contains("JOIN y"),
            "Expected JOIN y to be preserved (used in another JOIN ON), got: {}",
            sql
        );
    }
}
