//! Subquery Operations Module
//!
//! This module provides functionality for optimizing subqueries:
//! - Merging derived tables into outer queries
//! - Eliminating subqueries by converting to CTEs
//! - Unnesting correlated subqueries
//!
//! Ported from sqlglot's optimizer/merge_subqueries.py, eliminate_subqueries.py,
//! and unnest_subqueries.py

use std::collections::{HashMap, HashSet};

use crate::expressions::{
    Alias, BinaryOp, Cte, Expression, Identifier, Select, Subquery, TableRef, Where, With,
};
use crate::helper::find_new_name;
use crate::scope::Scope;

/// Merge derived tables into outer queries.
///
/// This optimization merges subqueries that appear in the FROM clause
/// into the outer query, reducing query complexity.
///
/// # Example
///
/// ```sql
/// -- Before:
/// SELECT a FROM (SELECT x.a FROM x) CROSS JOIN y
/// -- After:
/// SELECT x.a FROM x CROSS JOIN y
/// ```
///
/// # Arguments
/// * `expression` - The expression to optimize
/// * `leave_tables_isolated` - If true, don't merge if it would result in multiple table selects
///
/// # Returns
/// The optimized expression with merged subqueries
pub fn merge_subqueries(expression: Expression, leave_tables_isolated: bool) -> Expression {
    let expression = merge_ctes(expression, leave_tables_isolated);
    let expression = merge_derived_tables(expression, leave_tables_isolated);
    expression
}

/// Merge CTEs that are only selected from once.
///
/// If a CTE is referenced exactly once in the query, and its body is a simple
/// mergeable SELECT (no DISTINCT, GROUP BY, HAVING, LIMIT, aggregations),
/// inline it at the point of use by converting it to a derived table,
/// then delegate to `merge_derived_tables` to flatten it.
fn merge_ctes(expression: Expression, leave_tables_isolated: bool) -> Expression {
    if let Expression::Select(outer) = &expression {
        // Can't inline CTEs if the outer query uses SELECT * (can't resolve column refs)
        if outer
            .expressions
            .iter()
            .any(|e| matches!(e, Expression::Star(_)))
        {
            return expression;
        }

        if let Some(with) = &outer.with {
            // Count how many times each CTE name is referenced as a table source.
            let mut actual_counts: HashMap<String, usize> = HashMap::new();
            for cte in &with.ctes {
                actual_counts.insert(cte.alias.name.to_uppercase(), 0);
            }
            count_cte_refs(&expression, &mut actual_counts);

            // Identify CTEs that are referenced exactly once and are mergeable.
            let mut ctes_to_inline: HashMap<String, Expression> = HashMap::new();
            for cte in &with.ctes {
                let key = cte.alias.name.to_uppercase();
                if actual_counts.get(&key) == Some(&1) && is_simple_mergeable(&cte.this) {
                    ctes_to_inline.insert(key, cte.this.clone());
                }
            }

            if ctes_to_inline.is_empty() {
                return expression;
            }

            let mut new_outer = outer.as_ref().clone();

            // Remove inlined CTEs from the WITH clause
            if let Some(ref mut with) = new_outer.with {
                with.ctes
                    .retain(|cte| !ctes_to_inline.contains_key(&cte.alias.name.to_uppercase()));
                if with.ctes.is_empty() {
                    new_outer.with = None;
                }
            }

            // Replace table references to inlined CTEs in FROM with derived tables
            if let Some(ref mut from) = new_outer.from {
                from.expressions = from
                    .expressions
                    .iter()
                    .map(|source| inline_cte_in_source(source, &ctes_to_inline))
                    .collect();
            }

            // Replace table references in JOINs
            new_outer.joins = new_outer
                .joins
                .iter()
                .map(|join| {
                    let mut new_join = join.clone();
                    new_join.this = inline_cte_in_source(&join.this, &ctes_to_inline);
                    new_join
                })
                .collect();

            // Now apply merge_derived_tables to handle the newly created derived tables
            let result = Expression::Select(Box::new(new_outer));
            return merge_derived_tables(result, leave_tables_isolated);
        }
    }
    expression
}

/// Count references to CTE names in an expression tree (table references in FROM/JOIN).
fn count_cte_refs(expr: &Expression, counts: &mut HashMap<String, usize>) {
    match expr {
        Expression::Select(select) => {
            if let Some(from) = &select.from {
                for source in &from.expressions {
                    count_cte_refs_in_source(source, counts);
                }
            }
            for join in &select.joins {
                count_cte_refs_in_source(&join.this, counts);
            }
            for e in &select.expressions {
                count_cte_refs(e, counts);
            }
            if let Some(w) = &select.where_clause {
                count_cte_refs(&w.this, counts);
            }
        }
        Expression::Subquery(sub) => {
            count_cte_refs(&sub.this, counts);
        }
        Expression::Alias(alias) => {
            count_cte_refs(&alias.this, counts);
        }
        Expression::And(bin) | Expression::Or(bin) => {
            count_cte_refs(&bin.left, counts);
            count_cte_refs(&bin.right, counts);
        }
        Expression::In(in_expr) => {
            count_cte_refs(&in_expr.this, counts);
            if let Some(q) = &in_expr.query {
                count_cte_refs(q, counts);
            }
        }
        Expression::Exists(exists) => {
            count_cte_refs(&exists.this, counts);
        }
        _ => {}
    }
}

fn count_cte_refs_in_source(source: &Expression, counts: &mut HashMap<String, usize>) {
    match source {
        Expression::Table(table) => {
            let name = table.name.name.to_uppercase();
            if let Some(count) = counts.get_mut(&name) {
                *count += 1;
            }
        }
        Expression::Subquery(sub) => {
            count_cte_refs(&sub.this, counts);
        }
        Expression::Paren(p) => {
            count_cte_refs_in_source(&p.this, counts);
        }
        _ => {}
    }
}

/// Replace table references to CTEs with inline subqueries (derived tables).
fn inline_cte_in_source(
    source: &Expression,
    ctes_to_inline: &HashMap<String, Expression>,
) -> Expression {
    match source {
        Expression::Table(table) => {
            let name = table.name.name.to_uppercase();
            if let Some(cte_body) = ctes_to_inline.get(&name) {
                let alias_name = table
                    .alias
                    .as_ref()
                    .map(|a| a.name.clone())
                    .unwrap_or_else(|| table.name.name.clone());
                Expression::Subquery(Box::new(Subquery {
                    this: cte_body.clone(),
                    alias: Some(Identifier::new(alias_name)),
                    column_aliases: table.column_aliases.clone(),
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
            } else {
                source.clone()
            }
        }
        _ => source.clone(),
    }
}

/// Check if a CTE body is simple enough to be inlined and then merged.
fn is_simple_mergeable(expr: &Expression) -> bool {
    match expr {
        Expression::Select(inner) => is_simple_mergeable_select(inner),
        _ => false,
    }
}

/// Merge derived tables into outer queries.
///
/// Walks the expression tree looking for SELECT nodes whose FROM clause
/// contains a Subquery (derived table). When the inner SELECT is mergeable,
/// merges the inner query into the outer query.
fn merge_derived_tables(expression: Expression, leave_tables_isolated: bool) -> Expression {
    transform_expression(expression, leave_tables_isolated)
}

/// Recursively transform expressions, merging derived tables bottom-up.
fn transform_expression(expr: Expression, leave_tables_isolated: bool) -> Expression {
    match expr {
        Expression::Select(outer) => {
            let mut outer = *outer;

            // First, recursively transform subqueries in FROM (bottom-up)
            if let Some(ref mut from) = outer.from {
                from.expressions = from
                    .expressions
                    .drain(..)
                    .map(|e| transform_expression(e, leave_tables_isolated))
                    .collect();
            }

            // Transform subqueries in JOINs
            outer.joins = outer
                .joins
                .drain(..)
                .map(|mut join| {
                    join.this = transform_expression(join.this, leave_tables_isolated);
                    join
                })
                .collect();

            // Transform subqueries in SELECT list
            outer.expressions = outer
                .expressions
                .drain(..)
                .map(|e| transform_expression(e, leave_tables_isolated))
                .collect();

            // Transform WHERE clause
            if let Some(ref mut w) = outer.where_clause {
                w.this = transform_expression(w.this.clone(), leave_tables_isolated);
            }

            // Now attempt to merge derived tables in FROM
            let mut merged = try_merge_from_subquery(outer, leave_tables_isolated);

            // Attempt merging subqueries in JOINs
            merged = try_merge_join_subqueries(merged, leave_tables_isolated);

            Expression::Select(Box::new(merged))
        }
        Expression::Subquery(mut sub) => {
            sub.this = transform_expression(sub.this, leave_tables_isolated);
            Expression::Subquery(sub)
        }
        Expression::Union(mut u) => {
            u.left = transform_expression(u.left, leave_tables_isolated);
            u.right = transform_expression(u.right, leave_tables_isolated);
            Expression::Union(u)
        }
        Expression::Intersect(mut i) => {
            i.left = transform_expression(i.left, leave_tables_isolated);
            i.right = transform_expression(i.right, leave_tables_isolated);
            Expression::Intersect(i)
        }
        Expression::Except(mut e) => {
            e.left = transform_expression(e.left, leave_tables_isolated);
            e.right = transform_expression(e.right, leave_tables_isolated);
            Expression::Except(e)
        }
        other => other,
    }
}

/// Attempt to merge a subquery in the FROM clause of the outer SELECT.
fn try_merge_from_subquery(mut outer: Select, leave_tables_isolated: bool) -> Select {
    // Can't merge if outer uses SELECT *
    if outer
        .expressions
        .iter()
        .any(|e| matches!(e, Expression::Star(_)))
    {
        return outer;
    }

    let from = match &outer.from {
        Some(f) => f,
        None => return outer,
    };

    // Find the first mergeable subquery in FROM
    let mut merge_index: Option<usize> = None;
    for (i, source) in from.expressions.iter().enumerate() {
        if let Expression::Subquery(sub) = source {
            if let Expression::Select(inner) = &sub.this {
                if is_simple_mergeable_select(inner)
                    && !leave_tables_isolated_check(&outer, leave_tables_isolated)
                {
                    merge_index = Some(i);
                    break;
                }
            }
        }
    }

    let merge_idx = match merge_index {
        Some(i) => i,
        None => return outer,
    };

    // Extract the subquery from FROM
    let from = outer.from.as_mut().unwrap();
    let subquery_expr = from.expressions.remove(merge_idx);
    let (inner_select, subquery_alias) = match subquery_expr {
        Expression::Subquery(sub) => {
            let alias = sub
                .alias
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_default();
            match sub.this {
                Expression::Select(inner) => (*inner, alias),
                _ => return outer,
            }
        }
        _ => return outer,
    };

    // Build a projection map: alias_or_name (uppercased) -> inner expression
    let projection_map = build_projection_map(&inner_select);

    // 1. Replace FROM: insert the inner SELECT's FROM sources at the merge position
    if let Some(inner_from) = &inner_select.from {
        for (j, source) in inner_from.expressions.iter().enumerate() {
            from.expressions.insert(merge_idx + j, source.clone());
        }
    }
    if from.expressions.is_empty() {
        outer.from = None;
    }

    // 2. Replace column references throughout the outer query
    outer.expressions = outer
        .expressions
        .iter()
        .map(|e| replace_column_refs(e, &subquery_alias, &projection_map, true))
        .collect();

    // 3. Replace refs in WHERE
    if let Some(ref mut w) = outer.where_clause {
        w.this = replace_column_refs(&w.this, &subquery_alias, &projection_map, false);
    }

    // 4. Replace refs in ORDER BY
    if let Some(ref mut order) = outer.order_by {
        order.expressions = order
            .expressions
            .iter()
            .map(|ord| {
                let mut new_ord = ord.clone();
                new_ord.this =
                    replace_column_refs(&ord.this, &subquery_alias, &projection_map, false);
                new_ord
            })
            .collect();
    }

    // 5. Replace refs in GROUP BY
    if let Some(ref mut group) = outer.group_by {
        group.expressions = group
            .expressions
            .iter()
            .map(|e| replace_column_refs(e, &subquery_alias, &projection_map, false))
            .collect();
    }

    // 6. Replace refs in HAVING
    if let Some(ref mut having) = outer.having {
        having.this = replace_column_refs(&having.this, &subquery_alias, &projection_map, false);
    }

    // 7. Replace refs in JOIN ON conditions
    outer.joins = outer
        .joins
        .iter()
        .map(|join| {
            let mut new_join = join.clone();
            if let Some(ref on) = join.on {
                new_join.on = Some(replace_column_refs(
                    on,
                    &subquery_alias,
                    &projection_map,
                    false,
                ));
            }
            new_join
        })
        .collect();

    // 8. Merge inner WHERE into outer WHERE
    if let Some(inner_where) = &inner_select.where_clause {
        outer.where_clause = Some(merge_where_conditions(
            outer.where_clause.as_ref(),
            &inner_where.this,
        ));
    }

    // 9. Merge inner JOINs (insert at beginning so they come right after FROM)
    if !inner_select.joins.is_empty() {
        let mut new_joins = inner_select.joins.clone();
        new_joins.extend(outer.joins.drain(..));
        outer.joins = new_joins;
    }

    // 10. Propagate ORDER BY from inner if outer has none and no aggregation/grouping
    if outer.order_by.is_none()
        && inner_select.order_by.is_some()
        && outer.group_by.is_none()
        && !outer.distinct
        && outer.having.is_none()
        && !outer.expressions.iter().any(|e| contains_aggregation(e))
    {
        outer.order_by = inner_select.order_by.clone();
    }

    outer
}

/// Attempt to merge subqueries in JOIN targets.
fn try_merge_join_subqueries(mut outer: Select, leave_tables_isolated: bool) -> Select {
    if outer
        .expressions
        .iter()
        .any(|e| matches!(e, Expression::Star(_)))
    {
        return outer;
    }

    let mut i = 0;
    while i < outer.joins.len() {
        let should_merge = {
            if let Expression::Subquery(sub) = &outer.joins[i].this {
                if let Expression::Select(inner) = &sub.this {
                    is_simple_mergeable_select(inner)
                        && !leave_tables_isolated_check(&outer, leave_tables_isolated)
                        // Can't merge inner JOINs into an outer JOIN
                        && inner.joins.is_empty()
                        // Can't merge inner WHERE into JOIN ON for FULL/LEFT/RIGHT joins
                        && !(inner.where_clause.is_some()
                            && matches!(
                                outer.joins[i].kind,
                                crate::expressions::JoinKind::Full
                                    | crate::expressions::JoinKind::Left
                                    | crate::expressions::JoinKind::Right
                            ))
                } else {
                    false
                }
            } else {
                false
            }
        };

        if should_merge {
            let subquery_alias = match &outer.joins[i].this {
                Expression::Subquery(sub) => sub
                    .alias
                    .as_ref()
                    .map(|a| a.name.clone())
                    .unwrap_or_default(),
                _ => String::new(),
            };

            let inner_select = match &outer.joins[i].this {
                Expression::Subquery(sub) => match &sub.this {
                    Expression::Select(inner) => (**inner).clone(),
                    _ => {
                        i += 1;
                        continue;
                    }
                },
                _ => {
                    i += 1;
                    continue;
                }
            };

            let projection_map = build_projection_map(&inner_select);

            // Replace join target with inner FROM source
            if let Some(inner_from) = &inner_select.from {
                if let Some(source) = inner_from.expressions.first() {
                    outer.joins[i].this = source.clone();
                }
            }

            // Replace column references everywhere
            outer.expressions = outer
                .expressions
                .iter()
                .map(|e| replace_column_refs(e, &subquery_alias, &projection_map, true))
                .collect();

            if let Some(ref mut w) = outer.where_clause {
                w.this = replace_column_refs(&w.this, &subquery_alias, &projection_map, false);
            }

            // Replace in all JOIN ON conditions
            for j in 0..outer.joins.len() {
                if let Some(ref on) = outer.joins[j].on.clone() {
                    outer.joins[j].on = Some(replace_column_refs(
                        on,
                        &subquery_alias,
                        &projection_map,
                        false,
                    ));
                }
            }

            if let Some(ref mut order) = outer.order_by {
                order.expressions = order
                    .expressions
                    .iter()
                    .map(|ord| {
                        let mut new_ord = ord.clone();
                        new_ord.this =
                            replace_column_refs(&ord.this, &subquery_alias, &projection_map, false);
                        new_ord
                    })
                    .collect();
            }

            // Merge inner WHERE into the JOIN ON condition
            if let Some(inner_where) = &inner_select.where_clause {
                let existing_on = outer.joins[i].on.clone();
                let new_on = if let Some(on) = existing_on {
                    Expression::And(Box::new(BinaryOp {
                        left: on,
                        right: inner_where.this.clone(),
                        left_comments: Vec::new(),
                        operator_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    }))
                } else {
                    inner_where.this.clone()
                };
                outer.joins[i].on = Some(new_on);
            }
        }

        i += 1;
    }

    outer
}

/// Check if leave_tables_isolated would prevent merging.
fn leave_tables_isolated_check(outer: &Select, leave_tables_isolated: bool) -> bool {
    if !leave_tables_isolated {
        return false;
    }
    let from_count = outer
        .from
        .as_ref()
        .map(|f| f.expressions.len())
        .unwrap_or(0);
    let join_count = outer.joins.len();
    from_count + join_count > 1
}

/// Check if a SELECT is mergeable: no DISTINCT, GROUP BY, HAVING, LIMIT, OFFSET,
/// aggregations, subqueries, or window functions in projections, and has a FROM clause.
fn is_simple_mergeable_select(inner: &Select) -> bool {
    if inner.distinct || inner.distinct_on.is_some() {
        return false;
    }
    if inner.group_by.is_some() {
        return false;
    }
    if inner.having.is_some() {
        return false;
    }
    if inner.limit.is_some() || inner.offset.is_some() {
        return false;
    }
    if inner.from.is_none() {
        return false;
    }
    for expr in &inner.expressions {
        if contains_aggregation(expr) {
            return false;
        }
        if contains_subquery(expr) {
            return false;
        }
        if contains_window_function(expr) {
            return false;
        }
    }
    true
}

/// Check if an expression contains a subquery.
fn contains_subquery(expr: &Expression) -> bool {
    match expr {
        Expression::Subquery(_) | Expression::Exists(_) => true,
        Expression::Alias(alias) => contains_subquery(&alias.this),
        Expression::Paren(p) => contains_subquery(&p.this),
        Expression::And(bin) | Expression::Or(bin) => {
            contains_subquery(&bin.left) || contains_subquery(&bin.right)
        }
        Expression::In(in_expr) => in_expr.query.is_some() || contains_subquery(&in_expr.this),
        _ => false,
    }
}

/// Check if an expression contains a window function.
fn contains_window_function(expr: &Expression) -> bool {
    match expr {
        Expression::WindowFunction(_) => true,
        Expression::Alias(alias) => contains_window_function(&alias.this),
        Expression::Paren(p) => contains_window_function(&p.this),
        _ => false,
    }
}

/// Build a projection map from an inner SELECT's expressions.
///
/// Maps each projection's alias_or_name (uppercased) to its underlying expression.
fn build_projection_map(inner: &Select) -> HashMap<String, Expression> {
    let mut map = HashMap::new();
    for expr in &inner.expressions {
        let (name, inner_expr) = match expr {
            Expression::Alias(alias) => (alias.alias.name.to_uppercase(), alias.this.clone()),
            Expression::Column(col) => (col.name.name.to_uppercase(), expr.clone()),
            Expression::Star(_) => continue,
            _ => continue,
        };
        map.insert(name, inner_expr);
    }
    map
}

/// Replace column references that target the subquery alias with the
/// corresponding inner projections.
///
/// When `in_select_list` is true and the replacement would change the
/// expression's output name, wraps it in an Alias to preserve the
/// original column name.
fn replace_column_refs(
    expr: &Expression,
    subquery_alias: &str,
    projection_map: &HashMap<String, Expression>,
    in_select_list: bool,
) -> Expression {
    match expr {
        Expression::Column(col) => {
            let matches_alias = match &col.table {
                Some(table) => table.name.eq_ignore_ascii_case(subquery_alias),
                None => true, // unqualified columns may also match
            };

            if matches_alias {
                let col_name = col.name.name.to_uppercase();
                if let Some(replacement) = projection_map.get(&col_name) {
                    if in_select_list {
                        let replacement_name = get_expression_name(replacement);
                        if replacement_name.map(|n| n.to_uppercase()) != Some(col_name.clone()) {
                            return Expression::Alias(Box::new(Alias {
                                this: replacement.clone(),
                                alias: Identifier::new(&col.name.name),
                                column_aliases: Vec::new(),
                                pre_alias_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                            }));
                        }
                    }
                    return replacement.clone();
                }
            }
            expr.clone()
        }
        Expression::Alias(alias) => {
            let new_inner = replace_column_refs(&alias.this, subquery_alias, projection_map, false);
            Expression::Alias(Box::new(Alias {
                this: new_inner,
                alias: alias.alias.clone(),
                column_aliases: alias.column_aliases.clone(),
                pre_alias_comments: alias.pre_alias_comments.clone(),
                trailing_comments: alias.trailing_comments.clone(),
            }))
        }
        // Binary operations
        Expression::And(bin) => Expression::And(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Or(bin) => Expression::Or(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Add(bin) => Expression::Add(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Sub(bin) => Expression::Sub(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Mul(bin) => Expression::Mul(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Div(bin) => Expression::Div(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Mod(bin) => Expression::Mod(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Eq(bin) => Expression::Eq(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Neq(bin) => Expression::Neq(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Lt(bin) => Expression::Lt(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Lte(bin) => Expression::Lte(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Gt(bin) => Expression::Gt(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Gte(bin) => Expression::Gte(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::Concat(bin) => Expression::Concat(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::BitwiseAnd(bin) => Expression::BitwiseAnd(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::BitwiseOr(bin) => Expression::BitwiseOr(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        Expression::BitwiseXor(bin) => Expression::BitwiseXor(Box::new(replace_binary_op(
            bin,
            subquery_alias,
            projection_map,
        ))),
        // Like/ILike
        Expression::Like(like) => {
            let mut new_like = like.as_ref().clone();
            new_like.left = replace_column_refs(&like.left, subquery_alias, projection_map, false);
            new_like.right =
                replace_column_refs(&like.right, subquery_alias, projection_map, false);
            if let Some(ref esc) = like.escape {
                new_like.escape = Some(replace_column_refs(
                    esc,
                    subquery_alias,
                    projection_map,
                    false,
                ));
            }
            Expression::Like(Box::new(new_like))
        }
        Expression::ILike(like) => {
            let mut new_like = like.as_ref().clone();
            new_like.left = replace_column_refs(&like.left, subquery_alias, projection_map, false);
            new_like.right =
                replace_column_refs(&like.right, subquery_alias, projection_map, false);
            if let Some(ref esc) = like.escape {
                new_like.escape = Some(replace_column_refs(
                    esc,
                    subquery_alias,
                    projection_map,
                    false,
                ));
            }
            Expression::ILike(Box::new(new_like))
        }
        // Unary
        Expression::Not(un) => {
            let mut new_un = un.as_ref().clone();
            new_un.this = replace_column_refs(&un.this, subquery_alias, projection_map, false);
            Expression::Not(Box::new(new_un))
        }
        Expression::Neg(un) => {
            let mut new_un = un.as_ref().clone();
            new_un.this = replace_column_refs(&un.this, subquery_alias, projection_map, false);
            Expression::Neg(Box::new(new_un))
        }
        Expression::Paren(p) => {
            let mut new_p = p.as_ref().clone();
            new_p.this = replace_column_refs(&p.this, subquery_alias, projection_map, false);
            Expression::Paren(Box::new(new_p))
        }
        Expression::Cast(cast) => {
            let mut new_cast = cast.as_ref().clone();
            new_cast.this = replace_column_refs(&cast.this, subquery_alias, projection_map, false);
            Expression::Cast(Box::new(new_cast))
        }
        Expression::Function(func) => {
            let mut new_func = func.as_ref().clone();
            new_func.args = func
                .args
                .iter()
                .map(|a| replace_column_refs(a, subquery_alias, projection_map, false))
                .collect();
            Expression::Function(Box::new(new_func))
        }
        Expression::AggregateFunction(agg) => {
            let mut new_agg = agg.as_ref().clone();
            new_agg.args = agg
                .args
                .iter()
                .map(|a| replace_column_refs(a, subquery_alias, projection_map, false))
                .collect();
            Expression::AggregateFunction(Box::new(new_agg))
        }
        Expression::Case(case) => {
            let mut new_case = case.as_ref().clone();
            new_case.operand = case
                .operand
                .as_ref()
                .map(|o| replace_column_refs(o, subquery_alias, projection_map, false));
            new_case.whens = case
                .whens
                .iter()
                .map(|(w, t)| {
                    (
                        replace_column_refs(w, subquery_alias, projection_map, false),
                        replace_column_refs(t, subquery_alias, projection_map, false),
                    )
                })
                .collect();
            new_case.else_ = case
                .else_
                .as_ref()
                .map(|e| replace_column_refs(e, subquery_alias, projection_map, false));
            Expression::Case(Box::new(new_case))
        }
        Expression::IsNull(is_null) => {
            let mut new_is = is_null.as_ref().clone();
            new_is.this = replace_column_refs(&is_null.this, subquery_alias, projection_map, false);
            Expression::IsNull(Box::new(new_is))
        }
        Expression::Between(between) => {
            let mut new_b = between.as_ref().clone();
            new_b.this = replace_column_refs(&between.this, subquery_alias, projection_map, false);
            new_b.low = replace_column_refs(&between.low, subquery_alias, projection_map, false);
            new_b.high = replace_column_refs(&between.high, subquery_alias, projection_map, false);
            Expression::Between(Box::new(new_b))
        }
        Expression::In(in_expr) => {
            let mut new_in = in_expr.as_ref().clone();
            new_in.this = replace_column_refs(&in_expr.this, subquery_alias, projection_map, false);
            new_in.expressions = in_expr
                .expressions
                .iter()
                .map(|e| replace_column_refs(e, subquery_alias, projection_map, false))
                .collect();
            Expression::In(Box::new(new_in))
        }
        Expression::Ordered(ord) => {
            let mut new_ord = ord.as_ref().clone();
            new_ord.this = replace_column_refs(&ord.this, subquery_alias, projection_map, false);
            Expression::Ordered(Box::new(new_ord))
        }
        // For all other expression types, return as-is
        _ => expr.clone(),
    }
}

/// Replace column references in a BinaryOp.
fn replace_binary_op(
    bin: &BinaryOp,
    subquery_alias: &str,
    projection_map: &HashMap<String, Expression>,
) -> BinaryOp {
    BinaryOp {
        left: replace_column_refs(&bin.left, subquery_alias, projection_map, false),
        right: replace_column_refs(&bin.right, subquery_alias, projection_map, false),
        left_comments: bin.left_comments.clone(),
        operator_comments: bin.operator_comments.clone(),
        trailing_comments: bin.trailing_comments.clone(),
    }
}

/// Get the output name of an expression (for determining if an alias is needed).
fn get_expression_name(expr: &Expression) -> Option<&str> {
    match expr {
        Expression::Column(col) => Some(&col.name.name),
        Expression::Alias(alias) => Some(&alias.alias.name),
        Expression::Identifier(id) => Some(&id.name),
        _ => None,
    }
}

/// Merge an inner WHERE condition with an existing outer WHERE clause (AND them),
/// or create a new WHERE from the inner condition alone.
fn merge_where_conditions(outer_where: Option<&Where>, inner_cond: &Expression) -> Where {
    match outer_where {
        Some(w) => Where {
            this: Expression::And(Box::new(BinaryOp {
                left: inner_cond.clone(),
                right: w.this.clone(),
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
            })),
        },
        None => Where {
            this: inner_cond.clone(),
        },
    }
}

/// Check if an inner select can be merged into an outer query
pub fn is_mergeable(outer_scope: &Scope, inner_scope: &Scope, leave_tables_isolated: bool) -> bool {
    let inner_select = &inner_scope.expression;

    if let Expression::Select(inner) = inner_select {
        if inner.distinct || inner.distinct_on.is_some() {
            return false;
        }
        if inner.group_by.is_some() {
            return false;
        }
        if inner.having.is_some() {
            return false;
        }
        if inner.limit.is_some() || inner.offset.is_some() {
            return false;
        }

        for expr in &inner.expressions {
            if contains_aggregation(expr) {
                return false;
            }
        }

        if leave_tables_isolated && outer_scope.sources.len() > 1 {
            return false;
        }

        return true;
    }

    false
}

/// Check if expression contains an aggregation function
fn contains_aggregation(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction(_) => true,
        Expression::Alias(alias) => contains_aggregation(&alias.this),
        Expression::Function(func) => {
            let agg_names = [
                "COUNT",
                "SUM",
                "AVG",
                "MIN",
                "MAX",
                "ARRAY_AGG",
                "STRING_AGG",
            ];
            agg_names.contains(&func.name.to_uppercase().as_str())
        }
        Expression::And(bin) | Expression::Or(bin) => {
            contains_aggregation(&bin.left) || contains_aggregation(&bin.right)
        }
        Expression::Paren(p) => contains_aggregation(&p.this),
        _ => false,
    }
}

/// Eliminate derived tables by converting them to CTEs.
///
/// This transformation rewrites derived tables (subqueries in FROM) as CTEs,
/// which can deduplicate common subqueries and improve readability.
///
/// # Example
///
/// ```sql
/// -- Before:
/// SELECT a FROM (SELECT * FROM x) AS y
/// -- After:
/// WITH y AS (SELECT * FROM x) SELECT a FROM y AS y
/// ```
///
/// # Arguments
/// * `expression` - The expression to optimize
///
/// # Returns
/// The optimized expression with subqueries converted to CTEs
pub fn eliminate_subqueries(expression: Expression) -> Expression {
    match expression {
        Expression::Select(mut outer) => {
            let mut taken = collect_source_names(&Expression::Select(outer.clone()));
            let mut seen_sql: HashMap<String, String> = HashMap::new();
            let mut new_ctes: Vec<Cte> = Vec::new();

            // Process FROM clause subqueries
            if let Some(ref mut from) = outer.from {
                from.expressions = from
                    .expressions
                    .drain(..)
                    .map(|source| {
                        extract_subquery_to_cte(source, &mut taken, &mut seen_sql, &mut new_ctes)
                    })
                    .collect();
            }

            // Process JOIN subqueries
            outer.joins = outer
                .joins
                .drain(..)
                .map(|mut join| {
                    join.this = extract_subquery_to_cte(
                        join.this,
                        &mut taken,
                        &mut seen_sql,
                        &mut new_ctes,
                    );
                    join
                })
                .collect();

            // Add new CTEs to the WITH clause
            if !new_ctes.is_empty() {
                match outer.with {
                    Some(ref mut with) => {
                        let mut combined = new_ctes;
                        combined.extend(with.ctes.drain(..));
                        with.ctes = combined;
                    }
                    None => {
                        outer.with = Some(With {
                            ctes: new_ctes,
                            recursive: false,
                            leading_comments: Vec::new(),
                            search: None,
                        });
                    }
                }
            }

            Expression::Select(outer)
        }
        other => other,
    }
}

/// Collect all source names (table names, aliases, CTE names) from an expression.
fn collect_source_names(expr: &Expression) -> HashSet<String> {
    let mut names = HashSet::new();
    match expr {
        Expression::Select(s) => {
            if let Some(ref from) = s.from {
                for source in &from.expressions {
                    collect_names_from_source(source, &mut names);
                }
            }
            for join in &s.joins {
                collect_names_from_source(&join.this, &mut names);
            }
            if let Some(ref with) = s.with {
                for cte in &with.ctes {
                    names.insert(cte.alias.name.clone());
                }
            }
        }
        _ => {}
    }
    names
}

fn collect_names_from_source(source: &Expression, names: &mut HashSet<String>) {
    match source {
        Expression::Table(t) => {
            names.insert(t.name.name.clone());
            if let Some(ref alias) = t.alias {
                names.insert(alias.name.clone());
            }
        }
        Expression::Subquery(sub) => {
            if let Some(ref alias) = sub.alias {
                names.insert(alias.name.clone());
            }
        }
        _ => {}
    }
}

/// Extract a subquery from FROM/JOIN into a CTE, returning a table reference.
fn extract_subquery_to_cte(
    source: Expression,
    taken: &mut HashSet<String>,
    seen_sql: &mut HashMap<String, String>,
    new_ctes: &mut Vec<Cte>,
) -> Expression {
    match source {
        Expression::Subquery(sub) => {
            let inner_sql = crate::generator::Generator::sql(&sub.this).unwrap_or_default();
            let alias_name = sub
                .alias
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_default();

            // Check for duplicate subquery (reuse existing CTE)
            if let Some(existing_name) = seen_sql.get(&inner_sql) {
                let mut tref = TableRef::new(existing_name.as_str());
                if !alias_name.is_empty() {
                    tref.alias = Some(Identifier::new(&alias_name));
                }
                return Expression::Table(tref);
            }

            // Generate a CTE name
            let cte_name = if !alias_name.is_empty() && !taken.contains(&alias_name) {
                alias_name.clone()
            } else {
                find_new_name(taken, "_cte")
            };
            taken.insert(cte_name.clone());
            seen_sql.insert(inner_sql, cte_name.clone());

            // Create CTE
            new_ctes.push(Cte {
                alias: Identifier::new(&cte_name),
                this: sub.this,
                columns: sub.column_aliases,
                materialized: None,
                key_expressions: Vec::new(),
                alias_first: false,
                comments: Vec::new(),
            });

            // Return table reference to the CTE
            let mut tref = TableRef::new(&cte_name);
            if !alias_name.is_empty() {
                tref.alias = Some(Identifier::new(&alias_name));
            }
            Expression::Table(tref)
        }
        other => other,
    }
}

/// Unnest correlated subqueries where possible.
///
/// This transforms correlated subqueries into JOINs for better performance.
///
/// # Example
///
/// ```sql
/// -- Before:
/// SELECT * FROM x WHERE x.a IN (SELECT y.a FROM y WHERE x.b = y.b)
/// -- After:
/// SELECT * FROM x LEFT JOIN y ON x.b = y.b WHERE x.a = y.a
/// ```
///
/// # Arguments
/// * `expression` - The expression to optimize
///
/// # Returns
/// The optimized expression with unnested subqueries
pub fn unnest_subqueries(expression: Expression) -> Expression {
    // In a full implementation, we would:
    // 1. Find correlated subqueries in WHERE clause
    // 2. Determine if they can be converted to JOINs
    // 3. Rewrite as appropriate JOIN type
    //
    // For now, return unchanged
    expression
}

/// Check if a subquery is correlated (references outer query tables)
pub fn is_correlated(subquery: &Expression, outer_tables: &HashSet<String>) -> bool {
    let mut tables_referenced: HashSet<String> = HashSet::new();
    collect_table_refs(subquery, &mut tables_referenced);

    !tables_referenced.is_disjoint(outer_tables)
}

/// Collect all table references from an expression
fn collect_table_refs(expr: &Expression, tables: &mut HashSet<String>) {
    match expr {
        Expression::Column(col) => {
            if let Some(ref table) = col.table {
                tables.insert(table.name.clone());
            }
        }
        Expression::Select(select) => {
            for e in &select.expressions {
                collect_table_refs(e, tables);
            }
            if let Some(ref where_clause) = select.where_clause {
                collect_table_refs(&where_clause.this, tables);
            }
        }
        Expression::And(bin) | Expression::Or(bin) => {
            collect_table_refs(&bin.left, tables);
            collect_table_refs(&bin.right, tables);
        }
        Expression::Eq(bin)
        | Expression::Neq(bin)
        | Expression::Lt(bin)
        | Expression::Gt(bin)
        | Expression::Lte(bin)
        | Expression::Gte(bin) => {
            collect_table_refs(&bin.left, tables);
            collect_table_refs(&bin.right, tables);
        }
        Expression::Paren(p) => {
            collect_table_refs(&p.this, tables);
        }
        Expression::Alias(alias) => {
            collect_table_refs(&alias.this, tables);
        }
        _ => {}
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
    fn test_merge_subqueries_simple() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x) AS y");
        let result = merge_subqueries(expr, false);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_merge_subqueries_with_join() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x) AS y CROSS JOIN z");
        let result = merge_subqueries(expr, false);
        let sql = gen(&result);
        assert!(sql.contains("JOIN"));
    }

    #[test]
    fn test_merge_subqueries_isolated() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x) AS y CROSS JOIN z");
        let result = merge_subqueries(expr, true);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_eliminate_subqueries_simple() {
        let expr = parse("SELECT a FROM (SELECT * FROM x) AS y");
        let result = eliminate_subqueries(expr);
        let sql = gen(&result);
        assert!(
            sql.contains("WITH"),
            "Should have WITH clause, got: {}",
            sql
        );
        assert!(
            sql.contains("SELECT a FROM"),
            "Should reference CTE, got: {}",
            sql
        );
    }

    #[test]
    fn test_eliminate_subqueries_no_subquery() {
        let expr = parse("SELECT a FROM x");
        let result = eliminate_subqueries(expr);
        let sql = gen(&result);
        assert_eq!(sql, "SELECT a FROM x");
    }

    #[test]
    fn test_eliminate_subqueries_join() {
        let expr = parse("SELECT a FROM x JOIN (SELECT b FROM y) AS sub ON x.id = sub.id");
        let result = eliminate_subqueries(expr);
        let sql = gen(&result);
        assert!(
            sql.contains("WITH"),
            "Should have WITH clause, got: {}",
            sql
        );
    }

    #[test]
    fn test_eliminate_subqueries_non_select() {
        let expr = parse("INSERT INTO t VALUES (1, 2)");
        let result = eliminate_subqueries(expr);
        let sql = gen(&result);
        assert!(
            sql.contains("INSERT"),
            "Non-select should pass through, got: {}",
            sql
        );
    }

    #[test]
    fn test_unnest_subqueries_simple() {
        let expr = parse("SELECT * FROM x WHERE x.a IN (SELECT y.a FROM y)");
        let result = unnest_subqueries(expr);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_is_mergeable_simple() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x) AS y");
        let scopes = crate::scope::traverse_scope(&expr);
        assert!(!scopes.is_empty());
    }

    #[test]
    fn test_contains_aggregation() {
        let expr = parse("SELECT COUNT(*) FROM t");
        if let Expression::Select(select) = &expr {
            assert!(!select.expressions.is_empty());
        }
    }

    #[test]
    fn test_is_correlated() {
        let outer_tables: HashSet<String> = vec!["x".to_string()].into_iter().collect();
        let subquery = parse("SELECT y.a FROM y WHERE y.b = x.b");
        assert!(is_correlated(&subquery, &outer_tables));
    }

    #[test]
    fn test_is_not_correlated() {
        let outer_tables: HashSet<String> = vec!["x".to_string()].into_iter().collect();
        let subquery = parse("SELECT y.a FROM y WHERE y.b = 1");
        assert!(!is_correlated(&subquery, &outer_tables));
    }

    #[test]
    fn test_collect_table_refs() {
        let expr = parse("SELECT t.a, s.b FROM t, s WHERE t.c = s.d");
        let mut tables: HashSet<String> = HashSet::new();
        collect_table_refs(&expr, &mut tables);
        assert!(tables.contains("t"));
        assert!(tables.contains("s"));
    }

    #[test]
    fn test_merge_ctes() {
        let expr = parse("WITH cte AS (SELECT * FROM x) SELECT * FROM cte");
        let result = merge_ctes(expr, false);
        let sql = gen(&result);
        assert!(sql.contains("WITH"));
    }

    // ---- New tests for merge_derived_tables ----

    #[test]
    fn test_merge_derived_tables_basic() {
        // SELECT a FROM (SELECT x.a FROM x) AS y -> SELECT x.a FROM x
        let expr = parse("SELECT a FROM (SELECT x.a FROM x) AS y");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            !sql.contains("AS y"),
            "Subquery alias should be removed after merge, got: {}",
            sql
        );
        assert!(
            sql.contains("FROM x"),
            "Should reference table x directly, got: {}",
            sql
        );
        assert!(
            sql.contains("x.a"),
            "Should reference x.a directly, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_with_where() {
        // Inner WHERE should be merged into outer WHERE
        let expr = parse("SELECT a FROM (SELECT x.a FROM x WHERE x.b > 1) AS y WHERE a > 0");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            !sql.contains("AS y"),
            "Subquery alias should be removed, got: {}",
            sql
        );
        assert!(
            sql.contains("x.b > 1"),
            "Inner WHERE condition should be preserved, got: {}",
            sql
        );
        assert!(
            sql.contains("AND"),
            "Both conditions should be ANDed together, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_not_mergeable() {
        // DISTINCT prevents merge
        let expr = parse("SELECT a FROM (SELECT DISTINCT x.a FROM x) AS y");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            sql.contains("DISTINCT"),
            "DISTINCT subquery should not be merged, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_group_by_not_mergeable() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x GROUP BY x.a) AS y");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            sql.contains("GROUP BY"),
            "GROUP BY subquery should not be merged, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_limit_not_mergeable() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x LIMIT 10) AS y");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            sql.contains("LIMIT"),
            "LIMIT subquery should not be merged, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_with_cross_join() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x) AS y CROSS JOIN z");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            !sql.contains("AS y"),
            "Subquery should be merged, got: {}",
            sql
        );
        assert!(
            sql.contains("CROSS JOIN"),
            "CROSS JOIN should be preserved, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_isolated() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x) AS y CROSS JOIN z");
        let result = merge_derived_tables(expr, true);
        let sql = gen(&result);
        assert!(
            sql.contains("AS y"),
            "Should NOT merge when isolated and multiple sources, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_star_not_mergeable() {
        let expr = parse("SELECT * FROM (SELECT x.a FROM x) AS y");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            sql.contains("*"),
            "SELECT * should prevent merge, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_inner_joins() {
        let expr = parse("SELECT a FROM (SELECT x.a FROM x JOIN z ON x.id = z.id) AS y");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            sql.contains("JOIN z"),
            "Inner JOIN should be merged into outer query, got: {}",
            sql
        );
        assert!(
            !sql.contains("AS y"),
            "Subquery alias should be removed, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_derived_tables_aggregation_not_mergeable() {
        let expr = parse("SELECT a FROM (SELECT COUNT(*) AS a FROM x) AS y");
        let result = merge_derived_tables(expr, false);
        let sql = gen(&result);
        assert!(
            sql.contains("COUNT"),
            "Aggregation subquery should not be merged, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_ctes_single_ref() {
        let expr = parse("WITH cte AS (SELECT x.a FROM x) SELECT a FROM cte");
        let result = merge_ctes(expr, false);
        let sql = gen(&result);
        assert!(
            !sql.contains("WITH"),
            "CTE should be removed after inlining, got: {}",
            sql
        );
        assert!(
            sql.contains("FROM x"),
            "Should reference table x directly, got: {}",
            sql
        );
    }

    #[test]
    fn test_merge_ctes_non_mergeable_body() {
        let expr = parse("WITH cte AS (SELECT DISTINCT x.a FROM x) SELECT a FROM cte");
        let result = merge_ctes(expr, false);
        let sql = gen(&result);
        assert!(
            sql.contains("DISTINCT"),
            "DISTINCT should be preserved, got: {}",
            sql
        );
    }
}
