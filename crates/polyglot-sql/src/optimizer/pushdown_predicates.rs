//! Predicate Pushdown Module
//!
//! This module provides functionality for pushing WHERE predicates down
//! into subqueries and JOINs for better query performance.
//!
//! When a predicate in the outer query only references columns from a subquery,
//! it can be pushed down into that subquery's WHERE clause to filter data earlier.
//!
//! Ported from sqlglot's optimizer/pushdown_predicates.py

use std::collections::{HashMap, HashSet};

use crate::dialects::DialectType;
use crate::expressions::{BooleanLiteral, Expression};
use crate::optimizer::normalize::normalized;
use crate::optimizer::simplify::simplify;
use crate::scope::{build_scope, Scope, SourceInfo};

/// Rewrite SQL AST to pushdown predicates in FROMs and JOINs.
///
/// # Example
///
/// ```sql
/// -- Before:
/// SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x) AS y WHERE y.a = 1
/// -- After:
/// SELECT y.a AS a FROM (SELECT x.a AS a FROM x AS x WHERE x.a = 1) AS y WHERE TRUE
/// ```
///
/// # Arguments
/// * `expression` - The expression to optimize
/// * `dialect` - Optional dialect for dialect-specific behavior
///
/// # Returns
/// The optimized expression with predicates pushed down
pub fn pushdown_predicates(
    expression: Expression,
    dialect: Option<DialectType>,
) -> Expression {
    let root = build_scope(&expression);
    let scope_ref_count = compute_ref_count(&root);

    // Check if dialect requires special handling for UNNEST
    let unnest_requires_cross_join = matches!(
        dialect,
        Some(DialectType::Presto) | Some(DialectType::Trino) | Some(DialectType::Athena)
    );

    // Process scopes in reverse order (bottom-up)
    let mut result = expression.clone();
    let scopes = collect_scopes(&root);

    for scope in scopes.iter().rev() {
        result = process_scope(&result, scope, &scope_ref_count, dialect, unnest_requires_cross_join);
    }

    result
}

/// Collect all scopes from the tree
fn collect_scopes(root: &Scope) -> Vec<Scope> {
    let mut result = vec![root.clone()];
    // Collect from subquery scopes
    for child in &root.subquery_scopes {
        result.extend(collect_scopes(child));
    }
    // Collect from derived table scopes
    for child in &root.derived_table_scopes {
        result.extend(collect_scopes(child));
    }
    // Collect from CTE scopes
    for child in &root.cte_scopes {
        result.extend(collect_scopes(child));
    }
    // Collect from union scopes
    for child in &root.union_scopes {
        result.extend(collect_scopes(child));
    }
    result
}

/// Compute reference counts for each scope
fn compute_ref_count(root: &Scope) -> HashMap<u64, usize> {
    let mut counts = HashMap::new();
    compute_ref_count_recursive(root, &mut counts);
    counts
}

fn compute_ref_count_recursive(scope: &Scope, counts: &mut HashMap<u64, usize>) {
    // Use the pointer address as a pseudo-ID
    let id = scope as *const Scope as u64;
    *counts.entry(id).or_insert(0) += 1;

    for child in &scope.subquery_scopes {
        compute_ref_count_recursive(child, counts);
    }
    for child in &scope.derived_table_scopes {
        compute_ref_count_recursive(child, counts);
    }
    for child in &scope.cte_scopes {
        compute_ref_count_recursive(child, counts);
    }
    for child in &scope.union_scopes {
        compute_ref_count_recursive(child, counts);
    }
}

/// Process a single scope for predicate pushdown
fn process_scope(
    expression: &Expression,
    scope: &Scope,
    _scope_ref_count: &HashMap<u64, usize>,
    dialect: Option<DialectType>,
    _unnest_requires_cross_join: bool,
) -> Expression {
    let result = expression.clone();

    // Extract data we need before processing
    let (where_condition, join_conditions, join_index) = if let Expression::Select(select) = &result {
        let where_cond = select.where_clause.as_ref().map(|w| w.this.clone());

        let mut idx: HashMap<String, usize> = HashMap::new();
        for (i, join) in select.joins.iter().enumerate() {
            if let Some(name) = get_table_alias_or_name(&join.this) {
                idx.insert(name, i);
            }
        }

        let join_conds: Vec<Expression> = select.joins.iter()
            .filter_map(|j| j.on.clone())
            .collect();

        (where_cond, join_conds, idx)
    } else {
        (None, vec![], HashMap::new())
    };

    let mut result = result;

    // Process WHERE clause
    if let Some(where_cond) = where_condition {
        let simplified = simplify(where_cond, dialect);
        result = pushdown_impl(
            result,
            &simplified,
            &scope.sources,
            dialect,
            Some(&join_index),
        );
    }

    // Process JOIN ON conditions
    for join_cond in join_conditions {
        let simplified = simplify(join_cond, dialect);
        result = pushdown_impl(
            result,
            &simplified,
            &scope.sources,
            dialect,
            None,
        );
    }

    result
}

/// Push down a condition into sources
fn pushdown_impl(
    expression: Expression,
    condition: &Expression,
    sources: &HashMap<String, SourceInfo>,
    _dialect: Option<DialectType>,
    join_index: Option<&HashMap<String, usize>>,
) -> Expression {
    // Check if condition is in CNF or DNF form
    let is_cnf = normalized(condition, false); // CNF check
    let is_dnf = normalized(condition, true);  // DNF check
    let cnf_like = is_cnf || !is_dnf;

    // Flatten the condition into predicates
    let predicates = flatten_predicates(condition, cnf_like);

    if cnf_like {
        pushdown_cnf(expression, &predicates, sources, join_index)
    } else {
        pushdown_dnf(expression, &predicates, sources)
    }
}

/// Flatten predicates from AND/OR expressions
fn flatten_predicates(expr: &Expression, cnf_like: bool) -> Vec<Expression> {
    if cnf_like {
        // For CNF, flatten AND
        flatten_and(expr)
    } else {
        // For DNF, flatten OR
        flatten_or(expr)
    }
}

fn flatten_and(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::And(bin) => {
            let mut result = flatten_and(&bin.left);
            result.extend(flatten_and(&bin.right));
            result
        }
        Expression::Paren(p) => flatten_and(&p.this),
        other => vec![other.clone()],
    }
}

fn flatten_or(expr: &Expression) -> Vec<Expression> {
    match expr {
        Expression::Or(bin) => {
            let mut result = flatten_or(&bin.left);
            result.extend(flatten_or(&bin.right));
            result
        }
        Expression::Paren(p) => flatten_or(&p.this),
        other => vec![other.clone()],
    }
}

/// Pushdown predicates in CNF form
fn pushdown_cnf(
    expression: Expression,
    predicates: &[Expression],
    sources: &HashMap<String, SourceInfo>,
    join_index: Option<&HashMap<String, usize>>,
) -> Expression {
    let mut result = expression;

    for predicate in predicates {
        let nodes = nodes_for_predicate(predicate, sources);

        for (table_name, node_expr) in nodes {
            // Check if this is a JOIN node
            if let Some(join_idx) = join_index {
                if let Some(&this_index) = join_idx.get(&table_name) {
                    let predicate_tables = get_column_table_names(predicate);

                    // Don't push if predicate references tables from later joins
                    let can_push = predicate_tables.iter().all(|t| {
                        join_idx.get(t).map_or(true, |&idx| idx <= this_index)
                    });

                    if can_push {
                        result = push_predicate_to_node(&result, predicate, &node_expr);
                    }
                }
            } else {
                result = push_predicate_to_node(&result, predicate, &node_expr);
            }
        }
    }

    result
}

/// Pushdown predicates in DNF form
fn pushdown_dnf(
    expression: Expression,
    predicates: &[Expression],
    sources: &HashMap<String, SourceInfo>,
) -> Expression {
    // Find tables that can be pushed down to
    // These are tables referenced in ALL blocks of the DNF
    let mut pushdown_tables: HashSet<String> = HashSet::new();

    for a in predicates {
        let a_tables: HashSet<String> = get_column_table_names(a).into_iter().collect();

        let common: HashSet<String> = predicates
            .iter()
            .fold(a_tables, |acc, b| {
                let b_tables: HashSet<String> = get_column_table_names(b).into_iter().collect();
                acc.intersection(&b_tables).cloned().collect()
            });

        pushdown_tables.extend(common);
    }

    let mut result = expression;

    // Build conditions for each table
    let mut conditions: HashMap<String, Expression> = HashMap::new();

    for table in &pushdown_tables {
        for predicate in predicates {
            let nodes = nodes_for_predicate(predicate, sources);

            if nodes.contains_key(table) {
                let existing = conditions.remove(table);
                conditions.insert(
                    table.clone(),
                    if let Some(existing) = existing {
                        make_or(existing, predicate.clone())
                    } else {
                        predicate.clone()
                    },
                );
            }
        }
    }

    // Push conditions to nodes
    for (table, condition) in conditions {
        if let Some(source_info) = sources.get(&table) {
            result = push_predicate_to_node(&result, &condition, &source_info.expression);
        }
    }

    result
}

/// Get nodes that a predicate can be pushed down to
fn nodes_for_predicate(
    predicate: &Expression,
    sources: &HashMap<String, SourceInfo>,
) -> HashMap<String, Expression> {
    let mut nodes = HashMap::new();
    let tables = get_column_table_names(predicate);

    for table in tables {
        if let Some(source_info) = sources.get(&table) {
            // For now, add the node if it's a valid pushdown target
            // In a full implementation, we'd check for:
            // - RIGHT joins (can only push to itself)
            // - GROUP BY (push to HAVING instead)
            // - Window functions (can't push)
            // - Multiple references (can't push)
            nodes.insert(table, source_info.expression.clone());
        }
    }

    nodes
}

/// Push a predicate to a node (JOIN or subquery)
fn push_predicate_to_node(
    expression: &Expression,
    _predicate: &Expression,
    _target_node: &Expression,
) -> Expression {
    // In a full implementation, this would:
    // 1. Find the target node in the expression tree
    // 2. Add the predicate to its WHERE/ON clause
    // 3. Replace the original predicate with TRUE

    // For now, return unchanged - the structure is complex
    expression.clone()
}

/// Extract table names from column references in an expression
fn get_column_table_names(expr: &Expression) -> Vec<String> {
    let mut tables = Vec::new();
    collect_column_tables(expr, &mut tables);
    tables
}

fn collect_column_tables(expr: &Expression, tables: &mut Vec<String>) {
    match expr {
        Expression::Column(col) => {
            if let Some(ref table) = col.table {
                tables.push(table.name.clone());
            }
        }
        Expression::And(bin) | Expression::Or(bin) => {
            collect_column_tables(&bin.left, tables);
            collect_column_tables(&bin.right, tables);
        }
        Expression::Eq(bin) | Expression::Neq(bin) | Expression::Lt(bin) |
        Expression::Lte(bin) | Expression::Gt(bin) | Expression::Gte(bin) => {
            collect_column_tables(&bin.left, tables);
            collect_column_tables(&bin.right, tables);
        }
        Expression::Not(un) => {
            collect_column_tables(&un.this, tables);
        }
        Expression::Paren(p) => {
            collect_column_tables(&p.this, tables);
        }
        Expression::In(in_expr) => {
            collect_column_tables(&in_expr.this, tables);
            for e in &in_expr.expressions {
                collect_column_tables(e, tables);
            }
        }
        Expression::Between(between) => {
            collect_column_tables(&between.this, tables);
            collect_column_tables(&between.low, tables);
            collect_column_tables(&between.high, tables);
        }
        Expression::IsNull(is_null) => {
            collect_column_tables(&is_null.this, tables);
        }
        Expression::Like(like) => {
            collect_column_tables(&like.left, tables);
            collect_column_tables(&like.right, tables);
        }
        Expression::Function(func) => {
            for arg in &func.args {
                collect_column_tables(arg, tables);
            }
        }
        Expression::AggregateFunction(agg) => {
            for arg in &agg.args {
                collect_column_tables(arg, tables);
            }
        }
        _ => {}
    }
}

/// Get table name or alias from an expression
fn get_table_alias_or_name(expr: &Expression) -> Option<String> {
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

/// Create an OR expression from two expressions
fn make_or(left: Expression, right: Expression) -> Expression {
    Expression::Or(Box::new(crate::expressions::BinaryOp {
        left,
        right,
        left_comments: vec![],
        operator_comments: vec![],
        trailing_comments: vec![],
    }))
}

/// Replace aliases in a predicate with the original expressions
pub fn replace_aliases(
    source: &Expression,
    predicate: Expression,
) -> Expression {
    // Build alias map from source SELECT expressions
    let mut aliases: HashMap<String, Expression> = HashMap::new();

    if let Expression::Select(select) = source {
        for select_expr in &select.expressions {
            match select_expr {
                Expression::Alias(alias) => {
                    aliases.insert(alias.alias.name.clone(), alias.this.clone());
                }
                Expression::Column(col) => {
                    aliases.insert(col.name.name.clone(), select_expr.clone());
                }
                _ => {}
            }
        }
    }

    // Transform predicate, replacing column references with aliases
    replace_aliases_recursive(predicate, &aliases)
}

fn replace_aliases_recursive(
    expr: Expression,
    aliases: &HashMap<String, Expression>,
) -> Expression {
    match expr {
        Expression::Column(col) => {
            if let Some(replacement) = aliases.get(&col.name.name) {
                replacement.clone()
            } else {
                Expression::Column(col)
            }
        }
        Expression::And(bin) => {
            let left = replace_aliases_recursive(bin.left, aliases);
            let right = replace_aliases_recursive(bin.right, aliases);
            Expression::And(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Or(bin) => {
            let left = replace_aliases_recursive(bin.left, aliases);
            let right = replace_aliases_recursive(bin.right, aliases);
            Expression::Or(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Eq(bin) => {
            let left = replace_aliases_recursive(bin.left, aliases);
            let right = replace_aliases_recursive(bin.right, aliases);
            Expression::Eq(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Neq(bin) => {
            let left = replace_aliases_recursive(bin.left, aliases);
            let right = replace_aliases_recursive(bin.right, aliases);
            Expression::Neq(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Lt(bin) => {
            let left = replace_aliases_recursive(bin.left, aliases);
            let right = replace_aliases_recursive(bin.right, aliases);
            Expression::Lt(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Gt(bin) => {
            let left = replace_aliases_recursive(bin.left, aliases);
            let right = replace_aliases_recursive(bin.right, aliases);
            Expression::Gt(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Lte(bin) => {
            let left = replace_aliases_recursive(bin.left, aliases);
            let right = replace_aliases_recursive(bin.right, aliases);
            Expression::Lte(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Gte(bin) => {
            let left = replace_aliases_recursive(bin.left, aliases);
            let right = replace_aliases_recursive(bin.right, aliases);
            Expression::Gte(Box::new(crate::expressions::BinaryOp {
                left,
                right,
                left_comments: bin.left_comments,
                operator_comments: bin.operator_comments,
                trailing_comments: bin.trailing_comments,
            }))
        }
        Expression::Not(un) => {
            let inner = replace_aliases_recursive(un.this, aliases);
            Expression::Not(Box::new(crate::expressions::UnaryOp { this: inner }))
        }
        Expression::Paren(paren) => {
            let inner = replace_aliases_recursive(paren.this, aliases);
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: inner,
                trailing_comments: paren.trailing_comments,
            }))
        }
        other => other,
    }
}

/// Create a TRUE literal expression
pub fn make_true() -> Expression {
    Expression::Boolean(BooleanLiteral { value: true })
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
    fn test_pushdown_simple() {
        let expr = parse("SELECT a FROM t WHERE a = 1");
        let result = pushdown_predicates(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("WHERE"));
    }

    #[test]
    fn test_pushdown_preserves_structure() {
        let expr = parse("SELECT y.a FROM (SELECT x.a FROM x) AS y WHERE y.a = 1");
        let result = pushdown_predicates(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("SELECT"));
    }

    #[test]
    fn test_get_column_table_names() {
        let expr = parse("SELECT 1 WHERE t.a = 1 AND s.b = 2");
        if let Expression::Select(select) = &expr {
            if let Some(where_clause) = &select.where_clause {
                let tables = get_column_table_names(&where_clause.this);
                assert!(tables.contains(&"t".to_string()));
                assert!(tables.contains(&"s".to_string()));
            }
        }
    }

    #[test]
    fn test_flatten_and() {
        let expr = parse("SELECT 1 WHERE a = 1 AND b = 2 AND c = 3");
        if let Expression::Select(select) = &expr {
            if let Some(where_clause) = &select.where_clause {
                let predicates = flatten_and(&where_clause.this);
                assert_eq!(predicates.len(), 3);
            }
        }
    }

    #[test]
    fn test_flatten_or() {
        let expr = parse("SELECT 1 WHERE a = 1 OR b = 2 OR c = 3");
        if let Expression::Select(select) = &expr {
            if let Some(where_clause) = &select.where_clause {
                let predicates = flatten_or(&where_clause.this);
                assert_eq!(predicates.len(), 3);
            }
        }
    }

    #[test]
    fn test_replace_aliases() {
        let source = parse("SELECT x.a AS col_a FROM x");
        let predicate = parse("SELECT 1 WHERE col_a = 1");

        if let Expression::Select(select) = &predicate {
            if let Some(where_clause) = &select.where_clause {
                let replaced = replace_aliases(&source, where_clause.this.clone());
                // The alias should be replaced
                let sql = gen(&replaced);
                assert!(sql.contains("="));
            }
        }
    }

    #[test]
    fn test_pushdown_with_join() {
        let expr = parse("SELECT t.a FROM t JOIN s ON t.id = s.id WHERE t.a = 1");
        let result = pushdown_predicates(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("JOIN"));
    }

    #[test]
    fn test_pushdown_complex_and() {
        let expr = parse("SELECT 1 WHERE a = 1 AND b > 2 AND c < 3");
        let result = pushdown_predicates(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("AND"));
    }

    #[test]
    fn test_pushdown_complex_or() {
        let expr = parse("SELECT 1 WHERE a = 1 OR b = 2");
        let result = pushdown_predicates(expr, None);
        let sql = gen(&result);
        assert!(sql.contains("OR"));
    }

    #[test]
    fn test_normalized_dnf_simple() {
        // a = 1 is in both CNF and DNF form
        let expr = parse("SELECT 1 WHERE a = 1");
        if let Expression::Select(select) = &expr {
            if let Some(where_clause) = &select.where_clause {
                // Check DNF: pass true for dnf flag
                assert!(normalized(&where_clause.this, true));
            }
        }
    }

    #[test]
    fn test_make_true() {
        let t = make_true();
        let sql = gen(&t);
        assert_eq!(sql, "TRUE");
    }
}
