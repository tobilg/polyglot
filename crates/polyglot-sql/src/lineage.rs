//! Column Lineage Tracking
//!
//! This module provides functionality to track column lineage through SQL queries,
//! building a graph of how columns flow from source tables to the result set.
//! Supports UNION/INTERSECT/EXCEPT, CTEs, derived tables, subqueries, and star expansion.
//!

use crate::dialects::DialectType;
use crate::expressions::Expression;
use crate::scope::{build_scope, Scope};
use crate::traversal::ExpressionWalk;
use crate::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;

/// A node in the column lineage graph
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LineageNode {
    /// Name of this lineage step (e.g., "table.column")
    pub name: String,
    /// The expression at this node
    pub expression: Expression,
    /// The source expression (the full query context)
    pub source: Expression,
    /// Downstream nodes that depend on this one
    pub downstream: Vec<LineageNode>,
    /// Optional source name (e.g., for derived tables)
    pub source_name: String,
    /// Optional reference node name (e.g., for CTEs)
    pub reference_node_name: String,
}

impl LineageNode {
    /// Create a new lineage node
    pub fn new(name: impl Into<String>, expression: Expression, source: Expression) -> Self {
        Self {
            name: name.into(),
            expression,
            source,
            downstream: Vec::new(),
            source_name: String::new(),
            reference_node_name: String::new(),
        }
    }

    /// Iterate over all nodes in the lineage graph using DFS
    pub fn walk(&self) -> LineageWalker<'_> {
        LineageWalker { stack: vec![self] }
    }

    /// Get all downstream column names
    pub fn downstream_names(&self) -> Vec<String> {
        self.downstream.iter().map(|n| n.name.clone()).collect()
    }
}

/// Iterator for walking the lineage graph
pub struct LineageWalker<'a> {
    stack: Vec<&'a LineageNode>,
}

impl<'a> Iterator for LineageWalker<'a> {
    type Item = &'a LineageNode;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = self.stack.pop() {
            // Add children in reverse order so they're visited in order
            for child in node.downstream.iter().rev() {
                self.stack.push(child);
            }
            Some(node)
        } else {
            None
        }
    }
}

// ---------------------------------------------------------------------------
// ColumnRef: name or positional index for column lookup
// ---------------------------------------------------------------------------

/// Column reference for lineage tracing — by name or positional index.
enum ColumnRef<'a> {
    Name(&'a str),
    Index(usize),
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Build the lineage graph for a column in a SQL query
///
/// # Arguments
/// * `column` - The column name to trace lineage for
/// * `sql` - The SQL expression (SELECT, UNION, etc.)
/// * `dialect` - Optional dialect for parsing
/// * `trim_selects` - If true, trim the source SELECT to only include the target column
///
/// # Returns
/// The root lineage node for the specified column
///
/// # Example
/// ```ignore
/// use polyglot_sql::lineage::lineage;
/// use polyglot_sql::parse_one;
/// use polyglot_sql::DialectType;
///
/// let sql = "SELECT a, b + 1 AS c FROM t";
/// let expr = parse_one(sql, DialectType::Generic).unwrap();
/// let node = lineage("c", &expr, None, false).unwrap();
/// ```
pub fn lineage(
    column: &str,
    sql: &Expression,
    dialect: Option<DialectType>,
    trim_selects: bool,
) -> Result<LineageNode> {
    let scope = build_scope(sql);
    to_node(
        ColumnRef::Name(column),
        &scope,
        dialect,
        "",
        "",
        "",
        trim_selects,
    )
}

/// Get all source tables from a lineage graph
pub fn get_source_tables(node: &LineageNode) -> HashSet<String> {
    let mut tables = HashSet::new();
    collect_source_tables(node, &mut tables);
    tables
}

/// Recursively collect source table names from lineage graph
pub fn collect_source_tables(node: &LineageNode, tables: &mut HashSet<String>) {
    if let Expression::Table(table) = &node.source {
        tables.insert(table.name.name.clone());
    }
    for child in &node.downstream {
        collect_source_tables(child, tables);
    }
}

// ---------------------------------------------------------------------------
// Core recursive lineage builder
// ---------------------------------------------------------------------------

/// Recursively build a lineage node for a column in a scope.
fn to_node(
    column: ColumnRef<'_>,
    scope: &Scope,
    dialect: Option<DialectType>,
    scope_name: &str,
    source_name: &str,
    reference_node_name: &str,
    trim_selects: bool,
) -> Result<LineageNode> {
    to_node_inner(
        column,
        scope,
        dialect,
        scope_name,
        source_name,
        reference_node_name,
        trim_selects,
        &[],
    )
}

fn to_node_inner(
    column: ColumnRef<'_>,
    scope: &Scope,
    dialect: Option<DialectType>,
    scope_name: &str,
    source_name: &str,
    reference_node_name: &str,
    trim_selects: bool,
    ancestor_cte_scopes: &[Scope],
) -> Result<LineageNode> {
    let scope_expr = &scope.expression;

    // Build combined CTE scopes: current scope's cte_scopes + ancestors
    let mut all_cte_scopes: Vec<&Scope> = scope.cte_scopes.iter().collect();
    for s in ancestor_cte_scopes {
        all_cte_scopes.push(s);
    }

    // 0. Unwrap CTE scope — CTE scope expressions are Expression::Cte(...)
    //    but we need the inner query (SELECT/UNION) for column lookup.
    let effective_expr = match scope_expr {
        Expression::Cte(cte) => &cte.this,
        other => other,
    };

    // 1. Set operations (UNION / INTERSECT / EXCEPT)
    if matches!(
        effective_expr,
        Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_)
    ) {
        // For CTE wrapping a set op, create a temporary scope with the inner expression
        if matches!(scope_expr, Expression::Cte(_)) {
            let mut inner_scope = Scope::new(effective_expr.clone());
            inner_scope.union_scopes = scope.union_scopes.clone();
            inner_scope.sources = scope.sources.clone();
            inner_scope.cte_sources = scope.cte_sources.clone();
            inner_scope.cte_scopes = scope.cte_scopes.clone();
            inner_scope.derived_table_scopes = scope.derived_table_scopes.clone();
            inner_scope.subquery_scopes = scope.subquery_scopes.clone();
            return handle_set_operation(
                &column,
                &inner_scope,
                dialect,
                scope_name,
                source_name,
                reference_node_name,
                trim_selects,
                ancestor_cte_scopes,
            );
        }
        return handle_set_operation(
            &column,
            scope,
            dialect,
            scope_name,
            source_name,
            reference_node_name,
            trim_selects,
            ancestor_cte_scopes,
        );
    }

    // 2. Find the select expression for this column
    let select_expr = find_select_expr(effective_expr, &column)?;
    let column_name = resolve_column_name(&column, &select_expr);

    // 3. Trim source if requested
    let node_source = if trim_selects {
        trim_source(effective_expr, &select_expr)
    } else {
        effective_expr.clone()
    };

    // 4. Create the lineage node
    let mut node = LineageNode::new(&column_name, select_expr.clone(), node_source);
    node.source_name = source_name.to_string();
    node.reference_node_name = reference_node_name.to_string();

    // 5. Star handling — add downstream for each source
    if matches!(&select_expr, Expression::Star(_)) {
        for (name, source_info) in &scope.sources {
            let child = LineageNode::new(
                format!("{}.*", name),
                Expression::Star(crate::expressions::Star {
                    table: None,
                    except: None,
                    replace: None,
                    rename: None,
                    trailing_comments: vec![],
                }),
                source_info.expression.clone(),
            );
            node.downstream.push(child);
        }
        return Ok(node);
    }

    // 6. Subqueries in select — trace through scalar subqueries
    let subqueries: Vec<&Expression> =
        select_expr.find_all(|e| matches!(e, Expression::Subquery(sq) if sq.alias.is_none()));
    for sq_expr in subqueries {
        if let Expression::Subquery(sq) = sq_expr {
            for sq_scope in &scope.subquery_scopes {
                if sq_scope.expression == sq.this {
                    if let Ok(child) = to_node_inner(
                        ColumnRef::Index(0),
                        sq_scope,
                        dialect,
                        &column_name,
                        "",
                        "",
                        trim_selects,
                        ancestor_cte_scopes,
                    ) {
                        node.downstream.push(child);
                    }
                    break;
                }
            }
        }
    }

    // 7. Column references — trace each column to its source
    let col_refs = find_column_refs_in_expr(&select_expr);
    for col_ref in col_refs {
        let col_name = &col_ref.column;
        if let Some(ref table_id) = col_ref.table {
            let tbl = &table_id.name;
            resolve_qualified_column(
                &mut node,
                scope,
                dialect,
                tbl,
                col_name,
                &column_name,
                trim_selects,
                &all_cte_scopes,
            );
        } else {
            resolve_unqualified_column(
                &mut node,
                scope,
                dialect,
                col_name,
                &column_name,
                trim_selects,
                &all_cte_scopes,
            );
        }
    }

    Ok(node)
}

// ---------------------------------------------------------------------------
// Set operation handling
// ---------------------------------------------------------------------------

fn handle_set_operation(
    column: &ColumnRef<'_>,
    scope: &Scope,
    dialect: Option<DialectType>,
    scope_name: &str,
    source_name: &str,
    reference_node_name: &str,
    trim_selects: bool,
    ancestor_cte_scopes: &[Scope],
) -> Result<LineageNode> {
    let scope_expr = &scope.expression;

    // Determine column index
    let col_index = match column {
        ColumnRef::Name(name) => column_to_index(scope_expr, name)?,
        ColumnRef::Index(i) => *i,
    };

    let col_name = match column {
        ColumnRef::Name(name) => name.to_string(),
        ColumnRef::Index(_) => format!("_{col_index}"),
    };

    let mut node = LineageNode::new(&col_name, scope_expr.clone(), scope_expr.clone());
    node.source_name = source_name.to_string();
    node.reference_node_name = reference_node_name.to_string();

    // Recurse into each union branch
    for branch_scope in &scope.union_scopes {
        if let Ok(child) = to_node_inner(
            ColumnRef::Index(col_index),
            branch_scope,
            dialect,
            scope_name,
            "",
            "",
            trim_selects,
            ancestor_cte_scopes,
        ) {
            node.downstream.push(child);
        }
    }

    Ok(node)
}

// ---------------------------------------------------------------------------
// Column resolution helpers
// ---------------------------------------------------------------------------

fn resolve_qualified_column(
    node: &mut LineageNode,
    scope: &Scope,
    dialect: Option<DialectType>,
    table: &str,
    col_name: &str,
    parent_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[&Scope],
) {
    // Check if table is a CTE reference (cte_sources tracks CTE names)
    if scope.cte_sources.contains_key(table) {
        if let Some(child_scope) = find_child_scope_in(all_cte_scopes, scope, table) {
            // Build ancestor CTE scopes from all_cte_scopes for the recursive call
            let ancestors: Vec<Scope> = all_cte_scopes.iter().map(|s| (*s).clone()).collect();
            if let Ok(child) = to_node_inner(
                ColumnRef::Name(col_name),
                child_scope,
                dialect,
                parent_name,
                table,
                parent_name,
                trim_selects,
                &ancestors,
            ) {
                node.downstream.push(child);
                return;
            }
        }
    }

    // Check if table is a derived table (is_scope = true in sources)
    if let Some(source_info) = scope.sources.get(table) {
        if source_info.is_scope {
            if let Some(child_scope) = find_child_scope(scope, table) {
                let ancestors: Vec<Scope> = all_cte_scopes.iter().map(|s| (*s).clone()).collect();
                if let Ok(child) = to_node_inner(
                    ColumnRef::Name(col_name),
                    child_scope,
                    dialect,
                    parent_name,
                    table,
                    parent_name,
                    trim_selects,
                    &ancestors,
                ) {
                    node.downstream.push(child);
                    return;
                }
            }
        }
    }

    // Base table or unresolved — terminal node
    node.downstream
        .push(make_table_column_node(table, col_name));
}

fn resolve_unqualified_column(
    node: &mut LineageNode,
    scope: &Scope,
    dialect: Option<DialectType>,
    col_name: &str,
    parent_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[&Scope],
) {
    // Try to find which source this column belongs to.
    // Filter to only FROM-clause sources: add_cte_source adds all CTEs to sources
    // with Expression::Cte, but FROM-clause Table references overwrite with Expression::Table.
    // So CTE-only entries (not referenced in FROM) have Expression::Cte — exclude those.
    let from_source_names: Vec<&String> = scope
        .sources
        .iter()
        .filter(|(_, info)| !matches!(info.expression, Expression::Cte(_)))
        .map(|(name, _)| name)
        .collect();

    if from_source_names.len() == 1 {
        let tbl = from_source_names[0];
        resolve_qualified_column(
            node,
            scope,
            dialect,
            tbl,
            col_name,
            parent_name,
            trim_selects,
            all_cte_scopes,
        );
        return;
    }

    // Multiple sources — can't resolve without schema info, add unqualified node
    let child = LineageNode::new(
        col_name.to_string(),
        Expression::Column(crate::expressions::Column {
            name: crate::expressions::Identifier::new(col_name.to_string()),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        }),
        node.source.clone(),
    );
    node.downstream.push(child);
}

// ---------------------------------------------------------------------------
// Helper functions
// ---------------------------------------------------------------------------

/// Get the alias or name of an expression
fn get_alias_or_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Alias(alias) => Some(alias.alias.name.clone()),
        Expression::Column(col) => Some(col.name.name.clone()),
        Expression::Identifier(id) => Some(id.name.clone()),
        Expression::Star(_) => Some("*".to_string()),
        _ => None,
    }
}

/// Resolve the display name for a column reference.
fn resolve_column_name(column: &ColumnRef<'_>, select_expr: &Expression) -> String {
    match column {
        ColumnRef::Name(n) => n.to_string(),
        ColumnRef::Index(_) => get_alias_or_name(select_expr).unwrap_or_else(|| "?".to_string()),
    }
}

/// Find the select expression matching a column reference.
fn find_select_expr(scope_expr: &Expression, column: &ColumnRef<'_>) -> Result<Expression> {
    if let Expression::Select(ref select) = scope_expr {
        match column {
            ColumnRef::Name(name) => {
                for expr in &select.expressions {
                    if get_alias_or_name(expr).as_deref() == Some(name) {
                        return Ok(expr.clone());
                    }
                }
                Err(crate::error::Error::Parse(format!(
                    "Cannot find column '{}' in query",
                    name
                )))
            }
            ColumnRef::Index(idx) => select.expressions.get(*idx).cloned().ok_or_else(|| {
                crate::error::Error::Parse(format!("Column index {} out of range", idx))
            }),
        }
    } else {
        Err(crate::error::Error::Parse(
            "Expected SELECT expression for column lookup".to_string(),
        ))
    }
}

/// Find the positional index of a column name in a set operation's first SELECT branch.
fn column_to_index(set_op_expr: &Expression, name: &str) -> Result<usize> {
    let mut expr = set_op_expr;
    loop {
        match expr {
            Expression::Union(u) => expr = &u.left,
            Expression::Intersect(i) => expr = &i.left,
            Expression::Except(e) => expr = &e.left,
            Expression::Select(select) => {
                for (i, e) in select.expressions.iter().enumerate() {
                    if get_alias_or_name(e).as_deref() == Some(name) {
                        return Ok(i);
                    }
                }
                return Err(crate::error::Error::Parse(format!(
                    "Cannot find column '{}' in set operation",
                    name
                )));
            }
            _ => {
                return Err(crate::error::Error::Parse(
                    "Expected SELECT or set operation".to_string(),
                ))
            }
        }
    }
}

/// If trim_selects is enabled, return a copy of the SELECT with only the target column.
fn trim_source(select_expr: &Expression, target_expr: &Expression) -> Expression {
    if let Expression::Select(select) = select_expr {
        let mut trimmed = select.as_ref().clone();
        trimmed.expressions = vec![target_expr.clone()];
        Expression::Select(Box::new(trimmed))
    } else {
        select_expr.clone()
    }
}

/// Find the child scope (CTE or derived table) for a given source name.
fn find_child_scope<'a>(scope: &'a Scope, source_name: &str) -> Option<&'a Scope> {
    // Check CTE scopes
    if scope.cte_sources.contains_key(source_name) {
        for cte_scope in &scope.cte_scopes {
            if let Expression::Cte(cte) = &cte_scope.expression {
                if cte.alias.name == source_name {
                    return Some(cte_scope);
                }
            }
        }
    }

    // Check derived table scopes
    if let Some(source_info) = scope.sources.get(source_name) {
        if source_info.is_scope && !scope.cte_sources.contains_key(source_name) {
            if let Expression::Subquery(sq) = &source_info.expression {
                for dt_scope in &scope.derived_table_scopes {
                    if dt_scope.expression == sq.this {
                        return Some(dt_scope);
                    }
                }
            }
        }
    }

    None
}

/// Find a CTE scope by name, searching through a combined list of CTE scopes.
/// This handles nested CTEs where the current scope doesn't have the CTE scope
/// as a direct child but knows about it via cte_sources.
fn find_child_scope_in<'a>(
    all_cte_scopes: &[&'a Scope],
    scope: &'a Scope,
    source_name: &str,
) -> Option<&'a Scope> {
    // First try the scope's own cte_scopes
    for cte_scope in &scope.cte_scopes {
        if let Expression::Cte(cte) = &cte_scope.expression {
            if cte.alias.name == source_name {
                return Some(cte_scope);
            }
        }
    }

    // Then search through all ancestor CTE scopes
    for cte_scope in all_cte_scopes {
        if let Expression::Cte(cte) = &cte_scope.expression {
            if cte.alias.name == source_name {
                return Some(cte_scope);
            }
        }
    }

    // Fall back to derived table scopes
    if let Some(source_info) = scope.sources.get(source_name) {
        if source_info.is_scope {
            if let Expression::Subquery(sq) = &source_info.expression {
                for dt_scope in &scope.derived_table_scopes {
                    if dt_scope.expression == sq.this {
                        return Some(dt_scope);
                    }
                }
            }
        }
    }

    None
}

/// Create a terminal lineage node for a table.column reference.
fn make_table_column_node(table: &str, column: &str) -> LineageNode {
    LineageNode::new(
        format!("{}.{}", table, column),
        Expression::Column(crate::expressions::Column {
            name: crate::expressions::Identifier::new(column.to_string()),
            table: Some(crate::expressions::Identifier::new(table.to_string())),
            join_mark: false,
            trailing_comments: vec![],
        }),
        Expression::Table(crate::expressions::TableRef::new(table)),
    )
}

/// Simple column reference extracted from an expression
#[derive(Debug, Clone)]
struct SimpleColumnRef {
    table: Option<crate::expressions::Identifier>,
    column: String,
}

/// Find all column references in an expression (does not recurse into subqueries).
fn find_column_refs_in_expr(expr: &Expression) -> Vec<SimpleColumnRef> {
    let mut refs = Vec::new();
    collect_column_refs(expr, &mut refs);
    refs
}

fn collect_column_refs(expr: &Expression, refs: &mut Vec<SimpleColumnRef>) {
    match expr {
        Expression::Column(col) => {
            refs.push(SimpleColumnRef {
                table: col.table.clone(),
                column: col.name.name.clone(),
            });
        }
        Expression::Alias(alias) => {
            collect_column_refs(&alias.this, refs);
        }
        Expression::And(op)
        | Expression::Or(op)
        | Expression::Eq(op)
        | Expression::Neq(op)
        | Expression::Lt(op)
        | Expression::Lte(op)
        | Expression::Gt(op)
        | Expression::Gte(op)
        | Expression::Add(op)
        | Expression::Sub(op)
        | Expression::Mul(op)
        | Expression::Div(op)
        | Expression::Mod(op)
        | Expression::BitwiseAnd(op)
        | Expression::BitwiseOr(op)
        | Expression::BitwiseXor(op)
        | Expression::Concat(op) => {
            collect_column_refs(&op.left, refs);
            collect_column_refs(&op.right, refs);
        }
        Expression::Not(u) | Expression::Neg(u) | Expression::BitwiseNot(u) => {
            collect_column_refs(&u.this, refs);
        }
        Expression::Function(func) => {
            for arg in &func.args {
                collect_column_refs(arg, refs);
            }
        }
        Expression::AggregateFunction(func) => {
            for arg in &func.args {
                collect_column_refs(arg, refs);
            }
        }
        Expression::WindowFunction(wf) => {
            collect_column_refs(&wf.this, refs);
        }
        Expression::Case(case) => {
            if let Some(operand) = &case.operand {
                collect_column_refs(operand, refs);
            }
            for (cond, result) in &case.whens {
                collect_column_refs(cond, refs);
                collect_column_refs(result, refs);
            }
            if let Some(ref else_expr) = case.else_ {
                collect_column_refs(else_expr, refs);
            }
        }
        Expression::Cast(cast) => {
            collect_column_refs(&cast.this, refs);
        }
        Expression::Paren(p) => {
            collect_column_refs(&p.this, refs);
        }
        Expression::Coalesce(c) => {
            for e in &c.expressions {
                collect_column_refs(e, refs);
            }
        }
        // Don't recurse into subqueries — those are handled separately
        Expression::Subquery(_) | Expression::Exists(_) => {}
        _ => {}
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::{Dialect, DialectType};

    fn parse(sql: &str) -> Expression {
        let dialect = Dialect::get(DialectType::Generic);
        let ast = dialect.parse(sql).unwrap();
        ast.into_iter().next().unwrap()
    }

    #[test]
    fn test_simple_lineage() {
        let expr = parse("SELECT a FROM t");
        let node = lineage("a", &expr, None, false).unwrap();

        assert_eq!(node.name, "a");
        assert!(!node.downstream.is_empty(), "Should have downstream nodes");
        // Should trace to t.a
        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t.a"),
            "Expected t.a in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_walk() {
        let root = LineageNode {
            name: "col_a".to_string(),
            expression: Expression::Null(crate::expressions::Null),
            source: Expression::Null(crate::expressions::Null),
            downstream: vec![LineageNode::new(
                "t.a",
                Expression::Null(crate::expressions::Null),
                Expression::Null(crate::expressions::Null),
            )],
            source_name: String::new(),
            reference_node_name: String::new(),
        };

        let names: Vec<_> = root.walk().map(|n| n.name.clone()).collect();
        assert_eq!(names.len(), 2);
        assert_eq!(names[0], "col_a");
        assert_eq!(names[1], "t.a");
    }

    #[test]
    fn test_aliased_column() {
        let expr = parse("SELECT a + 1 AS b FROM t");
        let node = lineage("b", &expr, None, false).unwrap();

        assert_eq!(node.name, "b");
        // Should trace through the expression to t.a
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|n| n.contains("a")),
            "Expected to trace to column a, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_qualified_column() {
        let expr = parse("SELECT t.a FROM t");
        let node = lineage("a", &expr, None, false).unwrap();

        assert_eq!(node.name, "a");
        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t.a"),
            "Expected t.a, got: {:?}",
            names
        );
    }

    #[test]
    fn test_unqualified_column() {
        let expr = parse("SELECT a FROM t");
        let node = lineage("a", &expr, None, false).unwrap();

        // Unqualified but single source → resolved to t.a
        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t.a"),
            "Expected t.a, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_join() {
        let expr = parse("SELECT t.a, s.b FROM t JOIN s ON t.id = s.id");

        let node_a = lineage("a", &expr, None, false).unwrap();
        let names_a = node_a.downstream_names();
        assert!(
            names_a.iter().any(|n| n == "t.a"),
            "Expected t.a, got: {:?}",
            names_a
        );

        let node_b = lineage("b", &expr, None, false).unwrap();
        let names_b = node_b.downstream_names();
        assert!(
            names_b.iter().any(|n| n == "s.b"),
            "Expected s.b, got: {:?}",
            names_b
        );
    }

    #[test]
    fn test_lineage_derived_table() {
        let expr = parse("SELECT x.a FROM (SELECT a FROM t) AS x");
        let node = lineage("a", &expr, None, false).unwrap();

        assert_eq!(node.name, "a");
        // Should trace through the derived table to t.a
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|n| n == "t.a"),
            "Expected to trace through derived table to t.a, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_cte() {
        let expr = parse("WITH cte AS (SELECT a FROM t) SELECT a FROM cte");
        let node = lineage("a", &expr, None, false).unwrap();

        assert_eq!(node.name, "a");
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|n| n == "t.a"),
            "Expected to trace through CTE to t.a, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_union() {
        let expr = parse("SELECT a FROM t1 UNION SELECT a FROM t2");
        let node = lineage("a", &expr, None, false).unwrap();

        assert_eq!(node.name, "a");
        // Should have 2 downstream branches
        assert_eq!(
            node.downstream.len(),
            2,
            "Expected 2 branches for UNION, got {}",
            node.downstream.len()
        );
    }

    #[test]
    fn test_lineage_cte_union() {
        let expr = parse("WITH cte AS (SELECT a FROM t1 UNION SELECT a FROM t2) SELECT a FROM cte");
        let node = lineage("a", &expr, None, false).unwrap();

        // Should trace through CTE into both UNION branches
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.len() >= 3,
            "Expected at least 3 nodes for CTE with UNION, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_star() {
        let expr = parse("SELECT * FROM t");
        let node = lineage("*", &expr, None, false).unwrap();

        assert_eq!(node.name, "*");
        // Should have downstream for table t
        assert!(
            !node.downstream.is_empty(),
            "Star should produce downstream nodes"
        );
    }

    #[test]
    fn test_lineage_subquery_in_select() {
        let expr = parse("SELECT (SELECT MAX(b) FROM s) AS x FROM t");
        let node = lineage("x", &expr, None, false).unwrap();

        assert_eq!(node.name, "x");
        // Should have traced into the scalar subquery
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.len() >= 2,
            "Expected tracing into scalar subquery, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_multiple_columns() {
        let expr = parse("SELECT a, b FROM t");

        let node_a = lineage("a", &expr, None, false).unwrap();
        let node_b = lineage("b", &expr, None, false).unwrap();

        assert_eq!(node_a.name, "a");
        assert_eq!(node_b.name, "b");

        // Each should trace independently
        let names_a = node_a.downstream_names();
        let names_b = node_b.downstream_names();
        assert!(names_a.iter().any(|n| n == "t.a"));
        assert!(names_b.iter().any(|n| n == "t.b"));
    }

    #[test]
    fn test_get_source_tables() {
        let expr = parse("SELECT t.a, s.b FROM t JOIN s ON t.id = s.id");
        let node = lineage("a", &expr, None, false).unwrap();

        let tables = get_source_tables(&node);
        assert!(
            tables.contains("t"),
            "Expected source table 't', got: {:?}",
            tables
        );
    }

    #[test]
    fn test_lineage_column_not_found() {
        let expr = parse("SELECT a FROM t");
        let result = lineage("nonexistent", &expr, None, false);
        assert!(result.is_err());
    }

    #[test]
    fn test_lineage_nested_cte() {
        let expr = parse(
            "WITH cte1 AS (SELECT a FROM t), \
             cte2 AS (SELECT a FROM cte1) \
             SELECT a FROM cte2",
        );
        let node = lineage("a", &expr, None, false).unwrap();

        // Should trace through cte2 → cte1 → t
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.len() >= 3,
            "Expected to trace through nested CTEs, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_trim_selects_true() {
        let expr = parse("SELECT a, b, c FROM t");
        let node = lineage("a", &expr, None, true).unwrap();

        // The source should be trimmed to only include 'a'
        if let Expression::Select(select) = &node.source {
            assert_eq!(
                select.expressions.len(),
                1,
                "Trimmed source should have 1 expression, got {}",
                select.expressions.len()
            );
        } else {
            panic!("Expected Select source");
        }
    }

    #[test]
    fn test_trim_selects_false() {
        let expr = parse("SELECT a, b, c FROM t");
        let node = lineage("a", &expr, None, false).unwrap();

        // The source should keep all columns
        if let Expression::Select(select) = &node.source {
            assert_eq!(
                select.expressions.len(),
                3,
                "Untrimmed source should have 3 expressions"
            );
        } else {
            panic!("Expected Select source");
        }
    }

    #[test]
    fn test_lineage_expression_in_select() {
        let expr = parse("SELECT a + b AS c FROM t");
        let node = lineage("c", &expr, None, false).unwrap();

        // Should trace to both a and b from t
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.len() >= 3,
            "Expected to trace a + b to both columns, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_set_operation_by_index() {
        let expr = parse("SELECT a FROM t1 UNION SELECT b FROM t2");

        // Trace column "a" which is at index 0
        let node = lineage("a", &expr, None, false).unwrap();

        // UNION branches should be traced by index
        assert_eq!(node.downstream.len(), 2);
    }
}
