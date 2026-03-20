//! Column Lineage Tracking
//!
//! This module provides functionality to track column lineage through SQL queries,
//! building a graph of how columns flow from source tables to the result set.
//! Supports UNION/INTERSECT/EXCEPT, CTEs, derived tables, subqueries, and star expansion.
//!

use crate::dialects::DialectType;
use crate::expressions::{Expression, Identifier, Select};
use crate::optimizer::annotate_types::annotate_types;
use crate::optimizer::qualify_columns::{qualify_columns, QualifyColumnsOptions};
use crate::schema::{normalize_name, Schema};
use crate::scope::{build_scope, Scope};
use crate::traversal::ExpressionWalk;
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

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
    // Fast path: skip clone when there are no CTEs to expand
    let has_with = matches!(sql, Expression::Select(s) if s.with.is_some());
    if !has_with {
        return lineage_from_expression(column, sql, dialect, trim_selects);
    }
    let mut owned = sql.clone();
    expand_cte_stars(&mut owned, None);
    lineage_from_expression(column, &owned, dialect, trim_selects)
}

/// Build the lineage graph for a column in a SQL query using optional schema metadata.
///
/// When `schema` is provided, the query is first qualified with
/// `optimizer::qualify_columns`, allowing more accurate lineage for unqualified or
/// ambiguous column references.
///
/// # Arguments
/// * `column` - The column name to trace lineage for
/// * `sql` - The SQL expression (SELECT, UNION, etc.)
/// * `schema` - Optional schema used for qualification
/// * `dialect` - Optional dialect for qualification and lineage handling
/// * `trim_selects` - If true, trim the source SELECT to only include the target column
///
/// # Returns
/// The root lineage node for the specified column
pub fn lineage_with_schema(
    column: &str,
    sql: &Expression,
    schema: Option<&dyn Schema>,
    dialect: Option<DialectType>,
    trim_selects: bool,
) -> Result<LineageNode> {
    let mut qualified_expression = if let Some(schema) = schema {
        let options = if let Some(dialect_type) = dialect.or_else(|| schema.dialect()) {
            QualifyColumnsOptions::new().with_dialect(dialect_type)
        } else {
            QualifyColumnsOptions::new()
        };

        qualify_columns(sql.clone(), schema, &options).map_err(|e| {
            Error::internal(format!("Lineage qualification failed with schema: {}", e))
        })?
    } else {
        sql.clone()
    };

    // Annotate types in-place so lineage nodes carry type information
    annotate_types(&mut qualified_expression, schema, dialect);

    // Expand CTE stars on the already-owned expression (no extra clone).
    // Pass schema so that stars from external tables can also be resolved.
    expand_cte_stars(&mut qualified_expression, schema);

    lineage_from_expression(column, &qualified_expression, dialect, trim_selects)
}

fn lineage_from_expression(
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

// ---------------------------------------------------------------------------
// CTE star expansion
// ---------------------------------------------------------------------------

/// Normalize an identifier for CTE name matching.
///
/// Follows SQL semantics: unquoted identifiers are case-insensitive (lowercased),
/// quoted identifiers preserve their original case. This matches sqlglot's
/// `normalize_identifiers` behavior.
fn normalize_cte_name(ident: &Identifier) -> String {
    if ident.quoted {
        ident.name.clone()
    } else {
        ident.name.to_lowercase()
    }
}

/// Expand SELECT * in CTEs by walking CTE definitions in order and propagating
/// resolved column lists. This handles nested CTEs (e.g., cte2 AS (SELECT * FROM cte1))
/// which qualify_columns cannot resolve because it processes each SELECT independently.
///
/// When `schema` is provided, stars from external tables (not CTEs) are also resolved
/// by looking up column names in the schema. This enables correct expansion of patterns
/// like `WITH cte AS (SELECT * FROM external_table) SELECT * FROM cte`.
///
/// CTE name matching follows SQL identifier semantics: unquoted names are compared
/// case-insensitively (lowercased), while quoted names preserve their original case.
/// This matches sqlglot's `normalize_identifiers` behavior.
pub fn expand_cte_stars(expr: &mut Expression, schema: Option<&dyn Schema>) {
    let select = match expr {
        Expression::Select(s) => s,
        _ => return,
    };

    let with = match &mut select.with {
        Some(w) => w,
        None => return,
    };

    // Skip recursive CTEs — column resolution is complex and not needed for dbt patterns
    if with.recursive {
        return;
    }

    let mut resolved_cte_columns: HashMap<String, Vec<String>> = HashMap::new();

    for cte in &mut with.ctes {
        let cte_name = normalize_cte_name(&cte.alias);

        // If CTE has explicit column list (e.g., cte(a, b) AS (...)), use that
        if !cte.columns.is_empty() {
            let cols: Vec<String> = cte.columns.iter().map(|c| c.name.clone()).collect();
            resolved_cte_columns.insert(cte_name, cols);
            continue;
        }

        // Get the SELECT from the CTE body (handle UNION by taking left branch)
        let body_select = match get_leftmost_select_mut(&mut cte.this) {
            Some(s) => s,
            None => continue,
        };

        let columns = rewrite_stars_in_select(body_select, &resolved_cte_columns, schema);
        resolved_cte_columns.insert(cte_name, columns);
    }

    // Also expand stars in the outer SELECT itself
    rewrite_stars_in_select(select, &resolved_cte_columns, schema);
}

/// Get the leftmost SELECT from an expression, drilling through UNION/INTERSECT/EXCEPT.
///
/// Per the SQL standard, the column names of a set operation (UNION, INTERSECT, EXCEPT)
/// are determined by the left branch. This matches sqlglot's behavior.
fn get_leftmost_select_mut(expr: &mut Expression) -> Option<&mut Select> {
    match expr {
        Expression::Select(s) => Some(s),
        Expression::Union(u) => get_leftmost_select_mut(&mut u.left),
        Expression::Intersect(i) => get_leftmost_select_mut(&mut i.left),
        Expression::Except(e) => get_leftmost_select_mut(&mut e.left),
        Expression::Paren(p) => get_leftmost_select_mut(&mut p.this),
        _ => None,
    }
}

/// Rewrite star expressions in a SELECT using resolved CTE column lists.
/// Falls back to `schema` for external table column lookup.
/// Returns the list of output column names after expansion.
fn rewrite_stars_in_select(
    select: &mut Select,
    resolved_ctes: &HashMap<String, Vec<String>>,
    schema: Option<&dyn Schema>,
) -> Vec<String> {
    // The AST represents star expressions in two forms depending on syntax:
    //   - `SELECT *`      → Expression::Star (unqualified star)
    //   - `SELECT table.*` → Expression::Column { name: "*", table: Some(...) } (qualified star)
    // Both must be checked to handle all star patterns.
    let has_star = select
        .expressions
        .iter()
        .any(|e| matches!(e, Expression::Star(_)));
    let has_qualified_star = select
        .expressions
        .iter()
        .any(|e| matches!(e, Expression::Column(c) if c.name.name == "*"));

    if !has_star && !has_qualified_star {
        // No stars — just extract column names without rewriting
        return select
            .expressions
            .iter()
            .filter_map(get_expression_output_name)
            .collect();
    }

    let source_names = get_select_source_names(select);
    let source_fq_names = get_select_source_fq_names(select);
    let mut new_expressions = Vec::new();
    let mut result_columns = Vec::new();

    for expr in &select.expressions {
        match expr {
            Expression::Star(star) => {
                let qual = star.table.as_ref();
                if let Some(expanded) = expand_star_from_sources(
                    qual,
                    &source_names,
                    resolved_ctes,
                    schema,
                    &source_fq_names,
                ) {
                    for (src_alias, col_name) in &expanded {
                        let table_id = Identifier::new(src_alias);
                        new_expressions.push(make_column_expr(col_name, Some(&table_id)));
                        result_columns.push(col_name.clone());
                    }
                } else {
                    new_expressions.push(expr.clone());
                    result_columns.push("*".to_string());
                }
            }
            Expression::Column(c) if c.name.name == "*" => {
                let qual = c.table.as_ref();
                if let Some(expanded) = expand_star_from_sources(
                    qual,
                    &source_names,
                    resolved_ctes,
                    schema,
                    &source_fq_names,
                ) {
                    for (_src_alias, col_name) in &expanded {
                        // Keep the original table qualifier for qualified stars (table.*)
                        new_expressions.push(make_column_expr(col_name, c.table.as_ref()));
                        result_columns.push(col_name.clone());
                    }
                } else {
                    new_expressions.push(expr.clone());
                    result_columns.push("*".to_string());
                }
            }
            _ => {
                new_expressions.push(expr.clone());
                if let Some(name) = get_expression_output_name(expr) {
                    result_columns.push(name);
                }
            }
        }
    }

    select.expressions = new_expressions;
    result_columns
}

/// Try to expand a star expression by looking up source columns from resolved CTEs,
/// falling back to the schema for external tables.
/// Returns (source_alias, column_name) pairs so the caller can set table qualifiers.
/// `qualifier`: Optional table qualifier (for `table.*`). If None, expand all sources.
/// `source_fq_names`: Fully-qualified table names for schema lookup, parallel to `source_names`.
fn expand_star_from_sources(
    qualifier: Option<&Identifier>,
    source_names: &[SourceName],
    resolved_ctes: &HashMap<String, Vec<String>>,
    schema: Option<&dyn Schema>,
    source_fq_names: &[String],
) -> Option<Vec<(String, String)>> {
    let mut expanded = Vec::new();

    if let Some(qual) = qualifier {
        // Qualified star: table.*
        let qual_normalized = normalize_cte_name(qual);
        for (i, src) in source_names.iter().enumerate() {
            if src.normalized == qual_normalized || src.alias.to_lowercase() == qual_normalized {
                // Try CTE first
                if let Some(cols) = resolved_ctes.get(&src.normalized) {
                    expanded.extend(cols.iter().map(|c| (src.alias.clone(), c.clone())));
                    return Some(expanded);
                }
                // Fall back to schema
                if let Some(cols) = lookup_schema_columns(schema, source_fq_names.get(i)) {
                    expanded.extend(cols.into_iter().map(|c| (src.alias.clone(), c)));
                    return Some(expanded);
                }
            }
        }
        None
    } else {
        // Unqualified star: expand all sources
        let mut any_expanded = false;
        for (i, src) in source_names.iter().enumerate() {
            if let Some(cols) = resolved_ctes.get(&src.normalized) {
                expanded.extend(cols.iter().map(|c| (src.alias.clone(), c.clone())));
                any_expanded = true;
            } else if let Some(cols) = lookup_schema_columns(schema, source_fq_names.get(i)) {
                expanded.extend(cols.into_iter().map(|c| (src.alias.clone(), c)));
                any_expanded = true;
            } else {
                // Source is not a resolved CTE and not in schema — can't fully expand.
                // Intentionally conservative: partial expansion is not attempted because
                // an incomplete column list would cause downstream lineage resolution to
                // produce incorrect results (missing columns silently omitted).
                // This matches sqlglot's behavior: it also requires all sources to be
                // resolvable and raises SqlglotError when schema is missing.
                return None;
            }
        }
        if any_expanded {
            Some(expanded)
        } else {
            None
        }
    }
}

/// Look up column names for a table from the schema.
fn lookup_schema_columns(schema: Option<&dyn Schema>, fq_name: Option<&String>) -> Option<Vec<String>> {
    let schema = schema?;
    let name = fq_name?;
    schema.column_names(name).ok().filter(|cols| !cols.is_empty() && !cols.contains(&"*".to_string()))
}

/// Create a Column expression with the given name and optional table qualifier.
fn make_column_expr(name: &str, table: Option<&Identifier>) -> Expression {
    Expression::Column(Box::new(crate::expressions::Column {
        name: Identifier::new(name),
        table: table.cloned(),
        join_mark: false,
        trailing_comments: Vec::new(),
        span: None,
        inferred_type: None,
    }))
}

/// Extract the output name of a SELECT expression.
fn get_expression_output_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Alias(a) => Some(a.alias.name.clone()),
        Expression::Column(c) => Some(c.name.name.clone()),
        Expression::Identifier(id) => Some(id.name.clone()),
        Expression::Star(_) => Some("*".to_string()),
        _ => None,
    }
}

/// Extract source names from a SELECT's FROM and JOIN clauses.
/// Returns (alias_or_name, original_name) pairs.
/// Source name info with normalized name for CTE lookup (respects quoted vs unquoted).
struct SourceName {
    alias: String,
    /// The normalized name for CTE lookup: unquoted → lowercased, quoted → as-is.
    normalized: String,
}

fn get_select_source_names(select: &Select) -> Vec<SourceName> {
    let mut names = Vec::new();

    fn extract_source(expr: &Expression) -> Option<SourceName> {
        match expr {
            Expression::Table(t) => {
                let normalized = normalize_cte_name(&t.name);
                let alias = t
                    .alias
                    .as_ref()
                    .map(|a| a.name.clone())
                    .unwrap_or_else(|| t.name.name.clone());
                Some(SourceName { alias, normalized })
            }
            Expression::Subquery(s) => {
                let alias = s.alias.as_ref()?.name.clone();
                let normalized = alias.to_lowercase();
                Some(SourceName { alias: alias.clone(), normalized })
            }
            Expression::Paren(p) => extract_source(&p.this),
            _ => None,
        }
    }

    if let Some(from) = &select.from {
        for expr in &from.expressions {
            if let Some(pair) = extract_source(expr) {
                names.push(pair);
            }
        }
    }
    for join in &select.joins {
        if let Some(pair) = extract_source(&join.this) {
            names.push(pair);
        }
    }
    names
}

/// Extract fully-qualified source names from a SELECT's FROM and JOIN clauses.
/// Returns names in the same order as `get_select_source_names`.
fn get_select_source_fq_names(select: &Select) -> Vec<String> {
    let mut names = Vec::new();

    fn extract_fq_name(expr: &Expression) -> Option<String> {
        match expr {
            Expression::Table(t) => {
                let mut parts = Vec::new();
                if let Some(catalog) = &t.catalog {
                    parts.push(catalog.name.clone());
                }
                if let Some(schema) = &t.schema {
                    parts.push(schema.name.clone());
                }
                parts.push(t.name.name.clone());
                Some(parts.join("."))
            }
            Expression::Subquery(s) => s.alias.as_ref().map(|a| a.name.clone()),
            Expression::Paren(p) => extract_fq_name(&p.this),
            _ => None,
        }
    }

    if let Some(from) = &select.from {
        for expr in &from.expressions {
            names.push(extract_fq_name(expr).unwrap_or_default());
        }
    }
    for join in &select.joins {
        names.push(extract_fq_name(&join.this).unwrap_or_default());
    }
    names
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

/// Maximum recursion depth for lineage tracing to prevent stack overflow
/// on circular or deeply nested CTE chains.
const MAX_LINEAGE_DEPTH: usize = 64;

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
        0,
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
    depth: usize,
) -> Result<LineageNode> {
    if depth > MAX_LINEAGE_DEPTH {
        return Err(Error::internal(format!(
            "lineage recursion depth exceeded (>{MAX_LINEAGE_DEPTH}) — possible circular CTE reference for scope '{scope_name}'"
        )));
    }
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
                depth,
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
            depth,
        );
    }

    // 2. Find the select expression for this column
    let select_expr = find_select_expr(effective_expr, &column, dialect)?;
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
                    span: None,
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
                        depth + 1,
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
                depth,
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
                depth,
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
    depth: usize,
) -> Result<LineageNode> {
    let scope_expr = &scope.expression;

    // Determine column index
    let col_index = match column {
        ColumnRef::Name(name) => column_to_index(scope_expr, name, dialect)?,
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
            depth + 1,
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
    depth: usize,
) {
    // Resolve CTE alias: if `table` is a FROM alias for a CTE (e.g., `FROM my_cte AS t`),
    // resolve it to the actual CTE name so the CTE scope lookup succeeds.
    let resolved_cte_name = resolve_cte_alias(scope, table);
    let effective_table = resolved_cte_name.as_deref().unwrap_or(table);

    // Check if table is a CTE reference — check both the current scope's cte_sources
    // and ancestor CTE scopes (for sibling CTEs in parent WITH clauses).
    let is_cte = scope.cte_sources.contains_key(effective_table)
        || all_cte_scopes.iter().any(|s| {
            matches!(&s.expression, Expression::Cte(cte) if cte.alias.name == effective_table)
        });
    if is_cte {
        if let Some(child_scope) = find_child_scope_in(all_cte_scopes, scope, effective_table) {
            // Build ancestor CTE scopes from all_cte_scopes for the recursive call
            let ancestors: Vec<Scope> = all_cte_scopes.iter().map(|s| (*s).clone()).collect();
            if let Ok(child) = to_node_inner(
                ColumnRef::Name(col_name),
                child_scope,
                dialect,
                parent_name,
                effective_table,
                parent_name,
                trim_selects,
                &ancestors,
                depth + 1,
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
                    depth + 1,
                ) {
                    node.downstream.push(child);
                    return;
                }
            }
        }
    }

    // Base table source found in current scope: preserve alias in the display name
    // but store the resolved table expression and name for downstream consumers.
    if let Some(source_info) = scope.sources.get(table) {
        if !source_info.is_scope {
            node.downstream.push(make_table_column_node_from_source(
                table,
                col_name,
                &source_info.expression,
            ));
            return;
        }
    }

    // Base table or unresolved — terminal node
    node.downstream
        .push(make_table_column_node(table, col_name));
}

/// Resolve a FROM alias to the original CTE name.
///
/// When a query uses `FROM my_cte AS alias`, the scope's `sources` map contains
/// `"alias"` → CTE expression, but `cte_sources` only contains `"my_cte"`.
/// This function checks if `name` is such an alias and returns the CTE name.
fn resolve_cte_alias(scope: &Scope, name: &str) -> Option<String> {
    // If it's already a known CTE name, no resolution needed
    if scope.cte_sources.contains_key(name) {
        return None;
    }
    // Check if the source's expression is a CTE — if so, extract the CTE name
    if let Some(source_info) = scope.sources.get(name) {
        if source_info.is_scope {
            if let Expression::Cte(cte) = &source_info.expression {
                let cte_name = &cte.alias.name;
                if scope.cte_sources.contains_key(cte_name) {
                    return Some(cte_name.clone());
                }
            }
        }
    }
    None
}

fn resolve_unqualified_column(
    node: &mut LineageNode,
    scope: &Scope,
    dialect: Option<DialectType>,
    col_name: &str,
    parent_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[&Scope],
    depth: usize,
) {
    // Try to find which source this column belongs to.
    // Build the source list from the actual FROM/JOIN clauses to avoid
    // mixing in CTE definitions that are in scope but not referenced.
    let from_source_names = source_names_from_from_join(scope);

    if from_source_names.len() == 1 {
        let tbl = &from_source_names[0];
        resolve_qualified_column(
            node,
            scope,
            dialect,
            tbl,
            col_name,
            parent_name,
            trim_selects,
            all_cte_scopes,
            depth,
        );
        return;
    }

    // Multiple sources — can't resolve without schema info, add unqualified node
    let child = LineageNode::new(
        col_name.to_string(),
        Expression::Column(Box::new(crate::expressions::Column {
            name: crate::expressions::Identifier::new(col_name.to_string()),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
            span: None,
            inferred_type: None,
        })),
        node.source.clone(),
    );
    node.downstream.push(child);
}

fn source_names_from_from_join(scope: &Scope) -> Vec<String> {
    fn source_name(expr: &Expression) -> Option<String> {
        match expr {
            Expression::Table(table) => Some(
                table
                    .alias
                    .as_ref()
                    .map(|a| a.name.clone())
                    .unwrap_or_else(|| table.name.name.clone()),
            ),
            Expression::Subquery(subquery) => {
                subquery.alias.as_ref().map(|alias| alias.name.clone())
            }
            Expression::Paren(paren) => source_name(&paren.this),
            _ => None,
        }
    }

    let effective_expr = match &scope.expression {
        Expression::Cte(cte) => &cte.this,
        expr => expr,
    };

    let mut names = Vec::new();
    let mut seen = std::collections::HashSet::new();

    if let Expression::Select(select) = effective_expr {
        if let Some(from) = &select.from {
            for expr in &from.expressions {
                if let Some(name) = source_name(expr) {
                    if !name.is_empty() && seen.insert(name.clone()) {
                        names.push(name);
                    }
                }
            }
        }
        for join in &select.joins {
            if let Some(name) = source_name(&join.this) {
                if !name.is_empty() && seen.insert(name.clone()) {
                    names.push(name);
                }
            }
        }
    }

    names
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
fn find_select_expr(
    scope_expr: &Expression,
    column: &ColumnRef<'_>,
    dialect: Option<DialectType>,
) -> Result<Expression> {
    if let Expression::Select(ref select) = scope_expr {
        match column {
            ColumnRef::Name(name) => {
                let normalized_name = normalize_column_name(name, dialect);
                for expr in &select.expressions {
                    if let Some(alias_or_name) = get_alias_or_name(expr) {
                        if normalize_column_name(&alias_or_name, dialect) == normalized_name {
                            return Ok(expr.clone());
                        }
                    }
                }
                Err(crate::error::Error::parse(
                    format!("Cannot find column '{}' in query", name),
                    0,
                    0,
                    0,
                    0,
                ))
            }
            ColumnRef::Index(idx) => select.expressions.get(*idx).cloned().ok_or_else(|| {
                crate::error::Error::parse(format!("Column index {} out of range", idx), 0, 0, 0, 0)
            }),
        }
    } else {
        Err(crate::error::Error::parse(
            "Expected SELECT expression for column lookup",
            0,
            0,
            0,
            0,
        ))
    }
}

/// Find the positional index of a column name in a set operation's first SELECT branch.
fn column_to_index(
    set_op_expr: &Expression,
    name: &str,
    dialect: Option<DialectType>,
) -> Result<usize> {
    let normalized_name = normalize_column_name(name, dialect);
    let mut expr = set_op_expr;
    loop {
        match expr {
            Expression::Union(u) => expr = &u.left,
            Expression::Intersect(i) => expr = &i.left,
            Expression::Except(e) => expr = &e.left,
            Expression::Select(select) => {
                for (i, e) in select.expressions.iter().enumerate() {
                    if let Some(alias_or_name) = get_alias_or_name(e) {
                        if normalize_column_name(&alias_or_name, dialect) == normalized_name {
                            return Ok(i);
                        }
                    }
                }
                return Err(crate::error::Error::parse(
                    format!("Cannot find column '{}' in set operation", name),
                    0,
                    0,
                    0,
                    0,
                ));
            }
            _ => {
                return Err(crate::error::Error::parse(
                    "Expected SELECT or set operation",
                    0,
                    0,
                    0,
                    0,
                ))
            }
        }
    }
}

fn normalize_column_name(name: &str, dialect: Option<DialectType>) -> String {
    normalize_name(name, dialect, false, true)
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
    let mut node = LineageNode::new(
        format!("{}.{}", table, column),
        Expression::Column(Box::new(crate::expressions::Column {
            name: crate::expressions::Identifier::new(column.to_string()),
            table: Some(crate::expressions::Identifier::new(table.to_string())),
            join_mark: false,
            trailing_comments: vec![],
            span: None,
            inferred_type: None,
        })),
        Expression::Table(Box::new(crate::expressions::TableRef::new(table))),
    );
    node.source_name = table.to_string();
    node
}

fn table_name_from_table_ref(table_ref: &crate::expressions::TableRef) -> String {
    let mut parts: Vec<String> = Vec::new();
    if let Some(catalog) = &table_ref.catalog {
        parts.push(catalog.name.clone());
    }
    if let Some(schema) = &table_ref.schema {
        parts.push(schema.name.clone());
    }
    parts.push(table_ref.name.name.clone());
    parts.join(".")
}

fn make_table_column_node_from_source(
    table_alias: &str,
    column: &str,
    source: &Expression,
) -> LineageNode {
    let mut node = LineageNode::new(
        format!("{}.{}", table_alias, column),
        Expression::Column(Box::new(crate::expressions::Column {
            name: crate::expressions::Identifier::new(column.to_string()),
            table: Some(crate::expressions::Identifier::new(table_alias.to_string())),
            join_mark: false,
            trailing_comments: vec![],
            span: None,
            inferred_type: None,
        })),
        source.clone(),
    );

    if let Expression::Table(table_ref) = source {
        node.source_name = table_name_from_table_ref(table_ref);
    } else {
        node.source_name = table_alias.to_string();
    }

    node
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
    let mut stack: Vec<&Expression> = vec![expr];

    while let Some(current) = stack.pop() {
        match current {
            // === Leaf: collect Column references ===
            Expression::Column(col) => {
                refs.push(SimpleColumnRef {
                    table: col.table.clone(),
                    column: col.name.name.clone(),
                });
            }

            // === Boundary: don't recurse into subqueries (handled separately) ===
            Expression::Subquery(_) | Expression::Exists(_) => {}

            // === BinaryOp variants: left, right ===
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
            | Expression::BitwiseLeftShift(op)
            | Expression::BitwiseRightShift(op)
            | Expression::Concat(op)
            | Expression::Adjacent(op)
            | Expression::TsMatch(op)
            | Expression::PropertyEQ(op)
            | Expression::ArrayContainsAll(op)
            | Expression::ArrayContainedBy(op)
            | Expression::ArrayOverlaps(op)
            | Expression::JSONBContainsAllTopKeys(op)
            | Expression::JSONBContainsAnyTopKeys(op)
            | Expression::JSONBDeleteAtPath(op)
            | Expression::ExtendsLeft(op)
            | Expression::ExtendsRight(op)
            | Expression::Is(op)
            | Expression::MemberOf(op)
            | Expression::NullSafeEq(op)
            | Expression::NullSafeNeq(op)
            | Expression::Glob(op)
            | Expression::Match(op) => {
                stack.push(&op.left);
                stack.push(&op.right);
            }

            // === UnaryOp variants: this ===
            Expression::Not(u) | Expression::Neg(u) | Expression::BitwiseNot(u) => {
                stack.push(&u.this);
            }

            // === UnaryFunc variants: this ===
            Expression::Upper(f)
            | Expression::Lower(f)
            | Expression::Length(f)
            | Expression::LTrim(f)
            | Expression::RTrim(f)
            | Expression::Reverse(f)
            | Expression::Abs(f)
            | Expression::Sqrt(f)
            | Expression::Cbrt(f)
            | Expression::Ln(f)
            | Expression::Exp(f)
            | Expression::Sign(f)
            | Expression::Date(f)
            | Expression::Time(f)
            | Expression::DateFromUnixDate(f)
            | Expression::UnixDate(f)
            | Expression::UnixSeconds(f)
            | Expression::UnixMillis(f)
            | Expression::UnixMicros(f)
            | Expression::TimeStrToDate(f)
            | Expression::DateToDi(f)
            | Expression::DiToDate(f)
            | Expression::TsOrDiToDi(f)
            | Expression::TsOrDsToDatetime(f)
            | Expression::TsOrDsToTimestamp(f)
            | Expression::YearOfWeek(f)
            | Expression::YearOfWeekIso(f)
            | Expression::Initcap(f)
            | Expression::Ascii(f)
            | Expression::Chr(f)
            | Expression::Soundex(f)
            | Expression::ByteLength(f)
            | Expression::Hex(f)
            | Expression::LowerHex(f)
            | Expression::Unicode(f)
            | Expression::Radians(f)
            | Expression::Degrees(f)
            | Expression::Sin(f)
            | Expression::Cos(f)
            | Expression::Tan(f)
            | Expression::Asin(f)
            | Expression::Acos(f)
            | Expression::Atan(f)
            | Expression::IsNan(f)
            | Expression::IsInf(f)
            | Expression::ArrayLength(f)
            | Expression::ArraySize(f)
            | Expression::Cardinality(f)
            | Expression::ArrayReverse(f)
            | Expression::ArrayDistinct(f)
            | Expression::ArrayFlatten(f)
            | Expression::ArrayCompact(f)
            | Expression::Explode(f)
            | Expression::ExplodeOuter(f)
            | Expression::ToArray(f)
            | Expression::MapFromEntries(f)
            | Expression::MapKeys(f)
            | Expression::MapValues(f)
            | Expression::JsonArrayLength(f)
            | Expression::JsonKeys(f)
            | Expression::JsonType(f)
            | Expression::ParseJson(f)
            | Expression::ToJson(f)
            | Expression::Typeof(f)
            | Expression::BitwiseCount(f)
            | Expression::Year(f)
            | Expression::Month(f)
            | Expression::Day(f)
            | Expression::Hour(f)
            | Expression::Minute(f)
            | Expression::Second(f)
            | Expression::DayOfWeek(f)
            | Expression::DayOfWeekIso(f)
            | Expression::DayOfMonth(f)
            | Expression::DayOfYear(f)
            | Expression::WeekOfYear(f)
            | Expression::Quarter(f)
            | Expression::Epoch(f)
            | Expression::EpochMs(f)
            | Expression::TimeStrToUnix(f)
            | Expression::SHA(f)
            | Expression::SHA1Digest(f)
            | Expression::TimeToUnix(f)
            | Expression::JSONBool(f)
            | Expression::Int64(f)
            | Expression::MD5NumberLower64(f)
            | Expression::MD5NumberUpper64(f)
            | Expression::DateStrToDate(f)
            | Expression::DateToDateStr(f) => {
                stack.push(&f.this);
            }

            // === BinaryFunc variants: this, expression ===
            Expression::Power(f)
            | Expression::NullIf(f)
            | Expression::IfNull(f)
            | Expression::Nvl(f)
            | Expression::UnixToTimeStr(f)
            | Expression::Contains(f)
            | Expression::StartsWith(f)
            | Expression::EndsWith(f)
            | Expression::Levenshtein(f)
            | Expression::ModFunc(f)
            | Expression::Atan2(f)
            | Expression::IntDiv(f)
            | Expression::AddMonths(f)
            | Expression::MonthsBetween(f)
            | Expression::NextDay(f)
            | Expression::ArrayContains(f)
            | Expression::ArrayPosition(f)
            | Expression::ArrayAppend(f)
            | Expression::ArrayPrepend(f)
            | Expression::ArrayUnion(f)
            | Expression::ArrayExcept(f)
            | Expression::ArrayRemove(f)
            | Expression::StarMap(f)
            | Expression::MapFromArrays(f)
            | Expression::MapContainsKey(f)
            | Expression::ElementAt(f)
            | Expression::JsonMergePatch(f)
            | Expression::JSONBContains(f)
            | Expression::JSONBExtract(f) => {
                stack.push(&f.this);
                stack.push(&f.expression);
            }

            // === VarArgFunc variants: expressions ===
            Expression::Greatest(f)
            | Expression::Least(f)
            | Expression::Coalesce(f)
            | Expression::ArrayConcat(f)
            | Expression::ArrayIntersect(f)
            | Expression::ArrayZip(f)
            | Expression::MapConcat(f)
            | Expression::JsonArray(f) => {
                for e in &f.expressions {
                    stack.push(e);
                }
            }

            // === AggFunc variants: this, filter, having_max, limit ===
            Expression::Sum(f)
            | Expression::Avg(f)
            | Expression::Min(f)
            | Expression::Max(f)
            | Expression::ArrayAgg(f)
            | Expression::CountIf(f)
            | Expression::Stddev(f)
            | Expression::StddevPop(f)
            | Expression::StddevSamp(f)
            | Expression::Variance(f)
            | Expression::VarPop(f)
            | Expression::VarSamp(f)
            | Expression::Median(f)
            | Expression::Mode(f)
            | Expression::First(f)
            | Expression::Last(f)
            | Expression::AnyValue(f)
            | Expression::ApproxDistinct(f)
            | Expression::ApproxCountDistinct(f)
            | Expression::LogicalAnd(f)
            | Expression::LogicalOr(f)
            | Expression::Skewness(f)
            | Expression::ArrayConcatAgg(f)
            | Expression::ArrayUniqueAgg(f)
            | Expression::BoolXorAgg(f)
            | Expression::BitwiseAndAgg(f)
            | Expression::BitwiseOrAgg(f)
            | Expression::BitwiseXorAgg(f) => {
                stack.push(&f.this);
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
                if let Some((ref expr, _)) = f.having_max {
                    stack.push(expr);
                }
                if let Some(ref limit) = f.limit {
                    stack.push(limit);
                }
            }

            // === Generic Function / AggregateFunction: args ===
            Expression::Function(func) => {
                for arg in &func.args {
                    stack.push(arg);
                }
            }
            Expression::AggregateFunction(func) => {
                for arg in &func.args {
                    stack.push(arg);
                }
                if let Some(ref filter) = func.filter {
                    stack.push(filter);
                }
                if let Some(ref limit) = func.limit {
                    stack.push(limit);
                }
            }

            // === WindowFunction: this (skip Over for lineage purposes) ===
            Expression::WindowFunction(wf) => {
                stack.push(&wf.this);
            }

            // === Containers and special expressions ===
            Expression::Alias(a) => {
                stack.push(&a.this);
            }
            Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c) => {
                stack.push(&c.this);
                if let Some(ref fmt) = c.format {
                    stack.push(fmt);
                }
                if let Some(ref def) = c.default {
                    stack.push(def);
                }
            }
            Expression::Paren(p) => {
                stack.push(&p.this);
            }
            Expression::Annotated(a) => {
                stack.push(&a.this);
            }
            Expression::Case(case) => {
                if let Some(ref operand) = case.operand {
                    stack.push(operand);
                }
                for (cond, result) in &case.whens {
                    stack.push(cond);
                    stack.push(result);
                }
                if let Some(ref else_expr) = case.else_ {
                    stack.push(else_expr);
                }
            }
            Expression::Collation(c) => {
                stack.push(&c.this);
            }
            Expression::In(i) => {
                stack.push(&i.this);
                for e in &i.expressions {
                    stack.push(e);
                }
                if let Some(ref q) = i.query {
                    stack.push(q);
                }
                if let Some(ref u) = i.unnest {
                    stack.push(u);
                }
            }
            Expression::Between(b) => {
                stack.push(&b.this);
                stack.push(&b.low);
                stack.push(&b.high);
            }
            Expression::IsNull(n) => {
                stack.push(&n.this);
            }
            Expression::IsTrue(t) | Expression::IsFalse(t) => {
                stack.push(&t.this);
            }
            Expression::IsJson(j) => {
                stack.push(&j.this);
            }
            Expression::Like(l) | Expression::ILike(l) => {
                stack.push(&l.left);
                stack.push(&l.right);
                if let Some(ref esc) = l.escape {
                    stack.push(esc);
                }
            }
            Expression::SimilarTo(s) => {
                stack.push(&s.this);
                stack.push(&s.pattern);
                if let Some(ref esc) = s.escape {
                    stack.push(esc);
                }
            }
            Expression::Ordered(o) => {
                stack.push(&o.this);
            }
            Expression::Array(a) => {
                for e in &a.expressions {
                    stack.push(e);
                }
            }
            Expression::Tuple(t) => {
                for e in &t.expressions {
                    stack.push(e);
                }
            }
            Expression::Struct(s) => {
                for (_, e) in &s.fields {
                    stack.push(e);
                }
            }
            Expression::Subscript(s) => {
                stack.push(&s.this);
                stack.push(&s.index);
            }
            Expression::Dot(d) => {
                stack.push(&d.this);
            }
            Expression::MethodCall(m) => {
                stack.push(&m.this);
                for arg in &m.args {
                    stack.push(arg);
                }
            }
            Expression::ArraySlice(s) => {
                stack.push(&s.this);
                if let Some(ref start) = s.start {
                    stack.push(start);
                }
                if let Some(ref end) = s.end {
                    stack.push(end);
                }
            }
            Expression::Lambda(l) => {
                stack.push(&l.body);
            }
            Expression::NamedArgument(n) => {
                stack.push(&n.value);
            }
            Expression::BracedWildcard(e) | Expression::ReturnStmt(e) => {
                stack.push(e);
            }

            // === Custom function structs ===
            Expression::Substring(f) => {
                stack.push(&f.this);
                stack.push(&f.start);
                if let Some(ref len) = f.length {
                    stack.push(len);
                }
            }
            Expression::Trim(f) => {
                stack.push(&f.this);
                if let Some(ref chars) = f.characters {
                    stack.push(chars);
                }
            }
            Expression::Replace(f) => {
                stack.push(&f.this);
                stack.push(&f.old);
                stack.push(&f.new);
            }
            Expression::IfFunc(f) => {
                stack.push(&f.condition);
                stack.push(&f.true_value);
                if let Some(ref fv) = f.false_value {
                    stack.push(fv);
                }
            }
            Expression::Nvl2(f) => {
                stack.push(&f.this);
                stack.push(&f.true_value);
                stack.push(&f.false_value);
            }
            Expression::ConcatWs(f) => {
                stack.push(&f.separator);
                for e in &f.expressions {
                    stack.push(e);
                }
            }
            Expression::Count(f) => {
                if let Some(ref this) = f.this {
                    stack.push(this);
                }
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
            }
            Expression::GroupConcat(f) => {
                stack.push(&f.this);
                if let Some(ref sep) = f.separator {
                    stack.push(sep);
                }
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
            }
            Expression::StringAgg(f) => {
                stack.push(&f.this);
                if let Some(ref sep) = f.separator {
                    stack.push(sep);
                }
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
                if let Some(ref limit) = f.limit {
                    stack.push(limit);
                }
            }
            Expression::ListAgg(f) => {
                stack.push(&f.this);
                if let Some(ref sep) = f.separator {
                    stack.push(sep);
                }
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
            }
            Expression::SumIf(f) => {
                stack.push(&f.this);
                stack.push(&f.condition);
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
            }
            Expression::DateAdd(f) | Expression::DateSub(f) => {
                stack.push(&f.this);
                stack.push(&f.interval);
            }
            Expression::DateDiff(f) => {
                stack.push(&f.this);
                stack.push(&f.expression);
            }
            Expression::DateTrunc(f) | Expression::TimestampTrunc(f) => {
                stack.push(&f.this);
            }
            Expression::Extract(f) => {
                stack.push(&f.this);
            }
            Expression::Round(f) => {
                stack.push(&f.this);
                if let Some(ref d) = f.decimals {
                    stack.push(d);
                }
            }
            Expression::Floor(f) => {
                stack.push(&f.this);
                if let Some(ref s) = f.scale {
                    stack.push(s);
                }
                if let Some(ref t) = f.to {
                    stack.push(t);
                }
            }
            Expression::Ceil(f) => {
                stack.push(&f.this);
                if let Some(ref d) = f.decimals {
                    stack.push(d);
                }
                if let Some(ref t) = f.to {
                    stack.push(t);
                }
            }
            Expression::Log(f) => {
                stack.push(&f.this);
                if let Some(ref b) = f.base {
                    stack.push(b);
                }
            }
            Expression::AtTimeZone(f) => {
                stack.push(&f.this);
                stack.push(&f.zone);
            }
            Expression::Lead(f) | Expression::Lag(f) => {
                stack.push(&f.this);
                if let Some(ref off) = f.offset {
                    stack.push(off);
                }
                if let Some(ref def) = f.default {
                    stack.push(def);
                }
            }
            Expression::FirstValue(f) | Expression::LastValue(f) => {
                stack.push(&f.this);
            }
            Expression::NthValue(f) => {
                stack.push(&f.this);
                stack.push(&f.offset);
            }
            Expression::Position(f) => {
                stack.push(&f.substring);
                stack.push(&f.string);
                if let Some(ref start) = f.start {
                    stack.push(start);
                }
            }
            Expression::Decode(f) => {
                stack.push(&f.this);
                for (search, result) in &f.search_results {
                    stack.push(search);
                    stack.push(result);
                }
                if let Some(ref def) = f.default {
                    stack.push(def);
                }
            }
            Expression::CharFunc(f) => {
                for arg in &f.args {
                    stack.push(arg);
                }
            }
            Expression::ArraySort(f) => {
                stack.push(&f.this);
                if let Some(ref cmp) = f.comparator {
                    stack.push(cmp);
                }
            }
            Expression::ArrayJoin(f) | Expression::ArrayToString(f) => {
                stack.push(&f.this);
                stack.push(&f.separator);
                if let Some(ref nr) = f.null_replacement {
                    stack.push(nr);
                }
            }
            Expression::ArrayFilter(f) => {
                stack.push(&f.this);
                stack.push(&f.filter);
            }
            Expression::ArrayTransform(f) => {
                stack.push(&f.this);
                stack.push(&f.transform);
            }
            Expression::Sequence(f)
            | Expression::Generate(f)
            | Expression::ExplodingGenerateSeries(f) => {
                stack.push(&f.start);
                stack.push(&f.stop);
                if let Some(ref step) = f.step {
                    stack.push(step);
                }
            }
            Expression::JsonExtract(f)
            | Expression::JsonExtractScalar(f)
            | Expression::JsonQuery(f)
            | Expression::JsonValue(f) => {
                stack.push(&f.this);
                stack.push(&f.path);
            }
            Expression::JsonExtractPath(f) | Expression::JsonRemove(f) => {
                stack.push(&f.this);
                for p in &f.paths {
                    stack.push(p);
                }
            }
            Expression::JsonObject(f) => {
                for (k, v) in &f.pairs {
                    stack.push(k);
                    stack.push(v);
                }
            }
            Expression::JsonSet(f) | Expression::JsonInsert(f) => {
                stack.push(&f.this);
                for (path, val) in &f.path_values {
                    stack.push(path);
                    stack.push(val);
                }
            }
            Expression::Overlay(f) => {
                stack.push(&f.this);
                stack.push(&f.replacement);
                stack.push(&f.from);
                if let Some(ref len) = f.length {
                    stack.push(len);
                }
            }
            Expression::Convert(f) => {
                stack.push(&f.this);
                if let Some(ref style) = f.style {
                    stack.push(style);
                }
            }
            Expression::ApproxPercentile(f) => {
                stack.push(&f.this);
                stack.push(&f.percentile);
                if let Some(ref acc) = f.accuracy {
                    stack.push(acc);
                }
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
            }
            Expression::Percentile(f)
            | Expression::PercentileCont(f)
            | Expression::PercentileDisc(f) => {
                stack.push(&f.this);
                stack.push(&f.percentile);
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
            }
            Expression::WithinGroup(f) => {
                stack.push(&f.this);
            }
            Expression::Left(f) | Expression::Right(f) => {
                stack.push(&f.this);
                stack.push(&f.length);
            }
            Expression::Repeat(f) => {
                stack.push(&f.this);
                stack.push(&f.times);
            }
            Expression::Lpad(f) | Expression::Rpad(f) => {
                stack.push(&f.this);
                stack.push(&f.length);
                if let Some(ref fill) = f.fill {
                    stack.push(fill);
                }
            }
            Expression::Split(f) => {
                stack.push(&f.this);
                stack.push(&f.delimiter);
            }
            Expression::RegexpLike(f) => {
                stack.push(&f.this);
                stack.push(&f.pattern);
                if let Some(ref flags) = f.flags {
                    stack.push(flags);
                }
            }
            Expression::RegexpReplace(f) => {
                stack.push(&f.this);
                stack.push(&f.pattern);
                stack.push(&f.replacement);
                if let Some(ref flags) = f.flags {
                    stack.push(flags);
                }
            }
            Expression::RegexpExtract(f) => {
                stack.push(&f.this);
                stack.push(&f.pattern);
                if let Some(ref group) = f.group {
                    stack.push(group);
                }
            }
            Expression::ToDate(f) => {
                stack.push(&f.this);
                if let Some(ref fmt) = f.format {
                    stack.push(fmt);
                }
            }
            Expression::ToTimestamp(f) => {
                stack.push(&f.this);
                if let Some(ref fmt) = f.format {
                    stack.push(fmt);
                }
            }
            Expression::DateFormat(f) | Expression::FormatDate(f) => {
                stack.push(&f.this);
                stack.push(&f.format);
            }
            Expression::LastDay(f) => {
                stack.push(&f.this);
            }
            Expression::FromUnixtime(f) => {
                stack.push(&f.this);
                if let Some(ref fmt) = f.format {
                    stack.push(fmt);
                }
            }
            Expression::UnixTimestamp(f) => {
                if let Some(ref this) = f.this {
                    stack.push(this);
                }
                if let Some(ref fmt) = f.format {
                    stack.push(fmt);
                }
            }
            Expression::MakeDate(f) => {
                stack.push(&f.year);
                stack.push(&f.month);
                stack.push(&f.day);
            }
            Expression::MakeTimestamp(f) => {
                stack.push(&f.year);
                stack.push(&f.month);
                stack.push(&f.day);
                stack.push(&f.hour);
                stack.push(&f.minute);
                stack.push(&f.second);
                if let Some(ref tz) = f.timezone {
                    stack.push(tz);
                }
            }
            Expression::TruncFunc(f) => {
                stack.push(&f.this);
                if let Some(ref d) = f.decimals {
                    stack.push(d);
                }
            }
            Expression::ArrayFunc(f) => {
                for e in &f.expressions {
                    stack.push(e);
                }
            }
            Expression::Unnest(f) => {
                stack.push(&f.this);
                for e in &f.expressions {
                    stack.push(e);
                }
            }
            Expression::StructFunc(f) => {
                for (_, e) in &f.fields {
                    stack.push(e);
                }
            }
            Expression::StructExtract(f) => {
                stack.push(&f.this);
            }
            Expression::NamedStruct(f) => {
                for (k, v) in &f.pairs {
                    stack.push(k);
                    stack.push(v);
                }
            }
            Expression::MapFunc(f) => {
                for k in &f.keys {
                    stack.push(k);
                }
                for v in &f.values {
                    stack.push(v);
                }
            }
            Expression::TransformKeys(f) | Expression::TransformValues(f) => {
                stack.push(&f.this);
                stack.push(&f.transform);
            }
            Expression::JsonArrayAgg(f) => {
                stack.push(&f.this);
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
            }
            Expression::JsonObjectAgg(f) => {
                stack.push(&f.key);
                stack.push(&f.value);
                if let Some(ref filter) = f.filter {
                    stack.push(filter);
                }
            }
            Expression::NTile(f) => {
                if let Some(ref n) = f.num_buckets {
                    stack.push(n);
                }
            }
            Expression::Rand(f) => {
                if let Some(ref s) = f.seed {
                    stack.push(s);
                }
                if let Some(ref lo) = f.lower {
                    stack.push(lo);
                }
                if let Some(ref hi) = f.upper {
                    stack.push(hi);
                }
            }
            Expression::Any(q) | Expression::All(q) => {
                stack.push(&q.this);
                stack.push(&q.subquery);
            }
            Expression::Overlaps(o) => {
                if let Some(ref this) = o.this {
                    stack.push(this);
                }
                if let Some(ref expr) = o.expression {
                    stack.push(expr);
                }
                if let Some(ref ls) = o.left_start {
                    stack.push(ls);
                }
                if let Some(ref le) = o.left_end {
                    stack.push(le);
                }
                if let Some(ref rs) = o.right_start {
                    stack.push(rs);
                }
                if let Some(ref re) = o.right_end {
                    stack.push(re);
                }
            }
            Expression::Interval(i) => {
                if let Some(ref this) = i.this {
                    stack.push(this);
                }
            }
            Expression::TimeStrToTime(f) => {
                stack.push(&f.this);
                if let Some(ref zone) = f.zone {
                    stack.push(zone);
                }
            }
            Expression::JSONBExtractScalar(f) => {
                stack.push(&f.this);
                stack.push(&f.expression);
                if let Some(ref jt) = f.json_type {
                    stack.push(jt);
                }
            }

            // === True leaves and non-expression-bearing nodes ===
            // Literals, Identifier, Star, DataType, Placeholder, Boolean, Null,
            // CurrentDate/Time/Timestamp, RowNumber, Rank, DenseRank, PercentRank,
            // CumeDist, Random, Pi, SessionUser, DDL statements, clauses, etc.
            _ => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::{Dialect, DialectType};
    use crate::expressions::DataType;
    use crate::optimizer::annotate_types::annotate_types;
    use crate::parse_one;
    use crate::schema::{MappingSchema, Schema};

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
    fn test_lineage_with_schema_qualifies_root_expression_issue_40() {
        let query = "SELECT name FROM users";
        let dialect = Dialect::get(DialectType::BigQuery);
        let expr = dialect
            .parse(query)
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one expression");

        let mut schema = MappingSchema::with_dialect(DialectType::BigQuery);
        schema
            .add_table("users", &[("name".into(), DataType::Text)], None)
            .expect("schema setup");

        let node_without_schema = lineage("name", &expr, Some(DialectType::BigQuery), false)
            .expect("lineage without schema");
        let mut expr_without = node_without_schema.expression.clone();
        annotate_types(
            &mut expr_without,
            Some(&schema),
            Some(DialectType::BigQuery),
        );
        assert_eq!(
            expr_without.inferred_type(),
            None,
            "Expected unresolved root type without schema-aware lineage qualification"
        );

        let node_with_schema = lineage_with_schema(
            "name",
            &expr,
            Some(&schema),
            Some(DialectType::BigQuery),
            false,
        )
        .expect("lineage with schema");
        let mut expr_with = node_with_schema.expression.clone();
        annotate_types(&mut expr_with, Some(&schema), Some(DialectType::BigQuery));

        assert_eq!(expr_with.inferred_type(), Some(&DataType::Text));
    }

    #[test]
    fn test_lineage_with_schema_correlated_scalar_subquery() {
        let query = "SELECT id, (SELECT AVG(val) FROM t2 WHERE t2.id = t1.id) AS avg_val FROM t1";
        let dialect = Dialect::get(DialectType::BigQuery);
        let expr = dialect
            .parse(query)
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one expression");

        let mut schema = MappingSchema::with_dialect(DialectType::BigQuery);
        schema
            .add_table(
                "t1",
                &[("id".into(), DataType::BigInt { length: None })],
                None,
            )
            .expect("schema setup");
        schema
            .add_table(
                "t2",
                &[
                    ("id".into(), DataType::BigInt { length: None }),
                    ("val".into(), DataType::BigInt { length: None }),
                ],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "id",
            &expr,
            Some(&schema),
            Some(DialectType::BigQuery),
            false,
        )
        .expect("lineage_with_schema should handle correlated scalar subqueries");

        assert_eq!(node.name, "id");
    }

    #[test]
    fn test_lineage_with_schema_join_using() {
        let query = "SELECT a FROM t1 JOIN t2 USING(a)";
        let dialect = Dialect::get(DialectType::BigQuery);
        let expr = dialect
            .parse(query)
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one expression");

        let mut schema = MappingSchema::with_dialect(DialectType::BigQuery);
        schema
            .add_table(
                "t1",
                &[("a".into(), DataType::BigInt { length: None })],
                None,
            )
            .expect("schema setup");
        schema
            .add_table(
                "t2",
                &[("a".into(), DataType::BigInt { length: None })],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "a",
            &expr,
            Some(&schema),
            Some(DialectType::BigQuery),
            false,
        )
        .expect("lineage_with_schema should handle JOIN USING");

        assert_eq!(node.name, "a");
    }

    #[test]
    fn test_lineage_with_schema_qualified_table_name() {
        let query = "SELECT a FROM raw.t1";
        let dialect = Dialect::get(DialectType::BigQuery);
        let expr = dialect
            .parse(query)
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one expression");

        let mut schema = MappingSchema::with_dialect(DialectType::BigQuery);
        schema
            .add_table(
                "raw.t1",
                &[("a".into(), DataType::BigInt { length: None })],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "a",
            &expr,
            Some(&schema),
            Some(DialectType::BigQuery),
            false,
        )
        .expect("lineage_with_schema should handle dotted schema.table names");

        assert_eq!(node.name, "a");
    }

    #[test]
    fn test_lineage_with_schema_none_matches_lineage() {
        let expr = parse("SELECT a FROM t");
        let baseline = lineage("a", &expr, None, false).expect("lineage baseline");
        let with_none =
            lineage_with_schema("a", &expr, None, None, false).expect("lineage_with_schema");

        assert_eq!(with_none.name, baseline.name);
        assert_eq!(with_none.downstream_names(), baseline.downstream_names());
    }

    #[test]
    fn test_lineage_with_schema_bigquery_mixed_case_column_names_issue_60() {
        let dialect = Dialect::get(DialectType::BigQuery);
        let expr = dialect
            .parse("SELECT Name AS name FROM teams")
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one expression");

        let mut schema = MappingSchema::with_dialect(DialectType::BigQuery);
        schema
            .add_table(
                "teams",
                &[("Name".into(), DataType::String { length: None })],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "name",
            &expr,
            Some(&schema),
            Some(DialectType::BigQuery),
            false,
        )
        .expect("lineage_with_schema should resolve mixed-case BigQuery columns");

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "teams.Name"),
            "Expected teams.Name in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_bigquery_mixed_case_alias_lookup() {
        let dialect = Dialect::get(DialectType::BigQuery);
        let expr = dialect
            .parse("SELECT Name AS Name FROM teams")
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one expression");

        let node = lineage("name", &expr, Some(DialectType::BigQuery), false)
            .expect("lineage should resolve mixed-case aliases in BigQuery");

        assert_eq!(node.name, "name");
    }

    #[test]
    fn test_lineage_with_schema_snowflake_datediff_date_part_issue_61() {
        let expr = parse_one(
            "SELECT DATEDIFF(day, date_utc, CURRENT_DATE()) AS recency FROM fact.some_daily_metrics",
            DialectType::Snowflake,
        )
        .expect("parse");

        let mut schema = MappingSchema::with_dialect(DialectType::Snowflake);
        schema
            .add_table(
                "fact.some_daily_metrics",
                &[("date_utc".to_string(), DataType::Date)],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "recency",
            &expr,
            Some(&schema),
            Some(DialectType::Snowflake),
            false,
        )
        .expect("lineage_with_schema should not treat date part as a column");

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "some_daily_metrics.date_utc"),
            "Expected some_daily_metrics.date_utc in downstream, got: {:?}",
            names
        );
        assert!(
            !names.iter().any(|n| n.ends_with(".day") || n == "day"),
            "Did not expect date part to appear as lineage column, got: {:?}",
            names
        );
    }

    #[test]
    fn test_snowflake_datediff_parses_to_typed_ast() {
        let expr = parse_one(
            "SELECT DATEDIFF(day, date_utc, CURRENT_DATE()) AS recency FROM fact.some_daily_metrics",
            DialectType::Snowflake,
        )
        .expect("parse");

        match expr {
            Expression::Select(select) => match &select.expressions[0] {
                Expression::Alias(alias) => match &alias.this {
                    Expression::DateDiff(f) => {
                        assert_eq!(f.unit, Some(crate::expressions::IntervalUnit::Day));
                    }
                    other => panic!("expected DateDiff, got {other:?}"),
                },
                other => panic!("expected Alias, got {other:?}"),
            },
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn test_lineage_with_schema_snowflake_dateadd_date_part_issue_followup() {
        let expr = parse_one(
            "SELECT DATEADD(day, 1, date_utc) AS next_day FROM fact.some_daily_metrics",
            DialectType::Snowflake,
        )
        .expect("parse");

        let mut schema = MappingSchema::with_dialect(DialectType::Snowflake);
        schema
            .add_table(
                "fact.some_daily_metrics",
                &[("date_utc".to_string(), DataType::Date)],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "next_day",
            &expr,
            Some(&schema),
            Some(DialectType::Snowflake),
            false,
        )
        .expect("lineage_with_schema should not treat DATEADD date part as a column");

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "some_daily_metrics.date_utc"),
            "Expected some_daily_metrics.date_utc in downstream, got: {:?}",
            names
        );
        assert!(
            !names.iter().any(|n| n.ends_with(".day") || n == "day"),
            "Did not expect date part to appear as lineage column, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_with_schema_snowflake_date_part_identifier_issue_followup() {
        let expr = parse_one(
            "SELECT DATE_PART(day, date_utc) AS day_part FROM fact.some_daily_metrics",
            DialectType::Snowflake,
        )
        .expect("parse");

        let mut schema = MappingSchema::with_dialect(DialectType::Snowflake);
        schema
            .add_table(
                "fact.some_daily_metrics",
                &[("date_utc".to_string(), DataType::Date)],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "day_part",
            &expr,
            Some(&schema),
            Some(DialectType::Snowflake),
            false,
        )
        .expect("lineage_with_schema should not treat DATE_PART identifier as a column");

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "some_daily_metrics.date_utc"),
            "Expected some_daily_metrics.date_utc in downstream, got: {:?}",
            names
        );
        assert!(
            !names.iter().any(|n| n.ends_with(".day") || n == "day"),
            "Did not expect date part to appear as lineage column, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_with_schema_snowflake_date_part_string_literal_control() {
        let expr = parse_one(
            "SELECT DATE_PART('day', date_utc) AS day_part FROM fact.some_daily_metrics",
            DialectType::Snowflake,
        )
        .expect("parse");

        let mut schema = MappingSchema::with_dialect(DialectType::Snowflake);
        schema
            .add_table(
                "fact.some_daily_metrics",
                &[("date_utc".to_string(), DataType::Date)],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "day_part",
            &expr,
            Some(&schema),
            Some(DialectType::Snowflake),
            false,
        )
        .expect("quoted DATE_PART should continue to work");

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "some_daily_metrics.date_utc"),
            "Expected some_daily_metrics.date_utc in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_snowflake_dateadd_date_part_identifier_stays_generic_function() {
        let expr = parse_one(
            "SELECT DATEADD(day, 1, date_utc) AS next_day FROM fact.some_daily_metrics",
            DialectType::Snowflake,
        )
        .expect("parse");

        match expr {
            Expression::Select(select) => match &select.expressions[0] {
                Expression::Alias(alias) => match &alias.this {
                    Expression::Function(f) => {
                        assert_eq!(f.name.to_uppercase(), "DATEADD");
                        assert!(matches!(&f.args[0], Expression::Var(v) if v.this == "day"));
                    }
                    other => panic!("expected generic DATEADD function, got {other:?}"),
                },
                other => panic!("expected Alias, got {other:?}"),
            },
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn test_snowflake_date_part_identifier_stays_generic_function_with_var_arg() {
        let expr = parse_one(
            "SELECT DATE_PART(day, date_utc) AS day_part FROM fact.some_daily_metrics",
            DialectType::Snowflake,
        )
        .expect("parse");

        match expr {
            Expression::Select(select) => match &select.expressions[0] {
                Expression::Alias(alias) => match &alias.this {
                    Expression::Function(f) => {
                        assert_eq!(f.name.to_uppercase(), "DATE_PART");
                        assert!(matches!(&f.args[0], Expression::Var(v) if v.this == "day"));
                    }
                    other => panic!("expected generic DATE_PART function, got {other:?}"),
                },
                other => panic!("expected Alias, got {other:?}"),
            },
            other => panic!("expected Select, got {other:?}"),
        }
    }

    #[test]
    fn test_snowflake_date_part_string_literal_stays_generic_function() {
        let expr = parse_one(
            "SELECT DATE_PART('day', date_utc) AS day_part FROM fact.some_daily_metrics",
            DialectType::Snowflake,
        )
        .expect("parse");

        match expr {
            Expression::Select(select) => match &select.expressions[0] {
                Expression::Alias(alias) => match &alias.this {
                    Expression::Function(f) => {
                        assert_eq!(f.name.to_uppercase(), "DATE_PART");
                    }
                    other => panic!("expected generic DATE_PART function, got {other:?}"),
                },
                other => panic!("expected Alias, got {other:?}"),
            },
            other => panic!("expected Select, got {other:?}"),
        }
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
    fn test_lineage_alias_leaf_has_resolved_source_name() {
        let expr = parse("SELECT t1.col1 FROM table1 t1 JOIN table2 t2 ON t1.id = t2.id");
        let node = lineage("col1", &expr, None, false).unwrap();

        // Keep alias in the display lineage edge.
        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t1.col1"),
            "Expected aliased column edge t1.col1, got: {:?}",
            names
        );

        // Leaf should expose the resolved base table for consumers.
        let leaf = node
            .downstream
            .iter()
            .find(|n| n.name == "t1.col1")
            .expect("Expected t1.col1 leaf");
        assert_eq!(leaf.source_name, "table1");
        match &leaf.source {
            Expression::Table(table) => assert_eq!(table.name.name, "table1"),
            _ => panic!("Expected leaf source to be a table expression"),
        }
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

    // --- Tests for column lineage inside function calls (issue #18) ---

    fn print_node(node: &LineageNode, indent: usize) {
        let pad = "  ".repeat(indent);
        println!(
            "{pad}name={:?} source_name={:?}",
            node.name, node.source_name
        );
        for child in &node.downstream {
            print_node(child, indent + 1);
        }
    }

    #[test]
    fn test_issue18_repro() {
        // Exact scenario from the issue
        let query = "SELECT UPPER(name) as upper_name FROM users";
        println!("Query: {query}\n");

        let dialect = crate::dialects::Dialect::get(DialectType::BigQuery);
        let exprs = dialect.parse(query).unwrap();
        let expr = &exprs[0];

        let node = lineage("upper_name", expr, Some(DialectType::BigQuery), false).unwrap();
        println!("lineage(\"upper_name\"):");
        print_node(&node, 1);

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "users.name"),
            "Expected users.name in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_upper_function() {
        let expr = parse("SELECT UPPER(name) AS upper_name FROM users");
        let node = lineage("upper_name", &expr, None, false).unwrap();

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "users.name"),
            "Expected users.name in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_round_function() {
        let expr = parse("SELECT ROUND(price, 2) AS rounded FROM products");
        let node = lineage("rounded", &expr, None, false).unwrap();

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "products.price"),
            "Expected products.price in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_coalesce_function() {
        let expr = parse("SELECT COALESCE(a, b) AS val FROM t");
        let node = lineage("val", &expr, None, false).unwrap();

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t.a"),
            "Expected t.a in downstream, got: {:?}",
            names
        );
        assert!(
            names.iter().any(|n| n == "t.b"),
            "Expected t.b in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_count_function() {
        let expr = parse("SELECT COUNT(id) AS cnt FROM t");
        let node = lineage("cnt", &expr, None, false).unwrap();

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t.id"),
            "Expected t.id in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_sum_function() {
        let expr = parse("SELECT SUM(amount) AS total FROM t");
        let node = lineage("total", &expr, None, false).unwrap();

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t.amount"),
            "Expected t.amount in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_case_with_nested_functions() {
        let expr =
            parse("SELECT CASE WHEN x > 0 THEN UPPER(name) ELSE LOWER(name) END AS result FROM t");
        let node = lineage("result", &expr, None, false).unwrap();

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t.x"),
            "Expected t.x in downstream, got: {:?}",
            names
        );
        assert!(
            names.iter().any(|n| n == "t.name"),
            "Expected t.name in downstream, got: {:?}",
            names
        );
    }

    #[test]
    fn test_lineage_substring_function() {
        let expr = parse("SELECT SUBSTRING(name, 1, 3) AS short FROM t");
        let node = lineage("short", &expr, None, false).unwrap();

        let names = node.downstream_names();
        assert!(
            names.iter().any(|n| n == "t.name"),
            "Expected t.name in downstream, got: {:?}",
            names
        );
    }

    // --- CTE + SELECT * tests (ported from sqlglot test_lineage.py) ---

    #[test]
    fn test_lineage_cte_select_star() {
        // Ported from sqlglot: test_lineage_source_with_star
        // WITH y AS (SELECT * FROM x) SELECT a FROM y
        // After star expansion: SELECT y.a AS a FROM y
        let expr = parse("WITH y AS (SELECT * FROM x) SELECT a FROM y");
        let node = lineage("a", &expr, None, false).unwrap();

        assert_eq!(node.name, "a");
        // Should successfully resolve column 'a' through the CTE
        // (previously failed with "Cannot find column 'a' in query")
        assert!(
            !node.downstream.is_empty(),
            "Expected downstream nodes tracing through CTE, got none"
        );
    }

    #[test]
    fn test_lineage_cte_select_star_renamed_column() {
        // dbt standard pattern: CTE with column rename + outer SELECT *
        // This is the primary use case for dbt projects (jaffle-shop etc.)
        let expr =
            parse("WITH renamed AS (SELECT id AS customer_id FROM source) SELECT * FROM renamed");
        let node = lineage("customer_id", &expr, None, false).unwrap();

        assert_eq!(node.name, "customer_id");
        // Should trace customer_id → renamed CTE → source.id
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.len() >= 2,
            "Expected at least 2 nodes (customer_id → source), got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_cte_select_star_multiple_columns() {
        // CTE exposes multiple columns, outer SELECT * should resolve each
        let expr = parse("WITH cte AS (SELECT a, b, c FROM t) SELECT * FROM cte");

        for col in &["a", "b", "c"] {
            let node = lineage(col, &expr, None, false).unwrap();
            assert_eq!(node.name, *col);
            // Verify lineage resolves without error (star expanded to explicit columns)
            let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
            assert!(
                all_names.len() >= 2,
                "Expected at least 2 nodes for column {}, got: {:?}",
                col,
                all_names
            );
        }
    }

    #[test]
    fn test_lineage_nested_cte_select_star() {
        // Nested CTE star expansion: cte2 references cte1 via SELECT *
        let expr = parse(
            "WITH cte1 AS (SELECT a FROM t), \
             cte2 AS (SELECT * FROM cte1) \
             SELECT * FROM cte2",
        );
        let node = lineage("a", &expr, None, false).unwrap();

        assert_eq!(node.name, "a");
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.len() >= 3,
            "Expected at least 3 nodes (a → cte2 → cte1 → t.a), got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_three_level_nested_cte_star() {
        // Three-level nested CTE: cte3 → cte2 → cte1 → t
        let expr = parse(
            "WITH cte1 AS (SELECT x FROM t), \
             cte2 AS (SELECT * FROM cte1), \
             cte3 AS (SELECT * FROM cte2) \
             SELECT * FROM cte3",
        );
        let node = lineage("x", &expr, None, false).unwrap();

        assert_eq!(node.name, "x");
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.len() >= 4,
            "Expected at least 4 nodes through 3-level CTE chain, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_cte_union_star() {
        // CTE with UNION body, outer SELECT * should resolve from left branch
        let expr = parse(
            "WITH cte AS (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2) \
             SELECT * FROM cte",
        );
        let node = lineage("a", &expr, None, false).unwrap();

        assert_eq!(node.name, "a");
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.len() >= 2,
            "Expected at least 2 nodes for CTE union star, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_cte_star_unknown_table() {
        // When CTE references an unknown table, star expansion is skipped gracefully
        // and lineage falls back to normal resolution (which may fail)
        let expr = parse(
            "WITH cte AS (SELECT * FROM unknown_table) \
             SELECT * FROM cte",
        );
        // This should not panic — it may succeed or fail depending on resolution,
        // but should not crash
        let _result = lineage("x", &expr, None, false);
    }

    #[test]
    fn test_lineage_cte_explicit_columns() {
        // CTE with explicit column list: cte(x, y) AS (SELECT a, b FROM t)
        let expr = parse(
            "WITH cte(x, y) AS (SELECT a, b FROM t) \
             SELECT * FROM cte",
        );
        let node = lineage("x", &expr, None, false).unwrap();
        assert_eq!(node.name, "x");
    }

    #[test]
    fn test_lineage_cte_qualified_star() {
        // Qualified star: SELECT cte.* FROM cte
        let expr = parse(
            "WITH cte AS (SELECT a, b FROM t) \
             SELECT cte.* FROM cte",
        );
        for col in &["a", "b"] {
            let node = lineage(col, &expr, None, false).unwrap();
            assert_eq!(node.name, *col);
            let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
            assert!(
                all_names.len() >= 2,
                "Expected at least 2 nodes for qualified star column {}, got: {:?}",
                col,
                all_names
            );
        }
    }

    #[test]
    fn test_lineage_subquery_select_star() {
        // Ported from sqlglot: test_select_star
        // SELECT x FROM (SELECT * FROM table_a)
        let expr = parse("SELECT x FROM (SELECT * FROM table_a)");
        let node = lineage("x", &expr, None, false).unwrap();

        assert_eq!(node.name, "x");
        assert!(
            !node.downstream.is_empty(),
            "Expected downstream nodes for subquery with SELECT *, got none"
        );
    }

    #[test]
    fn test_lineage_cte_star_with_schema_external_table() {
        // CTE references an external table via SELECT * — schema enables expansion
        let sql = r#"WITH orders AS (SELECT * FROM stg_orders)
SELECT * FROM orders"#;
        let expr = parse(sql);

        let mut schema = MappingSchema::new();
        let cols = vec![
            ("order_id".to_string(), DataType::Unknown),
            ("customer_id".to_string(), DataType::Unknown),
            ("amount".to_string(), DataType::Unknown),
        ];
        schema.add_table("stg_orders", &cols, None).unwrap();

        let node = lineage_with_schema(
            "order_id",
            &expr,
            Some(&schema as &dyn Schema),
            None,
            false,
        )
        .unwrap();
        assert_eq!(node.name, "order_id");
    }

    #[test]
    fn test_lineage_cte_star_with_schema_three_part_name() {
        // CTE references an external table with fully-qualified 3-part name
        let sql = r#"WITH orders AS (SELECT * FROM "db"."schema"."stg_orders")
SELECT * FROM orders"#;
        let expr = parse(sql);

        let mut schema = MappingSchema::new();
        let cols = vec![
            ("order_id".to_string(), DataType::Unknown),
            ("customer_id".to_string(), DataType::Unknown),
        ];
        schema.add_table("db.schema.stg_orders", &cols, None).unwrap();

        let node = lineage_with_schema(
            "customer_id",
            &expr,
            Some(&schema as &dyn Schema),
            None,
            false,
        )
        .unwrap();
        assert_eq!(node.name, "customer_id");
    }

    #[test]
    fn test_lineage_cte_star_with_schema_nested() {
        // Nested CTEs: outer CTE references inner CTE with SELECT *,
        // inner CTE references external table with SELECT *
        let sql = r#"WITH
            raw AS (SELECT * FROM external_table),
            enriched AS (SELECT * FROM raw)
        SELECT * FROM enriched"#;
        let expr = parse(sql);

        let mut schema = MappingSchema::new();
        let cols = vec![
            ("id".to_string(), DataType::Unknown),
            ("name".to_string(), DataType::Unknown),
        ];
        schema.add_table("external_table", &cols, None).unwrap();

        let node = lineage_with_schema(
            "name",
            &expr,
            Some(&schema as &dyn Schema),
            None,
            false,
        )
        .unwrap();
        assert_eq!(node.name, "name");
    }

    #[test]
    fn test_lineage_cte_qualified_star_with_schema() {
        // CTE uses qualified star (orders.*) from a CTE whose columns
        // come from an external table via SELECT *
        let sql = r#"WITH
            orders AS (SELECT * FROM stg_orders),
            enriched AS (
                SELECT orders.*, 'extra' AS extra
                FROM orders
            )
        SELECT * FROM enriched"#;
        let expr = parse(sql);

        let mut schema = MappingSchema::new();
        let cols = vec![
            ("order_id".to_string(), DataType::Unknown),
            ("total".to_string(), DataType::Unknown),
        ];
        schema.add_table("stg_orders", &cols, None).unwrap();

        let node = lineage_with_schema(
            "order_id",
            &expr,
            Some(&schema as &dyn Schema),
            None,
            false,
        )
        .unwrap();
        assert_eq!(node.name, "order_id");

        // Also verify the extra column works
        let extra = lineage_with_schema(
            "extra",
            &expr,
            Some(&schema as &dyn Schema),
            None,
            false,
        )
        .unwrap();
        assert_eq!(extra.name, "extra");
    }

    #[test]
    fn test_lineage_cte_star_without_schema_still_works() {
        // Without schema, CTE-to-CTE star expansion still works
        let sql = r#"WITH
            cte1 AS (SELECT id, name FROM raw_table),
            cte2 AS (SELECT * FROM cte1)
        SELECT * FROM cte2"#;
        let expr = parse(sql);

        // No schema — should still resolve through CTE chain
        let node = lineage("id", &expr, None, false).unwrap();
        assert_eq!(node.name, "id");
    }

    #[test]
    fn test_lineage_nested_cte_star_with_join_and_schema() {
        // Reproduces dbt pattern: CTE chain with qualified star and JOIN
        // base_orders -> with_payments (JOIN) -> final -> outer SELECT
        let sql = r#"WITH
base_orders AS (
    SELECT * FROM stg_orders
),
with_payments AS (
    SELECT
        base_orders.*,
        p.amount
    FROM base_orders
    LEFT JOIN stg_payments p ON base_orders.order_id = p.order_id
),
final_cte AS (
    SELECT * FROM with_payments
)
SELECT * FROM final_cte"#;
        let expr = parse(sql);

        let mut schema = MappingSchema::new();
        let order_cols = vec![
            ("order_id".to_string(), crate::expressions::DataType::Unknown),
            ("customer_id".to_string(), crate::expressions::DataType::Unknown),
            ("status".to_string(), crate::expressions::DataType::Unknown),
        ];
        let pay_cols = vec![
            ("payment_id".to_string(), crate::expressions::DataType::Unknown),
            ("order_id".to_string(), crate::expressions::DataType::Unknown),
            ("amount".to_string(), crate::expressions::DataType::Unknown),
        ];
        schema.add_table("stg_orders", &order_cols, None).unwrap();
        schema.add_table("stg_payments", &pay_cols, None).unwrap();

        // order_id should trace back to stg_orders
        let node = lineage_with_schema(
            "order_id", &expr, Some(&schema as &dyn Schema), None, false,
        ).unwrap();
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();

        // The leaf should be "stg_orders.order_id" (not just "order_id")
        let has_table_qualified = all_names.iter().any(|n| n.contains('.') && n.contains("order_id"));
        assert!(has_table_qualified,
            "Expected table-qualified leaf like 'stg_orders.order_id', got: {:?}", all_names);

        // amount should trace back to stg_payments
        let node = lineage_with_schema(
            "amount", &expr, Some(&schema as &dyn Schema), None, false,
        ).unwrap();
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();

        let has_table_qualified = all_names.iter().any(|n| n.contains('.') && n.contains("amount"));
        assert!(has_table_qualified,
            "Expected table-qualified leaf like 'stg_payments.amount', got: {:?}", all_names);
    }

    #[test]
    fn test_lineage_cte_alias_resolution() {
        // FROM cte_name AS alias pattern: alias should resolve through CTE to source table
        let sql = r#"WITH import_stg_items AS (
    SELECT item_id, name, status FROM stg_items
)
SELECT base.item_id, base.status
FROM import_stg_items AS base"#;
        let expr = parse(sql);

        let node = lineage("item_id", &expr, None, false).unwrap();
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        // Should trace through alias "base" → CTE "import_stg_items" → "stg_items.item_id"
        assert!(
            all_names.iter().any(|n| n == "stg_items.item_id"),
            "Expected leaf 'stg_items.item_id', got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_cte_alias_with_schema_and_star() {
        // CTE alias + SELECT * expansion: FROM cte AS alias with star in CTE body
        let sql = r#"WITH import_stg AS (
    SELECT * FROM stg_items
)
SELECT base.item_id, base.status
FROM import_stg AS base"#;
        let expr = parse(sql);

        let mut schema = MappingSchema::new();
        schema
            .add_table(
                "stg_items",
                &[
                    ("item_id".to_string(), DataType::Unknown),
                    ("name".to_string(), DataType::Unknown),
                    ("status".to_string(), DataType::Unknown),
                ],
                None,
            )
            .unwrap();

        let node = lineage_with_schema(
            "item_id",
            &expr,
            Some(&schema as &dyn Schema),
            None,
            false,
        )
        .unwrap();
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|n| n == "stg_items.item_id"),
            "Expected leaf 'stg_items.item_id', got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_cte_alias_with_join() {
        // Multiple CTE aliases in a JOIN: each should resolve independently
        let sql = r#"WITH
    import_users AS (SELECT id, name FROM users),
    import_orders AS (SELECT id, user_id, amount FROM orders)
SELECT u.name, o.amount
FROM import_users AS u
LEFT JOIN import_orders AS o ON u.id = o.user_id"#;
        let expr = parse(sql);

        let node = lineage("name", &expr, None, false).unwrap();
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|n| n == "users.name"),
            "Expected leaf 'users.name', got: {:?}",
            all_names
        );

        let node = lineage("amount", &expr, None, false).unwrap();
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|n| n == "orders.amount"),
            "Expected leaf 'orders.amount', got: {:?}",
            all_names
        );
    }

    // -----------------------------------------------------------------------
    // Quoted CTE name tests — verifying SQL identifier case semantics
    // -----------------------------------------------------------------------

    #[test]
    fn test_lineage_unquoted_cte_case_insensitive() {
        // Unquoted CTE names are case-insensitive (both normalized to lowercase).
        // MyCte and MYCTE should match.
        let expr = parse(
            "WITH MyCte AS (SELECT id AS col FROM source) SELECT * FROM MYCTE",
        );
        let node = lineage("col", &expr, None, false).unwrap();
        assert_eq!(node.name, "col");
        assert!(
            !node.downstream.is_empty(),
            "Unquoted CTE should resolve case-insensitively"
        );
    }

    #[test]
    fn test_lineage_quoted_cte_case_preserved() {
        // Quoted CTE name preserves case. "MyCte" referenced as "MyCte" should match.
        let expr = parse(
            r#"WITH "MyCte" AS (SELECT id AS col FROM source) SELECT * FROM "MyCte""#,
        );
        let node = lineage("col", &expr, None, false).unwrap();
        assert_eq!(node.name, "col");
        assert!(
            !node.downstream.is_empty(),
            "Quoted CTE with matching case should resolve"
        );
    }

    #[test]
    fn test_lineage_quoted_cte_case_mismatch_no_expansion() {
        // Quoted CTE "MyCte" referenced as "mycte" — case mismatch.
        // sqlglot treats this as a table reference, not a CTE match.
        // Star expansion should NOT resolve through the CTE.
        let expr = parse(
            r#"WITH "MyCte" AS (SELECT id AS col FROM source) SELECT * FROM "mycte""#,
        );
        // lineage("col", ...) should fail because "mycte" is treated as an external
        // table (not matching CTE "MyCte"), and SELECT * cannot be expanded.
        let result = lineage("col", &expr, None, false);
        assert!(
            result.is_err(),
            "Quoted CTE with case mismatch should not expand star: {:?}",
            result
        );
    }

    #[test]
    fn test_lineage_mixed_quoted_unquoted_cte() {
        // Mix of unquoted and quoted CTEs in a nested chain.
        let expr = parse(
            r#"WITH unquoted AS (SELECT 1 AS a FROM t), "Quoted" AS (SELECT a FROM unquoted) SELECT * FROM "Quoted""#,
        );
        let node = lineage("a", &expr, None, false).unwrap();
        assert_eq!(node.name, "a");
        assert!(
            !node.downstream.is_empty(),
            "Mixed quoted/unquoted CTE chain should resolve"
        );
    }
}
