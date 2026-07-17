//! Column Lineage Tracking
//!
//! This module provides functionality to track column lineage through SQL queries,
//! building a graph of how columns flow from source tables to the result set.
//! Supports UNION/INTERSECT/EXCEPT, CTEs, derived tables, subqueries, and star expansion.
//!

use crate::dialects::DialectType;
use crate::expressions::{Expression, Identifier, JoinKind, NamedWindow, Select};
#[cfg(feature = "generate")]
use crate::generator::Generator;
use crate::optimizer::annotate_types::annotate_types;
use crate::optimizer::qualify_columns::{qualify_columns, QualifyColumnsOptions};
use crate::schema::{normalize_name, Schema};
use crate::scope::{
    build_scope, find_all_in_scope, Scope, ScopeType, SourceInfo as ScopeSourceInfo, SourceKind,
};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::rc::Rc;

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
    /// Semantic source kind for downstream consumers.
    pub source_kind: SourceKind,
    /// User-written source alias when different from canonical source name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source_alias: Option<String>,
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
            source_kind: SourceKind::Unknown,
            source_alias: None,
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

fn source_kind_for_scope_context(
    scope: &Scope,
    source_name: &str,
    reference_node_name: &str,
) -> SourceKind {
    if source_name.is_empty() && reference_node_name.is_empty() {
        return SourceKind::Root;
    }
    if let Some(source_info) = scope.sources.get(source_name) {
        return source_info.kind;
    }
    if scope.cte_sources.contains_key(source_name) {
        return SourceKind::Cte;
    }
    match scope.scope_type {
        ScopeType::Cte => SourceKind::Cte,
        ScopeType::DerivedTable => SourceKind::DerivedTable,
        ScopeType::Udtf => SourceKind::Virtual,
        _ => SourceKind::Unknown,
    }
}

fn apply_scope_context(
    node: &mut LineageNode,
    scope: &Scope,
    source_name: &str,
    reference_node_name: &str,
) {
    node.source_name = source_name.to_string();
    node.reference_node_name = reference_node_name.to_string();
    node.source_kind = source_kind_for_scope_context(scope, source_name, reference_node_name);
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
    let mut owned = lineage_normalized_expression(sql);
    // Fast path: skip clone when there are no CTEs to expand
    if has_lineage_with_clause(&owned) {
        expand_cte_stars(&mut owned, None);
    }
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
    let normalized_expression = lineage_normalized_expression(sql);
    let mut qualified_expression = if let Some(schema) = schema {
        let options = if let Some(dialect_type) = dialect.or_else(|| schema.dialect()) {
            QualifyColumnsOptions::new()
                .with_dialect(dialect_type)
                .with_allow_partial(true)
        } else {
            QualifyColumnsOptions::new().with_allow_partial(true)
        };

        qualify_columns(normalized_expression.clone(), schema, &options).map_err(|e| {
            Error::internal(format!("Lineage qualification failed with schema: {}", e))
        })?
    } else {
        normalized_expression
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

#[cfg(feature = "generate")]
pub(crate) fn lineage_by_index_from_expression(
    column_index: usize,
    sql: &Expression,
    dialect: Option<DialectType>,
    trim_selects: bool,
) -> Result<LineageNode> {
    let normalized = lineage_normalized_expression(sql);
    let scope = build_scope(&normalized);
    to_node(
        ColumnRef::Index(column_index),
        &scope,
        dialect,
        "",
        "",
        "",
        trim_selects,
    )
}

fn lineage_normalized_expression(sql: &Expression) -> Expression {
    match sql {
        Expression::Prepare(prepare) => lineage_normalized_expression(&prepare.statement),
        Expression::CreateTable(create) => create
            .as_select
            .as_ref()
            .map(|query| attach_with_to_query(query.clone(), create.with_cte.clone()))
            .unwrap_or_else(|| sql.clone()),
        Expression::CreateView(create) => lineage_normalized_expression(&create.query),
        Expression::Insert(insert) => insert
            .query
            .as_ref()
            .map(|query| attach_with_to_query(query.clone(), insert.with.clone()))
            .unwrap_or_else(|| sql.clone()),
        _ => sql.clone(),
    }
}

fn attach_with_to_query(
    mut query: Expression,
    with: Option<crate::expressions::With>,
) -> Expression {
    if let Some(with) = with {
        attach_with_to_query_mut(&mut query, with);
    }
    query
}

fn attach_with_to_query_mut(query: &mut Expression, with: crate::expressions::With) {
    match query {
        Expression::Select(select) => {
            if select.with.is_none() {
                select.with = Some(with);
            }
        }
        Expression::Union(union) => {
            if union.with.is_none() {
                union.with = Some(with);
            }
        }
        Expression::Intersect(intersect) => {
            if intersect.with.is_none() {
                intersect.with = Some(with);
            }
        }
        Expression::Except(except) => {
            if except.with.is_none() {
                except.with = Some(with);
            }
        }
        Expression::Paren(paren) => attach_with_to_query_mut(&mut paren.this, with),
        _ => {}
    }
}

fn has_lineage_with_clause(expr: &Expression) -> bool {
    match expr {
        Expression::Select(select) => select.with.is_some(),
        Expression::Union(union) => {
            union.with.is_some()
                || has_lineage_with_clause(&union.left)
                || has_lineage_with_clause(&union.right)
        }
        Expression::Intersect(intersect) => {
            intersect.with.is_some()
                || has_lineage_with_clause(&intersect.left)
                || has_lineage_with_clause(&intersect.right)
        }
        Expression::Except(except) => {
            except.with.is_some()
                || has_lineage_with_clause(&except.left)
                || has_lineage_with_clause(&except.right)
        }
        Expression::Paren(paren) => has_lineage_with_clause(&paren.this),
        _ => false,
    }
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
    if let Expression::Prepare(prepare) = expr {
        expand_cte_stars(&mut prepare.statement, schema);
        return;
    }

    let select = match expr {
        Expression::Select(s) => s,
        _ => return,
    };

    let with = match &mut select.with {
        Some(w) => w,
        None => return,
    };

    let mut resolved_cte_columns: HashMap<String, Vec<String>> = HashMap::new();

    for cte in &mut with.ctes {
        let cte_name = normalize_cte_name(&cte.alias);

        // If CTE has explicit column list (e.g., cte(a, b) AS (...)), use that
        if !cte.columns.is_empty() {
            let cols: Vec<String> = cte.columns.iter().map(|c| c.name.clone()).collect();
            resolved_cte_columns.insert(cte_name, cols);
            continue;
        }

        // Skip recursive CTEs (self-referencing) — their column resolution is complex.
        // A CTE is recursive if the WITH block is marked recursive AND the CTE body
        // references itself. We detect this conservatively: if the CTE name appears as
        // a source in its own body, skip it. Non-recursive CTEs in a recursive WITH
        // block are still expanded.
        if with.recursive {
            let is_self_referencing =
                if let Some(body_select) = get_leftmost_select_mut(&mut cte.this) {
                    let body_sources = get_select_sources(body_select);
                    body_sources.iter().any(|s| s.normalized == cte_name)
                } else {
                    false
                };
            if is_self_referencing {
                continue;
            }
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
    let mut current = expr;
    for _ in 0..MAX_LINEAGE_DEPTH {
        match current {
            Expression::Select(s) => return Some(s),
            Expression::Union(u) => current = &mut u.left,
            Expression::Intersect(i) => current = &mut i.left,
            Expression::Except(e) => current = &mut e.left,
            Expression::Paren(p) => current = &mut p.this,
            _ => return None,
        }
    }
    None
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

    let sources = get_select_sources(select);
    let mut new_expressions = Vec::new();
    let mut result_columns = Vec::new();

    for expr in &select.expressions {
        match expr {
            Expression::Star(star) => {
                let qual = star.table.as_ref();
                if let Some(expanded) =
                    expand_star_from_sources(qual, &sources, resolved_ctes, schema)
                {
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
                if let Some(expanded) =
                    expand_star_from_sources(qual, &sources, resolved_ctes, schema)
                {
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
fn expand_star_from_sources(
    qualifier: Option<&Identifier>,
    sources: &[SourceInfo],
    resolved_ctes: &HashMap<String, Vec<String>>,
    schema: Option<&dyn Schema>,
) -> Option<Vec<(String, String)>> {
    let mut expanded = Vec::new();

    if let Some(qual) = qualifier {
        // Qualified star: table.*
        let qual_normalized = normalize_cte_name(qual);
        for src in sources {
            if src.normalized == qual_normalized || src.alias.to_lowercase() == qual_normalized {
                // Try CTE first
                if let Some(cols) = resolved_ctes.get(&src.normalized) {
                    expanded.extend(cols.iter().map(|c| (src.alias.clone(), c.clone())));
                    return Some(expanded);
                }
                // Fall back to schema
                if let Some(cols) = lookup_schema_columns(schema, &src.fq_name) {
                    expanded.extend(cols.into_iter().map(|c| (src.alias.clone(), c)));
                    return Some(expanded);
                }
            }
        }
        None
    } else {
        // Unqualified star: expand all sources.
        // Intentionally conservative: if any source can't be resolved, the entire
        // expansion is aborted. Partial expansion would produce an incomplete column
        // list, causing downstream lineage resolution to silently omit columns.
        // This matches sqlglot's behavior (raises SqlglotError when schema is missing).
        let mut any_expanded = false;
        for src in sources {
            if let Some(cols) = resolved_ctes.get(&src.normalized) {
                expanded.extend(cols.iter().map(|c| (src.alias.clone(), c.clone())));
                any_expanded = true;
            } else if let Some(cols) = lookup_schema_columns(schema, &src.fq_name) {
                expanded.extend(cols.into_iter().map(|c| (src.alias.clone(), c)));
                any_expanded = true;
            } else {
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
fn lookup_schema_columns(schema: Option<&dyn Schema>, fq_name: &str) -> Option<Vec<String>> {
    let schema = schema?;
    if fq_name.is_empty() {
        return None;
    }
    schema
        .column_names(fq_name)
        .ok()
        .filter(|cols| !cols.is_empty() && !cols.contains(&"*".to_string()))
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

/// Source info extracted from a SELECT's FROM/JOIN clauses in a single pass.
struct SourceInfo {
    alias: String,
    /// Whether this source was introduced through a quoted identifier.
    ///
    /// The schema-less star passthrough heuristic must stay conservative for
    /// quoted sources because unresolved quoted table names can be distinct
    /// from similarly named CTEs that older scope paths still compare
    /// case-insensitively.
    quoted: bool,
    /// Normalized name for CTE lookup: unquoted → lowercased, quoted → as-is.
    normalized: String,
    /// Fully-qualified table name for schema lookup (e.g., "db.schema.table").
    fq_name: String,
}

/// Extract source info (alias, normalized CTE name, fully-qualified name) from a
/// SELECT's FROM and JOIN clauses in a single pass.
fn get_select_sources(select: &Select) -> Vec<SourceInfo> {
    let mut sources = Vec::new();

    fn extract_source(expr: &Expression) -> Option<SourceInfo> {
        fn virtual_source_info(alias: &Identifier) -> SourceInfo {
            SourceInfo {
                alias: alias.name.clone(),
                quoted: alias.quoted,
                normalized: normalize_cte_name(alias),
                fq_name: alias.name.clone(),
            }
        }

        fn named_virtual_source_info(alias: &str) -> SourceInfo {
            SourceInfo {
                alias: alias.to_string(),
                quoted: false,
                normalized: alias.to_lowercase(),
                fq_name: alias.to_string(),
            }
        }

        match expr {
            Expression::Table(t) => {
                let normalized = normalize_cte_name(&t.name);
                let alias = t
                    .alias
                    .as_ref()
                    .map(|a| a.name.clone())
                    .unwrap_or_else(|| t.name.name.clone());
                let mut parts = Vec::new();
                if let Some(catalog) = &t.catalog {
                    parts.push(catalog.name.clone());
                }
                if let Some(schema) = &t.schema {
                    parts.push(schema.name.clone());
                }
                parts.push(t.name.name.clone());
                let fq_name = parts.join(".");
                Some(SourceInfo {
                    alias,
                    quoted: t.name.quoted,
                    normalized,
                    fq_name,
                })
            }
            Expression::Subquery(s) => {
                let alias_identifier = s.alias.as_ref()?;
                let alias = alias_identifier.name.clone();
                let normalized = alias.to_lowercase();
                let fq_name = alias.clone();
                Some(SourceInfo {
                    alias,
                    quoted: alias_identifier.quoted,
                    normalized,
                    fq_name,
                })
            }
            Expression::Unnest(u) => u.alias.as_ref().map(virtual_source_info),
            Expression::Alias(a) if matches!(&a.this, Expression::Unnest(_)) => {
                Some(virtual_source_info(&a.alias))
            }
            Expression::Alias(a) if is_query_like_relation(&a.this) => {
                Some(virtual_source_info(&a.alias))
            }
            Expression::Lateral(lateral) => lateral.alias.as_deref().map(named_virtual_source_info),
            Expression::LateralView(lateral_view) => lateral_view
                .table_alias
                .as_ref()
                .or_else(|| lateral_view.column_aliases.first())
                .map(virtual_source_info),
            Expression::Pivot(pivot) => {
                let alias = pivot_lineage_source_name(
                    &pivot.this,
                    pivot.alias.as_ref().map(|alias| alias.name.as_str()),
                );
                Some(SourceInfo {
                    alias: alias.clone(),
                    quoted: false,
                    normalized: alias.to_lowercase(),
                    fq_name: alias,
                })
            }
            Expression::Unpivot(unpivot) => {
                let alias = pivot_lineage_source_name(
                    &unpivot.this,
                    unpivot.alias.as_ref().map(|alias| alias.name.as_str()),
                );
                Some(SourceInfo {
                    alias: alias.clone(),
                    quoted: false,
                    normalized: alias.to_lowercase(),
                    fq_name: alias,
                })
            }
            Expression::Paren(p) => extract_source(&p.this),
            _ => None,
        }
    }

    if let Some(from) = &select.from {
        for expr in &from.expressions {
            if let Some(info) = extract_source(expr) {
                sources.push(info);
            }
        }
    }
    for join in &select.joins {
        if is_semi_or_anti_join_kind(join.kind) {
            continue;
        }
        if let Some(info) = extract_source(&join.this) {
            sources.push(info);
        }
    }
    for lateral_view in &select.lateral_views {
        if let Some(info) = extract_source(&Expression::LateralView(Box::new(lateral_view.clone())))
        {
            sources.push(info);
        }
    }
    sources
}

fn pivot_lineage_source_name(source: &Expression, explicit_alias: Option<&str>) -> String {
    if let Some(alias) = explicit_alias {
        return alias.to_string();
    }

    match source {
        Expression::Table(table) => table
            .alias
            .as_ref()
            .map(|alias| alias.name.clone())
            .unwrap_or_else(|| table.name.name.clone()),
        Expression::Subquery(subquery) => subquery
            .alias
            .as_ref()
            .map(|alias| alias.name.clone())
            .unwrap_or_else(|| "_0".to_string()),
        Expression::Paren(paren) => pivot_lineage_source_name(&paren.this, explicit_alias),
        _ => "_0".to_string(),
    }
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
    ancestor_cte_scopes: &[Rc<Scope>],
    depth: usize,
) -> Result<LineageNode> {
    if depth > MAX_LINEAGE_DEPTH {
        return Err(Error::internal(format!(
            "lineage recursion depth exceeded (>{MAX_LINEAGE_DEPTH}) — possible circular CTE reference for scope '{scope_name}'"
        )));
    }
    let scope_expr = &scope.expression;

    // Build combined CTE scopes: current scope's cte_scopes + ancestors
    let mut all_cte_scopes: Vec<Rc<Scope>> = scope
        .cte_scopes
        .iter()
        .map(|s| Rc::new(s.clone()))
        .collect();
    for s in ancestor_cte_scopes {
        all_cte_scopes.push(Rc::clone(s));
    }
    let descendant_cte_scopes = descendant_cte_scope_clones(&all_cte_scopes, scope);

    // 0. Unwrap CTE scope — CTE scope expressions are Expression::Cte(...)
    //    but we need the inner query (SELECT/UNION) for column lookup.
    let effective_expr = effective_scope_expression(scope_expr);

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
                &descendant_cte_scopes,
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
            &descendant_cte_scopes,
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
    apply_scope_context(&mut node, scope, source_name, reference_node_name);

    // 5. Star handling — add downstream for each source
    if let Expression::Star(star) = &select_expr {
        let star_table = star
            .table
            .as_ref()
            .map(|identifier| identifier.name.as_str());
        for (name, source_info) in &scope.sources {
            if let Some(star_table) = star_table {
                let table_matches = name.eq_ignore_ascii_case(star_table)
                    || source_info
                        .alias
                        .as_deref()
                        .is_some_and(|alias| alias.eq_ignore_ascii_case(star_table))
                    || matches!(
                        &source_info.expression,
                        Expression::Table(table_ref)
                            if table_name_from_table_ref(table_ref).eq_ignore_ascii_case(star_table)
                    );
                if !table_matches {
                    continue;
                }
            }

            let mut child = LineageNode::new(
                format!("{}.*", name),
                Expression::Star(crate::expressions::Star {
                    table: star.table.clone(),
                    except: None,
                    replace: None,
                    rename: None,
                    trailing_comments: vec![],
                    span: None,
                }),
                source_info.expression.clone(),
            );
            apply_source_info_context(&mut child, name, source_info);
            node.downstream.push(child);
        }
        return Ok(node);
    }

    // 6. Subqueries in select — trace through scalar subqueries
    for query in query_expressions_in_scope(&select_expr) {
        for sq_scope in &scope.subquery_scopes {
            if sq_scope.expression == *query {
                if let Ok(child) = to_node_inner(
                    ColumnRef::Index(0),
                    sq_scope,
                    dialect,
                    &column_name,
                    "",
                    "",
                    trim_selects,
                    &descendant_cte_scopes,
                    depth + 1,
                ) {
                    node.downstream.push(child);
                }
                break;
            }
        }
    }

    // 7. Column references — trace each column to its source
    let col_refs = find_column_refs_in_expr_with_select(&select_expr, effective_expr, dialect);
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
            if let Some(alias_expr) =
                find_prior_select_alias_expr(effective_expr, &select_expr, col_name, dialect)
            {
                for alias_ref in
                    find_column_refs_in_expr_with_select(&alias_expr, effective_expr, dialect)
                {
                    if let Some(ref table_id) = alias_ref.table {
                        resolve_qualified_column(
                            &mut node,
                            scope,
                            dialect,
                            &table_id.name,
                            &alias_ref.column,
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
                            &alias_ref.column,
                            &column_name,
                            trim_selects,
                            &all_cte_scopes,
                            depth,
                        );
                    }
                }
                continue;
            }

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

fn descendant_cte_scope_clones(
    all_cte_scopes: &[Rc<Scope>],
    current_scope: &Scope,
) -> Vec<Rc<Scope>> {
    all_cte_scopes
        .iter()
        .filter(|scope| !is_same_cte_scope(scope, current_scope))
        .map(Rc::clone)
        .collect()
}

fn is_same_cte_scope(a: &Scope, b: &Scope) -> bool {
    match (&a.expression, &b.expression) {
        (Expression::Cte(x), Expression::Cte(y)) => {
            x.alias.name == y.alias.name && a.expression == b.expression
        }
        _ => a.expression == b.expression,
    }
}

fn effective_scope_expression(expr: &Expression) -> &Expression {
    match expr {
        Expression::Cte(cte) => effective_scope_expression(&cte.this),
        Expression::Subquery(subquery) => effective_scope_expression(&subquery.this),
        Expression::Paren(paren) => effective_scope_expression(&paren.this),
        other => other,
    }
}

fn query_expressions_in_scope(expr: &Expression) -> Vec<&Expression> {
    let mut queries = Vec::new();
    let mut seen = HashSet::new();

    for node in find_all_in_scope(
        expr,
        |node| {
            matches!(
                node,
                Expression::Subquery(subquery) if subquery.alias.is_none()
            ) || matches!(
                node,
                Expression::Exists(_) | Expression::In(_) | Expression::Any(_) | Expression::All(_)
            )
        },
        false,
    ) {
        let query = match node {
            Expression::Subquery(subquery) if subquery.alias.is_none() => Some(&subquery.this),
            Expression::Exists(exists) => Some(&exists.this),
            Expression::In(in_expr) => in_expr.query.as_ref(),
            Expression::Any(quantified) | Expression::All(quantified) => Some(&quantified.subquery),
            _ => None,
        };

        if let Some(query) = query {
            let key = query as *const Expression as usize;
            if seen.insert(key) {
                queries.push(query);
            }
        }
    }

    queries
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
    ancestor_cte_scopes: &[Rc<Scope>],
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
    apply_scope_context(&mut node, scope, source_name, reference_node_name);

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
    all_cte_scopes: &[Rc<Scope>],
    depth: usize,
) {
    // Resolve CTE alias: if `table` is a FROM alias for a CTE (e.g., `FROM my_cte AS t`),
    // resolve it to the actual CTE name so the CTE scope lookup succeeds.
    let resolved_cte_name = resolve_cte_alias(scope, table);
    let effective_table = resolved_cte_name.as_deref().unwrap_or(table);

    if let Some(source_info) = scope
        .sources
        .get(table)
        .or_else(|| scope.sources.get(effective_table))
    {
        match &source_info.expression {
            Expression::Pivot(pivot) => {
                if attach_pivot_dependencies(
                    node,
                    scope,
                    dialect,
                    pivot,
                    col_name,
                    trim_selects,
                    all_cte_scopes,
                    depth,
                ) {
                    return;
                }
            }
            Expression::Unpivot(unpivot) => {
                if attach_unpivot_dependencies(
                    node,
                    scope,
                    dialect,
                    unpivot,
                    col_name,
                    trim_selects,
                    all_cte_scopes,
                    depth,
                ) {
                    return;
                }
            }
            _ => {}
        }
    }

    // Check if table is a CTE reference — check both the current scope's cte_sources
    // and ancestor CTE scopes (for sibling CTEs in parent WITH clauses).
    let is_cte = scope.cte_sources.contains_key(effective_table)
        || all_cte_scopes.iter().any(
            |s| matches!(&s.expression, Expression::Cte(cte) if cte.alias.name == effective_table),
        );
    if is_cte {
        if let Some(child_scope) = find_child_scope_in(all_cte_scopes, scope, effective_table) {
            if let Ok(child) = to_node_inner(
                ColumnRef::Name(col_name),
                child_scope,
                dialect,
                parent_name,
                effective_table,
                parent_name,
                trim_selects,
                all_cte_scopes,
                depth + 1,
            ) {
                node.downstream.push(child);
                return;
            }
        }

        if let Some(source_info) = scope
            .sources
            .get(table)
            .or_else(|| scope.sources.get(effective_table))
            .filter(|source_info| source_info.kind == SourceKind::Cte)
        {
            node.downstream.push(make_table_column_node_from_source(
                effective_table,
                col_name,
                source_info,
            ));
            return;
        }
    }

    // Check if table is a derived table (is_scope = true in sources)
    if let Some(source_info) = scope.sources.get(table) {
        if source_info.is_scope {
            if let Some(child_scope) = find_child_scope(scope, table) {
                if let Ok(child) = to_node_inner(
                    ColumnRef::Name(col_name),
                    child_scope,
                    dialect,
                    parent_name,
                    table,
                    parent_name,
                    trim_selects,
                    all_cte_scopes,
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
            let mut child = make_table_column_node_from_source(table, col_name, source_info);
            if source_info.kind == SourceKind::Virtual {
                attach_virtual_source_dependencies(
                    &mut child,
                    scope,
                    dialect,
                    table,
                    &source_info.expression,
                    trim_selects,
                    all_cte_scopes,
                    depth,
                );
            }
            node.downstream.push(child);
            return;
        }
    }

    // Base table or unresolved — terminal node
    node.downstream
        .push(make_table_column_node(table, col_name));
}

fn attach_pivot_dependencies(
    node: &mut LineageNode,
    scope: &Scope,
    dialect: Option<DialectType>,
    pivot: &crate::expressions::Pivot,
    col_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[Rc<Scope>],
    depth: usize,
) -> bool {
    if pivot.unpivot {
        return false;
    }

    let mapping = pivot_lineage_column_mapping(pivot, scope, dialect);
    let Some(input_columns) = mapping.get(&normalize_column_name(col_name, dialect)) else {
        if pivot_implicit_source_column(pivot, col_name) {
            let col_ref = SimpleColumnRef {
                table: None,
                column: col_name.to_string(),
            };
            attach_pivot_input_column(
                node,
                scope,
                dialect,
                &pivot.this,
                &col_ref,
                trim_selects,
                all_cte_scopes,
                depth,
            );
            return true;
        }
        return false;
    };

    for col_ref in input_columns {
        attach_pivot_input_column(
            node,
            scope,
            dialect,
            &pivot.this,
            col_ref,
            trim_selects,
            all_cte_scopes,
            depth,
        );
    }
    true
}

fn attach_unpivot_dependencies(
    node: &mut LineageNode,
    scope: &Scope,
    dialect: Option<DialectType>,
    unpivot: &crate::expressions::Unpivot,
    col_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[Rc<Scope>],
    depth: usize,
) -> bool {
    let mapping = unpivot_column_mapping(unpivot, dialect);
    let Some(input_columns) = mapping.get(&normalize_column_name(col_name, dialect)) else {
        return false;
    };

    for col_ref in input_columns {
        attach_pivot_input_column(
            node,
            scope,
            dialect,
            &unpivot.this,
            col_ref,
            trim_selects,
            all_cte_scopes,
            depth,
        );
    }
    true
}

fn pivot_column_mapping(
    pivot: &crate::expressions::Pivot,
    dialect: Option<DialectType>,
) -> HashMap<String, Vec<SimpleColumnRef>> {
    let aggregations = pivot_aggregation_expressions(pivot);
    let output_columns = pivot_generated_output_columns(pivot, dialect);
    if aggregations.is_empty() || output_columns.is_empty() {
        return HashMap::new();
    }

    let mut mapping = HashMap::new();
    for (agg_index, agg) in aggregations.iter().enumerate() {
        let input_columns = find_column_refs_in_expr(agg, dialect);
        if input_columns.is_empty() {
            continue;
        }
        for col_index in (agg_index..output_columns.len()).step_by(aggregations.len()) {
            mapping.insert(
                normalize_column_name(&output_columns[col_index], dialect),
                input_columns.clone(),
            );
        }
    }
    mapping
}

fn pivot_lineage_column_mapping(
    pivot: &crate::expressions::Pivot,
    scope: &Scope,
    dialect: Option<DialectType>,
) -> HashMap<String, Vec<SimpleColumnRef>> {
    let mut mapping = pivot_column_mapping(pivot, dialect);
    let Some(pre_pivot_columns) = pre_pivot_output_columns(&pivot.this, scope) else {
        return mapping;
    };

    let output_columns = pivot_output_columns(pivot, &pre_pivot_columns, dialect);
    if output_columns.is_empty() {
        return mapping;
    }

    let base_mapping = mapping.clone();
    for (post_name, pre_name) in output_columns {
        let normalized_pre = normalize_column_name(&pre_name, dialect);
        let normalized_post = normalize_column_name(&post_name, dialect);

        if let Some(input_columns) = base_mapping.get(&normalized_pre) {
            mapping.insert(normalized_post, input_columns.clone());
        } else {
            mapping.insert(
                normalized_post,
                vec![SimpleColumnRef {
                    table: None,
                    column: pre_name,
                }],
            );
        }
    }

    mapping
}

fn pre_pivot_output_columns(source: &Expression, scope: &Scope) -> Option<Vec<String>> {
    match source {
        Expression::Subquery(subquery) => known_output_columns(&subquery.this),
        Expression::Table(table) if table.schema.is_none() && table.catalog.is_none() => scope
            .cte_sources
            .get(&table.name.name)
            .and_then(|source| known_output_columns(&source.expression)),
        Expression::Paren(paren) => pre_pivot_output_columns(&paren.this, scope),
        _ => None,
    }
}

fn known_output_columns(expression: &Expression) -> Option<Vec<String>> {
    let expression = match expression {
        Expression::Cte(cte) => &cte.this,
        Expression::Subquery(subquery) => &subquery.this,
        other => other,
    };
    let columns = crate::ast_transforms::get_output_column_names(expression);
    if columns.is_empty() || columns.iter().any(|column| column == "*") {
        None
    } else {
        Some(columns)
    }
}

fn pivot_output_columns(
    pivot: &crate::expressions::Pivot,
    pre_pivot_columns: &[String],
    dialect: Option<DialectType>,
) -> Vec<(String, String)> {
    let generated_outputs = pivot_generated_output_columns(pivot, dialect);
    let excluded = pivot_excluded_source_columns(pivot, dialect);

    if excluded.is_empty() || generated_outputs.is_empty() {
        return Vec::new();
    }

    let mut pre_rename: Vec<String> = pre_pivot_columns
        .iter()
        .filter(|column| !excluded.contains(&normalize_column_name(column, dialect)))
        .cloned()
        .collect();
    pre_rename.extend(generated_outputs);

    let post_rename = if pivot.alias_columns.is_empty() {
        pre_rename.clone()
    } else {
        let mut names: Vec<String> = pivot
            .alias_columns
            .iter()
            .map(|column| column.name.clone())
            .collect();
        names.extend(pre_rename.iter().skip(names.len()).cloned());
        names
    };

    post_rename.into_iter().zip(pre_rename).collect()
}

fn pivot_excluded_source_columns(
    pivot: &crate::expressions::Pivot,
    dialect: Option<DialectType>,
) -> HashSet<String> {
    pivot
        .fields
        .iter()
        .chain(pivot.expressions.iter())
        .chain(pivot.using.iter())
        .flat_map(|expr| find_column_refs_in_expr(expr, dialect))
        .map(|column| normalize_column_name(&column.column, dialect))
        .collect()
}

fn pivot_generated_output_columns(
    pivot: &crate::expressions::Pivot,
    _dialect: Option<DialectType>,
) -> Vec<String> {
    let fields = pivot_field_output_names(pivot);
    if fields.is_empty() {
        return Vec::new();
    }

    let aggregations = pivot_aggregation_expressions(pivot);
    if aggregations.is_empty() {
        return Vec::new();
    }

    let needs_suffix = aggregations.len() > 1;
    let mut outputs = Vec::new();
    for field in fields {
        for aggregation in aggregations {
            if let Some(suffix) = pivot_aggregation_output_suffix(aggregation, needs_suffix) {
                outputs.push(format!("{field}_{suffix}"));
            } else {
                outputs.push(field.clone());
            }
        }
    }
    outputs
}

fn pivot_aggregation_expressions(pivot: &crate::expressions::Pivot) -> &[Expression] {
    if pivot.using.is_empty() {
        &pivot.expressions
    } else {
        &pivot.using
    }
}

fn pivot_aggregation_output_suffix(expr: &Expression, needs_suffix: bool) -> Option<String> {
    match expr {
        Expression::Alias(alias) => Some(alias.alias.name.clone()),
        _ if needs_suffix => pivot_generated_aggregation_suffix(expr),
        _ => None,
    }
}

#[cfg(feature = "generate")]
fn pivot_generated_aggregation_suffix(expr: &Expression) -> Option<String> {
    Generator::sql(expr).ok().map(|sql| sql.to_lowercase())
}

#[cfg(not(feature = "generate"))]
fn pivot_generated_aggregation_suffix(expr: &Expression) -> Option<String> {
    pivot_expr_output_name(expr).or_else(|| Some(expr.variant_name().to_string()))
}

fn pivot_field_output_names(pivot: &crate::expressions::Pivot) -> Vec<String> {
    let mut names = Vec::new();
    for field in &pivot.fields {
        if let Expression::In(in_expr) = field {
            for expr in &in_expr.expressions {
                if let Some(name) = pivot_expr_output_name(expr) {
                    names.push(name);
                }
            }
        }
    }
    names
}

fn pivot_expr_output_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::PivotAlias(alias) => pivot_expr_output_name(&alias.alias),
        Expression::Alias(alias) => Some(alias.alias.name.clone()),
        Expression::Identifier(identifier) => Some(identifier.name.clone()),
        Expression::Column(column) => Some(column.name.name.clone()),
        Expression::Literal(literal) => Some(literal.value_str().to_string()),
        Expression::Var(var) => Some(var.this.clone()),
        Expression::Tuple(tuple) => tuple.expressions.first().and_then(pivot_expr_output_name),
        _ => None,
    }
}

fn pivot_implicit_source_column(pivot: &crate::expressions::Pivot, col_name: &str) -> bool {
    let pivot_columns: HashSet<String> = pivot
        .fields
        .iter()
        .filter_map(|field| match field {
            Expression::In(in_expr) => Some(&in_expr.this),
            _ => None,
        })
        .flat_map(|expr| find_column_refs_in_expr(expr, None))
        .map(|col| col.column.to_lowercase())
        .collect();
    let aggregation_columns: HashSet<String> = pivot
        .expressions
        .iter()
        .flat_map(|expr| find_column_refs_in_expr(expr, None))
        .map(|col| col.column.to_lowercase())
        .collect();

    let normalized = col_name.to_lowercase();
    !pivot_columns.contains(&normalized) && !aggregation_columns.contains(&normalized)
}

fn unpivot_column_mapping(
    unpivot: &crate::expressions::Unpivot,
    dialect: Option<DialectType>,
) -> HashMap<String, Vec<SimpleColumnRef>> {
    let value_columns: Vec<String> = std::iter::once(unpivot.value_column.name.clone())
        .chain(
            unpivot
                .extra_value_columns
                .iter()
                .map(|column| column.name.clone()),
        )
        .collect();
    let mut all_input_columns = Vec::new();
    let mut value_input_columns: Vec<Vec<SimpleColumnRef>> = vec![Vec::new(); value_columns.len()];

    for entry in &unpivot.columns {
        let columns = unpivot_entry_columns(entry);
        all_input_columns.extend(columns.clone());
        if columns.len() == value_columns.len() {
            for (idx, col_ref) in columns.into_iter().enumerate() {
                value_input_columns[idx].push(col_ref);
            }
        } else {
            for inputs in &mut value_input_columns {
                inputs.extend(columns.clone());
            }
        }
    }

    let mut mapping = HashMap::new();
    mapping.insert(
        normalize_column_name(&unpivot.name_column.name, dialect),
        all_input_columns.clone(),
    );
    for (idx, value_column) in value_columns.iter().enumerate() {
        mapping.insert(
            normalize_column_name(value_column, dialect),
            value_input_columns.get(idx).cloned().unwrap_or_default(),
        );
    }
    mapping
}

fn unpivot_entry_columns(expr: &Expression) -> Vec<SimpleColumnRef> {
    match expr {
        Expression::PivotAlias(alias) => unpivot_entry_columns(&alias.this),
        Expression::Tuple(tuple) => tuple
            .expressions
            .iter()
            .flat_map(unpivot_entry_columns)
            .collect(),
        Expression::Column(column) => vec![SimpleColumnRef {
            table: column.table.clone(),
            column: column.name.name.clone(),
        }],
        Expression::Identifier(identifier) => vec![SimpleColumnRef {
            table: None,
            column: identifier.name.clone(),
        }],
        _ => find_column_refs_in_expr(expr, None),
    }
}

fn attach_pivot_input_column(
    node: &mut LineageNode,
    scope: &Scope,
    dialect: Option<DialectType>,
    source_expr: &Expression,
    col_ref: &SimpleColumnRef,
    trim_selects: bool,
    all_cte_scopes: &[Rc<Scope>],
    depth: usize,
) {
    match source_expr {
        Expression::Table(table) => {
            let table_name = col_ref
                .table
                .as_ref()
                .map(|identifier| identifier.name.as_str())
                .unwrap_or(table.name.name.as_str());
            if scope.cte_sources.contains_key(table_name) {
                resolve_qualified_column(
                    node,
                    scope,
                    dialect,
                    table_name,
                    &col_ref.column,
                    &node.name.clone(),
                    trim_selects,
                    all_cte_scopes,
                    depth + 1,
                );
            } else {
                let mut source = ScopeSourceInfo::new(
                    Expression::Table(Box::new(table.as_ref().clone())),
                    false,
                    SourceKind::Table,
                );
                if let Some(alias) = &table.alias {
                    source = source.with_alias(alias.name.clone());
                }
                let source_key = table
                    .alias
                    .as_ref()
                    .map(|alias| alias.name.as_str())
                    .unwrap_or(table.name.name.as_str());
                node.downstream.push(make_table_column_node_from_source(
                    source_key,
                    &col_ref.column,
                    &source,
                ));
            }
        }
        Expression::Subquery(subquery) => {
            let source_scope = build_scope(&subquery.this);
            let child = if let Some(table) = &col_ref.table {
                let mut child_node = LineageNode::new(
                    &col_ref.column,
                    subquery.this.clone(),
                    subquery.this.clone(),
                );
                resolve_qualified_column(
                    &mut child_node,
                    &source_scope,
                    dialect,
                    &table.name,
                    &col_ref.column,
                    &node.name.clone(),
                    trim_selects,
                    all_cte_scopes,
                    depth + 1,
                );
                Ok(child_node)
            } else {
                to_node_inner(
                    ColumnRef::Name(&col_ref.column),
                    &source_scope,
                    dialect,
                    "",
                    "",
                    "",
                    trim_selects,
                    all_cte_scopes,
                    depth + 1,
                )
            };
            if let Ok(child) = child {
                node.downstream.push(child);
            }
        }
        Expression::Paren(paren) => attach_pivot_input_column(
            node,
            scope,
            dialect,
            &paren.this,
            col_ref,
            trim_selects,
            all_cte_scopes,
            depth,
        ),
        _ => {
            if let Some(table) = &col_ref.table {
                resolve_qualified_column(
                    node,
                    scope,
                    dialect,
                    &table.name,
                    &col_ref.column,
                    &node.name.clone(),
                    trim_selects,
                    all_cte_scopes,
                    depth + 1,
                );
            } else {
                node.downstream
                    .push(make_table_column_node("_", &col_ref.column));
            }
        }
    }
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
    all_cte_scopes: &[Rc<Scope>],
    depth: usize,
) {
    // Try to find which source this column belongs to.
    // Build the source list from the actual FROM/JOIN clauses to avoid
    // mixing in CTE definitions that are in scope but not referenced.
    let from_source_names = source_names_from_from_join(scope);

    if let Some(tbl) = unique_virtual_source_for_column(scope, &from_source_names, col_name) {
        resolve_qualified_column(
            node,
            scope,
            dialect,
            &tbl,
            col_name,
            parent_name,
            trim_selects,
            all_cte_scopes,
            depth,
        );
        return;
    }

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

fn unique_virtual_source_for_column(
    scope: &Scope,
    source_names: &[String],
    col_name: &str,
) -> Option<String> {
    let mut matches = source_names.iter().filter_map(|source_name| {
        let source = scope.sources.get(source_name)?;
        if source.kind == SourceKind::Virtual
            && virtual_source_output_columns(source)
                .any(|column| column.eq_ignore_ascii_case(col_name))
        {
            Some(source_name.clone())
        } else {
            None
        }
    });

    let first = matches.next()?;
    if matches.next().is_none() {
        Some(first)
    } else {
        None
    }
}

fn virtual_source_output_columns(
    source_info: &ScopeSourceInfo,
) -> Box<dyn Iterator<Item = String> + '_> {
    match &source_info.expression {
        Expression::Unnest(unnest) => Box::new(unnest_output_columns(unnest)),
        Expression::Alias(alias) if matches!(&alias.this, Expression::Unnest(_)) => {
            Box::new(alias_output_columns(alias))
        }
        Expression::Lateral(lateral) => Box::new(lateral_output_columns(lateral)),
        Expression::LateralView(lateral_view) => {
            Box::new(lateral_view_output_columns(lateral_view))
        }
        _ => Box::new(source_info.alias.clone().into_iter()),
    }
}

fn unnest_output_columns(
    unnest: &crate::expressions::UnnestFunc,
) -> impl Iterator<Item = String> + '_ {
    unnest
        .alias
        .iter()
        .map(|alias| alias.name.clone())
        .chain(unnest.offset_alias.iter().map(|alias| alias.name.clone()))
}

fn alias_output_columns(
    alias: &crate::expressions::Alias,
) -> Box<dyn Iterator<Item = String> + '_> {
    if alias.column_aliases.is_empty() {
        Box::new(std::iter::once(alias.alias.name.clone()))
    } else {
        Box::new(
            alias
                .column_aliases
                .iter()
                .map(|column| column.name.clone()),
        )
    }
}

fn lateral_output_columns(
    lateral: &crate::expressions::Lateral,
) -> Box<dyn Iterator<Item = String> + '_> {
    if lateral.column_aliases.is_empty() {
        default_virtual_output_columns(&lateral.this)
    } else {
        Box::new(lateral.column_aliases.iter().cloned())
    }
}

fn lateral_view_output_columns(
    lateral_view: &crate::expressions::LateralView,
) -> Box<dyn Iterator<Item = String> + '_> {
    Box::new(
        lateral_view
            .column_aliases
            .iter()
            .map(|column| column.name.clone()),
    )
}

fn default_virtual_output_columns(expr: &Expression) -> Box<dyn Iterator<Item = String> + '_> {
    match expr {
        Expression::Unnest(unnest) => Box::new(unnest_output_columns(unnest)),
        Expression::Alias(alias) if matches!(&alias.this, Expression::Unnest(_)) => {
            alias_output_columns(alias)
        }
        Expression::Function(function) if function.name.eq_ignore_ascii_case("FLATTEN") => {
            Box::new(
                ["seq", "key", "path", "index", "value", "this"]
                    .into_iter()
                    .map(String::from),
            )
        }
        _ => Box::new(std::iter::empty()),
    }
}

fn attach_virtual_source_dependencies(
    node: &mut LineageNode,
    scope: &Scope,
    dialect: Option<DialectType>,
    source_alias: &str,
    source_expr: &Expression,
    trim_selects: bool,
    all_cte_scopes: &[Rc<Scope>],
    depth: usize,
) {
    let parent_name = node.name.clone();
    let mut seen = HashSet::new();
    for col_ref in find_column_refs_in_expr(source_expr, dialect) {
        let key = (
            col_ref.table.as_ref().map(|t| t.name.clone()),
            col_ref.column.clone(),
        );
        if !seen.insert(key) {
            continue;
        }

        if let Some(table_id) = col_ref.table {
            let table = table_id.name;
            if table == source_alias {
                continue;
            }
            resolve_qualified_column(
                node,
                scope,
                dialect,
                &table,
                &col_ref.column,
                &parent_name,
                trim_selects,
                all_cte_scopes,
                depth + 1,
            );
        } else {
            let non_virtual_sources = non_virtual_source_names_from_from_join(scope);
            if non_virtual_sources.len() == 1 {
                resolve_qualified_column(
                    node,
                    scope,
                    dialect,
                    &non_virtual_sources[0],
                    &col_ref.column,
                    &parent_name,
                    trim_selects,
                    all_cte_scopes,
                    depth + 1,
                );
            }
        }
    }
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
            Expression::Unnest(unnest) => unnest.alias.as_ref().map(|alias| alias.name.clone()),
            Expression::Alias(alias) if matches!(&alias.this, Expression::Unnest(_)) => {
                Some(alias.alias.name.clone())
            }
            Expression::Alias(alias) if is_query_like_relation(&alias.this) => {
                Some(alias.alias.name.clone())
            }
            Expression::Lateral(lateral) => lateral.alias.clone(),
            Expression::LateralView(lateral_view) => lateral_view
                .table_alias
                .as_ref()
                .or_else(|| lateral_view.column_aliases.first())
                .map(|alias| alias.name.clone()),
            Expression::Pivot(pivot) => Some(pivot_lineage_source_name(
                &pivot.this,
                pivot.alias.as_ref().map(|alias| alias.name.as_str()),
            )),
            Expression::Unpivot(unpivot) => Some(pivot_lineage_source_name(
                &unpivot.this,
                unpivot.alias.as_ref().map(|alias| alias.name.as_str()),
            )),
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
            if is_semi_or_anti_join_kind(join.kind) {
                continue;
            }
            if let Some(name) = source_name(&join.this) {
                if !name.is_empty() && seen.insert(name.clone()) {
                    names.push(name);
                }
            }
        }
        for lateral_view in &select.lateral_views {
            if let Some(name) =
                source_name(&Expression::LateralView(Box::new(lateral_view.clone())))
            {
                if !name.is_empty() && seen.insert(name.clone()) {
                    names.push(name);
                }
            }
        }
    }

    names
}

fn is_semi_or_anti_join_kind(kind: JoinKind) -> bool {
    matches!(
        kind,
        JoinKind::Semi
            | JoinKind::Anti
            | JoinKind::LeftSemi
            | JoinKind::LeftAnti
            | JoinKind::RightSemi
            | JoinKind::RightAnti
    )
}

fn is_query_like_relation(expr: &Expression) -> bool {
    match expr {
        Expression::Select(_)
        | Expression::Subquery(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_) => true,
        Expression::Paren(paren) => is_query_like_relation(&paren.this),
        _ => false,
    }
}

fn derived_source_query(expr: &Expression) -> Option<&Expression> {
    match expr {
        Expression::Subquery(subquery) => Some(&subquery.this),
        Expression::Alias(alias) if is_query_like_relation(&alias.this) => Some(&alias.this),
        Expression::Select(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_) => Some(expr),
        Expression::Paren(paren) => derived_source_query(&paren.this),
        _ => None,
    }
}

fn expressions_equivalent_after_wrappers(left: &Expression, right: &Expression) -> bool {
    left == right || effective_scope_expression(left) == effective_scope_expression(right)
}

fn non_virtual_source_names_from_from_join(scope: &Scope) -> Vec<String> {
    source_names_from_from_join(scope)
        .into_iter()
        .filter(|name| {
            !matches!(
                scope.sources.get(name).map(|source| source.kind),
                Some(SourceKind::Virtual)
            )
        })
        .collect()
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
        // Annotated wraps an expression with trailing comments (e.g. `SELECT\n-- comment\na`).
        // Unwrap to get the actual column/alias name from the inner expression.
        Expression::Annotated(a) => get_alias_or_name(&a.this),
        _ => None,
    }
}

fn find_prior_select_alias_expr(
    scope_expr: &Expression,
    target_expr: &Expression,
    alias_name: &str,
    dialect: Option<DialectType>,
) -> Option<Expression> {
    let Expression::Select(select) = scope_expr else {
        return None;
    };

    let normalized_alias = normalize_column_name(alias_name, dialect);
    for expr in &select.expressions {
        if expr == target_expr {
            return None;
        }

        if let Expression::Alias(alias) = expr {
            if normalize_column_name(&alias.alias.name, dialect) == normalized_alias {
                return Some(alias.this.clone());
            }
        }
    }

    None
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
                if let Some(expr) = synthesize_star_passthrough_expr(select, name) {
                    return Ok(expr);
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

fn synthesize_star_passthrough_expr(select: &Select, name: &str) -> Option<Expression> {
    let sources = get_select_sources(select);
    if sources.is_empty() {
        return None;
    }

    let mut candidate_aliases = Vec::new();
    let mut seen = HashSet::new();

    for expr in &select.expressions {
        let aliases = match star_passthrough_source_aliases(expr, &sources) {
            StarPassthroughSources::None => continue,
            StarPassthroughSources::Ambiguous => return None,
            StarPassthroughSources::Aliases(aliases) => aliases,
        };

        for alias in aliases {
            if seen.insert(alias.clone()) {
                candidate_aliases.push(alias);
            }
        }
    }

    match candidate_aliases.as_slice() {
        [alias] => {
            let table = Identifier::new(alias.clone());
            Some(make_column_expr(name, Some(&table)))
        }
        _ => None,
    }
}

enum StarPassthroughSources {
    None,
    Ambiguous,
    Aliases(Vec<String>),
}

fn star_passthrough_source_aliases(
    expr: &Expression,
    sources: &[SourceInfo],
) -> StarPassthroughSources {
    match expr {
        Expression::Star(star) => star_source_aliases(star.table.as_ref(), sources),
        Expression::Column(column) if column.name.name == "*" => {
            star_source_aliases(column.table.as_ref(), sources)
        }
        Expression::Annotated(annotated) => {
            star_passthrough_source_aliases(&annotated.this, sources)
        }
        _ => StarPassthroughSources::None,
    }
}

fn star_source_aliases(
    qualifier: Option<&Identifier>,
    sources: &[SourceInfo],
) -> StarPassthroughSources {
    if let Some(qualifier) = qualifier {
        let mut aliases = Vec::new();

        for source in sources {
            if source_matches_star_qualifier(source, qualifier) {
                aliases.push(source.alias.clone());
            }
        }

        return match aliases.len() {
            0 => StarPassthroughSources::None,
            1 => StarPassthroughSources::Aliases(aliases),
            _ => StarPassthroughSources::Ambiguous,
        };
    }

    match sources {
        // Do not synthesize a source column for unresolved quoted table stars.
        // This keeps quoted CTE/table case semantics intact while still allowing
        // the schema-less fallback for common unquoted SELECT * passthroughs.
        [source] if source.quoted => StarPassthroughSources::None,
        [source] => StarPassthroughSources::Aliases(vec![source.alias.clone()]),
        [] => StarPassthroughSources::None,
        _ => StarPassthroughSources::Ambiguous,
    }
}

fn source_matches_star_qualifier(source: &SourceInfo, qualifier: &Identifier) -> bool {
    if source.normalized == normalize_cte_name(qualifier) {
        return true;
    }

    if qualifier.quoted {
        source.alias == qualifier.name
    } else {
        source.alias.eq_ignore_ascii_case(&qualifier.name)
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
            Expression::Subquery(subquery) => expr = &subquery.this,
            Expression::Cte(cte) => expr = &cte.this,
            Expression::Paren(paren) => expr = &paren.this,
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
            if let Some(query) = derived_source_query(&source_info.expression) {
                for dt_scope in &scope.derived_table_scopes {
                    if expressions_equivalent_after_wrappers(&dt_scope.expression, query) {
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
    all_cte_scopes: &'a [Rc<Scope>],
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
                return Some(cte_scope.as_ref());
            }
        }
    }

    // Fall back to derived table scopes
    if let Some(source_info) = scope.sources.get(source_name) {
        if source_info.is_scope {
            if let Some(query) = derived_source_query(&source_info.expression) {
                for dt_scope in &scope.derived_table_scopes {
                    if expressions_equivalent_after_wrappers(&dt_scope.expression, query) {
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
    node.source_kind = SourceKind::Table;
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

fn apply_source_info_context(
    node: &mut LineageNode,
    source_key: &str,
    source_info: &ScopeSourceInfo,
) {
    node.source_kind = source_info.kind;
    node.source_name =
        source_info
            .lineage_name
            .clone()
            .unwrap_or_else(|| match &source_info.expression {
                Expression::Table(table_ref) => table_name_from_table_ref(table_ref),
                _ => source_key.to_string(),
            });
    node.source_alias = source_info.alias.clone();
}

fn make_table_column_node_from_source(
    source_key: &str,
    column: &str,
    source_info: &ScopeSourceInfo,
) -> LineageNode {
    let lineage_name = source_info.lineage_name.as_deref().unwrap_or(source_key);
    let mut node = LineageNode::new(
        format!("{}.{}", lineage_name, column),
        Expression::Column(Box::new(crate::expressions::Column {
            name: crate::expressions::Identifier::new(column.to_string()),
            table: Some(crate::expressions::Identifier::new(
                lineage_name.to_string(),
            )),
            join_mark: false,
            trailing_comments: vec![],
            span: None,
            inferred_type: None,
        })),
        source_info.expression.clone(),
    );

    apply_source_info_context(&mut node, source_key, source_info);

    node
}

/// Simple column reference extracted from an expression
#[derive(Debug, Clone)]
struct SimpleColumnRef {
    table: Option<crate::expressions::Identifier>,
    column: String,
}

/// Find all column references in an expression (does not recurse into subqueries).
fn find_column_refs_in_expr(
    expr: &Expression,
    dialect: Option<DialectType>,
) -> Vec<SimpleColumnRef> {
    let mut refs = Vec::new();
    collect_column_refs(expr, dialect, &mut refs, None);
    refs
}

fn find_column_refs_in_expr_with_select(
    expr: &Expression,
    select_expr: &Expression,
    dialect: Option<DialectType>,
) -> Vec<SimpleColumnRef> {
    let named_windows = match select_expr {
        Expression::Select(select) => select.windows.as_deref(),
        _ => None,
    };
    let mut refs = Vec::new();
    collect_column_refs(expr, dialect, &mut refs, named_windows);
    refs
}

fn is_bigquery_safe_namespace_receiver(expr: &Expression) -> bool {
    match expr {
        Expression::Column(col) => {
            col.table.is_none() && !col.name.quoted && col.name.name.eq_ignore_ascii_case("SAFE")
        }
        Expression::Identifier(id) => !id.quoted && id.name.eq_ignore_ascii_case("SAFE"),
        _ => false,
    }
}

fn collect_column_refs(
    expr: &Expression,
    dialect: Option<DialectType>,
    refs: &mut Vec<SimpleColumnRef>,
    named_windows: Option<&[NamedWindow]>,
) {
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
                for e in &wf.over.partition_by {
                    stack.push(e);
                }
                for e in &wf.over.order_by {
                    stack.push(&e.this);
                }
                if let Some(keep) = &wf.keep {
                    for e in &keep.order_by {
                        stack.push(&e.this);
                    }
                }
                if let (Some(window_name), Some(named_windows)) =
                    (&wf.over.window_name, named_windows)
                {
                    for named_window in named_windows {
                        if named_window
                            .name
                            .name
                            .eq_ignore_ascii_case(&window_name.name)
                        {
                            for e in &named_window.spec.partition_by {
                                stack.push(e);
                            }
                            for e in &named_window.spec.order_by {
                                stack.push(&e.this);
                            }
                        }
                    }
                }
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
                if !matches!(dialect, Some(DialectType::BigQuery))
                    || !is_bigquery_safe_namespace_receiver(&m.this)
                {
                    stack.push(&m.this);
                }
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
            Expression::Lateral(l) => {
                stack.push(&l.this);
                if let Some(ref view) = l.view {
                    stack.push(view);
                }
                if let Some(ref outer) = l.outer {
                    stack.push(outer);
                }
                if let Some(ref ordinality) = l.ordinality {
                    stack.push(ordinality);
                }
            }
            Expression::LateralView(lv) => {
                stack.push(&lv.this);
            }
            Expression::TryCatch(t) => {
                for stmt in &t.try_body {
                    stack.push(stmt);
                }
                if let Some(catch_body) = &t.catch_body {
                    for stmt in catch_body {
                        stack.push(stmt);
                    }
                }
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
                for e in &f.order_by {
                    stack.push(&e.this);
                }
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
            Expression::JSONExtract(f) => {
                stack.push(&f.this);
                stack.push(&f.expression);
                for e in &f.expressions {
                    stack.push(e);
                }
                if let Some(ref option) = f.option {
                    stack.push(option);
                }
                if let Some(ref on_condition) = f.on_condition {
                    stack.push(on_condition);
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

    fn parse_dialect(sql: &str, dialect_type: DialectType) -> Expression {
        let dialect = Dialect::get(dialect_type);
        let ast = dialect.parse(sql).unwrap();
        ast.into_iter().next().unwrap()
    }

    fn lineage_names(node: &LineageNode) -> Vec<String> {
        node.walk().map(|n| n.name.clone()).collect()
    }

    fn assert_lineage_contains(node: &LineageNode, expected: &str) {
        let names = lineage_names(node);
        assert!(
            names.iter().any(|name| name == expected),
            "expected {expected} in lineage, got {names:?}"
        );
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
            source_kind: SourceKind::Unknown,
            source_alias: None,
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
    fn test_lineage_with_schema_tolerates_partial_schema_for_known_column() {
        let expr = parse_dialect("SELECT order_id, amount FROM t", DialectType::DuckDB);
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "t",
                &[("amount".into(), DataType::BigInt { length: None })],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "amount",
            &expr,
            Some(&schema),
            Some(DialectType::DuckDB),
            false,
        )
        .expect("lineage_with_schema should tolerate unrelated unknown columns");

        assert_lineage_contains(&node, "t.amount");
    }

    #[test]
    fn test_lineage_with_schema_tolerates_partial_schema_for_unknown_column() {
        let expr = parse_dialect("SELECT order_id, amount FROM t", DialectType::DuckDB);
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "t",
                &[("amount".into(), DataType::BigInt { length: None })],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "order_id",
            &expr,
            Some(&schema),
            Some(DialectType::DuckDB),
            false,
        )
        .expect("lineage_with_schema should keep unknown selected columns");

        assert_lineage_contains(&node, "t.order_id");
    }

    #[test]
    fn test_lineage_with_schema_tolerates_partial_schema_for_join_conditions() {
        let expr = parse_dialect(
            "SELECT a.order_id, b.amount FROM t a JOIN u b ON a.id = b.id",
            DialectType::DuckDB,
        );
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "t",
                &[("order_id".into(), DataType::BigInt { length: None })],
                None,
            )
            .expect("schema setup");
        schema
            .add_table(
                "u",
                &[("amount".into(), DataType::BigInt { length: None })],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "amount",
            &expr,
            Some(&schema),
            Some(DialectType::DuckDB),
            false,
        )
        .expect("lineage_with_schema should tolerate unknown join keys");

        assert_lineage_contains(&node, "b.amount");
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
    fn test_lineage_bigquery_unnest_alias_source_issue_209() {
        let expr = parse_one(
            r#"
SELECT date_val AS week_start
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-12-31', INTERVAL 1 WEEK)) AS date_val
"#,
            DialectType::BigQuery,
        )
        .expect("parse");

        let node = lineage("week_start", &expr, Some(DialectType::BigQuery), false)
            .expect("lineage should resolve UNNEST alias as a source");
        let child = node
            .downstream
            .first()
            .expect("week_start should have downstream lineage");

        assert_eq!(child.name, "_0.date_val");
        assert_eq!(child.source_name, "_0");
        assert_eq!(child.source_kind, SourceKind::Virtual);
        assert_eq!(child.source_alias.as_deref(), Some("date_val"));

        let Expression::Column(column) = &child.expression else {
            panic!(
                "expected downstream column expression, got {:?}",
                child.expression
            );
        };
        assert_eq!(column.name.name, "date_val");
        assert_eq!(
            column.table.as_ref().map(|table| table.name.as_str()),
            Some("_0")
        );
        assert!(
            matches!(&child.source, Expression::Alias(alias) if matches!(&alias.this, Expression::Unnest(_)) && alias.alias.name == "date_val"),
            "expected UNNEST source expression, got {:?}",
            child.source
        );
    }

    #[test]
    fn test_lineage_real_table_named_like_unnest_alias_is_not_virtual() {
        let expr =
            parse_one("SELECT date_val.id FROM date_val", DialectType::BigQuery).expect("parse");

        let node = lineage("id", &expr, Some(DialectType::BigQuery), false).expect("lineage");
        let child = node.downstream.first().expect("id should have lineage");

        assert_eq!(child.name, "date_val.id");
        assert_eq!(child.source_name, "date_val");
        assert_eq!(child.source_kind, SourceKind::Table);
        assert_eq!(child.source_alias, None);
    }

    #[test]
    fn test_lineage_multiple_bigquery_unnest_sources_get_stable_virtual_names() {
        let expr = parse_one(
            r#"
SELECT a.a AS first_value, b.b AS second_value
FROM UNNEST(GENERATE_ARRAY(1, 2)) AS a
JOIN UNNEST(GENERATE_ARRAY(3, 4)) AS b ON TRUE
"#,
            DialectType::BigQuery,
        )
        .expect("parse");

        let first =
            lineage("first_value", &expr, Some(DialectType::BigQuery), false).expect("lineage");
        let second =
            lineage("second_value", &expr, Some(DialectType::BigQuery), false).expect("lineage");

        let first_child = first.downstream.first().expect("first source");
        let second_child = second.downstream.first().expect("second source");

        assert_eq!(first_child.name, "_0.a");
        assert_eq!(first_child.source_name, "_0");
        assert_eq!(first_child.source_alias.as_deref(), Some("a"));
        assert_eq!(first_child.source_kind, SourceKind::Virtual);

        assert_eq!(second_child.name, "_1.b");
        assert_eq!(second_child.source_name, "_1");
        assert_eq!(second_child.source_alias.as_deref(), Some("b"));
        assert_eq!(second_child.source_kind, SourceKind::Virtual);
    }

    #[test]
    fn test_lineage_table_backed_unnest_points_to_real_source_column() {
        let expr = parse_one(
            r#"
SELECT item.item AS item
FROM t JOIN UNNEST(t.items) AS item ON TRUE
"#,
            DialectType::BigQuery,
        )
        .expect("parse");

        let node = lineage("item", &expr, Some(DialectType::BigQuery), false).expect("lineage");
        let virtual_child = node.downstream.first().expect("virtual item source");
        assert_eq!(virtual_child.name, "_0.item");
        assert_eq!(virtual_child.source_kind, SourceKind::Virtual);

        let real_child = virtual_child
            .downstream
            .first()
            .expect("UNNEST(t.items) should depend on t.items");
        assert_eq!(real_child.name, "t.items");
        assert_eq!(real_child.source_name, "t");
        assert_eq!(real_child.source_kind, SourceKind::Table);
    }

    #[test]
    fn test_lineage_table_backed_unnest_unqualified_column_resolves_to_virtual_source() {
        let expr = parse_one(
            r#"
SELECT item AS item
FROM t JOIN UNNEST(t.items) AS item ON TRUE
"#,
            DialectType::BigQuery,
        )
        .expect("parse");

        let node = lineage("item", &expr, Some(DialectType::BigQuery), false).expect("lineage");
        let virtual_child = node.downstream.first().expect("virtual item source");
        assert_eq!(virtual_child.name, "_0.item");
        assert_eq!(virtual_child.source_name, "_0");
        assert_eq!(virtual_child.source_kind, SourceKind::Virtual);
        assert_eq!(virtual_child.source_alias.as_deref(), Some("item"));

        let real_child = virtual_child
            .downstream
            .first()
            .expect("UNNEST(t.items) should depend on t.items");
        assert_eq!(real_child.name, "t.items");
        assert_eq!(real_child.source_name, "t");
        assert_eq!(real_child.source_kind, SourceKind::Table);
    }

    #[test]
    fn test_lineage_unnest_alias_columns_resolve_to_virtual_sources_across_dialects() {
        let cases = [
            (
                DialectType::PostgreSQL,
                "SELECT x AS out FROM t CROSS JOIN LATERAL UNNEST(items) AS u(x)",
            ),
            (
                DialectType::Presto,
                "SELECT x AS out FROM t CROSS JOIN UNNEST(items) AS u(x)",
            ),
            (
                DialectType::Trino,
                "SELECT x AS out FROM t CROSS JOIN UNNEST(items) AS u(x)",
            ),
        ];

        for (dialect, sql) in cases {
            let expr = parse_one(sql, dialect).unwrap_or_else(|e| panic!("parse {dialect:?}: {e}"));
            let node = lineage("out", &expr, Some(dialect), false)
                .unwrap_or_else(|e| panic!("lineage {dialect:?}: {e}"));
            let virtual_child = node
                .downstream
                .first()
                .unwrap_or_else(|| panic!("expected virtual child for {dialect:?}"));

            assert_eq!(
                virtual_child.name, "_0.x",
                "unexpected virtual child for {dialect:?}"
            );
            assert_eq!(virtual_child.source_name, "_0");
            assert_eq!(virtual_child.source_kind, SourceKind::Virtual);
            assert_eq!(virtual_child.source_alias.as_deref(), Some("u"));

            let real_child = virtual_child
                .downstream
                .first()
                .unwrap_or_else(|| panic!("expected table dependency for {dialect:?}"));
            assert_eq!(real_child.name, "t.items");
            assert_eq!(real_child.source_kind, SourceKind::Table);
        }
    }

    #[test]
    fn test_lineage_lateral_view_columns_resolve_to_virtual_sources() {
        let cases = [
            (
                DialectType::Spark,
                "SELECT x AS out FROM t LATERAL VIEW EXPLODE(items) u AS x",
            ),
            (
                DialectType::Hive,
                "SELECT x AS out FROM t LATERAL VIEW EXPLODE(items) u AS x",
            ),
        ];

        for (dialect, sql) in cases {
            let expr = parse_one(sql, dialect).unwrap_or_else(|e| panic!("parse {dialect:?}: {e}"));
            let node = lineage("out", &expr, Some(dialect), false)
                .unwrap_or_else(|e| panic!("lineage {dialect:?}: {e}"));
            let virtual_child = node
                .downstream
                .first()
                .unwrap_or_else(|| panic!("expected virtual child for {dialect:?}"));

            assert_eq!(virtual_child.name, "_0.x");
            assert_eq!(virtual_child.source_name, "_0");
            assert_eq!(virtual_child.source_kind, SourceKind::Virtual);
            assert_eq!(virtual_child.source_alias.as_deref(), Some("u"));

            let real_child = virtual_child
                .downstream
                .first()
                .unwrap_or_else(|| panic!("expected table dependency for {dialect:?}"));
            assert_eq!(real_child.name, "t.items");
            assert_eq!(real_child.source_kind, SourceKind::Table);
        }
    }

    #[test]
    fn test_lineage_snowflake_lateral_flatten_is_virtual_source() {
        let expr = parse_one(
            "SELECT f.value AS value FROM raw_events, LATERAL FLATTEN(INPUT => payload:items) AS f",
            DialectType::Snowflake,
        )
        .expect("parse");

        let node = lineage("value", &expr, Some(DialectType::Snowflake), false).expect("lineage");
        let virtual_child = node.downstream.first().expect("virtual flatten source");
        assert_eq!(virtual_child.name, "_0.value");
        assert_eq!(virtual_child.source_name, "_0");
        assert_eq!(virtual_child.source_kind, SourceKind::Virtual);
        assert_eq!(virtual_child.source_alias.as_deref(), Some("f"));

        let real_child = virtual_child
            .downstream
            .first()
            .expect("FLATTEN input should depend on raw_events.payload");
        assert_eq!(real_child.name, "raw_events.payload");
        assert_eq!(real_child.source_kind, SourceKind::Table);
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
    fn test_lineage_bigquery_safe_namespace_issue207() {
        let query = r#"
WITH import_cte AS (
  SELECT timestamp, data, operation
  FROM `project`.`dataset`.`source_table`
),
transform_cte AS (
  SELECT
    timestamp,
    SAFE.PARSE_JSON(data) AS json_data
  FROM import_cte
)
SELECT json_data FROM transform_cte
"#;
        let expr = parse_one(query, DialectType::BigQuery).expect("parse");
        let node = lineage("json_data", &expr, Some(DialectType::BigQuery), false)
            .expect("lineage should resolve SAFE.PARSE_JSON arguments");
        let names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();

        assert!(
            names.iter().any(|name| name == "source_table.data"),
            "expected source_table.data in lineage, got {names:?}"
        );
        assert!(
            !names
                .iter()
                .any(|name| name.eq_ignore_ascii_case("import_cte.safe")),
            "did not expect SAFE namespace receiver in lineage, got {names:?}"
        );
    }

    #[test]
    fn test_lineage_bigquery_safe_namespace_method_call_guard() {
        let expr = parse("SELECT SAFE.PARSE_JSON(data) AS json_data FROM t");
        let node = lineage("json_data", &expr, Some(DialectType::BigQuery), false)
            .expect("lineage should resolve SAFE.PARSE_JSON arguments");
        let names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();

        assert!(
            names.iter().any(|name| name == "t.data"),
            "expected t.data in lineage, got {names:?}"
        );
        assert!(
            !names.iter().any(|name| name.eq_ignore_ascii_case("t.safe")),
            "did not expect SAFE namespace receiver in lineage, got {names:?}"
        );
    }

    #[test]
    fn test_lineage_method_call_receiver_control() {
        let expr = parse("SELECT obj.METHOD(arg) AS out FROM t");
        let node = lineage("out", &expr, None, false).expect("lineage");
        let names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();

        assert!(
            names.iter().any(|name| name == "t.obj"),
            "expected ordinary method receiver to remain in lineage, got {names:?}"
        );
        assert!(
            names.iter().any(|name| name == "t.arg"),
            "expected method argument in lineage, got {names:?}"
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
    fn test_lineage_schema_less_cte_star_passthrough_resolves_base_column() {
        let expr = parse("WITH c AS (SELECT * FROM t) SELECT c.x FROM c");
        let node = lineage("x", &expr, None, false).unwrap();

        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|name| name == "t.x"),
            "Expected schema-less CTE star passthrough to reach t.x, got: {:?}",
            all_names
        );

        let cte_node = node
            .walk()
            .find(|child| child.source_kind == SourceKind::Cte && child.source_name == "c")
            .expect("expected CTE hop with source_name c");
        assert_eq!(cte_node.source_kind, SourceKind::Cte);
        assert_eq!(cte_node.source_name, "c");
    }

    #[test]
    fn test_lineage_schema_less_cte_star_passthrough_with_aggregation() {
        let expr = parse(
            "WITH c AS (SELECT * FROM t) \
             SELECT SUM(c.x) AS s FROM c GROUP BY 1",
        );
        let node = lineage("s", &expr, None, false).unwrap();

        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|name| name == "t.x"),
            "Expected aggregate over CTE star passthrough to reach t.x, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_schema_less_cte_star_passthrough_with_join_and_alias() {
        let expr = parse(
            "WITH a AS (SELECT * FROM t1), b AS (SELECT * FROM t2) \
             SELECT SUM(b.x) AS s FROM a LEFT JOIN b ON b.id = a.id GROUP BY a.k",
        );
        let node = lineage("s", &expr, None, false).unwrap();

        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|name| name == "t2.x"),
            "Expected joined CTE star passthrough to reach t2.x, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_schema_less_chained_cte_star_passthrough() {
        let expr = parse(
            "WITH c1 AS (SELECT * FROM t), \
             c2 AS (SELECT * FROM c1), \
             c3 AS (SELECT * FROM c2) \
             SELECT c3.x FROM c3",
        );
        let node = lineage("x", &expr, None, false).unwrap();

        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();
        assert!(
            all_names.iter().any(|name| name == "t.x"),
            "Expected chained CTE star passthrough to reach t.x, got: {:?}",
            all_names
        );
    }

    #[test]
    fn test_lineage_schema_less_unqualified_star_with_multiple_sources_does_not_guess() {
        let expr = parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
        let result = lineage("x", &expr, None, false);

        assert!(
            result.is_err(),
            "Unqualified star over multiple sources should remain ambiguous, got: {:?}",
            result
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

        let node =
            lineage_with_schema("order_id", &expr, Some(&schema as &dyn Schema), None, false)
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
        schema
            .add_table("db.schema.stg_orders", &cols, None)
            .unwrap();

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

        let node =
            lineage_with_schema("name", &expr, Some(&schema as &dyn Schema), None, false).unwrap();
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

        let node =
            lineage_with_schema("order_id", &expr, Some(&schema as &dyn Schema), None, false)
                .unwrap();
        assert_eq!(node.name, "order_id");

        // Also verify the extra column works
        let extra =
            lineage_with_schema("extra", &expr, Some(&schema as &dyn Schema), None, false).unwrap();
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
            (
                "order_id".to_string(),
                crate::expressions::DataType::Unknown,
            ),
            (
                "customer_id".to_string(),
                crate::expressions::DataType::Unknown,
            ),
            ("status".to_string(), crate::expressions::DataType::Unknown),
        ];
        let pay_cols = vec![
            (
                "payment_id".to_string(),
                crate::expressions::DataType::Unknown,
            ),
            (
                "order_id".to_string(),
                crate::expressions::DataType::Unknown,
            ),
            ("amount".to_string(), crate::expressions::DataType::Unknown),
        ];
        schema.add_table("stg_orders", &order_cols, None).unwrap();
        schema.add_table("stg_payments", &pay_cols, None).unwrap();

        // order_id should trace back to stg_orders
        let node =
            lineage_with_schema("order_id", &expr, Some(&schema as &dyn Schema), None, false)
                .unwrap();
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();

        // The leaf should be "stg_orders.order_id" (not just "order_id")
        let has_table_qualified = all_names
            .iter()
            .any(|n| n.contains('.') && n.contains("order_id"));
        assert!(
            has_table_qualified,
            "Expected table-qualified leaf like 'stg_orders.order_id', got: {:?}",
            all_names
        );

        // amount should trace back to stg_payments
        let node = lineage_with_schema("amount", &expr, Some(&schema as &dyn Schema), None, false)
            .unwrap();
        let all_names: Vec<_> = node.walk().map(|n| n.name.clone()).collect();

        let has_table_qualified = all_names
            .iter()
            .any(|n| n.contains('.') && n.contains("amount"));
        assert!(
            has_table_qualified,
            "Expected table-qualified leaf like 'stg_payments.amount', got: {:?}",
            all_names
        );
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

        let node = lineage_with_schema("item_id", &expr, Some(&schema as &dyn Schema), None, false)
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
        let expr = parse("WITH MyCte AS (SELECT id AS col FROM source) SELECT * FROM MYCTE");
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
        let expr = parse(r#"WITH "MyCte" AS (SELECT id AS col FROM source) SELECT * FROM "MyCte""#);
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
        let expr = parse(r#"WITH "MyCte" AS (SELECT id AS col FROM source) SELECT * FROM "mycte""#);
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

    // -----------------------------------------------------------------------
    // Known bugs: quoted CTE case sensitivity in scope/lineage tracing paths
    // -----------------------------------------------------------------------
    //
    // expand_cte_stars correctly handles quoted vs unquoted CTE names via
    // normalize_cte_name(). However, the scope system (scope.rs add_table_to_scope)
    // and the lineage tracing path (to_node_inner) use eq_ignore_ascii_case or
    // direct string comparison for CTE name matching, ignoring the quoted status.
    //
    // sqlglot's normalize_identifiers treats quoted identifiers as case-sensitive
    // and unquoted as case-insensitive. The scope system should do the same.
    //
    // Fixing these requires changes across scope.rs and lineage.rs CTE resolution,
    // which is broader than the star expansion scope of this PR.

    #[test]
    fn test_lineage_quoted_cte_case_mismatch_non_star_known_bug() {
        // Known bug: scope.rs add_table_to_scope uses eq_ignore_ascii_case for
        // all identifiers including quoted ones, so quoted CTE "MyCte" referenced
        // as "mycte" incorrectly resolves to the CTE.
        //
        // Per SQL semantics (and sqlglot behavior), quoted identifiers are
        // case-sensitive: "mycte" should NOT match CTE "MyCte".
        //
        // This test asserts the CURRENT BUGGY behavior. When the bug is fixed,
        // this test should fail — update the assertion to match correct behavior:
        //   child.source_name should be "" (table ref), not "MyCte" (CTE ref).
        let expr = parse(r#"WITH "MyCte" AS (SELECT 1 AS col) SELECT col FROM "mycte""#);
        let node = lineage("col", &expr, None, false).unwrap();
        assert!(!node.downstream.is_empty());
        let child = &node.downstream[0];
        // BUG: "mycte" incorrectly resolves to CTE "MyCte"
        assert_eq!(
            child.source_name, "MyCte",
            "Known bug: quoted CTE case mismatch should NOT resolve, but currently does. \
             If this fails, the bug may be fixed — update to assert source_name != \"MyCte\""
        );
    }

    #[test]
    fn test_lineage_quoted_cte_case_mismatch_qualified_col_known_bug() {
        // Known bug: same as above but with qualified column reference ("mycte".col).
        // scope.rs resolves "mycte" to CTE "MyCte" case-insensitively even for
        // quoted identifiers, so "mycte".col incorrectly traces through CTE "MyCte".
        //
        // This test asserts the CURRENT BUGGY behavior. When the bug is fixed,
        // this test should fail — update to assert source_name != "MyCte".
        let expr = parse(r#"WITH "MyCte" AS (SELECT 1 AS col) SELECT "mycte".col FROM "mycte""#);
        let node = lineage("col", &expr, None, false).unwrap();
        assert!(!node.downstream.is_empty());
        let child = &node.downstream[0];
        // BUG: "mycte".col incorrectly resolves through CTE "MyCte"
        assert_eq!(
            child.source_name, "MyCte",
            "Known bug: quoted CTE case mismatch should NOT resolve, but currently does. \
             If this fails, the bug may be fixed — update to assert source_name != \"MyCte\""
        );
    }

    #[test]
    fn test_lineage_recursive_cte_terminates_at_base_case() {
        let expr = parse_dialect(
            "WITH RECURSIVE nums AS (\
             SELECT 1 AS n \
             UNION ALL \
             SELECT n + 1 FROM nums WHERE n < 5\
             ) SELECT n FROM nums",
            DialectType::DuckDB,
        );
        let node = lineage("n", &expr, Some(DialectType::DuckDB), false).unwrap();
        let names = lineage_names(&node);

        assert!(
            names.len() <= 12,
            "recursive CTE lineage should not unroll repeatedly, got {names:?}"
        );
        assert!(
            node.walk()
                .any(|child| child.source_kind == SourceKind::Cte && child.source_name == "nums"),
            "expected recursive source to be marked as a CTE, got {names:?}"
        );
    }

    #[test]
    fn test_lineage_window_partition_and_order_columns() {
        let expr = parse(
            "WITH c AS (SELECT user_id, ts FROM events) \
             SELECT ROW_NUMBER() OVER (PARTITION BY c.user_id ORDER BY c.ts) AS out FROM c",
        );
        let node = lineage("out", &expr, None, false).unwrap();

        assert_lineage_contains(&node, "events.user_id");
        assert_lineage_contains(&node, "events.ts");
    }

    #[test]
    fn test_lineage_window_aggregate_order_column() {
        let expr = parse(
            "WITH c AS (SELECT amount, d FROM txns) \
             SELECT SUM(c.amount) OVER (ORDER BY c.d) AS running FROM c",
        );
        let node = lineage("running", &expr, None, false).unwrap();

        assert_lineage_contains(&node, "txns.amount");
        assert_lineage_contains(&node, "txns.d");
    }

    #[test]
    fn test_lineage_named_window_columns() {
        let expr = parse(
            "SELECT ROW_NUMBER() OVER w AS out \
             FROM events \
             WINDOW w AS (PARTITION BY user_id ORDER BY ts)",
        );
        let node = lineage("out", &expr, None, false).unwrap();

        assert_lineage_contains(&node, "events.user_id");
        assert_lineage_contains(&node, "events.ts");
    }

    #[test]
    fn test_lineage_within_group_order_column() {
        let expr =
            parse("SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS p FROM txns");
        let node = lineage("p", &expr, None, false).unwrap();

        assert_lineage_contains(&node, "txns.amount");
    }

    #[test]
    fn test_lineage_query_wrappers_resolve_inner_select() {
        for sql in [
            "CREATE TABLE tgt AS SELECT x FROM src",
            "CREATE VIEW v AS SELECT x FROM src",
            "INSERT INTO tgt SELECT x FROM src",
        ] {
            let expr = parse(sql);
            let node = lineage("x", &expr, None, false).unwrap();
            assert_lineage_contains(&node, "src.x");
        }
    }

    #[test]
    fn test_lineage_scalar_subquery_through_cte_reaches_base_table() {
        let expr = parse(
            "WITH c AS (SELECT x FROM t) \
             SELECT (SELECT SUM(x) FROM c) AS s FROM c LIMIT 1",
        );
        let node = lineage("s", &expr, None, false).unwrap();

        assert_lineage_contains(&node, "t.x");
        assert!(
            node.walk()
                .any(|child| child.source_kind == SourceKind::Cte && child.source_name == "c"),
            "expected scalar subquery CTE hop in lineage, got {:?}",
            lineage_names(&node)
        );
    }

    #[test]
    fn test_lineage_scalar_subqueries_inside_expression_wrappers() {
        for sql in [
            "WITH c AS (SELECT a, b FROM t) \
             SELECT CASE WHEN c.a > 0 THEN c.b ELSE (SELECT MAX(z) FROM o) END AS r FROM c",
            "WITH c AS (SELECT a FROM t) \
             SELECT COALESCE(c.a, (SELECT MAX(z) FROM o)) AS r FROM c",
            "WITH c AS (SELECT a FROM t) \
             SELECT CAST((SELECT MAX(z) FROM o) AS INT) + c.a AS r FROM c",
            "WITH c AS (SELECT a FROM t) \
             SELECT CASE WHEN c.a BETWEEN 0 AND (SELECT MAX(z) FROM o) THEN c.a END AS r FROM c",
        ] {
            let expr = parse_dialect(sql, DialectType::DuckDB);
            let node = lineage("r", &expr, Some(DialectType::DuckDB), false)
                .unwrap_or_else(|error| panic!("lineage failed for {sql}: {error}"));

            assert_lineage_contains(&node, "o.z");
            assert_lineage_contains(&node, "t.a");
        }
    }

    #[test]
    fn test_lineage_nested_set_operation_inside_derived_table() {
        let expr = parse_dialect(
            "SELECT v FROM ((SELECT v FROM t1 UNION ALL SELECT v FROM t2) \
             UNION ALL SELECT v FROM t3) u",
            DialectType::DuckDB,
        );
        let node = lineage("v", &expr, Some(DialectType::DuckDB), false).unwrap();

        assert_lineage_contains(&node, "t1.v");
        assert_lineage_contains(&node, "t2.v");
        assert_lineage_contains(&node, "t3.v");
    }

    #[test]
    fn test_lineage_select_alias_reference_resolves_to_alias_source() {
        let expr = parse_dialect(
            "WITH c AS (SELECT x FROM t) SELECT c.x AS a, a + 1 AS b FROM c",
            DialectType::DuckDB,
        );
        let node = lineage("b", &expr, Some(DialectType::DuckDB), false).unwrap();

        assert_lineage_contains(&node, "t.x");
    }

    #[test]
    fn test_lineage_pivot_output_resolves_aggregation_input() {
        let expr = parse_dialect(
            "SELECT * FROM (SELECT region, q, amt FROM sales) \
             PIVOT(SUM(amt) FOR q IN ('Q1' AS q1))",
            DialectType::DuckDB,
        );
        let node = lineage("q1", &expr, Some(DialectType::DuckDB), false).unwrap();

        assert_lineage_contains(&node, "sales.amt");
    }

    #[test]
    fn test_lineage_pivot_multi_aggregate_and_alias_columns() {
        let multi = parse_dialect(
            "SELECT * FROM (SELECT category, value, price FROM t) \
             PIVOT(SUM(value) AS value_sum, MAX(price) FOR category IN ('a' AS cat_a, 'b'))",
            DialectType::DuckDB,
        );
        let value_sum =
            lineage("cat_a_value_sum", &multi, Some(DialectType::DuckDB), false).unwrap();
        assert_lineage_contains(&value_sum, "t.value");

        let max_price =
            lineage("cat_a_max(price)", &multi, Some(DialectType::DuckDB), false).unwrap();
        assert_lineage_contains(&max_price, "t.price");

        let aliased = parse_dialect(
            "SELECT * FROM (SELECT region, q, amt FROM sales) \
             PIVOT(SUM(amt) FOR q IN ('Q1')) AS p(region2, p1)",
            DialectType::DuckDB,
        );
        let region = lineage("region2", &aliased, Some(DialectType::DuckDB), false).unwrap();
        assert_lineage_contains(&region, "sales.region");

        let pivot_value = lineage("p1", &aliased, Some(DialectType::DuckDB), false).unwrap();
        assert_lineage_contains(&pivot_value, "sales.amt");
    }

    #[test]
    fn test_lineage_pivot_through_cte_resolves_aggregation_input() {
        let expr = parse_dialect(
            "WITH src AS (SELECT region, q, amt FROM sales) \
             SELECT q1 FROM src PIVOT(SUM(amt) FOR q IN ('Q1' AS q1))",
            DialectType::DuckDB,
        );
        let node = lineage("q1", &expr, Some(DialectType::DuckDB), false).unwrap();

        assert_lineage_contains(&node, "sales.amt");
    }

    #[test]
    fn test_lineage_unpivot_value_resolves_input_columns() {
        let expr = parse_dialect(
            "SELECT name, val FROM t UNPIVOT(val FOR col IN (a, b, c))",
            DialectType::DuckDB,
        );
        let node = lineage("val", &expr, Some(DialectType::DuckDB), false).unwrap();

        assert_lineage_contains(&node, "t.a");
        assert_lineage_contains(&node, "t.b");
        assert_lineage_contains(&node, "t.c");
    }

    #[test]
    fn test_lineage_unpivot_multi_value_columns_resolve_positionally() {
        let expr = parse_dialect(
            "SELECT first_half_sales, second_half_sales, semester \
             FROM produce \
             UNPIVOT((first_half_sales, second_half_sales) \
             FOR semester IN ((q1, q2) AS 'semester_1', (q3, q4) AS 'semester_2'))",
            DialectType::BigQuery,
        );

        let first = lineage(
            "first_half_sales",
            &expr,
            Some(DialectType::BigQuery),
            false,
        )
        .unwrap();
        assert_lineage_contains(&first, "produce.q1");
        assert_lineage_contains(&first, "produce.q3");

        let second = lineage(
            "second_half_sales",
            &expr,
            Some(DialectType::BigQuery),
            false,
        )
        .unwrap();
        assert_lineage_contains(&second, "produce.q2");
        assert_lineage_contains(&second, "produce.q4");
    }

    #[test]
    fn test_lineage_top_level_union_over_ctes_reaches_base_tables() {
        let expr = parse(
            "WITH a AS (SELECT x FROM t1), b AS (SELECT x FROM t2) \
             SELECT x FROM a UNION SELECT x FROM b",
        );
        let node = lineage("x", &expr, None, false).unwrap();

        assert_lineage_contains(&node, "t1.x");
        assert_lineage_contains(&node, "t2.x");
    }

    #[test]
    fn test_lineage_star_excludes_semi_join_rhs_source() {
        let expr = parse_dialect(
            "SELECT * FROM orders LEFT SEMI JOIN customers ON orders.customer_id = customers.id",
            DialectType::DuckDB,
        );
        let node = lineage("customer_id", &expr, Some(DialectType::DuckDB), false).unwrap();

        assert_lineage_contains(&node, "orders.customer_id");
    }

    // --- Comment handling tests (ported from sqlglot test_lineage.py) ---

    /// sqlglot: test_node_name_doesnt_contain_comment
    /// Comments in column expressions should not affect lineage resolution.
    /// NOTE: This test uses SELECT * from a derived table, which is a separate
    /// known limitation in polyglot-sql (star expansion in subqueries).
    #[test]
    #[ignore = "requires derived table star expansion (separate issue)"]
    fn test_node_name_doesnt_contain_comment() {
        let expr = parse("SELECT * FROM (SELECT x /* c */ FROM t1) AS t2");
        let node = lineage("x", &expr, None, false).unwrap();

        assert_eq!(node.name, "x");
        assert!(!node.downstream.is_empty());
    }

    /// A line comment between SELECT and the first column wraps the column
    /// in an Annotated node. Lineage must unwrap it to find the column name.
    /// Verify that commented and uncommented queries produce identical lineage.
    #[test]
    fn test_comment_before_first_column_in_cte() {
        let sql_with_comment = "with t as (select 1 as a) select\n  -- comment\n  a from t";
        let sql_without_comment = "with t as (select 1 as a) select a from t";

        // Without comment — baseline
        let expr_ok = parse(sql_without_comment);
        let node_ok = lineage("a", &expr_ok, None, false).expect("without comment should succeed");

        // With comment — should produce identical lineage
        let expr_comment = parse(sql_with_comment);
        let node_comment = lineage("a", &expr_comment, None, false)
            .expect("with comment before first column should succeed");

        assert_eq!(node_ok.name, node_comment.name, "node names should match");
        assert_eq!(
            node_ok.downstream_names(),
            node_comment.downstream_names(),
            "downstream lineage should be identical with or without comment"
        );
    }

    /// Block comment between SELECT and first column.
    #[test]
    fn test_block_comment_before_first_column() {
        let sql = "with t as (select 1 as a) select /* section */ a from t";
        let expr = parse(sql);
        let node = lineage("a", &expr, None, false)
            .expect("block comment before first column should succeed");
        assert_eq!(node.name, "a");
        assert!(
            !node.downstream.is_empty(),
            "should have downstream lineage"
        );
    }

    /// Comment before first column should not affect second column resolution.
    #[test]
    fn test_comment_before_first_column_second_col_ok() {
        let sql = "with t as (select 1 as a, 2 as b) select\n  -- comment\n  a, b from t";
        let expr = parse(sql);

        let node_a =
            lineage("a", &expr, None, false).expect("column a with comment should succeed");
        assert_eq!(node_a.name, "a");

        let node_b =
            lineage("b", &expr, None, false).expect("column b with comment should succeed");
        assert_eq!(node_b.name, "b");
    }

    /// Aliased column with preceding comment.
    #[test]
    fn test_comment_before_aliased_column() {
        let sql = "with t as (select 1 as x) select\n  -- renamed\n  x as y from t";
        let expr = parse(sql);
        let node =
            lineage("y", &expr, None, false).expect("aliased column with comment should succeed");
        assert_eq!(node.name, "y");
        assert!(
            !node.downstream.is_empty(),
            "aliased column should have downstream lineage"
        );
    }
}
