//! Column Lineage Tracking
//!
//! This module provides functionality to track column lineage through SQL queries,
//! building a graph of how columns flow from source tables to the result set.
//! Supports UNION/INTERSECT/EXCEPT, CTEs, derived tables, subqueries, and star expansion.
//!

use crate::dialects::DialectType;
use crate::error::{ColumnResolutionReason, ColumnResolutionTarget};
use crate::expressions::{DataType, Expression, Identifier, JoinKind, NamedWindow, Select, With};
#[cfg(feature = "generate")]
use crate::generator::Generator;
use crate::optimizer::annotate_types::annotate_types;
use crate::optimizer::qualify_schema_aware_expression;
use crate::schema::{normalize_name, Schema};
use crate::scope::{
    build_scope, find_all_in_scope, Scope, ScopeType, SourceInfo as ScopeSourceInfo, SourceKind,
};
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// The ordered output description of a query.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryOutput {
    /// Output entries in projection order.
    pub columns: Vec<OutputColumn>,
    /// Whether every entry has a stable zero-based ordinal.
    pub ordinal_complete: bool,
}

/// One entry in a query's ordered output description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputColumn {
    /// A single output column with a known name.
    Named {
        name: String,
        /// The zero-based output ordinal, when knowable.
        ordinal: Option<usize>,
    },
    /// A single output column whose database-provided name is not reliable.
    Unnamed {
        /// The zero-based output ordinal, when knowable.
        ordinal: Option<usize>,
    },
    /// An unresolved wildcard that contributes an unknown number of columns.
    Wildcard {
        /// Optional table or source qualifier from `table.*`.
        qualifier: Option<String>,
        /// The first possible output ordinal, when no earlier wildcard exists.
        #[serde(rename = "startOrdinal")]
        start_ordinal: Option<usize>,
    },
}

/// The set operator that owns an immediate lineage branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetOperator {
    Union,
    Intersect,
    Except,
}

/// Metadata describing a lineage node's position in an immediate set operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetBranch {
    /// The set operator that owns this branch.
    pub operator: SetOperator,
    /// The zero-based branch ordinal within the immediate binary operation.
    pub ordinal: usize,
    /// Whether the immediate set operation uses `ALL` semantics.
    pub all: bool,
}

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
    /// Immediate set-operation branch metadata, when this node is a branch root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub set_branch: Option<SetBranch>,
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
            set_branch: None,
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
    source_kind_for_scope_context_with_type(
        scope,
        scope.scope_type,
        source_name,
        reference_node_name,
    )
}

fn source_kind_for_scope_context_with_type(
    scope: &Scope,
    scope_type: ScopeType,
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
    match scope_type {
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

fn apply_scope_context_with_type(
    node: &mut LineageNode,
    scope: &Scope,
    scope_type: ScopeType,
    source_name: &str,
    reference_node_name: &str,
) {
    node.source_name = source_name.to_string();
    node.reference_node_name = reference_node_name.to_string();
    node.source_kind = source_kind_for_scope_context_with_type(
        scope,
        scope_type,
        source_name,
        reference_node_name,
    );
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
    let prepared = prepare_lineage_expression(sql, None, dialect, false)?;
    lineage_from_column_ref(ColumnRef::Name(column), &prepared, dialect, trim_selects)
}

/// Build the lineage graph for the column at a zero-based output ordinal.
pub fn lineage_at(
    ordinal: usize,
    sql: &Expression,
    dialect: Option<DialectType>,
    trim_selects: bool,
) -> Result<LineageNode> {
    let prepared = prepare_lineage_expression(sql, None, dialect, false)?;
    lineage_from_column_ref(ColumnRef::Index(ordinal), &prepared, dialect, trim_selects)
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
    let prepared = prepare_lineage_expression(sql, schema, dialect, true)?;
    lineage_from_column_ref(ColumnRef::Name(column), &prepared, dialect, trim_selects)
}

/// Build schema-aware lineage for the column at a zero-based output ordinal.
pub fn lineage_at_with_schema(
    ordinal: usize,
    sql: &Expression,
    schema: Option<&dyn Schema>,
    dialect: Option<DialectType>,
    trim_selects: bool,
) -> Result<LineageNode> {
    let prepared = prepare_lineage_expression(sql, schema, dialect, true)?;
    lineage_from_column_ref(ColumnRef::Index(ordinal), &prepared, dialect, trim_selects)
}

/// Return the ordered output description of a query.
pub fn output_columns(sql: &Expression, dialect: Option<DialectType>) -> Result<QueryOutput> {
    let prepared = prepare_lineage_expression(sql, None, dialect, false)?;
    query_output_from_expression(&prepared, dialect)
}

/// Return the ordered output description of a query after schema-aware expansion.
pub fn output_columns_with_schema(
    sql: &Expression,
    schema: Option<&dyn Schema>,
    dialect: Option<DialectType>,
) -> Result<QueryOutput> {
    let prepared = prepare_lineage_expression(sql, schema, dialect, true)?;
    query_output_from_expression(&prepared, dialect)
}

fn prepare_lineage_expression(
    sql: &Expression,
    schema: Option<&dyn Schema>,
    dialect: Option<DialectType>,
    schema_aware: bool,
) -> Result<Expression> {
    let normalized = lineage_normalized_expression(sql);
    let mut prepared = if schema_aware {
        if let Some(schema) = schema {
            qualify_schema_aware_expression(normalized.clone(), schema, dialect).map_err(
                |error| {
                    Error::internal(format!("Lineage qualification failed with schema: {error}"))
                },
            )?
        } else {
            normalized
        }
    } else {
        normalized
    };

    if schema_aware {
        annotate_types(&mut prepared, schema, dialect);
        expand_cte_stars(&mut prepared, schema);
    } else if has_lineage_with_clause(&prepared) {
        expand_cte_stars(&mut prepared, None);
    }

    Ok(prepared)
}

fn lineage_from_column_ref(
    column: ColumnRef<'_>,
    sql: &Expression,
    dialect: Option<DialectType>,
    trim_selects: bool,
) -> Result<LineageNode> {
    let scope = build_scope(sql);
    to_node(column, scope, dialect, "", "", "", trim_selects)
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
        Expression::HanaUpsert(upsert) => lineage_normalized_expression(&upsert.source),
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
/// resolved column lists. This also handles chained CTEs in schema-free lineage
/// and analysis paths, where full column qualification is not performed.
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

    let resolved_cte_columns = {
        let with = match query_with_mut(expr) {
            Some(with) => with,
            None => return,
        };
        let is_recursive_with = with.recursive;
        let mut resolved_cte_columns: HashMap<String, Vec<String>> = HashMap::new();

        for cte in &mut with.ctes {
            let cte_name = normalize_cte_name(&cte.alias);
            let explicit_columns = (!cte.columns.is_empty())
                .then(|| cte.columns.iter().map(|c| c.name.clone()).collect());

            // Skip recursive CTE bodies — resolving their self-references safely is
            // more complex than ordered, non-recursive CTE propagation. Inspect every
            // set-operation arm because the recursive reference normally appears in
            // the right branch after a non-recursive base case.
            if is_recursive_with && query_references_source(&cte.this, &cte_name) {
                if let Some(columns) = explicit_columns {
                    resolved_cte_columns.insert(cte_name, columns);
                }
                continue;
            }

            // Rewrite every SELECT arm, but derive the CTE's implicit output names
            // from the leftmost arm only. Explicit CTE column aliases override those
            // implicit names without preventing safe body expansion.
            let implicit_columns =
                rewrite_stars_in_query(&mut cte.this, &resolved_cte_columns, schema);
            if let Some(columns) = explicit_columns.or(implicit_columns) {
                resolved_cte_columns.insert(cte_name, columns);
            }
        }

        resolved_cte_columns
    };

    // Also expand stars in every arm of the outer query. WITH can be attached
    // directly to a root set operation, so limiting this to Expression::Select
    // would skip the entire query.
    rewrite_stars_in_query(expr, &resolved_cte_columns, schema);
}

/// Get the WITH clause attached to a query root, drilling through parentheses.
fn query_with_mut(expr: &mut Expression) -> Option<&mut With> {
    let mut current = expr;
    loop {
        match current {
            Expression::Select(select) => return select.with.as_mut(),
            Expression::Union(union) => return union.with.as_mut(),
            Expression::Intersect(intersect) => return intersect.with.as_mut(),
            Expression::Except(except) => return except.with.as_mut(),
            Expression::Paren(p) => current = &mut p.this,
            Expression::Subquery(subquery) => current = &mut subquery.this,
            _ => return None,
        }
    }
}

/// Whether any SELECT arm in a query directly references `source_name`.
///
/// This is used to identify recursive CTEs whose self-reference commonly lives
/// in a non-leftmost set-operation branch.
fn query_references_source(expr: &Expression, source_name: &str) -> bool {
    let mut stack = vec![expr];

    while let Some(current) = stack.pop() {
        match current {
            Expression::Select(select) => {
                if get_select_sources(select)
                    .iter()
                    .any(|source| source.normalized == source_name)
                {
                    return true;
                }
            }
            Expression::Union(union) => {
                stack.push(&union.right);
                stack.push(&union.left);
            }
            Expression::Intersect(intersect) => {
                stack.push(&intersect.right);
                stack.push(&intersect.left);
            }
            Expression::Except(except) => {
                stack.push(&except.right);
                stack.push(&except.left);
            }
            Expression::Paren(paren) => stack.push(&paren.this),
            Expression::Subquery(subquery) => stack.push(&subquery.this),
            _ => {}
        }
    }

    false
}

/// Rewrite stars in every SELECT arm of a query.
///
/// The traversal visits left branches first, so the first returned column list
/// remains the set operation's output column list while all later arms are still
/// rewritten independently. An explicit stack avoids adding recursion pressure
/// for deeply nested set-operation chains.
fn rewrite_stars_in_query(
    expr: &mut Expression,
    resolved_ctes: &HashMap<String, Vec<String>>,
    schema: Option<&dyn Schema>,
) -> Option<Vec<String>> {
    let mut leftmost_columns = None;
    let mut stack = vec![expr];

    while let Some(current) = stack.pop() {
        match current {
            Expression::Select(select) => {
                let columns = rewrite_stars_in_select(select, resolved_ctes, schema);
                if leftmost_columns.is_none() {
                    leftmost_columns = Some(columns);
                }
            }
            Expression::Union(union) => {
                stack.push(&mut union.right);
                stack.push(&mut union.left);
            }
            Expression::Intersect(intersect) => {
                stack.push(&mut intersect.right);
                stack.push(&mut intersect.left);
            }
            Expression::Except(except) => {
                stack.push(&mut except.right);
                stack.push(&mut except.left);
            }
            Expression::Paren(paren) => stack.push(&mut paren.this),
            Expression::Subquery(subquery) => stack.push(&mut subquery.this),
            _ => {}
        }
    }

    leftmost_columns
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct ScopeId(usize);

struct IndexedScope {
    scope: Scope,
    subquery_scopes: Vec<ScopeId>,
    derived_table_scopes: Vec<ScopeId>,
    cte_scopes: Vec<ScopeId>,
    union_scopes: Vec<ScopeId>,
}

struct LineageScopeContext {
    scopes: Vec<IndexedScope>,
    /// Usage analysis must not claim undeclared qualifiers as physical tables.
    conservative: bool,
}

/// Reuse the lineage resolver for analysis of non-output column occurrences.
/// The caller selects the lexical owner and checks ambiguous/open sources first.
/// This keeps CTE, derived-table, virtual-source and set-operation tracing shared
/// with projection lineage, without manufacturing SELECT projections.
#[cfg(feature = "generate")]
pub(crate) struct ScopedLineage {
    context: LineageScopeContext,
    root: ScopeId,
    ctes: Vec<ScopeId>,
    dialect: Option<DialectType>,
}

#[cfg(feature = "generate")]
impl ScopedLineage {
    pub(crate) fn new(scope: Scope, inherited_ctes: &[Scope], dialect: DialectType) -> Self {
        let visible_ctes = scope.cte_sources.clone();
        let (mut context, root) = LineageScopeContext::from_scope(scope);
        context.conservative = true;
        let mut ctes = context.indexed(root).cte_scopes.clone();
        for cte_scope in inherited_ctes {
            if let Expression::Cte(cte) = &cte_scope.expression {
                if visible_ctes
                    .get(&cte.alias.name)
                    .is_some_and(|source| source.expression == cte_scope.expression)
                    && !ctes
                        .iter()
                        .any(|id| context.scope(*id).expression == cte_scope.expression)
                {
                    ctes.push(context.insert_scope(cte_scope.clone()));
                }
            }
        }
        Self {
            context,
            root,
            ctes,
            dialect: Some(dialect),
        }
    }

    pub(crate) fn column(&self, source: &str, column: &str) -> LineageNode {
        let mut node = LineageNode::new(
            column,
            Expression::qualified_column(source, column),
            Expression::Null(crate::expressions::Null),
        );
        resolve_qualified_column(
            &mut node,
            &self.context,
            self.root,
            self.dialect,
            source,
            column,
            column,
            false,
            &self.ctes,
            0,
        );
        node
    }

    pub(crate) fn output(&self, ordinal: usize) -> Result<LineageNode> {
        to_node_inner(
            ColumnRef::Index(ordinal),
            &self.context,
            self.root,
            self.dialect,
            "",
            "",
            "",
            false,
            &self.ctes,
            0,
        )
    }

    pub(crate) fn output_names(&self) -> Vec<String> {
        crate::ast_transforms::get_output_column_names_for_dialect(
            effective_scope_expression(&self.context.scope(self.root).expression),
            self.dialect,
        )
    }

    pub(crate) fn subquery_output(&self, index: usize) -> Result<LineageNode> {
        let scope_id = *self
            .context
            .indexed(self.root)
            .subquery_scopes
            .get(index)
            .ok_or_else(|| Error::internal("missing scalar subquery scope"))?;
        to_node_inner(
            ColumnRef::Index(0),
            &self.context,
            scope_id,
            self.dialect,
            "",
            "",
            "",
            false,
            &self.ctes,
            0,
        )
    }
}

impl LineageScopeContext {
    fn from_scope(scope: Scope) -> (Self, ScopeId) {
        let mut context = Self {
            scopes: Vec::new(),
            conservative: false,
        };
        let root = context.insert_scope(scope);
        (context, root)
    }

    fn insert_scope(&mut self, mut scope: Scope) -> ScopeId {
        let subquery_scopes = std::mem::take(&mut scope.subquery_scopes)
            .into_iter()
            .map(|child| self.insert_scope(child))
            .collect();
        let derived_table_scopes = std::mem::take(&mut scope.derived_table_scopes)
            .into_iter()
            .map(|child| self.insert_scope(child))
            .collect();
        let cte_scopes = std::mem::take(&mut scope.cte_scopes)
            .into_iter()
            .map(|child| self.insert_scope(child))
            .collect();
        let union_scopes = std::mem::take(&mut scope.union_scopes)
            .into_iter()
            .map(|child| self.insert_scope(child))
            .collect();

        let id = ScopeId(self.scopes.len());
        self.scopes.push(IndexedScope {
            scope,
            subquery_scopes,
            derived_table_scopes,
            cte_scopes,
            union_scopes,
        });
        id
    }

    fn indexed(&self, id: ScopeId) -> &IndexedScope {
        &self.scopes[id.0]
    }

    fn scope(&self, id: ScopeId) -> &Scope {
        &self.indexed(id).scope
    }
}

/// Recursively build a lineage node for a column in a scope.
fn to_node(
    column: ColumnRef<'_>,
    scope: Scope,
    dialect: Option<DialectType>,
    scope_name: &str,
    source_name: &str,
    reference_node_name: &str,
    trim_selects: bool,
) -> Result<LineageNode> {
    let (context, scope_id) = LineageScopeContext::from_scope(scope);
    to_node_inner(
        column,
        &context,
        scope_id,
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
    context: &LineageScopeContext,
    scope_id: ScopeId,
    dialect: Option<DialectType>,
    scope_name: &str,
    source_name: &str,
    reference_node_name: &str,
    trim_selects: bool,
    ancestor_cte_scopes: &[ScopeId],
    depth: usize,
) -> Result<LineageNode> {
    if depth > MAX_LINEAGE_DEPTH {
        return Err(Error::internal(format!(
            "lineage recursion depth exceeded (>{MAX_LINEAGE_DEPTH}) — possible circular CTE reference for scope '{scope_name}'"
        )));
    }
    let scope = context.scope(scope_id);
    let scope_expr = &scope.expression;

    // Build combined CTE scopes: current scope's cte_scopes + ancestors
    let mut all_cte_scopes = context.indexed(scope_id).cte_scopes.clone();
    all_cte_scopes.extend_from_slice(ancestor_cte_scopes);
    let descendant_cte_scopes = descendant_cte_scope_ids(&all_cte_scopes, scope_id);

    // 0. Unwrap CTE scope — CTE scope expressions are Expression::Cte(...)
    //    but we need the inner query (SELECT/UNION) for column lookup.
    let effective_expr = effective_scope_expression(scope_expr);
    // A CTE's explicit output names replace its projection names by ordinal.
    // Resolve that boundary before unwrapping, including unnamed expressions.
    let alias_ordinal = match (scope_expr, &column) {
        (Expression::Cte(cte), ColumnRef::Name(name)) if !cte.columns.is_empty() => {
            cte.columns.iter().position(|alias| {
                normalize_column_name(&alias.name, dialect) == normalize_column_name(name, dialect)
            })
        }
        _ => None,
    };
    let lookup_column = alias_ordinal.map(ColumnRef::Index);
    let lookup_column = lookup_column.as_ref().unwrap_or(&column);

    // 1. Set operations (UNION / INTERSECT / EXCEPT)
    if matches!(
        effective_expr,
        Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_)
    ) {
        let mut node = handle_set_operation(
            lookup_column,
            context,
            scope_id,
            effective_expr,
            matches!(scope_expr, Expression::Cte(_)).then_some(ScopeType::Root),
            dialect,
            scope_name,
            source_name,
            reference_node_name,
            trim_selects,
            &descendant_cte_scopes,
            depth,
        )?;
        if alias_ordinal.is_some() {
            if let ColumnRef::Name(name) = &column {
                node.name = (*name).to_string();
            }
        }
        return Ok(node);
    }

    // 2. Find the select expression for this column
    let select_expr = find_select_expr(effective_expr, lookup_column, dialect)?;
    let column_name = resolve_column_name(&column, &select_expr);

    // 3. Trim source if requested
    // Internal fact consumers need physical leaf sources, not a copy of the
    // entire SELECT for every output. Public lineage retains its source ASTs.
    let node_source = if context.conservative {
        Expression::Null(crate::expressions::Null)
    } else if trim_selects {
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
        for &sq_scope_id in &context.indexed(scope_id).subquery_scopes {
            if context.scope(sq_scope_id).expression == *query {
                if let Ok(child) = to_node_inner(
                    ColumnRef::Index(0),
                    context,
                    sq_scope_id,
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
                context,
                scope_id,
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
                            context,
                            scope_id,
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
                            context,
                            scope_id,
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
                context,
                scope_id,
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

fn descendant_cte_scope_ids(all_cte_scopes: &[ScopeId], current_scope: ScopeId) -> Vec<ScopeId> {
    all_cte_scopes
        .iter()
        .copied()
        .filter(|scope| *scope != current_scope)
        .collect()
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

fn set_branch_metadata(expression: &Expression, ordinal: usize) -> SetBranch {
    match expression {
        Expression::Union(union) => SetBranch {
            operator: SetOperator::Union,
            ordinal,
            all: union.all,
        },
        Expression::Intersect(intersect) => SetBranch {
            operator: SetOperator::Intersect,
            ordinal,
            all: intersect.all,
        },
        Expression::Except(except) => SetBranch {
            operator: SetOperator::Except,
            ordinal,
            all: except.all,
        },
        _ => unreachable!("set-operation metadata requires a set operation"),
    }
}

fn handle_set_operation(
    column: &ColumnRef<'_>,
    context: &LineageScopeContext,
    scope_id: ScopeId,
    scope_expr: &Expression,
    scope_type_override: Option<ScopeType>,
    dialect: Option<DialectType>,
    scope_name: &str,
    source_name: &str,
    reference_node_name: &str,
    trim_selects: bool,
    ancestor_cte_scopes: &[ScopeId],
    depth: usize,
) -> Result<LineageNode> {
    let scope = context.scope(scope_id);
    let trace_wildcard_by_name =
        matches!(column, ColumnRef::Name(name) if normalize_column_name(name, dialect) == "*");

    let aligned_layout = match crate::set_operation::set_operation_layout(scope_expr, dialect) {
        Ok(layout) => layout,
        Err(error) if error.is_indeterminate() => None,
        Err(error) => return Err(Error::invalid_input(error.to_string())),
    };

    // Determine column index
    let col_index = match column {
        ColumnRef::Name(_) if trace_wildcard_by_name => 0,
        ColumnRef::Name(name) => column_to_index(scope_expr, name, dialect)?,
        ColumnRef::Index(i) => *i,
    };

    if aligned_layout
        .as_ref()
        .is_some_and(|layout| col_index >= layout.outputs.len())
    {
        return Err(ordinal_resolution_error(
            col_index,
            ColumnResolutionReason::NotFound,
        ));
    }

    let col_name = match column {
        ColumnRef::Name(name) => name.to_string(),
        ColumnRef::Index(_) => aligned_layout
            .as_ref()
            .and_then(|layout| layout.outputs.get(col_index))
            .map(|output| output.identifier.name.clone())
            .unwrap_or_else(|| format!("_{col_index}")),
    };

    let mut node = LineageNode::new(&col_name, scope_expr.clone(), scope_expr.clone());
    if let Some(scope_type) = scope_type_override {
        apply_scope_context_with_type(
            &mut node,
            scope,
            scope_type,
            source_name,
            reference_node_name,
        );
    } else {
        apply_scope_context(&mut node, scope, source_name, reference_node_name);
    }

    let mut resolution_failure = None;

    // Recurse into each set-operation branch. Resolution failures are branch-local,
    // but genuine parser/internal errors must not be silently discarded.
    for (branch_ordinal, &branch_scope_id) in
        context.indexed(scope_id).union_scopes.iter().enumerate()
    {
        let branch_column = if trace_wildcard_by_name {
            match column {
                ColumnRef::Name(name) => ColumnRef::Name(name),
                ColumnRef::Index(_) => unreachable!("wildcard tracing is name-based"),
            }
        } else if let Some(layout) = &aligned_layout {
            let output = &layout.outputs[col_index];
            let branch_index = if branch_ordinal == 0 {
                output.left_ordinal
            } else {
                output.right_ordinal
            };
            let Some(branch_index) = branch_index else {
                // This branch contributes a dialect-inserted NULL for the
                // result slot, so it has no source expression to trace.
                continue;
            };
            ColumnRef::Index(branch_index)
        } else {
            ColumnRef::Index(col_index)
        };

        match to_node_inner(
            branch_column,
            context,
            branch_scope_id,
            dialect,
            scope_name,
            "",
            "",
            trim_selects,
            ancestor_cte_scopes,
            depth + 1,
        ) {
            Ok(mut child) => {
                child.set_branch = Some(set_branch_metadata(scope_expr, branch_ordinal));
                node.downstream.push(child);
            }
            Err(Error::ColumnResolution { reason, .. }) => {
                resolution_failure = Some(merge_resolution_reason(resolution_failure, reason));
            }
            Err(error) => return Err(error),
        }
    }

    if node.downstream.is_empty() {
        if let Some(reason) = resolution_failure {
            let target = match column {
                ColumnRef::Name(name) => ColumnResolutionTarget::Name {
                    name: name.to_string(),
                },
                ColumnRef::Index(ordinal) => ColumnResolutionTarget::Ordinal { ordinal: *ordinal },
            };
            return Err(column_resolution_error(target, reason));
        }
    }

    Ok(node)
}

fn merge_resolution_reason(
    current: Option<ColumnResolutionReason>,
    candidate: ColumnResolutionReason,
) -> ColumnResolutionReason {
    match (current, candidate) {
        (Some(ColumnResolutionReason::Ambiguous), _) | (_, ColumnResolutionReason::Ambiguous) => {
            ColumnResolutionReason::Ambiguous
        }
        (Some(ColumnResolutionReason::Indeterminate), _)
        | (_, ColumnResolutionReason::Indeterminate) => ColumnResolutionReason::Indeterminate,
        _ => ColumnResolutionReason::NotFound,
    }
}

// ---------------------------------------------------------------------------
// Column resolution helpers
// ---------------------------------------------------------------------------

fn resolve_qualified_column(
    node: &mut LineageNode,
    context: &LineageScopeContext,
    scope_id: ScopeId,
    dialect: Option<DialectType>,
    table: &str,
    col_name: &str,
    parent_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[ScopeId],
    depth: usize,
) {
    let scope = context.scope(scope_id);
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
                    context,
                    scope_id,
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
                    context,
                    scope_id,
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
            |scope_id| matches!(&context.scope(*scope_id).expression, Expression::Cte(cte) if cte.alias.name == effective_table),
        );
    if is_cte {
        if let Some(child_scope_id) =
            find_child_scope_in(context, all_cte_scopes, scope_id, effective_table)
        {
            if let Ok(child) = to_node_inner(
                ColumnRef::Name(col_name),
                context,
                child_scope_id,
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
            if let Some(child_scope_id) = find_child_scope(context, scope_id, table) {
                if let Ok(child) = to_node_inner(
                    ColumnRef::Name(col_name),
                    context,
                    child_scope_id,
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
                    context,
                    scope_id,
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
    let mut child = make_table_column_node(table, col_name);
    if context.conservative {
        child.source_kind = SourceKind::Unknown;
    }
    node.downstream.push(child);
}

fn attach_pivot_dependencies(
    node: &mut LineageNode,
    context: &LineageScopeContext,
    scope_id: ScopeId,
    dialect: Option<DialectType>,
    pivot: &crate::expressions::Pivot,
    col_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[ScopeId],
    depth: usize,
) -> bool {
    if pivot.unpivot {
        return false;
    }

    let scope = context.scope(scope_id);
    let mapping = pivot_lineage_column_mapping(pivot, scope, dialect);
    let Some(input_columns) = mapping.get(&normalize_column_name(col_name, dialect)) else {
        if pivot_implicit_source_column(pivot, col_name) {
            let col_ref = SimpleColumnRef {
                table: None,
                column: col_name.to_string(),
            };
            attach_pivot_input_column(
                node,
                context,
                scope_id,
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
            context,
            scope_id,
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
    context: &LineageScopeContext,
    scope_id: ScopeId,
    dialect: Option<DialectType>,
    unpivot: &crate::expressions::Unpivot,
    col_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[ScopeId],
    depth: usize,
) -> bool {
    let mapping = unpivot_column_mapping(unpivot, dialect);
    let Some(input_columns) = mapping.get(&normalize_column_name(col_name, dialect)) else {
        return false;
    };

    for col_ref in input_columns {
        attach_pivot_input_column(
            node,
            context,
            scope_id,
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
    context: &LineageScopeContext,
    scope_id: ScopeId,
    dialect: Option<DialectType>,
    source_expr: &Expression,
    col_ref: &SimpleColumnRef,
    trim_selects: bool,
    all_cte_scopes: &[ScopeId],
    depth: usize,
) {
    let scope = context.scope(scope_id);
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
                    context,
                    scope_id,
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
            let Some(source_scope_id) =
                find_derived_scope_for_query(context, scope_id, &subquery.this)
            else {
                return;
            };
            let child = if let Some(table) = &col_ref.table {
                let mut child_node = LineageNode::new(
                    &col_ref.column,
                    subquery.this.clone(),
                    subquery.this.clone(),
                );
                resolve_qualified_column(
                    &mut child_node,
                    context,
                    source_scope_id,
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
                    context,
                    source_scope_id,
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
            context,
            scope_id,
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
                    context,
                    scope_id,
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
    context: &LineageScopeContext,
    scope_id: ScopeId,
    dialect: Option<DialectType>,
    col_name: &str,
    parent_name: &str,
    trim_selects: bool,
    all_cte_scopes: &[ScopeId],
    depth: usize,
) {
    let scope = context.scope(scope_id);
    // Try to find which source this column belongs to.
    // Build the source list from the actual FROM/JOIN clauses to avoid
    // mixing in CTE definitions that are in scope but not referenced.
    let from_source_names = source_names_from_from_join(scope);

    if let Some(tbl) = unique_virtual_source_for_column(scope, &from_source_names, col_name) {
        resolve_qualified_column(
            node,
            context,
            scope_id,
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
            context,
            scope_id,
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

fn unnest_output_types(unnest: &crate::expressions::UnnestFunc) -> Vec<DataType> {
    let element_type = |expression: &Expression| match expression.inferred_type() {
        Some(DataType::Array { element_type, .. }) => (**element_type).clone(),
        _ => DataType::Unknown,
    };

    let mut types = vec![unnest
        .inferred_type
        .clone()
        .unwrap_or_else(|| element_type(&unnest.this))];
    types.extend(unnest.expressions.iter().map(element_type));
    if unnest.with_ordinality || unnest.offset_alias.is_some() {
        types.push(DataType::BigInt { length: None });
    }
    types
}

fn virtual_source_column_type(source_info: &ScopeSourceInfo, column: &str) -> Option<DataType> {
    let find_type = |names: Vec<String>, types: Vec<DataType>| {
        names
            .iter()
            .position(|name| name.eq_ignore_ascii_case(column))
            .and_then(|index| types.get(index).cloned())
    };

    match &source_info.expression {
        Expression::Unnest(unnest) => find_type(
            unnest_output_columns(unnest).collect(),
            unnest_output_types(unnest),
        ),
        Expression::Alias(alias) => match &alias.this {
            Expression::Unnest(unnest) => find_type(
                alias_output_columns(alias).collect(),
                unnest_output_types(unnest),
            ),
            _ => None,
        },
        Expression::Lateral(lateral) => match lateral.this.as_ref() {
            Expression::Unnest(unnest) => find_type(
                lateral_output_columns(lateral).collect(),
                unnest_output_types(unnest),
            ),
            _ => None,
        },
        _ => None,
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
    context: &LineageScopeContext,
    scope_id: ScopeId,
    dialect: Option<DialectType>,
    source_alias: &str,
    source_expr: &Expression,
    trim_selects: bool,
    all_cte_scopes: &[ScopeId],
    depth: usize,
) {
    let scope = context.scope(scope_id);
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
                context,
                scope_id,
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
                    context,
                    scope_id,
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

#[derive(Debug, Clone)]
struct OutputLayoutEntry {
    column: OutputColumn,
    projection_index: usize,
}

fn query_output_from_expression(
    expression: &Expression,
    dialect: Option<DialectType>,
) -> Result<QueryOutput> {
    match crate::set_operation::set_operation_layout(expression, dialect) {
        Ok(Some(layout)) => {
            return Ok(QueryOutput {
                columns: layout
                    .outputs
                    .into_iter()
                    .enumerate()
                    .map(|(ordinal, output)| OutputColumn::Named {
                        name: output.identifier.name,
                        ordinal: Some(ordinal),
                    })
                    .collect(),
                ordinal_complete: true,
            })
        }
        Ok(None) => {}
        Err(error) if error.is_indeterminate() => {}
        Err(error) => return Err(Error::invalid_input(error.to_string())),
    }

    let select = leftmost_output_select(expression).ok_or_else(|| {
        Error::invalid_input("output_columns requires a SELECT or set-operation query")
    })?;
    let entries = output_layout(select);
    let ordinal_complete = !entries
        .iter()
        .any(|entry| matches!(entry.column, OutputColumn::Wildcard { .. }));

    Ok(QueryOutput {
        columns: entries.into_iter().map(|entry| entry.column).collect(),
        ordinal_complete,
    })
}

fn leftmost_output_select(expression: &Expression) -> Option<&Select> {
    match expression {
        Expression::Select(select) => Some(select),
        Expression::Union(set_op) => leftmost_output_select(&set_op.left),
        Expression::Intersect(set_op) => leftmost_output_select(&set_op.left),
        Expression::Except(set_op) => leftmost_output_select(&set_op.left),
        Expression::Subquery(subquery) => leftmost_output_select(&subquery.this),
        Expression::Cte(cte) => leftmost_output_select(&cte.this),
        Expression::Paren(paren) => leftmost_output_select(&paren.this),
        _ => None,
    }
}

fn output_layout(select: &Select) -> Vec<OutputLayoutEntry> {
    let mut entries = Vec::new();
    let mut next_ordinal = Some(0usize);

    for (projection_index, projection) in select.expressions.iter().enumerate() {
        let projection = unwrap_output_annotation(projection);

        if let Some(qualifier) = output_wildcard_qualifier(projection) {
            entries.push(OutputLayoutEntry {
                column: OutputColumn::Wildcard {
                    qualifier,
                    start_ordinal: next_ordinal,
                },
                projection_index,
            });
            next_ordinal = None;
            continue;
        }

        if let Expression::Aliases(aliases) = projection {
            if !aliases.expressions.is_empty() {
                for alias in &aliases.expressions {
                    let ordinal = take_output_ordinal(&mut next_ordinal);
                    let column = get_alias_or_name(alias)
                        .map(|name| OutputColumn::Named { name, ordinal })
                        .unwrap_or(OutputColumn::Unnamed { ordinal });
                    entries.push(OutputLayoutEntry {
                        column,
                        projection_index,
                    });
                }
                continue;
            }
        }

        let ordinal = take_output_ordinal(&mut next_ordinal);
        let column = get_alias_or_name(projection)
            .map(|name| OutputColumn::Named { name, ordinal })
            .unwrap_or(OutputColumn::Unnamed { ordinal });
        entries.push(OutputLayoutEntry {
            column,
            projection_index,
        });
    }

    entries
}

fn take_output_ordinal(next_ordinal: &mut Option<usize>) -> Option<usize> {
    let ordinal = *next_ordinal;
    if let Some(value) = ordinal {
        *next_ordinal = Some(value + 1);
    }
    ordinal
}

fn unwrap_output_annotation(mut expression: &Expression) -> &Expression {
    while let Expression::Annotated(annotated) = expression {
        expression = &annotated.this;
    }
    expression
}

/// Return `Some(qualifier)` for a wildcard, where the inner option is the qualifier.
fn output_wildcard_qualifier(expression: &Expression) -> Option<Option<String>> {
    match expression {
        Expression::Star(star) => Some(star.table.as_ref().map(|table| table.name.clone())),
        Expression::Column(column) if column.name.name == "*" => {
            Some(column.table.as_ref().map(|table| table.name.clone()))
        }
        _ => None,
    }
}

fn column_resolution_error(
    target: ColumnResolutionTarget,
    reason: ColumnResolutionReason,
) -> Error {
    Error::column_resolution(target, reason)
}

fn name_resolution_error(name: &str, reason: ColumnResolutionReason) -> Error {
    column_resolution_error(
        ColumnResolutionTarget::Name {
            name: name.to_string(),
        },
        reason,
    )
}

fn ordinal_resolution_error(ordinal: usize, reason: ColumnResolutionReason) -> Error {
    column_resolution_error(ColumnResolutionTarget::Ordinal { ordinal }, reason)
}

fn find_select_expr_by_name(
    select: &Select,
    name: &str,
    dialect: Option<DialectType>,
) -> Result<Expression> {
    let normalized_name = normalize_column_name(name, dialect);
    let layout = output_layout(select);
    let mut matches = Vec::new();

    for entry in &layout {
        let is_match = match &entry.column {
            OutputColumn::Named {
                name: output_name, ..
            } => normalize_column_name(output_name, dialect) == normalized_name,
            OutputColumn::Wildcard { .. } => normalized_name == "*",
            OutputColumn::Unnamed { .. } => false,
        };
        if is_match {
            matches.push(entry.projection_index);
        }
    }
    match matches.as_slice() {
        [projection_index] => return Ok(select.expressions[*projection_index].clone()),
        [_, ..] => {
            return Err(name_resolution_error(
                name,
                ColumnResolutionReason::Ambiguous,
            ))
        }
        [] => {}
    }

    if let Some(expression) = synthesize_star_passthrough_expr(select, name) {
        return Ok(expression);
    }

    let reason = if layout
        .iter()
        .any(|entry| matches!(entry.column, OutputColumn::Wildcard { .. }))
    {
        ColumnResolutionReason::Indeterminate
    } else {
        ColumnResolutionReason::NotFound
    };
    Err(name_resolution_error(name, reason))
}

fn find_select_expr_by_ordinal(select: &Select, ordinal: usize) -> Result<Expression> {
    let layout = output_layout(select);

    for entry in &layout {
        match &entry.column {
            OutputColumn::Named {
                ordinal: Some(candidate),
                ..
            }
            | OutputColumn::Unnamed {
                ordinal: Some(candidate),
            } if *candidate == ordinal => {
                return Ok(select.expressions[entry.projection_index].clone())
            }
            OutputColumn::Wildcard { start_ordinal, .. } => {
                if match start_ordinal {
                    Some(start) => ordinal >= *start,
                    None => true,
                } {
                    return Err(ordinal_resolution_error(
                        ordinal,
                        ColumnResolutionReason::Indeterminate,
                    ));
                }
            }
            _ => {}
        }
    }

    Err(ordinal_resolution_error(
        ordinal,
        ColumnResolutionReason::NotFound,
    ))
}

fn output_name_to_ordinal(
    expression: &Expression,
    name: &str,
    dialect: Option<DialectType>,
) -> Result<usize> {
    match crate::set_operation::set_operation_layout(expression, dialect) {
        Ok(Some(layout)) => {
            let lookup = Identifier::new(name);
            let lookup_key = crate::set_operation::identifier_key(&lookup, dialect);
            let matches: Vec<_> = layout
                .outputs
                .iter()
                .enumerate()
                .filter(|(_, output)| {
                    (output.identifier.quoted && output.identifier.name == name)
                        || crate::set_operation::identifier_key(&output.identifier, dialect)
                            == lookup_key
                })
                .map(|(ordinal, _)| ordinal)
                .collect();

            return match matches.as_slice() {
                [ordinal] => Ok(*ordinal),
                [_, ..] => Err(name_resolution_error(
                    name,
                    ColumnResolutionReason::Ambiguous,
                )),
                [] => Err(name_resolution_error(
                    name,
                    ColumnResolutionReason::NotFound,
                )),
            };
        }
        Ok(None) => {}
        Err(error) if error.is_indeterminate() => {}
        Err(error) => return Err(Error::invalid_input(error.to_string())),
    }

    let select = leftmost_output_select(expression).ok_or_else(|| {
        Error::invalid_input("column resolution requires a SELECT or set-operation query")
    })?;
    let normalized_name = normalize_column_name(name, dialect);
    let layout = output_layout(select);
    let mut matches = Vec::new();

    for entry in &layout {
        if let OutputColumn::Named {
            name: output_name,
            ordinal,
        } = &entry.column
        {
            if normalize_column_name(output_name, dialect) == normalized_name {
                matches.push(*ordinal);
            }
        }
    }

    if matches.len() > 1 {
        return Err(name_resolution_error(
            name,
            ColumnResolutionReason::Ambiguous,
        ));
    }
    if let Some(ordinal) = matches.into_iter().next() {
        return ordinal
            .ok_or_else(|| name_resolution_error(name, ColumnResolutionReason::Indeterminate));
    }

    let reason = if layout
        .iter()
        .any(|entry| matches!(entry.column, OutputColumn::Wildcard { .. }))
    {
        ColumnResolutionReason::Indeterminate
    } else {
        ColumnResolutionReason::NotFound
    };
    Err(name_resolution_error(name, reason))
}

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
            ColumnRef::Name(name) => find_select_expr_by_name(select, name, dialect),
            ColumnRef::Index(ordinal) => find_select_expr_by_ordinal(select, *ordinal),
        }
    } else {
        Err(Error::invalid_input(
            "column resolution requires a SELECT expression",
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
    output_name_to_ordinal(set_op_expr, name, dialect)
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
fn find_child_scope(
    context: &LineageScopeContext,
    scope_id: ScopeId,
    source_name: &str,
) -> Option<ScopeId> {
    let indexed = context.indexed(scope_id);
    let scope = &indexed.scope;

    // Check CTE scopes
    if scope.cte_sources.contains_key(source_name) {
        for &cte_scope_id in &indexed.cte_scopes {
            let cte_scope = context.scope(cte_scope_id);
            if let Expression::Cte(cte) = &cte_scope.expression {
                if cte.alias.name == source_name {
                    return Some(cte_scope_id);
                }
            }
        }
    }

    // Check derived table scopes
    if let Some(source_info) = scope.sources.get(source_name) {
        if source_info.is_scope && !scope.cte_sources.contains_key(source_name) {
            if let Some(query) = derived_source_query(&source_info.expression) {
                for &dt_scope_id in &indexed.derived_table_scopes {
                    let dt_scope = context.scope(dt_scope_id);
                    if expressions_equivalent_after_wrappers(&dt_scope.expression, query) {
                        return Some(dt_scope_id);
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
fn find_child_scope_in(
    context: &LineageScopeContext,
    all_cte_scopes: &[ScopeId],
    scope_id: ScopeId,
    source_name: &str,
) -> Option<ScopeId> {
    let indexed = context.indexed(scope_id);
    let scope = &indexed.scope;

    // First try the scope's own cte_scopes
    for &cte_scope_id in &indexed.cte_scopes {
        let cte_scope = context.scope(cte_scope_id);
        if let Expression::Cte(cte) = &cte_scope.expression {
            if cte.alias.name == source_name {
                return Some(cte_scope_id);
            }
        }
    }

    // Then search through all ancestor CTE scopes
    for &cte_scope_id in all_cte_scopes {
        let cte_scope = context.scope(cte_scope_id);
        if let Expression::Cte(cte) = &cte_scope.expression {
            if cte.alias.name == source_name {
                return Some(cte_scope_id);
            }
        }
    }

    // Fall back to derived table scopes
    if let Some(source_info) = scope.sources.get(source_name) {
        if source_info.is_scope {
            if let Some(query) = derived_source_query(&source_info.expression) {
                for &dt_scope_id in &indexed.derived_table_scopes {
                    let dt_scope = context.scope(dt_scope_id);
                    if expressions_equivalent_after_wrappers(&dt_scope.expression, query) {
                        return Some(dt_scope_id);
                    }
                }
            }
        }
    }

    None
}

fn find_derived_scope_for_query(
    context: &LineageScopeContext,
    scope_id: ScopeId,
    query: &Expression,
) -> Option<ScopeId> {
    context
        .indexed(scope_id)
        .derived_table_scopes
        .iter()
        .copied()
        .find(|derived_scope_id| {
            expressions_equivalent_after_wrappers(
                &context.scope(*derived_scope_id).expression,
                query,
            )
        })
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
    let inferred_type = (source_info.kind == SourceKind::Virtual)
        .then(|| virtual_source_column_type(source_info, column))
        .flatten();
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
            inferred_type,
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

    const ISSUE_368_SQL: &str = "with
base as (
  select 1 as col_a
),
literal_branch as (
  select 2 as col_a
),
unioned as (
  select * from base
  union all
  select * from literal_branch
)
select col_a from unioned";

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
            set_branch: None,
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
    fn test_lineage_with_schema_natural_join_merged_column() {
        let query = "SELECT shared_key AS output_key FROM source_table \
                     NATURAL JOIN (SELECT shared_key FROM source_table) derived";
        let dialect = Dialect::get(DialectType::DuckDB);
        let expr = dialect
            .parse(query)
            .unwrap()
            .into_iter()
            .next()
            .expect("expected one expression");

        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "source_table",
                &[(
                    "shared_key".into(),
                    DataType::VarChar {
                        length: None,
                        parenthesized_length: false,
                    },
                )],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "output_key",
            &expr,
            Some(&schema),
            Some(DialectType::DuckDB),
            false,
        )
        .expect("lineage_with_schema should resolve a NATURAL JOIN merged column");

        let terminal_nodes: Vec<_> = node
            .walk()
            .filter(|candidate| candidate.downstream.is_empty())
            .collect();
        assert_eq!(
            terminal_nodes.len(),
            2,
            "both NATURAL JOIN inputs should remain visible: {node:#?}"
        );
        assert!(terminal_nodes.iter().all(|candidate| {
            candidate.source_kind == SourceKind::Table
                && candidate.source_name == "source_table"
                && candidate.name.ends_with(".shared_key")
        }));
    }

    #[test]
    fn test_lineage_with_schema_propagates_type_through_anonymous_derived_table() {
        let expr = parse_dialect(
            "SELECT source_value FROM (SELECT source_value FROM source_table)",
            DialectType::DuckDB,
        );
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        let expected = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };
        schema
            .add_table(
                "source_table",
                &[("source_value".into(), expected.clone())],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "source_value",
            &expr,
            Some(&schema),
            Some(DialectType::DuckDB),
            false,
        )
        .expect("lineage_with_schema should resolve an anonymous derived table");

        assert_eq!(node.expression.inferred_type(), Some(&expected));
        let terminal_nodes: Vec<_> = node
            .walk()
            .filter(|candidate| candidate.downstream.is_empty())
            .collect();
        assert_eq!(terminal_nodes.len(), 1, "{node:#?}");
        assert_eq!(terminal_nodes[0].source_kind, SourceKind::Table);
        assert_eq!(terminal_nodes[0].source_name, "source_table");
        assert_eq!(terminal_nodes[0].name, "source_table.source_value");
    }

    #[test]
    fn test_issue_412_duckdb_date_trunc_preserves_timestamp_lineage_type() {
        let expr = parse_dialect(
            "SELECT DATE_TRUNC('month', event_timestamp) AS truncated_timestamp \
             FROM source_table",
            DialectType::DuckDB,
        );
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        let timestamp = DataType::Timestamp {
            precision: None,
            timezone: false,
        };
        schema
            .add_table(
                "source_table",
                &[("event_timestamp".into(), timestamp.clone())],
                None,
            )
            .expect("schema setup");

        let node =
            lineage_at_with_schema(0, &expr, Some(&schema), Some(DialectType::DuckDB), false)
                .expect("schema-aware lineage");

        assert_eq!(node.expression.inferred_type(), Some(&timestamp));
        let Expression::Alias(alias) = &node.expression else {
            panic!("expected aliased DATE_TRUNC projection");
        };
        assert_eq!(alias.this.inferred_type(), Some(&timestamp));
        let Expression::Function(function) = &alias.this else {
            panic!("expected generic DATE_TRUNC function");
        };
        assert_eq!(function.args[1].inferred_type(), Some(&timestamp));
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
    fn test_lineage_with_schema_propagates_unnest_element_type() {
        let expr = parse_dialect(
            "SELECT u.tag FROM events AS e, UNNEST(e.tags) AS u(tag)",
            DialectType::DuckDB,
        );
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "events",
                &[(
                    "tags".into(),
                    DataType::Array {
                        element_type: Box::new(DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        }),
                        dimension: None,
                    },
                )],
                None,
            )
            .expect("schema setup");

        let node = lineage_with_schema(
            "tag",
            &expr,
            Some(&schema),
            Some(DialectType::DuckDB),
            false,
        )
        .expect("lineage_with_schema");
        let expected = DataType::VarChar {
            length: None,
            parenthesized_length: false,
        };

        assert_eq!(node.expression.inferred_type(), Some(&expected));
        let virtual_child = node
            .downstream
            .iter()
            .find(|child| child.source_kind == SourceKind::Virtual)
            .expect("virtual UNNEST output");
        assert_eq!(virtual_child.expression.inferred_type(), Some(&expected));
    }

    #[test]
    fn test_lineage_with_schema_resolves_struct_fields_issue_408() {
        let struct_type = DataType::Struct {
            fields: vec![crate::expressions::StructField::new(
                "field_value".into(),
                DataType::Text,
            )],
            nested: true,
        };
        let mut schema = MappingSchema::with_dialect(DialectType::DuckDB);
        schema
            .add_table(
                "source_table",
                &[
                    ("composite_value".into(), struct_type.clone()),
                    (
                        "nested_items".into(),
                        DataType::Array {
                            element_type: Box::new(struct_type),
                            dimension: None,
                        },
                    ),
                ],
                None,
            )
            .expect("schema setup");

        let direct = parse_dialect(
            "SELECT composite_value.field_value AS output_value FROM source_table",
            DialectType::DuckDB,
        );
        let direct_node = lineage_with_schema(
            "output_value",
            &direct,
            Some(&schema),
            Some(DialectType::DuckDB),
            false,
        )
        .expect("direct struct lineage");
        assert_eq!(
            direct_node.expression.inferred_type(),
            Some(&DataType::Text)
        );
        assert_lineage_contains(&direct_node, "source_table.composite_value");

        let unnested = parse_dialect(
            "SELECT item.field_value AS output_value FROM source_table s \
             CROSS JOIN UNNEST(s.nested_items) AS expanded(item)",
            DialectType::DuckDB,
        );
        let unnest_node = lineage_with_schema(
            "output_value",
            &unnested,
            Some(&schema),
            Some(DialectType::DuckDB),
            false,
        )
        .expect("UNNEST struct lineage");
        assert_eq!(
            unnest_node.expression.inferred_type(),
            Some(&DataType::Text)
        );
        assert!(
            unnest_node.walk().any(|node| {
                node.source_kind == SourceKind::Table
                    && node.source_name == "source_table"
                    && node.name.ends_with(".nested_items")
            }),
            "expected physical nested_items lineage, got {:?}",
            lineage_names(&unnest_node)
        );
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
    fn test_lineage_deeply_nested_cte_reaches_base_table() {
        let expr = parse(
            "WITH outer_cte AS (\
             WITH middle_cte AS (\
             WITH inner_cte AS (SELECT x AS col FROM base_table) \
             SELECT col FROM inner_cte\
             ) SELECT col FROM middle_cte\
             ) SELECT col FROM outer_cte",
        );
        let node = lineage("col", &expr, None, false).unwrap();

        assert_lineage_contains(&node, "base_table.x");
        for cte_name in ["outer_cte", "middle_cte", "inner_cte"] {
            assert!(
                node.walk().any(|child| child.source_name == cte_name),
                "expected lineage to include CTE {cte_name}, got {:?}",
                lineage_names(&node)
            );
        }
    }

    #[test]
    fn test_lineage_reused_nested_cte_traces_each_reference() {
        let expr = parse(
            "WITH shared AS (\
             WITH nested AS (SELECT x AS col FROM base_table) \
             SELECT col FROM nested\
             ) \
             SELECT s0.col + s1.col + s2.col AS total \
             FROM shared AS s0 \
             CROSS JOIN shared AS s1 \
             CROSS JOIN shared AS s2",
        );
        let node = lineage("total", &expr, None, false).unwrap();

        let base_references = node
            .walk()
            .filter(|child| child.name == "base_table.x")
            .count();
        assert_eq!(
            base_references,
            3,
            "each shared CTE reference should reach base_table.x: {:?}",
            lineage_names(&node)
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

    #[test]
    fn test_issue_384_output_columns_preserve_unknown_positions() {
        let expr = parse("SELECT a, 1, *, tail FROM unknown_source");
        let output = output_columns(&expr, None).expect("output columns");

        assert!(!output.ordinal_complete);
        assert_eq!(
            output.columns,
            vec![
                OutputColumn::Named {
                    name: "a".to_string(),
                    ordinal: Some(0),
                },
                OutputColumn::Unnamed { ordinal: Some(1) },
                OutputColumn::Wildcard {
                    qualifier: None,
                    start_ordinal: Some(2),
                },
                OutputColumn::Named {
                    name: "tail".to_string(),
                    ordinal: None,
                },
            ]
        );
    }

    #[test]
    fn test_issue_384_output_columns_use_leftmost_set_operation_branch() {
        let expr = parse(
            "SELECT * FROM unknown_source UNION ALL \
             SELECT known_first AS x, known_second AS y FROM known_source",
        );
        let output = output_columns(&expr, None).expect("output columns");

        assert!(!output.ordinal_complete);
        assert_eq!(
            output.columns,
            vec![OutputColumn::Wildcard {
                qualifier: None,
                start_ordinal: Some(0),
            }]
        );
    }

    #[test]
    fn test_issue_384_schema_expands_output_wildcard() {
        let expr = parse("SELECT * FROM unknown_source");
        let mut schema = MappingSchema::new();
        schema
            .add_table(
                "unknown_source",
                &[
                    ("first_col".to_string(), DataType::Text),
                    ("second_col".to_string(), DataType::Text),
                ],
                None,
            )
            .expect("schema setup");

        let output = output_columns_with_schema(&expr, Some(&schema), None)
            .expect("schema-aware output columns");
        assert!(output.ordinal_complete);
        assert_eq!(
            output.columns,
            vec![
                OutputColumn::Named {
                    name: "first_col".to_string(),
                    ordinal: Some(0),
                },
                OutputColumn::Named {
                    name: "second_col".to_string(),
                    ordinal: Some(1),
                },
            ]
        );
    }

    #[test]
    fn test_issue_411_duckdb_and_snowflake_union_by_name_layout_and_lineage() {
        let sql = "SELECT 1 AS left_value UNION ALL BY NAME SELECT 2 AS right_value";

        for dialect in [DialectType::DuckDB, DialectType::Snowflake] {
            let expr = parse_dialect(sql, dialect);
            let output = output_columns(&expr, Some(dialect)).expect("output columns");
            assert_eq!(
                output.columns,
                vec![
                    OutputColumn::Named {
                        name: "left_value".to_string(),
                        ordinal: Some(0),
                    },
                    OutputColumn::Named {
                        name: "right_value".to_string(),
                        ordinal: Some(1),
                    },
                ],
                "unexpected output layout for {dialect:?}"
            );

            let left = lineage_at(0, &expr, Some(dialect), false).expect("left lineage");
            assert_eq!(
                left.downstream
                    .iter()
                    .map(|node| node.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["left_value"]
            );
            assert_eq!(
                left.downstream[0].set_branch,
                Some(SetBranch {
                    operator: SetOperator::Union,
                    ordinal: 0,
                    all: true,
                })
            );

            let right = lineage_at(1, &expr, Some(dialect), false).expect("right lineage");
            assert_eq!(
                right
                    .downstream
                    .iter()
                    .map(|node| node.name.as_str())
                    .collect::<Vec<_>>(),
                vec!["right_value"]
            );
            assert_eq!(
                right.downstream[0].set_branch,
                Some(SetBranch {
                    operator: SetOperator::Union,
                    ordinal: 1,
                    all: true,
                })
            );
        }
    }

    #[test]
    fn test_issue_411_bigquery_strict_by_name_reorders_lineage() {
        let dialect = DialectType::BigQuery;
        let expr = parse_dialect(
            "SELECT 1 AS a, 2 AS b UNION ALL BY NAME SELECT 3 AS b, 4 AS a",
            dialect,
        );
        let output = output_columns(&expr, Some(dialect)).expect("output columns");
        assert_eq!(
            output.columns,
            vec![
                OutputColumn::Named {
                    name: "a".to_string(),
                    ordinal: Some(0),
                },
                OutputColumn::Named {
                    name: "b".to_string(),
                    ordinal: Some(1),
                },
            ]
        );

        for (ordinal, expected_name) in [(0, "a"), (1, "b")] {
            let node = lineage_at(ordinal, &expr, Some(dialect), false).expect("lineage");
            assert_eq!(
                node.downstream
                    .iter()
                    .map(|child| child.name.as_str())
                    .collect::<Vec<_>>(),
                vec![expected_name, expected_name]
            );
        }
    }

    #[test]
    fn test_issue_411_bigquery_by_name_modes_and_explicit_order() {
        let dialect = DialectType::BigQuery;
        for (sql, expected) in [
            (
                "SELECT 1 AS a, 2 AS b INNER UNION ALL BY NAME SELECT 3 AS b, 4 AS c",
                vec!["b"],
            ),
            (
                "SELECT 1 AS a, 2 AS b LEFT OUTER UNION ALL BY NAME SELECT 3 AS b, 4 AS c",
                vec!["a", "b"],
            ),
            (
                "SELECT 1 AS a, 2 AS b FULL OUTER UNION ALL BY NAME SELECT 3 AS b, 4 AS c",
                vec!["a", "b", "c"],
            ),
            (
                "SELECT 1 AS a, 2 AS b FULL OUTER UNION ALL BY NAME ON (c, a) SELECT 3 AS b, 4 AS c",
                vec!["c", "a"],
            ),
            (
                "SELECT 1 AS a, 2 AS b INTERSECT DISTINCT BY NAME SELECT 3 AS b, 4 AS a",
                vec!["a", "b"],
            ),
            (
                "SELECT 1 AS a, 2 AS b EXCEPT DISTINCT BY NAME SELECT 3 AS b, 4 AS a",
                vec!["a", "b"],
            ),
        ] {
            let expr = parse_dialect(sql, dialect);
            let output = output_columns(&expr, Some(dialect)).expect("output columns");
            assert_eq!(
                output
                    .columns
                    .iter()
                    .filter_map(|column| match column {
                        OutputColumn::Named { name, .. } => Some(name.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>(),
                expected,
                "unexpected output layout for {sql}"
            );
        }
    }

    #[test]
    fn test_issue_411_name_matching_uses_dialect_identifier_rules() {
        let sql = "SELECT 1 AS \"lower\" UNION ALL BY NAME SELECT 2 AS lower";

        let duckdb = parse_dialect(sql, DialectType::DuckDB);
        assert_eq!(
            output_columns(&duckdb, Some(DialectType::DuckDB))
                .expect("DuckDB output")
                .columns
                .len(),
            1,
            "DuckDB quoted identifiers are case-insensitive"
        );

        let snowflake = parse_dialect(sql, DialectType::Snowflake);
        assert_eq!(
            output_columns(&snowflake, Some(DialectType::Snowflake))
                .expect("Snowflake output")
                .columns
                .len(),
            2,
            "Snowflake quoted lowercase and unquoted uppercase identifiers differ"
        );
    }

    #[test]
    fn test_issue_383_lineage_at_traces_resolvable_set_operation_branch() {
        let expr = parse(
            "SELECT * FROM unknown_source UNION ALL \
             SELECT known_first AS x, known_second AS y FROM known_source",
        );
        let node = lineage_at(1, &expr, None, false).expect("ordinal lineage");

        assert_lineage_contains(&node, "known_source.known_second");
        assert_eq!(node.downstream.len(), 1);
        assert_eq!(
            node.downstream[0].set_branch,
            Some(SetBranch {
                operator: SetOperator::Union,
                ordinal: 1,
                all: true,
            })
        );

        let expr = parse(
            "SELECT known_first, known_second FROM known_source EXCEPT \
             SELECT * FROM unknown_source",
        );
        let node = lineage_at(1, &expr, None, false).expect("left-only ordinal lineage");

        assert_lineage_contains(&node, "known_source.known_second");
        assert_eq!(node.downstream.len(), 1);
        assert_eq!(
            node.downstream[0].set_branch,
            Some(SetBranch {
                operator: SetOperator::Except,
                ordinal: 0,
                all: false,
            })
        );
    }

    #[test]
    fn test_issue_383_unresolved_wildcard_does_not_shift_ordinal() {
        let expr = parse(
            "SELECT *, tail FROM unknown_source UNION ALL \
             SELECT known_first, known_second FROM known_source",
        );
        let node = lineage_at(1, &expr, None, false).expect("partial ordinal lineage");
        let names = lineage_names(&node);

        assert!(names.iter().any(|name| name == "known_source.known_second"));
        assert!(!names
            .iter()
            .any(|name| name.ends_with(".tail") || name == "tail"));
    }

    #[test]
    fn test_issue_383_lineage_at_ignores_branch_output_names() {
        let expr = parse(
            "SELECT left_value AS left_name FROM left_source UNION ALL \
             SELECT right_value AS right_name FROM right_source",
        );
        let node = lineage_at(0, &expr, None, false).expect("ordinal lineage");

        assert_lineage_contains(&node, "left_source.left_value");
        assert_lineage_contains(&node, "right_source.right_value");
    }

    #[test]
    fn test_issue_383_lineage_at_supports_all_set_operations() {
        for (sql_operator, operator, all) in [
            ("UNION ALL", SetOperator::Union, true),
            ("INTERSECT", SetOperator::Intersect, false),
            ("EXCEPT", SetOperator::Except, false),
        ] {
            let expr = parse(&format!(
                "SELECT left_value AS left_name FROM left_source {sql_operator} \
                 SELECT right_value AS right_name FROM right_source"
            ));
            let node = lineage_at(0, &expr, None, false).expect("ordinal lineage");
            assert_eq!(
                node.downstream.len(),
                2,
                "expected both branches for {sql_operator}"
            );
            assert_eq!(
                node.downstream
                    .iter()
                    .map(|child| child.set_branch)
                    .collect::<Vec<_>>(),
                vec![
                    Some(SetBranch {
                        operator,
                        ordinal: 0,
                        all,
                    }),
                    Some(SetBranch {
                        operator,
                        ordinal: 1,
                        all,
                    }),
                ]
            );
        }
    }

    #[test]
    fn test_issue_383_lineage_at_with_schema_expands_wildcard() {
        let expr = parse(
            "SELECT * FROM unknown_source UNION ALL \
             SELECT known_first, known_second FROM known_source",
        );
        let mut schema = MappingSchema::new();
        schema
            .add_table(
                "unknown_source",
                &[
                    ("first_col".to_string(), DataType::Text),
                    ("second_col".to_string(), DataType::Text),
                ],
                None,
            )
            .expect("schema setup");
        schema
            .add_table(
                "known_source",
                &[
                    ("known_first".to_string(), DataType::Text),
                    ("known_second".to_string(), DataType::Text),
                ],
                None,
            )
            .expect("schema setup");

        let node = lineage_at_with_schema(1, &expr, Some(&schema), None, false)
            .expect("schema-aware ordinal lineage");
        assert_lineage_contains(&node, "unknown_source.second_col");
        assert_lineage_contains(&node, "known_source.known_second");
    }

    #[test]
    fn test_issue_385_structured_lineage_resolution_errors() {
        let out_of_range = lineage_at(1, &parse("SELECT a FROM t"), None, false)
            .expect_err("ordinal should be out of range");
        assert!(matches!(
            out_of_range,
            Error::ColumnResolution {
                target: ColumnResolutionTarget::Ordinal { ordinal: 1 },
                reason: ColumnResolutionReason::NotFound,
            }
        ));

        let indeterminate = lineage(
            "tail",
            &parse(
                "SELECT *, tail FROM unknown_source UNION ALL \
                 SELECT known_first, known_second FROM known_source",
            ),
            None,
            false,
        )
        .expect_err("tail ordinal should be indeterminate");
        assert!(matches!(
            indeterminate,
            Error::ColumnResolution {
                target: ColumnResolutionTarget::Name { ref name },
                reason: ColumnResolutionReason::Indeterminate,
            } if name == "tail"
        ));

        let ambiguous = lineage(
            "duplicate",
            &parse("SELECT a AS duplicate, b AS duplicate FROM t"),
            None,
            false,
        )
        .expect_err("duplicate output name should be ambiguous");
        assert!(matches!(
            ambiguous,
            Error::ColumnResolution {
                target: ColumnResolutionTarget::Name { ref name },
                reason: ColumnResolutionReason::Ambiguous,
            } if name == "duplicate"
        ));
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
    fn test_issue_368_expand_cte_stars_rewrites_every_union_arm() {
        let mut expr = parse_one(ISSUE_368_SQL, DialectType::BigQuery).unwrap();

        expand_cte_stars(&mut expr, None);

        assert_eq!(
            crate::generate(&expr, DialectType::BigQuery).unwrap(),
            "WITH base AS (SELECT 1 AS col_a), literal_branch AS (SELECT 2 AS col_a), \
             unioned AS (SELECT base.col_a FROM base UNION ALL \
             SELECT literal_branch.col_a FROM literal_branch) \
             SELECT col_a FROM unioned"
        );
    }

    #[test]
    fn test_issue_368_lineage_resolves_non_leftmost_union_star() {
        let expr = parse_one(ISSUE_368_SQL, DialectType::BigQuery).unwrap();

        let node = lineage("col_a", &expr, Some(DialectType::BigQuery), false).unwrap();
        let names = lineage_names(&node);

        assert!(
            node.walk().any(|child| {
                child.name == "col_a"
                    && child.source_name == "literal_branch"
                    && child.source_kind == SourceKind::Cte
            }),
            "expected the right UNION branch to resolve to literal_branch.col_a, got {node:#?}"
        );
        assert!(
            !names
                .iter()
                .any(|name| name == "*" || name == "literal_branch.*"),
            "did not expect an unresolved right-branch star, got {names:?}"
        );
    }

    #[test]
    fn test_expand_cte_stars_rewrites_all_set_operation_kinds() {
        for operator in ["UNION ALL", "INTERSECT", "EXCEPT"] {
            let mut expr = parse(&format!(
                "WITH left_cte AS (SELECT 1 AS col_a), \
                 right_cte AS (SELECT 2 AS col_a), \
                 combined AS (SELECT * FROM left_cte {operator} SELECT * FROM right_cte) \
                 SELECT col_a FROM combined"
            ));

            expand_cte_stars(&mut expr, None);

            let sql = crate::generate(&expr, DialectType::Generic).unwrap();
            assert!(
                sql.contains("SELECT left_cte.col_a FROM left_cte"),
                "expected left arm expansion for {operator}, got {sql}"
            );
            assert!(
                sql.contains("SELECT right_cte.col_a FROM right_cte"),
                "expected right arm expansion for {operator}, got {sql}"
            );
        }
    }

    #[test]
    fn test_expand_cte_stars_rewrites_nested_parenthesized_set_operations() {
        let mut expr = parse(
            "WITH a AS (SELECT 1 AS x), \
             b AS (SELECT 2 AS x), \
             c AS (SELECT 3 AS x), \
             combined AS ((SELECT * FROM a UNION ALL SELECT * FROM b) \
             UNION ALL SELECT * FROM c) \
             SELECT x FROM combined",
        );

        expand_cte_stars(&mut expr, None);

        let sql = crate::generate(&expr, DialectType::Generic).unwrap();
        for source in ["a", "b", "c"] {
            assert!(
                sql.contains(&format!("SELECT {source}.x FROM {source}")),
                "expected nested arm {source} to be expanded, got {sql}"
            );
        }
    }

    #[test]
    fn test_expand_cte_stars_rewrites_root_set_operation() {
        let mut expr = parse(
            "WITH a AS (SELECT 1 AS x), b AS (SELECT 2 AS x) \
             SELECT * FROM a UNION ALL SELECT * FROM b",
        );

        expand_cte_stars(&mut expr, None);

        let sql = crate::generate(&expr, DialectType::Generic).unwrap();
        assert!(
            sql.contains("SELECT a.x FROM a UNION ALL SELECT b.x FROM b"),
            "expected both root UNION arms to be expanded, got {sql}"
        );
    }

    #[test]
    fn test_expand_cte_stars_preserves_leftmost_output_names() {
        let mut expr = parse(
            "WITH a AS (SELECT 1 AS left_name), \
             b AS (SELECT 2 AS right_name), \
             combined AS (SELECT * FROM a UNION ALL SELECT * FROM b) \
             SELECT * FROM combined",
        );

        expand_cte_stars(&mut expr, None);

        let sql = crate::generate(&expr, DialectType::Generic).unwrap();
        assert!(
            sql.ends_with("SELECT combined.left_name FROM combined"),
            "expected the set operation output name to come from the left arm, got {sql}"
        );
        assert!(
            sql.contains("SELECT b.right_name FROM b"),
            "expected the differently named right arm to still be expanded, got {sql}"
        );
    }

    #[test]
    fn test_expand_cte_stars_rewrites_body_with_explicit_cte_columns() {
        let mut expr = parse(
            "WITH a AS (SELECT 1 AS x), \
             b AS (SELECT 2 AS x), \
             combined(output_name) AS (SELECT * FROM a UNION ALL SELECT * FROM b) \
             SELECT * FROM combined",
        );

        expand_cte_stars(&mut expr, None);

        let sql = crate::generate(&expr, DialectType::Generic).unwrap();
        assert!(
            sql.contains("SELECT a.x FROM a UNION ALL SELECT b.x FROM b"),
            "expected explicit aliases not to suppress body expansion, got {sql}"
        );
        assert!(
            sql.ends_with("SELECT combined.output_name FROM combined"),
            "expected the explicit CTE output name to override the body name, got {sql}"
        );
    }

    #[test]
    fn test_expand_cte_stars_keeps_recursive_self_reference_conservative() {
        let mut expr = parse(
            "WITH RECURSIVE r(x) AS (\
             SELECT 1 AS x UNION ALL SELECT * FROM r\
             ) SELECT * FROM r",
        );

        expand_cte_stars(&mut expr, None);

        let sql = crate::generate(&expr, DialectType::Generic).unwrap();
        assert!(
            sql.contains("UNION ALL SELECT * FROM r"),
            "expected the recursive body star to remain untouched, got {sql}"
        );
        assert!(
            sql.ends_with("SELECT r.x FROM r"),
            "expected the explicit recursive CTE column to expand the outer star, got {sql}"
        );
    }

    #[test]
    fn test_expand_cte_stars_preserves_genuinely_unresolved_branch_star() {
        let mut expr = parse(
            "WITH known AS (SELECT 1 AS x), \
             combined AS (SELECT * FROM known UNION ALL SELECT * FROM missing) \
             SELECT * FROM combined",
        );

        expand_cte_stars(&mut expr, None);

        let sql = crate::generate(&expr, DialectType::Generic).unwrap();
        assert!(
            sql.contains("SELECT known.x FROM known UNION ALL SELECT * FROM missing"),
            "expected only the resolvable branch to expand, got {sql}"
        );
        assert!(
            sql.ends_with("SELECT combined.x FROM combined"),
            "expected the leftmost output name to remain usable, got {sql}"
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
        for cte_name in ["a", "b"] {
            assert!(
                node.walk().any(|child| child.source_name == cte_name),
                "expected set-operation lineage to retain CTE source {cte_name}: {:?}",
                lineage_names(&node)
            );
        }
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
