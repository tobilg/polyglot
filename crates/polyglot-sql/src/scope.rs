//! Scope Analysis Module
//!
//! This module provides scope analysis for SQL queries, enabling detection of
//! correlated subqueries, column references, and scope relationships.
//!
//! Ported from sqlglot's optimizer/scope.py

use crate::expressions::Expression;
use crate::traversal::ExpressionWalk;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
#[cfg(feature = "bindings")]
use ts_rs::TS;

/// Type of scope in a SQL query
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[cfg_attr(feature = "bindings", ts(export))]
pub enum ScopeType {
    /// Root scope of the query
    Root,
    /// Subquery scope (e.g., WHERE x IN (SELECT ...))
    Subquery,
    /// Derived table scope (e.g., FROM (SELECT ...) AS t)
    DerivedTable,
    /// Common Table Expression scope
    Cte,
    /// Union/Intersect/Except scope
    SetOperation,
    /// User-Defined Table Function scope
    Udtf,
}

/// Semantic kind of a source registered in a scope.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceKind {
    /// Root query or statement context
    Root,
    /// Physical table source
    Table,
    /// Derived table/subquery source
    DerivedTable,
    /// Common Table Expression source
    Cte,
    /// Virtual table source such as UNNEST / UDTF
    Virtual,
    /// Unresolved or synthetic fallback source
    Unknown,
}

impl Default for SourceKind {
    fn default() -> Self {
        Self::Unknown
    }
}

/// Unwrap query containers without creating a new lexical context.
pub(crate) fn scope_query(expression: &Expression) -> &Expression {
    match expression {
        Expression::Cte(cte) => scope_query(&cte.this),
        Expression::Subquery(subquery) => scope_query(&subquery.this),
        Expression::Paren(paren) => scope_query(&paren.this),
        Expression::Alias(alias) => scope_query(&alias.this),
        Expression::Prepare(prepare) => scope_query(&prepare.statement),
        Expression::CreateTable(create) if create.as_select.is_some() => {
            scope_query(create.as_select.as_ref().unwrap())
        }
        _ => expression,
    }
}

/// A lightweight resolution view containing only selected sources. CTE
/// declarations remain available, but do not themselves cause ambiguity.
pub(crate) fn selected_reference_scope(scope: &Scope) -> Scope {
    let query = scope_query(&scope.expression);
    let aliases: HashSet<_> = walk_in_scope(query, false)
        .filter_map(|node| match node {
            Expression::Table(table) => {
                Some(table.alias.as_ref().unwrap_or(&table.name).name.clone())
            }
            _ => None,
        })
        .collect();
    let mut selected = Scope::new(query.clone());
    selected.cte_sources = scope.cte_sources.clone();
    selected.sources = scope
        .sources
        .iter()
        .filter(|(name, source)| {
            // Source keys use the spelling of the actual FROM/JOIN binding.
            // Folding here can accidentally retain an unused, quoted CTE.
            source.kind != SourceKind::Cte || aliases.contains(*name)
        })
        .map(|(name, source)| (name.clone(), source.clone()))
        .collect();
    selected
}

/// Information about a source (table or subquery) in a scope
#[derive(Debug, Clone)]
pub struct SourceInfo {
    /// The source expression (Table or subquery)
    pub expression: Expression,
    /// Whether this source is a scope (vs. a plain table)
    pub is_scope: bool,
    /// Semantic source kind for lineage consumers.
    pub kind: SourceKind,
    /// User-written alias, when it should be preserved separately from lineage name.
    pub alias: Option<String>,
    /// Canonical lineage source name, e.g. synthetic `_0` for virtual sources.
    pub lineage_name: Option<String>,
}

impl SourceInfo {
    pub fn new(expression: Expression, is_scope: bool, kind: SourceKind) -> Self {
        Self {
            expression,
            is_scope,
            kind,
            alias: None,
            lineage_name: None,
        }
    }

    pub fn with_alias(mut self, alias: impl Into<String>) -> Self {
        self.alias = Some(alias.into());
        self
    }

    pub fn with_lineage_name(mut self, lineage_name: impl Into<String>) -> Self {
        self.lineage_name = Some(lineage_name.into());
        self
    }
}

/// A column reference found in a scope
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnRef {
    /// The table/alias qualifier (if any)
    pub table: Option<String>,
    /// The column name
    pub name: String,
}

/// Represents a scope in a SQL query
///
/// A scope is the context of a SELECT statement and its sources.
/// Scopes can be nested (subqueries, CTEs, derived tables) and form a tree.
#[derive(Debug, Clone)]
pub struct Scope {
    /// The expression at the root of this scope
    pub expression: Expression,

    /// Type of this scope relative to its parent
    pub scope_type: ScopeType,

    /// Mapping of source names to their info
    pub sources: HashMap<String, SourceInfo>,

    /// Sources from LATERAL views (have access to preceding sources)
    pub lateral_sources: HashMap<String, SourceInfo>,

    /// CTE sources available to this scope
    pub cte_sources: HashMap<String, SourceInfo>,

    /// If this is a derived table or CTE with alias columns, this is that list
    /// e.g., `SELECT * FROM (SELECT ...) AS y(col1, col2)` => ["col1", "col2"]
    pub outer_columns: Vec<String>,

    /// Whether this scope can potentially be correlated
    /// (true for subqueries and UDTFs)
    pub can_be_correlated: bool,

    /// Whether this derived table sees only preceding FROM/JOIN sources.
    pub is_lateral: bool,

    /// Child subquery scopes
    pub subquery_scopes: Vec<Scope>,

    /// Child derived table scopes
    pub derived_table_scopes: Vec<Scope>,

    /// Child CTE scopes
    pub cte_scopes: Vec<Scope>,

    /// Child UDTF (User Defined Table Function) scopes
    pub udtf_scopes: Vec<Scope>,

    /// Combined derived_table_scopes + udtf_scopes in definition order
    pub table_scopes: Vec<Scope>,

    /// Union/set operation scopes (left and right)
    pub union_scopes: Vec<Scope>,

    /// Cached columns
    columns_cache: Option<Vec<ColumnRef>>,

    /// Cached external columns
    external_columns_cache: Option<Vec<ColumnRef>>,
}

impl Scope {
    /// Create a new root scope
    pub fn new(expression: Expression) -> Self {
        Self {
            expression,
            scope_type: ScopeType::Root,
            sources: HashMap::new(),
            lateral_sources: HashMap::new(),
            cte_sources: HashMap::new(),
            outer_columns: Vec::new(),
            can_be_correlated: false,
            is_lateral: false,
            subquery_scopes: Vec::new(),
            derived_table_scopes: Vec::new(),
            cte_scopes: Vec::new(),
            udtf_scopes: Vec::new(),
            table_scopes: Vec::new(),
            union_scopes: Vec::new(),
            columns_cache: None,
            external_columns_cache: None,
        }
    }

    /// Create a child scope branching from this one
    pub fn branch(&self, expression: Expression, scope_type: ScopeType) -> Self {
        self.branch_with_options(expression, scope_type, None, None, None)
    }

    /// Create a child scope with additional options
    pub fn branch_with_options(
        &self,
        expression: Expression,
        scope_type: ScopeType,
        sources: Option<HashMap<String, SourceInfo>>,
        lateral_sources: Option<HashMap<String, SourceInfo>>,
        outer_columns: Option<Vec<String>>,
    ) -> Self {
        let can_be_correlated = self.can_be_correlated
            || scope_type == ScopeType::Subquery
            || scope_type == ScopeType::Udtf;

        Self {
            expression,
            scope_type,
            sources: sources.unwrap_or_default(),
            lateral_sources: lateral_sources.unwrap_or_default(),
            cte_sources: self.cte_sources.clone(),
            outer_columns: outer_columns.unwrap_or_default(),
            can_be_correlated,
            is_lateral: false,
            subquery_scopes: Vec::new(),
            derived_table_scopes: Vec::new(),
            cte_scopes: Vec::new(),
            udtf_scopes: Vec::new(),
            table_scopes: Vec::new(),
            union_scopes: Vec::new(),
            columns_cache: None,
            external_columns_cache: None,
        }
    }

    /// Clear all cached properties
    pub fn clear_cache(&mut self) {
        self.columns_cache = None;
        self.external_columns_cache = None;
    }

    /// Add a source to this scope
    pub fn add_source(&mut self, name: String, expression: Expression, is_scope: bool) {
        let kind = if is_scope {
            SourceKind::DerivedTable
        } else {
            SourceKind::Table
        };
        self.add_source_info(name, SourceInfo::new(expression, is_scope, kind));
    }

    /// Add a preconfigured source to this scope
    pub fn add_source_info(&mut self, name: String, info: SourceInfo) {
        self.sources.insert(name, info);
        self.clear_cache();
    }

    /// Add a virtual source such as UNNEST / UDTF.
    pub fn add_virtual_source(&mut self, alias: String, expression: Expression) {
        let lineage_name = self.next_virtual_source_name();
        let info = SourceInfo::new(expression, false, SourceKind::Virtual)
            .with_alias(alias.clone())
            .with_lineage_name(lineage_name);
        self.add_source_info(alias, info);
    }

    fn next_virtual_source_name(&self) -> String {
        let count = self
            .sources
            .values()
            .filter(|source| source.kind == SourceKind::Virtual)
            .count();
        format!("_{}", count)
    }

    /// Add a lateral source to this scope
    pub fn add_lateral_source(&mut self, name: String, expression: Expression, is_scope: bool) {
        let kind = if is_scope {
            SourceKind::DerivedTable
        } else {
            SourceKind::Table
        };
        let info = SourceInfo::new(expression.clone(), is_scope, kind);
        self.sources.insert(name.clone(), info.clone());
        self.lateral_sources.insert(name, info);
        self.clear_cache();
    }

    /// Add a CTE source to this scope
    pub fn add_cte_source(&mut self, name: String, expression: Expression) {
        let info = SourceInfo::new(expression, true, SourceKind::Cte);
        self.cte_sources.insert(name.clone(), info.clone());
        self.sources.insert(name, info);
        self.clear_cache();
    }

    /// Rename a source
    pub fn rename_source(&mut self, old_name: &str, new_name: String) {
        if let Some(source) = self.sources.remove(old_name) {
            self.sources.insert(new_name, source);
        }
        self.clear_cache();
    }

    /// Remove a source
    pub fn remove_source(&mut self, name: &str) {
        self.sources.remove(name);
        self.clear_cache();
    }

    /// Collect all column references in this scope
    pub fn columns(&mut self) -> &[ColumnRef] {
        if self.columns_cache.is_none() {
            let mut columns = Vec::new();
            collect_columns(&self.expression, &mut columns);
            self.columns_cache = Some(columns);
        }
        self.columns_cache.as_ref().unwrap()
    }

    /// Collect projected output column names for this scope's query expression.
    ///
    /// This is intended for result schema style output columns (e.g. UNION
    /// outputs), unlike [`Self::columns`], which returns raw referenced columns.
    pub fn output_columns(&self) -> Vec<String> {
        crate::ast_transforms::get_output_column_names(&self.expression)
    }

    /// Get all source names in this scope
    pub fn source_names(&self) -> HashSet<String> {
        let mut names: HashSet<String> = self.sources.keys().cloned().collect();
        names.extend(self.cte_sources.keys().cloned());
        names
    }

    /// Get columns that reference sources outside this scope
    pub fn external_columns(&mut self) -> Vec<ColumnRef> {
        if self.external_columns_cache.is_some() {
            return self.external_columns_cache.clone().unwrap();
        }

        let source_names = self.source_names();
        let columns = self.columns().to_vec();

        let external: Vec<ColumnRef> = columns
            .into_iter()
            .filter(|col| {
                // A column is external if it has a table qualifier that's not in our sources
                match &col.table {
                    Some(table) => !source_names.contains(table),
                    None => false, // Unqualified columns might be local
                }
            })
            .collect();

        self.external_columns_cache = Some(external.clone());
        external
    }

    /// Get columns that reference sources in this scope (not external)
    pub fn local_columns(&mut self) -> Vec<ColumnRef> {
        let external_set: HashSet<_> = self.external_columns().into_iter().collect();
        let columns = self.columns().to_vec();

        columns
            .into_iter()
            .filter(|col| !external_set.contains(col))
            .collect()
    }

    /// Get unqualified columns (columns without table qualifier)
    pub fn unqualified_columns(&mut self) -> Vec<ColumnRef> {
        self.columns()
            .iter()
            .filter(|c| c.table.is_none())
            .cloned()
            .collect()
    }

    /// Get columns for a specific source
    pub fn source_columns(&mut self, source_name: &str) -> Vec<ColumnRef> {
        self.columns()
            .iter()
            .filter(|col| col.table.as_deref() == Some(source_name))
            .cloned()
            .collect()
    }

    /// Determine if this scope is a correlated subquery
    ///
    /// A subquery is correlated if:
    /// 1. It can be correlated (is a subquery or UDTF), AND
    /// 2. It references columns from outer scopes
    pub fn is_correlated_subquery(&mut self) -> bool {
        self.can_be_correlated && !self.external_columns().is_empty()
    }

    /// Check if this is a subquery scope
    pub fn is_subquery(&self) -> bool {
        self.scope_type == ScopeType::Subquery
    }

    /// Check if this is a derived table scope
    pub fn is_derived_table(&self) -> bool {
        self.scope_type == ScopeType::DerivedTable
    }

    /// Check if this is a CTE scope
    pub fn is_cte(&self) -> bool {
        self.scope_type == ScopeType::Cte
    }

    /// Check if this is the root scope
    pub fn is_root(&self) -> bool {
        self.scope_type == ScopeType::Root
    }

    /// Check if this is a UDTF scope
    pub fn is_udtf(&self) -> bool {
        self.scope_type == ScopeType::Udtf
    }

    /// Check if this is a union/set operation scope
    pub fn is_union(&self) -> bool {
        self.scope_type == ScopeType::SetOperation
    }

    /// Traverse all scopes in this tree (depth-first post-order)
    pub fn traverse(&self) -> Vec<&Scope> {
        let mut result = Vec::new();
        self.traverse_impl(&mut result);
        result
    }

    fn traverse_impl<'a>(&'a self, result: &mut Vec<&'a Scope>) {
        // First traverse children
        for scope in &self.cte_scopes {
            scope.traverse_impl(result);
        }
        for scope in &self.union_scopes {
            scope.traverse_impl(result);
        }
        for scope in &self.table_scopes {
            scope.traverse_impl(result);
        }
        for scope in &self.subquery_scopes {
            scope.traverse_impl(result);
        }
        // Then add self
        result.push(self);
    }

    /// Count references to each scope in this tree
    pub fn ref_count(&self) -> HashMap<usize, usize> {
        let mut counts: HashMap<usize, usize> = HashMap::new();

        for scope in self.traverse() {
            for (_, source_info) in scope.sources.iter() {
                if source_info.is_scope {
                    let id = &source_info.expression as *const _ as usize;
                    *counts.entry(id).or_insert(0) += 1;
                }
            }
        }

        counts
    }
}

/// Collect all column references from an expression tree
fn collect_columns(expr: &Expression, columns: &mut Vec<ColumnRef>) {
    match expr {
        Expression::Column(col) => {
            columns.push(ColumnRef {
                table: col.table.as_ref().map(|t| t.name.clone()),
                name: col.name.name.clone(),
            });
        }
        Expression::Select(select) => {
            // Collect from SELECT expressions
            for e in &select.expressions {
                collect_columns(e, columns);
            }
            // Collect from JOIN ON / MATCH_CONDITION clauses
            for join in &select.joins {
                if let Some(on) = &join.on {
                    collect_columns(on, columns);
                }
                if let Some(match_condition) = &join.match_condition {
                    collect_columns(match_condition, columns);
                }
            }
            // Collect from WHERE
            if let Some(where_clause) = &select.where_clause {
                collect_columns(&where_clause.this, columns);
            }
            // Collect from HAVING
            if let Some(having) = &select.having {
                collect_columns(&having.this, columns);
            }
            // Collect from ORDER BY
            if let Some(order_by) = &select.order_by {
                for ord in &order_by.expressions {
                    collect_columns(&ord.this, columns);
                }
            }
            // Collect from GROUP BY
            if let Some(group_by) = &select.group_by {
                for e in &group_by.expressions {
                    collect_columns(e, columns);
                }
            }
            // Note: We don't recurse into FROM/JOIN source subqueries here
            // as those create their own scopes.
        }
        // Binary operations
        Expression::And(bin)
        | Expression::Or(bin)
        | Expression::Add(bin)
        | Expression::Sub(bin)
        | Expression::Mul(bin)
        | Expression::Div(bin)
        | Expression::Mod(bin)
        | Expression::Eq(bin)
        | Expression::Neq(bin)
        | Expression::Lt(bin)
        | Expression::Lte(bin)
        | Expression::Gt(bin)
        | Expression::Gte(bin)
        | Expression::BitwiseAnd(bin)
        | Expression::BitwiseOr(bin)
        | Expression::BitwiseXor(bin)
        | Expression::Concat(bin) => {
            collect_columns(&bin.left, columns);
            collect_columns(&bin.right, columns);
        }
        // LIKE/ILIKE operations
        Expression::Like(like) | Expression::ILike(like) => {
            collect_columns(&like.left, columns);
            collect_columns(&like.right, columns);
            if let Some(escape) = &like.escape {
                collect_columns(escape, columns);
            }
        }
        // Unary operations
        Expression::Not(un) | Expression::Neg(un) | Expression::BitwiseNot(un) => {
            collect_columns(&un.this, columns);
        }
        Expression::Function(func) => {
            for arg in &func.args {
                collect_columns(arg, columns);
            }
        }
        Expression::AggregateFunction(agg) => {
            for arg in &agg.args {
                collect_columns(arg, columns);
            }
        }
        Expression::WindowFunction(wf) => {
            collect_columns(&wf.this, columns);
            for e in &wf.over.partition_by {
                collect_columns(e, columns);
            }
            for e in &wf.over.order_by {
                collect_columns(&e.this, columns);
            }
        }
        Expression::Alias(alias) => {
            collect_columns(&alias.this, columns);
        }
        Expression::Case(case) => {
            if let Some(operand) = &case.operand {
                collect_columns(operand, columns);
            }
            for (when_expr, then_expr) in &case.whens {
                collect_columns(when_expr, columns);
                collect_columns(then_expr, columns);
            }
            if let Some(else_clause) = &case.else_ {
                collect_columns(else_clause, columns);
            }
        }
        Expression::Paren(paren) => {
            collect_columns(&paren.this, columns);
        }
        Expression::Ordered(ord) => {
            collect_columns(&ord.this, columns);
        }
        Expression::In(in_expr) => {
            collect_columns(&in_expr.this, columns);
            for e in &in_expr.expressions {
                collect_columns(e, columns);
            }
            // Note: in_expr.query is a subquery - creates its own scope
        }
        Expression::Between(between) => {
            collect_columns(&between.this, columns);
            collect_columns(&between.low, columns);
            collect_columns(&between.high, columns);
        }
        Expression::IsNull(is_null) => {
            collect_columns(&is_null.this, columns);
        }
        Expression::Cast(cast) => {
            collect_columns(&cast.this, columns);
        }
        Expression::Extract(extract) => {
            collect_columns(&extract.this, columns);
        }
        Expression::Exists(_) | Expression::Subquery(_) => {
            // These create their own scopes - don't collect from here
        }
        Expression::Prepare(prepare) => {
            collect_columns(&prepare.statement, columns);
        }
        _ => {
            // For other expressions, we might need to add more cases
        }
    }
}

/// Build scope tree from an expression
///
/// This traverses the expression tree and builds a hierarchy of Scope objects
/// that track sources and column references at each level.
pub fn build_scope(expression: &Expression) -> Scope {
    build_scope_with_ctes(expression, &HashMap::new())
}

/// Build a query scope with CTE definitions inherited from its lexical parent.
pub(crate) fn build_scope_with_ctes(
    expression: &Expression,
    ctes: &HashMap<String, SourceInfo>,
) -> Scope {
    let mut root = Scope::new(expression.clone());
    root.cte_sources = ctes.clone();
    build_scope_impl(expression, &mut root);
    root
}

fn build_scope_impl(expression: &Expression, current_scope: &mut Scope) {
    match expression {
        Expression::Prepare(prepare) => {
            build_scope_impl(&prepare.statement, current_scope);
        }
        Expression::Select(select) => {
            // Process CTEs first
            if let Some(with) = &select.with {
                process_ctes(with, current_scope);
            }

            // Register relations in order. CTE declarations are available for
            // FROM lookup, but only selected preceding bindings are lateral inputs.
            let mut preceding_sources = HashSet::new();
            for table in select
                .from
                .iter()
                .flat_map(|from| &from.expressions)
                .chain(select.joins.iter().map(|join| &join.this))
            {
                if let Some(name) = add_table_to_scope(table, current_scope, &preceding_sources) {
                    preceding_sources.insert(name);
                }
            }

            // Process table-generating lateral views (Hive/Spark style UDTFs).
            for lateral_view in &select.lateral_views {
                add_lateral_view_to_scope(lateral_view, current_scope);
            }

            // Process subqueries in WHERE, SELECT expressions, etc.
            collect_subqueries(expression, current_scope);
        }
        Expression::Union(union) => {
            if let Some(with) = &union.with {
                process_ctes(with, current_scope);
            }

            let mut left_scope = current_scope.branch(union.left.clone(), ScopeType::SetOperation);
            build_scope_impl(&union.left, &mut left_scope);

            let mut right_scope =
                current_scope.branch(union.right.clone(), ScopeType::SetOperation);
            build_scope_impl(&union.right, &mut right_scope);

            current_scope.union_scopes.push(left_scope);
            current_scope.union_scopes.push(right_scope);
        }
        Expression::Intersect(intersect) => {
            if let Some(with) = &intersect.with {
                process_ctes(with, current_scope);
            }

            let mut left_scope =
                current_scope.branch(intersect.left.clone(), ScopeType::SetOperation);
            build_scope_impl(&intersect.left, &mut left_scope);

            let mut right_scope =
                current_scope.branch(intersect.right.clone(), ScopeType::SetOperation);
            build_scope_impl(&intersect.right, &mut right_scope);

            current_scope.union_scopes.push(left_scope);
            current_scope.union_scopes.push(right_scope);
        }
        Expression::Except(except) => {
            if let Some(with) = &except.with {
                process_ctes(with, current_scope);
            }

            let mut left_scope = current_scope.branch(except.left.clone(), ScopeType::SetOperation);
            build_scope_impl(&except.left, &mut left_scope);

            let mut right_scope =
                current_scope.branch(except.right.clone(), ScopeType::SetOperation);
            build_scope_impl(&except.right, &mut right_scope);

            current_scope.union_scopes.push(left_scope);
            current_scope.union_scopes.push(right_scope);
        }
        Expression::CreateTable(create) => {
            // Handle CREATE TABLE ... AS [WITH ...] SELECT ...
            // Process CTEs if present
            if let Some(with) = &create.with_cte {
                process_ctes(with, current_scope);
            }
            // Traverse the AS SELECT body
            if let Some(as_select) = &create.as_select {
                build_scope_impl(as_select, current_scope);
            }
        }
        Expression::Subquery(subquery) => {
            build_scope_impl(&subquery.this, current_scope);
        }
        Expression::Paren(paren) => {
            build_scope_impl(&paren.this, current_scope);
        }
        _ => {}
    }
}

fn process_ctes(with: &crate::expressions::With, current_scope: &mut Scope) {
    for cte in &with.ctes {
        let cte_name = cte.alias.name.clone();
        let cte_expr = Expression::Cte(Box::new(cte.clone()));
        let mut cte_scope = current_scope.branch(cte_expr.clone(), ScopeType::Cte);

        if with.recursive && cte_body_self_references(cte) {
            cte_scope.add_cte_source(cte_name.clone(), cte_expr.clone());
        }

        build_scope_impl(&cte.this, &mut cte_scope);
        current_scope.add_cte_source(cte_name, cte_expr);
        current_scope.cte_scopes.push(cte_scope);
    }
}

fn cte_body_self_references(cte: &crate::expressions::Cte) -> bool {
    let cte_name = cte.alias.name.as_str();
    !cte.this
        .find_all(|expr| match expr {
            Expression::Table(table) if table.schema.is_none() && table.catalog.is_none() => {
                table.name.name.eq_ignore_ascii_case(cte_name)
            }
            _ => false,
        })
        .is_empty()
}

fn add_table_to_scope(
    expr: &Expression,
    scope: &mut Scope,
    preceding_sources: &HashSet<String>,
) -> Option<String> {
    match expr {
        Expression::Table(table) => {
            let name = table
                .alias
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_else(|| table.name.name.clone());
            let cte_source = if table.schema.is_none() && table.catalog.is_none() {
                scope.cte_sources.get(&table.name.name).or_else(|| {
                    scope
                        .cte_sources
                        .iter()
                        .find(|(cte_name, _)| cte_name.eq_ignore_ascii_case(&table.name.name))
                        .map(|(_, source)| source)
                })
            } else {
                None
            };

            if let Some(source) = cte_source {
                scope.add_source_info(name.clone(), source.clone());
            } else {
                let mut source = SourceInfo::new(expr.clone(), false, SourceKind::Table);
                if let Some(alias) = &table.alias {
                    source = source.with_alias(alias.name.clone());
                }
                scope.add_source_info(name.clone(), source);
            }
            Some(name)
        }
        Expression::Subquery(subquery) => {
            let name = subquery
                .alias
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_default();

            let mut derived_scope = scope.branch(subquery.this.clone(), ScopeType::DerivedTable);
            if subquery.lateral {
                derived_scope.is_lateral = true;
                derived_scope.can_be_correlated = true;
                derived_scope.lateral_sources = preceding_sources
                    .iter()
                    .filter_map(|name| {
                        scope
                            .sources
                            .get(name)
                            .map(|source| (name.clone(), source.clone()))
                    })
                    .collect();
            }
            build_scope_impl(&subquery.this, &mut derived_scope);

            scope.add_source(name.clone(), expr.clone(), true);
            scope.derived_table_scopes.push(derived_scope);
            Some(name)
        }
        Expression::Unnest(unnest) => {
            if let Some(alias) = &unnest.alias {
                scope.add_virtual_source(alias.name.clone(), expr.clone());
            }
            unnest.alias.as_ref().map(|alias| alias.name.clone())
        }
        Expression::Values(values) => {
            let name = values
                .alias
                .as_ref()
                .map(|alias| alias.name.clone())
                .unwrap_or_default();
            scope.add_virtual_source(name.clone(), expr.clone());
            Some(name)
        }
        Expression::Alias(alias) if matches!(&alias.this, Expression::Unnest(_)) => {
            scope.add_virtual_source(alias.alias.name.clone(), expr.clone());
            Some(alias.alias.name.clone())
        }
        Expression::Alias(alias) if is_query_like_relation(&alias.this) => {
            let outer_columns = alias
                .column_aliases
                .iter()
                .map(|column| column.name.clone())
                .collect::<Vec<_>>();
            let mut derived_scope = scope.branch_with_options(
                alias.this.clone(),
                ScopeType::DerivedTable,
                None,
                None,
                Some(outer_columns),
            );
            build_scope_impl(&alias.this, &mut derived_scope);

            scope.add_source(alias.alias.name.clone(), expr.clone(), true);
            scope.derived_table_scopes.push(derived_scope);
            Some(alias.alias.name.clone())
        }
        Expression::Lateral(lateral) => {
            if let Some(alias) = &lateral.alias {
                scope.add_virtual_source(alias.clone(), expr.clone());
            }
            lateral.alias.clone()
        }
        Expression::LateralView(lateral_view) => {
            add_lateral_view_to_scope(lateral_view, scope);
            lateral_view
                .table_alias
                .as_ref()
                .or_else(|| lateral_view.column_aliases.first())
                .map(|alias| alias.name.clone())
        }
        Expression::Pivot(pivot) => {
            let name =
                pivot_source_name(&pivot.this, pivot.alias.as_ref().map(|a| a.name.as_str()));
            scope.add_source_info(
                name.clone(),
                SourceInfo::new(expr.clone(), false, SourceKind::DerivedTable),
            );
            add_pivot_inner_scope(&pivot.this, scope);
            Some(name)
        }
        Expression::Unpivot(unpivot) => {
            let name = pivot_source_name(
                &unpivot.this,
                unpivot.alias.as_ref().map(|a| a.name.as_str()),
            );
            scope.add_source_info(
                name.clone(),
                SourceInfo::new(expr.clone(), false, SourceKind::DerivedTable),
            );
            add_pivot_inner_scope(&unpivot.this, scope);
            Some(name)
        }
        Expression::Paren(paren) => add_table_to_scope(&paren.this, scope, preceding_sources),
        _ => None,
    }
}

fn is_query_like_relation(expr: &Expression) -> bool {
    match expr {
        Expression::Select(_)
        | Expression::Values(_)
        | Expression::Subquery(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_) => true,
        Expression::Paren(paren) => is_query_like_relation(&paren.this),
        _ => false,
    }
}

fn pivot_source_name(source: &Expression, explicit_alias: Option<&str>) -> String {
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
        Expression::Paren(paren) => pivot_source_name(&paren.this, explicit_alias),
        _ => "_0".to_string(),
    }
}

fn add_pivot_inner_scope(source: &Expression, scope: &mut Scope) {
    match source {
        Expression::Subquery(subquery) => {
            let mut derived_scope = scope.branch(subquery.this.clone(), ScopeType::DerivedTable);
            build_scope_impl(&subquery.this, &mut derived_scope);
            scope.derived_table_scopes.push(derived_scope);
        }
        Expression::Paren(paren) => add_pivot_inner_scope(&paren.this, scope),
        _ => {}
    }
}

fn add_lateral_view_to_scope(lateral_view: &crate::expressions::LateralView, scope: &mut Scope) {
    let alias = lateral_view
        .table_alias
        .as_ref()
        .or_else(|| lateral_view.column_aliases.first())
        .map(|alias| alias.name.clone());

    if let Some(alias) = alias {
        scope.add_virtual_source(
            alias,
            Expression::LateralView(Box::new(lateral_view.clone())),
        );
    }
}

fn collect_subqueries(expr: &Expression, parent_scope: &mut Scope) {
    if matches!(expr, Expression::Select(_)) {
        use crate::ast_children::ChildPathSegment::{Field, Index};

        // Include JOIN predicates, ORDER BY and QUALIFY as well as projections
        // and WHERE, but do not register FROM/JOIN queries again as scalar
        // subqueries (including derived tables without aliases).
        crate::ast_children::for_each_child(expr, |path, child| {
            if !matches!(
                path,
                [Field("from" | "with" | "lateral_views"), ..]
                    | [Field("joins"), Index(_), Field("this"), ..]
            ) {
                collect_subqueries_in_expr(child, parent_scope);
            }
        });
    }
}

fn collect_subqueries_in_expr(expr: &Expression, parent_scope: &mut Scope) {
    let mut seen = HashSet::new();
    for node in walk_in_scope(expr, false) {
        let query = match node {
            Expression::Subquery(subquery) if subquery.alias.is_none() => Some(&subquery.this),
            Expression::Exists(exists) => Some(&exists.this),
            Expression::In(in_expr) => in_expr.query.as_ref(),
            Expression::Any(quantified) | Expression::All(quantified) => Some(&quantified.subquery),
            _ => None,
        };

        let Some(query) = query else {
            continue;
        };

        let key = query as *const Expression as usize;
        if !seen.insert(key) {
            continue;
        }

        let mut sub_scope = parent_scope.branch(query.clone(), ScopeType::Subquery);
        build_scope_impl(query, &mut sub_scope);
        parent_scope.subquery_scopes.push(sub_scope);
    }
}

/// Walk within a scope, yielding expressions without crossing scope boundaries.
///
/// This iterator visits all nodes in the syntax tree, stopping at nodes that
/// start child scopes (CTEs, derived tables, subqueries in FROM/JOIN).
///
/// # Arguments
/// * `expression` - The expression to walk
/// * `bfs` - If true, uses breadth-first search; otherwise uses depth-first search
///
/// # Returns
/// An iterator over expressions within the scope
pub fn walk_in_scope<'a>(
    expression: &'a Expression,
    bfs: bool,
) -> impl Iterator<Item = &'a Expression> {
    WalkInScopeIter::new(expression, bfs)
}

/// Iterator for walking within a scope
struct WalkInScopeIter<'a> {
    queue: VecDeque<&'a Expression>,
    bfs: bool,
}

impl<'a> WalkInScopeIter<'a> {
    fn new(expression: &'a Expression, bfs: bool) -> Self {
        let mut queue = VecDeque::new();
        queue.push_back(expression);
        Self { queue, bfs }
    }

    fn should_stop_at(&self, expr: &Expression, is_root: bool) -> bool {
        if is_root {
            return false;
        }

        // Stop at CTE definitions
        if matches!(expr, Expression::Cte(_)) {
            return true;
        }

        // Stop at subqueries that are derived tables (in FROM/JOIN)
        if let Expression::Subquery(subquery) = expr {
            if subquery.alias.is_some() {
                return true;
            }
        }

        // Stop at standalone SELECT/UNION/etc that would be subqueries
        if matches!(
            expr,
            Expression::Select(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
        ) {
            return true;
        }

        false
    }

    fn get_children(&self, expr: &'a Expression) -> Vec<&'a Expression> {
        let mut children = Vec::new();

        match expr {
            Expression::Prepare(prepare) => {
                children.push(&prepare.statement);
            }
            Expression::Select(select) => {
                // Walk SELECT expressions
                for e in &select.expressions {
                    children.push(e);
                }
                // Walk FROM (but tables/subqueries create new scopes)
                if let Some(from) = &select.from {
                    for table in &from.expressions {
                        if !self.should_stop_at(table, false) {
                            children.push(table);
                        }
                    }
                }
                // Walk JOINs (but their sources create new scopes)
                for join in &select.joins {
                    if !self.should_stop_at(&join.this, false) {
                        children.push(&join.this);
                    }
                    if let Some(on) = &join.on {
                        children.push(on);
                    }
                    if let Some(condition) = &join.match_condition {
                        children.push(condition);
                    }
                }
                // Walk WHERE
                if let Some(where_clause) = &select.where_clause {
                    children.push(&where_clause.this);
                }
                // Walk GROUP BY
                if let Some(group_by) = &select.group_by {
                    for e in &group_by.expressions {
                        children.push(e);
                    }
                }
                // Walk HAVING
                if let Some(having) = &select.having {
                    children.push(&having.this);
                }
                if let Some(qualify) = &select.qualify {
                    children.push(&qualify.this);
                }
                // Walk ORDER BY
                if let Some(order_by) = &select.order_by {
                    for ord in &order_by.expressions {
                        children.push(&ord.this);
                    }
                }
                // Walk LIMIT
                if let Some(limit) = &select.limit {
                    children.push(&limit.this);
                }
                // Walk OFFSET
                if let Some(offset) = &select.offset {
                    children.push(&offset.this);
                }
            }
            Expression::And(bin)
            | Expression::Or(bin)
            | Expression::Add(bin)
            | Expression::Sub(bin)
            | Expression::Mul(bin)
            | Expression::Div(bin)
            | Expression::Mod(bin)
            | Expression::Eq(bin)
            | Expression::Neq(bin)
            | Expression::Lt(bin)
            | Expression::Lte(bin)
            | Expression::Gt(bin)
            | Expression::Gte(bin)
            | Expression::BitwiseAnd(bin)
            | Expression::BitwiseOr(bin)
            | Expression::BitwiseXor(bin)
            | Expression::Concat(bin) => {
                children.push(&bin.left);
                children.push(&bin.right);
            }
            Expression::Like(like) | Expression::ILike(like) => {
                children.push(&like.left);
                children.push(&like.right);
                if let Some(escape) = &like.escape {
                    children.push(escape);
                }
            }
            Expression::Not(un) | Expression::Neg(un) | Expression::BitwiseNot(un) => {
                children.push(&un.this);
            }
            Expression::Function(func) => {
                for arg in &func.args {
                    children.push(arg);
                }
            }
            Expression::AggregateFunction(agg) => {
                for arg in &agg.args {
                    children.push(arg);
                }
            }
            Expression::WindowFunction(wf) => {
                children.push(&wf.this);
                for e in &wf.over.partition_by {
                    children.push(e);
                }
                for e in &wf.over.order_by {
                    children.push(&e.this);
                }
            }
            Expression::Alias(alias) => {
                children.push(&alias.this);
            }
            Expression::Case(case) => {
                if let Some(operand) = &case.operand {
                    children.push(operand);
                }
                for (when_expr, then_expr) in &case.whens {
                    children.push(when_expr);
                    children.push(then_expr);
                }
                if let Some(else_clause) = &case.else_ {
                    children.push(else_clause);
                }
            }
            Expression::Paren(paren) => {
                children.push(&paren.this);
            }
            Expression::Ordered(ord) => {
                children.push(&ord.this);
            }
            Expression::In(in_expr) => {
                children.push(&in_expr.this);
                for e in &in_expr.expressions {
                    children.push(e);
                }
                // Note: in_expr.query creates a new scope - don't traverse
            }
            Expression::Between(between) => {
                children.push(&between.this);
                children.push(&between.low);
                children.push(&between.high);
            }
            Expression::IsNull(is_null) => {
                children.push(&is_null.this);
            }
            Expression::Cast(cast) => {
                children.push(&cast.this);
            }
            Expression::Extract(extract) => {
                children.push(&extract.this);
            }
            Expression::Coalesce(coalesce) => {
                for e in &coalesce.expressions {
                    children.push(e);
                }
            }
            Expression::NullIf(nullif) => {
                children.push(&nullif.this);
                children.push(&nullif.expression);
            }
            Expression::Table(_table) => {
                // Tables don't have child expressions to traverse within scope
                // (joins are handled at the Select level)
            }
            Expression::TryCatch(try_catch) => {
                for stmt in &try_catch.try_body {
                    children.push(stmt);
                }
                if let Some(catch_body) = &try_catch.catch_body {
                    for stmt in catch_body {
                        children.push(stmt);
                    }
                }
            }
            Expression::Column(_) | Expression::Literal(_) | Expression::Identifier(_) => {
                // Leaf nodes - no children
            }
            // Subqueries and Exists create new scopes - don't traverse into them
            Expression::Subquery(_) | Expression::Exists(_) => {}
            _ => {
                // Use the canonical AST traversal for other scalar expressions
                // (typed functions, field access, filters, etc.). Scope
                // boundaries are still enforced by should_stop_at.
                children.extend(expr.children());
            }
        }

        children
    }
}

impl<'a> Iterator for WalkInScopeIter<'a> {
    type Item = &'a Expression;

    fn next(&mut self) -> Option<Self::Item> {
        let expr = if self.bfs {
            self.queue.pop_front()?
        } else {
            self.queue.pop_back()?
        };

        // Get children that don't cross scope boundaries
        let children = self.get_children(expr);

        if self.bfs {
            for child in children {
                if !self.should_stop_at(child, false) {
                    self.queue.push_back(child);
                }
            }
        } else {
            for child in children.into_iter().rev() {
                if !self.should_stop_at(child, false) {
                    self.queue.push_back(child);
                }
            }
        }

        Some(expr)
    }
}

/// Find the first expression matching the predicate within this scope.
///
/// This does NOT traverse into subscopes.
///
/// # Arguments
/// * `expression` - The root expression
/// * `predicate` - Function that returns true for matching expressions
/// * `bfs` - If true, uses breadth-first search; otherwise depth-first
///
/// # Returns
/// The first matching expression, or None
pub fn find_in_scope<'a, F>(
    expression: &'a Expression,
    predicate: F,
    bfs: bool,
) -> Option<&'a Expression>
where
    F: Fn(&Expression) -> bool,
{
    walk_in_scope(expression, bfs).find(|e| predicate(e))
}

/// Find all expressions matching the predicate within this scope.
///
/// This does NOT traverse into subscopes.
///
/// # Arguments
/// * `expression` - The root expression
/// * `predicate` - Function that returns true for matching expressions
/// * `bfs` - If true, uses breadth-first search; otherwise depth-first
///
/// # Returns
/// A vector of matching expressions
pub fn find_all_in_scope<'a, F>(
    expression: &'a Expression,
    predicate: F,
    bfs: bool,
) -> Vec<&'a Expression>
where
    F: Fn(&Expression) -> bool,
{
    walk_in_scope(expression, bfs)
        .filter(|e| predicate(e))
        .collect()
}

/// Traverse an expression by its "scopes".
///
/// Returns a list of all scopes in depth-first post-order.
///
/// # Arguments
/// * `expression` - The expression to traverse
///
/// # Returns
/// A vector of all scopes found
pub fn traverse_scope(expression: &Expression) -> Vec<Scope> {
    match expression {
        Expression::Select(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_)
        | Expression::Prepare(_)
        | Expression::CreateTable(_) => {
            let root = build_scope(expression);
            root.traverse().into_iter().cloned().collect()
        }
        _ => Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;

    fn parse_and_build_scope(sql: &str) -> Scope {
        let ast = Parser::parse_sql(sql).expect("Failed to parse SQL");
        build_scope(&ast[0])
    }

    #[test]
    fn test_simple_select_scope() {
        let mut scope = parse_and_build_scope("SELECT a, b FROM t");

        assert!(scope.is_root());
        assert!(!scope.can_be_correlated);
        assert!(scope.sources.contains_key("t"));

        let columns = scope.columns();
        assert_eq!(columns.len(), 2);
    }

    #[test]
    fn test_derived_table_scope() {
        let mut scope = parse_and_build_scope("SELECT x.a FROM (SELECT a FROM t) AS x");

        assert!(scope.sources.contains_key("x"));
        assert_eq!(scope.derived_table_scopes.len(), 1);

        let derived = &mut scope.derived_table_scopes[0];
        assert!(derived.is_derived_table());
        assert!(derived.sources.contains_key("t"));
    }

    #[test]
    fn test_lateral_preceding_sources_472() {
        let scope = parse_and_build_scope(
            "WITH c AS (SELECT a FROM t) SELECT n.a FROM t, LATERAL (SELECT t.a) n, c",
        );
        let lateral = &scope.derived_table_scopes[0];
        assert!(lateral.is_lateral);
        assert!(lateral.can_be_correlated);
        assert_eq!(lateral.lateral_sources.len(), 1);
        assert!(lateral.lateral_sources.contains_key("t"));
        assert!(!lateral.sources.contains_key("t"));
        let scope = parse_and_build_scope("SELECT n.a FROM t, (SELECT t.a) n");
        assert!(!scope.derived_table_scopes[0].is_lateral);
        assert!(scope.derived_table_scopes[0].lateral_sources.is_empty());
    }

    #[test]
    fn test_subquery_collection_respects_relation_boundaries() {
        let scope = parse_and_build_scope(
            "SELECT t.a FROM t JOIN (SELECT a FROM s) ON t.a = 1 \
             WHERE EXISTS (SELECT 1 FROM u) ORDER BY (SELECT MAX(a) FROM v)",
        );
        assert_eq!(scope.derived_table_scopes.len(), 1);
        assert_eq!(scope.subquery_scopes.len(), 2);
        let scope = parse_and_build_scope(
            "SELECT t.a FROM t JOIN s ON EXISTS (SELECT 1 FROM u WHERE u.a = t.a)",
        );
        assert_eq!(scope.subquery_scopes.len(), 1);
        assert!(scope.subquery_scopes[0].sources.contains_key("u"));
    }

    #[test]
    fn test_non_correlated_subquery() {
        let mut scope = parse_and_build_scope("SELECT * FROM t WHERE EXISTS (SELECT b FROM s)");

        assert_eq!(scope.subquery_scopes.len(), 1);

        let subquery = &mut scope.subquery_scopes[0];
        assert!(subquery.is_subquery());
        assert!(subquery.can_be_correlated);

        // The subquery references only 's', which is in its own sources
        assert!(subquery.sources.contains_key("s"));
        assert!(!subquery.is_correlated_subquery());
    }

    #[test]
    fn test_correlated_subquery() {
        let mut scope =
            parse_and_build_scope("SELECT * FROM t WHERE EXISTS (SELECT b FROM s WHERE s.x = t.y)");

        assert_eq!(scope.subquery_scopes.len(), 1);

        let subquery = &mut scope.subquery_scopes[0];
        assert!(subquery.is_subquery());
        assert!(subquery.can_be_correlated);

        // The subquery references 't.y' which is external
        let external = subquery.external_columns();
        assert!(!external.is_empty());
        assert!(external.iter().any(|c| c.table.as_deref() == Some("t")));
        assert!(subquery.is_correlated_subquery());
    }

    #[test]
    fn test_cte_scope() {
        let scope = parse_and_build_scope("WITH cte AS (SELECT a FROM t) SELECT * FROM cte");

        assert_eq!(scope.cte_scopes.len(), 1);
        assert!(scope.cte_sources.contains_key("cte"));

        let cte = &scope.cte_scopes[0];
        assert!(cte.is_cte());
    }

    #[test]
    fn test_multiple_sources() {
        let scope = parse_and_build_scope("SELECT t.a, s.b FROM t JOIN s ON t.id = s.id");

        assert!(scope.sources.contains_key("t"));
        assert!(scope.sources.contains_key("s"));
        assert_eq!(scope.sources.len(), 2);
    }

    #[test]
    fn test_aliased_table() {
        let scope = parse_and_build_scope("SELECT x.a FROM t AS x");

        // Should be indexed by alias, not original name
        assert!(scope.sources.contains_key("x"));
        assert!(!scope.sources.contains_key("t"));
    }

    #[test]
    fn test_local_columns() {
        let mut scope = parse_and_build_scope("SELECT t.a, t.b, s.c FROM t JOIN s ON t.id = s.id");

        let local = scope.local_columns();
        // All columns are local since both t and s are in scope.
        // Includes JOIN ON references (t.id, s.id).
        assert_eq!(local.len(), 5);
        assert!(local.iter().all(|c| c.table.is_some()));
    }

    #[test]
    fn test_columns_include_join_on_clause_references() {
        let mut scope = parse_and_build_scope(
            "SELECT o.total FROM orders o JOIN customers c ON c.id = o.customer_id",
        );

        let cols: Vec<String> = scope
            .columns()
            .iter()
            .map(|c| match &c.table {
                Some(t) => format!("{}.{}", t, c.name),
                None => c.name.clone(),
            })
            .collect();

        assert!(cols.contains(&"o.total".to_string()));
        assert!(cols.contains(&"c.id".to_string()));
        assert!(cols.contains(&"o.customer_id".to_string()));
    }

    #[test]
    fn test_unqualified_columns() {
        let mut scope = parse_and_build_scope("SELECT a, b, t.c FROM t");

        let unqualified = scope.unqualified_columns();
        // Only a and b are unqualified
        assert_eq!(unqualified.len(), 2);
        assert!(unqualified.iter().all(|c| c.table.is_none()));
    }

    #[test]
    fn test_source_columns() {
        let mut scope = parse_and_build_scope("SELECT t.a, t.b, s.c FROM t JOIN s ON t.id = s.id");

        let t_cols = scope.source_columns("t");
        // t.a, t.b, and t.id from JOIN condition
        assert!(t_cols.len() >= 2);
        assert!(t_cols.iter().all(|c| c.table.as_deref() == Some("t")));

        let s_cols = scope.source_columns("s");
        // s.c and s.id from JOIN condition
        assert!(s_cols.len() >= 1);
        assert!(s_cols.iter().all(|c| c.table.as_deref() == Some("s")));
    }

    #[test]
    fn test_rename_source() {
        let mut scope = parse_and_build_scope("SELECT a FROM t");

        assert!(scope.sources.contains_key("t"));
        scope.rename_source("t", "new_name".to_string());
        assert!(!scope.sources.contains_key("t"));
        assert!(scope.sources.contains_key("new_name"));
    }

    #[test]
    fn test_remove_source() {
        let mut scope = parse_and_build_scope("SELECT a FROM t");

        assert!(scope.sources.contains_key("t"));
        scope.remove_source("t");
        assert!(!scope.sources.contains_key("t"));
    }

    #[test]
    fn test_walk_in_scope() {
        let ast = Parser::parse_sql("SELECT a, b FROM t WHERE a > 1").expect("Failed to parse");
        let expr = &ast[0];

        // Walk should visit all expressions within the scope
        let walked: Vec<_> = walk_in_scope(expr, true).collect();
        assert!(!walked.is_empty());

        // Should include the root SELECT
        assert!(walked.iter().any(|e| matches!(e, Expression::Select(_))));
        // Should include columns
        assert!(walked.iter().any(|e| matches!(e, Expression::Column(_))));
    }

    #[test]
    fn test_find_in_scope() {
        let ast = Parser::parse_sql("SELECT a, b FROM t WHERE a > 1").expect("Failed to parse");
        let expr = &ast[0];

        // Find the first column
        let found = find_in_scope(expr, |e| matches!(e, Expression::Column(_)), true);
        assert!(found.is_some());
        assert!(matches!(found.unwrap(), Expression::Column(_)));
    }

    #[test]
    fn test_find_all_in_scope() {
        let ast = Parser::parse_sql("SELECT a, b, c FROM t").expect("Failed to parse");
        let expr = &ast[0];

        // Find all columns
        let found = find_all_in_scope(expr, |e| matches!(e, Expression::Column(_)), true);
        assert_eq!(found.len(), 3);
    }

    #[test]
    fn test_traverse_scope() {
        let ast =
            Parser::parse_sql("SELECT a FROM (SELECT b FROM t) AS x").expect("Failed to parse");
        let expr = &ast[0];

        let scopes = traverse_scope(expr);
        // traverse_scope returns all scopes via Scope::traverse
        // which includes derived table and root scopes
        assert!(!scopes.is_empty());
        // The root scope is always included
        assert!(scopes.iter().any(|s| s.is_root()));
    }

    #[test]
    fn test_branch_with_options() {
        let ast = Parser::parse_sql("SELECT a FROM t").expect("Failed to parse");
        let scope = build_scope(&ast[0]);

        let child = scope.branch_with_options(
            ast[0].clone(),
            ScopeType::Subquery, // Use Subquery to test can_be_correlated
            None,
            None,
            Some(vec!["col1".to_string(), "col2".to_string()]),
        );

        assert_eq!(child.outer_columns, vec!["col1", "col2"]);
        assert!(child.can_be_correlated); // Subqueries are correlated
    }

    #[test]
    fn test_is_udtf() {
        let ast = Parser::parse_sql("SELECT a FROM t").expect("Failed to parse");
        let scope = Scope::new(ast[0].clone());
        assert!(!scope.is_udtf());

        let root = build_scope(&ast[0]);
        let udtf_scope = root.branch(ast[0].clone(), ScopeType::Udtf);
        assert!(udtf_scope.is_udtf());
    }

    #[test]
    fn test_is_union() {
        let scope = parse_and_build_scope("SELECT a FROM t UNION SELECT b FROM s");

        assert!(scope.is_root());
        assert_eq!(scope.union_scopes.len(), 2);
        // The children are set operation scopes
        assert!(scope.union_scopes[0].is_union());
        assert!(scope.union_scopes[1].is_union());
    }

    #[test]
    fn test_union_output_columns() {
        let scope = parse_and_build_scope(
            "SELECT id, name FROM customers UNION ALL SELECT id, name FROM employees",
        );
        assert_eq!(scope.output_columns(), vec!["id", "name"]);
    }

    #[test]
    fn test_clear_cache() {
        let mut scope = parse_and_build_scope("SELECT t.a FROM t");

        // First call populates cache
        let _ = scope.columns();
        assert!(scope.columns_cache.is_some());

        // Clear cache
        scope.clear_cache();
        assert!(scope.columns_cache.is_none());
        assert!(scope.external_columns_cache.is_none());
    }

    #[test]
    fn test_scope_traverse() {
        let scope = parse_and_build_scope(
            "WITH cte AS (SELECT a FROM t) SELECT * FROM cte WHERE EXISTS (SELECT b FROM s)",
        );

        let traversed = scope.traverse();
        // Should include: CTE scope, subquery scope, root scope
        assert!(traversed.len() >= 3);
    }

    #[test]
    fn test_create_table_as_select_scope() {
        // Simple CTAS
        let scope = parse_and_build_scope("CREATE TABLE out_table AS SELECT 1 AS id FROM src");
        assert!(
            scope.sources.contains_key("src"),
            "CTAS scope should contain the FROM table"
        );
        assert!(
            !scope.sources.contains_key("out_table"),
            "CTAS target table should not be treated as a source"
        );

        // CTAS with multiple FROM tables
        let scope = parse_and_build_scope(
            "CREATE TABLE out_table AS SELECT a.id FROM foo AS a JOIN bar AS b ON a.id = b.id",
        );
        assert!(scope.sources.contains_key("a"));
        assert!(scope.sources.contains_key("b"));
        assert!(
            !scope.sources.contains_key("out_table"),
            "CTAS target table should not be treated as a source"
        );

        // CTAS with CTEs
        let scope = parse_and_build_scope(
            "CREATE TABLE out_table AS WITH cte AS (SELECT 1 AS id FROM src) SELECT * FROM cte",
        );
        assert!(
            scope.sources.contains_key("cte"),
            "CTAS with CTE should resolve CTE as source"
        );
        assert!(
            !scope.sources.contains_key("out_table"),
            "CTAS target table should not be treated as a source"
        );
        assert_eq!(scope.cte_scopes.len(), 1);
    }

    #[test]
    fn test_create_table_as_select_traverse() {
        let ast = Parser::parse_sql("CREATE TABLE t AS SELECT a FROM src").unwrap();
        let scopes = traverse_scope(&ast[0]);
        assert!(
            !scopes.is_empty(),
            "traverse_scope should return scopes for CTAS"
        );
    }
}
