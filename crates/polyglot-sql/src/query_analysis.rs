//! Compact query analysis facts.
//!
//! This module intentionally builds on the existing parser, scope builder, type
//! annotator, and lineage implementation. It is a convenience API: callers that
//! need the full AST or full lineage graph should continue using those lower
//! level APIs directly.

use crate::ast_transforms::get_output_column_names_for_dialect;
use crate::dialects::{Dialect, DialectType};
use crate::expressions::{DataType, Expression, Identifier, JoinKind, TableRef, With};
use crate::lineage::{LineageNode, ScopedLineage};
use crate::optimizer::annotate_types::annotate_types;
use crate::optimizer::qualify_schema_aware_expression;
use crate::schema::{MappingSchema, Schema};
use crate::scope::{build_scope, Scope, SourceInfo, SourceKind};
use crate::traversal::{
    contains_aggregate, is_aggregate, is_arithmetic, is_comparison, is_logical, ExpressionWalk,
};
use crate::validation::{mapping_schema_from_validation_schema_with_dialect, ValidationSchema};
use crate::{parse_one_with_options, ComplexityGuardOptions, Error, ParseOptions, Result};
use serde::{Deserialize, Serialize};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};

mod column_uses;

/// Options for [`analyze_query`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default, deny_unknown_fields)]
pub struct AnalyzeQueryOptions {
    /// Per-call limits used for parsing and rendering analysis facts.
    pub complexity_guard: Option<ComplexityGuardOptions>,
    /// SQL dialect used for parsing and dialect-aware rendering.
    pub dialect: DialectType,
    /// Optional validation schema used for qualification and type annotation.
    pub schema: Option<ValidationSchema>,
}

/// Compact facts about a query's output shape and data dependencies.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryAnalysis {
    pub shape: QueryShape,
    pub ctes: Vec<String>,
    pub cte_facts: Vec<CteFact>,
    pub projections: Vec<ProjectionFact>,
    pub relations: Vec<RelationFact>,
    pub base_tables: Vec<RelationFact>,
    pub star_projections: Vec<StarProjectionFact>,
    pub set_operations: Vec<SetOperationFact>,
    /// Clause-specific uses, separate from output projection lineage.
    #[serde(default)]
    pub column_uses: Vec<ColumnUseFact>,
}

/// A half-open range in the original SQL, measured in Unicode characters.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct QuerySourceSpan {
    pub start: usize,
    pub end: usize,
}

/// The syntactic role of a column-containing expression.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ColumnUseContext {
    Join,
    Filter,
    Group,
    Having,
    Qualify,
    WindowPartition,
    WindowOrder,
    WindowFrame,
    Order,
    AggregateOrder,
    SetOperationFilter,
}

/// One resolved dependency of an original column occurrence. Several terminal
/// dependencies can share a span (for example, a reference to a computed CTE
/// column). Spans identify the use, not the upstream column's definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnUseReferenceFact {
    #[serde(flatten)]
    pub reference: ColumnReferenceFact,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<QuerySourceSpan>,
}

/// One containing expression, with references in occurrence order. Paths are
/// deterministic within an analysis, not persistent IDs across SQL edits.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnUseFact {
    pub context: ColumnUseContext,
    pub scope_path: String,
    pub expression_path: String,
    /// Dialect-rendered SQL; not necessarily the original source substring.
    pub expression_sql: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub span: Option<QuerySourceSpan>,
    pub references: Vec<ColumnUseReferenceFact>,
}

/// Top-level query shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryShape {
    Select,
    SetOperation,
}

/// Compact fact about one output projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProjectionFact {
    pub index: usize,
    pub name: Option<String>,
    pub is_star: bool,
    pub star_table: Option<String>,
    pub transform_kind: TransformKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transform_function: Option<TransformFunctionFact>,
    pub cast_type: Option<String>,
    pub type_hint: Option<String>,
    pub nullability: ProjectionNullability,
    pub upstream: Vec<ColumnReferenceFact>,
}

/// Compact fact about a function-like projection transform.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransformFunctionFact {
    pub name: String,
    pub literal_args: Vec<String>,
    pub column_args: Vec<ColumnReferenceFact>,
}

/// Compact fact about one top-level CTE definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CteFact {
    pub name: String,
    pub columns: Vec<String>,
    pub body_sql: String,
    pub output_columns: Vec<String>,
}

/// Compact fact about one original star projection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StarProjectionFact {
    pub index: usize,
    pub table: Option<String>,
    pub expanded_columns: Vec<String>,
}

/// Compact fact about an upstream column reference.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnReferenceFact {
    pub source_name: Option<String>,
    pub source_alias: Option<String>,
    pub source_kind: SourceKind,
    pub table: Option<String>,
    pub column: String,
    pub unqualified: bool,
    pub confidence: ReferenceConfidence,
    /// Strongest transform on the lineage path from this reference to the
    /// projection output, through CTEs, derived tables, and set-operation
    /// branches. Steps rank `Direct < Star < Cast < Expression < Aggregation`;
    /// a `Constant` step has no upstream reference. Only projection upstream
    /// references carry this field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path_transform_kind: Option<TransformKind>,
    /// Function names on the same path, from the output to the source, in the
    /// dialect spelling (`ROUND`, `SUM`, `ROW_NUMBER`). Operators, `CASE`, and
    /// casts are not functions.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub path_functions: Vec<String>,
    /// True when a step on the path is a window function.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub path_window: bool,
}

/// Compact fact about a relation visible in the root scope.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RelationFact {
    pub name: String,
    pub alias: Option<String>,
    pub kind: SourceKind,
    pub columns: Vec<String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub table: Option<String>,
}

/// Compact fact about a set operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetOperationFact {
    pub kind: String,
    pub all: bool,
    pub distinct: bool,
    pub output_columns: Vec<String>,
    pub branches: Vec<SetOperationBranchFact>,
}

/// Compact facts for one immediate set-operation branch.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SetOperationBranchFact {
    pub index: usize,
    pub role: SetOperationBranchRole,
    pub projections: Vec<ProjectionFact>,
}

/// Semantic contribution of one set-operation branch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SetOperationBranchRole {
    Value,
    Filter,
}

/// High-level kind of transformation performed by a projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TransformKind {
    Direct,
    Cast,
    Aggregation,
    Constant,
    Expression,
    Star,
}

/// Confidence level for a compact upstream column reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReferenceConfidence {
    Resolved,
    Ambiguous,
    Unknown,
}

/// Conservative nullability classification for one output projection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionNullability {
    NonNull,
    Nullable,
    Unknown,
}

/// Analyze a single SELECT or set-operation query.
pub fn analyze_query(sql: &str, options: AnalyzeQueryOptions) -> Result<QueryAnalysis> {
    let mut expression = parse_one_with_options(
        sql,
        options.dialect,
        &ParseOptions {
            complexity_guard: options.complexity_guard,
        },
    )?;
    expression = effective_query(expression);
    ensure_query(&expression)?;
    expression = crate::binding::bind_lambdas(expression, options.dialect);
    let original_expression = expression.clone();

    let mapping_schema = options
        .schema
        .as_ref()
        .map(|schema| analysis_mapping_schema(schema, options.dialect));
    let schema_info = options.schema.as_ref().map(AnalysisSchemaInfo::from_schema);
    let cte_facts = top_level_cte_facts(
        &original_expression,
        options.dialect,
        options.complexity_guard,
    )?;
    if let Some(schema) = mapping_schema.as_ref() {
        use crate::optimizer::qualify_columns::QualifyColumnsError;
        expression = match qualify_schema_aware_expression(
            expression.clone(),
            schema,
            Some(options.dialect),
        ) {
            Ok(qualified) => qualified,
            // Analysis is not validation. Incomplete schemas and unresolved
            // lexical references must still yield conservative usage facts.
            Err(
                QualifyColumnsError::UnknownTable(_)
                | QualifyColumnsError::UnknownColumn(_)
                | QualifyColumnsError::AmbiguousColumn(_)
                | QualifyColumnsError::ColumnNotResolved { .. },
            ) => expression,
            Err(error) => {
                return Err(Error::internal(format!(
                    "query analysis qualification failed: {error}"
                )))
            }
        };
    }

    crate::lineage::expand_cte_stars(
        &mut expression,
        mapping_schema.as_ref().map(|schema| schema as &dyn Schema),
    );
    let star_projections = star_projection_facts(
        &original_expression,
        &expression,
        mapping_schema.as_ref(),
        options.dialect,
    );
    annotate_types(
        &mut expression,
        mapping_schema.as_ref().map(|schema| schema as &dyn Schema),
        Some(options.dialect),
    );

    let scope = build_scope(&expression);
    let original_scope = build_scope(&original_expression);
    let empty_schema = MappingSchema::with_dialect(options.dialect);
    let mut uncertain_columns = HashMap::new();
    column_uses::collect_uncertain_occurrences(
        &original_scope,
        mapping_schema.as_ref().unwrap_or(&empty_schema),
        &mut uncertain_columns,
    );
    let nullability_context = NullabilityContext::new(
        &scope,
        schema_info.as_ref(),
        mapping_schema.as_ref(),
        options.dialect,
        &uncertain_columns,
    );
    let shape = if is_set_operation(&expression) {
        QueryShape::SetOperation
    } else {
        QueryShape::Select
    };

    Ok(QueryAnalysis {
        shape,
        ctes: collect_cte_names(&expression),
        cte_facts,
        projections: projection_facts_for_query(
            &expression,
            &scope,
            options.dialect,
            &nullability_context,
        ),
        relations: relation_facts(&scope, mapping_schema.as_ref(), options.dialect),
        base_tables: base_table_facts(&scope, mapping_schema.as_ref(), options.dialect),
        star_projections,
        set_operations: set_operation_facts(
            &expression,
            &scope,
            options.dialect,
            &nullability_context,
        ),
        column_uses: column_uses::collect(
            &original_scope,
            &scope,
            mapping_schema.as_ref(),
            options.dialect,
            &uncertain_columns,
            options.complexity_guard,
        ),
    })
}

fn analysis_mapping_schema(schema: &ValidationSchema, dialect: DialectType) -> MappingSchema {
    mapping_schema_from_validation_schema_with_dialect(schema, dialect)
}

fn validation_table_names(table: &crate::validation::SchemaTable) -> Vec<String> {
    let mut names = Vec::new();

    names.push(table.name.to_ascii_lowercase());
    if let Some(schema_name) = &table.schema {
        names.push(format!(
            "{}.{}",
            schema_name.to_ascii_lowercase(),
            table.name.to_ascii_lowercase()
        ));
    }
    for alias in &table.aliases {
        names.push(alias.to_ascii_lowercase());
    }

    names.sort();
    names.dedup();
    names
}

#[derive(Debug, Clone)]
struct AnalysisColumnInfo {
    nullable: Option<bool>,
    primary_key: bool,
}

#[derive(Debug, Clone)]
struct AnalysisSchemaInfo {
    columns: HashMap<(String, String), AnalysisColumnInfo>,
}

impl AnalysisSchemaInfo {
    fn from_schema(schema: &ValidationSchema) -> Self {
        let mut columns = HashMap::new();

        for table in &schema.tables {
            let table_names = validation_table_names(table);
            let primary_keys: HashSet<String> = table
                .primary_key
                .iter()
                .map(|column| column.to_ascii_lowercase())
                .collect();

            for column in &table.columns {
                let info = AnalysisColumnInfo {
                    nullable: column.nullable,
                    primary_key: column.primary_key
                        || primary_keys.contains(&column.name.to_ascii_lowercase()),
                };

                for table_name in &table_names {
                    columns.insert(
                        (
                            normalize_lookup_name(table_name),
                            normalize_lookup_name(&column.name),
                        ),
                        info.clone(),
                    );
                }
            }
        }

        Self { columns }
    }

    fn column(&self, table: &str, column: &str) -> Option<&AnalysisColumnInfo> {
        self.columns
            .get(&(normalize_lookup_name(table), normalize_lookup_name(column)))
    }
}

struct NullabilityContext<'a> {
    lineage: RefCell<HashMap<*const Scope, ScopedLineage>>,
    output_types: RefCell<crate::optimizer::set_operation_types::OutputResolver>,
    schema: Option<&'a AnalysisSchemaInfo>,
    mapping_schema: Option<&'a MappingSchema>,
    empty_schema: MappingSchema,
    dialect: DialectType,
    scopes: Vec<NullabilityScope<'a>>,
    scope_ids: HashMap<*const Scope, usize>,
    outputs: RefCell<HashMap<(usize, usize), ProjectionNullability>>,
    resolving: RefCell<HashSet<(usize, usize)>>,
    uncertain_columns: &'a HashMap<(usize, usize), ReferenceConfidence>,
}

struct NullabilityScope<'a> {
    scope: &'a Scope,
    selected: Scope,
    bindings: HashMap<String, &'a Expression>,
    ctes: HashMap<String, usize>,
    derived: Vec<usize>,
    branches: Vec<usize>,
    nullable_sources: HashSet<String>,
}

impl<'a> NullabilityContext<'a> {
    fn new(
        scope: &'a Scope,
        schema: Option<&'a AnalysisSchemaInfo>,
        mapping_schema: Option<&'a MappingSchema>,
        dialect: DialectType,
        uncertain_columns: &'a HashMap<(usize, usize), ReferenceConfidence>,
    ) -> Self {
        let mut context = Self {
            lineage: RefCell::new(HashMap::new()),
            output_types: RefCell::new(crate::optimizer::set_operation_types::OutputResolver::new(
                dialect,
            )),
            schema,
            mapping_schema,
            empty_schema: MappingSchema::with_dialect(dialect),
            dialect,
            scopes: Vec::new(),
            scope_ids: HashMap::new(),
            outputs: RefCell::new(HashMap::new()),
            resolving: RefCell::new(HashSet::new()),
            uncertain_columns,
        };
        context.index_scope(scope, HashMap::new(), None);
        context
    }

    // Keep each CTE's definition environment, rather than resolving it in the
    // environment of a later consumer that may shadow the same names.
    fn index_scope(
        &mut self,
        scope: &'a Scope,
        mut ctes: HashMap<String, usize>,
        recursive_name: Option<&Identifier>,
    ) -> usize {
        let id = self.scopes.len();
        self.scope_ids.insert(scope, id);
        if let Some(name) = recursive_name {
            ctes.insert(
                crate::set_operation::identifier_key(name, Some(self.dialect)),
                id,
            );
        }
        let query = crate::scope::scope_query(&scope.expression);
        let bindings: HashMap<_, _> = if let Expression::Select(select) = query {
            select
                .from
                .iter()
                .flat_map(|from| &from.expressions)
                .chain(select.joins.iter().map(|join| &join.this))
                .filter_map(|expression| {
                    expression_source_name(expression).map(|name| (name, expression))
                })
                .collect()
        } else {
            HashMap::new()
        };
        self.scopes.push(NullabilityScope {
            scope,
            selected: crate::scope::selected_reference_scope(scope),
            bindings,
            ctes: HashMap::new(),
            derived: Vec::new(),
            branches: Vec::new(),
            nullable_sources: nullable_source_names(query, self.dialect),
        });
        let recursive = with_clause(crate::scope::scope_query(&scope.expression))
            .is_some_and(|with| with.recursive);
        for child in &scope.cte_scopes {
            if let Expression::Cte(cte) = &child.expression {
                let name = &cte.alias;
                let child_id = self.index_scope(child, ctes.clone(), recursive.then_some(name));
                ctes.insert(
                    crate::set_operation::identifier_key(name, Some(self.dialect)),
                    child_id,
                );
            }
        }
        self.scopes[id].ctes = ctes.clone();
        for child in &scope.derived_table_scopes {
            let child_id = self.index_scope(child, ctes.clone(), None);
            self.scopes[id].derived.push(child_id);
        }
        for child in &scope.union_scopes {
            let child_id = self.index_scope(child, ctes.clone(), None);
            self.scopes[id].branches.push(child_id);
        }
        id
    }
}

fn top_level_cte_facts(
    expression: &Expression,
    dialect: DialectType,
    guard: Option<ComplexityGuardOptions>,
) -> Result<Vec<CteFact>> {
    let Some(with_clause) = with_clause(expression) else {
        return Ok(Vec::new());
    };

    with_clause
        .ctes
        .iter()
        .map(|cte| {
            Ok(CteFact {
                name: cte.alias.name.clone(),
                columns: cte
                    .columns
                    .iter()
                    .map(|column| column.name.clone())
                    .collect(),
                body_sql: Dialect::get(dialect).generate_with_guard(&cte.this, guard)?,
                output_columns: get_output_column_names_for_dialect(&cte.this, Some(dialect)),
            })
        })
        .collect()
}

/// `resolved` is `expression` after qualification and CTE star expansion. Its
/// sources give the columns of derived tables and CTEs that select a star.
fn star_projection_facts(
    expression: &Expression,
    resolved: &Expression,
    mapping_schema: Option<&MappingSchema>,
    dialect: DialectType,
) -> Vec<StarProjectionFact> {
    let scope = build_scope(resolved);
    let ordered_sources = ordered_source_names_for_query(resolved);

    select_expressions_for_query(expression)
        .iter()
        .enumerate()
        .filter_map(|(index, projection)| {
            let inner = unwrap_projection_alias(projection);
            if !projection_is_star(inner) {
                return None;
            }

            let table = projection_star_table(inner);
            let mut expanded_columns = match inner {
                Expression::Star(star) if star.has_except_pattern() => Vec::new(),
                _ => expanded_star_columns(
                    table.as_deref(),
                    &scope,
                    &ordered_sources,
                    mapping_schema,
                    dialect,
                ),
            };
            if let Expression::Star(star) = inner {
                expanded_columns = expanded_columns
                    .into_iter()
                    .filter(|column| !star.excludes(column, Some(dialect)))
                    .map(|column| {
                        star.renamed(&column, Some(dialect))
                            .map_or(column, |new| new.name)
                    })
                    .collect();
            }

            Some(StarProjectionFact {
                index,
                table,
                expanded_columns,
            })
        })
        .collect()
}

fn expanded_star_columns(
    star_table: Option<&str>,
    scope: &Scope,
    ordered_sources: &[String],
    mapping_schema: Option<&MappingSchema>,
    dialect: DialectType,
) -> Vec<String> {
    let mut columns = Vec::new();
    let mut source_names: Vec<String> = if ordered_sources.is_empty() {
        let mut names: Vec<_> = scope.sources.keys().cloned().collect();
        names.sort();
        names
    } else {
        ordered_sources.to_vec()
    };

    source_names.dedup();

    let empty_schema = MappingSchema::with_dialect(dialect);
    let mut resolver = crate::resolver::Resolver::new(
        scope,
        mapping_schema.map_or(&empty_schema as &dyn Schema, |schema| schema),
        false,
    );
    for source_name in source_names {
        let Some(source) = scope.sources.get(&source_name) else {
            continue;
        };

        if let Some(star_table) = star_table {
            let matches = source_name.eq_ignore_ascii_case(star_table)
                || source
                    .alias
                    .as_deref()
                    .is_some_and(|alias| alias.eq_ignore_ascii_case(star_table))
                || source_table_name(source)
                    .is_some_and(|table| table.eq_ignore_ascii_case(star_table));

            if !matches {
                continue;
            }
        }

        match &source.expression {
            // The resolver derives PIVOT and UNPIVOT outputs from the IN list.
            Expression::Pivot(_) | Expression::Unpivot(_) => columns.extend(
                resolver
                    .get_source_columns(&source_name)
                    .unwrap_or_default(),
            ),
            _ => columns.extend(source_columns(source, mapping_schema, dialect)),
        }
    }

    columns
}

fn ordered_source_names_for_query(expression: &Expression) -> Vec<String> {
    match expression {
        Expression::Select(select) => ordered_source_names_for_select(select),
        Expression::Union(union) => ordered_source_names_for_query(&union.left),
        Expression::Intersect(intersect) => ordered_source_names_for_query(&intersect.left),
        Expression::Except(except) => ordered_source_names_for_query(&except.left),
        Expression::Subquery(subquery) => ordered_source_names_for_query(&subquery.this),
        _ => Vec::new(),
    }
}

fn ordered_source_names_for_select(select: &crate::expressions::Select) -> Vec<String> {
    let mut sources = Vec::new();

    if let Some(from) = &select.from {
        for expression in &from.expressions {
            if let Some(source_name) = expression_source_name(expression) {
                sources.push(source_name);
            }
        }
    }

    for join in &select.joins {
        if let Some(source_name) = expression_source_name(&join.this) {
            sources.push(source_name);
        }
    }

    sources
}

fn nullable_source_names(expression: &Expression, dialect: DialectType) -> HashSet<String> {
    match expression {
        Expression::Select(select) => nullable_source_names_for_select(select, dialect),
        Expression::Union(union) => nullable_source_names(&union.left, dialect),
        Expression::Intersect(intersect) => nullable_source_names(&intersect.left, dialect),
        Expression::Except(except) => nullable_source_names(&except.left, dialect),
        Expression::Subquery(subquery) => nullable_source_names(&subquery.this, dialect),
        _ => HashSet::new(),
    }
}

fn nullable_source_names_for_select(
    select: &crate::expressions::Select,
    dialect: DialectType,
) -> HashSet<String> {
    let mut nullable = HashSet::new();
    let mut left_sources = Vec::new();

    if let Some(from) = &select.from {
        for expression in &from.expressions {
            if let Some(identifier) = expression_source_identifier(expression) {
                left_sources.push(crate::set_operation::identifier_key(
                    identifier,
                    Some(dialect),
                ));
            }
        }
    }

    for join in &select.joins {
        let right_source = expression_source_identifier(&join.this)
            .map(|identifier| crate::set_operation::identifier_key(identifier, Some(dialect)));

        if join_nullable_left(join.kind) {
            for source_name in &left_sources {
                nullable.insert(source_name.clone());
            }
        }

        if join_nullable_right(join.kind) {
            if let Some(source_name) = &right_source {
                nullable.insert(source_name.clone());
            }
        }

        if let Some(source_name) = right_source {
            left_sources.push(source_name);
        }
    }

    nullable
}

fn join_nullable_left(kind: JoinKind) -> bool {
    matches!(
        kind,
        JoinKind::Right
            | JoinKind::NaturalRight
            | JoinKind::AsOfRight
            | JoinKind::Full
            | JoinKind::NaturalFull
            | JoinKind::Outer
    )
}

fn join_nullable_right(kind: JoinKind) -> bool {
    matches!(
        kind,
        JoinKind::Left
            | JoinKind::NaturalLeft
            | JoinKind::AsOfLeft
            | JoinKind::LeftLateral
            | JoinKind::OuterApply
            | JoinKind::LeftArray
            | JoinKind::Full
            | JoinKind::NaturalFull
            | JoinKind::Outer
    )
}

fn expression_source_name(expression: &Expression) -> Option<String> {
    expression_source_identifier(expression).map(|identifier| identifier.name.clone())
}

fn expression_source_identifier(expression: &Expression) -> Option<&Identifier> {
    match expression {
        Expression::Table(table) => Some(table.alias.as_ref().unwrap_or(&table.name)),
        Expression::Subquery(subquery) => subquery.alias.as_ref(),
        Expression::Alias(alias) => Some(&alias.alias),
        Expression::Cte(cte) => Some(&cte.alias),
        _ => None,
    }
}

fn normalize_lookup_name(name: &str) -> String {
    name.to_ascii_lowercase()
}

fn effective_query(expression: Expression) -> Expression {
    match expression {
        Expression::Prepare(prepare) => prepare.statement,
        Expression::Subquery(subquery) if subquery.alias.is_none() => subquery.this,
        other => other,
    }
}

fn ensure_query(expression: &Expression) -> Result<()> {
    if matches!(
        expression,
        Expression::Select(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
    ) {
        Ok(())
    } else {
        Err(Error::internal(
            "analyze_query requires a SELECT or set operation query",
        ))
    }
}

fn is_set_operation(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_)
    )
}

fn collect_cte_names(expression: &Expression) -> Vec<String> {
    let mut names = Vec::new();
    let mut seen = HashSet::new();
    collect_cte_names_inner(expression, &mut names, &mut seen);
    names
}

fn collect_cte_names_inner(
    expression: &Expression,
    names: &mut Vec<String>,
    seen: &mut HashSet<String>,
) {
    if let Some(with_clause) = with_clause(expression) {
        collect_with_names(with_clause, names, seen);
    }

    match expression {
        Expression::Union(union) => {
            collect_cte_names_inner(&union.left, names, seen);
            collect_cte_names_inner(&union.right, names, seen);
        }
        Expression::Intersect(intersect) => {
            collect_cte_names_inner(&intersect.left, names, seen);
            collect_cte_names_inner(&intersect.right, names, seen);
        }
        Expression::Except(except) => {
            collect_cte_names_inner(&except.left, names, seen);
            collect_cte_names_inner(&except.right, names, seen);
        }
        Expression::Subquery(subquery) => collect_cte_names_inner(&subquery.this, names, seen),
        _ => {}
    }
}

fn collect_with_names(with_clause: &With, names: &mut Vec<String>, seen: &mut HashSet<String>) {
    for cte in &with_clause.ctes {
        if seen.insert(cte.alias.name.clone()) {
            names.push(cte.alias.name.clone());
        }
        collect_cte_names_inner(&cte.this, names, seen);
    }
}

fn with_clause(expression: &Expression) -> Option<&With> {
    match expression {
        Expression::Select(select) => select.with.as_ref(),
        Expression::Union(union) => union.with.as_ref(),
        Expression::Intersect(intersect) => intersect.with.as_ref(),
        Expression::Except(except) => except.with.as_ref(),
        _ => None,
    }
}

fn projection_facts_for_query(
    expression: &Expression,
    scope: &Scope,
    dialect: DialectType,
    nullability_context: &NullabilityContext<'_>,
) -> Vec<ProjectionFact> {
    let expressions = projection_sources_for_query(expression, dialect);
    let names = get_output_column_names_for_dialect(expression, Some(dialect));
    let mut type_query = expression;
    loop {
        type_query = match type_query {
            Expression::Subquery(subquery) => &subquery.this,
            Expression::Paren(paren) => &paren.this,
            Expression::Annotated(annotated) => &annotated.this,
            _ => break,
        };
    }
    let resolved_outputs = is_set_operation(type_query).then(|| {
        nullability_context
            .output_types
            .borrow_mut()
            .resolve(expression)
    });

    expressions
        .iter()
        .enumerate()
        .map(|(index, (projection, null_padded))| {
            let mut fact = projection_fact(
                index,
                names
                    .get(index)
                    .cloned()
                    .or_else(|| projection_name(projection)),
                projection,
                expression,
                scope,
                dialect,
                nullability_context,
            );
            if *null_padded {
                fact.nullability = ProjectionNullability::Nullable;
            }
            if let Some(outputs) = &resolved_outputs {
                let output = outputs.get(index);
                fact.type_hint = output
                    .and_then(|output| output.data_type())
                    .and_then(|data_type| render_data_type(data_type, dialect));
                fact.cast_type = output
                    .and_then(|output| output.cast_type.as_ref())
                    .and_then(|data_type| render_data_type(data_type, dialect));
            }
            fact
        })
        .collect()
}

/// Return one representative projection for each result ordinal together with
/// whether any immediate name-aligned branch contributes a synthetic NULL.
fn projection_sources_for_query(
    expression: &Expression,
    dialect: DialectType,
) -> Vec<(&Expression, bool)> {
    match expression {
        Expression::Subquery(subquery) => {
            return projection_sources_for_query(&subquery.this, dialect)
        }
        Expression::Paren(paren) => return projection_sources_for_query(&paren.this, dialect),
        Expression::Annotated(annotated) => {
            return projection_sources_for_query(&annotated.this, dialect)
        }
        _ => {}
    }
    match crate::set_operation::set_operation_layout(expression, Some(dialect)) {
        Ok(Some(layout)) => layout
            .outputs
            .iter()
            .filter_map(|output| {
                let null_padded = output.left_ordinal.is_none() || output.right_ordinal.is_none();
                output
                    .left_ordinal
                    .and_then(|ordinal| {
                        projection_source_for_ordinal(
                            set_operation_left(expression)?,
                            ordinal,
                            dialect,
                        )
                    })
                    .or_else(|| {
                        output.right_ordinal.and_then(|ordinal| {
                            projection_source_for_ordinal(
                                set_operation_right(expression)?,
                                ordinal,
                                dialect,
                            )
                        })
                    })
                    .map(|(projection, nested_null_padded)| {
                        (projection, null_padded || nested_null_padded)
                    })
            })
            .collect(),
        _ => select_expressions_for_query(expression)
            .into_iter()
            .map(|projection| (projection, false))
            .collect(),
    }
}

fn projection_source_for_ordinal(
    expression: &Expression,
    ordinal: usize,
    dialect: DialectType,
) -> Option<(&Expression, bool)> {
    match crate::set_operation::set_operation_layout(expression, Some(dialect)) {
        Ok(Some(layout)) => {
            let output = layout.outputs.get(ordinal)?;
            let null_padded = output.left_ordinal.is_none() || output.right_ordinal.is_none();
            output
                .left_ordinal
                .and_then(|child_ordinal| {
                    projection_source_for_ordinal(
                        set_operation_left(expression)?,
                        child_ordinal,
                        dialect,
                    )
                })
                .or_else(|| {
                    output.right_ordinal.and_then(|child_ordinal| {
                        projection_source_for_ordinal(
                            set_operation_right(expression)?,
                            child_ordinal,
                            dialect,
                        )
                    })
                })
                .map(|(projection, nested_null_padded)| {
                    (projection, null_padded || nested_null_padded)
                })
        }
        _ => match expression {
            Expression::Select(select) => select
                .expressions
                .get(ordinal)
                .map(|projection| (projection, false)),
            Expression::Union(union) => {
                projection_source_for_ordinal(&union.left, ordinal, dialect)
            }
            Expression::Intersect(intersect) => {
                projection_source_for_ordinal(&intersect.left, ordinal, dialect)
            }
            Expression::Except(except) => {
                projection_source_for_ordinal(&except.left, ordinal, dialect)
            }
            Expression::Subquery(subquery) => {
                projection_source_for_ordinal(&subquery.this, ordinal, dialect)
            }
            Expression::Paren(paren) => {
                projection_source_for_ordinal(&paren.this, ordinal, dialect)
            }
            _ => None,
        },
    }
}

fn set_operation_left(expression: &Expression) -> Option<&Expression> {
    match expression {
        Expression::Union(set_op) => Some(&set_op.left),
        Expression::Intersect(set_op) => Some(&set_op.left),
        Expression::Except(set_op) => Some(&set_op.left),
        _ => None,
    }
}

fn set_operation_right(expression: &Expression) -> Option<&Expression> {
    match expression {
        Expression::Union(set_op) => Some(&set_op.right),
        Expression::Intersect(set_op) => Some(&set_op.right),
        Expression::Except(set_op) => Some(&set_op.right),
        _ => None,
    }
}

fn select_expressions_for_query(expression: &Expression) -> Vec<&Expression> {
    match expression {
        Expression::Select(select) => select.expressions.iter().collect(),
        Expression::Union(union) => select_expressions_for_query(&union.left),
        Expression::Intersect(intersect) => select_expressions_for_query(&intersect.left),
        Expression::Except(except) => select_expressions_for_query(&except.left),
        Expression::Subquery(subquery) => select_expressions_for_query(&subquery.this),
        _ => Vec::new(),
    }
}

fn projection_fact(
    index: usize,
    name: Option<String>,
    projection: &Expression,
    _query: &Expression,
    scope: &Scope,
    dialect: DialectType,
    nullability_context: &NullabilityContext<'_>,
) -> ProjectionFact {
    let inner = unwrap_projection_alias(projection);
    let is_star = projection_is_star(inner);
    let kind = transform_kind(inner);
    let mut lineage = nullability_context.lineage.borrow_mut();
    let prepared = lineage
        .entry(scope as *const Scope)
        .or_insert_with(|| ScopedLineage::new(scope.clone(), &[], dialect));
    // A traced node whose leaves are constants has no upstream columns.
    let mut upstream = prepared
        .output(index)
        .ok()
        .filter(|node| !node.downstream.is_empty())
        .map(|node| {
            let uncertain = lineage_uncertainty(&node, nullability_context.uncertain_columns);
            let mut references =
                terminal_references_from_lineage(&node, Some(&PathTransform::new(dialect)));
            if let Some(confidence) = uncertain {
                for reference in &mut references {
                    reference.confidence = confidence;
                }
            }
            references
        })
        .unwrap_or_else(|| fallback_column_references(inner, scope));
    if let Some(confidence) = expression_uncertainty(inner, nullability_context.uncertain_columns) {
        for reference in &mut upstream {
            reference.confidence = confidence;
        }
    }
    // A fallback reference has no traced path, so it takes the projection step.
    if upstream
        .iter()
        .any(|reference| reference.path_transform_kind.is_none())
    {
        let projection_path = PathTransform::new(dialect).step(inner);
        for reference in &mut upstream {
            if reference.path_transform_kind.is_none() {
                projection_path.apply(reference);
            }
        }
    }
    if let Some(schema) = nullability_context.mapping_schema {
        for reference in &mut upstream {
            if let Some(table) = &reference.table {
                if let Ok(columns) = schema.column_names(table) {
                    if !columns.is_empty()
                        && !columns.iter().any(|column| {
                            column == "*"
                                || crate::schema::normalize_name(column, Some(dialect), false, true)
                                    == crate::schema::normalize_name(
                                        &reference.column,
                                        Some(dialect),
                                        false,
                                        true,
                                    )
                        })
                    {
                        reference.confidence = ReferenceConfidence::Unknown;
                    }
                }
            }
        }
    }

    let mut transform_function = transform_function_fact(inner, scope, dialect);
    if let Some(function) = &mut transform_function {
        for reference in &mut function.column_args {
            if let Some(upstream) = upstream.iter().find(|upstream| {
                upstream.column == reference.column && upstream.table == reference.table
            }) {
                reference.confidence = upstream.confidence;
            } else if upstream
                .iter()
                .any(|upstream| upstream.confidence != ReferenceConfidence::Resolved)
            {
                reference.confidence = ReferenceConfidence::Unknown;
            }
        }
    }
    ProjectionFact {
        index,
        name,
        is_star,
        star_table: projection_star_table(inner),
        transform_kind: kind,
        transform_function,
        cast_type: cast_type(inner, dialect),
        type_hint: projection
            .inferred_type()
            .or_else(|| inner.inferred_type())
            .filter(|data_type| **data_type != DataType::Unknown)
            .and_then(|data_type| render_data_type(data_type, dialect)),
        nullability: nullability_context
            .scope_ids
            .get(&(scope as *const Scope))
            .map(|id| nullability_context.output(*id, index, 0))
            .unwrap_or(ProjectionNullability::Unknown),
        upstream,
    }
}

fn expression_uncertainty(
    expression: &Expression,
    uncertain: &HashMap<(usize, usize), ReferenceConfidence>,
) -> Option<ReferenceConfidence> {
    combine_uncertainty(
        crate::scope::walk_in_scope(expression, false).filter_map(|node| match node {
            Expression::Column(column) => column
                .span
                .or(column.name.span)
                .and_then(|span| uncertain.get(&(span.start, span.end)))
                .copied(),
            _ => None,
        }),
    )
}

fn combine_uncertainty(
    confidences: impl Iterator<Item = ReferenceConfidence>,
) -> Option<ReferenceConfidence> {
    confidences.fold(None, |current, confidence| {
        Some(
            if current == Some(ReferenceConfidence::Ambiguous)
                || confidence == ReferenceConfidence::Ambiguous
            {
                ReferenceConfidence::Ambiguous
            } else {
                confidence
            },
        )
    })
}

fn lineage_uncertainty(
    node: &LineageNode,
    uncertain: &HashMap<(usize, usize), ReferenceConfidence>,
) -> Option<ReferenceConfidence> {
    combine_uncertainty(
        node.walk()
            .filter_map(|node| expression_uncertainty(&node.expression, uncertain)),
    )
}

fn transform_function_fact(
    expression: &Expression,
    scope: &Scope,
    dialect: DialectType,
) -> Option<TransformFunctionFact> {
    let mut matches = expression
        .find_all(|candidate| transform_function_fact_for_node(candidate, scope, dialect).is_some())
        .into_iter();

    let first = matches.next()?;
    if matches.next().is_some() {
        return None;
    }

    transform_function_fact_for_node(first, scope, dialect)
}

fn transform_function_fact_for_node(
    expression: &Expression,
    scope: &Scope,
    dialect: DialectType,
) -> Option<TransformFunctionFact> {
    match expression {
        Expression::Function(function) => Some(transform_function_from_args(
            &function.name,
            &function.args,
            scope,
            dialect,
        )),
        Expression::AggregateFunction(function) => Some(transform_function_from_args(
            &function.name,
            &function.args,
            scope,
            dialect,
        )),
        Expression::DateTrunc(function) => Some(transform_function_from_parts(
            "DATE_TRUNC",
            vec![datetime_field_name(&function.unit)],
            vec![&function.this],
            scope,
            dialect,
        )),
        Expression::TimestampTrunc(function) => Some(transform_function_from_parts(
            "TIMESTAMP_TRUNC",
            vec![datetime_field_name(&function.unit)],
            vec![&function.this],
            scope,
            dialect,
        )),
        Expression::TimeTrunc(function) => {
            let mut args = vec![function.this.as_ref()];
            if let Some(zone) = function.zone.as_deref() {
                args.push(zone);
            }
            Some(transform_function_from_parts(
                "TIME_TRUNC",
                vec![function.unit.clone()],
                args,
                scope,
                dialect,
            ))
        }
        Expression::Extract(function) => Some(transform_function_from_parts(
            "EXTRACT",
            vec![datetime_field_name(&function.field)],
            vec![&function.this],
            scope,
            dialect,
        )),
        Expression::DateAdd(function) => Some(transform_function_from_parts(
            "DATE_ADD",
            Vec::new(),
            vec![&function.this, &function.interval],
            scope,
            dialect,
        )),
        Expression::DateSub(function) => Some(transform_function_from_parts(
            "DATE_SUB",
            Vec::new(),
            vec![&function.this, &function.interval],
            scope,
            dialect,
        )),
        Expression::DateDiff(function) => Some(transform_function_from_parts(
            "DATE_DIFF",
            Vec::new(),
            vec![&function.this, &function.expression],
            scope,
            dialect,
        )),
        _ => None,
    }
}

fn transform_function_from_args(
    name: &str,
    args: &[Expression],
    scope: &Scope,
    dialect: DialectType,
) -> TransformFunctionFact {
    let literal_args = args
        .iter()
        .filter_map(|arg| literal_argument(arg, dialect))
        .collect();
    transform_function_from_parts(name, literal_args, args.iter().collect(), scope, dialect)
}

fn transform_function_from_parts(
    name: &str,
    literal_args: Vec<String>,
    args: Vec<&Expression>,
    scope: &Scope,
    _dialect: DialectType,
) -> TransformFunctionFact {
    let column_args = dedupe_column_refs(
        args.into_iter()
            .flat_map(|arg| fallback_column_references(arg, scope))
            .collect(),
    );

    TransformFunctionFact {
        name: name.to_string(),
        literal_args,
        column_args,
    }
}

fn literal_argument(expression: &Expression, dialect: DialectType) -> Option<String> {
    match expression {
        Expression::Literal(literal) => Some(literal.value_str().to_string()),
        Expression::Boolean(boolean) => Some(boolean.value.to_string()),
        Expression::Null(_) => Some("NULL".to_string()),
        Expression::Identifier(identifier) => Some(identifier.name.clone()),
        Expression::Var(var) => Some(var.this.clone()),
        Expression::DataType(data_type) => render_data_type(data_type, dialect),
        _ => None,
    }
}

fn datetime_field_name(field: &crate::expressions::DateTimeField) -> String {
    match field {
        crate::expressions::DateTimeField::Year => "year".to_string(),
        crate::expressions::DateTimeField::Month => "month".to_string(),
        crate::expressions::DateTimeField::Day => "day".to_string(),
        crate::expressions::DateTimeField::Hour => "hour".to_string(),
        crate::expressions::DateTimeField::Minute => "minute".to_string(),
        crate::expressions::DateTimeField::Second => "second".to_string(),
        crate::expressions::DateTimeField::Millisecond => "millisecond".to_string(),
        crate::expressions::DateTimeField::Microsecond => "microsecond".to_string(),
        crate::expressions::DateTimeField::DayOfWeek => "day_of_week".to_string(),
        crate::expressions::DateTimeField::DayOfYear => "day_of_year".to_string(),
        crate::expressions::DateTimeField::Week => "week".to_string(),
        crate::expressions::DateTimeField::WeekWithModifier(modifier) => {
            format!("week({modifier})")
        }
        crate::expressions::DateTimeField::Quarter => "quarter".to_string(),
        crate::expressions::DateTimeField::Epoch => "epoch".to_string(),
        crate::expressions::DateTimeField::Timezone => "timezone".to_string(),
        crate::expressions::DateTimeField::TimezoneHour => "timezone_hour".to_string(),
        crate::expressions::DateTimeField::TimezoneMinute => "timezone_minute".to_string(),
        crate::expressions::DateTimeField::Date => "date".to_string(),
        crate::expressions::DateTimeField::Time => "time".to_string(),
        crate::expressions::DateTimeField::Custom(name) => name.clone(),
    }
}

fn unwrap_projection_alias(expression: &Expression) -> &Expression {
    match expression {
        Expression::Alias(alias) => unwrap_projection_alias(&alias.this),
        Expression::Annotated(annotated) => unwrap_projection_alias(&annotated.this),
        Expression::Paren(paren) => unwrap_projection_alias(&paren.this),
        _ => expression,
    }
}

fn projection_name(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Alias(alias) => Some(alias.alias.name.clone()),
        Expression::Column(column) => Some(column.name.name.clone()),
        Expression::Identifier(identifier) => Some(identifier.name.clone()),
        Expression::Star(_) => Some("*".to_string()),
        Expression::Annotated(annotated) => projection_name(&annotated.this),
        _ => None,
    }
}

fn projection_is_star(expression: &Expression) -> bool {
    matches!(expression, Expression::Star(_))
        || matches!(expression, Expression::Column(column) if column.name.name == "*")
}

fn projection_star_table(expression: &Expression) -> Option<String> {
    match expression {
        Expression::Star(star) => star
            .table
            .as_ref()
            .map(|identifier| identifier.name.clone()),
        Expression::Column(column) if column.name.name == "*" => column
            .table
            .as_ref()
            .map(|identifier| identifier.name.clone()),
        _ => None,
    }
}

fn transform_kind(expression: &Expression) -> TransformKind {
    if projection_is_star(expression) {
        TransformKind::Star
    } else if is_cast_expression(expression) {
        TransformKind::Cast
    } else if contains_aggregate(expression) {
        TransformKind::Aggregation
    } else if matches!(
        expression,
        Expression::Column(_) | Expression::Identifier(_)
    ) {
        TransformKind::Direct
    } else if is_simple_constant(expression) {
        TransformKind::Constant
    } else {
        TransformKind::Expression
    }
}

fn is_cast_expression(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Cast(_) | Expression::TryCast(_) | Expression::SafeCast(_)
    )
}

fn cast_type(expression: &Expression, dialect: DialectType) -> Option<String> {
    match expression {
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            render_data_type(&cast.to, dialect)
        }
        _ => None,
    }
}

fn render_data_type(data_type: &DataType, dialect: DialectType) -> Option<String> {
    if dialect == DialectType::ClickHouse {
        return crate::generator::Generator::with_config(
            Dialect::get(dialect).generator_config().clone(),
        )
        .generate_type_hint(data_type)
        .ok();
    }
    Dialect::get(dialect)
        .generate(&Expression::DataType(data_type.clone()))
        .ok()
}

fn is_simple_constant(expression: &Expression) -> bool {
    match expression {
        Expression::Literal(_) | Expression::Boolean(_) | Expression::Null(_) => true,
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            is_simple_constant(&cast.this)
        }
        Expression::Neg(unary) | Expression::BitwiseNot(unary) => is_simple_constant(&unary.this),
        _ => false,
    }
}

impl NullabilityContext<'_> {
    // Inference is optional metadata: cycles and excessive dependency depth
    // must yield Unknown, not recurse indefinitely or claim non-nullability.
    const MAX_DEPTH: usize = 128;

    fn output(&self, scope_id: usize, ordinal: usize, depth: usize) -> ProjectionNullability {
        let key = (scope_id, ordinal);
        if let Some(value) = self.outputs.borrow().get(&key) {
            return *value;
        }
        if depth >= Self::MAX_DEPTH || !self.resolving.borrow_mut().insert(key) {
            return ProjectionNullability::Unknown;
        }
        let value = self.output_inner(scope_id, ordinal, depth + 1);
        self.resolving.borrow_mut().remove(&key);
        self.outputs.borrow_mut().insert(key, value);
        value
    }

    fn output_inner(&self, scope_id: usize, ordinal: usize, depth: usize) -> ProjectionNullability {
        use ProjectionNullability::*;
        let frame = &self.scopes[scope_id];
        let query = crate::scope::scope_query(&frame.scope.expression);
        if let Expression::Select(select) = query {
            return select
                .expressions
                .get(ordinal)
                .map(|expression| self.expression(scope_id, expression, depth))
                .unwrap_or(Unknown);
        }
        if !is_set_operation(query) || frame.branches.len() != 2 {
            return Unknown;
        }
        let (left, right) =
            match crate::set_operation::set_operation_layout(query, Some(self.dialect)) {
                Ok(Some(layout)) => match layout.outputs.get(ordinal) {
                    Some(output) => (output.left_ordinal, output.right_ordinal),
                    None => return Unknown,
                },
                Ok(None) => (Some(ordinal), Some(ordinal)),
                Err(_) => return Unknown,
            };
        let branch = |id, ordinal: Option<usize>| {
            ordinal
                .map(|ordinal| self.output(id, ordinal, depth))
                .unwrap_or(Nullable)
        };
        let left = branch(frame.branches[0], left);
        if matches!(query, Expression::Except(_)) {
            return left;
        }
        let right = branch(frame.branches[1], right);
        if matches!(query, Expression::Intersect(_)) {
            return match (left, right) {
                (NonNull, _) | (_, NonNull) => NonNull,
                (Nullable, Nullable) => Nullable,
                _ => Unknown,
            };
        }
        match (left, right) {
            (NonNull, NonNull) => NonNull,
            (Nullable, _) | (_, Nullable) => Nullable,
            _ => Unknown,
        }
    }

    fn expression(
        &self,
        scope_id: usize,
        expression: &Expression,
        depth: usize,
    ) -> ProjectionNullability {
        use ProjectionNullability::*;
        if depth >= Self::MAX_DEPTH {
            return Unknown;
        }
        let depth = depth + 1;
        match expression {
            Expression::Alias(alias) => self.expression(scope_id, &alias.this, depth),
            Expression::Annotated(annotated) => self.expression(scope_id, &annotated.this, depth),
            Expression::Paren(paren) => self.expression(scope_id, &paren.this, depth),
            Expression::Literal(_)
            | Expression::Boolean(_)
            | Expression::Count(_)
            | Expression::CountIf(_) => NonNull,
            Expression::Null(_) => Nullable,
            Expression::Cast(cast) => self.expression(scope_id, &cast.this, depth),
            Expression::Column(column) => {
                if column.span.or(column.name.span).is_some_and(|span| {
                    self.uncertain_columns.contains_key(&(span.start, span.end))
                }) {
                    Unknown
                } else {
                    self.column(scope_id, &column.name, column.table.as_ref(), depth)
                }
            }
            Expression::Identifier(identifier) => self.column(scope_id, identifier, None, depth),
            Expression::Coalesce(func) => {
                let mut all_nullable = !func.expressions.is_empty();
                for expression in &func.expressions {
                    match self.expression(scope_id, expression, depth) {
                        NonNull => return NonNull,
                        Nullable => {}
                        Unknown => all_nullable = false,
                    }
                }
                if all_nullable {
                    Nullable
                } else {
                    Unknown
                }
            }
            _ => Unknown,
        }
    }

    fn source_scope(
        &self,
        frame: &NullabilityScope<'_>,
        name: &str,
        source: &SourceInfo,
    ) -> Option<usize> {
        if let Some(Expression::Table(table)) = frame.bindings.get(name).copied() {
            if table.schema.is_none() && table.catalog.is_none() {
                return frame
                    .ctes
                    .get(&crate::set_operation::identifier_key(
                        &table.name,
                        Some(self.dialect),
                    ))
                    .copied();
            }
        }
        if source.kind == SourceKind::DerivedTable && source.is_scope {
            return frame.derived.iter().copied().find(|id| {
                crate::scope::scope_query(&self.scopes[*id].scope.expression)
                    == crate::scope::scope_query(&source.expression)
            });
        }
        None
    }

    fn source_columns(
        &self,
        frame: &NullabilityScope<'_>,
        name: &str,
        source: &SourceInfo,
    ) -> Option<Vec<Identifier>> {
        if let Some(id) = self.source_scope(frame, name, source) {
            let expression = if matches!(
                frame.bindings.get(name).copied(),
                Some(Expression::Table(_))
            ) {
                &self.scopes[id].scope.expression
            } else {
                &source.expression
            };
            return crate::set_operation::query_output_identifiers(expression, Some(self.dialect))
                .ok();
        }
        if source.kind == SourceKind::Table {
            let mut resolver = crate::resolver::Resolver::new(
                &frame.selected,
                self.mapping_schema.unwrap_or(&self.empty_schema),
                false,
            );
            let columns = resolver.get_source_columns(name).ok()?;
            if columns.is_empty() || columns.iter().any(|name| name == "*") {
                return None;
            }
            Some(columns.into_iter().map(Identifier::new).collect())
        } else {
            crate::set_operation::query_output_identifiers(&source.expression, Some(self.dialect))
                .ok()
        }
    }

    fn column(
        &self,
        scope_id: usize,
        column: &Identifier,
        qualifier: Option<&Identifier>,
        depth: usize,
    ) -> ProjectionNullability {
        use ProjectionNullability::*;
        let frame = &self.scopes[scope_id];
        let column_key = crate::set_operation::identifier_key(column, Some(self.dialect));
        let same_column = |name: &Identifier| {
            crate::set_operation::identifier_key(name, Some(self.dialect)) == column_key
        };
        let binding = if let Some(qualifier) = qualifier {
            let key = crate::set_operation::identifier_key(qualifier, Some(self.dialect));
            let mut matches = frame
                .bindings
                .iter()
                .filter(|(_, expression)| {
                    expression_source_identifier(expression).is_some_and(|identifier| {
                        crate::set_operation::identifier_key(identifier, Some(self.dialect)) == key
                    })
                })
                .filter_map(|(name, _)| frame.selected.sources.get_key_value(name));
            let binding = matches.next();
            if matches.next().is_some() {
                return Unknown;
            }
            binding
        } else {
            let mut binding = None;
            for (name, source) in &frame.selected.sources {
                // An open source can also provide this column. Do not pick a
                // known source just because the other source lacks metadata.
                let Some(columns) = self.source_columns(frame, name, source) else {
                    return Unknown;
                };
                if columns.iter().any(&same_column) {
                    if binding.is_some() {
                        return Unknown;
                    }
                    binding = Some((name, source));
                }
            }
            binding
        };
        let Some((name, source)) = binding else {
            return Unknown;
        };
        if frame
            .bindings
            .get(name)
            .and_then(|expression| expression_source_identifier(expression))
            .is_some_and(|identifier| {
                frame
                    .nullable_sources
                    .contains(&crate::set_operation::identifier_key(
                        identifier,
                        Some(self.dialect),
                    ))
            })
        {
            return Nullable;
        }
        let target = self.source_scope(frame, name, source);
        if target.is_none() && source.kind == SourceKind::Table {
            let info = self.schema.and_then(|schema| {
                source_table_name(source).and_then(|table| schema.column(&table, &column.name))
            });
            return match info {
                Some(info) if info.primary_key || info.nullable == Some(false) => NonNull,
                Some(info) if info.nullable == Some(true) => Nullable,
                _ => Unknown,
            };
        }
        let Some(columns) = self.source_columns(frame, name, source) else {
            return Unknown;
        };
        let mut ordinals = columns
            .iter()
            .enumerate()
            .filter(|(_, name)| same_column(name))
            .map(|(ordinal, _)| ordinal);
        let Some(ordinal) = ordinals.next() else {
            return Unknown;
        };
        if ordinals.next().is_some() {
            return Unknown;
        }
        target
            .map(|id| self.output(id, ordinal, depth))
            .unwrap_or(Unknown)
    }
}

/// Pass a path to report the transforms of each reference on its lineage path.
fn terminal_references_from_lineage(
    node: &LineageNode,
    path: Option<&PathTransform>,
) -> Vec<ColumnReferenceFact> {
    let mut refs = Vec::new();
    collect_terminal_references(node, path, &mut refs);
    dedupe_column_refs(refs)
}

fn collect_terminal_references(
    node: &LineageNode,
    path: Option<&PathTransform>,
    refs: &mut Vec<ColumnReferenceFact>,
) {
    // A set-operation node only routes to its branches; each branch is a step.
    let stepped;
    let path = match path {
        Some(path) if !is_set_operation(&node.expression) => {
            stepped = path.step(unwrap_projection_alias(&node.expression));
            Some(&stepped)
        }
        path => path,
    };
    if node.downstream.is_empty() {
        if let Some(mut reference) = column_reference_from_lineage_node(node) {
            if let Some(path) = path {
                path.apply(&mut reference);
            }
            refs.push(reference);
        }
        return;
    }

    for child in &node.downstream {
        collect_terminal_references(child, path, refs);
    }
}

/// The transforms on one lineage path, from the projection output to a source column.
#[derive(Clone)]
struct PathTransform {
    dialect: DialectType,
    kind: TransformKind,
    functions: Vec<String>,
    window: bool,
}

impl PathTransform {
    fn new(dialect: DialectType) -> Self {
        Self {
            dialect,
            kind: TransformKind::Direct,
            functions: Vec::new(),
            window: false,
        }
    }

    /// Add the projection expression of one scope on the path.
    fn step(&self, expression: &Expression) -> Self {
        let mut next = self.clone();
        // Nested queries are separate lineage steps. Their filters and unused
        // outputs must not contribute transforms to this projection.
        for node in crate::scope::walk_in_scope(expression, false) {
            next.kind = combine_transform_kinds(next.kind, path_node_kind(node));
            if matches!(node, Expression::WindowFunction(_)) {
                next.window = true;
            } else if let Some(name) = function_name(node, self.dialect) {
                if !next.functions.contains(&name) {
                    next.functions.push(name);
                }
            }
        }
        next
    }

    fn apply(&self, reference: &mut ColumnReferenceFact) {
        reference.path_transform_kind = Some(self.kind);
        reference.path_functions = self.functions.clone();
        reference.path_window = self.window;
    }
}

/// Unlike the legacy outer-expression kind, path metadata includes transforms
/// inside casts and other wrappers, without crossing query scope boundaries.
fn path_node_kind(node: &Expression) -> TransformKind {
    if is_aggregate(node) {
        return TransformKind::Aggregation;
    }
    match node {
        Expression::Column(_)
        | Expression::Identifier(_)
        | Expression::Alias(_)
        | Expression::Paren(_)
        | Expression::Annotated(_)
        | Expression::Subquery(_)
        | Expression::Select(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_)
        | Expression::Cte(_) => TransformKind::Direct,
        Expression::Star(_) => TransformKind::Star,
        Expression::Cast(_) | Expression::TryCast(_) | Expression::SafeCast(_) => {
            TransformKind::Cast
        }
        Expression::Literal(_)
        | Expression::Boolean(_)
        | Expression::Null(_)
        | Expression::DataType(_) => TransformKind::Constant,
        _ => TransformKind::Expression,
    }
}

/// The name of a function call node, as the dialect writes it.
fn function_name(node: &Expression, dialect: DialectType) -> Option<String> {
    match node {
        Expression::Function(function) => {
            if !function.qualified_name.is_empty() {
                return function
                    .qualified_name
                    .iter()
                    .map(|identifier| {
                        crate::generate(&Expression::Identifier(identifier.clone()), dialect).ok()
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(|parts| parts.join("."));
            }
            if function.quoted {
                return crate::generate(
                    &Expression::Identifier(Identifier::quoted(&function.name)),
                    dialect,
                )
                .ok();
            }
            if function.name.contains('.') {
                return Some(function.name.clone());
            }
            use crate::generator::NormalizeFunctions;
            Some(
                match Dialect::get(dialect).generator_config().normalize_functions {
                    NormalizeFunctions::Upper => function.name.to_uppercase(),
                    NormalizeFunctions::Lower => function.name.to_lowercase(),
                    NormalizeFunctions::None => function.name.clone(),
                },
            )
        }
        Expression::AggregateFunction(function) => Some(function.name.to_uppercase()),
        Expression::Column(_)
        | Expression::Identifier(_)
        | Expression::Literal(_)
        | Expression::Alias(_)
        | Expression::Paren(_)
        | Expression::Star(_)
        | Expression::Cast(_)
        | Expression::TryCast(_)
        | Expression::SafeCast(_)
        | Expression::Subquery(_)
        | Expression::Exists(_)
        | Expression::Select(_)
        | Expression::Case(_) => None,
        node if is_arithmetic(node) || is_comparison(node) || is_logical(node) => None,
        // A typed node (`Sum`, `Coalesce`) has no name field; its SQL starts with it.
        _ => {
            let sql = crate::generate(node, dialect).ok()?;
            let (name, _) = sql.split_once('(')?;
            let is_name =
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            is_name.then(|| name.to_uppercase())
        }
    }
}

fn combine_transform_kinds(left: TransformKind, right: TransformKind) -> TransformKind {
    let rank = |kind| match kind {
        TransformKind::Constant => 0,
        TransformKind::Direct => 1,
        TransformKind::Star => 2,
        TransformKind::Cast => 3,
        TransformKind::Expression => 4,
        TransformKind::Aggregation => 5,
    };
    if rank(right) > rank(left) {
        right
    } else {
        left
    }
}

fn column_reference_from_lineage_node(node: &LineageNode) -> Option<ColumnReferenceFact> {
    match &node.expression {
        Expression::Column(column) => {
            let source_name = non_empty_string(node.source_name.clone());
            let table =
                lineage_node_table(node).or_else(|| column.table.as_ref().map(|t| t.name.clone()));
            let confidence = if node.source_kind == SourceKind::Unknown && source_name.is_none() {
                ReferenceConfidence::Unknown
            } else {
                ReferenceConfidence::Resolved
            };
            Some(ColumnReferenceFact {
                source_name,
                source_alias: node.source_alias.clone(),
                source_kind: node.source_kind,
                table,
                column: column.name.name.clone(),
                unqualified: column.table.is_none(),
                confidence,
                path_transform_kind: None,
                path_functions: Vec::new(),
                path_window: false,
            })
        }
        Expression::Star(_) => Some(ColumnReferenceFact {
            source_name: non_empty_string(node.source_name.clone()),
            source_alias: node.source_alias.clone(),
            source_kind: node.source_kind,
            table: lineage_node_table(node),
            column: "*".to_string(),
            unqualified: true,
            confidence: if node.source_kind == SourceKind::Unknown {
                ReferenceConfidence::Unknown
            } else {
                ReferenceConfidence::Resolved
            },
            path_transform_kind: None,
            path_functions: Vec::new(),
            path_window: false,
        }),
        _ => None,
    }
}

fn lineage_node_table(node: &LineageNode) -> Option<String> {
    match &node.source {
        Expression::Table(table) => Some(table_name(table)),
        _ => None,
    }
}

fn fallback_column_references(expression: &Expression, scope: &Scope) -> Vec<ColumnReferenceFact> {
    let mut refs = Vec::new();
    let source_count = scope.sources.len();
    let single_source = if source_count == 1 {
        scope.sources.iter().next()
    } else {
        None
    };

    for column_expr in expression.find_all(|candidate| matches!(candidate, Expression::Column(_))) {
        if let Expression::Column(column) = column_expr {
            if column.name.name == "*" {
                continue;
            }
            let source = column
                .table
                .as_ref()
                .and_then(|table| scope.sources.get(&table.name));
            let (source_name, source_alias, source_kind, table, confidence) =
                if let Some(table_identifier) = &column.table {
                    if let Some(source) = source {
                        (
                            Some(table_identifier.name.clone()),
                            source.alias.clone(),
                            source.kind,
                            source_table_name(source)
                                .or_else(|| Some(table_identifier.name.clone())),
                            ReferenceConfidence::Resolved,
                        )
                    } else {
                        (
                            Some(table_identifier.name.clone()),
                            None,
                            SourceKind::Unknown,
                            Some(table_identifier.name.clone()),
                            ReferenceConfidence::Unknown,
                        )
                    }
                } else if let Some((name, source)) = single_source {
                    (
                        Some(name.clone()),
                        source.alias.clone(),
                        source.kind,
                        source_table_name(source).or_else(|| Some(name.clone())),
                        ReferenceConfidence::Resolved,
                    )
                } else if source_count > 1 {
                    (
                        None,
                        None,
                        SourceKind::Unknown,
                        None,
                        ReferenceConfidence::Ambiguous,
                    )
                } else {
                    (
                        None,
                        None,
                        SourceKind::Unknown,
                        None,
                        ReferenceConfidence::Unknown,
                    )
                };

            refs.push(ColumnReferenceFact {
                source_name,
                source_alias,
                source_kind,
                table,
                column: column.name.name.clone(),
                unqualified: column.table.is_none(),
                confidence,
                path_transform_kind: None,
                path_functions: Vec::new(),
                path_window: false,
            });
        }
    }

    dedupe_column_refs(refs)
}

fn dedupe_column_refs(refs: Vec<ColumnReferenceFact>) -> Vec<ColumnReferenceFact> {
    let mut seen = HashMap::new();
    let mut deduped = Vec::new();

    for reference in refs {
        let key = (
            reference.source_name.clone(),
            reference.source_alias.clone(),
            reference.table.clone(),
            reference.column.clone(),
            format!("{:?}", reference.source_kind),
            reference.unqualified,
            format!("{:?}", reference.confidence),
        );
        if let Some(&index) = seen.get(&key) {
            // Paths that reach the same column keep the strongest kind and all functions.
            let existing: &mut ColumnReferenceFact = &mut deduped[index];
            if let (Some(left), Some(right)) =
                (existing.path_transform_kind, reference.path_transform_kind)
            {
                existing.path_transform_kind = Some(combine_transform_kinds(left, right));
            }
            for function in reference.path_functions {
                if !existing.path_functions.contains(&function) {
                    existing.path_functions.push(function);
                }
            }
            existing.path_window |= reference.path_window;
        } else {
            seen.insert(key, deduped.len());
            deduped.push(reference);
        }
    }

    deduped
}

fn relation_facts(
    scope: &Scope,
    mapping_schema: Option<&crate::schema::MappingSchema>,
    dialect: DialectType,
) -> Vec<RelationFact> {
    let mut relations = Vec::new();
    let mut seen = HashSet::new();
    collect_relation_facts(scope, mapping_schema, dialect, &mut seen, &mut relations);

    relations.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.alias.cmp(&right.alias))
    });
    relations
}

fn collect_relation_facts(
    scope: &Scope,
    mapping_schema: Option<&crate::schema::MappingSchema>,
    dialect: DialectType,
    seen: &mut HashSet<String>,
    relations: &mut Vec<RelationFact>,
) {
    for relation in scope.sources.iter().map(|(source_name, source)| {
        let identity = source_table_identity(source);
        RelationFact {
            name: source
                .lineage_name
                .clone()
                .or_else(|| identity.as_ref().map(|identity| identity.name.clone()))
                .unwrap_or_else(|| source_name.clone()),
            alias: source.alias.clone().or_else(|| source_alias(source)),
            kind: source.kind,
            columns: source_columns(source, mapping_schema, dialect),
            catalog: identity
                .as_ref()
                .and_then(|identity| identity.catalog.clone()),
            schema: identity
                .as_ref()
                .and_then(|identity| identity.schema.clone()),
            table: identity
                .as_ref()
                .and_then(|identity| identity.table.clone()),
        }
    }) {
        let key = format!("{:?}|{}|{:?}", relation.kind, relation.name, relation.alias);
        if seen.insert(key) {
            relations.push(relation);
        }
    }

    for branch_scope in &scope.union_scopes {
        collect_relation_facts(branch_scope, mapping_schema, dialect, seen, relations);
    }
}

fn base_table_facts(
    scope: &Scope,
    mapping_schema: Option<&crate::schema::MappingSchema>,
    dialect: DialectType,
) -> Vec<RelationFact> {
    let mut relations = Vec::new();
    let mut seen = HashSet::new();

    collect_base_table_facts(scope, mapping_schema, dialect, &mut seen, &mut relations);

    relations.sort_by(|left, right| left.name.cmp(&right.name));
    relations
}

fn collect_base_table_facts(
    scope: &Scope,
    mapping_schema: Option<&crate::schema::MappingSchema>,
    dialect: DialectType,
    seen: &mut HashSet<String>,
    relations: &mut Vec<RelationFact>,
) {
    for source in scope.sources.values() {
        if source.kind != SourceKind::Table {
            continue;
        }

        let Some(identity) = source_table_identity(source) else {
            continue;
        };

        if seen.insert(identity.name.clone()) {
            relations.push(RelationFact {
                name: identity.name,
                alias: source.alias.clone().or_else(|| source_alias(source)),
                kind: SourceKind::Table,
                columns: source_columns(source, mapping_schema, dialect),
                catalog: identity.catalog,
                schema: identity.schema,
                table: identity.table,
            });
        }
    }

    for child_scope in scope
        .cte_scopes
        .iter()
        .chain(scope.union_scopes.iter())
        .chain(scope.table_scopes.iter())
        .chain(scope.derived_table_scopes.iter())
        .chain(scope.subquery_scopes.iter())
    {
        collect_base_table_facts(child_scope, mapping_schema, dialect, seen, relations);
    }
}

fn source_columns(
    source: &SourceInfo,
    mapping_schema: Option<&crate::schema::MappingSchema>,
    dialect: DialectType,
) -> Vec<String> {
    match &source.expression {
        Expression::Table(table) => mapping_schema
            .and_then(|schema| schema.column_names(&table_name(table)).ok())
            .unwrap_or_default(),
        Expression::Select(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_) => {
            get_output_column_names_for_dialect(&source.expression, Some(dialect))
        }
        Expression::Subquery(subquery) => {
            get_output_column_names_for_dialect(&subquery.this, Some(dialect))
        }
        Expression::Cte(cte) if !cte.columns.is_empty() => cte
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        Expression::Cte(cte) => get_output_column_names_for_dialect(&cte.this, Some(dialect)),
        _ => Vec::new(),
    }
}

fn source_table_name(source: &SourceInfo) -> Option<String> {
    source_table_identity(source).map(|identity| identity.name)
}

fn source_alias(source: &SourceInfo) -> Option<String> {
    match &source.expression {
        Expression::Table(table) => table.alias.as_ref().map(|alias| alias.name.clone()),
        Expression::Subquery(subquery) => subquery.alias.as_ref().map(|alias| alias.name.clone()),
        _ => None,
    }
}

fn table_name(table: &TableRef) -> String {
    let mut parts = Vec::new();
    if let Some(catalog) = &table.catalog {
        parts.push(catalog.name.clone());
    }
    if let Some(schema) = &table.schema {
        parts.push(schema.name.clone());
    }
    parts.push(table.name.name.clone());
    parts.join(".")
}

#[derive(Debug, Clone)]
struct RelationIdentity {
    name: String,
    catalog: Option<String>,
    schema: Option<String>,
    table: Option<String>,
}

fn source_table_identity(source: &SourceInfo) -> Option<RelationIdentity> {
    match &source.expression {
        Expression::Table(table) => Some(table_identity(table)),
        _ => None,
    }
}

fn table_identity(table: &TableRef) -> RelationIdentity {
    RelationIdentity {
        name: table_name(table),
        catalog: table.catalog.as_ref().map(|catalog| catalog.name.clone()),
        schema: table.schema.as_ref().map(|schema| schema.name.clone()),
        table: Some(table.name.name.clone()),
    }
}

fn set_operation_facts(
    expression: &Expression,
    scope: &Scope,
    dialect: DialectType,
    nullability: &NullabilityContext<'_>,
) -> Vec<SetOperationFact> {
    let mut facts = Vec::new();
    collect_set_operation_facts(expression, scope, dialect, nullability, &mut facts);
    facts
}

fn collect_set_operation_facts(
    expression: &Expression,
    scope: &Scope,
    dialect: DialectType,
    nullability: &NullabilityContext<'_>,
    facts: &mut Vec<SetOperationFact>,
) {
    match expression {
        Expression::Union(union) => {
            facts.push(SetOperationFact {
                kind: "union".to_string(),
                all: union.all,
                distinct: union.distinct,
                output_columns: get_output_column_names_for_dialect(expression, Some(dialect)),
                branches: set_operation_branches(
                    &union.left,
                    &union.right,
                    scope,
                    dialect,
                    SetOperationBranchRole::Value,
                    nullability,
                ),
            });
            collect_set_operation_facts(
                &union.left,
                scope.union_scopes.first().unwrap_or(scope),
                dialect,
                nullability,
                facts,
            );
            collect_set_operation_facts(
                &union.right,
                scope.union_scopes.get(1).unwrap_or(scope),
                dialect,
                nullability,
                facts,
            );
        }
        Expression::Intersect(intersect) => {
            facts.push(SetOperationFact {
                kind: "intersect".to_string(),
                all: intersect.all,
                distinct: intersect.distinct,
                output_columns: get_output_column_names_for_dialect(expression, Some(dialect)),
                branches: set_operation_branches(
                    &intersect.left,
                    &intersect.right,
                    scope,
                    dialect,
                    SetOperationBranchRole::Filter,
                    nullability,
                ),
            });
            collect_set_operation_facts(
                &intersect.left,
                scope.union_scopes.first().unwrap_or(scope),
                dialect,
                nullability,
                facts,
            );
            collect_set_operation_facts(
                &intersect.right,
                scope.union_scopes.get(1).unwrap_or(scope),
                dialect,
                nullability,
                facts,
            );
        }
        Expression::Except(except) => {
            facts.push(SetOperationFact {
                kind: "except".to_string(),
                all: except.all,
                distinct: except.distinct,
                output_columns: get_output_column_names_for_dialect(expression, Some(dialect)),
                branches: set_operation_branches(
                    &except.left,
                    &except.right,
                    scope,
                    dialect,
                    SetOperationBranchRole::Filter,
                    nullability,
                ),
            });
            collect_set_operation_facts(
                &except.left,
                scope.union_scopes.first().unwrap_or(scope),
                dialect,
                nullability,
                facts,
            );
            collect_set_operation_facts(
                &except.right,
                scope.union_scopes.get(1).unwrap_or(scope),
                dialect,
                nullability,
                facts,
            );
        }
        Expression::Subquery(subquery) => {
            collect_set_operation_facts(&subquery.this, scope, dialect, nullability, facts);
        }
        _ => {}
    }
}

fn set_operation_branches(
    left: &Expression,
    right: &Expression,
    scope: &Scope,
    dialect: DialectType,
    right_role: SetOperationBranchRole,
    nullability: &NullabilityContext<'_>,
) -> Vec<SetOperationBranchFact> {
    vec![
        SetOperationBranchFact {
            index: 0,
            role: SetOperationBranchRole::Value,
            projections: projection_facts_for_query(
                left,
                scope.union_scopes.first().unwrap_or(scope),
                dialect,
                nullability,
            ),
        },
        SetOperationBranchFact {
            index: 1,
            role: right_role,
            projections: projection_facts_for_query(
                right,
                scope.union_scopes.get(1).unwrap_or(scope),
                dialect,
                nullability,
            ),
        },
    ]
}

fn non_empty_string(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}
