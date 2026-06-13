//! Compact query analysis facts.
//!
//! This module intentionally builds on the existing parser, scope builder, type
//! annotator, and lineage implementation. It is a convenience API: callers that
//! need the full AST or full lineage graph should continue using those lower
//! level APIs directly.

use crate::ast_transforms::get_output_column_names;
use crate::dialects::{Dialect, DialectType};
use crate::expressions::{DataType, Expression, JoinKind, TableRef, With};
use crate::lineage::{lineage_by_index_from_expression, LineageNode};
use crate::optimizer::annotate_types::annotate_types;
use crate::optimizer::qualify_columns::{qualify_columns, QualifyColumnsOptions};
use crate::schema::{MappingSchema, Schema};
use crate::scope::{build_scope, Scope, SourceInfo, SourceKind};
use crate::traversal::{contains_aggregate, ExpressionWalk};
use crate::validation::{mapping_schema_from_validation_schema, ValidationSchema};
use crate::{parse_data_type, parse_one, Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Options for [`analyze_query`].
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct AnalyzeQueryOptions {
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
    pub projections: Vec<ProjectionFact>,
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
    let mut expression = parse_one(sql, options.dialect)?;
    expression = effective_query(expression);
    ensure_query(&expression)?;
    let original_expression = expression.clone();

    let mapping_schema = options
        .schema
        .as_ref()
        .map(|schema| analysis_mapping_schema(schema, options.dialect));
    let schema_info = options.schema.as_ref().map(AnalysisSchemaInfo::from_schema);
    let cte_facts = top_level_cte_facts(&original_expression, options.dialect)?;
    let star_projections = star_projection_facts(&original_expression, mapping_schema.as_ref());

    if let Some(schema) = mapping_schema.as_ref() {
        let qualify_options = QualifyColumnsOptions::new().with_dialect(options.dialect);
        expression = qualify_columns(expression, schema, &qualify_options)
            .map_err(|e| Error::internal(format!("query analysis qualification failed: {e}")))?;
    }

    let annotation_schema = mapping_schema.as_ref().map(|schema| {
        let mut alias_schema = schema.clone();
        add_scope_aliases_to_schema(
            &build_scope(&expression),
            schema,
            &mut alias_schema,
            options.dialect,
        );
        alias_schema
    });

    annotate_types(
        &mut expression,
        annotation_schema
            .as_ref()
            .map(|schema| schema as &dyn Schema),
        Some(options.dialect),
    );
    crate::lineage::expand_cte_stars(
        &mut expression,
        annotation_schema
            .as_ref()
            .or(mapping_schema.as_ref())
            .map(|schema| schema as &dyn Schema),
    );

    let scope = build_scope(&expression);
    let nullability_context = NullabilityContext {
        schema: schema_info.as_ref(),
        nullable_sources: nullable_source_names(&expression),
    };
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
        relations: relation_facts(&scope, mapping_schema.as_ref()),
        base_tables: base_table_facts(&scope, mapping_schema.as_ref()),
        star_projections,
        set_operations: set_operation_facts(&expression, &scope, options.dialect),
    })
}

fn analysis_mapping_schema(schema: &ValidationSchema, dialect: DialectType) -> MappingSchema {
    let broad_schema = mapping_schema_from_validation_schema(schema);
    let mut mapping_schema = MappingSchema::with_dialect(dialect);

    for table in &schema.tables {
        let table_names = validation_table_names(table);
        if table_names.is_empty() {
            continue;
        }

        let fallback_table = table_names[0].as_str();
        let columns: Vec<(String, DataType)> = table
            .columns
            .iter()
            .map(|column| {
                let data_type = parse_analysis_data_type(&column.data_type, dialect)
                    .unwrap_or_else(|| {
                        broad_schema
                            .get_column_type(fallback_table, &column.name)
                            .unwrap_or(DataType::Unknown)
                    });
                (column.name.to_ascii_lowercase(), data_type)
            })
            .collect();

        for table_name in table_names {
            let _ = mapping_schema.add_table(&table_name, &columns, Some(dialect));
        }
    }

    mapping_schema
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

fn parse_analysis_data_type(data_type: &str, dialect: DialectType) -> Option<DataType> {
    let trimmed = data_type.trim();
    if trimmed.is_empty() {
        return None;
    }
    parse_data_type(trimmed, dialect).ok()
}

fn add_scope_aliases_to_schema(
    scope: &Scope,
    source_schema: &MappingSchema,
    target_schema: &mut MappingSchema,
    dialect: DialectType,
) {
    for child_scope in scope.traverse() {
        for (source_name, source) in &child_scope.sources {
            if source.kind != SourceKind::Table {
                continue;
            }
            if let Some(table_name) = source_table_name(source) {
                if source_name == &table_name {
                    continue;
                }
                if let Ok(column_names) = source_schema.column_names(&table_name) {
                    let columns: Vec<(String, DataType)> = column_names
                        .iter()
                        .map(|column| {
                            (
                                column.clone(),
                                source_schema
                                    .get_column_type(&table_name, column)
                                    .unwrap_or(DataType::Unknown),
                            )
                        })
                        .collect();
                    let _ = target_schema.add_table(source_name, &columns, Some(dialect));
                }
            }
        }
    }
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
    schema: Option<&'a AnalysisSchemaInfo>,
    nullable_sources: HashSet<String>,
}

fn top_level_cte_facts(expression: &Expression, dialect: DialectType) -> Result<Vec<CteFact>> {
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
                body_sql: Dialect::get(dialect).generate(&cte.this)?,
                output_columns: get_output_column_names(&cte.this),
            })
        })
        .collect()
}

fn star_projection_facts(
    expression: &Expression,
    mapping_schema: Option<&MappingSchema>,
) -> Vec<StarProjectionFact> {
    let scope = build_scope(expression);
    let ordered_sources = ordered_source_names_for_query(expression);

    select_expressions_for_query(expression)
        .iter()
        .enumerate()
        .filter_map(|(index, projection)| {
            let inner = unwrap_projection_alias(projection);
            if !projection_is_star(inner) {
                return None;
            }

            let table = projection_star_table(inner);
            let expanded_columns =
                expanded_star_columns(table.as_deref(), &scope, &ordered_sources, mapping_schema);

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

        columns.extend(source_columns(source, mapping_schema));
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

fn nullable_source_names(expression: &Expression) -> HashSet<String> {
    match expression {
        Expression::Select(select) => nullable_source_names_for_select(select),
        Expression::Union(union) => nullable_source_names(&union.left),
        Expression::Intersect(intersect) => nullable_source_names(&intersect.left),
        Expression::Except(except) => nullable_source_names(&except.left),
        Expression::Subquery(subquery) => nullable_source_names(&subquery.this),
        _ => HashSet::new(),
    }
}

fn nullable_source_names_for_select(select: &crate::expressions::Select) -> HashSet<String> {
    let mut nullable = HashSet::new();
    let mut left_sources = Vec::new();

    if let Some(from) = &select.from {
        for expression in &from.expressions {
            if let Some(source_name) = expression_source_name(expression) {
                left_sources.push(source_name);
            }
        }
    }

    for join in &select.joins {
        let right_source = expression_source_name(&join.this);

        if join_nullable_left(join.kind) {
            for source_name in &left_sources {
                nullable.insert(normalize_lookup_name(source_name));
            }
        }

        if join_nullable_right(join.kind) {
            if let Some(source_name) = &right_source {
                nullable.insert(normalize_lookup_name(source_name));
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
    match expression {
        Expression::Table(table) => table
            .alias
            .as_ref()
            .map(|alias| alias.name.clone())
            .or_else(|| Some(table.name.name.clone())),
        Expression::Subquery(subquery) => subquery.alias.as_ref().map(|alias| alias.name.clone()),
        Expression::Alias(alias) => Some(alias.alias.name.clone()),
        Expression::Cte(cte) => Some(cte.alias.name.clone()),
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
    let expressions = select_expressions_for_query(expression);
    let names = get_output_column_names(expression);

    expressions
        .iter()
        .enumerate()
        .map(|(index, projection)| {
            projection_fact(
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
            )
        })
        .collect()
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
    query: &Expression,
    scope: &Scope,
    dialect: DialectType,
    nullability_context: &NullabilityContext<'_>,
) -> ProjectionFact {
    let inner = unwrap_projection_alias(projection);
    let is_star = projection_is_star(inner);
    let upstream = lineage_by_index_from_expression(index, query, Some(dialect), false)
        .map(|node| terminal_references_from_lineage(&node))
        .ok()
        .filter(|refs| !refs.is_empty())
        .unwrap_or_else(|| fallback_column_references(inner, scope));

    ProjectionFact {
        index,
        name,
        is_star,
        star_table: projection_star_table(inner),
        transform_kind: transform_kind(inner),
        transform_function: transform_function_fact(inner, scope, dialect),
        cast_type: cast_type(inner, dialect),
        type_hint: projection
            .inferred_type()
            .or_else(|| inner.inferred_type())
            .and_then(|data_type| render_data_type(data_type, dialect)),
        nullability: projection_nullability(inner, scope, nullability_context),
        upstream,
    }
}

fn transform_function_fact(
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
        Expression::WindowFunction(function) => {
            transform_function_fact(&function.this, scope, dialect)
        }
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

fn projection_nullability(
    expression: &Expression,
    scope: &Scope,
    context: &NullabilityContext<'_>,
) -> ProjectionNullability {
    match expression {
        Expression::Alias(alias) => projection_nullability(&alias.this, scope, context),
        Expression::Annotated(annotated) => projection_nullability(&annotated.this, scope, context),
        Expression::Paren(paren) => projection_nullability(&paren.this, scope, context),
        Expression::Literal(_) | Expression::Boolean(_) => ProjectionNullability::NonNull,
        Expression::Null(_) => ProjectionNullability::Nullable,
        Expression::Count(_) | Expression::CountIf(_) => ProjectionNullability::NonNull,
        Expression::Cast(cast) => projection_nullability(&cast.this, scope, context),
        Expression::TryCast(_) | Expression::SafeCast(_) => ProjectionNullability::Unknown,
        Expression::Column(column) => column_nullability(
            &column.name.name,
            column.table.as_ref().map(|table| table.name.as_str()),
            scope,
            context,
        ),
        Expression::Identifier(identifier) => {
            column_nullability(&identifier.name, None, scope, context)
        }
        Expression::Coalesce(func) => coalesce_nullability(&func.expressions, scope, context),
        _ => ProjectionNullability::Unknown,
    }
}

fn column_nullability(
    column_name: &str,
    source_name: Option<&str>,
    scope: &Scope,
    context: &NullabilityContext<'_>,
) -> ProjectionNullability {
    let resolved_source_name = source_name
        .map(str::to_string)
        .or_else(|| single_scope_source_name(scope));

    if let Some(source_name) = &resolved_source_name {
        if context
            .nullable_sources
            .contains(&normalize_lookup_name(source_name))
        {
            return ProjectionNullability::Nullable;
        }
    }

    let Some(schema) = context.schema else {
        return ProjectionNullability::Unknown;
    };

    let table_name = resolved_source_name
        .as_ref()
        .and_then(|name| scope.sources.get(name).and_then(source_table_name))
        .or(resolved_source_name);

    let Some(table_name) = table_name else {
        return ProjectionNullability::Unknown;
    };

    match schema.column(&table_name, column_name) {
        Some(info) if info.primary_key || info.nullable == Some(false) => {
            ProjectionNullability::NonNull
        }
        Some(info) if info.nullable == Some(true) => ProjectionNullability::Nullable,
        Some(_) | None => ProjectionNullability::Unknown,
    }
}

fn single_scope_source_name(scope: &Scope) -> Option<String> {
    if scope.sources.len() == 1 {
        scope.sources.keys().next().cloned()
    } else {
        None
    }
}

fn coalesce_nullability(
    expressions: &[Expression],
    scope: &Scope,
    context: &NullabilityContext<'_>,
) -> ProjectionNullability {
    if expressions.is_empty() {
        return ProjectionNullability::Unknown;
    }

    let mut all_nullable = true;

    for expression in expressions {
        match projection_nullability(unwrap_projection_alias(expression), scope, context) {
            ProjectionNullability::NonNull => return ProjectionNullability::NonNull,
            ProjectionNullability::Nullable => {}
            ProjectionNullability::Unknown => all_nullable = false,
        }
    }

    if all_nullable {
        ProjectionNullability::Nullable
    } else {
        ProjectionNullability::Unknown
    }
}

fn terminal_references_from_lineage(node: &LineageNode) -> Vec<ColumnReferenceFact> {
    let mut refs = Vec::new();
    collect_terminal_references(node, &mut refs);
    dedupe_column_refs(refs)
}

fn collect_terminal_references(node: &LineageNode, refs: &mut Vec<ColumnReferenceFact>) {
    if node.downstream.is_empty() {
        if let Some(reference) = column_reference_from_lineage_node(node) {
            refs.push(reference);
        }
        return;
    }

    for child in &node.downstream {
        collect_terminal_references(child, refs);
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
            });
        }
    }

    dedupe_column_refs(refs)
}

fn dedupe_column_refs(refs: Vec<ColumnReferenceFact>) -> Vec<ColumnReferenceFact> {
    let mut seen = HashSet::new();
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
        if seen.insert(key) {
            deduped.push(reference);
        }
    }

    deduped
}

fn relation_facts(
    scope: &Scope,
    mapping_schema: Option<&crate::schema::MappingSchema>,
) -> Vec<RelationFact> {
    let mut relations = Vec::new();
    let mut seen = HashSet::new();
    collect_relation_facts(scope, mapping_schema, &mut seen, &mut relations);

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
            columns: source_columns(source, mapping_schema),
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
        collect_relation_facts(branch_scope, mapping_schema, seen, relations);
    }
}

fn base_table_facts(
    scope: &Scope,
    mapping_schema: Option<&crate::schema::MappingSchema>,
) -> Vec<RelationFact> {
    let mut relations = Vec::new();
    let mut seen = HashSet::new();

    collect_base_table_facts(scope, mapping_schema, &mut seen, &mut relations);

    relations.sort_by(|left, right| left.name.cmp(&right.name));
    relations
}

fn collect_base_table_facts(
    scope: &Scope,
    mapping_schema: Option<&crate::schema::MappingSchema>,
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
                columns: source_columns(source, mapping_schema),
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
        collect_base_table_facts(child_scope, mapping_schema, seen, relations);
    }
}

fn source_columns(
    source: &SourceInfo,
    mapping_schema: Option<&crate::schema::MappingSchema>,
) -> Vec<String> {
    match &source.expression {
        Expression::Table(table) => mapping_schema
            .and_then(|schema| schema.column_names(&table_name(table)).ok())
            .unwrap_or_default(),
        Expression::Select(_)
        | Expression::Union(_)
        | Expression::Intersect(_)
        | Expression::Except(_) => get_output_column_names(&source.expression),
        Expression::Subquery(subquery) => get_output_column_names(&subquery.this),
        Expression::Cte(cte) if !cte.columns.is_empty() => cte
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect(),
        Expression::Cte(cte) => get_output_column_names(&cte.this),
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
) -> Vec<SetOperationFact> {
    let mut facts = Vec::new();
    collect_set_operation_facts(expression, scope, dialect, &mut facts);
    facts
}

fn collect_set_operation_facts(
    expression: &Expression,
    scope: &Scope,
    dialect: DialectType,
    facts: &mut Vec<SetOperationFact>,
) {
    match expression {
        Expression::Union(union) => {
            facts.push(SetOperationFact {
                kind: "union".to_string(),
                all: union.all,
                distinct: union.distinct,
                output_columns: get_output_column_names(expression),
                branches: set_operation_branches(&union.left, &union.right, scope, dialect),
            });
            collect_set_operation_facts(&union.left, scope, dialect, facts);
            collect_set_operation_facts(&union.right, scope, dialect, facts);
        }
        Expression::Intersect(intersect) => {
            facts.push(SetOperationFact {
                kind: "intersect".to_string(),
                all: intersect.all,
                distinct: intersect.distinct,
                output_columns: get_output_column_names(expression),
                branches: set_operation_branches(&intersect.left, &intersect.right, scope, dialect),
            });
            collect_set_operation_facts(&intersect.left, scope, dialect, facts);
            collect_set_operation_facts(&intersect.right, scope, dialect, facts);
        }
        Expression::Except(except) => {
            facts.push(SetOperationFact {
                kind: "except".to_string(),
                all: except.all,
                distinct: except.distinct,
                output_columns: get_output_column_names(expression),
                branches: set_operation_branches(&except.left, &except.right, scope, dialect),
            });
            collect_set_operation_facts(&except.left, scope, dialect, facts);
            collect_set_operation_facts(&except.right, scope, dialect, facts);
        }
        Expression::Subquery(subquery) => {
            collect_set_operation_facts(&subquery.this, scope, dialect, facts);
        }
        _ => {}
    }
}

fn set_operation_branches(
    left: &Expression,
    right: &Expression,
    scope: &Scope,
    dialect: DialectType,
) -> Vec<SetOperationBranchFact> {
    vec![
        SetOperationBranchFact {
            index: 0,
            projections: projection_facts_for_branch(left, scope, dialect),
        },
        SetOperationBranchFact {
            index: 1,
            projections: projection_facts_for_branch(right, scope, dialect),
        },
    ]
}

fn projection_facts_for_branch(
    expression: &Expression,
    root_scope: &Scope,
    dialect: DialectType,
) -> Vec<ProjectionFact> {
    let branch_scope = build_scope(expression);
    let scope = if branch_scope.sources.is_empty() {
        root_scope
    } else {
        &branch_scope
    };
    let nullability_context = NullabilityContext {
        schema: None,
        nullable_sources: nullable_source_names(expression),
    };
    projection_facts_for_query(expression, scope, dialect, &nullability_context)
}

fn non_empty_string(value: String) -> Option<String> {
    if value.is_empty() {
        None
    } else {
        Some(value)
    }
}
