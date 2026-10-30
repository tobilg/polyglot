//! OpenLineage-compatible payload generation.
//!
//! This module only builds OpenLineage JSON-compatible structures from SQL
//! analysis. It deliberately does not implement transports, clients, retries,
//! buffering, or runtime lifecycle management.

use crate::dialects::{Dialect, DialectType};
use crate::expressions::*;
use crate::lineage::{self, LineageNode, SetOperator};
use crate::schema::Schema;
use crate::scope::SourceKind;
use crate::traversal::{contains_aggregate, ExpressionWalk};
use crate::{mapping_schema_from_validation_schema_with_dialect, Error, Result, ValidationSchema};
use serde::de::{self, Deserializer};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, HashSet};

pub const OPENLINEAGE_SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json";
pub const COLUMN_LINEAGE_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-2-0/ColumnLineageDatasetFacet.json";
pub const SQL_JOB_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-1-0/SQLJobFacet.json";
pub const JOB_TYPE_JOB_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/2-0-3/JobTypeJobFacet.json";
pub const SCHEMA_DATASET_FACET_SCHEMA_URL: &str =
    "https://openlineage.io/spec/facets/1-2-0/SchemaDatasetFacet.json";

/// Dataset identity in OpenLineage (`namespace`, `name`).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageDatasetId {
    pub namespace: String,
    pub name: String,
}

impl OpenLineageDatasetId {
    pub fn new(namespace: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            name: name.into(),
        }
    }
}

/// Options shared by OpenLineage payload generation helpers.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase", default)]
pub struct OpenLineageOptions {
    #[serde(deserialize_with = "deserialize_dialect_type")]
    pub dialect: DialectType,
    pub producer: String,
    pub dataset_namespace: Option<String>,
    pub dataset_mappings: BTreeMap<String, OpenLineageDatasetId>,
    pub output_dataset: Option<OpenLineageDatasetId>,
    pub schema: Option<ValidationSchema>,
    pub job_namespace: Option<String>,
    pub job_name: Option<String>,
    pub event_time: Option<String>,
    pub run_id: Option<String>,
    pub event_type: Option<OpenLineageRunEventType>,
}

/// OpenLineage run event type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum OpenLineageRunEventType {
    Start,
    Running,
    Complete,
    Abort,
    Fail,
    Other,
}

/// Non-fatal issue encountered while generating OpenLineage output.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageWarning {
    pub code: String,
    pub message: String,
}

impl OpenLineageWarning {
    fn new(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageColumnLineageResult {
    pub facet: ColumnLineageDatasetFacet,
    pub inputs: Vec<OpenLineageDataset>,
    pub outputs: Vec<OpenLineageDataset>,
    pub warnings: Vec<OpenLineageWarning>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageEventResult {
    pub event: Value,
    pub warnings: Vec<OpenLineageWarning>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenLineageDataset {
    pub namespace: String,
    pub name: String,
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    pub facets: BTreeMap<String, Value>,
}

impl std::convert::From<OpenLineageDatasetId> for OpenLineageDataset {
    fn from(id: OpenLineageDatasetId) -> Self {
        Self {
            namespace: id.namespace,
            name: id.name,
            facets: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnLineageDatasetFacet {
    #[serde(rename = "_producer")]
    pub producer: String,
    #[serde(rename = "_schemaURL")]
    pub schema_url: String,
    pub fields: BTreeMap<String, ColumnLineageField>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnLineageField {
    pub input_fields: Vec<OpenLineageInputField>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OpenLineageInputField {
    pub namespace: String,
    pub name: String,
    pub field: String,
    #[serde(skip_serializing_if = "Vec::is_empty", default)]
    pub transformations: Vec<OpenLineageTransformation>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OpenLineageTransformation {
    #[serde(rename = "type")]
    pub type_: String,
    pub subtype: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub masking: Option<bool>,
}

#[derive(Debug, Clone)]
struct StatementAnalysis {
    query: Expression,
    inputs: Vec<OpenLineageDatasetId>,
    output: OpenLineageDatasetId,
    output_column_names: Vec<String>,
}

#[derive(Debug, Clone)]
struct OutputField {
    name: String,
    lineage_name: String,
    expression: Option<Expression>,
    star_source_table: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TerminalField {
    table: String,
    field: String,
    dependency: TerminalDependency,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum TerminalDependency {
    Direct,
    Filter,
}

/// Produce a standalone OpenLineage columnLineage facet plus inferred datasets.
pub fn openlineage_column_lineage(
    sql: &str,
    options: &OpenLineageOptions,
) -> Result<OpenLineageColumnLineageResult> {
    validate_common_options(options)?;

    let mut warnings = Vec::new();
    let schema_mapping = options
        .schema
        .as_ref()
        .map(|schema| mapping_schema_from_validation_schema_with_dialect(schema, options.dialect));
    let dialect = Dialect::get(options.dialect);
    let mut expressions = dialect.parse(sql)?;
    if expressions.len() != 1 {
        return Err(Error::parse(
            format!(
                "OpenLineage generation expects exactly one statement, found {}",
                expressions.len()
            ),
            0,
            0,
            0,
            0,
        ));
    }

    let expr = expressions.remove(0);
    let analysis = analyze_statement(&expr, options, &mut warnings)?;
    let mut output_fields = output_fields_for_query(
        &analysis.query,
        schema_mapping.as_ref().map(|s| s as &dyn Schema),
        options.dialect,
        &mut warnings,
    )?;
    apply_output_column_names(
        &mut output_fields,
        &analysis.output_column_names,
        &mut warnings,
    );

    let mut fields = BTreeMap::new();
    for output_field in output_fields {
        if fields.contains_key(&output_field.name) {
            warnings.push(OpenLineageWarning::new(
                "W_DUPLICATE_OUTPUT_FIELD",
                format!(
                    "Duplicate output field '{}' was merged in the OpenLineage fields map",
                    output_field.name
                ),
            ));
        }

        let input_fields = input_fields_for_output(
            &analysis.query,
            &output_field,
            options,
            schema_mapping.as_ref().map(|s| s as &dyn Schema),
            &mut warnings,
        )?;

        fields.insert(output_field.name, ColumnLineageField { input_fields });
    }

    let mut outputs = vec![OpenLineageDataset::from(analysis.output.clone())];
    attach_output_facets(&mut outputs[0], &analysis.output, options, &fields)?;

    Ok(OpenLineageColumnLineageResult {
        facet: ColumnLineageDatasetFacet {
            producer: options.producer.clone(),
            schema_url: COLUMN_LINEAGE_FACET_SCHEMA_URL.to_string(),
            fields,
        },
        inputs: analysis
            .inputs
            .into_iter()
            .map(OpenLineageDataset::from)
            .collect(),
        outputs,
        warnings,
    })
}

/// Produce an OpenLineage JobEvent as JSON.
pub fn openlineage_job_event(
    sql: &str,
    options: &OpenLineageOptions,
) -> Result<OpenLineageEventResult> {
    let job_namespace = required_option(&options.job_namespace, "jobNamespace")?;
    let job_name = required_option(&options.job_name, "jobName")?;
    let event_time = required_option(&options.event_time, "eventTime")?;

    let result = openlineage_column_lineage(sql, options)?;
    let event = json!({
        "eventTime": event_time,
        "producer": options.producer,
        "schemaURL": OPENLINEAGE_SCHEMA_URL,
        "job": {
            "namespace": job_namespace,
            "name": job_name,
            "facets": job_facets(sql, options),
        },
        "inputs": result.inputs,
        "outputs": result.outputs,
    });

    Ok(OpenLineageEventResult {
        event,
        warnings: result.warnings,
    })
}

/// Produce an OpenLineage RunEvent as JSON.
pub fn openlineage_run_event(
    sql: &str,
    options: &OpenLineageOptions,
) -> Result<OpenLineageEventResult> {
    let job_namespace = required_option(&options.job_namespace, "jobNamespace")?;
    let job_name = required_option(&options.job_name, "jobName")?;
    let event_time = required_option(&options.event_time, "eventTime")?;
    let run_id = required_option(&options.run_id, "runId")?;
    let event_type = options
        .event_type
        .ok_or_else(|| Error::parse("Missing required option: eventType", 0, 0, 0, 0))?;

    let result = openlineage_column_lineage(sql, options)?;
    let event = json!({
        "eventTime": event_time,
        "eventType": event_type,
        "producer": options.producer,
        "schemaURL": OPENLINEAGE_SCHEMA_URL,
        "run": {
            "runId": run_id,
            "facets": {},
        },
        "job": {
            "namespace": job_namespace,
            "name": job_name,
            "facets": job_facets(sql, options),
        },
        "inputs": result.inputs,
        "outputs": result.outputs,
    });

    Ok(OpenLineageEventResult {
        event,
        warnings: result.warnings,
    })
}

fn validate_common_options(options: &OpenLineageOptions) -> Result<()> {
    if options.producer.trim().is_empty() {
        return Err(Error::parse(
            "Missing required option: producer",
            0,
            0,
            0,
            0,
        ));
    }
    Ok(())
}

fn required_option(value: &Option<String>, name: &str) -> Result<String> {
    match value.as_ref().filter(|v| !v.trim().is_empty()) {
        Some(value) => Ok(value.clone()),
        None => Err(Error::parse(
            format!("Missing required option: {name}"),
            0,
            0,
            0,
            0,
        )),
    }
}

fn analyze_statement(
    expr: &Expression,
    options: &OpenLineageOptions,
    warnings: &mut Vec<OpenLineageWarning>,
) -> Result<StatementAnalysis> {
    match expr {
        Expression::Prepare(prepare) => analyze_statement(&prepare.statement, options, warnings),
        Expression::Select(select) => {
            let output = if let Some(into) = &select.into {
                dataset_from_expression(&into.this, options)?
            } else {
                options.output_dataset.clone().ok_or_else(|| {
                    Error::parse(
                        "OpenLineage outputDataset is required for SELECT statements without SELECT INTO",
                        0,
                        0,
                        0,
                        0,
                    )
                })?
            };
            Ok(StatementAnalysis {
                query: expr.clone(),
                inputs: collect_input_datasets(expr, options, Some(&output), warnings)?,
                output,
                output_column_names: Vec::new(),
            })
        }
        Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_) => {
            let output = options.output_dataset.clone().ok_or_else(|| {
                Error::parse(
                    "OpenLineage outputDataset is required for set-operation queries",
                    0,
                    0,
                    0,
                    0,
                )
            })?;
            Ok(StatementAnalysis {
                query: expr.clone(),
                inputs: collect_input_datasets(expr, options, Some(&output), warnings)?,
                output,
                output_column_names: Vec::new(),
            })
        }
        Expression::Insert(insert) => {
            let output = dataset_from_table_ref(&insert.table, options)?;
            let query = insert.query.clone().ok_or_else(|| {
                Error::unsupported(
                    "OpenLineage column lineage for INSERT without query",
                    options.dialect.to_string(),
                )
            })?;
            Ok(StatementAnalysis {
                inputs: collect_input_datasets(&query, options, Some(&output), warnings)?,
                query,
                output,
                output_column_names: insert.columns.iter().map(|col| col.name.clone()).collect(),
            })
        }
        Expression::CreateTable(create) => {
            let output = dataset_from_table_ref(&create.name, options)?;
            let query = create.as_select.clone().ok_or_else(|| {
                Error::unsupported(
                    "OpenLineage column lineage for CREATE TABLE without AS SELECT",
                    options.dialect.to_string(),
                )
            })?;
            Ok(StatementAnalysis {
                inputs: collect_input_datasets(&query, options, Some(&output), warnings)?,
                query,
                output,
                output_column_names: create
                    .columns
                    .iter()
                    .map(|col| col.name.name.clone())
                    .collect(),
            })
        }
        _ => Err(Error::unsupported(
            format!("OpenLineage generation for {}", expr.variant_name()),
            options.dialect.to_string(),
        )),
    }
}

fn output_fields_for_query(
    query: &Expression,
    schema: Option<&dyn Schema>,
    dialect: DialectType,
    warnings: &mut Vec<OpenLineageWarning>,
) -> Result<Vec<OutputField>> {
    // A star over a CTE is not in the schema. Expand it from the CTE output columns.
    let mut query = query.clone();
    lineage::expand_cte_stars(&mut query, schema);
    let select = leftmost_select(&query).ok_or_else(|| {
        Error::unsupported(
            "OpenLineage output field extraction for non-SELECT query",
            dialect.to_string(),
        )
    })?;

    let mut fields = Vec::new();
    for (idx, expr) in select.expressions.iter().enumerate() {
        if is_star_expr(expr) {
            expand_star_output_fields(select, expr, schema, dialect, warnings, &mut fields);
            continue;
        }

        let name = output_name(expr).unwrap_or_else(|| format!("_{idx}"));
        fields.push(OutputField {
            lineage_name: name.clone(),
            name,
            expression: Some(expr.clone()),
            star_source_table: None,
        });
    }
    Ok(fields)
}

fn apply_output_column_names(
    fields: &mut [OutputField],
    output_column_names: &[String],
    warnings: &mut Vec<OpenLineageWarning>,
) {
    if output_column_names.is_empty() {
        return;
    }
    if output_column_names.len() != fields.len() {
        warnings.push(OpenLineageWarning::new(
            "W_OUTPUT_COLUMN_COUNT_MISMATCH",
            format!(
                "Target column count ({}) does not match projected column count ({})",
                output_column_names.len(),
                fields.len()
            ),
        ));
        return;
    }
    for (field, output_name) in fields.iter_mut().zip(output_column_names) {
        field.name = output_name.clone();
    }
}

fn input_fields_for_output(
    query: &Expression,
    output_field: &OutputField,
    options: &OpenLineageOptions,
    schema: Option<&dyn Schema>,
    warnings: &mut Vec<OpenLineageWarning>,
) -> Result<Vec<OpenLineageInputField>> {
    if let Some(table) = &output_field.star_source_table {
        return terminal_fields_to_openlineage(
            vec![TerminalField {
                table: table.clone(),
                field: output_field.lineage_name.clone(),
                dependency: TerminalDependency::Direct,
            }],
            "IDENTITY",
            Some(format!("SELECT {}", output_field.lineage_name)),
            options,
            warnings,
        );
    }

    let lineage_result = if let Some(schema) = schema {
        lineage::lineage_with_schema(
            &output_field.lineage_name,
            query,
            Some(schema),
            Some(options.dialect),
            false,
        )
    } else {
        lineage::lineage(
            &output_field.lineage_name,
            query,
            Some(options.dialect),
            false,
        )
    };

    let node = match lineage_result {
        Ok(node) => node,
        Err(err) => {
            warnings.push(OpenLineageWarning::new(
                "W_UNRESOLVED_OUTPUT_FIELD",
                format!(
                    "Could not resolve lineage for output field '{}': {}",
                    output_field.name, err
                ),
            ));
            return Ok(Vec::new());
        }
    };

    let mut terminals = BTreeSet::new();
    collect_terminal_fields(&node, TerminalDependency::Direct, &mut terminals);
    let terminals: Vec<TerminalField> = terminals.into_iter().collect();

    if terminals.is_empty() {
        if has_virtual_terminal(&node) {
            return Ok(Vec::new());
        }
        warnings.push(OpenLineageWarning::new(
            "W_EMPTY_FIELD_LINEAGE",
            format!(
                "No input fields were found for output field '{}'",
                output_field.name
            ),
        ));
        return Ok(Vec::new());
    }

    let subtype = transformation_subtype(output_field.expression.as_ref(), &terminals);
    let description = output_field
        .expression
        .as_ref()
        .and_then(|expr| transformation_description(expr, options.dialect));

    terminal_fields_to_openlineage(terminals, subtype, description, options, warnings)
}

fn transformation_description(expr: &Expression, dialect: DialectType) -> Option<String> {
    #[cfg(feature = "generate")]
    {
        Some(expr.sql_for(dialect))
    }

    #[cfg(not(feature = "generate"))]
    {
        let _ = (expr, dialect);
        None
    }
}

fn terminal_fields_to_openlineage(
    terminals: Vec<TerminalField>,
    subtype: &str,
    description: Option<String>,
    options: &OpenLineageOptions,
    warnings: &mut Vec<OpenLineageWarning>,
) -> Result<Vec<OpenLineageInputField>> {
    let mut grouped =
        BTreeMap::<(String, String, String), BTreeSet<OpenLineageTransformation>>::new();
    for terminal in terminals {
        let dataset = dataset_from_table_name(&terminal.table, options).map_err(|err| {
            warnings.push(OpenLineageWarning::new(
                "W_UNRESOLVED_DATASET",
                format!(
                    "Could not map table '{}' to an OpenLineage dataset: {}",
                    terminal.table, err
                ),
            ));
            err
        })?;
        let (type_, transformation_subtype) = match terminal.dependency {
            TerminalDependency::Direct => ("DIRECT", subtype),
            TerminalDependency::Filter => ("INDIRECT", "FILTER"),
        };
        grouped
            .entry((dataset.namespace, dataset.name, terminal.field))
            .or_default()
            .insert(OpenLineageTransformation {
                type_: type_.to_string(),
                subtype: transformation_subtype.to_string(),
                description: description.clone(),
                masking: Some(false),
            });
    }
    Ok(grouped
        .into_iter()
        .map(
            |((namespace, name, field), transformations)| OpenLineageInputField {
                namespace,
                name,
                field,
                transformations: transformations.into_iter().collect(),
            },
        )
        .collect())
}

fn transformation_subtype(expr: Option<&Expression>, terminals: &[TerminalField]) -> &'static str {
    let Some(expr) = expr else {
        return "TRANSFORMATION";
    };
    let unaliased = unalias(expr);
    if expression_contains_aggregate(unaliased) {
        return "AGGREGATION";
    }
    let distinct_fields = terminals
        .iter()
        .map(|terminal| (&terminal.table, &terminal.field))
        .collect::<BTreeSet<_>>();
    if distinct_fields.len() == 1 {
        if let Expression::Column(col) = unaliased {
            if col.name.name == terminals[0].field {
                return "IDENTITY";
            }
        }
    }
    "TRANSFORMATION"
}

fn collect_terminal_fields(
    node: &LineageNode,
    inherited_dependency: TerminalDependency,
    terminals: &mut BTreeSet<TerminalField>,
) {
    let dependency = if inherited_dependency == TerminalDependency::Filter
        || matches!(
            node.set_branch,
            Some(branch)
                if branch.ordinal == 1
                    && matches!(branch.operator, SetOperator::Intersect | SetOperator::Except)
        ) {
        TerminalDependency::Filter
    } else {
        TerminalDependency::Direct
    };

    if node.downstream.is_empty() {
        if node.source_kind == SourceKind::Virtual {
            return;
        }
        if let Expression::Column(column) = &node.expression {
            let table = if !node.source_name.is_empty() {
                Some(node.source_name.clone())
            } else if let Expression::Table(table) = &node.source {
                Some(table_ref_qualified_name(table))
            } else {
                column.table.as_ref().map(|t| t.name.clone())
            };
            if let Some(table) = table.filter(|t| !t.is_empty()) {
                terminals.insert(TerminalField {
                    table,
                    field: column.name.name.clone(),
                    dependency,
                });
            }
        }
        return;
    }

    for child in &node.downstream {
        collect_terminal_fields(child, dependency, terminals);
    }
}

fn has_virtual_terminal(node: &LineageNode) -> bool {
    if node.downstream.is_empty() {
        return node.source_kind == SourceKind::Virtual;
    }
    node.downstream.iter().any(has_virtual_terminal)
}

fn expression_contains_aggregate(expr: &Expression) -> bool {
    contains_aggregate(expr)
}

fn collect_input_datasets(
    expr: &Expression,
    options: &OpenLineageOptions,
    output: Option<&OpenLineageDatasetId>,
    warnings: &mut Vec<OpenLineageWarning>,
) -> Result<Vec<OpenLineageDatasetId>> {
    let cte_aliases = collect_cte_aliases(expr, options.dialect);
    let mut seen = BTreeSet::new();
    let mut result = Vec::new();

    for table in expr.dfs().filter_map(|node| match node {
        Expression::Table(table) => Some(table),
        _ => None,
    }) {
        let qname = table_ref_qualified_name(table);
        let normalized = normalize_identifier(&table.name.name, options.dialect, true);
        if cte_aliases.contains(&normalized) {
            continue;
        }
        if output
            .map(|out| out.name == qname || out.name == table.name.name)
            .unwrap_or(false)
        {
            continue;
        }
        match dataset_from_table_name(&qname, options) {
            Ok(dataset) => {
                if seen.insert((dataset.namespace.clone(), dataset.name.clone())) {
                    result.push(dataset);
                }
            }
            Err(err) => warnings.push(OpenLineageWarning::new(
                "W_UNRESOLVED_DATASET",
                format!("Could not map input table '{qname}': {err}"),
            )),
        }
    }

    Ok(result)
}

fn attach_output_facets(
    output: &mut OpenLineageDataset,
    output_id: &OpenLineageDatasetId,
    options: &OpenLineageOptions,
    fields: &BTreeMap<String, ColumnLineageField>,
) -> Result<()> {
    let column_lineage = ColumnLineageDatasetFacet {
        producer: options.producer.clone(),
        schema_url: COLUMN_LINEAGE_FACET_SCHEMA_URL.to_string(),
        fields: fields.clone(),
    };
    output.facets.insert(
        "columnLineage".to_string(),
        serde_json::to_value(column_lineage).map_err(openlineage_serialization_error)?,
    );

    if let Some(schema_facet) = schema_facet_for_dataset(output_id, options) {
        output.facets.insert(
            "schema".to_string(),
            serde_json::to_value(schema_facet).map_err(openlineage_serialization_error)?,
        );
    }

    Ok(())
}

fn job_facets(sql: &str, options: &OpenLineageOptions) -> Value {
    json!({
        "sql": {
            "_producer": options.producer,
            "_schemaURL": SQL_JOB_FACET_SCHEMA_URL,
            "query": sql,
            "dialect": options.dialect.to_string(),
        },
        "jobType": {
            "_producer": options.producer,
            "_schemaURL": JOB_TYPE_JOB_FACET_SCHEMA_URL,
            "processingType": "BATCH",
            "integration": "POLYGLOT_SQL",
            "jobType": "QUERY",
        }
    })
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SchemaDatasetFacet {
    #[serde(rename = "_producer")]
    producer: String,
    #[serde(rename = "_schemaURL")]
    schema_url: String,
    fields: Vec<SchemaDatasetFacetField>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct SchemaDatasetFacetField {
    name: String,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    #[serde(rename = "type")]
    data_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    ordinal_position: Option<usize>,
}

fn schema_facet_for_dataset(
    output: &OpenLineageDatasetId,
    options: &OpenLineageOptions,
) -> Option<SchemaDatasetFacet> {
    let schema = options.schema.as_ref()?;
    let table = schema.tables.iter().find(|table| {
        let qname = if let Some(schema_name) = &table.schema {
            format!("{}.{}", schema_name, table.name)
        } else {
            table.name.clone()
        };
        output.name == table.name || output.name == qname
    })?;

    Some(SchemaDatasetFacet {
        producer: options.producer.clone(),
        schema_url: SCHEMA_DATASET_FACET_SCHEMA_URL.to_string(),
        fields: table
            .columns
            .iter()
            .enumerate()
            .map(|(idx, col)| SchemaDatasetFacetField {
                name: col.name.clone(),
                data_type: col.data_type.clone(),
                ordinal_position: Some(idx + 1),
            })
            .collect(),
    })
}

fn expand_star_output_fields(
    select: &Select,
    star_expr: &Expression,
    schema: Option<&dyn Schema>,
    dialect: DialectType,
    warnings: &mut Vec<OpenLineageWarning>,
    fields: &mut Vec<OutputField>,
) {
    let Some(schema) = schema else {
        warnings.push(OpenLineageWarning::new(
            "W_STAR_WITHOUT_SCHEMA",
            "SELECT * cannot be expanded into OpenLineage column lineage without schema metadata",
        ));
        return;
    };

    let qualifier = star_qualifier(star_expr);
    let star = match star_expr {
        Expression::Star(star) => Some(star),
        _ => None,
    };
    if star.is_some_and(|star| star.has_except_pattern()) {
        warnings.push(OpenLineageWarning::new(
            "W_STAR_EXCEPT_PATTERN",
            "SELECT * EXCEPT with a pattern cannot be expanded into OpenLineage column lineage",
        ));
        return;
    }
    let sources = select_source_tables(select);
    for (alias, source) in sources {
        let (columns, star_source_table) = match source {
            StarSource::Table(qname) => {
                if qualifier
                    .as_ref()
                    .map(|q| q != &alias && q != &qname)
                    .unwrap_or(false)
                {
                    continue;
                }
                match schema.column_names(&qname) {
                    Ok(columns) => (columns, Some(qname)),
                    Err(err) => {
                        warnings.push(OpenLineageWarning::new(
                            "W_STAR_SCHEMA_LOOKUP_FAILED",
                            format!("Could not expand SELECT * for table '{qname}': {err}"),
                        ));
                        continue;
                    }
                }
            }
            StarSource::Derived(query) => {
                if qualifier.as_ref().is_some_and(|q| q != &alias) {
                    continue;
                }
                let columns =
                    lineage::output_columns_with_schema(query, Some(schema), Some(dialect))
                        .ok()
                        .and_then(|output| {
                            output
                                .columns
                                .into_iter()
                                .map(|column| match column {
                                    lineage::OutputColumn::Named { name, .. } => Some(name),
                                    _ => None,
                                })
                                .collect::<Option<Vec<_>>>()
                        });
                let Some(columns) = columns else {
                    warnings.push(OpenLineageWarning::new(
                        "W_STAR_SCHEMA_LOOKUP_FAILED",
                        format!("Could not expand SELECT * for derived table '{alias}'"),
                    ));
                    continue;
                };
                (columns, None)
            }
        };

        for name in columns {
            if star.is_some_and(|star| star.excludes(&name, Some(dialect))) {
                continue;
            }
            let output_name = star
                .and_then(|star| star.renamed(&name, Some(dialect)))
                .map_or_else(|| name.clone(), |new| new.name);
            if let Some(replacement) = star.and_then(|star| star.replacement(&name, Some(dialect)))
            {
                fields.push(OutputField {
                    lineage_name: output_name.clone(),
                    name: output_name,
                    expression: Some(Expression::Alias(Box::new(replacement.clone()))),
                    star_source_table: None,
                });
                continue;
            }
            // Derived table columns resolve through the lineage of the whole query.
            let (lineage_name, expression) = match &star_source_table {
                Some(_) => (name, None),
                None => (
                    output_name.clone(),
                    Some(Expression::qualified_column(alias.as_str(), name)),
                ),
            };
            fields.push(OutputField {
                name: output_name,
                lineage_name,
                expression,
                star_source_table: star_source_table.clone(),
            });
        }
    }
}

/// A FROM or JOIN source that a star can expand.
enum StarSource<'a> {
    Table(String),
    Derived(&'a Expression),
}

fn select_source_tables(select: &Select) -> Vec<(String, StarSource<'_>)> {
    let mut result = Vec::new();
    if let Some(from) = &select.from {
        for expr in &from.expressions {
            collect_source_table(expr, &mut result);
        }
    }
    for join in &select.joins {
        collect_source_table(&join.this, &mut result);
    }
    result
}

fn collect_source_table<'a>(expr: &'a Expression, result: &mut Vec<(String, StarSource<'a>)>) {
    match expr {
        Expression::Table(table) => {
            let qname = table_ref_qualified_name(table);
            let alias = table
                .alias
                .as_ref()
                .map(|a| a.name.clone())
                .unwrap_or_else(|| table.name.name.clone());
            result.push((alias, StarSource::Table(qname)));
        }
        Expression::Subquery(subquery) => {
            if let Some(alias) = &subquery.alias {
                result.push((alias.name.clone(), StarSource::Derived(&subquery.this)));
            }
        }
        Expression::Alias(alias) => collect_source_table(&alias.this, result),
        Expression::Paren(paren) => collect_source_table(&paren.this, result),
        _ => {}
    }
}

fn leftmost_select(expr: &Expression) -> Option<&Select> {
    match expr {
        Expression::Prepare(prepare) => leftmost_select(&prepare.statement),
        Expression::Select(select) => Some(select),
        Expression::Union(union) => leftmost_select(&union.left),
        Expression::Intersect(intersect) => leftmost_select(&intersect.left),
        Expression::Except(except) => leftmost_select(&except.left),
        Expression::Subquery(subquery) => leftmost_select(&subquery.this),
        _ => None,
    }
}

fn output_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Alias(alias) => Some(alias.alias.name.clone()),
        Expression::Column(col) => Some(col.name.name.clone()),
        Expression::Identifier(id) => Some(id.name.clone()),
        Expression::Annotated(a) => output_name(&a.this),
        _ => None,
    }
}

fn unalias(expr: &Expression) -> &Expression {
    match expr {
        Expression::Alias(alias) => &alias.this,
        Expression::Annotated(a) => unalias(&a.this),
        _ => expr,
    }
}

fn is_star_expr(expr: &Expression) -> bool {
    matches!(expr, Expression::Star(_))
        || matches!(expr, Expression::Column(col) if col.name.name == "*")
}

fn star_qualifier(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Star(star) => star.table.as_ref().map(|t| t.name.clone()),
        Expression::Column(col) if col.name.name == "*" => {
            col.table.as_ref().map(|t| t.name.clone())
        }
        _ => None,
    }
}

fn dataset_from_expression(
    expr: &Expression,
    options: &OpenLineageOptions,
) -> Result<OpenLineageDatasetId> {
    match expr {
        Expression::Table(table) => dataset_from_table_ref(table, options),
        Expression::Identifier(id) => dataset_from_table_name(&id.name, options),
        _ => Err(Error::unsupported(
            "OpenLineage dataset extraction from non-table expression",
            options.dialect.to_string(),
        )),
    }
}

fn dataset_from_table_ref(
    table: &TableRef,
    options: &OpenLineageOptions,
) -> Result<OpenLineageDatasetId> {
    dataset_from_table_name(&table_ref_qualified_name(table), options)
}

fn dataset_from_table_name(
    table_name: &str,
    options: &OpenLineageOptions,
) -> Result<OpenLineageDatasetId> {
    if let Some(mapped) = options.dataset_mappings.get(table_name) {
        return Ok(mapped.clone());
    }
    let namespace = options.dataset_namespace.as_ref().ok_or_else(|| {
        Error::parse(
            format!(
                "Missing datasetNamespace or explicit dataset mapping for table '{}'",
                table_name
            ),
            0,
            0,
            0,
            0,
        )
    })?;
    Ok(OpenLineageDatasetId::new(namespace, table_name))
}

fn table_ref_qualified_name(table: &TableRef) -> String {
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

fn collect_cte_aliases(expr: &Expression, dialect: DialectType) -> HashSet<String> {
    let mut aliases = HashSet::new();
    for node in expr.dfs() {
        match node {
            Expression::Select(select) => {
                if let Some(with) = &select.with {
                    collect_with_aliases(with, dialect, &mut aliases);
                }
            }
            Expression::Union(union) => {
                if let Some(with) = &union.with {
                    collect_with_aliases(with, dialect, &mut aliases);
                }
            }
            Expression::Intersect(intersect) => {
                if let Some(with) = &intersect.with {
                    collect_with_aliases(with, dialect, &mut aliases);
                }
            }
            Expression::Except(except) => {
                if let Some(with) = &except.with {
                    collect_with_aliases(with, dialect, &mut aliases);
                }
            }
            _ => {}
        }
    }
    aliases
}

fn collect_with_aliases(with: &With, dialect: DialectType, aliases: &mut HashSet<String>) {
    for cte in &with.ctes {
        aliases.insert(normalize_identifier(&cte.alias.name, dialect, true));
    }
}

fn normalize_identifier(name: &str, dialect: DialectType, is_table: bool) -> String {
    crate::schema::normalize_name(name, Some(dialect), is_table, true)
}

fn openlineage_serialization_error(err: serde_json::Error) -> Error {
    Error::internal(format!("OpenLineage serialization failed: {err}"))
}

fn deserialize_dialect_type<'de, D>(deserializer: D) -> std::result::Result<DialectType, D::Error>
where
    D: Deserializer<'de>,
{
    let value = String::deserialize(deserializer)?;
    value.parse::<DialectType>().map_err(de::Error::custom)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn options() -> OpenLineageOptions {
        OpenLineageOptions {
            dialect: DialectType::PostgreSQL,
            producer: "https://github.com/tobilg/polyglot".to_string(),
            dataset_namespace: Some("postgres://warehouse".to_string()),
            output_dataset: Some(OpenLineageDatasetId::new(
                "postgres://warehouse",
                "analytics.out",
            )),
            job_namespace: Some("polyglot-tests".to_string()),
            job_name: Some("lineage-test".to_string()),
            event_time: Some("2026-05-18T00:00:00Z".to_string()),
            run_id: Some("3b452093-782c-4ef2-9c0c-aafe2aa6f34d".to_string()),
            event_type: Some(OpenLineageRunEventType::Complete),
            ..Default::default()
        }
    }

    #[test]
    fn deserializes_dialect_aliases_in_options() {
        let options: OpenLineageOptions =
            serde_json::from_str(r#"{"producer":"polyglot","dialect":"postgres"}"#)
                .expect("options");
        assert_eq!(options.dialect, DialectType::PostgreSQL);
    }

    #[test]
    fn emits_identity_column_lineage_for_select() {
        let result = openlineage_column_lineage("SELECT a FROM t", &options()).expect("lineage");
        let field = result.facet.fields.get("a").expect("field a");
        assert_eq!(field.input_fields.len(), 1);
        assert_eq!(field.input_fields[0].name, "t");
        assert_eq!(field.input_fields[0].field, "a");
        assert_eq!(field.input_fields[0].transformations[0].subtype, "IDENTITY");
    }

    #[test]
    fn emits_set_operation_value_and_filter_dependencies() {
        let union = openlineage_column_lineage(
            "SELECT a FROM left_table UNION ALL SELECT b FROM right_table",
            &options(),
        )
        .expect("union lineage");
        let union_field = union.facet.fields.get("a").expect("union field");
        assert_eq!(union_field.input_fields.len(), 2);
        assert!(union_field.input_fields.iter().all(|input| input
            .transformations
            .iter()
            .all(|transformation| transformation.type_ == "DIRECT")));

        for operator in ["EXCEPT", "INTERSECT"] {
            let result = openlineage_column_lineage(
                &format!("SELECT a FROM left_table {operator} SELECT b FROM right_table"),
                &options(),
            )
            .unwrap_or_else(|error| panic!("{operator} lineage failed: {error}"));
            let field = result.facet.fields.get("a").expect("output field");
            let left = field
                .input_fields
                .iter()
                .find(|input| input.name == "left_table")
                .expect("left input");
            let right = field
                .input_fields
                .iter()
                .find(|input| input.name == "right_table")
                .expect("right input");
            assert!(left
                .transformations
                .iter()
                .all(|transformation| transformation.type_ == "DIRECT"));
            assert!(right.transformations.iter().any(|transformation| {
                transformation.type_ == "INDIRECT" && transformation.subtype == "FILTER"
            }));
        }

        let merged = openlineage_column_lineage(
            "SELECT a FROM shared_table EXCEPT SELECT a FROM shared_table",
            &options(),
        )
        .expect("merged dependency lineage");
        let merged_field = merged.facet.fields.get("a").expect("merged field");
        assert_eq!(merged_field.input_fields.len(), 1);
        assert!(merged_field.input_fields[0]
            .transformations
            .iter()
            .any(|transformation| transformation.type_ == "DIRECT"));
        assert!(merged_field.input_fields[0]
            .transformations
            .iter()
            .any(|transformation| {
                transformation.type_ == "INDIRECT" && transformation.subtype == "FILTER"
            }));

        let nested = openlineage_column_lineage(
            "SELECT a FROM left_table EXCEPT \
             (SELECT b FROM right_table UNION ALL SELECT c FROM third_table)",
            &options(),
        )
        .expect("nested set-operation lineage");
        let nested_field = nested.facet.fields.get("a").expect("nested field");
        for table in ["right_table", "third_table"] {
            let input = nested_field
                .input_fields
                .iter()
                .find(|input| input.name == table)
                .unwrap_or_else(|| panic!("missing nested input {table}"));
            assert!(input.transformations.iter().any(|transformation| {
                transformation.type_ == "INDIRECT" && transformation.subtype == "FILTER"
            }));
        }
    }

    #[test]
    fn emits_column_lineage_for_prepared_statement_body() {
        let result = openlineage_column_lineage(
            "PREPARE leak AS SELECT id FROM sensitive_table WHERE id = $1",
            &options(),
        )
        .expect("lineage");
        let field = result.facet.fields.get("id").expect("field id");
        assert_eq!(field.input_fields.len(), 1);
        assert_eq!(field.input_fields[0].name, "sensitive_table");
        assert_eq!(field.input_fields[0].field, "id");
    }

    #[test]
    fn resolves_input_dataset_behind_table_alias() {
        let result = openlineage_column_lineage("SELECT o.total FROM orders o", &options())
            .expect("lineage");
        let field = result.facet.fields.get("total").expect("field total");
        assert_eq!(field.input_fields[0].name, "orders");
        assert_eq!(field.input_fields[0].field, "total");
    }

    #[test]
    fn emits_transformation_column_lineage_for_expression() {
        let result =
            openlineage_column_lineage("SELECT a + b AS c FROM t", &options()).expect("lineage");
        let field = result.facet.fields.get("c").expect("field c");
        assert_eq!(field.input_fields.len(), 2);
        assert!(field.input_fields.iter().any(|f| f.field == "a"));
        assert!(field.input_fields.iter().any(|f| f.field == "b"));
        assert!(field
            .input_fields
            .iter()
            .all(|f| f.transformations[0].subtype == "TRANSFORMATION"));
    }

    #[test]
    fn omits_bigquery_safe_namespace_from_column_lineage_issue207() {
        let mut opts = options();
        opts.dialect = DialectType::BigQuery;

        let result = openlineage_column_lineage(
            r#"
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
"#,
            &opts,
        )
        .expect("lineage");
        let field = result.facet.fields.get("json_data").expect("json_data");

        assert!(
            field.input_fields.iter().any(|input| input.field == "data"),
            "expected data input field, got {:?}",
            field.input_fields
        );
        assert!(
            !field
                .input_fields
                .iter()
                .any(|input| input.field.eq_ignore_ascii_case("safe")),
            "did not expect SAFE namespace as input field, got {:?}",
            field.input_fields
        );
    }

    #[test]
    fn emits_bigquery_unnest_alias_column_lineage_issue209() {
        let mut opts = options();
        opts.dialect = DialectType::BigQuery;
        opts.dataset_namespace = Some("bigquery://warehouse".to_string());
        opts.output_dataset = Some(OpenLineageDatasetId::new(
            "bigquery://warehouse",
            "calendar",
        ));

        let result = openlineage_column_lineage(
            r#"
SELECT date_val AS week_start
FROM UNNEST(GENERATE_DATE_ARRAY('2024-01-01', '2024-12-31', INTERVAL 1 WEEK)) AS date_val
"#,
            &opts,
        )
        .expect("lineage");
        let field = result.facet.fields.get("week_start").expect("week_start");

        assert!(field.input_fields.is_empty());
        assert!(
            result
                .warnings
                .iter()
                .all(|warning| warning.code != "W_EMPTY_FIELD_LINEAGE"),
            "did not expect empty-lineage warning, got {:?}",
            result.warnings
        );
    }

    #[test]
    fn emits_bigquery_table_backed_unnest_column_lineage() {
        let mut opts = options();
        opts.dialect = DialectType::BigQuery;
        opts.dataset_namespace = Some("bigquery://warehouse".to_string());
        opts.output_dataset = Some(OpenLineageDatasetId::new("bigquery://warehouse", "items"));

        let result = openlineage_column_lineage(
            r#"
SELECT item.item AS item
FROM t JOIN UNNEST(t.items) AS item ON TRUE
"#,
            &opts,
        )
        .expect("lineage");
        let field = result.facet.fields.get("item").expect("item");

        assert_eq!(field.input_fields.len(), 1);
        assert_eq!(field.input_fields[0].name, "t");
        assert_eq!(field.input_fields[0].field, "items");
    }

    #[test]
    fn emits_aggregation_column_lineage() {
        let result =
            openlineage_column_lineage("SELECT SUM(amount) AS total FROM orders", &options())
                .expect("lineage");
        let field = result.facet.fields.get("total").expect("field total");
        assert_eq!(field.input_fields[0].field, "amount");
        assert_eq!(
            field.input_fields[0].transformations[0].subtype,
            "AGGREGATION"
        );
    }

    #[test]
    fn infers_insert_output_dataset() {
        let mut opts = options();
        opts.output_dataset = None;
        let result =
            openlineage_column_lineage("INSERT INTO analytics.out SELECT a FROM raw.input", &opts)
                .expect("lineage");
        assert_eq!(result.outputs[0].name, "analytics.out");
        assert_eq!(result.inputs[0].name, "raw.input");
    }

    #[test]
    fn maps_insert_target_columns_to_output_fields() {
        let mut opts = options();
        opts.output_dataset = None;
        let result = openlineage_column_lineage(
            "INSERT INTO analytics.out (target_a) SELECT source_a FROM raw.input",
            &opts,
        )
        .expect("lineage");
        let field = result.facet.fields.get("target_a").expect("target field");
        assert_eq!(field.input_fields[0].field, "source_a");
        assert!(!result.facet.fields.contains_key("source_a"));
    }

    #[test]
    fn pure_select_requires_output_dataset() {
        let mut opts = options();
        opts.output_dataset = None;
        let err = openlineage_column_lineage("SELECT a FROM t", &opts).unwrap_err();
        assert!(err.to_string().contains("outputDataset is required"));
    }

    #[test]
    fn emits_job_event_payload() {
        let result = openlineage_job_event("SELECT a FROM t", &options()).expect("event");
        assert_eq!(result.event["job"]["namespace"], "polyglot-tests");
        assert_eq!(
            result.event["job"]["facets"]["sql"]["_schemaURL"],
            SQL_JOB_FACET_SCHEMA_URL
        );
        assert_eq!(
            result.event["outputs"][0]["facets"]["columnLineage"]["fields"]["a"]["inputFields"][0]
                ["field"],
            "a"
        );
    }

    #[test]
    fn emits_run_event_payload() {
        let result = openlineage_run_event("SELECT a FROM t", &options()).expect("event");
        assert_eq!(result.event["eventType"], "COMPLETE");
        assert_eq!(
            result.event["run"]["runId"],
            "3b452093-782c-4ef2-9c0c-aafe2aa6f34d"
        );
    }

    #[test]
    fn select_star_without_schema_warns() {
        let result = openlineage_column_lineage("SELECT * FROM t", &options()).expect("lineage");
        assert!(result.facet.fields.is_empty());
        assert!(result
            .warnings
            .iter()
            .any(|w| w.code == "W_STAR_WITHOUT_SCHEMA"));
    }

    #[test]
    fn select_star_with_schema_expands_fields() {
        let mut opts = options();
        opts.schema = Some(ValidationSchema {
            strict: None,
            tables: vec![crate::validation::SchemaTable {
                name: "t".to_string(),
                schema: None,
                columns: vec![
                    crate::validation::SchemaColumn {
                        name: "a".to_string(),
                        data_type: "INT".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                    crate::validation::SchemaColumn {
                        name: "b".to_string(),
                        data_type: "TEXT".to_string(),
                        nullable: None,
                        primary_key: false,
                        unique: false,
                        references: None,
                    },
                ],
                aliases: vec![],
                primary_key: vec![],
                unique_keys: vec![],
                foreign_keys: vec![],
            }],
        });

        let result = openlineage_column_lineage("SELECT * FROM t", &opts).expect("lineage");
        assert!(result.facet.fields.contains_key("a"));
        assert!(result.facet.fields.contains_key("b"));
    }

    #[test]
    fn select_star_applies_except_replace_rename() {
        let schema: ValidationSchema = serde_json::from_value(json!({
            "tables": [{
                "name": "orders",
                "schema": "s",
                "columns": [
                    {"name": "order_id", "type": "INT"},
                    {"name": "amount", "type": "INT"},
                    {"name": "payload", "type": "TEXT"}
                ]
            }]
        }))
        .expect("schema");
        let lineage = |dialect, sql| {
            let mut opts = options();
            opts.dialect = dialect;
            opts.schema = Some(schema.clone());
            openlineage_column_lineage(sql, &opts).expect("lineage")
        };

        for (dialect, sql) in [
            (
                DialectType::Snowflake,
                "SELECT * EXCLUDE (payload) FROM s.orders",
            ),
            (
                DialectType::BigQuery,
                "SELECT * EXCEPT (payload) FROM s.orders",
            ),
            (
                DialectType::ClickHouse,
                "SELECT * EXCEPT (payload) FROM s.orders",
            ),
        ] {
            let result = lineage(dialect, sql);
            // Snowflake normalizes unquoted schema columns to upper case.
            let fields: Vec<_> = result
                .facet
                .fields
                .keys()
                .map(|f| f.to_lowercase())
                .collect();
            assert_eq!(fields, vec!["amount", "order_id"], "{sql}");
        }

        let result = lineage(
            DialectType::BigQuery,
            "SELECT * REPLACE (amount * 2 AS amount) FROM s.orders",
        );
        let inputs = &result.facet.fields["amount"].input_fields;
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "s.orders");
        assert_eq!(inputs[0].field, "amount");
        assert_eq!(inputs[0].transformations[0].subtype, "TRANSFORMATION");

        let result = lineage(
            DialectType::Snowflake,
            "SELECT * RENAME (amount AS order_amount) FROM s.orders",
        );
        assert!(!result.facet.fields.contains_key("AMOUNT"));
        let inputs = &result.facet.fields["ORDER_AMOUNT"].input_fields;
        assert_eq!(inputs.len(), 1);
        assert_eq!(inputs[0].name, "s.orders");
        assert_eq!(inputs[0].field, "AMOUNT");
        assert_eq!(inputs[0].transformations[0].subtype, "IDENTITY");

        let result = lineage(
            DialectType::Snowflake,
            r#"SELECT * RENAME (amount AS "order_amount") FROM s.orders"#,
        );
        assert!(result.facet.fields.contains_key("order_amount"));

        let result = lineage(
            DialectType::ClickHouse,
            "SELECT * EXCEPT ('^pay.*') FROM s.orders",
        );
        assert!(result.facet.fields.is_empty());
        assert!(result
            .warnings
            .iter()
            .any(|w| w.code == "W_STAR_EXCEPT_PATTERN"));
    }

    #[test]
    fn select_star_expands_cte_fields() {
        let mut opts = options();
        opts.schema = Some(
            serde_json::from_value(json!({
                "tables": [{
                    "name": "orders",
                    "schema": "s",
                    "columns": [
                        {"name": "order_id", "type": "INT"},
                        {"name": "amount", "type": "INT"}
                    ]
                }]
            }))
            .expect("schema"),
        );

        for (sql, fields) in [
            (
                "WITH c AS (SELECT * FROM s.orders) SELECT * FROM c",
                vec!["amount", "order_id"],
            ),
            (
                "WITH a AS (SELECT * FROM s.orders), c AS (SELECT * FROM a) SELECT c.* FROM c",
                vec!["amount", "order_id"],
            ),
            (
                "WITH c AS (SELECT order_id, amount FROM s.orders) SELECT * FROM c WHERE amount > 0",
                vec!["amount", "order_id"],
            ),
        ] {
            let result = openlineage_column_lineage(sql, &opts).expect("lineage");
            assert!(result.warnings.is_empty(), "{sql}: {:?}", result.warnings);
            assert_eq!(
                result.facet.fields.keys().collect::<Vec<_>>(),
                fields,
                "{sql}"
            );
            for field in fields {
                let inputs = &result.facet.fields[field].input_fields;
                assert_eq!(inputs.len(), 1, "{sql}: {field}");
                assert_eq!(inputs[0].name, "s.orders", "{sql}: {field}");
                assert_eq!(inputs[0].field, field, "{sql}: {field}");
                assert_eq!(
                    inputs[0].transformations[0].subtype, "IDENTITY",
                    "{sql}: {field}"
                );
            }
        }
    }

    #[test]
    fn select_star_expands_derived_table_fields() {
        let mut opts = options();
        opts.schema = Some(
            serde_json::from_value(json!({
                "tables": [{
                    "name": "customers",
                    "schema": "s",
                    "columns": [
                        {"name": "customer_id", "type": "INT"},
                        {"name": "created_at", "type": "TIMESTAMP"}
                    ]
                }]
            }))
            .expect("schema"),
        );

        for sql in [
            "SELECT * FROM (SELECT * FROM s.customers) sbq",
            "SELECT sbq.* FROM (SELECT * FROM s.customers) sbq",
            "SELECT *, created_at AS dbt_updated_at FROM (SELECT * FROM s.customers) sbq",
        ] {
            let result = openlineage_column_lineage(sql, &opts).expect("lineage");
            for field in ["customer_id", "created_at"] {
                let inputs = &result.facet.fields[field].input_fields;
                assert_eq!(inputs.len(), 1, "{sql}: {field}");
                assert_eq!(inputs[0].name, "s.customers", "{sql}: {field}");
                assert_eq!(inputs[0].field, field, "{sql}: {field}");
                assert_eq!(
                    inputs[0].transformations[0].subtype, "IDENTITY",
                    "{sql}: {field}"
                );
            }
        }
    }
}
