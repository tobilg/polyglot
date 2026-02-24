//! Schema-aware and semantic SQL validation.
//!
//! This module extends syntax validation with:
//! - schema checks (unknown tables/columns)
//! - optional semantic warnings (SELECT *, LIMIT without ORDER BY, etc.)

use crate::ast_transforms::get_aggregate_functions;
use crate::dialects::{Dialect, DialectType};
use crate::error::{ValidationError, ValidationResult};
use crate::expressions::{
    Column, DataType, Expression, Function, Insert, JoinKind, TableRef, Update,
};
use crate::function_registry::canonical_typed_function_name_upper;
use crate::optimizer::annotate_types;
use crate::resolver::Resolver;
use crate::schema::{MappingSchema, Schema as SqlSchema, SchemaError, SchemaResult, TABLE_PARTS};
use crate::scope::build_scope;
use crate::traversal::ExpressionWalk;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Column definition used for schema-aware validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumn {
    /// Column name.
    pub name: String,
    /// Optional column data type (currently informational).
    #[serde(default, rename = "type")]
    pub data_type: String,
    /// Whether the column allows NULL values.
    #[serde(default)]
    pub nullable: Option<bool>,
    /// Whether this column is part of a primary key.
    #[serde(default, rename = "primaryKey")]
    pub primary_key: bool,
    /// Whether this column has a uniqueness constraint.
    #[serde(default)]
    pub unique: bool,
    /// Optional column-level foreign key reference.
    #[serde(default)]
    pub references: Option<SchemaColumnReference>,
}

/// Column-level foreign key reference metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaColumnReference {
    /// Referenced table name.
    pub table: String,
    /// Referenced column name.
    pub column: String,
    /// Optional schema/namespace of referenced table.
    #[serde(default)]
    pub schema: Option<String>,
}

/// Table-level foreign key reference metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaForeignKey {
    /// Optional FK name.
    #[serde(default)]
    pub name: Option<String>,
    /// Source columns in the current table.
    pub columns: Vec<String>,
    /// Referenced target table + columns.
    pub references: SchemaTableReference,
}

/// Target of a table-level foreign key.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaTableReference {
    /// Referenced table name.
    pub table: String,
    /// Referenced target columns.
    pub columns: Vec<String>,
    /// Optional schema/namespace of referenced table.
    #[serde(default)]
    pub schema: Option<String>,
}

/// Table definition used for schema-aware validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaTable {
    /// Table name.
    pub name: String,
    /// Optional schema/namespace name.
    #[serde(default)]
    pub schema: Option<String>,
    /// Column definitions.
    pub columns: Vec<SchemaColumn>,
    /// Optional aliases that should resolve to this table.
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Optional primary key column list.
    #[serde(default, rename = "primaryKey")]
    pub primary_key: Vec<String>,
    /// Optional unique key groups.
    #[serde(default, rename = "uniqueKeys")]
    pub unique_keys: Vec<Vec<String>>,
    /// Optional table-level foreign keys.
    #[serde(default, rename = "foreignKeys")]
    pub foreign_keys: Vec<SchemaForeignKey>,
}

/// Schema payload used for schema-aware validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationSchema {
    /// Known tables.
    pub tables: Vec<SchemaTable>,
    /// Default strict mode for unknown identifiers.
    #[serde(default)]
    pub strict: Option<bool>,
}

/// Options for schema-aware validation.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SchemaValidationOptions {
    /// Enables type compatibility checks for expressions, DML assignments, and set operations.
    #[serde(default)]
    pub check_types: bool,
    /// Enables FK/reference integrity checks and query-level reference quality checks.
    #[serde(default)]
    pub check_references: bool,
    /// If true/false, overrides schema.strict.
    #[serde(default)]
    pub strict: Option<bool>,
    /// Enables semantic warnings (W001..W004).
    #[serde(default)]
    pub semantic: bool,
    /// Enables strict syntax checks (e.g. rejects trailing commas before clause boundaries).
    #[serde(default)]
    pub strict_syntax: bool,
}

/// Validation error/warning codes used by schema-aware validation.
pub mod validation_codes {
    // Existing schema and semantic checks.
    pub const E_PARSE_OR_OPTIONS: &str = "E000";
    pub const E_UNKNOWN_TABLE: &str = "E200";
    pub const E_UNKNOWN_COLUMN: &str = "E201";

    pub const W_SELECT_STAR: &str = "W001";
    pub const W_AGGREGATE_WITHOUT_GROUP_BY: &str = "W002";
    pub const W_DISTINCT_ORDER_BY: &str = "W003";
    pub const W_LIMIT_WITHOUT_ORDER_BY: &str = "W004";

    // Phase 2 (type checks): E210-E219, W210-W219.
    pub const E_TYPE_MISMATCH: &str = "E210";
    pub const E_INVALID_PREDICATE_TYPE: &str = "E211";
    pub const E_INVALID_ARITHMETIC_TYPE: &str = "E212";
    pub const E_INVALID_FUNCTION_ARGUMENT_TYPE: &str = "E213";
    pub const E_INVALID_ASSIGNMENT_TYPE: &str = "E214";
    pub const E_SETOP_TYPE_MISMATCH: &str = "E215";
    pub const E_SETOP_ARITY_MISMATCH: &str = "E216";
    pub const E_INCOMPATIBLE_COMPARISON_TYPES: &str = "E217";
    pub const E_INVALID_CAST: &str = "E218";
    pub const E_UNKNOWN_INFERRED_TYPE: &str = "E219";

    pub const W_IMPLICIT_CAST_COMPARISON: &str = "W210";
    pub const W_IMPLICIT_CAST_ARITHMETIC: &str = "W211";
    pub const W_IMPLICIT_CAST_ASSIGNMENT: &str = "W212";
    pub const W_LOSSY_CAST: &str = "W213";
    pub const W_SETOP_IMPLICIT_COERCION: &str = "W214";
    pub const W_PREDICATE_NULLABILITY: &str = "W215";
    pub const W_FUNCTION_ARGUMENT_COERCION: &str = "W216";
    pub const W_AGGREGATE_TYPE_COERCION: &str = "W217";
    pub const W_POSSIBLE_OVERFLOW: &str = "W218";
    pub const W_POSSIBLE_TRUNCATION: &str = "W219";

    // Phase 2 (reference checks): E220-E229, W220-W229.
    pub const E_INVALID_FOREIGN_KEY_REFERENCE: &str = "E220";
    pub const E_AMBIGUOUS_COLUMN_REFERENCE: &str = "E221";
    pub const E_UNRESOLVED_REFERENCE: &str = "E222";
    pub const E_CTE_COLUMN_COUNT_MISMATCH: &str = "E223";
    pub const E_MISSING_REFERENCE_TARGET: &str = "E224";

    pub const W_CARTESIAN_JOIN: &str = "W220";
    pub const W_JOIN_NOT_USING_DECLARED_REFERENCE: &str = "W221";
    pub const W_WEAK_REFERENCE_INTEGRITY: &str = "W222";
}

/// Canonical type family used by schema/type checks.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TypeFamily {
    Unknown,
    Boolean,
    Integer,
    Numeric,
    String,
    Binary,
    Date,
    Time,
    Timestamp,
    Interval,
    Json,
    Uuid,
    Array,
    Map,
    Struct,
}

impl TypeFamily {
    pub fn is_numeric(self) -> bool {
        matches!(self, TypeFamily::Integer | TypeFamily::Numeric)
    }

    pub fn is_temporal(self) -> bool {
        matches!(
            self,
            TypeFamily::Date | TypeFamily::Time | TypeFamily::Timestamp | TypeFamily::Interval
        )
    }
}

#[derive(Debug, Clone)]
struct TableSchemaEntry {
    columns: HashMap<String, TypeFamily>,
    column_order: Vec<String>,
}

fn lower(s: &str) -> String {
    s.to_lowercase()
}

fn split_type_args(data_type: &str) -> Option<(&str, &str)> {
    let open = data_type.find('(')?;
    if !data_type.ends_with(')') || open + 1 >= data_type.len() {
        return None;
    }
    let base = data_type[..open].trim();
    let inner = data_type[open + 1..data_type.len() - 1].trim();
    Some((base, inner))
}

/// Canonicalize a schema type string into a stable `TypeFamily`.
pub fn canonical_type_family(data_type: &str) -> TypeFamily {
    let trimmed = data_type
        .trim()
        .trim_matches(|c| c == '"' || c == '\'' || c == '`');
    if trimmed.is_empty() {
        return TypeFamily::Unknown;
    }

    // Normalize whitespace and lowercase for matching.
    let normalized = trimmed
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .to_lowercase();

    // Strip common wrappers first.
    if let Some((base, inner)) = split_type_args(&normalized) {
        match base {
            "nullable" | "lowcardinality" => return canonical_type_family(inner),
            "array" | "list" => return TypeFamily::Array,
            "map" => return TypeFamily::Map,
            "struct" | "row" | "record" => return TypeFamily::Struct,
            _ => {}
        }
    }

    if normalized.starts_with("array<") || normalized.starts_with("list<") {
        return TypeFamily::Array;
    }
    if normalized.starts_with("map<") {
        return TypeFamily::Map;
    }
    if normalized.starts_with("struct<")
        || normalized.starts_with("row<")
        || normalized.starts_with("record<")
        || normalized.starts_with("object<")
    {
        return TypeFamily::Struct;
    }

    if normalized.ends_with("[]") {
        return TypeFamily::Array;
    }

    // Remove parameter list if present, e.g. VARCHAR(255), DECIMAL(10,2).
    let mut base = normalized
        .split('(')
        .next()
        .unwrap_or("")
        .trim()
        .to_string();
    if base.is_empty() {
        return TypeFamily::Unknown;
    }

    base = base.strip_prefix("unsigned ").unwrap_or(&base).to_string();
    base = base.strip_suffix(" unsigned").unwrap_or(&base).to_string();

    match base.as_str() {
        "bool" | "boolean" => TypeFamily::Boolean,
        "tinyint" | "smallint" | "int2" | "int" | "integer" | "int4" | "int8" | "bigint"
        | "serial" | "smallserial" | "bigserial" | "utinyint" | "usmallint" | "uinteger"
        | "ubigint" | "uint8" | "uint16" | "uint32" | "uint64" | "int16" | "int32" | "int64" => {
            TypeFamily::Integer
        }
        "numeric" | "decimal" | "dec" | "number" | "float" | "float4" | "float8" | "real"
        | "double" | "double precision" | "bfloat16" | "float16" | "float32" | "float64" => {
            TypeFamily::Numeric
        }
        "char" | "character" | "varchar" | "character varying" | "nchar" | "nvarchar" | "text"
        | "string" | "clob" => TypeFamily::String,
        "binary" | "varbinary" | "blob" | "bytea" | "bytes" => TypeFamily::Binary,
        "date" => TypeFamily::Date,
        "time" => TypeFamily::Time,
        "timestamp"
        | "timestamptz"
        | "datetime"
        | "datetime2"
        | "smalldatetime"
        | "timestamp with time zone"
        | "timestamp without time zone" => TypeFamily::Timestamp,
        "interval" => TypeFamily::Interval,
        "json" | "jsonb" | "variant" => TypeFamily::Json,
        "uuid" | "uniqueidentifier" => TypeFamily::Uuid,
        "array" | "list" => TypeFamily::Array,
        "map" => TypeFamily::Map,
        "struct" | "row" | "record" | "object" => TypeFamily::Struct,
        _ => TypeFamily::Unknown,
    }
}

fn build_schema_map(schema: &ValidationSchema) -> HashMap<String, TableSchemaEntry> {
    let mut map = HashMap::new();

    for table in &schema.tables {
        let column_order: Vec<String> = table.columns.iter().map(|c| lower(&c.name)).collect();
        let columns: HashMap<String, TypeFamily> = table
            .columns
            .iter()
            .map(|c| (lower(&c.name), canonical_type_family(&c.data_type)))
            .collect();
        let entry = TableSchemaEntry {
            columns,
            column_order,
        };

        let simple_name = lower(&table.name);
        map.insert(simple_name, entry.clone());

        if let Some(table_schema) = &table.schema {
            map.insert(
                format!("{}.{}", lower(table_schema), lower(&table.name)),
                entry.clone(),
            );
        }

        for alias in &table.aliases {
            map.insert(lower(alias), entry.clone());
        }
    }

    map
}

fn type_family_to_data_type(family: TypeFamily) -> DataType {
    match family {
        TypeFamily::Unknown => DataType::Unknown,
        TypeFamily::Boolean => DataType::Boolean,
        TypeFamily::Integer => DataType::Int {
            length: None,
            integer_spelling: false,
        },
        TypeFamily::Numeric => DataType::Double {
            precision: None,
            scale: None,
        },
        TypeFamily::String => DataType::VarChar {
            length: None,
            parenthesized_length: false,
        },
        TypeFamily::Binary => DataType::VarBinary { length: None },
        TypeFamily::Date => DataType::Date,
        TypeFamily::Time => DataType::Time {
            precision: None,
            timezone: false,
        },
        TypeFamily::Timestamp => DataType::Timestamp {
            precision: None,
            timezone: false,
        },
        TypeFamily::Interval => DataType::Interval {
            unit: None,
            to: None,
        },
        TypeFamily::Json => DataType::Json,
        TypeFamily::Uuid => DataType::Uuid,
        TypeFamily::Array => DataType::Array {
            element_type: Box::new(DataType::Unknown),
            dimension: None,
        },
        TypeFamily::Map => DataType::Map {
            key_type: Box::new(DataType::Unknown),
            value_type: Box::new(DataType::Unknown),
        },
        TypeFamily::Struct => DataType::Struct {
            fields: Vec::new(),
            nested: false,
        },
    }
}

fn build_resolver_schema(schema: &ValidationSchema) -> MappingSchema {
    let mut mapping = MappingSchema::new();

    for table in &schema.tables {
        let columns: Vec<(String, DataType)> = table
            .columns
            .iter()
            .map(|column| {
                (
                    lower(&column.name),
                    type_family_to_data_type(canonical_type_family(&column.data_type)),
                )
            })
            .collect();

        let mut table_names = Vec::new();
        table_names.push(lower(&table.name));
        if let Some(table_schema) = &table.schema {
            table_names.push(format!("{}.{}", lower(table_schema), lower(&table.name)));
        }
        for alias in &table.aliases {
            table_names.push(lower(alias));
        }

        let mut dedup = HashSet::new();
        for table_name in table_names {
            if dedup.insert(table_name.clone()) {
                let _ = mapping.add_table(&table_name, &columns, None);
            }
        }
    }

    mapping
}

fn collect_cte_aliases(expr: &Expression) -> HashSet<String> {
    let mut aliases = HashSet::new();

    for node in expr.dfs() {
        match node {
            Expression::Select(select) => {
                if let Some(with) = &select.with {
                    for cte in &with.ctes {
                        aliases.insert(lower(&cte.alias.name));
                    }
                }
            }
            Expression::Insert(insert) => {
                if let Some(with) = &insert.with {
                    for cte in &with.ctes {
                        aliases.insert(lower(&cte.alias.name));
                    }
                }
            }
            Expression::Update(update) => {
                if let Some(with) = &update.with {
                    for cte in &with.ctes {
                        aliases.insert(lower(&cte.alias.name));
                    }
                }
            }
            Expression::Delete(delete) => {
                if let Some(with) = &delete.with {
                    for cte in &with.ctes {
                        aliases.insert(lower(&cte.alias.name));
                    }
                }
            }
            Expression::Union(union) => {
                if let Some(with) = &union.with {
                    for cte in &with.ctes {
                        aliases.insert(lower(&cte.alias.name));
                    }
                }
            }
            Expression::Intersect(intersect) => {
                if let Some(with) = &intersect.with {
                    for cte in &with.ctes {
                        aliases.insert(lower(&cte.alias.name));
                    }
                }
            }
            Expression::Except(except) => {
                if let Some(with) = &except.with {
                    for cte in &with.ctes {
                        aliases.insert(lower(&cte.alias.name));
                    }
                }
            }
            Expression::Merge(merge) => {
                if let Some(with_) = &merge.with_ {
                    if let Expression::With(with_clause) = with_.as_ref() {
                        for cte in &with_clause.ctes {
                            aliases.insert(lower(&cte.alias.name));
                        }
                    }
                }
            }
            _ => {}
        }
    }

    aliases
}

fn table_ref_candidates(table: &TableRef) -> Vec<String> {
    let name = lower(&table.name.name);
    let schema = table.schema.as_ref().map(|s| lower(&s.name));
    let catalog = table.catalog.as_ref().map(|c| lower(&c.name));

    let mut candidates = Vec::new();
    if let (Some(catalog), Some(schema)) = (&catalog, &schema) {
        candidates.push(format!("{}.{}.{}", catalog, schema, name));
    }
    if let Some(schema) = &schema {
        candidates.push(format!("{}.{}", schema, name));
    }
    candidates.push(name);
    candidates
}

fn table_ref_display_name(table: &TableRef) -> String {
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

#[derive(Debug, Default, Clone)]
struct TypeCheckContext {
    referenced_tables: HashSet<String>,
    table_aliases: HashMap<String, String>,
}

fn type_family_name(family: TypeFamily) -> &'static str {
    match family {
        TypeFamily::Unknown => "unknown",
        TypeFamily::Boolean => "boolean",
        TypeFamily::Integer => "integer",
        TypeFamily::Numeric => "numeric",
        TypeFamily::String => "string",
        TypeFamily::Binary => "binary",
        TypeFamily::Date => "date",
        TypeFamily::Time => "time",
        TypeFamily::Timestamp => "timestamp",
        TypeFamily::Interval => "interval",
        TypeFamily::Json => "json",
        TypeFamily::Uuid => "uuid",
        TypeFamily::Array => "array",
        TypeFamily::Map => "map",
        TypeFamily::Struct => "struct",
    }
}

fn is_string_like(family: TypeFamily) -> bool {
    matches!(family, TypeFamily::String)
}

fn is_string_or_binary(family: TypeFamily) -> bool {
    matches!(family, TypeFamily::String | TypeFamily::Binary)
}

fn type_issue(
    strict: bool,
    error_code: &str,
    warning_code: &str,
    message: impl Into<String>,
) -> ValidationError {
    if strict {
        ValidationError::error(message.into(), error_code)
    } else {
        ValidationError::warning(message.into(), warning_code)
    }
}

fn data_type_family(data_type: &DataType) -> TypeFamily {
    match data_type {
        DataType::Boolean => TypeFamily::Boolean,
        DataType::TinyInt { .. }
        | DataType::SmallInt { .. }
        | DataType::Int { .. }
        | DataType::BigInt { .. } => TypeFamily::Integer,
        DataType::Float { .. } | DataType::Double { .. } | DataType::Decimal { .. } => {
            TypeFamily::Numeric
        }
        DataType::Char { .. }
        | DataType::VarChar { .. }
        | DataType::String { .. }
        | DataType::Text
        | DataType::TextWithLength { .. }
        | DataType::CharacterSet { .. } => TypeFamily::String,
        DataType::Binary { .. } | DataType::VarBinary { .. } | DataType::Blob => TypeFamily::Binary,
        DataType::Date => TypeFamily::Date,
        DataType::Time { .. } => TypeFamily::Time,
        DataType::Timestamp { .. } => TypeFamily::Timestamp,
        DataType::Interval { .. } => TypeFamily::Interval,
        DataType::Json | DataType::JsonB => TypeFamily::Json,
        DataType::Uuid => TypeFamily::Uuid,
        DataType::Array { .. } | DataType::List { .. } => TypeFamily::Array,
        DataType::Map { .. } => TypeFamily::Map,
        DataType::Struct { .. } | DataType::Object { .. } | DataType::Union { .. } => {
            TypeFamily::Struct
        }
        DataType::Nullable { inner } => data_type_family(inner),
        DataType::Custom { name } => canonical_type_family(name),
        DataType::Unknown => TypeFamily::Unknown,
        DataType::Bit { .. } | DataType::VarBit { .. } => TypeFamily::Binary,
        DataType::Enum { .. } | DataType::Set { .. } => TypeFamily::String,
        DataType::Vector { .. } => TypeFamily::Array,
        DataType::Geometry { .. } | DataType::Geography { .. } => TypeFamily::Struct,
    }
}

fn collect_type_check_context(
    stmt: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
) -> TypeCheckContext {
    fn add_table_to_context(
        table: &TableRef,
        schema_map: &HashMap<String, TableSchemaEntry>,
        context: &mut TypeCheckContext,
    ) {
        let resolved_key = table_ref_candidates(table)
            .into_iter()
            .find(|k| schema_map.contains_key(k));

        let Some(table_key) = resolved_key else {
            return;
        };

        context.referenced_tables.insert(table_key.clone());
        context
            .table_aliases
            .insert(lower(&table.name.name), table_key.clone());
        if let Some(alias) = &table.alias {
            context
                .table_aliases
                .insert(lower(&alias.name), table_key.clone());
        }
    }

    let mut context = TypeCheckContext::default();
    let cte_aliases = collect_cte_aliases(stmt);

    for node in stmt.find_all(|e| matches!(e, Expression::Table(_))) {
        let Expression::Table(table) = node else {
            continue;
        };

        if cte_aliases.contains(&lower(&table.name.name)) {
            continue;
        }

        add_table_to_context(table, schema_map, &mut context);
    }

    // Seed DML target tables explicitly because they are struct fields and may
    // not appear as standalone Expression::Table nodes in traversal output.
    match stmt {
        Expression::Insert(insert) => {
            add_table_to_context(&insert.table, schema_map, &mut context);
        }
        Expression::Update(update) => {
            add_table_to_context(&update.table, schema_map, &mut context);
            for table in &update.extra_tables {
                add_table_to_context(table, schema_map, &mut context);
            }
        }
        Expression::Delete(delete) => {
            add_table_to_context(&delete.table, schema_map, &mut context);
            for table in &delete.using {
                add_table_to_context(table, schema_map, &mut context);
            }
            for table in &delete.tables {
                add_table_to_context(table, schema_map, &mut context);
            }
        }
        _ => {}
    }

    context
}

fn resolve_table_schema_entry<'a>(
    table: &TableRef,
    schema_map: &'a HashMap<String, TableSchemaEntry>,
) -> Option<(String, &'a TableSchemaEntry)> {
    let key = table_ref_candidates(table)
        .into_iter()
        .find(|k| schema_map.contains_key(k))?;
    let entry = schema_map.get(&key)?;
    Some((key, entry))
}

fn reference_issue(strict: bool, message: impl Into<String>) -> ValidationError {
    if strict {
        ValidationError::error(
            message.into(),
            validation_codes::E_INVALID_FOREIGN_KEY_REFERENCE,
        )
    } else {
        ValidationError::warning(message.into(), validation_codes::W_WEAK_REFERENCE_INTEGRITY)
    }
}

fn reference_table_candidates(
    table_name: &str,
    explicit_schema: Option<&str>,
    source_schema: Option<&str>,
) -> Vec<String> {
    let mut candidates = Vec::new();
    let raw = lower(table_name);

    if let Some(schema) = explicit_schema {
        candidates.push(format!("{}.{}", lower(schema), raw));
    }

    if raw.contains('.') {
        candidates.push(raw.clone());
        if let Some(last) = raw.rsplit('.').next() {
            candidates.push(last.to_string());
        }
    } else {
        if let Some(schema) = source_schema {
            candidates.push(format!("{}.{}", lower(schema), raw));
        }
        candidates.push(raw);
    }

    let mut dedup = HashSet::new();
    candidates
        .into_iter()
        .filter(|c| dedup.insert(c.clone()))
        .collect()
}

fn resolve_reference_table_key(
    table_name: &str,
    explicit_schema: Option<&str>,
    source_schema: Option<&str>,
    schema_map: &HashMap<String, TableSchemaEntry>,
) -> Option<String> {
    reference_table_candidates(table_name, explicit_schema, source_schema)
        .into_iter()
        .find(|candidate| schema_map.contains_key(candidate))
}

fn key_types_compatible(source: TypeFamily, target: TypeFamily) -> bool {
    if source == TypeFamily::Unknown || target == TypeFamily::Unknown {
        return true;
    }
    if source == target {
        return true;
    }
    if source.is_numeric() && target.is_numeric() {
        return true;
    }
    if source.is_temporal() && target.is_temporal() {
        return true;
    }
    false
}

fn table_key_hints(table: &SchemaTable) -> HashSet<String> {
    let mut hints = HashSet::new();
    for column in &table.columns {
        if column.primary_key || column.unique {
            hints.insert(lower(&column.name));
        }
    }
    for key_col in &table.primary_key {
        hints.insert(lower(key_col));
    }
    for group in &table.unique_keys {
        if group.len() == 1 {
            if let Some(col) = group.first() {
                hints.insert(lower(col));
            }
        }
    }
    hints
}

fn check_reference_integrity(
    schema: &ValidationSchema,
    schema_map: &HashMap<String, TableSchemaEntry>,
    strict: bool,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    let mut key_hints_lookup: HashMap<String, HashSet<String>> = HashMap::new();
    for table in &schema.tables {
        let simple = lower(&table.name);
        key_hints_lookup.insert(simple, table_key_hints(table));
        if let Some(schema_name) = &table.schema {
            let qualified = format!("{}.{}", lower(schema_name), lower(&table.name));
            key_hints_lookup.insert(qualified, table_key_hints(table));
        }
    }

    for table in &schema.tables {
        let source_table_display = if let Some(schema_name) = &table.schema {
            format!("{}.{}", schema_name, table.name)
        } else {
            table.name.clone()
        };
        let source_schema = table.schema.as_deref();
        let source_columns: HashMap<String, TypeFamily> = table
            .columns
            .iter()
            .map(|col| (lower(&col.name), canonical_type_family(&col.data_type)))
            .collect();

        for source_col in &table.columns {
            let Some(reference) = &source_col.references else {
                continue;
            };
            let source_type = canonical_type_family(&source_col.data_type);

            let Some(target_key) = resolve_reference_table_key(
                &reference.table,
                reference.schema.as_deref(),
                source_schema,
                schema_map,
            ) else {
                errors.push(reference_issue(
                    strict,
                    format!(
                        "Foreign key reference '{}.{}' points to unknown table '{}'",
                        source_table_display, source_col.name, reference.table
                    ),
                ));
                continue;
            };

            let target_column = lower(&reference.column);
            let Some(target_entry) = schema_map.get(&target_key) else {
                errors.push(reference_issue(
                    strict,
                    format!(
                        "Foreign key reference '{}.{}' points to unknown table '{}'",
                        source_table_display, source_col.name, reference.table
                    ),
                ));
                continue;
            };

            let Some(target_type) = target_entry.columns.get(&target_column).copied() else {
                errors.push(reference_issue(
                    strict,
                    format!(
                        "Foreign key reference '{}.{}' points to unknown column '{}.{}'",
                        source_table_display, source_col.name, target_key, reference.column
                    ),
                ));
                continue;
            };

            if !key_types_compatible(source_type, target_type) {
                errors.push(reference_issue(
                    strict,
                    format!(
                        "Foreign key type mismatch for '{}.{}' -> '{}.{}': {} vs {}",
                        source_table_display,
                        source_col.name,
                        target_key,
                        reference.column,
                        type_family_name(source_type),
                        type_family_name(target_type)
                    ),
                ));
            }

            if let Some(target_key_hints) = key_hints_lookup.get(&target_key) {
                if !target_key_hints.contains(&target_column) {
                    errors.push(ValidationError::warning(
                        format!(
                            "Referenced column '{}.{}' is not marked as primary/unique key",
                            target_key, reference.column
                        ),
                        validation_codes::W_WEAK_REFERENCE_INTEGRITY,
                    ));
                }
            }
        }

        for foreign_key in &table.foreign_keys {
            if foreign_key.columns.is_empty() || foreign_key.references.columns.is_empty() {
                errors.push(reference_issue(
                    strict,
                    format!(
                        "Table-level foreign key on '{}' has empty source or target column list",
                        source_table_display
                    ),
                ));
                continue;
            }
            if foreign_key.columns.len() != foreign_key.references.columns.len() {
                errors.push(reference_issue(
                    strict,
                    format!(
                        "Table-level foreign key on '{}' has {} source columns but {} target columns",
                        source_table_display,
                        foreign_key.columns.len(),
                        foreign_key.references.columns.len()
                    ),
                ));
                continue;
            }

            let Some(target_key) = resolve_reference_table_key(
                &foreign_key.references.table,
                foreign_key.references.schema.as_deref(),
                source_schema,
                schema_map,
            ) else {
                errors.push(reference_issue(
                    strict,
                    format!(
                        "Table-level foreign key on '{}' points to unknown table '{}'",
                        source_table_display, foreign_key.references.table
                    ),
                ));
                continue;
            };

            let Some(target_entry) = schema_map.get(&target_key) else {
                errors.push(reference_issue(
                    strict,
                    format!(
                        "Table-level foreign key on '{}' points to unknown table '{}'",
                        source_table_display, foreign_key.references.table
                    ),
                ));
                continue;
            };

            for (source_col, target_col) in foreign_key
                .columns
                .iter()
                .zip(foreign_key.references.columns.iter())
            {
                let source_col_name = lower(source_col);
                let target_col_name = lower(target_col);

                let Some(source_type) = source_columns.get(&source_col_name).copied() else {
                    errors.push(reference_issue(
                        strict,
                        format!(
                            "Table-level foreign key on '{}' references unknown source column '{}'",
                            source_table_display, source_col
                        ),
                    ));
                    continue;
                };

                let Some(target_type) = target_entry.columns.get(&target_col_name).copied() else {
                    errors.push(reference_issue(
                        strict,
                        format!(
                            "Table-level foreign key on '{}' references unknown target column '{}.{}'",
                            source_table_display, target_key, target_col
                        ),
                    ));
                    continue;
                };

                if !key_types_compatible(source_type, target_type) {
                    errors.push(reference_issue(
                        strict,
                        format!(
                            "Table-level foreign key type mismatch '{}.{}' -> '{}.{}': {} vs {}",
                            source_table_display,
                            source_col,
                            target_key,
                            target_col,
                            type_family_name(source_type),
                            type_family_name(target_type)
                        ),
                    ));
                }

                if let Some(target_key_hints) = key_hints_lookup.get(&target_key) {
                    if !target_key_hints.contains(&target_col_name) {
                        errors.push(ValidationError::warning(
                            format!(
                                "Referenced column '{}.{}' is not marked as primary/unique key",
                                target_key, target_col
                            ),
                            validation_codes::W_WEAK_REFERENCE_INTEGRITY,
                        ));
                    }
                }
            }
        }
    }

    errors
}

fn resolve_unqualified_column_type(
    column_name: &str,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
) -> TypeFamily {
    let candidate_tables: Vec<&String> = if !context.referenced_tables.is_empty() {
        context.referenced_tables.iter().collect()
    } else {
        schema_map.keys().collect()
    };

    let mut families = HashSet::new();
    for table_name in candidate_tables {
        if let Some(table_schema) = schema_map.get(table_name) {
            if let Some(family) = table_schema.columns.get(column_name) {
                families.insert(*family);
            }
        }
    }

    if families.len() == 1 {
        *families.iter().next().unwrap_or(&TypeFamily::Unknown)
    } else {
        TypeFamily::Unknown
    }
}

fn resolve_column_type(
    column: &Column,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
) -> TypeFamily {
    let column_name = lower(&column.name.name);
    if column_name.is_empty() {
        return TypeFamily::Unknown;
    }

    if let Some(table) = &column.table {
        let mut table_key = lower(&table.name);
        if let Some(mapped) = context.table_aliases.get(&table_key) {
            table_key = mapped.clone();
        }

        return schema_map
            .get(&table_key)
            .and_then(|t| t.columns.get(&column_name))
            .copied()
            .unwrap_or(TypeFamily::Unknown);
    }

    resolve_unqualified_column_type(&column_name, schema_map, context)
}

struct TypeInferenceSchema<'a> {
    schema_map: &'a HashMap<String, TableSchemaEntry>,
    context: &'a TypeCheckContext,
}

impl TypeInferenceSchema<'_> {
    fn resolve_table_key(&self, table: &str) -> Option<String> {
        let mut table_key = lower(table);
        if let Some(mapped) = self.context.table_aliases.get(&table_key) {
            table_key = mapped.clone();
        }
        if self.schema_map.contains_key(&table_key) {
            Some(table_key)
        } else {
            None
        }
    }
}

impl SqlSchema for TypeInferenceSchema<'_> {
    fn dialect(&self) -> Option<DialectType> {
        None
    }

    fn add_table(
        &mut self,
        _table: &str,
        _columns: &[(String, DataType)],
        _dialect: Option<DialectType>,
    ) -> SchemaResult<()> {
        Err(SchemaError::InvalidStructure(
            "Type inference schema is read-only".to_string(),
        ))
    }

    fn column_names(&self, table: &str) -> SchemaResult<Vec<String>> {
        let table_key = self
            .resolve_table_key(table)
            .ok_or_else(|| SchemaError::TableNotFound(table.to_string()))?;
        let entry = self
            .schema_map
            .get(&table_key)
            .ok_or_else(|| SchemaError::TableNotFound(table.to_string()))?;
        Ok(entry.column_order.clone())
    }

    fn get_column_type(&self, table: &str, column: &str) -> SchemaResult<DataType> {
        let col_name = lower(column);
        if table.is_empty() {
            let family = resolve_unqualified_column_type(&col_name, self.schema_map, self.context);
            return if family == TypeFamily::Unknown {
                Err(SchemaError::ColumnNotFound {
                    table: "<unqualified>".to_string(),
                    column: column.to_string(),
                })
            } else {
                Ok(type_family_to_data_type(family))
            };
        }

        let table_key = self
            .resolve_table_key(table)
            .ok_or_else(|| SchemaError::TableNotFound(table.to_string()))?;
        let entry = self
            .schema_map
            .get(&table_key)
            .ok_or_else(|| SchemaError::TableNotFound(table.to_string()))?;
        let family =
            entry
                .columns
                .get(&col_name)
                .copied()
                .ok_or_else(|| SchemaError::ColumnNotFound {
                    table: table.to_string(),
                    column: column.to_string(),
                })?;
        Ok(type_family_to_data_type(family))
    }

    fn has_column(&self, table: &str, column: &str) -> bool {
        self.get_column_type(table, column).is_ok()
    }

    fn supported_table_args(&self) -> &[&str] {
        TABLE_PARTS
    }

    fn is_empty(&self) -> bool {
        self.schema_map.is_empty()
    }

    fn depth(&self) -> usize {
        1
    }
}

fn infer_expression_type_family(
    expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
) -> TypeFamily {
    let inference_schema = TypeInferenceSchema {
        schema_map,
        context,
    };
    if let Some(data_type) = annotate_types(expr, Some(&inference_schema), None) {
        let family = data_type_family(&data_type);
        if family != TypeFamily::Unknown {
            return family;
        }
    }

    infer_expression_type_family_fallback(expr, schema_map, context)
}

fn infer_expression_type_family_fallback(
    expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
) -> TypeFamily {
    match expr {
        Expression::Literal(literal) => match literal {
            crate::expressions::Literal::Number(value) => {
                if value.contains('.') || value.contains('e') || value.contains('E') {
                    TypeFamily::Numeric
                } else {
                    TypeFamily::Integer
                }
            }
            crate::expressions::Literal::HexNumber(_) => TypeFamily::Integer,
            crate::expressions::Literal::Date(_) => TypeFamily::Date,
            crate::expressions::Literal::Time(_) => TypeFamily::Time,
            crate::expressions::Literal::Timestamp(_)
            | crate::expressions::Literal::Datetime(_) => TypeFamily::Timestamp,
            crate::expressions::Literal::HexString(_)
            | crate::expressions::Literal::BitString(_)
            | crate::expressions::Literal::ByteString(_) => TypeFamily::Binary,
            _ => TypeFamily::String,
        },
        Expression::Boolean(_) => TypeFamily::Boolean,
        Expression::Null(_) => TypeFamily::Unknown,
        Expression::Column(column) => resolve_column_type(column, schema_map, context),
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            data_type_family(&cast.to)
        }
        Expression::Alias(alias) => {
            infer_expression_type_family_fallback(&alias.this, schema_map, context)
        }
        Expression::Neg(unary) => {
            infer_expression_type_family_fallback(&unary.this, schema_map, context)
        }
        Expression::Add(op) | Expression::Sub(op) | Expression::Mul(op) => {
            let left = infer_expression_type_family_fallback(&op.left, schema_map, context);
            let right = infer_expression_type_family_fallback(&op.right, schema_map, context);
            if left == TypeFamily::Unknown || right == TypeFamily::Unknown {
                TypeFamily::Unknown
            } else if left == TypeFamily::Integer && right == TypeFamily::Integer {
                TypeFamily::Integer
            } else if left.is_numeric() && right.is_numeric() {
                TypeFamily::Numeric
            } else if left.is_temporal() || right.is_temporal() {
                left
            } else {
                TypeFamily::Unknown
            }
        }
        Expression::Div(_) | Expression::Mod(_) => TypeFamily::Numeric,
        Expression::Concat(_) => TypeFamily::String,
        Expression::Eq(_)
        | Expression::Neq(_)
        | Expression::Lt(_)
        | Expression::Lte(_)
        | Expression::Gt(_)
        | Expression::Gte(_)
        | Expression::Like(_)
        | Expression::ILike(_)
        | Expression::And(_)
        | Expression::Or(_)
        | Expression::Not(_)
        | Expression::Between(_)
        | Expression::In(_)
        | Expression::IsNull(_)
        | Expression::IsTrue(_)
        | Expression::IsFalse(_)
        | Expression::Is(_) => TypeFamily::Boolean,
        Expression::Length(_) => TypeFamily::Integer,
        Expression::Upper(_)
        | Expression::Lower(_)
        | Expression::Trim(_)
        | Expression::LTrim(_)
        | Expression::RTrim(_)
        | Expression::Replace(_)
        | Expression::Substring(_)
        | Expression::Left(_)
        | Expression::Right(_)
        | Expression::Repeat(_)
        | Expression::Lpad(_)
        | Expression::Rpad(_)
        | Expression::ConcatWs(_) => TypeFamily::String,
        Expression::Abs(_)
        | Expression::Round(_)
        | Expression::Floor(_)
        | Expression::Ceil(_)
        | Expression::Power(_)
        | Expression::Sqrt(_)
        | Expression::Cbrt(_)
        | Expression::Ln(_)
        | Expression::Log(_)
        | Expression::Exp(_) => TypeFamily::Numeric,
        Expression::DateAdd(_) | Expression::DateSub(_) | Expression::ToDate(_) => TypeFamily::Date,
        Expression::ToTimestamp(_) => TypeFamily::Timestamp,
        Expression::DateDiff(_) | Expression::Extract(_) => TypeFamily::Integer,
        Expression::CurrentDate(_) => TypeFamily::Date,
        Expression::CurrentTime(_) => TypeFamily::Time,
        Expression::CurrentTimestamp(_) | Expression::CurrentTimestampLTZ(_) => {
            TypeFamily::Timestamp
        }
        Expression::Interval(_) => TypeFamily::Interval,
        _ => TypeFamily::Unknown,
    }
}

fn are_comparable(left: TypeFamily, right: TypeFamily) -> bool {
    if left == TypeFamily::Unknown || right == TypeFamily::Unknown {
        return true;
    }
    if left == right {
        return true;
    }
    if left.is_numeric() && right.is_numeric() {
        return true;
    }
    if left.is_temporal() && right.is_temporal() {
        return true;
    }
    false
}

fn check_function_argument(
    errors: &mut Vec<ValidationError>,
    strict: bool,
    function_name: &str,
    arg_index: usize,
    family: TypeFamily,
    expected: &str,
    valid: bool,
) {
    if family == TypeFamily::Unknown || valid {
        return;
    }

    errors.push(type_issue(
        strict,
        validation_codes::E_INVALID_FUNCTION_ARGUMENT_TYPE,
        validation_codes::W_FUNCTION_ARGUMENT_COERCION,
        format!(
            "Function '{}' argument {} expects {}, found {}",
            function_name,
            arg_index + 1,
            expected,
            type_family_name(family)
        ),
    ));
}

fn function_dispatch_name(name: &str) -> String {
    let upper = name
        .rsplit('.')
        .next()
        .unwrap_or(name)
        .trim()
        .to_uppercase();
    lower(canonical_typed_function_name_upper(&upper))
}

fn check_generic_function(
    function: &Function,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
    strict: bool,
    errors: &mut Vec<ValidationError>,
) {
    let name = function_dispatch_name(&function.name);

    let arg_family = |index: usize| -> Option<TypeFamily> {
        function
            .args
            .get(index)
            .map(|arg| infer_expression_type_family(arg, schema_map, context))
    };

    match name.as_str() {
        "abs" | "sqrt" | "cbrt" | "ln" | "exp" => {
            if let Some(family) = arg_family(0) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    0,
                    family,
                    "a numeric argument",
                    family.is_numeric(),
                );
            }
        }
        "round" | "floor" | "ceil" | "ceiling" => {
            if let Some(family) = arg_family(0) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    0,
                    family,
                    "a numeric argument",
                    family.is_numeric(),
                );
            }
            if let Some(family) = arg_family(1) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    1,
                    family,
                    "a numeric argument",
                    family.is_numeric(),
                );
            }
        }
        "power" | "pow" => {
            for i in [0_usize, 1_usize] {
                if let Some(family) = arg_family(i) {
                    check_function_argument(
                        errors,
                        strict,
                        &name,
                        i,
                        family,
                        "a numeric argument",
                        family.is_numeric(),
                    );
                }
            }
        }
        "length" | "char_length" | "character_length" => {
            if let Some(family) = arg_family(0) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    0,
                    family,
                    "a string or binary argument",
                    is_string_or_binary(family),
                );
            }
        }
        "upper" | "lower" | "trim" | "ltrim" | "rtrim" | "reverse" => {
            if let Some(family) = arg_family(0) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    0,
                    family,
                    "a string argument",
                    is_string_like(family),
                );
            }
        }
        "substring" | "substr" => {
            if let Some(family) = arg_family(0) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    0,
                    family,
                    "a string argument",
                    is_string_like(family),
                );
            }
            if let Some(family) = arg_family(1) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    1,
                    family,
                    "a numeric argument",
                    family.is_numeric(),
                );
            }
            if let Some(family) = arg_family(2) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    2,
                    family,
                    "a numeric argument",
                    family.is_numeric(),
                );
            }
        }
        "replace" => {
            for i in [0_usize, 1_usize, 2_usize] {
                if let Some(family) = arg_family(i) {
                    check_function_argument(
                        errors,
                        strict,
                        &name,
                        i,
                        family,
                        "a string argument",
                        is_string_like(family),
                    );
                }
            }
        }
        "left" | "right" | "repeat" | "lpad" | "rpad" => {
            if let Some(family) = arg_family(0) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    0,
                    family,
                    "a string argument",
                    is_string_like(family),
                );
            }
            if let Some(family) = arg_family(1) {
                check_function_argument(
                    errors,
                    strict,
                    &name,
                    1,
                    family,
                    "a numeric argument",
                    family.is_numeric(),
                );
            }
            if (name == "lpad" || name == "rpad") && function.args.len() > 2 {
                if let Some(family) = arg_family(2) {
                    check_function_argument(
                        errors,
                        strict,
                        &name,
                        2,
                        family,
                        "a string argument",
                        is_string_like(family),
                    );
                }
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct DeclaredRelationship {
    source_table: String,
    source_column: String,
    target_table: String,
    target_column: String,
}

fn build_declared_relationships(
    schema: &ValidationSchema,
    schema_map: &HashMap<String, TableSchemaEntry>,
) -> Vec<DeclaredRelationship> {
    let mut relationships = HashSet::new();

    for table in &schema.tables {
        let Some(source_key) =
            resolve_reference_table_key(&table.name, table.schema.as_deref(), None, schema_map)
        else {
            continue;
        };

        for column in &table.columns {
            let Some(reference) = &column.references else {
                continue;
            };
            let Some(target_key) = resolve_reference_table_key(
                &reference.table,
                reference.schema.as_deref(),
                table.schema.as_deref(),
                schema_map,
            ) else {
                continue;
            };

            relationships.insert(DeclaredRelationship {
                source_table: source_key.clone(),
                source_column: lower(&column.name),
                target_table: target_key,
                target_column: lower(&reference.column),
            });
        }

        for foreign_key in &table.foreign_keys {
            if foreign_key.columns.len() != foreign_key.references.columns.len() {
                continue;
            }
            let Some(target_key) = resolve_reference_table_key(
                &foreign_key.references.table,
                foreign_key.references.schema.as_deref(),
                table.schema.as_deref(),
                schema_map,
            ) else {
                continue;
            };

            for (source_col, target_col) in foreign_key
                .columns
                .iter()
                .zip(foreign_key.references.columns.iter())
            {
                relationships.insert(DeclaredRelationship {
                    source_table: source_key.clone(),
                    source_column: lower(source_col),
                    target_table: target_key.clone(),
                    target_column: lower(target_col),
                });
            }
        }
    }

    relationships.into_iter().collect()
}

fn resolve_column_binding(
    column: &Column,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
    resolver: &mut Resolver<'_>,
) -> Option<(String, String)> {
    let column_name = lower(&column.name.name);
    if column_name.is_empty() {
        return None;
    }

    if let Some(table) = &column.table {
        let mut table_key = lower(&table.name);
        if let Some(mapped) = context.table_aliases.get(&table_key) {
            table_key = mapped.clone();
        }
        if schema_map.contains_key(&table_key) {
            return Some((table_key, column_name));
        }
        return None;
    }

    if let Some(resolved_source) = resolver.get_table(&column_name) {
        let mut table_key = lower(&resolved_source);
        if let Some(mapped) = context.table_aliases.get(&table_key) {
            table_key = mapped.clone();
        }
        if schema_map.contains_key(&table_key) {
            return Some((table_key, column_name));
        }
    }

    let candidates: Vec<String> = context
        .referenced_tables
        .iter()
        .filter_map(|table_name| {
            schema_map
                .get(table_name)
                .filter(|entry| entry.columns.contains_key(&column_name))
                .map(|_| table_name.clone())
        })
        .collect();
    if candidates.len() == 1 {
        return Some((candidates[0].clone(), column_name));
    }
    None
}

fn extract_join_equality_pairs(
    expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
    resolver: &mut Resolver<'_>,
    pairs: &mut Vec<((String, String), (String, String))>,
) {
    match expr {
        Expression::And(op) => {
            extract_join_equality_pairs(&op.left, schema_map, context, resolver, pairs);
            extract_join_equality_pairs(&op.right, schema_map, context, resolver, pairs);
        }
        Expression::Paren(paren) => {
            extract_join_equality_pairs(&paren.this, schema_map, context, resolver, pairs);
        }
        Expression::Eq(op) => {
            let (Expression::Column(left_col), Expression::Column(right_col)) =
                (&op.left, &op.right)
            else {
                return;
            };
            let Some(left) = resolve_column_binding(left_col, schema_map, context, resolver) else {
                return;
            };
            let Some(right) = resolve_column_binding(right_col, schema_map, context, resolver)
            else {
                return;
            };
            pairs.push((left, right));
        }
        _ => {}
    }
}

fn relationship_matches_pair(
    relationship: &DeclaredRelationship,
    left_table: &str,
    left_column: &str,
    right_table: &str,
    right_column: &str,
) -> bool {
    (relationship.source_table == left_table
        && relationship.source_column == left_column
        && relationship.target_table == right_table
        && relationship.target_column == right_column)
        || (relationship.source_table == right_table
            && relationship.source_column == right_column
            && relationship.target_table == left_table
            && relationship.target_column == left_column)
}

fn resolved_table_key_from_expr(
    expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
) -> Option<String> {
    match expr {
        Expression::Table(table) => resolve_table_schema_entry(table, schema_map).map(|(k, _)| k),
        Expression::Alias(alias) => resolved_table_key_from_expr(&alias.this, schema_map),
        _ => None,
    }
}

fn select_from_table_keys(
    select: &crate::expressions::Select,
    schema_map: &HashMap<String, TableSchemaEntry>,
) -> HashSet<String> {
    let mut keys = HashSet::new();
    if let Some(from_clause) = &select.from {
        for expr in &from_clause.expressions {
            if let Some(key) = resolved_table_key_from_expr(expr, schema_map) {
                keys.insert(key);
            }
        }
    }
    keys
}

fn is_natural_or_implied_join(kind: JoinKind) -> bool {
    matches!(
        kind,
        JoinKind::Natural
            | JoinKind::NaturalLeft
            | JoinKind::NaturalRight
            | JoinKind::NaturalFull
            | JoinKind::CrossApply
            | JoinKind::OuterApply
            | JoinKind::AsOf
            | JoinKind::AsOfLeft
            | JoinKind::AsOfRight
            | JoinKind::Lateral
            | JoinKind::LeftLateral
    )
}

fn check_query_reference_quality(
    stmt: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    resolver_schema: &MappingSchema,
    strict: bool,
    relationships: &[DeclaredRelationship],
) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    for node in stmt.dfs() {
        let Expression::Select(select) = node else {
            continue;
        };

        let select_expr = Expression::Select(select.clone());
        let context = collect_type_check_context(&select_expr, schema_map);
        let scope = build_scope(&select_expr);
        let mut resolver = Resolver::new(&scope, resolver_schema, true);

        if context.referenced_tables.len() > 1 {
            let using_columns: HashSet<String> = select
                .joins
                .iter()
                .flat_map(|join| join.using.iter().map(|id| lower(&id.name)))
                .collect();

            let mut seen = HashSet::new();
            for column_expr in select_expr
                .find_all(|e| matches!(e, Expression::Column(Column { table: None, .. })))
            {
                let Expression::Column(column) = column_expr else {
                    continue;
                };

                let col_name = lower(&column.name.name);
                if col_name.is_empty()
                    || using_columns.contains(&col_name)
                    || !seen.insert(col_name.clone())
                {
                    continue;
                }

                if resolver.is_ambiguous(&col_name) {
                    let source_count = resolver.sources_for_column(&col_name).len();
                    errors.push(if strict {
                        ValidationError::error(
                            format!(
                                "Ambiguous unqualified column '{}' found in {} referenced tables",
                                col_name, source_count
                            ),
                            validation_codes::E_AMBIGUOUS_COLUMN_REFERENCE,
                        )
                    } else {
                        ValidationError::warning(
                            format!(
                                "Ambiguous unqualified column '{}' found in {} referenced tables",
                                col_name, source_count
                            ),
                            validation_codes::W_WEAK_REFERENCE_INTEGRITY,
                        )
                    });
                }
            }
        }

        let mut cumulative_left_tables = select_from_table_keys(select, schema_map);

        for join in &select.joins {
            let right_table_key = resolved_table_key_from_expr(&join.this, schema_map);
            let has_explicit_condition = join.on.is_some() || !join.using.is_empty();
            let cartesian_like_kind = matches!(
                join.kind,
                JoinKind::Cross
                    | JoinKind::Implicit
                    | JoinKind::Array
                    | JoinKind::LeftArray
                    | JoinKind::Paste
            );

            if right_table_key.is_some()
                && (cartesian_like_kind
                    || (!has_explicit_condition && !is_natural_or_implied_join(join.kind)))
            {
                errors.push(ValidationError::warning(
                    "Potential cartesian join: JOIN without ON/USING condition",
                    validation_codes::W_CARTESIAN_JOIN,
                ));
            }

            if let (Some(on_expr), Some(right_key)) = (&join.on, right_table_key.clone()) {
                if join.using.is_empty() {
                    let mut eq_pairs = Vec::new();
                    extract_join_equality_pairs(
                        on_expr,
                        schema_map,
                        &context,
                        &mut resolver,
                        &mut eq_pairs,
                    );

                    let relevant_relationships: Vec<&DeclaredRelationship> = relationships
                        .iter()
                        .filter(|rel| {
                            cumulative_left_tables.contains(&rel.source_table)
                                && rel.target_table == right_key
                                || (cumulative_left_tables.contains(&rel.target_table)
                                    && rel.source_table == right_key)
                        })
                        .collect();

                    if !relevant_relationships.is_empty() {
                        let uses_declared_fk = eq_pairs.iter().any(|((lt, lc), (rt, rc))| {
                            relevant_relationships
                                .iter()
                                .any(|rel| relationship_matches_pair(rel, lt, lc, rt, rc))
                        });
                        if !uses_declared_fk {
                            errors.push(ValidationError::warning(
                                "JOIN predicate does not use declared foreign-key relationship columns",
                                validation_codes::W_JOIN_NOT_USING_DECLARED_REFERENCE,
                            ));
                        }
                    }
                }
            }

            if let Some(right_key) = right_table_key {
                cumulative_left_tables.insert(right_key);
            }
        }
    }

    errors
}

fn are_setop_compatible(left: TypeFamily, right: TypeFamily) -> bool {
    if left == TypeFamily::Unknown || right == TypeFamily::Unknown {
        return true;
    }
    if left == right {
        return true;
    }
    if left.is_numeric() && right.is_numeric() {
        return true;
    }
    if left.is_temporal() && right.is_temporal() {
        return true;
    }
    false
}

fn merged_setop_family(left: TypeFamily, right: TypeFamily) -> TypeFamily {
    if left == TypeFamily::Unknown {
        return right;
    }
    if right == TypeFamily::Unknown {
        return left;
    }
    if left == right {
        return left;
    }
    if left.is_numeric() && right.is_numeric() {
        if left == TypeFamily::Numeric || right == TypeFamily::Numeric {
            return TypeFamily::Numeric;
        }
        return TypeFamily::Integer;
    }
    if left.is_temporal() && right.is_temporal() {
        if left == TypeFamily::Timestamp || right == TypeFamily::Timestamp {
            return TypeFamily::Timestamp;
        }
        if left == TypeFamily::Date || right == TypeFamily::Date {
            return TypeFamily::Date;
        }
        return TypeFamily::Time;
    }
    TypeFamily::Unknown
}

fn are_assignment_compatible(target: TypeFamily, source: TypeFamily) -> bool {
    if target == TypeFamily::Unknown || source == TypeFamily::Unknown {
        return true;
    }
    if target == source {
        return true;
    }

    match target {
        TypeFamily::Boolean => source == TypeFamily::Boolean,
        TypeFamily::Integer | TypeFamily::Numeric => source.is_numeric(),
        TypeFamily::Date | TypeFamily::Time | TypeFamily::Timestamp | TypeFamily::Interval => {
            source.is_temporal()
        }
        TypeFamily::String => true,
        TypeFamily::Binary => matches!(source, TypeFamily::Binary | TypeFamily::String),
        TypeFamily::Json => matches!(source, TypeFamily::Json | TypeFamily::String),
        TypeFamily::Uuid => matches!(source, TypeFamily::Uuid | TypeFamily::String),
        TypeFamily::Array => source == TypeFamily::Array,
        TypeFamily::Map => source == TypeFamily::Map,
        TypeFamily::Struct => source == TypeFamily::Struct,
        TypeFamily::Unknown => true,
    }
}

fn projection_families(
    query_expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
) -> Option<Vec<TypeFamily>> {
    match query_expr {
        Expression::Select(select) => {
            if select
                .expressions
                .iter()
                .any(|e| matches!(e, Expression::Star(_) | Expression::BracedWildcard(_)))
            {
                return None;
            }
            let select_expr = Expression::Select(select.clone());
            let context = collect_type_check_context(&select_expr, schema_map);
            Some(
                select
                    .expressions
                    .iter()
                    .map(|e| infer_expression_type_family(e, schema_map, &context))
                    .collect(),
            )
        }
        Expression::Subquery(subquery) => projection_families(&subquery.this, schema_map),
        Expression::Union(union) => {
            let left = projection_families(&union.left, schema_map)?;
            let right = projection_families(&union.right, schema_map)?;
            if left.len() != right.len() {
                return None;
            }
            Some(
                left.into_iter()
                    .zip(right)
                    .map(|(l, r)| merged_setop_family(l, r))
                    .collect(),
            )
        }
        Expression::Intersect(intersect) => {
            let left = projection_families(&intersect.left, schema_map)?;
            let right = projection_families(&intersect.right, schema_map)?;
            if left.len() != right.len() {
                return None;
            }
            Some(
                left.into_iter()
                    .zip(right)
                    .map(|(l, r)| merged_setop_family(l, r))
                    .collect(),
            )
        }
        Expression::Except(except) => {
            let left = projection_families(&except.left, schema_map)?;
            let right = projection_families(&except.right, schema_map)?;
            if left.len() != right.len() {
                return None;
            }
            Some(
                left.into_iter()
                    .zip(right)
                    .map(|(l, r)| merged_setop_family(l, r))
                    .collect(),
            )
        }
        Expression::Values(values) => {
            let first_row = values.expressions.first()?;
            let context = TypeCheckContext::default();
            Some(
                first_row
                    .expressions
                    .iter()
                    .map(|e| infer_expression_type_family(e, schema_map, &context))
                    .collect(),
            )
        }
        _ => None,
    }
}

fn check_set_operation_compatibility(
    op_name: &str,
    left_expr: &Expression,
    right_expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    strict: bool,
    errors: &mut Vec<ValidationError>,
) {
    let Some(left_projection) = projection_families(left_expr, schema_map) else {
        return;
    };
    let Some(right_projection) = projection_families(right_expr, schema_map) else {
        return;
    };

    if left_projection.len() != right_projection.len() {
        errors.push(type_issue(
            strict,
            validation_codes::E_SETOP_ARITY_MISMATCH,
            validation_codes::W_SETOP_IMPLICIT_COERCION,
            format!(
                "{} operands return different column counts: left {}, right {}",
                op_name,
                left_projection.len(),
                right_projection.len()
            ),
        ));
        return;
    }

    for (idx, (left, right)) in left_projection
        .into_iter()
        .zip(right_projection)
        .enumerate()
    {
        if !are_setop_compatible(left, right) {
            errors.push(type_issue(
                strict,
                validation_codes::E_SETOP_TYPE_MISMATCH,
                validation_codes::W_SETOP_IMPLICIT_COERCION,
                format!(
                    "{} column {} has incompatible types: {} vs {}",
                    op_name,
                    idx + 1,
                    type_family_name(left),
                    type_family_name(right)
                ),
            ));
        }
    }
}

fn check_insert_assignments(
    stmt: &Expression,
    insert: &Insert,
    schema_map: &HashMap<String, TableSchemaEntry>,
    strict: bool,
    errors: &mut Vec<ValidationError>,
) {
    let Some((target_table_key, table_schema)) =
        resolve_table_schema_entry(&insert.table, schema_map)
    else {
        return;
    };

    let mut target_columns = Vec::new();
    if insert.columns.is_empty() {
        target_columns.extend(table_schema.column_order.iter().cloned());
    } else {
        for column in &insert.columns {
            let col_name = lower(&column.name);
            if table_schema.columns.contains_key(&col_name) {
                target_columns.push(col_name);
            } else {
                errors.push(if strict {
                    ValidationError::error(
                        format!(
                            "Unknown column '{}' in table '{}'",
                            column.name, target_table_key
                        ),
                        validation_codes::E_UNKNOWN_COLUMN,
                    )
                } else {
                    ValidationError::warning(
                        format!(
                            "Unknown column '{}' in table '{}'",
                            column.name, target_table_key
                        ),
                        validation_codes::E_UNKNOWN_COLUMN,
                    )
                });
            }
        }
    }

    if target_columns.is_empty() {
        return;
    }

    let context = collect_type_check_context(stmt, schema_map);

    if !insert.default_values {
        for (row_idx, row) in insert.values.iter().enumerate() {
            if row.len() != target_columns.len() {
                errors.push(type_issue(
                    strict,
                    validation_codes::E_INVALID_ASSIGNMENT_TYPE,
                    validation_codes::W_IMPLICIT_CAST_ASSIGNMENT,
                    format!(
                        "INSERT row {} has {} values but target has {} columns",
                        row_idx + 1,
                        row.len(),
                        target_columns.len()
                    ),
                ));
                continue;
            }

            for (value, target_column) in row.iter().zip(target_columns.iter()) {
                let Some(target_family) = table_schema.columns.get(target_column).copied() else {
                    continue;
                };
                let source_family = infer_expression_type_family(value, schema_map, &context);
                if !are_assignment_compatible(target_family, source_family) {
                    errors.push(type_issue(
                        strict,
                        validation_codes::E_INVALID_ASSIGNMENT_TYPE,
                        validation_codes::W_IMPLICIT_CAST_ASSIGNMENT,
                        format!(
                            "INSERT assignment type mismatch for '{}.{}': expected {}, found {}",
                            target_table_key,
                            target_column,
                            type_family_name(target_family),
                            type_family_name(source_family)
                        ),
                    ));
                }
            }
        }
    }

    if let Some(query) = &insert.query {
        // DuckDB BY NAME maps source columns by name, not position.
        if insert.by_name {
            return;
        }

        let Some(source_projection) = projection_families(query, schema_map) else {
            return;
        };

        if source_projection.len() != target_columns.len() {
            errors.push(type_issue(
                strict,
                validation_codes::E_INVALID_ASSIGNMENT_TYPE,
                validation_codes::W_IMPLICIT_CAST_ASSIGNMENT,
                format!(
                    "INSERT source query has {} columns but target has {} columns",
                    source_projection.len(),
                    target_columns.len()
                ),
            ));
            return;
        }

        for (source_family, target_column) in
            source_projection.into_iter().zip(target_columns.iter())
        {
            let Some(target_family) = table_schema.columns.get(target_column).copied() else {
                continue;
            };
            if !are_assignment_compatible(target_family, source_family) {
                errors.push(type_issue(
                    strict,
                    validation_codes::E_INVALID_ASSIGNMENT_TYPE,
                    validation_codes::W_IMPLICIT_CAST_ASSIGNMENT,
                    format!(
                        "INSERT assignment type mismatch for '{}.{}': expected {}, found {}",
                        target_table_key,
                        target_column,
                        type_family_name(target_family),
                        type_family_name(source_family)
                    ),
                ));
            }
        }
    }
}

fn check_update_assignments(
    stmt: &Expression,
    update: &Update,
    schema_map: &HashMap<String, TableSchemaEntry>,
    strict: bool,
    errors: &mut Vec<ValidationError>,
) {
    let Some((target_table_key, table_schema)) =
        resolve_table_schema_entry(&update.table, schema_map)
    else {
        return;
    };

    let context = collect_type_check_context(stmt, schema_map);

    for (column, value) in &update.set {
        let col_name = lower(&column.name);
        let Some(target_family) = table_schema.columns.get(&col_name).copied() else {
            errors.push(if strict {
                ValidationError::error(
                    format!(
                        "Unknown column '{}' in table '{}'",
                        column.name, target_table_key
                    ),
                    validation_codes::E_UNKNOWN_COLUMN,
                )
            } else {
                ValidationError::warning(
                    format!(
                        "Unknown column '{}' in table '{}'",
                        column.name, target_table_key
                    ),
                    validation_codes::E_UNKNOWN_COLUMN,
                )
            });
            continue;
        };

        let source_family = infer_expression_type_family(value, schema_map, &context);
        if !are_assignment_compatible(target_family, source_family) {
            errors.push(type_issue(
                strict,
                validation_codes::E_INVALID_ASSIGNMENT_TYPE,
                validation_codes::W_IMPLICIT_CAST_ASSIGNMENT,
                format!(
                    "UPDATE assignment type mismatch for '{}.{}': expected {}, found {}",
                    target_table_key,
                    col_name,
                    type_family_name(target_family),
                    type_family_name(source_family)
                ),
            ));
        }
    }
}

fn check_types(
    stmt: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    strict: bool,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    let context = collect_type_check_context(stmt, schema_map);

    for node in stmt.dfs() {
        match node {
            Expression::Insert(insert) => {
                check_insert_assignments(stmt, insert, schema_map, strict, &mut errors);
            }
            Expression::Update(update) => {
                check_update_assignments(stmt, update, schema_map, strict, &mut errors);
            }
            Expression::Union(union) => {
                check_set_operation_compatibility(
                    "UNION",
                    &union.left,
                    &union.right,
                    schema_map,
                    strict,
                    &mut errors,
                );
            }
            Expression::Intersect(intersect) => {
                check_set_operation_compatibility(
                    "INTERSECT",
                    &intersect.left,
                    &intersect.right,
                    schema_map,
                    strict,
                    &mut errors,
                );
            }
            Expression::Except(except) => {
                check_set_operation_compatibility(
                    "EXCEPT",
                    &except.left,
                    &except.right,
                    schema_map,
                    strict,
                    &mut errors,
                );
            }
            Expression::Select(select) => {
                if let Some(prewhere) = &select.prewhere {
                    let family = infer_expression_type_family(prewhere, schema_map, &context);
                    if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                        errors.push(type_issue(
                            strict,
                            validation_codes::E_INVALID_PREDICATE_TYPE,
                            validation_codes::W_PREDICATE_NULLABILITY,
                            format!(
                                "PREWHERE clause expects a boolean predicate, found {}",
                                type_family_name(family)
                            ),
                        ));
                    }
                }

                if let Some(where_clause) = &select.where_clause {
                    let family =
                        infer_expression_type_family(&where_clause.this, schema_map, &context);
                    if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                        errors.push(type_issue(
                            strict,
                            validation_codes::E_INVALID_PREDICATE_TYPE,
                            validation_codes::W_PREDICATE_NULLABILITY,
                            format!(
                                "WHERE clause expects a boolean predicate, found {}",
                                type_family_name(family)
                            ),
                        ));
                    }
                }

                if let Some(having_clause) = &select.having {
                    let family =
                        infer_expression_type_family(&having_clause.this, schema_map, &context);
                    if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                        errors.push(type_issue(
                            strict,
                            validation_codes::E_INVALID_PREDICATE_TYPE,
                            validation_codes::W_PREDICATE_NULLABILITY,
                            format!(
                                "HAVING clause expects a boolean predicate, found {}",
                                type_family_name(family)
                            ),
                        ));
                    }
                }

                for join in &select.joins {
                    if let Some(on) = &join.on {
                        let family = infer_expression_type_family(on, schema_map, &context);
                        if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                            errors.push(type_issue(
                                strict,
                                validation_codes::E_INVALID_PREDICATE_TYPE,
                                validation_codes::W_PREDICATE_NULLABILITY,
                                format!(
                                    "JOIN ON expects a boolean predicate, found {}",
                                    type_family_name(family)
                                ),
                            ));
                        }
                    }
                    if let Some(match_condition) = &join.match_condition {
                        let family =
                            infer_expression_type_family(match_condition, schema_map, &context);
                        if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                            errors.push(type_issue(
                                strict,
                                validation_codes::E_INVALID_PREDICATE_TYPE,
                                validation_codes::W_PREDICATE_NULLABILITY,
                                format!(
                                    "JOIN MATCH_CONDITION expects a boolean predicate, found {}",
                                    type_family_name(family)
                                ),
                            ));
                        }
                    }
                }
            }
            Expression::Where(where_clause) => {
                let family = infer_expression_type_family(&where_clause.this, schema_map, &context);
                if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                    errors.push(type_issue(
                        strict,
                        validation_codes::E_INVALID_PREDICATE_TYPE,
                        validation_codes::W_PREDICATE_NULLABILITY,
                        format!(
                            "WHERE clause expects a boolean predicate, found {}",
                            type_family_name(family)
                        ),
                    ));
                }
            }
            Expression::Having(having_clause) => {
                let family =
                    infer_expression_type_family(&having_clause.this, schema_map, &context);
                if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                    errors.push(type_issue(
                        strict,
                        validation_codes::E_INVALID_PREDICATE_TYPE,
                        validation_codes::W_PREDICATE_NULLABILITY,
                        format!(
                            "HAVING clause expects a boolean predicate, found {}",
                            type_family_name(family)
                        ),
                    ));
                }
            }
            Expression::And(op) | Expression::Or(op) => {
                for (side, expr) in [("left", &op.left), ("right", &op.right)] {
                    let family = infer_expression_type_family(expr, schema_map, &context);
                    if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                        errors.push(type_issue(
                            strict,
                            validation_codes::E_INVALID_PREDICATE_TYPE,
                            validation_codes::W_PREDICATE_NULLABILITY,
                            format!(
                                "Logical {} operand expects boolean, found {}",
                                side,
                                type_family_name(family)
                            ),
                        ));
                    }
                }
            }
            Expression::Not(unary) => {
                let family = infer_expression_type_family(&unary.this, schema_map, &context);
                if family != TypeFamily::Unknown && family != TypeFamily::Boolean {
                    errors.push(type_issue(
                        strict,
                        validation_codes::E_INVALID_PREDICATE_TYPE,
                        validation_codes::W_PREDICATE_NULLABILITY,
                        format!("NOT expects boolean, found {}", type_family_name(family)),
                    ));
                }
            }
            Expression::Eq(op)
            | Expression::Neq(op)
            | Expression::Lt(op)
            | Expression::Lte(op)
            | Expression::Gt(op)
            | Expression::Gte(op) => {
                let left = infer_expression_type_family(&op.left, schema_map, &context);
                let right = infer_expression_type_family(&op.right, schema_map, &context);
                if !are_comparable(left, right) {
                    errors.push(type_issue(
                        strict,
                        validation_codes::E_INCOMPATIBLE_COMPARISON_TYPES,
                        validation_codes::W_IMPLICIT_CAST_COMPARISON,
                        format!(
                            "Incompatible comparison between {} and {}",
                            type_family_name(left),
                            type_family_name(right)
                        ),
                    ));
                }
            }
            Expression::Like(op) | Expression::ILike(op) => {
                let left = infer_expression_type_family(&op.left, schema_map, &context);
                let right = infer_expression_type_family(&op.right, schema_map, &context);
                if left != TypeFamily::Unknown
                    && right != TypeFamily::Unknown
                    && (!is_string_like(left) || !is_string_like(right))
                {
                    errors.push(type_issue(
                        strict,
                        validation_codes::E_INCOMPATIBLE_COMPARISON_TYPES,
                        validation_codes::W_IMPLICIT_CAST_COMPARISON,
                        format!(
                            "LIKE/ILIKE expects string operands, found {} and {}",
                            type_family_name(left),
                            type_family_name(right)
                        ),
                    ));
                }
            }
            Expression::Between(between) => {
                let this_family = infer_expression_type_family(&between.this, schema_map, &context);
                let low_family = infer_expression_type_family(&between.low, schema_map, &context);
                let high_family = infer_expression_type_family(&between.high, schema_map, &context);

                if !are_comparable(this_family, low_family)
                    || !are_comparable(this_family, high_family)
                {
                    errors.push(type_issue(
                        strict,
                        validation_codes::E_INCOMPATIBLE_COMPARISON_TYPES,
                        validation_codes::W_IMPLICIT_CAST_COMPARISON,
                        format!(
                            "BETWEEN bounds are incompatible with {} (found {} and {})",
                            type_family_name(this_family),
                            type_family_name(low_family),
                            type_family_name(high_family)
                        ),
                    ));
                }
            }
            Expression::In(in_expr) => {
                let this_family = infer_expression_type_family(&in_expr.this, schema_map, &context);
                for value in &in_expr.expressions {
                    let item_family = infer_expression_type_family(value, schema_map, &context);
                    if !are_comparable(this_family, item_family) {
                        errors.push(type_issue(
                            strict,
                            validation_codes::E_INCOMPATIBLE_COMPARISON_TYPES,
                            validation_codes::W_IMPLICIT_CAST_COMPARISON,
                            format!(
                                "IN item type {} is incompatible with {}",
                                type_family_name(item_family),
                                type_family_name(this_family)
                            ),
                        ));
                        break;
                    }
                }
            }
            Expression::Add(op)
            | Expression::Sub(op)
            | Expression::Mul(op)
            | Expression::Div(op)
            | Expression::Mod(op) => {
                let left = infer_expression_type_family(&op.left, schema_map, &context);
                let right = infer_expression_type_family(&op.right, schema_map, &context);

                if left == TypeFamily::Unknown || right == TypeFamily::Unknown {
                    continue;
                }

                let temporal_ok = matches!(node, Expression::Add(_) | Expression::Sub(_))
                    && ((left.is_temporal() && right.is_numeric())
                        || (right.is_temporal() && left.is_numeric())
                        || (matches!(node, Expression::Sub(_))
                            && left.is_temporal()
                            && right.is_temporal()));

                if !(left.is_numeric() && right.is_numeric()) && !temporal_ok {
                    errors.push(type_issue(
                        strict,
                        validation_codes::E_INVALID_ARITHMETIC_TYPE,
                        validation_codes::W_IMPLICIT_CAST_ARITHMETIC,
                        format!(
                            "Arithmetic operation expects numeric-compatible operands, found {} and {}",
                            type_family_name(left),
                            type_family_name(right)
                        ),
                    ));
                }
            }
            Expression::Function(function) => {
                check_generic_function(function, schema_map, &context, strict, &mut errors);
            }
            Expression::Upper(func)
            | Expression::Lower(func)
            | Expression::LTrim(func)
            | Expression::RTrim(func)
            | Expression::Reverse(func) => {
                let family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "string_function",
                    0,
                    family,
                    "a string argument",
                    is_string_like(family),
                );
            }
            Expression::Length(func) => {
                let family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "length",
                    0,
                    family,
                    "a string or binary argument",
                    is_string_or_binary(family),
                );
            }
            Expression::Trim(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "trim",
                    0,
                    this_family,
                    "a string argument",
                    is_string_like(this_family),
                );
                if let Some(chars) = &func.characters {
                    let chars_family = infer_expression_type_family(chars, schema_map, &context);
                    check_function_argument(
                        &mut errors,
                        strict,
                        "trim",
                        1,
                        chars_family,
                        "a string argument",
                        is_string_like(chars_family),
                    );
                }
            }
            Expression::Substring(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "substring",
                    0,
                    this_family,
                    "a string argument",
                    is_string_like(this_family),
                );

                let start_family = infer_expression_type_family(&func.start, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "substring",
                    1,
                    start_family,
                    "a numeric argument",
                    start_family.is_numeric(),
                );
                if let Some(length) = &func.length {
                    let length_family = infer_expression_type_family(length, schema_map, &context);
                    check_function_argument(
                        &mut errors,
                        strict,
                        "substring",
                        2,
                        length_family,
                        "a numeric argument",
                        length_family.is_numeric(),
                    );
                }
            }
            Expression::Replace(func) => {
                for (arg, idx) in [
                    (&func.this, 0_usize),
                    (&func.old, 1_usize),
                    (&func.new, 2_usize),
                ] {
                    let family = infer_expression_type_family(arg, schema_map, &context);
                    check_function_argument(
                        &mut errors,
                        strict,
                        "replace",
                        idx,
                        family,
                        "a string argument",
                        is_string_like(family),
                    );
                }
            }
            Expression::Left(func) | Expression::Right(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "left_right",
                    0,
                    this_family,
                    "a string argument",
                    is_string_like(this_family),
                );
                let length_family =
                    infer_expression_type_family(&func.length, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "left_right",
                    1,
                    length_family,
                    "a numeric argument",
                    length_family.is_numeric(),
                );
            }
            Expression::Repeat(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "repeat",
                    0,
                    this_family,
                    "a string argument",
                    is_string_like(this_family),
                );
                let times_family = infer_expression_type_family(&func.times, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "repeat",
                    1,
                    times_family,
                    "a numeric argument",
                    times_family.is_numeric(),
                );
            }
            Expression::Lpad(func) | Expression::Rpad(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "pad",
                    0,
                    this_family,
                    "a string argument",
                    is_string_like(this_family),
                );
                let length_family =
                    infer_expression_type_family(&func.length, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "pad",
                    1,
                    length_family,
                    "a numeric argument",
                    length_family.is_numeric(),
                );
                if let Some(fill) = &func.fill {
                    let fill_family = infer_expression_type_family(fill, schema_map, &context);
                    check_function_argument(
                        &mut errors,
                        strict,
                        "pad",
                        2,
                        fill_family,
                        "a string argument",
                        is_string_like(fill_family),
                    );
                }
            }
            Expression::Abs(func)
            | Expression::Sqrt(func)
            | Expression::Cbrt(func)
            | Expression::Ln(func)
            | Expression::Exp(func) => {
                let family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "numeric_function",
                    0,
                    family,
                    "a numeric argument",
                    family.is_numeric(),
                );
            }
            Expression::Round(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "round",
                    0,
                    this_family,
                    "a numeric argument",
                    this_family.is_numeric(),
                );
                if let Some(decimals) = &func.decimals {
                    let decimals_family =
                        infer_expression_type_family(decimals, schema_map, &context);
                    check_function_argument(
                        &mut errors,
                        strict,
                        "round",
                        1,
                        decimals_family,
                        "a numeric argument",
                        decimals_family.is_numeric(),
                    );
                }
            }
            Expression::Floor(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "floor",
                    0,
                    this_family,
                    "a numeric argument",
                    this_family.is_numeric(),
                );
                if let Some(scale) = &func.scale {
                    let scale_family = infer_expression_type_family(scale, schema_map, &context);
                    check_function_argument(
                        &mut errors,
                        strict,
                        "floor",
                        1,
                        scale_family,
                        "a numeric argument",
                        scale_family.is_numeric(),
                    );
                }
            }
            Expression::Ceil(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "ceil",
                    0,
                    this_family,
                    "a numeric argument",
                    this_family.is_numeric(),
                );
                if let Some(decimals) = &func.decimals {
                    let decimals_family =
                        infer_expression_type_family(decimals, schema_map, &context);
                    check_function_argument(
                        &mut errors,
                        strict,
                        "ceil",
                        1,
                        decimals_family,
                        "a numeric argument",
                        decimals_family.is_numeric(),
                    );
                }
            }
            Expression::Power(func) => {
                let left_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "power",
                    0,
                    left_family,
                    "a numeric argument",
                    left_family.is_numeric(),
                );
                let right_family =
                    infer_expression_type_family(&func.expression, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "power",
                    1,
                    right_family,
                    "a numeric argument",
                    right_family.is_numeric(),
                );
            }
            Expression::Log(func) => {
                let this_family = infer_expression_type_family(&func.this, schema_map, &context);
                check_function_argument(
                    &mut errors,
                    strict,
                    "log",
                    0,
                    this_family,
                    "a numeric argument",
                    this_family.is_numeric(),
                );
                if let Some(base) = &func.base {
                    let base_family = infer_expression_type_family(base, schema_map, &context);
                    check_function_argument(
                        &mut errors,
                        strict,
                        "log",
                        1,
                        base_family,
                        "a numeric argument",
                        base_family.is_numeric(),
                    );
                }
            }
            _ => {}
        }
    }

    errors
}

fn check_semantics(stmt: &Expression) -> Vec<ValidationError> {
    let mut errors = Vec::new();

    let Expression::Select(select) = stmt else {
        return errors;
    };
    let select_expr = Expression::Select(select.clone());

    // W001: SELECT * is discouraged
    if !select_expr
        .find_all(|e| matches!(e, Expression::Star(_)))
        .is_empty()
    {
        errors.push(ValidationError::warning(
            "SELECT * is discouraged; specify columns explicitly for better performance and maintainability",
            validation_codes::W_SELECT_STAR,
        ));
    }

    // W002: aggregate + non-aggregate columns without GROUP BY
    let aggregate_count = get_aggregate_functions(&select_expr).len();
    if aggregate_count > 0 && select.group_by.is_none() {
        let has_non_aggregate_column = select.expressions.iter().any(|expr| {
            matches!(expr, Expression::Column(_) | Expression::Identifier(_))
                && get_aggregate_functions(expr).is_empty()
        });

        if has_non_aggregate_column {
            errors.push(ValidationError::warning(
                "Mixing aggregate functions with non-aggregated columns without GROUP BY may cause errors in strict SQL mode",
                validation_codes::W_AGGREGATE_WITHOUT_GROUP_BY,
            ));
        }
    }

    // W003: DISTINCT with ORDER BY
    if select.distinct && select.order_by.is_some() {
        errors.push(ValidationError::warning(
            "DISTINCT with ORDER BY: ensure ORDER BY columns are in SELECT list",
            validation_codes::W_DISTINCT_ORDER_BY,
        ));
    }

    // W004: LIMIT without ORDER BY
    if select.limit.is_some() && select.order_by.is_none() {
        errors.push(ValidationError::warning(
            "LIMIT without ORDER BY produces non-deterministic results",
            validation_codes::W_LIMIT_WITHOUT_ORDER_BY,
        ));
    }

    errors
}

fn validate_statement_with_schema(
    stmt: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    strict: bool,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    let cte_aliases = collect_cte_aliases(stmt);

    let mut referenced_tables: HashSet<String> = HashSet::new();
    let mut table_aliases: HashMap<String, String> = HashMap::new();
    let mut seen_tables: HashSet<String> = HashSet::new();

    // Table validation (E200)
    for node in stmt.find_all(|e| matches!(e, Expression::Table(_))) {
        let Expression::Table(table) = node else {
            continue;
        };

        if cte_aliases.contains(&lower(&table.name.name)) {
            continue;
        }

        let resolved_key = table_ref_candidates(table)
            .into_iter()
            .find(|k| schema_map.contains_key(k));
        let table_key = resolved_key
            .clone()
            .unwrap_or_else(|| lower(&table_ref_display_name(table)));

        // Keep alias map for all seen table refs, including unknown ones.
        if let Some(alias) = &table.alias {
            table_aliases.insert(lower(&alias.name), table_key.clone());
        }

        if !seen_tables.insert(table_key.clone()) {
            continue;
        }
        referenced_tables.insert(table_key.clone());

        if resolved_key.is_none() {
            let err = if strict {
                ValidationError::error(
                    format!("Unknown table '{}'", table_ref_display_name(table)),
                    validation_codes::E_UNKNOWN_TABLE,
                )
            } else {
                ValidationError::warning(
                    format!("Unknown table '{}'", table_ref_display_name(table)),
                    validation_codes::E_UNKNOWN_TABLE,
                )
            };
            errors.push(err);
        }
    }

    // Column validation (E201)
    for node in stmt.find_all(|e| matches!(e, Expression::Column(_))) {
        let Expression::Column(column) = node else {
            continue;
        };

        let col_name = lower(&column.name.name);
        if col_name.is_empty() {
            continue;
        }

        let mut table_name = column.table.as_ref().map(|t| lower(&t.name));
        if let Some(name) = &table_name {
            if let Some(mapped) = table_aliases.get(name) {
                table_name = Some(mapped.clone());
            }
        }

        if let Some(table_name) = table_name {
            if let Some(table_schema) = schema_map.get(&table_name) {
                if !table_schema.columns.contains_key(&col_name) {
                    let err = if strict {
                        ValidationError::error(
                            format!("Unknown column '{}' in table '{}'", col_name, table_name),
                            validation_codes::E_UNKNOWN_COLUMN,
                        )
                    } else {
                        ValidationError::warning(
                            format!("Unknown column '{}' in table '{}'", col_name, table_name),
                            validation_codes::E_UNKNOWN_COLUMN,
                        )
                    };
                    errors.push(err);
                }
            }
            continue;
        }

        if referenced_tables.len() == 1 {
            if let Some(single_table) = referenced_tables.iter().next() {
                if let Some(table_schema) = schema_map.get(single_table) {
                    if !table_schema.columns.contains_key(&col_name) {
                        let err = if strict {
                            ValidationError::error(
                                format!(
                                    "Unknown column '{}' in table '{}'",
                                    col_name, single_table
                                ),
                                validation_codes::E_UNKNOWN_COLUMN,
                            )
                        } else {
                            ValidationError::warning(
                                format!(
                                    "Unknown column '{}' in table '{}'",
                                    col_name, single_table
                                ),
                                validation_codes::E_UNKNOWN_COLUMN,
                            )
                        };
                        errors.push(err);
                    }
                }
            }
        } else if referenced_tables.len() > 1 {
            let found = referenced_tables.iter().any(|table_name| {
                schema_map
                    .get(table_name)
                    .map(|s| s.columns.contains_key(&col_name))
                    .unwrap_or(false)
            });

            if !found {
                let err = if strict {
                    ValidationError::error(
                        format!(
                            "Unknown column '{}' (not found in any referenced table)",
                            col_name
                        ),
                        validation_codes::E_UNKNOWN_COLUMN,
                    )
                } else {
                    ValidationError::warning(
                        format!(
                            "Unknown column '{}' (not found in any referenced table)",
                            col_name
                        ),
                        validation_codes::E_UNKNOWN_COLUMN,
                    )
                };
                errors.push(err);
            }
        } else if !schema_map.is_empty() {
            let found = schema_map
                .values()
                .any(|table| table.columns.contains_key(&col_name));
            if !found {
                let err = if strict {
                    ValidationError::error(
                        format!("Unknown column '{}'", col_name),
                        validation_codes::E_UNKNOWN_COLUMN,
                    )
                } else {
                    ValidationError::warning(
                        format!("Unknown column '{}'", col_name),
                        validation_codes::E_UNKNOWN_COLUMN,
                    )
                };
                errors.push(err);
            }
        }
    }

    errors
}

/// Validate SQL using syntax + schema-aware checks, with optional semantic warnings.
pub fn validate_with_schema(
    sql: &str,
    dialect: DialectType,
    schema: &ValidationSchema,
    options: &SchemaValidationOptions,
) -> ValidationResult {
    let strict = options.strict.unwrap_or(schema.strict.unwrap_or(true));

    // Syntax validation first.
    let syntax_result = crate::validate_with_options(
        sql,
        dialect,
        &crate::ValidationOptions {
            strict_syntax: options.strict_syntax,
        },
    );
    if !syntax_result.valid {
        return syntax_result;
    }

    let d = Dialect::get(dialect);
    let statements = match d.parse(sql) {
        Ok(exprs) => exprs,
        Err(e) => {
            return ValidationResult::with_errors(vec![ValidationError::error(
                e.to_string(),
                validation_codes::E_PARSE_OR_OPTIONS,
            )]);
        }
    };

    let schema_map = build_schema_map(schema);
    let resolver_schema = build_resolver_schema(schema);
    let mut all_errors = syntax_result.errors;
    let declared_relationships = if options.check_references {
        build_declared_relationships(schema, &schema_map)
    } else {
        Vec::new()
    };

    if options.check_references {
        all_errors.extend(check_reference_integrity(schema, &schema_map, strict));
    }

    for stmt in &statements {
        if options.semantic {
            all_errors.extend(check_semantics(stmt));
        }
        all_errors.extend(validate_statement_with_schema(stmt, &schema_map, strict));
        if options.check_types {
            all_errors.extend(check_types(stmt, &schema_map, strict));
        }
        if options.check_references {
            all_errors.extend(check_query_reference_quality(
                stmt,
                &schema_map,
                &resolver_schema,
                strict,
                &declared_relationships,
            ));
        }
    }

    ValidationResult::with_errors(all_errors)
}

#[cfg(test)]
mod tests;
