//! Schema-aware and semantic SQL validation.
//!
//! This module extends syntax validation with:
//! - schema checks (unknown tables/columns)
//! - optional semantic warnings (SELECT *, LIMIT without ORDER BY, etc.)

use crate::dialects::{Dialect, DialectType};
use crate::error::{ValidationError, ValidationResult};
use crate::expressions::{
    Column, DataType, Expression, Function, Insert, JoinKind, OracleDataType, TableRef, Update,
};
use crate::function_catalog::FunctionCatalog;
#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
use crate::function_catalog::{
    FunctionNameCase as CoreFunctionNameCase, FunctionSignature as CoreFunctionSignature,
    HashMapFunctionCatalog,
};
use crate::function_registry::canonical_typed_function_name_upper;
use crate::optimizer::annotate_types::annotate_types;
use crate::optimizer::normalize_identifiers::{get_normalization_strategy, normalize_identifier};
use crate::optimizer::qualify_columns::normalize_dotted_columns_in_scope;
use crate::resolver::Resolver;
use crate::schema::{MappingSchema, Schema as SqlSchema};
use crate::scope::{build_scope, walk_in_scope};
use crate::traversal::ExpressionWalk;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
use std::sync::LazyLock;

/// Column definition used for schema-aware validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SchemaColumn {
    /// Column name.
    pub name: String,
    /// Optional column data type used by type inference and validation.
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct ValidationSchema {
    /// Known tables.
    pub tables: Vec<SchemaTable>,
    /// Default strict mode for unknown identifiers.
    #[serde(default)]
    pub strict: Option<bool>,
}

/// Options for schema-aware validation.
/// JSON accepts snake_case names and camelCase aliases for compound names;
/// unrecognized options are rejected to avoid silently disabling checks.
#[derive(Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct SchemaValidationOptions {
    /// Per-call parser limits, shared by syntax and schema validation.
    #[serde(default, alias = "complexityGuard")]
    pub complexity_guard: Option<crate::ComplexityGuardOptions>,
    /// Enables type compatibility checks for expressions, DML assignments, and set operations.
    #[serde(default, alias = "checkTypes")]
    pub check_types: bool,
    /// Enables FK/reference integrity checks and query-level reference quality checks.
    #[serde(default, alias = "checkReferences")]
    pub check_references: bool,
    /// If true/false, overrides schema.strict.
    #[serde(default)]
    pub strict: Option<bool>,
    /// Enables semantic correctness errors (E230..E232) and quality warnings (W001..W004).
    #[serde(default)]
    pub semantic: bool,
    /// Enables strict syntax checks (e.g. rejects trailing commas before clause boundaries).
    #[serde(default, alias = "strictSyntax")]
    pub strict_syntax: bool,
    /// Optional external function catalog plugin for dialect-specific function validation.
    #[serde(skip, default)]
    pub function_catalog: Option<Arc<dyn FunctionCatalog>>,
}

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
fn to_core_name_case(
    case: polyglot_sql_function_catalogs::FunctionNameCase,
) -> CoreFunctionNameCase {
    match case {
        polyglot_sql_function_catalogs::FunctionNameCase::Insensitive => {
            CoreFunctionNameCase::Insensitive
        }
        polyglot_sql_function_catalogs::FunctionNameCase::Sensitive => {
            CoreFunctionNameCase::Sensitive
        }
    }
}

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
fn to_core_signatures(
    signatures: Vec<polyglot_sql_function_catalogs::FunctionSignature>,
) -> Vec<CoreFunctionSignature> {
    signatures
        .into_iter()
        .map(|signature| CoreFunctionSignature {
            min_arity: signature.min_arity,
            max_arity: signature.max_arity,
        })
        .collect()
}

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
struct EmbeddedCatalogSink<'a> {
    catalog: &'a mut HashMapFunctionCatalog,
    dialect_cache: HashMap<&'static str, Option<DialectType>>,
}

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
impl<'a> EmbeddedCatalogSink<'a> {
    fn resolve_dialect(&mut self, dialect: &'static str) -> Option<DialectType> {
        if let Some(cached) = self.dialect_cache.get(dialect) {
            return *cached;
        }
        let parsed = dialect.parse::<DialectType>().ok();
        self.dialect_cache.insert(dialect, parsed);
        parsed
    }
}

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
impl<'a> polyglot_sql_function_catalogs::CatalogSink for EmbeddedCatalogSink<'a> {
    fn set_dialect_name_case(
        &mut self,
        dialect: &'static str,
        name_case: polyglot_sql_function_catalogs::FunctionNameCase,
    ) {
        if let Some(core_dialect) = self.resolve_dialect(dialect) {
            self.catalog
                .set_dialect_name_case(core_dialect, to_core_name_case(name_case));
        }
    }

    fn set_function_name_case(
        &mut self,
        dialect: &'static str,
        function_name: &str,
        name_case: polyglot_sql_function_catalogs::FunctionNameCase,
    ) {
        if let Some(core_dialect) = self.resolve_dialect(dialect) {
            self.catalog.set_function_name_case(
                core_dialect,
                function_name,
                to_core_name_case(name_case),
            );
        }
    }

    fn register(
        &mut self,
        dialect: &'static str,
        function_name: &str,
        signatures: Vec<polyglot_sql_function_catalogs::FunctionSignature>,
    ) {
        if let Some(core_dialect) = self.resolve_dialect(dialect) {
            self.catalog
                .register(core_dialect, function_name, to_core_signatures(signatures));
        }
    }
}

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
fn embedded_function_catalog_arc() -> Arc<dyn FunctionCatalog> {
    static EMBEDDED: LazyLock<Arc<HashMapFunctionCatalog>> = LazyLock::new(|| {
        let mut catalog = HashMapFunctionCatalog::default();
        let mut sink = EmbeddedCatalogSink {
            catalog: &mut catalog,
            dialect_cache: HashMap::new(),
        };
        polyglot_sql_function_catalogs::register_enabled_catalogs(&mut sink);
        Arc::new(catalog)
    });

    EMBEDDED.clone()
}

#[cfg(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
))]
fn default_embedded_function_catalog() -> Option<Arc<dyn FunctionCatalog>> {
    Some(embedded_function_catalog_arc())
}

#[cfg(not(any(
    feature = "function-catalog-clickhouse",
    feature = "function-catalog-duckdb",
    feature = "function-catalog-all-dialects"
)))]
fn default_embedded_function_catalog() -> Option<Arc<dyn FunctionCatalog>> {
    None
}

/// Validation error/warning codes used by schema-aware validation.
pub mod validation_codes {
    // Existing schema and semantic checks.
    pub const E_PARSE_OR_OPTIONS: &str = "E000";
    pub const E_UNKNOWN_TABLE: &str = "E200";
    pub const E_UNKNOWN_COLUMN: &str = "E201";
    pub const E_UNKNOWN_FUNCTION: &str = "E202";
    pub const E_INVALID_FUNCTION_ARITY: &str = "E203";
    pub const E_INVALID_GROUPING: &str = "E230";
    pub const E_INVALID_AGGREGATE: &str = "E231";
    pub const E_INVALID_WINDOW: &str = "E232";

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
        | "ubigint" | "uint8" | "uint16" | "uint32" | "uint64" | "int16" | "int32" | "int64"
        | "hugeint" | "int128" | "largeint" | "uhugeint" | "uint128" => TypeFamily::Integer,
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

/// Build a `MappingSchema` from a `ValidationSchema` payload.
///
/// This is useful for APIs that already accept `ValidationSchema`-shaped input
/// (for example JSON wrappers) and need to run schema-aware lineage or other
/// resolver-based analysis.
pub fn mapping_schema_from_validation_schema(schema: &ValidationSchema) -> MappingSchema {
    build_resolver_schema(schema)
}

/// Build a dialect-aware `MappingSchema` while preserving nested types such
/// as ARRAY, MAP, and STRUCT. If a type spelling is not understood by the
/// selected dialect, this falls back to the validation resolver's broad type
/// family so existing best-effort behavior is retained.
pub fn mapping_schema_from_validation_schema_with_dialect(
    schema: &ValidationSchema,
    dialect: DialectType,
) -> MappingSchema {
    let broad_schema = build_resolver_schema(schema);
    let dialect_impl = Dialect::get(dialect);
    let mut mapping = MappingSchema::with_dialect(dialect);

    for table in &schema.tables {
        let fallback_table = lower(&table.name);
        let columns: Vec<(String, DataType)> = table
            .columns
            .iter()
            .map(|column| {
                let data_type = dialect_impl
                    .parse_data_type(column.data_type.trim())
                    .unwrap_or_else(|_| {
                        broad_schema
                            .get_column_type(&fallback_table, &column.name)
                            .unwrap_or(DataType::Unknown)
                    });
                (column.name.clone(), data_type)
            })
            .collect();

        let mut table_names = vec![table.name.clone()];
        if let Some(table_schema) = &table.schema {
            table_names.push(format!("{}.{}", table_schema, table.name));
        }
        table_names.extend(table.aliases.iter().cloned());

        let mut seen = HashSet::new();
        for table_name in table_names {
            if seen.insert(lower(&table_name)) {
                let _ = mapping.add_table(&table_name, &columns, Some(dialect));
            }
        }
    }

    mapping
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
        DataType::Vertica { vertica_type } => match vertica_type.as_ref() {
            crate::expressions::VerticaDataType::Array { .. }
            | crate::expressions::VerticaDataType::Set { .. } => TypeFamily::Array,
            crate::expressions::VerticaDataType::Row { .. } => TypeFamily::Struct,
            crate::expressions::VerticaDataType::LongVarBinary { .. } => TypeFamily::Binary,
            crate::expressions::VerticaDataType::Interval { .. } => TypeFamily::Interval,
        },
        DataType::Boolean => TypeFamily::Boolean,
        DataType::TinyInt { .. }
        | DataType::SmallInt { .. }
        | DataType::Int { .. }
        | DataType::BigInt { .. }
        | DataType::Int128
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64
        | DataType::UInt128 => TypeFamily::Integer,
        DataType::Float { .. } | DataType::Double { .. } | DataType::Decimal { .. } => {
            TypeFamily::Numeric
        }
        DataType::Hana { hana_type } => match hana_type.name.as_str() {
            "TINYINT" | "SMALLINT" | "INT" | "BIGINT" => TypeFamily::Integer,
            "TIMESTAMP" | "SECONDDATE" => TypeFamily::Timestamp,
            "TIME" => TypeFamily::Time,
            "FLOAT" | "DECIMAL" | "SMALLDECIMAL" => TypeFamily::Numeric,
            "BINTEXT" => TypeFamily::Binary,
            "ALPHANUM" | "VARCHAR" | "NVARCHAR" | "CLOB" | "NCLOB" | "TEXT" | "SHORTTEXT" => {
                TypeFamily::String
            }
            _ => TypeFamily::Unknown,
        },
        DataType::Oracle { oracle_type } => match oracle_type {
            OracleDataType::Number { .. }
            | OracleDataType::BinaryFloat
            | OracleDataType::BinaryDouble
            | OracleDataType::Float { .. } => TypeFamily::Numeric,
            OracleDataType::Character { .. }
            | OracleDataType::Clob { .. }
            | OracleDataType::Long { raw: false }
            | OracleDataType::RowId => TypeFamily::String,
            OracleDataType::Blob
            | OracleDataType::Raw { .. }
            | OracleDataType::Long { raw: true } => TypeFamily::Binary,
            OracleDataType::Date => TypeFamily::Date,
            OracleDataType::Timestamp { .. } => TypeFamily::Timestamp,
            OracleDataType::IntervalYearToMonth { .. }
            | OracleDataType::IntervalDayToSecond { .. } => TypeFamily::Interval,
        },
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
    let cte_aliases: HashSet<_> = build_scope(stmt)
        .cte_sources
        .keys()
        .map(|name| lower(name))
        .collect();

    for node in walk_in_scope(stmt, false) {
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

fn infer_expression_type_family(
    expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
) -> TypeFamily {
    // Scoped annotation wins, including Unknown; never rebind an annotated
    // reference against unrelated sources in the statement-wide catalogue.
    if let Some(data_type) = expr.inferred_type() {
        return data_type_family(data_type);
    }
    infer_expression_type_family_fallback(expr, schema_map, context)
}

fn infer_expression_type_family_fallback(
    expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    context: &TypeCheckContext,
) -> TypeFamily {
    match expr {
        Expression::Literal(literal) => match literal.as_ref() {
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
        // The shared annotator already resolved every lexical input. Missing
        // annotations must not trigger a second, statement-global lookup.
        Expression::Column(_) => TypeFamily::Unknown,
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

fn function_base_name(name: &str) -> &str {
    name.rsplit('.').next().unwrap_or(name).trim()
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

fn check_function_catalog(
    function: &Function,
    dialect: DialectType,
    function_catalog: Option<&dyn FunctionCatalog>,
    strict: bool,
    errors: &mut Vec<ValidationError>,
) {
    let Some(catalog) = function_catalog else {
        return;
    };

    let raw_name = function_base_name(&function.name);
    let normalized_name = function_dispatch_name(&function.name);
    let arity = function.args.len();
    let Some(signatures) = catalog.lookup(dialect, raw_name, &normalized_name) else {
        errors.push(if strict {
            ValidationError::error(
                format!(
                    "Unknown function '{}' for dialect {:?}",
                    function.name, dialect
                ),
                validation_codes::E_UNKNOWN_FUNCTION,
            )
        } else {
            ValidationError::warning(
                format!(
                    "Unknown function '{}' for dialect {:?}",
                    function.name, dialect
                ),
                validation_codes::E_UNKNOWN_FUNCTION,
            )
        });
        return;
    };

    if signatures.iter().any(|sig| sig.matches_arity(arity)) {
        return;
    }

    let expected = signatures
        .iter()
        .map(|sig| sig.describe_arity())
        .collect::<Vec<_>>()
        .join(", ");
    errors.push(if strict {
        ValidationError::error(
            format!(
                "Invalid arity for function '{}': got {}, expected {}",
                function.name, arity, expected
            ),
            validation_codes::E_INVALID_FUNCTION_ARITY,
        )
    } else {
        ValidationError::warning(
            format!(
                "Invalid arity for function '{}': got {}, expected {}",
                function.name, arity, expected
            ),
            validation_codes::E_INVALID_FUNCTION_ARITY,
        )
    });
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
    query: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    dialect: DialectType,
) -> Option<Vec<TypeFamily>> {
    let pair = match query {
        Expression::Union(query) => Some((&query.left, &query.right)),
        Expression::Intersect(query) => Some((&query.left, &query.right)),
        Expression::Except(query) => Some((&query.left, &query.right)),
        _ => None,
    };
    if let Some((left, right)) = pair {
        let left = projection_families(left, schema_map, dialect)?;
        let right = projection_families(right, schema_map, dialect)?;
        return match crate::set_operation::set_operation_layout(query, Some(dialect)).ok()? {
            Some(layout) => Some(
                layout
                    .outputs
                    .iter()
                    .map(|output| {
                        merged_setop_family(
                            output
                                .left_ordinal
                                .and_then(|i| left.get(i).copied())
                                .unwrap_or(TypeFamily::Unknown),
                            output
                                .right_ordinal
                                .and_then(|i| right.get(i).copied())
                                .unwrap_or(TypeFamily::Unknown),
                        )
                    })
                    .collect(),
            ),
            None if left.len() == right.len() => Some(
                left.into_iter()
                    .zip(right)
                    .map(|(l, r)| merged_setop_family(l, r))
                    .collect(),
            ),
            _ => None,
        };
    }
    match query {
        Expression::Select(select) => {
            if select
                .expressions
                .iter()
                .any(|e| matches!(e, Expression::Star(_) | Expression::BracedWildcard(_)))
            {
                return None;
            }
            let context = collect_type_check_context(query, schema_map);
            Some(
                select
                    .expressions
                    .iter()
                    .map(|e| infer_expression_type_family(e, schema_map, &context))
                    .collect(),
            )
        }
        Expression::Subquery(query) => projection_families(&query.this, schema_map, dialect),
        Expression::Paren(query) => projection_families(&query.this, schema_map, dialect),
        Expression::Values(values) => Some(
            values
                .expressions
                .first()?
                .expressions
                .iter()
                .map(|e| infer_expression_type_family(e, schema_map, &TypeCheckContext::default()))
                .collect(),
        ),
        _ => None,
    }
}

fn check_set_operation_compatibility(
    query: &Expression,
    dialect: DialectType,
    op_name: &str,
    left_expr: &Expression,
    right_expr: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    strict: bool,
    errors: &mut Vec<ValidationError>,
) {
    let Some(mut left_projection) = projection_families(left_expr, schema_map, dialect) else {
        return;
    };
    let Some(mut right_projection) = projection_families(right_expr, schema_map, dialect) else {
        return;
    };

    match crate::set_operation::set_operation_layout(query, Some(dialect)) {
        Ok(Some(layout)) => {
            let left = left_projection;
            let right = right_projection;
            left_projection = layout
                .outputs
                .iter()
                .map(|output| {
                    output
                        .left_ordinal
                        .and_then(|i| left.get(i).copied())
                        .unwrap_or(TypeFamily::Unknown)
                })
                .collect();
            right_projection = layout
                .outputs
                .iter()
                .map(|output| {
                    output
                        .right_ordinal
                        .and_then(|i| right.get(i).copied())
                        .unwrap_or(TypeFamily::Unknown)
                })
                .collect();
        }
        Err(error) => {
            if !error.is_indeterminate() {
                errors.push(type_issue(
                    strict,
                    validation_codes::E_SETOP_ARITY_MISMATCH,
                    validation_codes::W_SETOP_IMPLICIT_COERCION,
                    error.to_string(),
                ));
            }
            return;
        }
        Ok(None) => {}
    }
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
    dialect: DialectType,
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

        let Some(source_projection) = projection_families(query, schema_map, dialect) else {
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
    dialect: DialectType,
    schema_map: &HashMap<String, TableSchemaEntry>,
    function_catalog: Option<&dyn FunctionCatalog>,
    strict: bool,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    let context = collect_type_check_context(stmt, schema_map);

    for node in stmt.dfs() {
        for (clause, predicate) in crate::binding::predicates(node) {
            let family = infer_expression_type_family(predicate, schema_map, &context);
            if !predicate_compatible(family, dialect) {
                errors.push(type_issue(
                    strict,
                    validation_codes::E_INVALID_PREDICATE_TYPE,
                    validation_codes::W_PREDICATE_NULLABILITY,
                    format!(
                        "{clause} expects a boolean predicate, found {}",
                        type_family_name(family)
                    ),
                ));
            }
        }
        match node {
            Expression::Insert(insert) => {
                check_insert_assignments(dialect, stmt, insert, schema_map, strict, &mut errors);
            }
            Expression::Update(update) => {
                check_update_assignments(stmt, update, schema_map, strict, &mut errors);
            }
            Expression::Union(union) => {
                check_set_operation_compatibility(
                    node,
                    dialect,
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
                    node,
                    dialect,
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
                    node,
                    dialect,
                    "EXCEPT",
                    &except.left,
                    &except.right,
                    schema_map,
                    strict,
                    &mut errors,
                );
            }
            Expression::And(op) | Expression::Or(op) => {
                for (side, expr) in [("left", &op.left), ("right", &op.right)] {
                    let family = infer_expression_type_family(expr, schema_map, &context);
                    if !predicate_compatible(family, dialect) {
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
                if !predicate_compatible(family, dialect) {
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
                check_function_catalog(function, dialect, function_catalog, strict, &mut errors);
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

fn predicate_compatible(family: TypeFamily, dialect: DialectType) -> bool {
    matches!(family, TypeFamily::Unknown | TypeFamily::Boolean)
        || (matches!(
            dialect,
            DialectType::MySQL | DialectType::SQLite | DialectType::DuckDB
        ) && family.is_numeric())
}

mod semantics;
pub(crate) use semantics::check_semantics;

fn resolve_scope_source_name(scope: &crate::scope::Scope, name: &str) -> Option<String> {
    scope
        .sources
        .get_key_value(name)
        .map(|(k, _)| k.clone())
        .or_else(|| {
            scope
                .sources
                .keys()
                .find(|source| source.eq_ignore_ascii_case(name))
                .cloned()
        })
}

fn source_has_column(
    scope: &crate::scope::Scope,
    source: &str,
    columns: &[String],
    column: &crate::expressions::Identifier,
    dialect: DialectType,
) -> bool {
    let strategy = get_normalization_strategy(Some(dialect));
    let expected = normalize_identifier(column.clone(), strategy).name;
    columns.iter().any(|name| {
        if name == "*" {
            return true;
        }
        let identifier = scope
            .sources
            .get(source)
            .and_then(|source| source_output_identifier(&source.expression, name))
            .cloned()
            .unwrap_or_else(|| crate::binding::schema_identifier(name));
        normalize_identifier(identifier, strategy).name == expected
    })
}

fn source_display_name(scope: &crate::scope::Scope, source_name: &str) -> String {
    scope
        .sources
        .get(source_name)
        .map(|source| match &source.expression {
            Expression::Table(table) => lower(&table_ref_display_name(table)),
            _ => lower(source_name),
        })
        .unwrap_or_else(|| lower(source_name))
}

/// Attach original token locations without manufacturing offsets for synthetic ASTs.
fn reference_diagnostic(
    message: String,
    code: &str,
    strict: bool,
    span: Option<crate::tokens::Span>,
) -> ValidationError {
    let mut error = if strict {
        ValidationError::error(message, code)
    } else {
        ValidationError::warning(message, code)
    };
    if let Some(span) = span {
        error = error
            .with_location(span.line, span.column)
            .with_span(Some(span.start), Some(span.end));
    }
    error
}

use crate::scope::{scope_query, selected_reference_scope as selected_validation_scope};

/// Validation starts from SQL, so source ranges identify individual references
/// across the scope tree's AST copies. Never bind synthetic/unpositioned columns.
/// Types, not defining expressions, are stored to keep alias chains linear in size.
type ProjectionAliasBindings = HashMap<(usize, usize), DataType>;

fn projection_reference_key(column: &Column) -> Option<(usize, usize)> {
    if column.table.is_some() {
        return None;
    }
    column
        .name
        .span
        .or(column.span)
        .map(|span| (span.start, span.end))
}

use crate::binding::bound_identifier as validation_bound_identifier;

/// Apply already resolved bindings to the private validation AST. The canonical
/// visitor keeps this stack-safe and covers references inside typed functions.
fn apply_projection_alias_bindings(
    expression: Expression,
    bindings: &ProjectionAliasBindings,
) -> Expression {
    if bindings.is_empty() {
        return expression;
    }
    enum Task {
        Visit(Expression),
        Finish(Expression, usize),
    }
    let mut pending = vec![Task::Visit(expression)];
    let mut results = Vec::new();
    while let Some(task) = pending.pop() {
        match task {
            Task::Visit(mut expression) => {
                if let Expression::Column(column) = &expression {
                    if let Some(data_type) =
                        projection_reference_key(column).and_then(|key| bindings.get(&key))
                    {
                        results.push(validation_bound_identifier(column.name.clone(), data_type));
                        continue;
                    }
                }
                let mut children = Vec::new();
                crate::ast_children::for_each_child_mut(&mut expression, |child| {
                    children.push(std::mem::replace(
                        child,
                        Expression::Null(crate::expressions::Null),
                    ));
                });
                pending.push(Task::Finish(expression, children.len()));
                pending.extend(children.into_iter().rev().map(Task::Visit));
            }
            Task::Finish(mut expression, count) => {
                let mut children = results.split_off(results.len() - count).into_iter();
                crate::ast_children::for_each_child_mut(&mut expression, |child| {
                    *child = children.next().expect("alias binding child");
                });
                results.push(expression);
            }
        }
    }
    results.pop().expect("alias binding result")
}

fn bind_scope_projection_aliases(
    scope: &mut crate::scope::Scope,
    ancestors: &[&crate::scope::Scope],
    resolver_schema: &MappingSchema,
    dialect: DialectType,
    check_types: bool,
    bindings: &mut ProjectionAliasBindings,
) {
    let Expression::Select(select) = &scope.expression else {
        return;
    };
    if !select
        .expressions
        .iter()
        .any(|expression| matches!(expression, Expression::Alias(_)))
    {
        return;
    }

    // A real input wins over a lateral alias. Ambiguous inputs must also remain
    // columns so the ordinary reference checker can diagnose them. Open sources
    // do not provide enough information to safely choose an alias instead.
    let strategy = get_normalization_strategy(Some(dialect));
    let mut inputs = HashSet::new();
    let mut open = false;
    for source_scope in std::iter::once(&*scope).chain(ancestors.iter().copied()) {
        let mut resolver = Resolver::new(source_scope, resolver_schema, true);
        for source in source_scope.sources.keys() {
            let columns = resolver.get_source_columns(source).unwrap_or_default();
            open |= columns.is_empty() || columns.iter().any(|name| name == "*");
            let expression = &source_scope.sources[source].expression;
            for name in columns {
                let identifier = source_output_identifier(expression, &name)
                    .cloned()
                    .unwrap_or_else(|| crate::expressions::Identifier::new(name));
                inputs.insert(normalize_identifier(identifier, strategy).name);
            }
        }
    }
    if open {
        return;
    }

    // A child SELECT can use CTEs declared in a containing WITH. Reconstruct
    // that lexical catalogue for type inference, in original declaration order.
    let mut ctes: Vec<_> = if check_types {
        scope
            .cte_sources
            .values()
            .filter_map(|source| match &source.expression {
                Expression::Cte(cte) => Some(cte.as_ref().clone()),
                _ => None,
            })
            .collect()
    } else {
        Vec::new()
    };
    ctes.sort_by_key(|cte| cte.alias.span.map(|span| span.start));
    let mut aliases = HashMap::<String, Option<DataType>>::new();
    let Expression::Select(select) = &mut scope.expression else {
        unreachable!()
    };
    // Keep the original sources/CTEs for scoped type inference, without copying
    // the entire projection list for each alias. Bound uses are constant-size.
    let mut projections = std::mem::take(&mut select.expressions);
    let mut type_select = select.clone();
    if !ctes.is_empty() {
        type_select.with = Some(crate::expressions::With {
            ctes,
            recursive: false,
            leading_comments: Vec::new(),
            search: None,
        });
    }
    for projection in projections.iter_mut() {
        let mut references = ProjectionAliasBindings::new();
        for node in walk_in_scope(projection, false) {
            if !crate::binding::lateral_aliases(dialect) {
                break;
            }
            let Expression::Column(column) = node else {
                continue;
            };
            let Some(key) = projection_reference_key(column) else {
                continue;
            };
            let name = normalize_identifier(column.name.clone(), strategy).name;
            if inputs.contains(&name) {
                continue;
            }
            if let Some(Some(data_type)) = aliases.get(&name) {
                references.insert(key, data_type.clone());
            }
        }
        let original = std::mem::replace(projection, Expression::Null(crate::expressions::Null));
        *projection = apply_projection_alias_bindings(original, &references);
        bindings.extend(references);
        if let Expression::Alias(alias) = projection {
            let name = normalize_identifier(alias.alias.clone(), strategy).name;
            // Duplicate names are not evidence of an unambiguous binding.
            if aliases.contains_key(&name) {
                aliases.insert(name, None);
                continue;
            }
            if !check_types {
                aliases.insert(name, Some(DataType::Unknown));
                continue;
            }
            let mut typed = Expression::Select(type_select.clone());
            if let Expression::Select(query) = &mut typed {
                query.expressions.push(alias.this.clone());
            }
            // Child queries were bound first. Their aliases must contribute
            // their actual types, not remain unresolved inputs in this copy.
            typed = apply_projection_alias_bindings(typed, bindings);
            annotate_types(&mut typed, Some(resolver_schema), Some(dialect));
            let data_type = match typed {
                Expression::Select(query) => query.expressions[0]
                    .inferred_type()
                    .cloned()
                    .unwrap_or(DataType::Unknown),
                _ => unreachable!(),
            };
            aliases.insert(name, Some(data_type));
        }
    }
    select.expressions = projections;
    // Only clause-specific output visibility is enabled. JOIN ON remains an
    // input scope, and aliases never become visible in a nested query.
    use crate::binding::{clause_aliases, AliasClause};
    let mut clauses = Vec::new();
    if clause_aliases(dialect, AliasClause::Where) {
        if let Some(clause) = &mut select.where_clause {
            clauses.push(&mut clause.this);
        }
    }
    if clause_aliases(dialect, AliasClause::Group) {
        if let Some(clause) = &mut select.group_by {
            clauses.extend(clause.expressions.iter_mut());
        }
    }
    if clause_aliases(dialect, AliasClause::Having) {
        if let Some(clause) = &mut select.having {
            clauses.push(&mut clause.this);
        }
    }
    if clause_aliases(dialect, AliasClause::Qualify) {
        if let Some(clause) = &mut select.qualify {
            clauses.push(&mut clause.this);
        }
    }
    for clause in clauses {
        let mut references = ProjectionAliasBindings::new();
        for node in walk_in_scope(clause, false) {
            let Expression::Column(column) = node else {
                continue;
            };
            let Some(key) = projection_reference_key(column) else {
                continue;
            };
            let name = normalize_identifier(column.name.clone(), strategy).name;
            if !inputs.contains(&name) {
                if let Some(Some(data_type)) = aliases.get(&name) {
                    references.insert(key, data_type.clone());
                }
            }
        }
        let original = std::mem::replace(clause, Expression::Null(crate::expressions::Null));
        *clause = apply_projection_alias_bindings(original, &references);
        bindings.extend(references);
    }
}

/// Resolver output names are strings. Retain quoting when those names were
/// declared by a CTE/derived table, rather than folding a quoted output name.
fn source_output_identifier<'a>(
    expression: &'a Expression,
    name: &str,
) -> Option<&'a crate::expressions::Identifier> {
    let columns: &[crate::expressions::Identifier] = match expression {
        Expression::Cte(cte) => &cte.columns,
        Expression::Subquery(query) => &query.column_aliases,
        Expression::Alias(alias) => &alias.column_aliases,
        Expression::Table(table) => &table.column_aliases,
        Expression::Paren(paren) => return source_output_identifier(&paren.this, name),
        _ => &[],
    };
    if !columns.is_empty() {
        return columns.iter().find(|column| column.name == name);
    }
    match scope_query(expression) {
        Expression::Select(select) => select.expressions.iter().find_map(|projection| {
            let identifier = match projection {
                Expression::Alias(alias) => &alias.alias,
                Expression::Column(column) => &column.name,
                Expression::Identifier(identifier) => identifier,
                _ => return None,
            };
            (identifier.name == name).then_some(identifier)
        }),
        Expression::Union(query) => source_output_identifier(&query.left, name),
        Expression::Intersect(query) => source_output_identifier(&query.left, name),
        Expression::Except(query) => source_output_identifier(&query.left, name),
        _ => None,
    }
}

/// Prepare the private validation AST, not the public parser/serialized AST.
/// Bound parameters become identifiers (optionally typed), leaving only free
/// column references for schema resolution. Doing this once, before dotted
/// normalization, also prevents type checks from borrowing a shadowed table
/// column's type. Nested query scopes do not inherit lambda parameters.
use crate::binding::bind_lambdas as bind_validation_lambdas;

#[derive(Clone, Copy)]
struct ReferenceValidationOptions {
    dialect: DialectType,
    strict: bool,
    check_references: bool,
    check_types: bool,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum OutputNameResolution {
    InputOnly,
    OutputFirst,
    InputFirst,
}

/// Collect ordering references together with whether they can resolve to the
/// current SELECT's outputs. Other clauses use the ordinary input-source walk.
fn order_by_validation_columns(
    order_by: &crate::expressions::OrderBy,
    dialect: DialectType,
) -> Vec<(&Column, OutputNameResolution)> {
    // Only enable aliases inside scalar expressions for dialects whose rules
    // support them. In particular, PostgreSQL and T-SQL require standalone
    // output names; `ORDER BY alias + 1` must resolve against input columns.
    let scalar_resolution = match dialect {
        // DuckDB's scalar binder prefers inputs inside ordering expressions,
        // unlike its standalone ORDER BY output-name lookup.
        DialectType::DuckDB => OutputNameResolution::InputFirst,
        DialectType::Snowflake | DialectType::BigQuery => OutputNameResolution::OutputFirst,
        _ => OutputNameResolution::InputOnly,
    };
    let mut columns = Vec::new();
    let mut pending: Vec<_> = order_by
        .expressions
        .iter()
        .rev()
        .map(|ordered| (&ordered.this, OutputNameResolution::OutputFirst))
        .collect();
    while let Some((node, resolution)) = pending.pop() {
        match node {
            // Child query scopes are validated separately and must not inherit
            // their parent's SELECT-list aliases.
            Expression::Select(_)
            | Expression::Union(_)
            | Expression::Intersect(_)
            | Expression::Except(_)
            | Expression::Subquery(_)
            | Expression::Exists(_)
            | Expression::Cte(_) => continue,
            Expression::Column(column) => columns.push((column.as_ref(), resolution)),
            Expression::Paren(paren) => pending.push((&paren.this, resolution)),
            _ => {
                let input_only = resolution == OutputNameResolution::InputOnly
                    || matches!(
                        node,
                        Expression::WindowFunction(_)
                            | Expression::WithinGroup(_)
                            | Expression::Filter(_)
                    )
                    || crate::traversal::is_aggregate(node);
                if input_only {
                    columns.extend(walk_in_scope(node, false).filter_map(|node| match node {
                        Expression::Column(column) => {
                            Some((column.as_ref(), OutputNameResolution::InputOnly))
                        }
                        _ => None,
                    }));
                } else {
                    pending.extend(
                        node.children()
                            .into_iter()
                            .rev()
                            .map(|child| (child, scalar_resolution)),
                    );
                }
            }
        }
    }
    columns
}

fn validate_scope_columns(
    scope: &crate::scope::Scope,
    ancestors: &[&crate::scope::Scope],
    schema_map: &HashMap<String, TableSchemaEntry>,
    resolver_schema: &MappingSchema,
    options: ReferenceValidationOptions,
) -> Vec<ValidationError> {
    let ReferenceValidationOptions {
        dialect,
        strict,
        check_references,
        ..
    } = options;
    let mut errors = Vec::new();
    let Expression::Select(select) = &scope.expression else {
        return errors;
    };

    // Normalize struct access with the actual lexical context. A real outer
    // qualifier must not be reinterpreted as a column of an open local table.
    let mut visible = crate::scope::Scope::new(scope.expression.clone());
    for source_scope in std::iter::once(scope).chain(ancestors.iter().copied()) {
        for (name, source) in &source_scope.sources {
            if resolve_scope_source_name(&visible, name).is_none() {
                visible.sources.insert(name.clone(), source.clone());
            }
        }
    }
    let mut normalized = select.clone();
    let mut normalizer = Resolver::new(&visible, resolver_schema, true);
    let _ = normalize_dotted_columns_in_scope(&mut normalized, &visible, &mut normalizer);
    let strategy = get_normalization_strategy(Some(dialect));
    let mut output_names = HashMap::<String, usize>::new();
    for projection in &normalized.expressions {
        let identifier = match projection {
            // Until wildcards are expanded, output-name uniqueness is not
            // known. Preserve source validation instead of letting an explicit
            // alias hide a conflicting column contributed by a wildcard.
            Expression::Star(_) => {
                output_names.clear();
                break;
            }
            Expression::Alias(alias) => &alias.alias,
            Expression::Column(column) if column.name.name != "*" => &column.name,
            Expression::Column(_) => {
                output_names.clear();
                break;
            }
            _ => continue,
        };
        let name = normalize_identifier(identifier.clone(), strategy).name;
        *output_names.entry(name).or_default() += 1;
    }
    // Separate query-level ordering from the ordinary scope walk. This avoids
    // accidentally exempting a same-named reference in another clause, window,
    // or nested query, and leaves the caller's AST untouched.
    let order_by = normalized.order_by.take();
    let expression = Expression::Select(normalized);
    let mut resolvers: Vec<_> = std::iter::once(scope)
        .chain(ancestors.iter().copied())
        .map(|scope| (scope, Resolver::new(scope, resolver_schema, true)))
        .collect();
    let using_columns: HashSet<String> = select
        .joins
        .iter()
        .flat_map(|join| join.using.iter().map(|id| lower(&id.name)))
        .collect();

    let input_columns = walk_in_scope(&expression, false).filter_map(|node| match node {
        Expression::Column(column) => Some((column.as_ref(), OutputNameResolution::InputOnly)),
        _ => None,
    });
    let order_columns = order_by
        .as_ref()
        .into_iter()
        .flat_map(|order| order_by_validation_columns(order, dialect));
    for (column, resolution) in input_columns.chain(order_columns) {
        let output_matches =
            if resolution != OutputNameResolution::InputOnly && column.table.is_none() {
                let name = normalize_identifier(column.name.clone(), strategy).name;
                // Only an unambiguous output name takes precedence over sources.
                // Duplicate output names retain the existing reference checks.
                output_names.get(&name) == Some(&1)
            } else {
                false
            };
        if output_matches && resolution == OutputNameResolution::OutputFirst {
            continue;
        }
        let name = lower(&column.name.name);
        if name.is_empty() || name == "*" {
            continue;
        }
        let span = column.name.span.or(column.span);
        if let Some(qualifier) = &column.table {
            let resolved = resolvers.iter_mut().find_map(|(scope, resolver)| {
                resolve_scope_source_name(scope, &qualifier.name).map(|source| {
                    (
                        *scope,
                        resolver.get_source_columns(&source).unwrap_or_default(),
                        source,
                    )
                })
            });
            if let Some((source_scope, columns, source)) = resolved {
                if !columns.is_empty()
                    && !source_has_column(source_scope, &source, &columns, &column.name, dialect)
                {
                    errors.push(reference_diagnostic(
                        format!(
                            "Unknown column '{}' in table '{}'",
                            name,
                            source_display_name(source_scope, &source)
                        ),
                        validation_codes::E_UNKNOWN_COLUMN,
                        strict,
                        span,
                    ));
                }
            } else {
                errors.push(reference_diagnostic(
                    format!(
                        "Unknown table or alias '{}' referenced by column '{}'",
                        qualifier.name, name
                    ),
                    validation_codes::E_UNRESOLVED_REFERENCE,
                    strict,
                    qualifier.span.or(column.span),
                ));
            }
            continue;
        }

        let mut found = false;
        for (source_scope, resolver) in &mut resolvers {
            let mut matches = 0;
            let mut open = false;
            for source in source_scope.sources.keys() {
                let columns = resolver.get_source_columns(source).unwrap_or_default();
                open |= columns.is_empty() || columns.iter().any(|name| name == "*");
                // Wildcards are not evidence of a definite ambiguity.
                matches += usize::from(
                    !columns.iter().any(|name| name == "*")
                        && source_has_column(source_scope, source, &columns, &column.name, dialect),
                );
            }
            if matches > 0 || open {
                if matches > 1 && check_references && !using_columns.contains(&name) {
                    errors.push(reference_diagnostic(
                        format!(
                            "Ambiguous unqualified column '{}' found in {} referenced tables",
                            name, matches
                        ),
                        if strict {
                            validation_codes::E_AMBIGUOUS_COLUMN_REFERENCE
                        } else {
                            validation_codes::W_WEAK_REFERENCE_INTEGRITY
                        },
                        strict,
                        span,
                    ));
                }
                found = true;
                break;
            }
        }
        if !found {
            if output_matches && resolution == OutputNameResolution::InputFirst {
                continue;
            }
            // Preserve the existing schema-only lookup for standalone column
            // expressions (SELECT id). Never consult unrelated tables when
            // the query has actual sources or an enclosing query context.
            if scope.sources.is_empty()
                && ancestors.is_empty()
                && (schema_map.is_empty()
                    || schema_map
                        .values()
                        .any(|table| table.columns.contains_key(&name)))
            {
                continue;
            }
            let message = if scope.sources.len() == 1 {
                let source = scope.sources.keys().next().unwrap();
                format!(
                    "Unknown column '{}' in table '{}'",
                    name,
                    source_display_name(scope, source)
                )
            } else {
                format!(
                    "Unknown column '{}' (not found in any referenced table)",
                    name
                )
            };
            errors.push(reference_diagnostic(
                message,
                validation_codes::E_UNKNOWN_COLUMN,
                strict,
                span,
            ));
        }
    }
    errors
}

fn validate_scope_tree(
    scope: &crate::scope::Scope,
    ancestors: &[&crate::scope::Scope],
    schema_map: &HashMap<String, TableSchemaEntry>,
    resolver_schema: &MappingSchema,
    options: ReferenceValidationOptions,
    errors: &mut Vec<ValidationError>,
    bindings: &mut ProjectionAliasBindings,
) {
    let mut selected = selected_validation_scope(scope);
    // Bind children before consumers, but retain the existing parent-first
    // diagnostic order. Only physical sources are inherited, never output aliases.
    let mut child_errors = Vec::new();
    // CTEs and ordinary derived tables cannot see their containing SELECT's
    // sources, but may retain ancestors of an enclosing correlated subquery.
    for child in scope
        .cte_scopes
        .iter()
        .chain(&scope.derived_table_scopes)
        .chain(&scope.union_scopes)
    {
        validate_scope_tree(
            child,
            ancestors,
            schema_map,
            resolver_schema,
            options,
            &mut child_errors,
            bindings,
        );
    }
    let outer: Vec<_> = std::iter::once(&selected)
        .chain(ancestors.iter().copied())
        .collect();
    for child in scope.subquery_scopes.iter().chain(&scope.udtf_scopes) {
        validate_scope_tree(
            child,
            &outer,
            schema_map,
            resolver_schema,
            options,
            &mut child_errors,
            bindings,
        );
    }
    bind_scope_projection_aliases(
        &mut selected,
        ancestors,
        resolver_schema,
        options.dialect,
        options.check_types,
        bindings,
    );
    for node in walk_in_scope(&selected.expression, false) {
        let Expression::Table(table) = node else {
            continue;
        };
        let is_cte = table.schema.is_none()
            && table.catalog.is_none()
            && selected
                .cte_sources
                .keys()
                .any(|name| name.eq_ignore_ascii_case(&table.name.name));
        if !is_cte {
            validate_schema_table(table, schema_map, options.strict, errors);
        }
    }
    errors.extend(validate_scope_columns(
        &selected,
        ancestors,
        schema_map,
        resolver_schema,
        options,
    ));
    errors.extend(child_errors);
}

fn validate_schema_table(
    table: &TableRef,
    schema_map: &HashMap<String, TableSchemaEntry>,
    strict: bool,
    errors: &mut Vec<ValidationError>,
) {
    if !table_ref_candidates(table)
        .iter()
        .any(|key| schema_map.contains_key(key))
    {
        errors.push(reference_diagnostic(
            format!("Unknown table '{}'", table_ref_display_name(table)),
            validation_codes::E_UNKNOWN_TABLE,
            strict,
            table.name.span,
        ));
    }
}

fn validate_statement_with_schema(
    stmt: &Expression,
    schema_map: &HashMap<String, TableSchemaEntry>,
    resolver_schema: &MappingSchema,
    options: ReferenceValidationOptions,
    bindings: &mut ProjectionAliasBindings,
) -> Vec<ValidationError> {
    let mut errors = Vec::new();
    // Visit each query tree once, including queries embedded in DML/DDL.
    let mut pending = vec![stmt];
    while let Some(expression) = pending.pop() {
        if let Some(select) = crate::binding::dml_scope(expression) {
            let query = Expression::Select(Box::new(select));
            let scope = build_scope(&query);
            validate_scope_tree(
                &scope,
                &[],
                schema_map,
                resolver_schema,
                options,
                &mut errors,
                bindings,
            );
            let target_columns = match expression {
                Expression::Insert(insert) => {
                    Some((&insert.table, insert.columns.iter().collect::<Vec<_>>()))
                }
                Expression::Update(update) => Some((
                    &update.table,
                    update.set.iter().map(|(column, _)| column).collect(),
                )),
                _ => None,
            };
            if let Some((table, columns)) = target_columns {
                if let Some((_, entry)) = resolve_table_schema_entry(table, schema_map) {
                    for column in columns {
                        if !entry.columns.is_empty()
                            && !entry.columns.contains_key("*")
                            && !entry.columns.contains_key(&lower(&column.name))
                        {
                            errors.push(reference_diagnostic(
                                format!(
                                    "Unknown column '{}' in table '{}'",
                                    column.name, table.name.name
                                ),
                                validation_codes::E_UNKNOWN_COLUMN,
                                options.strict,
                                column.span,
                            ));
                        }
                    }
                }
            }
            if let Expression::Insert(insert) = expression {
                // VALUES has no implicit target-table input scope.
                for value in insert.values.iter().flatten() {
                    for node in walk_in_scope(value, false) {
                        if let Expression::Column(column) = node {
                            errors.push(reference_diagnostic(
                                format!("Unknown column '{}' in INSERT VALUES", column.name.name),
                                validation_codes::E_UNKNOWN_COLUMN,
                                options.strict,
                                column.name.span.or(column.span),
                            ));
                        }
                    }
                    pending.extend(value.children().into_iter().filter(|child| {
                        matches!(child, Expression::Subquery(_) | Expression::Select(_))
                    }));
                }
                if let Some(query) = &insert.query {
                    // A leading WITH belongs to both the INSERT and its source.
                    let mut source = crate::expressions::Select::new();
                    source.with = insert.with.clone();
                    source.expressions.push(query.clone());
                    let scope = build_scope(&Expression::Select(Box::new(source)));
                    validate_scope_tree(
                        &scope,
                        &[],
                        schema_map,
                        resolver_schema,
                        options,
                        &mut errors,
                        bindings,
                    );
                }
            }
            if let Expression::Merge(merge) = expression {
                let target = match merge.this.as_ref() {
                    Expression::Table(table) => Some(table.as_ref()),
                    Expression::Alias(alias) => match &alias.this {
                        Expression::Table(table) => Some(table.as_ref()),
                        _ => None,
                    },
                    _ => None,
                };
                if let Some((table, (_, entry))) = target.and_then(|table| {
                    resolve_table_schema_entry(table, schema_map).map(|entry| (table, entry))
                }) {
                    if let Some(whens) = &merge.whens {
                        for node in whens.dfs() {
                            let Expression::When(when) = node else {
                                continue;
                            };
                            let Expression::Tuple(action) = when.then.as_ref() else {
                                continue;
                            };
                            let Some(Expression::Var(kind)) = action.expressions.first() else {
                                continue;
                            };
                            let Some(Expression::Tuple(assignments)) = action.expressions.get(1)
                            else {
                                continue;
                            };
                            for assignment in &assignments.expressions {
                                let target = if kind.this.eq_ignore_ascii_case("UPDATE") {
                                    if let Expression::Eq(eq) = assignment {
                                        &eq.left
                                    } else {
                                        continue;
                                    }
                                } else if kind.this.eq_ignore_ascii_case("INSERT") {
                                    assignment
                                } else {
                                    continue;
                                };
                                let identifier = match target {
                                    Expression::Identifier(id) => id,
                                    Expression::Column(column) => &column.name,
                                    _ => continue,
                                };
                                if !entry.columns.is_empty()
                                    && !entry.columns.contains_key("*")
                                    && !entry.columns.contains_key(&lower(&identifier.name))
                                {
                                    errors.push(reference_diagnostic(
                                        format!(
                                            "Unknown column '{}' in table '{}'",
                                            identifier.name, table.name.name
                                        ),
                                        validation_codes::E_UNKNOWN_COLUMN,
                                        options.strict,
                                        identifier.span,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
            continue;
        }
        if matches!(
            scope_query(expression),
            Expression::Select(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_)
        ) {
            let scope = build_scope(expression);
            validate_scope_tree(
                &scope,
                &[],
                schema_map,
                resolver_schema,
                options,
                &mut errors,
                bindings,
            );
        } else {
            // DDL may declare a new table; only its query inputs are references.
            if let Expression::CreateTable(create) = expression {
                if let Some(query) = &create.as_select {
                    pending.push(query);
                }
                if let Some(source) = &create.clone_source {
                    validate_schema_table(source, schema_map, options.strict, &mut errors);
                }
                continue;
            }
            if let Expression::Table(table) = expression {
                validate_schema_table(table, schema_map, options.strict, &mut errors);
            }
            pending.extend(expression.children().into_iter().rev());
        }
    }
    errors
}

/// Validate SQL using syntax + schema-aware checks, with optional semantic checks.
pub fn validate_with_schema(
    sql: &str,
    dialect: DialectType,
    schema: &ValidationSchema,
    options: &SchemaValidationOptions,
) -> ValidationResult {
    let strict = options.strict.unwrap_or(schema.strict.unwrap_or(true));

    // Parse once, preserving syntax-error precedence and the caller's guards.
    let d = Dialect::get(dialect);
    let statements = match crate::parse_for_validation(
        sql,
        &d,
        &crate::ValidationOptions {
            complexity_guard: options.complexity_guard,
            strict_syntax: options.strict_syntax,
            semantic: options.semantic,
        },
    ) {
        Ok(exprs) => exprs,
        Err(result) => return result,
    };

    let schema_map = build_schema_map(schema);
    let resolver_schema = mapping_schema_from_validation_schema_with_dialect(schema, dialect);
    let mut all_errors = Vec::new();
    let embedded_function_catalog = if options.check_types && options.function_catalog.is_none() {
        default_embedded_function_catalog()
    } else {
        None
    };
    let effective_function_catalog = options
        .function_catalog
        .as_deref()
        .or_else(|| embedded_function_catalog.as_deref());
    let declared_relationships = if options.check_references {
        build_declared_relationships(schema, &schema_map)
    } else {
        Vec::new()
    };

    if options.check_references {
        all_errors.extend(check_reference_integrity(schema, &schema_map, strict));
    }

    for mut statement in statements {
        if options.semantic {
            all_errors.extend(check_semantics(&statement, dialect, Some(schema)));
        }
        crate::binding::bind_dml_pseudoreferences(&mut statement);
        let statement = bind_validation_lambdas(statement, dialect);
        let mut bindings = ProjectionAliasBindings::new();
        all_errors.extend(validate_statement_with_schema(
            &statement,
            &schema_map,
            &resolver_schema,
            ReferenceValidationOptions {
                dialect,
                strict,
                check_references: options.check_references,
                check_types: options.check_types,
            },
            &mut bindings,
        ));
        let mut statement = apply_projection_alias_bindings(statement, &bindings);
        if options.check_types {
            // Expand known stars using the same qualification path as lineage.
            // Open/partial schemas remain conservative if qualification fails.
            if statement.dfs().any(|node| {
                matches!(
                    node,
                    Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_)
                )
            }) && statement
                .dfs()
                .any(|node| matches!(node, Expression::Star(_)))
            {
                if let Ok(qualified) = crate::optimizer::qualify_schema_aware_expression(
                    statement.clone(),
                    &resolver_schema,
                    Some(dialect),
                ) {
                    statement = qualified;
                }
            }
            annotate_types(&mut statement, Some(&resolver_schema), Some(dialect));
        }
        let stmt = &statement;
        if options.check_types {
            all_errors.extend(check_types(
                stmt,
                dialect,
                &schema_map,
                effective_function_catalog,
                strict,
            ));
        }
        if options.check_references {
            all_errors.extend(check_query_reference_quality(
                stmt,
                &schema_map,
                &resolver_schema,
                &declared_relationships,
            ));
        }
    }

    ValidationResult::with_errors(all_errors)
}

#[cfg(test)]
mod tests;
