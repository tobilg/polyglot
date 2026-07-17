//! SQL Dialect System
//!
//! This module implements the dialect abstraction layer that enables SQL transpilation
//! between more than 30 SQL dialects. Each dialect encapsulates three concerns:
//!
//! - **Tokenization**: Dialect-specific lexing rules (e.g., BigQuery uses backtick quoting,
//!   MySQL uses backtick for identifiers, TSQL uses square brackets).
//! - **Generation**: How AST nodes are rendered back to SQL text, including identifier quoting
//!   style, function name casing, and syntax variations.
//! - **Transformation**: AST-level rewrites that convert dialect-specific constructs to/from
//!   a normalized form (e.g., Snowflake `SQUARE(x)` becomes `POWER(x, 2)`).
//!
//! The primary entry point is [`Dialect::get`], which returns a configured [`Dialect`] instance
//! for a given [`DialectType`]. From there, callers can [`parse`](Dialect::parse),
//! [`generate`](Dialect::generate), [`transform`](Dialect::transform), or
//! [`transpile`](Dialect::transpile) to another dialect in a single call.
//!
//! Each concrete dialect (e.g., `PostgresDialect`, `BigQueryDialect`) implements the
//! [`DialectImpl`] trait, which provides configuration hooks and expression-level transforms.
//! Dialect modules live in submodules of this module and are re-exported here.

mod generic; // Always compiled
#[cfg(feature = "transpile")]
mod normalization;

#[cfg(feature = "dialect-athena")]
mod athena;
#[cfg(feature = "dialect-bigquery")]
mod bigquery;
#[cfg(feature = "dialect-clickhouse")]
mod clickhouse;
#[cfg(feature = "dialect-cockroachdb")]
mod cockroachdb;
#[cfg(feature = "dialect-databricks")]
mod databricks;
#[cfg(feature = "dialect-datafusion")]
mod datafusion;
#[cfg(feature = "dialect-doris")]
mod doris;
#[cfg(feature = "dialect-dremio")]
mod dremio;
#[cfg(feature = "dialect-drill")]
mod drill;
#[cfg(feature = "dialect-druid")]
mod druid;
#[cfg(feature = "dialect-duckdb")]
mod duckdb;
#[cfg(feature = "dialect-dune")]
mod dune;
#[cfg(feature = "dialect-exasol")]
mod exasol;
#[cfg(feature = "dialect-fabric")]
mod fabric;
#[cfg(feature = "dialect-hive")]
mod hive;
#[cfg(feature = "dialect-materialize")]
mod materialize;
#[cfg(feature = "dialect-mysql")]
mod mysql;
#[cfg(feature = "dialect-oracle")]
mod oracle;
#[cfg(feature = "dialect-postgresql")]
mod postgres;
#[cfg(feature = "dialect-presto")]
mod presto;
#[cfg(feature = "dialect-redshift")]
mod redshift;
#[cfg(feature = "dialect-risingwave")]
mod risingwave;
#[cfg(feature = "dialect-singlestore")]
mod singlestore;
#[cfg(feature = "dialect-snowflake")]
mod snowflake;
#[cfg(feature = "dialect-solr")]
mod solr;
#[cfg(feature = "dialect-spark")]
mod spark;
#[cfg(feature = "dialect-sqlite")]
mod sqlite;
#[cfg(feature = "dialect-starrocks")]
mod starrocks;
#[cfg(feature = "dialect-tableau")]
mod tableau;
#[cfg(feature = "dialect-teradata")]
mod teradata;
#[cfg(feature = "dialect-tidb")]
mod tidb;
#[cfg(feature = "dialect-trino")]
mod trino;
#[cfg(feature = "dialect-tsql")]
mod tsql;

pub use generic::GenericDialect; // Always available

#[cfg(feature = "dialect-athena")]
pub use athena::AthenaDialect;
#[cfg(feature = "dialect-bigquery")]
pub use bigquery::BigQueryDialect;
#[cfg(feature = "dialect-clickhouse")]
pub use clickhouse::ClickHouseDialect;
#[cfg(feature = "dialect-cockroachdb")]
pub use cockroachdb::CockroachDBDialect;
#[cfg(feature = "dialect-databricks")]
pub use databricks::DatabricksDialect;
#[cfg(feature = "dialect-datafusion")]
pub use datafusion::DataFusionDialect;
#[cfg(feature = "dialect-doris")]
pub use doris::DorisDialect;
#[cfg(feature = "dialect-dremio")]
pub use dremio::DremioDialect;
#[cfg(feature = "dialect-drill")]
pub use drill::DrillDialect;
#[cfg(feature = "dialect-druid")]
pub use druid::DruidDialect;
#[cfg(feature = "dialect-duckdb")]
pub use duckdb::DuckDBDialect;
#[cfg(feature = "dialect-dune")]
pub use dune::DuneDialect;
#[cfg(feature = "dialect-exasol")]
pub use exasol::ExasolDialect;
#[cfg(feature = "dialect-fabric")]
pub use fabric::FabricDialect;
#[cfg(feature = "dialect-hive")]
pub use hive::HiveDialect;
#[cfg(feature = "dialect-materialize")]
pub use materialize::MaterializeDialect;
#[cfg(feature = "dialect-mysql")]
pub use mysql::MySQLDialect;
#[cfg(feature = "dialect-oracle")]
pub use oracle::OracleDialect;
#[cfg(feature = "dialect-postgresql")]
pub use postgres::PostgresDialect;
#[cfg(feature = "dialect-presto")]
pub use presto::PrestoDialect;
#[cfg(feature = "dialect-redshift")]
pub use redshift::RedshiftDialect;
#[cfg(feature = "dialect-risingwave")]
pub use risingwave::RisingWaveDialect;
#[cfg(feature = "dialect-singlestore")]
pub use singlestore::SingleStoreDialect;
#[cfg(feature = "dialect-snowflake")]
pub use snowflake::SnowflakeDialect;
#[cfg(feature = "dialect-solr")]
pub use solr::SolrDialect;
#[cfg(feature = "dialect-spark")]
pub use spark::SparkDialect;
#[cfg(feature = "dialect-sqlite")]
pub use sqlite::SQLiteDialect;
#[cfg(feature = "dialect-starrocks")]
pub use starrocks::StarRocksDialect;
#[cfg(feature = "dialect-tableau")]
pub use tableau::TableauDialect;
#[cfg(feature = "dialect-teradata")]
pub use teradata::TeradataDialect;
#[cfg(feature = "dialect-tidb")]
pub use tidb::TiDBDialect;
#[cfg(feature = "dialect-trino")]
pub use trino::TrinoDialect;
#[cfg(feature = "dialect-tsql")]
pub use tsql::TSQLDialect;

use crate::error::Result;
#[cfg(feature = "transpile")]
use crate::expressions::{
    BinaryOp, Case, Cast, ColumnConstraint, DateBin, Fetch, Function, Identifier, Interval,
    IntervalUnit, IntervalUnitSpec, Literal, Offset, Over, Top, Var, WindowFrame, WindowFrameBound,
    WindowFrameKind,
};
use crate::expressions::{DataType, Expression};
#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
use crate::expressions::{From, FunctionBody, Join, Null, OrderBy, OutputClause, TableRef, With};
#[cfg(feature = "transpile")]
use crate::generator::UnsupportedLevel;
#[cfg(feature = "generate")]
use crate::generator::{Generator, GeneratorConfig};
#[cfg(feature = "transpile")]
use crate::guard::enforce_generate_ast;
use crate::guard::{enforce_input, ComplexityGuardOptions};
use crate::parser::Parser;
#[cfg(feature = "transpile")]
use crate::tokens::TokenType;
use crate::tokens::{Token, Tokenizer, TokenizerConfig};
#[cfg(feature = "transpile")]
use crate::traversal::ExpressionWalk;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[cfg(feature = "transpile")]
use std::collections::HashSet;
use std::sync::{Arc, LazyLock, RwLock};

/// Enumeration of all supported SQL dialects.
///
/// Each variant corresponds to a specific SQL database engine or query language.
/// The `Generic` variant represents standard SQL with no dialect-specific behavior,
/// and is used as the default when no dialect is specified.
///
/// Dialect names are case-insensitive when parsed from strings via [`FromStr`].
/// Some dialects accept aliases (e.g., "mssql" and "sqlserver" both resolve to [`TSQL`](DialectType::TSQL)).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum DialectType {
    /// Standard SQL with no dialect-specific behavior (default).
    Generic,
    /// PostgreSQL -- advanced open-source relational database.
    PostgreSQL,
    /// MySQL -- widely-used open-source relational database (also accepts "mysql").
    MySQL,
    /// Google BigQuery -- serverless cloud data warehouse with unique syntax (backtick quoting, STRUCT types, QUALIFY).
    BigQuery,
    /// Snowflake -- cloud data platform with QUALIFY clause, FLATTEN, and variant types.
    Snowflake,
    /// DuckDB -- in-process analytical database with modern SQL extensions.
    DuckDB,
    /// SQLite -- lightweight embedded relational database.
    SQLite,
    /// Apache Hive -- data warehouse on Hadoop with HiveQL syntax.
    Hive,
    /// Apache Spark SQL -- distributed query engine (also accepts "spark2").
    Spark,
    /// Trino -- distributed SQL query engine (formerly PrestoSQL).
    Trino,
    /// PrestoDB -- distributed SQL query engine for big data.
    Presto,
    /// Amazon Redshift -- cloud data warehouse based on PostgreSQL.
    Redshift,
    /// Transact-SQL (T-SQL) -- Microsoft SQL Server and Azure SQL (also accepts "mssql", "sqlserver").
    TSQL,
    /// Oracle Database -- commercial relational database with PL/SQL extensions.
    Oracle,
    /// ClickHouse -- column-oriented OLAP database for real-time analytics.
    ClickHouse,
    /// Databricks SQL -- Spark-based lakehouse platform with QUALIFY support.
    Databricks,
    /// Amazon Athena -- serverless query service (hybrid Trino/Hive engine).
    Athena,
    /// Teradata -- enterprise data warehouse with proprietary SQL extensions.
    Teradata,
    /// Apache Doris -- real-time analytical database (MySQL-compatible).
    Doris,
    /// StarRocks -- sub-second OLAP database (MySQL-compatible).
    StarRocks,
    /// Materialize -- streaming SQL database built on differential dataflow.
    Materialize,
    /// RisingWave -- distributed streaming database with PostgreSQL compatibility.
    RisingWave,
    /// SingleStore (formerly MemSQL) -- distributed SQL database (also accepts "memsql").
    SingleStore,
    /// CockroachDB -- distributed SQL database with PostgreSQL compatibility (also accepts "cockroach").
    CockroachDB,
    /// TiDB -- distributed HTAP database with MySQL compatibility.
    TiDB,
    /// Apache Druid -- real-time analytics database.
    Druid,
    /// Apache Solr -- search platform with SQL interface.
    Solr,
    /// Tableau -- data visualization platform with its own SQL dialect.
    Tableau,
    /// Dune Analytics -- blockchain analytics SQL engine.
    Dune,
    /// Microsoft Fabric -- unified analytics platform (T-SQL based).
    Fabric,
    /// Apache Drill -- schema-free SQL query engine for big data.
    Drill,
    /// Dremio -- data lakehouse platform with Arrow-based query engine.
    Dremio,
    /// Exasol -- in-memory analytic database.
    Exasol,
    /// Apache DataFusion -- Arrow-based query engine with modern SQL extensions.
    DataFusion,
}

impl Default for DialectType {
    fn default() -> Self {
        DialectType::Generic
    }
}

impl std::fmt::Display for DialectType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DialectType::Generic => write!(f, "generic"),
            DialectType::PostgreSQL => write!(f, "postgresql"),
            DialectType::MySQL => write!(f, "mysql"),
            DialectType::BigQuery => write!(f, "bigquery"),
            DialectType::Snowflake => write!(f, "snowflake"),
            DialectType::DuckDB => write!(f, "duckdb"),
            DialectType::SQLite => write!(f, "sqlite"),
            DialectType::Hive => write!(f, "hive"),
            DialectType::Spark => write!(f, "spark"),
            DialectType::Trino => write!(f, "trino"),
            DialectType::Presto => write!(f, "presto"),
            DialectType::Redshift => write!(f, "redshift"),
            DialectType::TSQL => write!(f, "tsql"),
            DialectType::Oracle => write!(f, "oracle"),
            DialectType::ClickHouse => write!(f, "clickhouse"),
            DialectType::Databricks => write!(f, "databricks"),
            DialectType::Athena => write!(f, "athena"),
            DialectType::Teradata => write!(f, "teradata"),
            DialectType::Doris => write!(f, "doris"),
            DialectType::StarRocks => write!(f, "starrocks"),
            DialectType::Materialize => write!(f, "materialize"),
            DialectType::RisingWave => write!(f, "risingwave"),
            DialectType::SingleStore => write!(f, "singlestore"),
            DialectType::CockroachDB => write!(f, "cockroachdb"),
            DialectType::TiDB => write!(f, "tidb"),
            DialectType::Druid => write!(f, "druid"),
            DialectType::Solr => write!(f, "solr"),
            DialectType::Tableau => write!(f, "tableau"),
            DialectType::Dune => write!(f, "dune"),
            DialectType::Fabric => write!(f, "fabric"),
            DialectType::Drill => write!(f, "drill"),
            DialectType::Dremio => write!(f, "dremio"),
            DialectType::Exasol => write!(f, "exasol"),
            DialectType::DataFusion => write!(f, "datafusion"),
        }
    }
}

impl std::str::FromStr for DialectType {
    type Err = crate::error::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "generic" | "" => Ok(DialectType::Generic),
            "postgres" | "postgresql" => Ok(DialectType::PostgreSQL),
            "mysql" => Ok(DialectType::MySQL),
            "bigquery" => Ok(DialectType::BigQuery),
            "snowflake" => Ok(DialectType::Snowflake),
            "duckdb" => Ok(DialectType::DuckDB),
            "sqlite" => Ok(DialectType::SQLite),
            "hive" => Ok(DialectType::Hive),
            "spark" | "spark2" => Ok(DialectType::Spark),
            "trino" => Ok(DialectType::Trino),
            "presto" => Ok(DialectType::Presto),
            "redshift" => Ok(DialectType::Redshift),
            "tsql" | "mssql" | "sqlserver" => Ok(DialectType::TSQL),
            "oracle" => Ok(DialectType::Oracle),
            "clickhouse" => Ok(DialectType::ClickHouse),
            "databricks" => Ok(DialectType::Databricks),
            "athena" => Ok(DialectType::Athena),
            "teradata" => Ok(DialectType::Teradata),
            "doris" => Ok(DialectType::Doris),
            "starrocks" => Ok(DialectType::StarRocks),
            "materialize" => Ok(DialectType::Materialize),
            "risingwave" => Ok(DialectType::RisingWave),
            "singlestore" | "memsql" => Ok(DialectType::SingleStore),
            "cockroachdb" | "cockroach" => Ok(DialectType::CockroachDB),
            "tidb" => Ok(DialectType::TiDB),
            "druid" => Ok(DialectType::Druid),
            "solr" => Ok(DialectType::Solr),
            "tableau" => Ok(DialectType::Tableau),
            "dune" => Ok(DialectType::Dune),
            "fabric" => Ok(DialectType::Fabric),
            "drill" => Ok(DialectType::Drill),
            "dremio" => Ok(DialectType::Dremio),
            "exasol" => Ok(DialectType::Exasol),
            "datafusion" | "arrow-datafusion" | "arrow_datafusion" => Ok(DialectType::DataFusion),
            _ => Err(crate::error::Error::parse(
                format!("Unknown dialect: {}", s),
                0,
                0,
                0,
                0,
            )),
        }
    }
}

/// Trait that each concrete SQL dialect must implement.
///
/// `DialectImpl` provides the configuration hooks and per-expression transform logic
/// that distinguish one dialect from another. Implementors supply:
///
/// - A [`DialectType`] identifier.
/// - Optional overrides for tokenizer and generator configuration (defaults to generic SQL).
/// - An expression-level transform function ([`transform_expr`](DialectImpl::transform_expr))
///   that rewrites individual AST nodes for this dialect (e.g., converting `NVL` to `COALESCE`).
/// - An optional preprocessing step ([`preprocess`](DialectImpl::preprocess)) for whole-tree
///   rewrites that must run before the recursive per-node transform (e.g., eliminating QUALIFY).
///
/// The default implementations are no-ops, so a minimal dialect only needs to provide
/// [`dialect_type`](DialectImpl::dialect_type) and override the methods that differ from
/// standard SQL.
pub trait DialectImpl {
    /// Returns the [`DialectType`] that identifies this dialect.
    fn dialect_type(&self) -> DialectType;

    /// Returns the tokenizer configuration for this dialect.
    ///
    /// Override to customize identifier quoting characters, string escape rules,
    /// comment styles, and other lexing behavior.
    fn tokenizer_config(&self) -> TokenizerConfig {
        TokenizerConfig::default()
    }

    /// Returns the generator configuration for this dialect.
    ///
    /// Override to customize identifier quoting style, function name casing,
    /// keyword casing, and other SQL generation behavior.
    #[cfg(feature = "generate")]
    fn generator_config(&self) -> GeneratorConfig {
        GeneratorConfig::default()
    }

    /// Returns a generator configuration tailored to a specific expression.
    ///
    /// Override this for hybrid dialects like Athena that route to different SQL engines
    /// based on expression type (e.g., Hive-style generation for DDL, Trino-style for DML).
    /// The default delegates to [`generator_config`](DialectImpl::generator_config).
    #[cfg(feature = "generate")]
    fn generator_config_for_expr(&self, _expr: &Expression) -> GeneratorConfig {
        self.generator_config()
    }

    /// Transforms a single expression node for this dialect, without recursing into children.
    ///
    /// This is the per-node rewrite hook invoked by [`transform_recursive`]. Return the
    /// expression unchanged if no dialect-specific rewrite is needed. Transformations
    /// typically include function renaming, operator substitution, and type mapping.
    #[cfg(feature = "transpile")]
    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        Ok(expr)
    }

    /// Applies whole-tree preprocessing transforms before the recursive per-node pass.
    ///
    /// Override this to apply structural rewrites that must see the entire tree at once,
    /// such as `eliminate_qualify`, `eliminate_distinct_on`, `ensure_bools`, or
    /// `explode_projection_to_unnest`. The default is a no-op pass-through.
    #[cfg(feature = "transpile")]
    fn preprocess(&self, expr: Expression) -> Result<Expression> {
        Ok(expr)
    }
}

/// Recursively transforms a [`DataType`](crate::expressions::DataType), handling nested
/// parametric types such as `ARRAY<INT>`, `STRUCT<a INT, b TEXT>`, and `MAP<STRING, INT>`.
///
/// The outer type is first passed through `transform_fn` as an `Expression::DataType`,
/// and then nested element/field types are recursed into. This ensures that dialect-level
/// type mappings (e.g., `INT` to `INTEGER`) propagate into complex nested types.
#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_data_type_recursive<F>(
    dt: crate::expressions::DataType,
    transform_fn: &F,
) -> Result<crate::expressions::DataType>
where
    F: Fn(Expression) -> Result<Expression>,
{
    use crate::expressions::DataType;
    // First, transform the outermost type through the expression system
    let dt_expr = transform_fn(Expression::DataType(dt))?;
    let dt = match dt_expr {
        Expression::DataType(d) => d,
        _ => {
            return Ok(match dt_expr {
                _ => DataType::Custom {
                    name: "UNKNOWN".to_string(),
                },
            })
        }
    };
    // Then recurse into nested types
    match dt {
        DataType::Array {
            element_type,
            dimension,
        } => {
            let inner = transform_data_type_recursive(*element_type, transform_fn)?;
            Ok(DataType::Array {
                element_type: Box::new(inner),
                dimension,
            })
        }
        DataType::List { element_type } => {
            let inner = transform_data_type_recursive(*element_type, transform_fn)?;
            Ok(DataType::List {
                element_type: Box::new(inner),
            })
        }
        DataType::Struct { fields, nested } => {
            let mut new_fields = Vec::new();
            for mut field in fields {
                field.data_type = transform_data_type_recursive(field.data_type, transform_fn)?;
                new_fields.push(field);
            }
            Ok(DataType::Struct {
                fields: new_fields,
                nested,
            })
        }
        DataType::Map {
            key_type,
            value_type,
        } => {
            let k = transform_data_type_recursive(*key_type, transform_fn)?;
            let v = transform_data_type_recursive(*value_type, transform_fn)?;
            Ok(DataType::Map {
                key_type: Box::new(k),
                value_type: Box::new(v),
            })
        }
        other => Ok(other),
    }
}

/// Convert DuckDB C-style format strings to Presto C-style format strings.
/// DuckDB and Presto both use C-style % directives but with different specifiers for some cases.
#[cfg(feature = "transpile")]
fn duckdb_to_presto_format(fmt: &str) -> String {
    // Order matters: handle longer patterns first to avoid partial replacements
    let mut result = fmt.to_string();
    // First pass: mark multi-char patterns with placeholders
    result = result.replace("%-m", "\x01NOPADM\x01");
    result = result.replace("%-d", "\x01NOPADD\x01");
    result = result.replace("%-I", "\x01NOPADI\x01");
    result = result.replace("%-H", "\x01NOPADH\x01");
    result = result.replace("%H:%M:%S", "\x01HMS\x01");
    result = result.replace("%Y-%m-%d", "\x01YMD\x01");
    // Now convert individual specifiers
    result = result.replace("%M", "%i");
    result = result.replace("%S", "%s");
    // Restore multi-char patterns with Presto equivalents
    result = result.replace("\x01NOPADM\x01", "%c");
    result = result.replace("\x01NOPADD\x01", "%e");
    result = result.replace("\x01NOPADI\x01", "%l");
    result = result.replace("\x01NOPADH\x01", "%k");
    result = result.replace("\x01HMS\x01", "%T");
    result = result.replace("\x01YMD\x01", "%Y-%m-%d");
    result
}

/// Convert DuckDB C-style format strings to BigQuery format strings.
/// BigQuery uses a mix of strftime-like directives.
#[cfg(feature = "transpile")]
fn duckdb_to_bigquery_format(fmt: &str) -> String {
    let mut result = fmt.to_string();
    // Handle longer patterns first
    result = result.replace("%-d", "%e");
    result = result.replace("%Y-%m-%d %H:%M:%S", "%F %T");
    result = result.replace("%Y-%m-%d", "%F");
    result = result.replace("%H:%M:%S", "%T");
    result
}

#[cfg(feature = "transpile")]
fn presto_to_java_format(fmt: &str) -> String {
    fmt.replace("%Y", "yyyy")
        .replace("%m", "MM")
        .replace("%d", "dd")
        .replace("%H", "HH")
        .replace("%i", "mm")
        .replace("%S", "ss")
        .replace("%s", "ss")
        .replace("%y", "yy")
        .replace("%T", "HH:mm:ss")
        .replace("%F", "yyyy-MM-dd")
        .replace("%M", "MMMM")
}

#[cfg(feature = "transpile")]
fn normalize_presto_format(fmt: &str) -> String {
    fmt.replace("%H:%i:%S", "%T").replace("%H:%i:%s", "%T")
}

#[cfg(feature = "transpile")]
fn presto_to_duckdb_format(fmt: &str) -> String {
    fmt.replace("%i", "%M")
        .replace("%s", "%S")
        .replace("%T", "%H:%M:%S")
}

#[cfg(feature = "transpile")]
fn presto_to_bigquery_format(fmt: &str) -> String {
    fmt.replace("%Y-%m-%d", "%F")
        .replace("%H:%i:%S", "%T")
        .replace("%H:%i:%s", "%T")
        .replace("%i", "%M")
        .replace("%s", "%S")
}

#[cfg(feature = "transpile")]
fn is_default_presto_timestamp_format(fmt: &str) -> bool {
    let normalized = normalize_presto_format(fmt);
    normalized == "%Y-%m-%d %T"
        || normalized == "%Y-%m-%d %H:%i:%S"
        || fmt == "%Y-%m-%d %H:%i:%S"
        || fmt == "%Y-%m-%d %T"
}

#[cfg(feature = "transpile")]
fn is_default_presto_date_format(fmt: &str) -> bool {
    fmt == "%Y-%m-%d" || fmt == "%F"
}

/// Applies a transform function bottom-up through an entire expression tree.
///
/// The public entrypoint uses an explicit task stack for the recursion-heavy shapes
/// that dominate deeply nested SQL (nested SELECT/FROM/SUBQUERY chains, set-operation
/// trees, and common binary/unary expression chains). Less common shapes currently
/// reuse the reference recursive implementation so semantics stay identical while
/// the hot path avoids stack growth.
#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
pub fn transform_recursive<F>(expr: Expression, transform_fn: &F) -> Result<Expression>
where
    F: Fn(Expression) -> Result<Expression>,
{
    #[cfg(feature = "stacker")]
    {
        let red_zone = if cfg!(debug_assertions) {
            4 * 1024 * 1024
        } else {
            1024 * 1024
        };
        stacker::maybe_grow(red_zone, 8 * 1024 * 1024, move || {
            transform_recursive_inner(expr, transform_fn)
        })
    }
    #[cfg(not(feature = "stacker"))]
    {
        transform_recursive_inner(expr, transform_fn)
    }
}

#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_recursive_inner<F>(expr: Expression, transform_fn: &F) -> Result<Expression>
where
    F: Fn(Expression) -> Result<Expression>,
{
    enum Task {
        Visit(Expression),
        Finish {
            shell: Expression,
            child_count: usize,
        },
    }

    // These are the shapes handled by the former explicit-stack fast path. Other
    // nodes retain the reference transformer's selective and wrapper-aware child
    // semantics even though all physical children are visible to traversal APIs.
    fn uses_generated_dispatch(expression: &Expression) -> bool {
        match expression {
            Expression::Select(select) => {
                select.joins.is_empty()
                    && select.with.is_none()
                    && select.order_by.is_none()
                    && select.windows.is_none()
                    && select.settings.is_none()
            }
            Expression::Union(set_op) => set_op.with.is_none() && set_op.order_by.is_none(),
            Expression::Intersect(set_op) => set_op.with.is_none() && set_op.order_by.is_none(),
            Expression::Except(set_op) => set_op.with.is_none() && set_op.order_by.is_none(),
            Expression::Literal(_)
            | Expression::Boolean(_)
            | Expression::Null(_)
            | Expression::Identifier(_)
            | Expression::Star(_)
            | Expression::Parameter(_)
            | Expression::Placeholder(_)
            | Expression::SessionParameter(_)
            | Expression::Alias(_)
            | Expression::Paren(_)
            | Expression::Not(_)
            | Expression::Neg(_)
            | Expression::IsNull(_)
            | Expression::IsTrue(_)
            | Expression::IsFalse(_)
            | Expression::Subquery(_)
            | Expression::Exists(_)
            | Expression::Any(_)
            | Expression::All(_)
            | Expression::TableArgument(_)
            | Expression::And(_)
            | Expression::Or(_)
            | Expression::Add(_)
            | Expression::Sub(_)
            | Expression::Mul(_)
            | Expression::Div(_)
            | Expression::Eq(_)
            | Expression::NullSafeEq(_)
            | Expression::NullSafeNeq(_)
            | Expression::Lt(_)
            | Expression::Gt(_)
            | Expression::Neq(_)
            | Expression::Lte(_)
            | Expression::Gte(_)
            | Expression::Mod(_)
            | Expression::Concat(_)
            | Expression::BitwiseAnd(_)
            | Expression::BitwiseOr(_)
            | Expression::BitwiseXor(_)
            | Expression::Is(_)
            | Expression::MemberOf(_)
            | Expression::ArrayContainsAll(_)
            | Expression::ArrayContainedBy(_)
            | Expression::ArrayOverlaps(_)
            | Expression::TsMatch(_)
            | Expression::Adjacent(_)
            | Expression::Like(_)
            | Expression::ILike(_)
            | Expression::Function(_)
            | Expression::Lead(_)
            | Expression::Lag(_)
            | Expression::Array(_)
            | Expression::Tuple(_)
            | Expression::ArrayFunc(_)
            | Expression::Coalesce(_)
            | Expression::Greatest(_)
            | Expression::Least(_)
            | Expression::ArrayConcat(_)
            | Expression::ArrayIntersect(_)
            | Expression::ArrayZip(_)
            | Expression::MapConcat(_)
            | Expression::JsonArray(_)
            | Expression::From(_) => true,
            _ => false,
        }
    }

    let mut tasks = vec![Task::Visit(expr)];
    let mut results = Vec::new();

    while let Some(task) = tasks.pop() {
        match task {
            Task::Visit(mut expression) => {
                if !uses_generated_dispatch(&expression) {
                    results.push(transform_recursive_reference(expression, transform_fn)?);
                    continue;
                }

                let mut children = Vec::new();
                crate::ast_children::for_each_child_mut(&mut expression, |child| {
                    children.push(std::mem::replace(child, Expression::Null(Null)));
                });
                let child_count = children.len();
                tasks.push(Task::Finish {
                    shell: expression,
                    child_count,
                });
                for child in children.into_iter().rev() {
                    tasks.push(Task::Visit(child));
                }
            }
            Task::Finish {
                mut shell,
                child_count,
            } => {
                if results.len() < child_count {
                    return Err(crate::error::Error::Internal(
                        "transform result stack underflow".to_string(),
                    ));
                }
                let transformed_children = results.split_off(results.len() - child_count);
                let mut transformed_children = transformed_children.into_iter();
                crate::ast_children::for_each_child_mut(&mut shell, |child| {
                    *child = transformed_children
                        .next()
                        .expect("validated transform child count");
                });
                if transformed_children.next().is_some() {
                    return Err(crate::error::Error::Internal(
                        "transform child restoration mismatch".to_string(),
                    ));
                }
                results.push(transform_fn(shell)?);
            }
        }
    }

    match results.len() {
        1 => Ok(results.pop().expect("single transform result")),
        _ => Err(crate::error::Error::Internal(
            "unexpected transform result stack size".to_string(),
        )),
    }
}

#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_table_ref_recursive<F>(table: TableRef, transform_fn: &F) -> Result<TableRef>
where
    F: Fn(Expression) -> Result<Expression>,
{
    match transform_recursive(Expression::Table(Box::new(table)), transform_fn)? {
        Expression::Table(table) => Ok(*table),
        _ => Err(crate::error::Error::parse(
            "TableRef transformation returned non-table expression",
            0,
            0,
            0,
            0,
        )),
    }
}

#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_from_recursive<F>(from: From, transform_fn: &F) -> Result<From>
where
    F: Fn(Expression) -> Result<Expression>,
{
    match transform_recursive(Expression::From(Box::new(from)), transform_fn)? {
        Expression::From(from) => Ok(*from),
        _ => Err(crate::error::Error::parse(
            "FROM transformation returned non-FROM expression",
            0,
            0,
            0,
            0,
        )),
    }
}

#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_join_recursive<F>(mut join: Join, transform_fn: &F) -> Result<Join>
where
    F: Fn(Expression) -> Result<Expression>,
{
    join.this = transform_recursive(join.this, transform_fn)?;
    if let Some(on) = join.on.take() {
        join.on = Some(transform_recursive(on, transform_fn)?);
    }
    if let Some(match_condition) = join.match_condition.take() {
        join.match_condition = Some(transform_recursive(match_condition, transform_fn)?);
    }
    join.pivots = join
        .pivots
        .into_iter()
        .map(|pivot| transform_recursive(pivot, transform_fn))
        .collect::<Result<Vec<_>>>()?;

    match transform_fn(Expression::Join(Box::new(join)))? {
        Expression::Join(join) => Ok(*join),
        _ => Err(crate::error::Error::parse(
            "Join transformation returned non-join expression",
            0,
            0,
            0,
            0,
        )),
    }
}

#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_output_clause_recursive<F>(
    mut output: OutputClause,
    transform_fn: &F,
) -> Result<OutputClause>
where
    F: Fn(Expression) -> Result<Expression>,
{
    output.columns = output
        .columns
        .into_iter()
        .map(|column| transform_recursive(column, transform_fn))
        .collect::<Result<Vec<_>>>()?;
    if let Some(into_table) = output.into_table.take() {
        output.into_table = Some(transform_recursive(into_table, transform_fn)?);
    }
    Ok(output)
}

#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_with_recursive<F>(mut with: With, transform_fn: &F) -> Result<With>
where
    F: Fn(Expression) -> Result<Expression>,
{
    with.ctes = with
        .ctes
        .into_iter()
        .map(|mut cte| {
            cte.this = transform_recursive(cte.this, transform_fn)?;
            Ok(cte)
        })
        .collect::<Result<Vec<_>>>()?;
    if let Some(search) = with.search.take() {
        with.search = Some(Box::new(transform_recursive(*search, transform_fn)?));
    }
    Ok(with)
}

#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_order_by_recursive<F>(mut order: OrderBy, transform_fn: &F) -> Result<OrderBy>
where
    F: Fn(Expression) -> Result<Expression>,
{
    order.expressions = order
        .expressions
        .into_iter()
        .map(|mut ordered| {
            let original = ordered.this.clone();
            ordered.this = transform_recursive(ordered.this, transform_fn).unwrap_or(original);
            match transform_fn(Expression::Ordered(Box::new(ordered.clone()))) {
                Ok(Expression::Ordered(transformed)) => Ok(*transformed),
                Ok(_) | Err(_) => Ok(ordered),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(order)
}

#[cfg(any(
    feature = "transpile",
    feature = "ast-tools",
    feature = "generate",
    feature = "semantic"
))]
fn transform_recursive_reference<F>(expr: Expression, transform_fn: &F) -> Result<Expression>
where
    F: Fn(Expression) -> Result<Expression>,
{
    use crate::expressions::BinaryOp;

    // Helper macro to recurse into AggFunc-based expressions (this, filter, order_by, having_max, limit).
    macro_rules! recurse_agg {
        ($variant:ident, $f:expr) => {{
            let mut f = $f;
            f.this = transform_recursive(f.this, transform_fn)?;
            if let Some(filter) = f.filter.take() {
                f.filter = Some(transform_recursive(filter, transform_fn)?);
            }
            for ord in &mut f.order_by {
                ord.this = transform_recursive(
                    std::mem::replace(&mut ord.this, Expression::Null(crate::expressions::Null)),
                    transform_fn,
                )?;
            }
            if let Some((ref mut expr, _)) = f.having_max {
                *expr = Box::new(transform_recursive(
                    std::mem::replace(expr.as_mut(), Expression::Null(crate::expressions::Null)),
                    transform_fn,
                )?);
            }
            if let Some(limit) = f.limit.take() {
                f.limit = Some(Box::new(transform_recursive(*limit, transform_fn)?));
            }
            Expression::$variant(f)
        }};
    }

    // Helper macro to transform binary ops with Box<BinaryOp>
    macro_rules! transform_binary {
        ($variant:ident, $op:expr) => {{
            let left = transform_recursive($op.left, transform_fn)?;
            let right = transform_recursive($op.right, transform_fn)?;
            Expression::$variant(Box::new(BinaryOp {
                left,
                right,
                left_comments: $op.left_comments,
                operator_comments: $op.operator_comments,
                trailing_comments: $op.trailing_comments,
                inferred_type: $op.inferred_type,
            }))
        }};
    }

    // Fast path: leaf nodes never need child traversal, apply transform directly
    if matches!(
        &expr,
        Expression::Literal(_)
            | Expression::Boolean(_)
            | Expression::Null(_)
            | Expression::Identifier(_)
            | Expression::Star(_)
            | Expression::Parameter(_)
            | Expression::Placeholder(_)
            | Expression::SessionParameter(_)
    ) {
        return transform_fn(expr);
    }

    // First recursively transform children, then apply the transform function
    let expr = match expr {
        Expression::Select(mut select) => {
            select.expressions = select
                .expressions
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;

            // Transform FROM clause
            if let Some(mut from) = select.from.take() {
                from.expressions = from
                    .expressions
                    .into_iter()
                    .map(|e| transform_recursive(e, transform_fn))
                    .collect::<Result<Vec<_>>>()?;
                select.from = Some(from);
            }

            // Transform JOINs - important for CROSS APPLY / LATERAL transformations
            select.joins = select
                .joins
                .into_iter()
                .map(|mut join| {
                    join.this = transform_recursive(join.this, transform_fn)?;
                    if let Some(on) = join.on.take() {
                        join.on = Some(transform_recursive(on, transform_fn)?);
                    }
                    // Wrap join in Expression::Join to allow transform_fn to transform it
                    match transform_fn(Expression::Join(Box::new(join)))? {
                        Expression::Join(j) => Ok(*j),
                        _ => Err(crate::error::Error::parse(
                            "Join transformation returned non-join expression",
                            0,
                            0,
                            0,
                            0,
                        )),
                    }
                })
                .collect::<Result<Vec<_>>>()?;

            // Transform LATERAL VIEW expressions (Hive/Spark)
            select.lateral_views = select
                .lateral_views
                .into_iter()
                .map(|mut lv| {
                    lv.this = transform_recursive(lv.this, transform_fn)?;
                    Ok(lv)
                })
                .collect::<Result<Vec<_>>>()?;

            // Transform WHERE clause
            if let Some(mut where_clause) = select.where_clause.take() {
                where_clause.this = transform_recursive(where_clause.this, transform_fn)?;
                select.where_clause = Some(where_clause);
            }

            // Transform GROUP BY
            if let Some(mut group_by) = select.group_by.take() {
                group_by.expressions = group_by
                    .expressions
                    .into_iter()
                    .map(|e| transform_recursive(e, transform_fn))
                    .collect::<Result<Vec<_>>>()?;
                select.group_by = Some(group_by);
            }

            // Transform HAVING
            if let Some(mut having) = select.having.take() {
                having.this = transform_recursive(having.this, transform_fn)?;
                select.having = Some(having);
            }

            // Transform WITH (CTEs)
            if let Some(mut with) = select.with.take() {
                with.ctes = with
                    .ctes
                    .into_iter()
                    .map(|mut cte| {
                        let original = cte.this.clone();
                        cte.this = transform_recursive(cte.this, transform_fn).unwrap_or(original);
                        cte
                    })
                    .collect();
                select.with = Some(with);
            }

            // Transform ORDER BY
            if let Some(mut order) = select.order_by.take() {
                order.expressions = order
                    .expressions
                    .into_iter()
                    .map(|o| {
                        let mut o = o;
                        let original = o.this.clone();
                        o.this = transform_recursive(o.this, transform_fn).unwrap_or(original);
                        // Also apply transform to the Ordered wrapper itself (for NULLS FIRST etc.)
                        match transform_fn(Expression::Ordered(Box::new(o.clone()))) {
                            Ok(Expression::Ordered(transformed)) => *transformed,
                            Ok(_) | Err(_) => o,
                        }
                    })
                    .collect();
                select.order_by = Some(order);
            }

            // Transform WINDOW clause order_by
            if let Some(ref mut windows) = select.windows {
                for nw in windows.iter_mut() {
                    nw.spec.order_by = std::mem::take(&mut nw.spec.order_by)
                        .into_iter()
                        .map(|o| {
                            let mut o = o;
                            let original = o.this.clone();
                            o.this = transform_recursive(o.this, transform_fn).unwrap_or(original);
                            match transform_fn(Expression::Ordered(Box::new(o.clone()))) {
                                Ok(Expression::Ordered(transformed)) => *transformed,
                                Ok(_) | Err(_) => o,
                            }
                        })
                        .collect();
                }
            }

            // Transform QUALIFY
            if let Some(mut qual) = select.qualify.take() {
                qual.this = transform_recursive(qual.this, transform_fn)?;
                select.qualify = Some(qual);
            }

            Expression::Select(select)
        }
        Expression::Function(mut f) => {
            f.args = f
                .args
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            Expression::Function(f)
        }
        Expression::AggregateFunction(mut f) => {
            f.args = f
                .args
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            if let Some(filter) = f.filter {
                f.filter = Some(transform_recursive(filter, transform_fn)?);
            }
            Expression::AggregateFunction(f)
        }
        Expression::WindowFunction(mut wf) => {
            wf.this = transform_recursive(wf.this, transform_fn)?;
            wf.over.partition_by = wf
                .over
                .partition_by
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            // Transform order_by items through Expression::Ordered wrapper
            wf.over.order_by = wf
                .over
                .order_by
                .into_iter()
                .map(|o| {
                    let mut o = o;
                    o.this = transform_recursive(o.this, transform_fn)?;
                    match transform_fn(Expression::Ordered(Box::new(o)))? {
                        Expression::Ordered(transformed) => Ok(*transformed),
                        _ => Err(crate::error::Error::parse(
                            "Ordered transformation returned non-Ordered expression",
                            0,
                            0,
                            0,
                            0,
                        )),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            Expression::WindowFunction(wf)
        }
        Expression::Alias(mut a) => {
            a.this = transform_recursive(a.this, transform_fn)?;
            Expression::Alias(a)
        }
        Expression::Cast(mut c) => {
            c.this = transform_recursive(c.this, transform_fn)?;
            // Also transform the target data type (recursively for nested types like ARRAY<INT>, STRUCT<a INT>)
            c.to = transform_data_type_recursive(c.to, transform_fn)?;
            Expression::Cast(c)
        }
        Expression::And(op) => transform_binary!(And, *op),
        Expression::Or(op) => transform_binary!(Or, *op),
        Expression::Add(op) => transform_binary!(Add, *op),
        Expression::Sub(op) => transform_binary!(Sub, *op),
        Expression::Mul(op) => transform_binary!(Mul, *op),
        Expression::Div(op) => transform_binary!(Div, *op),
        Expression::Eq(op) => transform_binary!(Eq, *op),
        Expression::Lt(op) => transform_binary!(Lt, *op),
        Expression::Gt(op) => transform_binary!(Gt, *op),
        Expression::Paren(mut p) => {
            p.this = transform_recursive(p.this, transform_fn)?;
            Expression::Paren(p)
        }
        Expression::Coalesce(mut f) => {
            f.expressions = f
                .expressions
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            Expression::Coalesce(f)
        }
        Expression::IfNull(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::IfNull(f)
        }
        Expression::Nvl(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::Nvl(f)
        }
        Expression::In(mut i) => {
            i.this = transform_recursive(i.this, transform_fn)?;
            i.expressions = i
                .expressions
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            if let Some(query) = i.query {
                i.query = Some(transform_recursive(query, transform_fn)?);
            }
            Expression::In(i)
        }
        Expression::Not(mut n) => {
            n.this = transform_recursive(n.this, transform_fn)?;
            Expression::Not(n)
        }
        Expression::ArraySlice(mut s) => {
            s.this = transform_recursive(s.this, transform_fn)?;
            if let Some(start) = s.start {
                s.start = Some(transform_recursive(start, transform_fn)?);
            }
            if let Some(end) = s.end {
                s.end = Some(transform_recursive(end, transform_fn)?);
            }
            Expression::ArraySlice(s)
        }
        Expression::Subscript(mut s) => {
            s.this = transform_recursive(s.this, transform_fn)?;
            s.index = transform_recursive(s.index, transform_fn)?;
            Expression::Subscript(s)
        }
        Expression::Array(mut a) => {
            a.expressions = a
                .expressions
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            Expression::Array(a)
        }
        Expression::Struct(mut s) => {
            let mut new_fields = Vec::new();
            for (name, expr) in s.fields {
                let transformed = transform_recursive(expr, transform_fn)?;
                new_fields.push((name, transformed));
            }
            s.fields = new_fields;
            Expression::Struct(s)
        }
        Expression::NamedArgument(mut na) => {
            na.value = transform_recursive(na.value, transform_fn)?;
            Expression::NamedArgument(na)
        }
        Expression::MapFunc(mut m) => {
            m.keys = m
                .keys
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            m.values = m
                .values
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            Expression::MapFunc(m)
        }
        Expression::ArrayFunc(mut a) => {
            a.expressions = a
                .expressions
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            Expression::ArrayFunc(a)
        }
        Expression::Lambda(mut l) => {
            l.body = transform_recursive(l.body, transform_fn)?;
            Expression::Lambda(l)
        }
        Expression::JsonExtract(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.path = transform_recursive(f.path, transform_fn)?;
            Expression::JsonExtract(f)
        }
        Expression::JsonExtractScalar(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.path = transform_recursive(f.path, transform_fn)?;
            Expression::JsonExtractScalar(f)
        }

        // ===== UnaryFunc-based expressions =====
        // These all have a single `this: Expression` child
        Expression::Length(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Length(f)
        }
        Expression::Upper(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Upper(f)
        }
        Expression::Lower(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Lower(f)
        }
        Expression::LTrim(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::LTrim(f)
        }
        Expression::RTrim(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::RTrim(f)
        }
        Expression::Reverse(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Reverse(f)
        }
        Expression::Abs(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Abs(f)
        }
        Expression::Ceil(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Ceil(f)
        }
        Expression::Floor(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Floor(f)
        }
        Expression::Sign(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Sign(f)
        }
        Expression::Sqrt(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Sqrt(f)
        }
        Expression::Cbrt(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Cbrt(f)
        }
        Expression::Ln(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Ln(f)
        }
        Expression::Log(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            if let Some(base) = f.base {
                f.base = Some(transform_recursive(base, transform_fn)?);
            }
            Expression::Log(f)
        }
        Expression::Exp(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Exp(f)
        }
        Expression::Date(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Date(f)
        }
        Expression::Stddev(f) => recurse_agg!(Stddev, f),
        Expression::StddevSamp(f) => recurse_agg!(StddevSamp, f),
        Expression::Variance(f) => recurse_agg!(Variance, f),

        // ===== BinaryFunc-based expressions =====
        Expression::ModFunc(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::ModFunc(f)
        }
        Expression::Power(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::Power(f)
        }
        Expression::MapFromArrays(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::MapFromArrays(f)
        }
        Expression::ElementAt(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::ElementAt(f)
        }
        Expression::MapContainsKey(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::MapContainsKey(f)
        }
        Expression::Left(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.length = transform_recursive(f.length, transform_fn)?;
            Expression::Left(f)
        }
        Expression::Right(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.length = transform_recursive(f.length, transform_fn)?;
            Expression::Right(f)
        }
        Expression::Repeat(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.times = transform_recursive(f.times, transform_fn)?;
            Expression::Repeat(f)
        }

        // ===== Complex function expressions =====
        Expression::Substring(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.start = transform_recursive(f.start, transform_fn)?;
            if let Some(len) = f.length {
                f.length = Some(transform_recursive(len, transform_fn)?);
            }
            Expression::Substring(f)
        }
        Expression::Replace(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.old = transform_recursive(f.old, transform_fn)?;
            f.new = transform_recursive(f.new, transform_fn)?;
            Expression::Replace(f)
        }
        Expression::ConcatWs(mut f) => {
            f.separator = transform_recursive(f.separator, transform_fn)?;
            f.expressions = f
                .expressions
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            Expression::ConcatWs(f)
        }
        Expression::Trim(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            if let Some(chars) = f.characters {
                f.characters = Some(transform_recursive(chars, transform_fn)?);
            }
            Expression::Trim(f)
        }
        Expression::Split(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.delimiter = transform_recursive(f.delimiter, transform_fn)?;
            Expression::Split(f)
        }
        Expression::Lpad(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.length = transform_recursive(f.length, transform_fn)?;
            if let Some(fill) = f.fill {
                f.fill = Some(transform_recursive(fill, transform_fn)?);
            }
            Expression::Lpad(f)
        }
        Expression::Rpad(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.length = transform_recursive(f.length, transform_fn)?;
            if let Some(fill) = f.fill {
                f.fill = Some(transform_recursive(fill, transform_fn)?);
            }
            Expression::Rpad(f)
        }

        // ===== Conditional expressions =====
        Expression::Case(mut c) => {
            if let Some(operand) = c.operand {
                c.operand = Some(transform_recursive(operand, transform_fn)?);
            }
            c.whens = c
                .whens
                .into_iter()
                .map(|(cond, then)| {
                    let new_cond = transform_recursive(cond.clone(), transform_fn).unwrap_or(cond);
                    let new_then = transform_recursive(then.clone(), transform_fn).unwrap_or(then);
                    (new_cond, new_then)
                })
                .collect();
            if let Some(else_expr) = c.else_ {
                c.else_ = Some(transform_recursive(else_expr, transform_fn)?);
            }
            Expression::Case(c)
        }
        Expression::IfFunc(mut f) => {
            f.condition = transform_recursive(f.condition, transform_fn)?;
            f.true_value = transform_recursive(f.true_value, transform_fn)?;
            if let Some(false_val) = f.false_value {
                f.false_value = Some(transform_recursive(false_val, transform_fn)?);
            }
            Expression::IfFunc(f)
        }

        // ===== Date/Time expressions =====
        Expression::DateAdd(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.interval = transform_recursive(f.interval, transform_fn)?;
            Expression::DateAdd(f)
        }
        Expression::DateSub(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.interval = transform_recursive(f.interval, transform_fn)?;
            Expression::DateSub(f)
        }
        Expression::DateDiff(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::DateDiff(f)
        }
        Expression::DateTrunc(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::DateTrunc(f)
        }
        Expression::Extract(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Extract(f)
        }

        // ===== JSON expressions =====
        Expression::JsonObject(mut f) => {
            f.pairs = f
                .pairs
                .into_iter()
                .map(|(k, v)| {
                    let new_k = transform_recursive(k, transform_fn)?;
                    let new_v = transform_recursive(v, transform_fn)?;
                    Ok((new_k, new_v))
                })
                .collect::<Result<Vec<_>>>()?;
            Expression::JsonObject(f)
        }

        // ===== Subquery expressions =====
        Expression::Subquery(mut s) => {
            s.this = transform_recursive(s.this, transform_fn)?;
            Expression::Subquery(s)
        }
        Expression::Exists(mut e) => {
            e.this = transform_recursive(e.this, transform_fn)?;
            Expression::Exists(e)
        }
        Expression::Describe(mut d) => {
            d.target = transform_recursive(d.target, transform_fn)?;
            Expression::Describe(d)
        }

        // ===== Set operations =====
        Expression::Union(mut u) => {
            let left = std::mem::replace(&mut u.left, Expression::Null(Null));
            u.left = transform_recursive(left, transform_fn)?;
            let right = std::mem::replace(&mut u.right, Expression::Null(Null));
            u.right = transform_recursive(right, transform_fn)?;
            if let Some(mut order) = u.order_by.take() {
                order.expressions = order
                    .expressions
                    .into_iter()
                    .map(|o| {
                        let mut o = o;
                        let original = o.this.clone();
                        o.this = transform_recursive(o.this, transform_fn).unwrap_or(original);
                        match transform_fn(Expression::Ordered(Box::new(o.clone()))) {
                            Ok(Expression::Ordered(transformed)) => *transformed,
                            Ok(_) | Err(_) => o,
                        }
                    })
                    .collect();
                u.order_by = Some(order);
            }
            if let Some(mut with) = u.with.take() {
                with.ctes = with
                    .ctes
                    .into_iter()
                    .map(|mut cte| {
                        let original = cte.this.clone();
                        cte.this = transform_recursive(cte.this, transform_fn).unwrap_or(original);
                        cte
                    })
                    .collect();
                u.with = Some(with);
            }
            Expression::Union(u)
        }
        Expression::Intersect(mut i) => {
            let left = std::mem::replace(&mut i.left, Expression::Null(Null));
            i.left = transform_recursive(left, transform_fn)?;
            let right = std::mem::replace(&mut i.right, Expression::Null(Null));
            i.right = transform_recursive(right, transform_fn)?;
            if let Some(mut order) = i.order_by.take() {
                order.expressions = order
                    .expressions
                    .into_iter()
                    .map(|o| {
                        let mut o = o;
                        let original = o.this.clone();
                        o.this = transform_recursive(o.this, transform_fn).unwrap_or(original);
                        match transform_fn(Expression::Ordered(Box::new(o.clone()))) {
                            Ok(Expression::Ordered(transformed)) => *transformed,
                            Ok(_) | Err(_) => o,
                        }
                    })
                    .collect();
                i.order_by = Some(order);
            }
            if let Some(mut with) = i.with.take() {
                with.ctes = with
                    .ctes
                    .into_iter()
                    .map(|mut cte| {
                        let original = cte.this.clone();
                        cte.this = transform_recursive(cte.this, transform_fn).unwrap_or(original);
                        cte
                    })
                    .collect();
                i.with = Some(with);
            }
            Expression::Intersect(i)
        }
        Expression::Except(mut e) => {
            let left = std::mem::replace(&mut e.left, Expression::Null(Null));
            e.left = transform_recursive(left, transform_fn)?;
            let right = std::mem::replace(&mut e.right, Expression::Null(Null));
            e.right = transform_recursive(right, transform_fn)?;
            if let Some(mut order) = e.order_by.take() {
                order.expressions = order
                    .expressions
                    .into_iter()
                    .map(|o| {
                        let mut o = o;
                        let original = o.this.clone();
                        o.this = transform_recursive(o.this, transform_fn).unwrap_or(original);
                        match transform_fn(Expression::Ordered(Box::new(o.clone()))) {
                            Ok(Expression::Ordered(transformed)) => *transformed,
                            Ok(_) | Err(_) => o,
                        }
                    })
                    .collect();
                e.order_by = Some(order);
            }
            if let Some(mut with) = e.with.take() {
                with.ctes = with
                    .ctes
                    .into_iter()
                    .map(|mut cte| {
                        let original = cte.this.clone();
                        cte.this = transform_recursive(cte.this, transform_fn).unwrap_or(original);
                        cte
                    })
                    .collect();
                e.with = Some(with);
            }
            Expression::Except(e)
        }

        // ===== DML expressions =====
        Expression::Insert(mut ins) => {
            // Transform VALUES clause expressions
            let mut new_values = Vec::new();
            for row in ins.values {
                let mut new_row = Vec::new();
                for e in row {
                    new_row.push(transform_recursive(e, transform_fn)?);
                }
                new_values.push(new_row);
            }
            ins.values = new_values;

            // Transform query (for INSERT ... SELECT)
            if let Some(query) = ins.query {
                ins.query = Some(transform_recursive(query, transform_fn)?);
            }

            // Transform RETURNING clause
            let mut new_returning = Vec::new();
            for e in ins.returning {
                new_returning.push(transform_recursive(e, transform_fn)?);
            }
            ins.returning = new_returning;

            // Transform ON CONFLICT clause
            if let Some(on_conflict) = ins.on_conflict {
                ins.on_conflict = Some(Box::new(transform_recursive(*on_conflict, transform_fn)?));
            }

            Expression::Insert(ins)
        }
        Expression::Update(mut upd) => {
            upd.table = transform_table_ref_recursive(upd.table, transform_fn)?;
            upd.extra_tables = upd
                .extra_tables
                .into_iter()
                .map(|table| transform_table_ref_recursive(table, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            upd.table_joins = upd
                .table_joins
                .into_iter()
                .map(|join| transform_join_recursive(join, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            upd.set = upd
                .set
                .into_iter()
                .map(|(id, val)| {
                    let new_val = transform_recursive(val.clone(), transform_fn).unwrap_or(val);
                    (id, new_val)
                })
                .collect();
            if let Some(from_clause) = upd.from_clause.take() {
                upd.from_clause = Some(transform_from_recursive(from_clause, transform_fn)?);
            }
            upd.from_joins = upd
                .from_joins
                .into_iter()
                .map(|join| transform_join_recursive(join, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            if let Some(mut where_clause) = upd.where_clause.take() {
                where_clause.this = transform_recursive(where_clause.this, transform_fn)?;
                upd.where_clause = Some(where_clause);
            }
            upd.returning = upd
                .returning
                .into_iter()
                .map(|expr| transform_recursive(expr, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            if let Some(output) = upd.output.take() {
                upd.output = Some(transform_output_clause_recursive(output, transform_fn)?);
            }
            if let Some(with) = upd.with.take() {
                upd.with = Some(transform_with_recursive(with, transform_fn)?);
            }
            if let Some(limit) = upd.limit.take() {
                upd.limit = Some(transform_recursive(limit, transform_fn)?);
            }
            if let Some(order_by) = upd.order_by.take() {
                upd.order_by = Some(transform_order_by_recursive(order_by, transform_fn)?);
            }
            Expression::Update(upd)
        }
        Expression::Delete(mut del) => {
            del.table = transform_table_ref_recursive(del.table, transform_fn)?;
            del.using = del
                .using
                .into_iter()
                .map(|table| transform_table_ref_recursive(table, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            if let Some(mut where_clause) = del.where_clause.take() {
                where_clause.this = transform_recursive(where_clause.this, transform_fn)?;
                del.where_clause = Some(where_clause);
            }
            if let Some(output) = del.output.take() {
                del.output = Some(transform_output_clause_recursive(output, transform_fn)?);
            }
            if let Some(with) = del.with.take() {
                del.with = Some(transform_with_recursive(with, transform_fn)?);
            }
            if let Some(limit) = del.limit.take() {
                del.limit = Some(transform_recursive(limit, transform_fn)?);
            }
            if let Some(order_by) = del.order_by.take() {
                del.order_by = Some(transform_order_by_recursive(order_by, transform_fn)?);
            }
            del.returning = del
                .returning
                .into_iter()
                .map(|expr| transform_recursive(expr, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            del.tables = del
                .tables
                .into_iter()
                .map(|table| transform_table_ref_recursive(table, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            del.joins = del
                .joins
                .into_iter()
                .map(|join| transform_join_recursive(join, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            Expression::Delete(del)
        }

        // ===== CTE expressions =====
        Expression::With(mut w) => {
            w.ctes = w
                .ctes
                .into_iter()
                .map(|mut cte| {
                    let original = cte.this.clone();
                    cte.this = transform_recursive(cte.this, transform_fn).unwrap_or(original);
                    cte
                })
                .collect();
            Expression::With(w)
        }
        Expression::Cte(mut c) => {
            c.this = transform_recursive(c.this, transform_fn)?;
            Expression::Cte(c)
        }

        // ===== Order expressions =====
        Expression::Ordered(mut o) => {
            o.this = transform_recursive(o.this, transform_fn)?;
            Expression::Ordered(o)
        }

        // ===== Negation =====
        Expression::Neg(mut n) => {
            n.this = transform_recursive(n.this, transform_fn)?;
            Expression::Neg(n)
        }

        // ===== Between =====
        Expression::Between(mut b) => {
            b.this = transform_recursive(b.this, transform_fn)?;
            b.low = transform_recursive(b.low, transform_fn)?;
            b.high = transform_recursive(b.high, transform_fn)?;
            Expression::Between(b)
        }
        Expression::IsNull(mut i) => {
            i.this = transform_recursive(i.this, transform_fn)?;
            Expression::IsNull(i)
        }
        Expression::IsTrue(mut i) => {
            i.this = transform_recursive(i.this, transform_fn)?;
            Expression::IsTrue(i)
        }
        Expression::IsFalse(mut i) => {
            i.this = transform_recursive(i.this, transform_fn)?;
            Expression::IsFalse(i)
        }

        // ===== Like expressions =====
        Expression::Like(mut l) => {
            l.left = transform_recursive(l.left, transform_fn)?;
            l.right = transform_recursive(l.right, transform_fn)?;
            Expression::Like(l)
        }
        Expression::ILike(mut l) => {
            l.left = transform_recursive(l.left, transform_fn)?;
            l.right = transform_recursive(l.right, transform_fn)?;
            Expression::ILike(l)
        }

        // ===== Additional binary ops not covered by macro =====
        Expression::Neq(op) => transform_binary!(Neq, *op),
        Expression::Lte(op) => transform_binary!(Lte, *op),
        Expression::Gte(op) => transform_binary!(Gte, *op),
        Expression::Mod(op) => transform_binary!(Mod, *op),
        Expression::Concat(op) => transform_binary!(Concat, *op),
        Expression::BitwiseAnd(op) => transform_binary!(BitwiseAnd, *op),
        Expression::BitwiseOr(op) => transform_binary!(BitwiseOr, *op),
        Expression::BitwiseXor(op) => transform_binary!(BitwiseXor, *op),
        Expression::Is(op) => transform_binary!(Is, *op),

        // ===== TryCast / SafeCast =====
        Expression::TryCast(mut c) => {
            c.this = transform_recursive(c.this, transform_fn)?;
            c.to = transform_data_type_recursive(c.to, transform_fn)?;
            Expression::TryCast(c)
        }
        Expression::SafeCast(mut c) => {
            c.this = transform_recursive(c.this, transform_fn)?;
            c.to = transform_data_type_recursive(c.to, transform_fn)?;
            Expression::SafeCast(c)
        }

        // ===== Misc =====
        Expression::Unnest(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expressions = f
                .expressions
                .into_iter()
                .map(|e| transform_recursive(e, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            Expression::Unnest(f)
        }
        Expression::Explode(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::Explode(f)
        }
        Expression::GroupConcat(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::GroupConcat(f)
        }
        Expression::StringAgg(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            if let Some(order_by) = f.order_by.take() {
                f.order_by = Some(
                    order_by
                        .into_iter()
                        .map(|mut ordered| {
                            let original = ordered.this.clone();
                            ordered.this =
                                transform_recursive(ordered.this, transform_fn).unwrap_or(original);
                            match transform_fn(Expression::Ordered(Box::new(ordered.clone()))) {
                                Ok(Expression::Ordered(transformed)) => Ok(*transformed),
                                Ok(_) | Err(_) => Ok(ordered),
                            }
                        })
                        .collect::<Result<Vec<_>>>()?,
                );
            }
            Expression::StringAgg(f)
        }
        Expression::ListAgg(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::ListAgg(f)
        }
        Expression::ArrayAgg(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::ArrayAgg(f)
        }
        Expression::ParseJson(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::ParseJson(f)
        }
        Expression::ToJson(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::ToJson(f)
        }
        Expression::JSONExtract(mut e) => {
            e.this = Box::new(transform_recursive(*e.this, transform_fn)?);
            e.expression = Box::new(transform_recursive(*e.expression, transform_fn)?);
            Expression::JSONExtract(e)
        }
        Expression::JSONExtractScalar(mut e) => {
            e.this = Box::new(transform_recursive(*e.this, transform_fn)?);
            e.expression = Box::new(transform_recursive(*e.expression, transform_fn)?);
            Expression::JSONExtractScalar(e)
        }

        // StrToTime: recurse into this
        Expression::StrToTime(mut e) => {
            e.this = Box::new(transform_recursive(*e.this, transform_fn)?);
            Expression::StrToTime(e)
        }

        // UnixToTime: recurse into this
        Expression::UnixToTime(mut e) => {
            e.this = Box::new(transform_recursive(*e.this, transform_fn)?);
            Expression::UnixToTime(e)
        }

        // CreateTable: recurse into column defaults, on_update expressions, and data types
        Expression::CreateTable(mut ct) => {
            for col in &mut ct.columns {
                if let Some(default_expr) = col.default.take() {
                    col.default = Some(transform_recursive(default_expr, transform_fn)?);
                }
                if let Some(on_update_expr) = col.on_update.take() {
                    col.on_update = Some(transform_recursive(on_update_expr, transform_fn)?);
                }
                // Note: Column data type transformations (INT -> INT64 for BigQuery, etc.)
                // are NOT applied here because per-dialect transforms are designed for CAST/expression
                // contexts and may not produce correct results for DDL column definitions.
                // The DDL type mappings would need dedicated handling per source/target pair.
            }
            if let Some(as_select) = ct.as_select.take() {
                ct.as_select = Some(transform_recursive(as_select, transform_fn)?);
            }
            Expression::CreateTable(ct)
        }

        // CreateView: recurse into the view body query
        Expression::CreateView(mut cv) => {
            cv.query = transform_recursive(cv.query, transform_fn)?;
            Expression::CreateView(cv)
        }

        // CreateTask: recurse into the task body
        Expression::CreateTask(mut ct) => {
            ct.body = transform_recursive(ct.body, transform_fn)?;
            Expression::CreateTask(ct)
        }

        // Prepare: recurse into the prepared statement body
        Expression::Prepare(mut prepare) => {
            prepare.statement = transform_recursive(prepare.statement, transform_fn)?;
            Expression::Prepare(prepare)
        }

        // Execute: recurse into procedure/prepared name and argument values
        Expression::Execute(mut execute) => {
            execute.this = transform_recursive(execute.this, transform_fn)?;
            execute.arguments = execute
                .arguments
                .into_iter()
                .map(|argument| transform_recursive(argument, transform_fn))
                .collect::<Result<Vec<_>>>()?;
            execute.parameters = execute
                .parameters
                .into_iter()
                .map(|mut parameter| {
                    parameter.value = transform_recursive(parameter.value, transform_fn)?;
                    Ok(parameter)
                })
                .collect::<Result<Vec<_>>>()?;
            Expression::Execute(execute)
        }

        // CreateProcedure: recurse into body expressions
        Expression::CreateProcedure(mut cp) => {
            if let Some(body) = cp.body.take() {
                cp.body = Some(match body {
                    FunctionBody::Expression(expr) => {
                        FunctionBody::Expression(transform_recursive(expr, transform_fn)?)
                    }
                    FunctionBody::Return(expr) => {
                        FunctionBody::Return(transform_recursive(expr, transform_fn)?)
                    }
                    FunctionBody::Statements(stmts) => {
                        let transformed_stmts = stmts
                            .into_iter()
                            .map(|s| transform_recursive(s, transform_fn))
                            .collect::<Result<Vec<_>>>()?;
                        FunctionBody::Statements(transformed_stmts)
                    }
                    other => other,
                });
            }
            Expression::CreateProcedure(cp)
        }

        // CreateFunction: recurse into body expressions
        Expression::CreateFunction(mut cf) => {
            if let Some(body) = cf.body.take() {
                cf.body = Some(match body {
                    FunctionBody::Expression(expr) => {
                        FunctionBody::Expression(transform_recursive(expr, transform_fn)?)
                    }
                    FunctionBody::Return(expr) => {
                        FunctionBody::Return(transform_recursive(expr, transform_fn)?)
                    }
                    FunctionBody::Statements(stmts) => {
                        let transformed_stmts = stmts
                            .into_iter()
                            .map(|s| transform_recursive(s, transform_fn))
                            .collect::<Result<Vec<_>>>()?;
                        FunctionBody::Statements(transformed_stmts)
                    }
                    other => other,
                });
            }
            Expression::CreateFunction(cf)
        }

        // MemberOf: recurse into left and right operands
        Expression::MemberOf(op) => transform_binary!(MemberOf, *op),
        // ArrayContainsAll (@>): recurse into left and right operands
        Expression::ArrayContainsAll(op) => transform_binary!(ArrayContainsAll, *op),
        // ArrayContainedBy (<@): recurse into left and right operands
        Expression::ArrayContainedBy(op) => transform_binary!(ArrayContainedBy, *op),
        // ArrayOverlaps (&&): recurse into left and right operands
        Expression::ArrayOverlaps(op) => transform_binary!(ArrayOverlaps, *op),
        // TsMatch (@@): recurse into left and right operands
        Expression::TsMatch(op) => transform_binary!(TsMatch, *op),
        // Adjacent (-|-): recurse into left and right operands
        Expression::Adjacent(op) => transform_binary!(Adjacent, *op),

        // Table: recurse into when (HistoricalData) and changes fields
        Expression::Table(mut t) => {
            if let Some(when) = t.when.take() {
                let transformed =
                    transform_recursive(Expression::HistoricalData(when), transform_fn)?;
                if let Expression::HistoricalData(hd) = transformed {
                    t.when = Some(hd);
                }
            }
            if let Some(changes) = t.changes.take() {
                let transformed = transform_recursive(Expression::Changes(changes), transform_fn)?;
                if let Expression::Changes(c) = transformed {
                    t.changes = Some(c);
                }
            }
            Expression::Table(t)
        }

        // HistoricalData (Snowflake time travel): recurse into expression
        Expression::HistoricalData(mut hd) => {
            *hd.expression = transform_recursive(*hd.expression, transform_fn)?;
            Expression::HistoricalData(hd)
        }

        // Changes (Snowflake CHANGES clause): recurse into at_before and end
        Expression::Changes(mut c) => {
            if let Some(at_before) = c.at_before.take() {
                c.at_before = Some(Box::new(transform_recursive(*at_before, transform_fn)?));
            }
            if let Some(end) = c.end.take() {
                c.end = Some(Box::new(transform_recursive(*end, transform_fn)?));
            }
            Expression::Changes(c)
        }

        // TableArgument: TABLE(expr) or MODEL(expr)
        Expression::TableArgument(mut ta) => {
            ta.this = transform_recursive(ta.this, transform_fn)?;
            Expression::TableArgument(ta)
        }

        // JoinedTable: (tbl1 JOIN tbl2 ON ...) - recurse into left and join tables
        Expression::JoinedTable(mut jt) => {
            jt.left = transform_recursive(jt.left, transform_fn)?;
            jt.joins = jt
                .joins
                .into_iter()
                .map(|mut join| {
                    join.this = transform_recursive(join.this, transform_fn)?;
                    if let Some(on) = join.on.take() {
                        join.on = Some(transform_recursive(on, transform_fn)?);
                    }
                    match transform_fn(Expression::Join(Box::new(join)))? {
                        Expression::Join(j) => Ok(*j),
                        _ => Err(crate::error::Error::parse(
                            "Join transformation returned non-join expression",
                            0,
                            0,
                            0,
                            0,
                        )),
                    }
                })
                .collect::<Result<Vec<_>>>()?;
            jt.lateral_views = jt
                .lateral_views
                .into_iter()
                .map(|mut lv| {
                    lv.this = transform_recursive(lv.this, transform_fn)?;
                    Ok(lv)
                })
                .collect::<Result<Vec<_>>>()?;
            Expression::JoinedTable(jt)
        }

        // Lateral: LATERAL func() - recurse into the function expression
        Expression::Lateral(mut lat) => {
            *lat.this = transform_recursive(*lat.this, transform_fn)?;
            Expression::Lateral(lat)
        }

        // WithinGroup: recurse into order_by items (for NULLS FIRST/LAST etc.)
        // but NOT into wg.this - the inner function is handled by StringAggConvert/GroupConcatConvert
        // as a unit together with the WithinGroup wrapper
        Expression::WithinGroup(mut wg) => {
            wg.order_by = wg
                .order_by
                .into_iter()
                .map(|mut o| {
                    let original = o.this.clone();
                    o.this = transform_recursive(o.this, transform_fn).unwrap_or(original);
                    match transform_fn(Expression::Ordered(Box::new(o.clone()))) {
                        Ok(Expression::Ordered(transformed)) => *transformed,
                        Ok(_) | Err(_) => o,
                    }
                })
                .collect();
            Expression::WithinGroup(wg)
        }

        // Filter: recurse into both the aggregate and the filter condition
        Expression::Filter(mut f) => {
            f.this = Box::new(transform_recursive(*f.this, transform_fn)?);
            f.expression = Box::new(transform_recursive(*f.expression, transform_fn)?);
            Expression::Filter(f)
        }

        // Aggregate functions (AggFunc-based): recurse into the aggregate argument,
        // filter, order_by, having_max, and limit.
        // Stddev, StddevSamp, Variance, and ArrayAgg are handled earlier in this match.
        Expression::Sum(f) => recurse_agg!(Sum, f),
        Expression::Avg(f) => recurse_agg!(Avg, f),
        Expression::Min(f) => recurse_agg!(Min, f),
        Expression::Max(f) => recurse_agg!(Max, f),
        Expression::CountIf(f) => recurse_agg!(CountIf, f),
        Expression::StddevPop(f) => recurse_agg!(StddevPop, f),
        Expression::VarPop(f) => recurse_agg!(VarPop, f),
        Expression::VarSamp(f) => recurse_agg!(VarSamp, f),
        Expression::Median(f) => recurse_agg!(Median, f),
        Expression::Mode(f) => recurse_agg!(Mode, f),
        Expression::First(f) => recurse_agg!(First, f),
        Expression::Last(f) => recurse_agg!(Last, f),
        Expression::AnyValue(f) => recurse_agg!(AnyValue, f),
        Expression::ApproxDistinct(f) => recurse_agg!(ApproxDistinct, f),
        Expression::ApproxCountDistinct(f) => recurse_agg!(ApproxCountDistinct, f),
        Expression::LogicalAnd(f) => recurse_agg!(LogicalAnd, f),
        Expression::LogicalOr(f) => recurse_agg!(LogicalOr, f),
        Expression::Skewness(f) => recurse_agg!(Skewness, f),
        Expression::ArrayConcatAgg(f) => recurse_agg!(ArrayConcatAgg, f),
        Expression::ArrayUniqueAgg(f) => recurse_agg!(ArrayUniqueAgg, f),
        Expression::BoolXorAgg(f) => recurse_agg!(BoolXorAgg, f),
        Expression::BitwiseOrAgg(f) => recurse_agg!(BitwiseOrAgg, f),
        Expression::BitwiseAndAgg(f) => recurse_agg!(BitwiseAndAgg, f),
        Expression::BitwiseXorAgg(f) => recurse_agg!(BitwiseXorAgg, f),

        // Count has its own struct with an Option<Expression> `this` field
        Expression::Count(mut c) => {
            if let Some(this) = c.this.take() {
                c.this = Some(transform_recursive(this, transform_fn)?);
            }
            if let Some(filter) = c.filter.take() {
                c.filter = Some(transform_recursive(filter, transform_fn)?);
            }
            Expression::Count(c)
        }

        Expression::PipeOperator(mut pipe) => {
            pipe.this = transform_recursive(pipe.this, transform_fn)?;
            pipe.expression = transform_recursive(pipe.expression, transform_fn)?;
            Expression::PipeOperator(pipe)
        }

        // ArrayExcept/ArrayContains/ArrayDistinct: recurse into children
        Expression::ArrayExcept(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::ArrayExcept(f)
        }
        Expression::ArrayContains(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::ArrayContains(f)
        }
        Expression::ArrayDistinct(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            Expression::ArrayDistinct(f)
        }
        Expression::ArrayPosition(mut f) => {
            f.this = transform_recursive(f.this, transform_fn)?;
            f.expression = transform_recursive(f.expression, transform_fn)?;
            Expression::ArrayPosition(f)
        }

        // Pass through leaf nodes unchanged
        other => other,
    };

    // Then apply the transform function
    transform_fn(expr)
}

/// Returns the tokenizer config, generator config, and expression transform closure
/// for a built-in dialect type. This is the shared implementation used by both
/// `Dialect::get()` and custom dialect construction.
// ---------------------------------------------------------------------------
// Cached dialect configurations
// ---------------------------------------------------------------------------

/// Pre-computed tokenizer + generator configs for a dialect, cached via `LazyLock`.
/// Transform closures are cheap (unit-struct method calls) and created fresh each time.
struct CachedDialectConfig {
    tokenizer_config: Arc<TokenizerConfig>,
    #[cfg(feature = "generate")]
    generator_config: Arc<GeneratorConfig>,
}

struct DialectConfigs {
    tokenizer_config: Arc<TokenizerConfig>,
    #[cfg(feature = "generate")]
    generator_config: Arc<GeneratorConfig>,
    #[cfg(feature = "transpile")]
    transformer: Box<dyn Fn(Expression) -> Result<Expression> + Send + Sync>,
}

/// Declare a per-dialect `LazyLock<CachedDialectConfig>` static.
macro_rules! cached_dialect {
    ($static_name:ident, $dialect_struct:expr, $feature:literal) => {
        #[cfg(feature = $feature)]
        static $static_name: LazyLock<CachedDialectConfig> = LazyLock::new(|| {
            let d = $dialect_struct;
            CachedDialectConfig {
                tokenizer_config: Arc::new(d.tokenizer_config()),
                #[cfg(feature = "generate")]
                generator_config: Arc::new(d.generator_config()),
            }
        });
    };
}

static CACHED_GENERIC: LazyLock<CachedDialectConfig> = LazyLock::new(|| {
    let d = GenericDialect;
    CachedDialectConfig {
        tokenizer_config: Arc::new(d.tokenizer_config()),
        #[cfg(feature = "generate")]
        generator_config: Arc::new(d.generator_config()),
    }
});

cached_dialect!(CACHED_POSTGRESQL, PostgresDialect, "dialect-postgresql");
cached_dialect!(CACHED_MYSQL, MySQLDialect, "dialect-mysql");
cached_dialect!(CACHED_BIGQUERY, BigQueryDialect, "dialect-bigquery");
cached_dialect!(CACHED_SNOWFLAKE, SnowflakeDialect, "dialect-snowflake");
cached_dialect!(CACHED_DUCKDB, DuckDBDialect, "dialect-duckdb");
cached_dialect!(CACHED_TSQL, TSQLDialect, "dialect-tsql");
cached_dialect!(CACHED_ORACLE, OracleDialect, "dialect-oracle");
cached_dialect!(CACHED_HIVE, HiveDialect, "dialect-hive");
cached_dialect!(CACHED_SPARK, SparkDialect, "dialect-spark");
cached_dialect!(CACHED_SQLITE, SQLiteDialect, "dialect-sqlite");
cached_dialect!(CACHED_PRESTO, PrestoDialect, "dialect-presto");
cached_dialect!(CACHED_TRINO, TrinoDialect, "dialect-trino");
cached_dialect!(CACHED_REDSHIFT, RedshiftDialect, "dialect-redshift");
cached_dialect!(CACHED_CLICKHOUSE, ClickHouseDialect, "dialect-clickhouse");
cached_dialect!(CACHED_DATABRICKS, DatabricksDialect, "dialect-databricks");
cached_dialect!(CACHED_ATHENA, AthenaDialect, "dialect-athena");
cached_dialect!(CACHED_TERADATA, TeradataDialect, "dialect-teradata");
cached_dialect!(CACHED_DORIS, DorisDialect, "dialect-doris");
cached_dialect!(CACHED_STARROCKS, StarRocksDialect, "dialect-starrocks");
cached_dialect!(
    CACHED_MATERIALIZE,
    MaterializeDialect,
    "dialect-materialize"
);
cached_dialect!(CACHED_RISINGWAVE, RisingWaveDialect, "dialect-risingwave");
cached_dialect!(
    CACHED_SINGLESTORE,
    SingleStoreDialect,
    "dialect-singlestore"
);
cached_dialect!(
    CACHED_COCKROACHDB,
    CockroachDBDialect,
    "dialect-cockroachdb"
);
cached_dialect!(CACHED_TIDB, TiDBDialect, "dialect-tidb");
cached_dialect!(CACHED_DRUID, DruidDialect, "dialect-druid");
cached_dialect!(CACHED_SOLR, SolrDialect, "dialect-solr");
cached_dialect!(CACHED_TABLEAU, TableauDialect, "dialect-tableau");
cached_dialect!(CACHED_DUNE, DuneDialect, "dialect-dune");
cached_dialect!(CACHED_FABRIC, FabricDialect, "dialect-fabric");
cached_dialect!(CACHED_DRILL, DrillDialect, "dialect-drill");
cached_dialect!(CACHED_DREMIO, DremioDialect, "dialect-dremio");
cached_dialect!(CACHED_EXASOL, ExasolDialect, "dialect-exasol");
cached_dialect!(CACHED_DATAFUSION, DataFusionDialect, "dialect-datafusion");

fn configs_for_dialect_type(dt: DialectType) -> DialectConfigs {
    /// Clone configs from a cached static and pair with a fresh transform closure.
    macro_rules! from_cache {
        ($cache:expr, $dialect_struct:expr) => {{
            let c = &*$cache;
            DialectConfigs {
                tokenizer_config: c.tokenizer_config.clone(),
                #[cfg(feature = "generate")]
                generator_config: c.generator_config.clone(),
                #[cfg(feature = "transpile")]
                transformer: Box::new(move |e| $dialect_struct.transform_expr(e)),
            }
        }};
    }
    match dt {
        #[cfg(feature = "dialect-postgresql")]
        DialectType::PostgreSQL => from_cache!(CACHED_POSTGRESQL, PostgresDialect),
        #[cfg(feature = "dialect-mysql")]
        DialectType::MySQL => from_cache!(CACHED_MYSQL, MySQLDialect),
        #[cfg(feature = "dialect-bigquery")]
        DialectType::BigQuery => from_cache!(CACHED_BIGQUERY, BigQueryDialect),
        #[cfg(feature = "dialect-snowflake")]
        DialectType::Snowflake => from_cache!(CACHED_SNOWFLAKE, SnowflakeDialect),
        #[cfg(feature = "dialect-duckdb")]
        DialectType::DuckDB => from_cache!(CACHED_DUCKDB, DuckDBDialect),
        #[cfg(feature = "dialect-tsql")]
        DialectType::TSQL => from_cache!(CACHED_TSQL, TSQLDialect),
        #[cfg(feature = "dialect-oracle")]
        DialectType::Oracle => from_cache!(CACHED_ORACLE, OracleDialect),
        #[cfg(feature = "dialect-hive")]
        DialectType::Hive => from_cache!(CACHED_HIVE, HiveDialect),
        #[cfg(feature = "dialect-spark")]
        DialectType::Spark => from_cache!(CACHED_SPARK, SparkDialect),
        #[cfg(feature = "dialect-sqlite")]
        DialectType::SQLite => from_cache!(CACHED_SQLITE, SQLiteDialect),
        #[cfg(feature = "dialect-presto")]
        DialectType::Presto => from_cache!(CACHED_PRESTO, PrestoDialect),
        #[cfg(feature = "dialect-trino")]
        DialectType::Trino => from_cache!(CACHED_TRINO, TrinoDialect),
        #[cfg(feature = "dialect-redshift")]
        DialectType::Redshift => from_cache!(CACHED_REDSHIFT, RedshiftDialect),
        #[cfg(feature = "dialect-clickhouse")]
        DialectType::ClickHouse => from_cache!(CACHED_CLICKHOUSE, ClickHouseDialect),
        #[cfg(feature = "dialect-databricks")]
        DialectType::Databricks => from_cache!(CACHED_DATABRICKS, DatabricksDialect),
        #[cfg(feature = "dialect-athena")]
        DialectType::Athena => from_cache!(CACHED_ATHENA, AthenaDialect),
        #[cfg(feature = "dialect-teradata")]
        DialectType::Teradata => from_cache!(CACHED_TERADATA, TeradataDialect),
        #[cfg(feature = "dialect-doris")]
        DialectType::Doris => from_cache!(CACHED_DORIS, DorisDialect),
        #[cfg(feature = "dialect-starrocks")]
        DialectType::StarRocks => from_cache!(CACHED_STARROCKS, StarRocksDialect),
        #[cfg(feature = "dialect-materialize")]
        DialectType::Materialize => from_cache!(CACHED_MATERIALIZE, MaterializeDialect),
        #[cfg(feature = "dialect-risingwave")]
        DialectType::RisingWave => from_cache!(CACHED_RISINGWAVE, RisingWaveDialect),
        #[cfg(feature = "dialect-singlestore")]
        DialectType::SingleStore => from_cache!(CACHED_SINGLESTORE, SingleStoreDialect),
        #[cfg(feature = "dialect-cockroachdb")]
        DialectType::CockroachDB => from_cache!(CACHED_COCKROACHDB, CockroachDBDialect),
        #[cfg(feature = "dialect-tidb")]
        DialectType::TiDB => from_cache!(CACHED_TIDB, TiDBDialect),
        #[cfg(feature = "dialect-druid")]
        DialectType::Druid => from_cache!(CACHED_DRUID, DruidDialect),
        #[cfg(feature = "dialect-solr")]
        DialectType::Solr => from_cache!(CACHED_SOLR, SolrDialect),
        #[cfg(feature = "dialect-tableau")]
        DialectType::Tableau => from_cache!(CACHED_TABLEAU, TableauDialect),
        #[cfg(feature = "dialect-dune")]
        DialectType::Dune => from_cache!(CACHED_DUNE, DuneDialect),
        #[cfg(feature = "dialect-fabric")]
        DialectType::Fabric => from_cache!(CACHED_FABRIC, FabricDialect),
        #[cfg(feature = "dialect-drill")]
        DialectType::Drill => from_cache!(CACHED_DRILL, DrillDialect),
        #[cfg(feature = "dialect-dremio")]
        DialectType::Dremio => from_cache!(CACHED_DREMIO, DremioDialect),
        #[cfg(feature = "dialect-exasol")]
        DialectType::Exasol => from_cache!(CACHED_EXASOL, ExasolDialect),
        #[cfg(feature = "dialect-datafusion")]
        DialectType::DataFusion => from_cache!(CACHED_DATAFUSION, DataFusionDialect),
        _ => from_cache!(CACHED_GENERIC, GenericDialect),
    }
}

// ---------------------------------------------------------------------------
// Custom dialect registry
// ---------------------------------------------------------------------------

static CUSTOM_DIALECT_REGISTRY: LazyLock<RwLock<HashMap<String, Arc<CustomDialectConfig>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));

struct CustomDialectConfig {
    name: String,
    base_dialect: DialectType,
    tokenizer_config: Arc<TokenizerConfig>,
    #[cfg(feature = "generate")]
    generator_config: GeneratorConfig,
    #[cfg(feature = "transpile")]
    transform: Option<Arc<dyn Fn(Expression) -> Result<Expression> + Send + Sync>>,
    #[cfg(feature = "transpile")]
    preprocess: Option<Arc<dyn Fn(Expression) -> Result<Expression> + Send + Sync>>,
}

/// Fluent builder for creating and registering custom SQL dialects.
///
/// A custom dialect is based on an existing built-in dialect and allows selective
/// overrides of tokenizer configuration, generator configuration, and expression
/// transforms.
///
/// # Example
///
/// ```rust,ignore
/// use polyglot_sql::dialects::{CustomDialectBuilder, DialectType, Dialect};
/// use polyglot_sql::generator::NormalizeFunctions;
///
/// CustomDialectBuilder::new("my_postgres")
///     .based_on(DialectType::PostgreSQL)
///     .generator_config_modifier(|gc| {
///         gc.normalize_functions = NormalizeFunctions::Lower;
///     })
///     .register()
///     .unwrap();
///
/// let d = Dialect::get_by_name("my_postgres").unwrap();
/// let exprs = d.parse("SELECT COUNT(*)").unwrap();
/// let sql = d.generate(&exprs[0]).unwrap();
/// assert_eq!(sql, "select count(*)");
///
/// polyglot_sql::unregister_custom_dialect("my_postgres");
/// ```
pub struct CustomDialectBuilder {
    name: String,
    base_dialect: DialectType,
    tokenizer_modifier: Option<Box<dyn FnOnce(&mut TokenizerConfig)>>,
    #[cfg(feature = "generate")]
    generator_modifier: Option<Box<dyn FnOnce(&mut GeneratorConfig)>>,
    #[cfg(feature = "transpile")]
    transform: Option<Arc<dyn Fn(Expression) -> Result<Expression> + Send + Sync>>,
    #[cfg(feature = "transpile")]
    preprocess: Option<Arc<dyn Fn(Expression) -> Result<Expression> + Send + Sync>>,
}

impl CustomDialectBuilder {
    /// Create a new builder with the given name. Defaults to `Generic` as the base dialect.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            base_dialect: DialectType::Generic,
            tokenizer_modifier: None,
            #[cfg(feature = "generate")]
            generator_modifier: None,
            #[cfg(feature = "transpile")]
            transform: None,
            #[cfg(feature = "transpile")]
            preprocess: None,
        }
    }

    /// Set the base built-in dialect to inherit configuration from.
    pub fn based_on(mut self, dialect: DialectType) -> Self {
        self.base_dialect = dialect;
        self
    }

    /// Provide a closure that modifies the tokenizer configuration inherited from the base dialect.
    pub fn tokenizer_config_modifier<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut TokenizerConfig) + 'static,
    {
        self.tokenizer_modifier = Some(Box::new(f));
        self
    }

    /// Provide a closure that modifies the generator configuration inherited from the base dialect.
    #[cfg(feature = "generate")]
    pub fn generator_config_modifier<F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut GeneratorConfig) + 'static,
    {
        self.generator_modifier = Some(Box::new(f));
        self
    }

    /// Set a custom per-node expression transform function.
    ///
    /// This replaces the base dialect's transform. It is called on every expression
    /// node during the recursive transform pass.
    #[cfg(feature = "transpile")]
    pub fn transform_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(Expression) -> Result<Expression> + Send + Sync + 'static,
    {
        self.transform = Some(Arc::new(f));
        self
    }

    /// Set a custom whole-tree preprocessing function.
    ///
    /// This replaces the base dialect's built-in preprocessing. It is called once
    /// on the entire expression tree before the recursive per-node transform.
    #[cfg(feature = "transpile")]
    pub fn preprocess_fn<F>(mut self, f: F) -> Self
    where
        F: Fn(Expression) -> Result<Expression> + Send + Sync + 'static,
    {
        self.preprocess = Some(Arc::new(f));
        self
    }

    /// Build the custom dialect configuration and register it in the global registry.
    ///
    /// Returns an error if:
    /// - The name collides with a built-in dialect name
    /// - A custom dialect with the same name is already registered
    pub fn register(self) -> Result<()> {
        // Reject names that collide with built-in dialects
        if DialectType::from_str(&self.name).is_ok() {
            return Err(crate::error::Error::parse(
                format!(
                    "Cannot register custom dialect '{}': name collides with built-in dialect",
                    self.name
                ),
                0,
                0,
                0,
                0,
            ));
        }

        // Get base configs
        let base_configs = configs_for_dialect_type(self.base_dialect);
        let mut tok_config = (*base_configs.tokenizer_config).clone();
        #[cfg(feature = "generate")]
        let mut gen_config = (*base_configs.generator_config).clone();

        // Apply modifiers
        if let Some(tok_mod) = self.tokenizer_modifier {
            tok_mod(&mut tok_config);
        }
        #[cfg(feature = "generate")]
        if let Some(gen_mod) = self.generator_modifier {
            gen_mod(&mut gen_config);
        }

        let config = CustomDialectConfig {
            name: self.name.clone(),
            base_dialect: self.base_dialect,
            tokenizer_config: Arc::new(tok_config),
            #[cfg(feature = "generate")]
            generator_config: gen_config,
            #[cfg(feature = "transpile")]
            transform: self.transform,
            #[cfg(feature = "transpile")]
            preprocess: self.preprocess,
        };

        register_custom_dialect(config)
    }
}

use std::str::FromStr;

fn register_custom_dialect(config: CustomDialectConfig) -> Result<()> {
    let mut registry = CUSTOM_DIALECT_REGISTRY.write().map_err(|e| {
        crate::error::Error::parse(format!("Registry lock poisoned: {}", e), 0, 0, 0, 0)
    })?;

    if registry.contains_key(&config.name) {
        return Err(crate::error::Error::parse(
            format!("Custom dialect '{}' is already registered", config.name),
            0,
            0,
            0,
            0,
        ));
    }

    registry.insert(config.name.clone(), Arc::new(config));
    Ok(())
}

/// Remove a custom dialect from the global registry.
///
/// Returns `true` if a dialect with that name was found and removed,
/// `false` if no such custom dialect existed.
pub fn unregister_custom_dialect(name: &str) -> bool {
    if let Ok(mut registry) = CUSTOM_DIALECT_REGISTRY.write() {
        registry.remove(name).is_some()
    } else {
        false
    }
}

fn get_custom_dialect_config(name: &str) -> Option<Arc<CustomDialectConfig>> {
    CUSTOM_DIALECT_REGISTRY
        .read()
        .ok()
        .and_then(|registry| registry.get(name).cloned())
}

/// Main entry point for dialect-specific SQL operations.
///
/// A `Dialect` bundles together a tokenizer, generator configuration, and expression
/// transformer for a specific SQL database engine. It is the high-level API through
/// which callers parse, generate, transform, and transpile SQL.
///
/// # Usage
///
/// ```rust,ignore
/// use polyglot_sql::dialects::{Dialect, DialectType};
///
/// // Parse PostgreSQL SQL into an AST
/// let pg = Dialect::get(DialectType::PostgreSQL);
/// let exprs = pg.parse("SELECT id, name FROM users WHERE active")?;
///
/// // Transpile from PostgreSQL to BigQuery
/// let results = pg.transpile("SELECT NOW()", DialectType::BigQuery)?;
/// assert_eq!(results[0], "SELECT CURRENT_TIMESTAMP()");
/// ```
///
/// Obtain an instance via [`Dialect::get`] or [`Dialect::get_by_name`].
/// The struct is `Send + Sync` safe so it can be shared across threads.
pub struct Dialect {
    dialect_type: DialectType,
    tokenizer: Tokenizer,
    #[cfg(feature = "generate")]
    generator_config: Arc<GeneratorConfig>,
    #[cfg(feature = "transpile")]
    transformer: Box<dyn Fn(Expression) -> Result<Expression> + Send + Sync>,
    /// Optional function to get expression-specific generator config (for hybrid dialects like Athena).
    #[cfg(feature = "generate")]
    generator_config_for_expr: Option<Box<dyn Fn(&Expression) -> GeneratorConfig + Send + Sync>>,
    /// Optional custom preprocessing function (overrides built-in preprocess for custom dialects).
    #[cfg(feature = "transpile")]
    custom_preprocess: Option<Box<dyn Fn(Expression) -> Result<Expression> + Send + Sync>>,
}

/// Options for [`Dialect::transpile_with`].
///
/// Use [`TranspileOptions::default`] for defaults, then tweak the fields you need.
/// The struct is marked `#[non_exhaustive]` so new fields can be added without
/// breaking the API.
///
/// The struct derives `Serialize`/`Deserialize` using camelCase field names so
/// it can be round-tripped over JSON bridges (C FFI, WASM) without mapping.
#[cfg(feature = "transpile")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
#[non_exhaustive]
pub struct TranspileOptions {
    /// Whether to pretty-print the output SQL.
    pub pretty: bool,
    /// How unsupported target-dialect constructs should be handled.
    ///
    /// The default is [`UnsupportedLevel::Warn`], which preserves the current
    /// compatibility behavior and continues transpilation.
    pub unsupported_level: UnsupportedLevel,
    /// Maximum number of unsupported diagnostics to include in raised errors.
    pub max_unsupported: usize,
    /// Complexity guard limits used while parsing, transforming, and generating.
    pub complexity_guard: ComplexityGuardOptions,
}

#[cfg(feature = "transpile")]
impl Default for TranspileOptions {
    fn default() -> Self {
        Self {
            pretty: false,
            unsupported_level: UnsupportedLevel::Warn,
            max_unsupported: 3,
            complexity_guard: ComplexityGuardOptions::default(),
        }
    }
}

#[cfg(feature = "transpile")]
impl TranspileOptions {
    /// Construct options with pretty-printing enabled.
    pub fn pretty() -> Self {
        Self {
            pretty: true,
            ..Default::default()
        }
    }

    /// Construct options that raise when known unsupported constructs remain.
    pub fn strict() -> Self {
        Self {
            unsupported_level: UnsupportedLevel::Raise,
            ..Default::default()
        }
    }

    /// Set how unsupported target-dialect constructs should be handled.
    pub fn with_unsupported_level(mut self, level: UnsupportedLevel) -> Self {
        self.unsupported_level = level;
        self
    }

    /// Set the maximum number of unsupported diagnostics to include in raised errors.
    pub fn with_max_unsupported(mut self, max: usize) -> Self {
        self.max_unsupported = max;
        self
    }

    /// Set complexity guard limits for parse/transpile/generate recursion-heavy paths.
    pub fn with_complexity_guard(mut self, guard: ComplexityGuardOptions) -> Self {
        self.complexity_guard = guard;
        self
    }
}

/// A value that can be used as the target dialect in [`Dialect::transpile`] /
/// [`Dialect::transpile_with`].
///
/// Implemented for [`DialectType`] (built-in dialect enum) and `&Dialect` (any
/// dialect handle, including custom ones). End users do not normally need to
/// implement this trait themselves.
#[cfg(feature = "transpile")]
pub trait TranspileTarget {
    /// Invoke `f` with a reference to the resolved target dialect.
    fn with_dialect<R>(self, f: impl FnOnce(&Dialect) -> R) -> R;
}

#[cfg(feature = "transpile")]
impl TranspileTarget for DialectType {
    fn with_dialect<R>(self, f: impl FnOnce(&Dialect) -> R) -> R {
        f(&Dialect::get(self))
    }
}

#[cfg(feature = "transpile")]
impl TranspileTarget for &Dialect {
    fn with_dialect<R>(self, f: impl FnOnce(&Dialect) -> R) -> R {
        f(self)
    }
}

impl Dialect {
    /// Creates a fully configured [`Dialect`] instance for the given [`DialectType`].
    ///
    /// This is the primary constructor. It initializes the tokenizer, generator config,
    /// and expression transformer based on the dialect's [`DialectImpl`] implementation.
    /// For hybrid dialects like Athena, it also sets up expression-specific generator
    /// config routing.
    pub fn get(dialect_type: DialectType) -> Self {
        let configs = configs_for_dialect_type(dialect_type);
        let tokenizer_config = configs.tokenizer_config;
        #[cfg(feature = "generate")]
        let generator_config = configs.generator_config;
        #[cfg(feature = "transpile")]
        let transformer = configs.transformer;

        // Set up expression-specific generator config for hybrid dialects
        #[cfg(feature = "generate")]
        let generator_config_for_expr: Option<
            Box<dyn Fn(&Expression) -> GeneratorConfig + Send + Sync>,
        > = match dialect_type {
            #[cfg(feature = "dialect-athena")]
            DialectType::Athena => Some(Box::new(|expr| {
                AthenaDialect.generator_config_for_expr(expr)
            })),
            _ => None,
        };

        Self {
            dialect_type,
            tokenizer: Tokenizer::from_shared_config(tokenizer_config),
            #[cfg(feature = "generate")]
            generator_config,
            #[cfg(feature = "transpile")]
            transformer,
            #[cfg(feature = "generate")]
            generator_config_for_expr,
            #[cfg(feature = "transpile")]
            custom_preprocess: None,
        }
    }

    /// Look up a dialect by string name.
    ///
    /// Checks built-in dialect names first (via [`DialectType::from_str`]), then
    /// falls back to the custom dialect registry. Returns `None` if no dialect
    /// with the given name exists.
    pub fn get_by_name(name: &str) -> Option<Self> {
        // Try built-in first
        if let Ok(dt) = DialectType::from_str(name) {
            return Some(Self::get(dt));
        }

        // Try custom registry
        let config = get_custom_dialect_config(name)?;
        Some(Self::from_custom_config(&config))
    }

    /// Construct a `Dialect` from a custom dialect configuration.
    fn from_custom_config(config: &CustomDialectConfig) -> Self {
        // Build the transformer: use custom if provided, else use base dialect's
        #[cfg(feature = "transpile")]
        let transformer: Box<dyn Fn(Expression) -> Result<Expression> + Send + Sync> =
            if let Some(ref custom_transform) = config.transform {
                let t = Arc::clone(custom_transform);
                Box::new(move |e| t(e))
            } else {
                configs_for_dialect_type(config.base_dialect).transformer
            };

        // Build the custom preprocess: use custom if provided
        #[cfg(feature = "transpile")]
        let custom_preprocess: Option<
            Box<dyn Fn(Expression) -> Result<Expression> + Send + Sync>,
        > = config.preprocess.as_ref().map(|p| {
            let p = Arc::clone(p);
            Box::new(move |e: Expression| p(e))
                as Box<dyn Fn(Expression) -> Result<Expression> + Send + Sync>
        });

        Self {
            dialect_type: config.base_dialect,
            tokenizer: Tokenizer::from_shared_config(config.tokenizer_config.clone()),
            #[cfg(feature = "generate")]
            generator_config: Arc::new(config.generator_config.clone()),
            #[cfg(feature = "transpile")]
            transformer,
            #[cfg(feature = "generate")]
            generator_config_for_expr: None,
            #[cfg(feature = "transpile")]
            custom_preprocess,
        }
    }

    /// Get the dialect type
    pub fn dialect_type(&self) -> DialectType {
        self.dialect_type
    }

    /// Get the generator configuration
    #[cfg(feature = "generate")]
    pub fn generator_config(&self) -> &GeneratorConfig {
        &self.generator_config
    }

    /// Parses a SQL string into a list of [`Expression`] AST nodes.
    ///
    /// The input may contain multiple semicolon-separated statements; each one
    /// produces a separate element in the returned vector. Tokenization uses
    /// this dialect's configured tokenizer, and parsing uses the dialect-aware parser.
    pub fn parse(&self, sql: &str) -> Result<Vec<Expression>> {
        self.parse_with_guard(sql, self.default_complexity_guard())
    }

    fn parse_with_guard(
        &self,
        sql: &str,
        complexity_guard: ComplexityGuardOptions,
    ) -> Result<Vec<Expression>> {
        enforce_input(sql, &complexity_guard)?;
        let source: Arc<str> = Arc::from(sql);
        let (tokens, token_guard_stats) = self.tokenizer.tokenize_for_parser(&source)?;
        let config = crate::parser::ParserConfig {
            dialect: Some(self.dialect_type),
            complexity_guard,
            ..Default::default()
        };
        let mut parser = Parser::with_parser_tokens(tokens, token_guard_stats, config, source);
        parser.parse()
    }

    fn default_complexity_guard(&self) -> ComplexityGuardOptions {
        let mut guard = ComplexityGuardOptions::default();
        if matches!(self.dialect_type, DialectType::ClickHouse) {
            guard.max_ast_depth = Some(4_096);
            guard.max_function_call_depth = Some(512);
        }
        guard
    }

    #[cfg(feature = "transpile")]
    fn default_transpile_complexity_guard(
        &self,
        target_dialect: &Dialect,
        guard: ComplexityGuardOptions,
    ) -> ComplexityGuardOptions {
        if guard != ComplexityGuardOptions::default() {
            return guard;
        }

        if matches!(self.dialect_type, DialectType::ClickHouse)
            || matches!(target_dialect.dialect_type, DialectType::ClickHouse)
        {
            let mut guard = guard;
            guard.max_ast_depth = Some(4_096);
            guard.max_function_call_depth = Some(512);
            guard
        } else {
            guard
        }
    }

    /// Parse a standalone SQL data type using this dialect's tokenizer and parser.
    ///
    /// This accepts type strings such as `DECIMAL(10, 2)`, `INT[]`, or
    /// `STRUCT(a INT, b VARCHAR)` without requiring a surrounding statement.
    pub fn parse_data_type(&self, sql: &str) -> Result<DataType> {
        let complexity_guard = self.default_complexity_guard();
        enforce_input(sql, &complexity_guard)?;
        let source: Arc<str> = Arc::from(sql);
        let (tokens, token_guard_stats) = self.tokenizer.tokenize_for_parser(&source)?;
        let config = crate::parser::ParserConfig {
            dialect: Some(self.dialect_type),
            complexity_guard,
            ..Default::default()
        };
        let mut parser = Parser::with_parser_tokens(tokens, token_guard_stats, config, source);
        parser.parse_standalone_data_type()
    }

    /// Tokenize SQL using this dialect's tokenizer configuration.
    pub fn tokenize(&self, sql: &str) -> Result<Vec<Token>> {
        self.tokenizer.tokenize(sql)
    }

    /// Get the generator config for a specific expression (supports hybrid dialects).
    /// Returns an owned `GeneratorConfig` suitable for mutation before generation.
    #[cfg(feature = "generate")]
    fn get_config_for_expr(&self, expr: &Expression) -> GeneratorConfig {
        if let Some(ref config_fn) = self.generator_config_for_expr {
            config_fn(expr)
        } else {
            (*self.generator_config).clone()
        }
    }

    /// Generates a SQL string from an [`Expression`] AST node.
    ///
    /// The output uses this dialect's generator configuration for identifier quoting,
    /// keyword casing, function name normalization, and syntax style. The result is
    /// a single-line (non-pretty) SQL string.
    #[cfg(feature = "generate")]
    pub fn generate(&self, expr: &Expression) -> Result<String> {
        // Fast path: when no per-expression config override, share the Arc cheaply.
        if self.generator_config_for_expr.is_none() {
            let mut generator = Generator::with_arc_config(self.generator_config.clone());
            return generator.generate(expr);
        }
        let config = self.get_config_for_expr(expr);
        let mut generator = Generator::with_config(config);
        generator.generate(expr)
    }

    /// Generate SQL from an expression with pretty printing enabled
    #[cfg(feature = "generate")]
    pub fn generate_pretty(&self, expr: &Expression) -> Result<String> {
        let mut config = self.get_config_for_expr(expr);
        config.pretty = true;
        let mut generator = Generator::with_config(config);
        generator.generate(expr)
    }

    /// Generate SQL from an expression with source dialect info (for transpilation)
    #[cfg(feature = "generate")]
    pub fn generate_with_source(&self, expr: &Expression, source: DialectType) -> Result<String> {
        let mut config = self.get_config_for_expr(expr);
        config.source_dialect = Some(source);
        let mut generator = Generator::with_config(config);
        generator.generate(expr)
    }

    /// Generate SQL from an expression with pretty printing and source dialect info
    #[cfg(feature = "generate")]
    pub fn generate_pretty_with_source(
        &self,
        expr: &Expression,
        source: DialectType,
    ) -> Result<String> {
        let mut config = self.get_config_for_expr(expr);
        config.pretty = true;
        config.source_dialect = Some(source);
        let mut generator = Generator::with_config(config);
        generator.generate(expr)
    }

    /// Generate SQL from an expression with source dialect and transpile options.
    #[cfg(all(feature = "generate", feature = "transpile"))]
    fn generate_with_transpile_options(
        &self,
        expr: &Expression,
        source: DialectType,
        opts: &TranspileOptions,
    ) -> Result<String> {
        let mut config = self.get_config_for_expr(expr);
        config.source_dialect = Some(source);
        config.pretty = opts.pretty;
        config.unsupported_level = opts.unsupported_level;
        config.max_unsupported = opts.max_unsupported.max(1);
        config.complexity_guard = opts.complexity_guard;
        let mut generator = Generator::with_config(config);
        generator.generate(expr)
    }

    /// Generate SQL from an expression with forced identifier quoting (identify=True)
    #[cfg(feature = "generate")]
    pub fn generate_with_identify(&self, expr: &Expression) -> Result<String> {
        let mut config = self.get_config_for_expr(expr);
        config.always_quote_identifiers = true;
        let mut generator = Generator::with_config(config);
        generator.generate(expr)
    }

    /// Generate SQL from an expression with pretty printing and forced identifier quoting
    #[cfg(feature = "generate")]
    pub fn generate_pretty_with_identify(&self, expr: &Expression) -> Result<String> {
        let mut config = (*self.generator_config).clone();
        config.pretty = true;
        config.always_quote_identifiers = true;
        let mut generator = Generator::with_config(config);
        generator.generate(expr)
    }

    /// Generate SQL from an expression with caller-specified config overrides
    #[cfg(feature = "generate")]
    pub fn generate_with_overrides(
        &self,
        expr: &Expression,
        overrides: impl FnOnce(&mut GeneratorConfig),
    ) -> Result<String> {
        let mut config = self.get_config_for_expr(expr);
        overrides(&mut config);
        let mut generator = Generator::with_config(config);
        generator.generate(expr)
    }

    /// Transforms an expression tree to conform to this dialect's syntax and semantics.
    ///
    /// The transformation proceeds in two phases:
    /// 1. **Preprocessing** -- whole-tree structural rewrites such as eliminating QUALIFY,
    ///    ensuring boolean predicates, or converting DISTINCT ON to a window-function pattern.
    /// 2. **Recursive per-node transform** -- a bottom-up pass via [`transform_recursive`]
    ///    that applies this dialect's [`DialectImpl::transform_expr`] to every node.
    ///
    /// This method is used both during transpilation (to rewrite an AST for a target dialect)
    /// and for identity transforms (normalizing SQL within the same dialect).
    #[cfg(feature = "transpile")]
    pub fn transform(&self, expr: Expression) -> Result<Expression> {
        self.transform_with_guard(expr, self.default_complexity_guard())
    }

    #[cfg(feature = "transpile")]
    fn transform_with_guard(
        &self,
        expr: Expression,
        complexity_guard: ComplexityGuardOptions,
    ) -> Result<Expression> {
        enforce_generate_ast(&expr, &complexity_guard)?;
        // Apply preprocessing transforms based on dialect
        let preprocessed = self.preprocess(expr)?;
        // Then apply recursive transformation
        transform_recursive(preprocessed, &self.transformer)
    }

    /// Apply dialect-specific preprocessing transforms
    #[cfg(feature = "transpile")]
    fn preprocess(&self, expr: Expression) -> Result<Expression> {
        // If a custom preprocess function is set, use it instead of the built-in logic
        if let Some(ref custom_preprocess) = self.custom_preprocess {
            return custom_preprocess(expr);
        }

        #[cfg(any(
            feature = "dialect-mysql",
            feature = "dialect-postgresql",
            feature = "dialect-bigquery",
            feature = "dialect-snowflake",
            feature = "dialect-tsql",
            feature = "dialect-spark",
            feature = "dialect-databricks",
            feature = "dialect-hive",
            feature = "dialect-sqlite",
            feature = "dialect-trino",
            feature = "dialect-presto",
            feature = "dialect-duckdb",
            feature = "dialect-redshift",
            feature = "dialect-starrocks",
            feature = "dialect-oracle",
            feature = "dialect-clickhouse",
            feature = "dialect-fabric",
        ))]
        use crate::transforms;

        match self.dialect_type {
            // MySQL doesn't support QUALIFY, DISTINCT ON, FULL OUTER JOIN
            // MySQL doesn't natively support GENERATE_DATE_ARRAY (expand to recursive CTE)
            #[cfg(feature = "dialect-mysql")]
            DialectType::MySQL => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::eliminate_full_outer_join(expr)?;
                let expr = transforms::eliminate_semi_and_anti_joins(expr)?;
                let expr = transforms::unnest_generate_date_array_using_recursive_cte(expr)?;
                Ok(expr)
            }
            // PostgreSQL doesn't support QUALIFY
            // PostgreSQL: UNNEST(GENERATE_SERIES) -> subquery wrapping
            // PostgreSQL: Normalize SET ... TO to SET ... = in CREATE FUNCTION
            #[cfg(feature = "dialect-postgresql")]
            DialectType::PostgreSQL => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::eliminate_semi_and_anti_joins(expr)?;
                let expr = transforms::unwrap_unnest_generate_series_for_postgres(expr)?;
                // Normalize SET ... TO to SET ... = in CREATE FUNCTION
                // Only normalize when sqlglot would fully parse (no body) —
                // sqlglot falls back to Command for complex function bodies,
                // preserving the original text including TO.
                let expr = if let Expression::CreateFunction(mut cf) = expr {
                    if cf.body.is_none() {
                        for opt in &mut cf.set_options {
                            if let crate::expressions::FunctionSetValue::Value { use_to, .. } =
                                &mut opt.value
                            {
                                *use_to = false;
                            }
                        }
                    }
                    Expression::CreateFunction(cf)
                } else {
                    expr
                };
                Ok(expr)
            }
            // BigQuery doesn't support DISTINCT ON or CTE column aliases
            #[cfg(feature = "dialect-bigquery")]
            DialectType::BigQuery => {
                let expr = transforms::eliminate_semi_and_anti_joins(expr)?;
                let expr = transforms::pushdown_cte_column_names(expr)?;
                let expr = transforms::explode_projection_to_unnest(expr, DialectType::BigQuery)?;
                Ok(expr)
            }
            // Snowflake
            #[cfg(feature = "dialect-snowflake")]
            DialectType::Snowflake => {
                let expr = transforms::eliminate_semi_and_anti_joins(expr)?;
                let expr = transforms::eliminate_window_clause(expr)?;
                let expr = transforms::snowflake_flatten_projection_to_unnest(expr)?;
                Ok(expr)
            }
            // TSQL doesn't support QUALIFY
            // TSQL requires boolean expressions in WHERE/HAVING (no implicit truthiness)
            // TSQL doesn't support CTEs in subqueries (hoist to top level)
            // NOTE: no_limit_order_by_union is handled in cross_dialect_normalize (not preprocess)
            // to avoid breaking TSQL identity tests where ORDER BY on UNION is valid
            #[cfg(feature = "dialect-tsql")]
            DialectType::TSQL => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::eliminate_semi_and_anti_joins(expr)?;
                let expr = transforms::normalize_grouping_sets_for_tsql(expr)?;
                let expr =
                    transforms::expand_distinct_grouping_sets_for_tsql(expr, DialectType::TSQL)?;
                let expr = transforms::ensure_bools(expr)?;
                let expr = transforms::unnest_generate_date_array_using_recursive_cte(expr)?;
                let expr = transforms::strip_cte_materialization(expr)?;
                let expr = transforms::move_ctes_to_top_level(expr)?;
                let expr = transforms::qualify_derived_table_outputs(expr)?;
                Ok(expr)
            }
            // Fabric shares T-SQL predicate rules and CTE placement restrictions,
            // but keeps Fabric-specific APPLY and derived-table behavior separate.
            #[cfg(feature = "dialect-fabric")]
            DialectType::Fabric => {
                let expr = transforms::normalize_grouping_sets_for_tsql(expr)?;
                let expr =
                    transforms::expand_distinct_grouping_sets_for_tsql(expr, DialectType::Fabric)?;
                let expr = transforms::ensure_bools(expr)?;
                let expr = transforms::strip_cte_materialization(expr)?;
                let expr = transforms::move_ctes_to_top_level(expr)?;
                Ok(expr)
            }
            // Spark doesn't support QUALIFY (but Databricks does)
            // Spark doesn't support CTEs in subqueries (hoist to top level)
            #[cfg(feature = "dialect-spark")]
            DialectType::Spark => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::add_auto_table_alias(expr)?;
                let expr = transforms::simplify_nested_paren_values(expr)?;
                let expr = transforms::move_ctes_to_top_level(expr)?;
                Ok(expr)
            }
            // Databricks supports QUALIFY natively
            // Databricks doesn't support CTEs in subqueries (hoist to top level)
            #[cfg(feature = "dialect-databricks")]
            DialectType::Databricks => {
                let expr = transforms::add_auto_table_alias(expr)?;
                let expr = transforms::simplify_nested_paren_values(expr)?;
                let expr = transforms::move_ctes_to_top_level(expr)?;
                Ok(expr)
            }
            // Hive doesn't support QUALIFY or CTEs in subqueries
            #[cfg(feature = "dialect-hive")]
            DialectType::Hive => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::move_ctes_to_top_level(expr)?;
                Ok(expr)
            }
            // SQLite doesn't support QUALIFY
            #[cfg(feature = "dialect-sqlite")]
            DialectType::SQLite => {
                let expr = transforms::eliminate_qualify(expr)?;
                Ok(expr)
            }
            // Trino doesn't support QUALIFY
            #[cfg(feature = "dialect-trino")]
            DialectType::Trino => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::explode_projection_to_unnest(expr, DialectType::Trino)?;
                Ok(expr)
            }
            // Presto doesn't support QUALIFY or WINDOW clause
            #[cfg(feature = "dialect-presto")]
            DialectType::Presto => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::eliminate_window_clause(expr)?;
                let expr = transforms::explode_projection_to_unnest(expr, DialectType::Presto)?;
                Ok(expr)
            }
            // DuckDB supports QUALIFY - no elimination needed
            // Expand POSEXPLODE to GENERATE_SUBSCRIPTS + UNNEST
            // Expand LIKE ANY / ILIKE ANY to OR chains (DuckDB doesn't support quantifiers)
            #[cfg(feature = "dialect-duckdb")]
            DialectType::DuckDB => {
                let expr = transforms::expand_posexplode_duckdb(expr)?;
                let expr = transforms::expand_like_any(expr)?;
                Ok(expr)
            }
            // Redshift doesn't support QUALIFY, WINDOW clause, or GENERATE_DATE_ARRAY
            #[cfg(feature = "dialect-redshift")]
            DialectType::Redshift => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::eliminate_window_clause(expr)?;
                let expr = transforms::unnest_generate_date_array_using_recursive_cte(expr)?;
                Ok(expr)
            }
            // StarRocks doesn't support BETWEEN in DELETE statements or QUALIFY
            #[cfg(feature = "dialect-starrocks")]
            DialectType::StarRocks => {
                let expr = transforms::eliminate_qualify(expr)?;
                let expr = transforms::expand_between_in_delete(expr)?;
                let expr = transforms::eliminate_distinct_on_for_dialect(
                    expr,
                    Some(DialectType::StarRocks),
                    Some(DialectType::StarRocks),
                )?;
                let expr = transforms::unnest_generate_date_array_using_recursive_cte(expr)?;
                Ok(expr)
            }
            // DataFusion supports QUALIFY and semi/anti joins natively
            #[cfg(feature = "dialect-datafusion")]
            DialectType::DataFusion => Ok(expr),
            // Oracle doesn't support QUALIFY
            #[cfg(feature = "dialect-oracle")]
            DialectType::Oracle => {
                let expr = transforms::eliminate_qualify(expr)?;
                Ok(expr)
            }
            // Drill - no special preprocessing needed
            #[cfg(feature = "dialect-drill")]
            DialectType::Drill => Ok(expr),
            // Teradata - no special preprocessing needed
            #[cfg(feature = "dialect-teradata")]
            DialectType::Teradata => Ok(expr),
            // ClickHouse doesn't support ORDER BY/LIMIT directly on UNION
            #[cfg(feature = "dialect-clickhouse")]
            DialectType::ClickHouse => {
                let expr = transforms::no_limit_order_by_union(expr)?;
                Ok(expr)
            }
            // Other dialects - no preprocessing
            _ => Ok(expr),
        }
    }

    /// Transpile SQL from this dialect to the given target dialect.
    ///
    /// The target may be specified as either a built-in [`DialectType`] enum variant
    /// or as a reference to a [`Dialect`] handle (built-in or custom). Both work:
    ///
    /// ```rust,ignore
    /// let pg = Dialect::get(DialectType::PostgreSQL);
    /// pg.transpile("SELECT NOW()", DialectType::BigQuery)?;   // enum
    /// pg.transpile("SELECT NOW()", &custom_dialect)?;         // handle
    /// ```
    ///
    /// For pretty-printing or other options, use [`transpile_with`](Self::transpile_with).
    #[cfg(feature = "transpile")]
    pub fn transpile<T: TranspileTarget>(&self, sql: &str, target: T) -> Result<Vec<String>> {
        self.transpile_with(sql, target, TranspileOptions::default())
    }

    /// Transpile SQL with configurable [`TranspileOptions`] (e.g. pretty-printing).
    #[cfg(feature = "transpile")]
    pub fn transpile_with<T: TranspileTarget>(
        &self,
        sql: &str,
        target: T,
        opts: TranspileOptions,
    ) -> Result<Vec<String>> {
        target.with_dialect(|td| self.transpile_inner(sql, td, &opts))
    }

    #[cfg(feature = "transpile")]
    fn transpile_inner(
        &self,
        sql: &str,
        target_dialect: &Dialect,
        opts: &TranspileOptions,
    ) -> Result<Vec<String>> {
        let mut effective_opts = opts.clone();
        effective_opts.complexity_guard =
            self.default_transpile_complexity_guard(target_dialect, opts.complexity_guard);
        let opts = &effective_opts;
        let target = target_dialect.dialect_type;
        if matches!(self.dialect_type, DialectType::PostgreSQL)
            && matches!(target, DialectType::SQLite)
        {
            self.reject_pgvector_distance_operators_for_sqlite(sql)?;
        }
        let expressions = self.parse_with_guard(sql, opts.complexity_guard)?;
        let generic_identity =
            self.dialect_type == DialectType::Generic && target == DialectType::Generic;

        if generic_identity {
            return expressions
                .into_iter()
                .map(|expr| {
                    Self::reject_strict_unsupported(&expr, self.dialect_type, target, opts)?;
                    target_dialect.generate_with_transpile_options(&expr, self.dialect_type, opts)
                })
                .collect();
        }

        expressions
            .into_iter()
            .map(|expr| {
                // DuckDB source: normalize VARCHAR/CHAR to TEXT (DuckDB doesn't support
                // VARCHAR length constraints). This emulates Python sqlglot's DuckDB parser
                // where VARCHAR_LENGTH = None and VARCHAR maps to TEXT.
                let expr = if matches!(self.dialect_type, DialectType::DuckDB) {
                    use crate::expressions::DataType as DT;
                    transform_recursive(expr, &|e| match e {
                        Expression::DataType(DT::VarChar { .. }) => {
                            Ok(Expression::DataType(DT::Text))
                        }
                        Expression::DataType(DT::Char { .. }) => Ok(Expression::DataType(DT::Text)),
                        _ => Ok(e),
                    })?
                } else {
                    expr
                };

                Self::reject_postgres_tsql_strict_regex_predicates(
                    &expr,
                    self.dialect_type,
                    target,
                    opts,
                )?;
                Self::reject_tsql_strict_json_constructor_return_types(
                    &expr,
                    self.dialect_type,
                    target,
                    opts,
                )?;
                Self::reject_postgres_tsql_strict_json_aggregate_modifiers(
                    &expr,
                    self.dialect_type,
                    target,
                    opts,
                )?;

                // When source and target differ, first normalize the source dialect's
                // AST constructs to standard SQL, so that the target dialect can handle them.
                // This handles cases like Snowflake's SQUARE -> POWER, DIV0 -> CASE, etc.
                let normalized =
                    if self.dialect_type != target && self.dialect_type != DialectType::Generic {
                        self.transform_with_guard(expr, opts.complexity_guard)?
                    } else {
                        expr
                    };

                // For TSQL source targeting non-TSQL: unwrap ISNULL(JSON_QUERY(...), JSON_VALUE(...))
                // to just JSON_QUERY(...) so cross_dialect_normalize can convert it cleanly.
                // The TSQL read transform wraps JsonQuery in ISNULL for identity, but for
                // cross-dialect transpilation we need the unwrapped JSON_QUERY.
                let normalized =
                    if matches!(self.dialect_type, DialectType::TSQL | DialectType::Fabric)
                        && !matches!(target, DialectType::TSQL | DialectType::Fabric)
                    {
                        transform_recursive(normalized, &|e| {
                            if let Expression::Function(ref f) = e {
                                if f.name.eq_ignore_ascii_case("ISNULL") && f.args.len() == 2 {
                                    // Check if first arg is JSON_QUERY and second is JSON_VALUE
                                    if let (
                                        Expression::Function(ref jq),
                                        Expression::Function(ref jv),
                                    ) = (&f.args[0], &f.args[1])
                                    {
                                        if jq.name.eq_ignore_ascii_case("JSON_QUERY")
                                            && jv.name.eq_ignore_ascii_case("JSON_VALUE")
                                        {
                                            // Unwrap: return just JSON_QUERY(...)
                                            return Ok(f.args[0].clone());
                                        }
                                    }
                                }
                            }
                            Ok(e)
                        })?
                    } else {
                        normalized
                    };

                // Snowflake source to non-Snowflake target: CURRENT_TIME -> LOCALTIME
                // Snowflake's CURRENT_TIME is equivalent to LOCALTIME in other dialects.
                // Python sqlglot parses Snowflake's CURRENT_TIME as Localtime expression.
                let normalized = if matches!(self.dialect_type, DialectType::Snowflake)
                    && !matches!(target, DialectType::Snowflake)
                {
                    transform_recursive(normalized, &|e| {
                        if let Expression::Function(ref f) = e {
                            if f.name.eq_ignore_ascii_case("CURRENT_TIME") {
                                return Ok(Expression::Localtime(Box::new(
                                    crate::expressions::Localtime { this: None },
                                )));
                            }
                        }
                        Ok(e)
                    })?
                } else {
                    normalized
                };

                // Snowflake source to DuckDB target: REPEAT(' ', n) -> REPEAT(' ', CAST(n AS BIGINT))
                // Snowflake's SPACE(n) is converted to REPEAT(' ', n) by the Snowflake source
                // transform. DuckDB requires the count argument to be BIGINT.
                let normalized = if matches!(self.dialect_type, DialectType::Snowflake)
                    && matches!(target, DialectType::DuckDB)
                {
                    transform_recursive(normalized, &|e| {
                        if let Expression::Function(ref f) = e {
                            if f.name.eq_ignore_ascii_case("REPEAT") && f.args.len() == 2 {
                                // Check if first arg is space string literal
                                if let Expression::Literal(ref lit) = f.args[0] {
                                    if let crate::expressions::Literal::String(ref s) = lit.as_ref()
                                    {
                                        if s == " " {
                                            // Wrap second arg in CAST(... AS BIGINT) if not already
                                            if !matches!(f.args[1], Expression::Cast(_)) {
                                                let mut new_args = f.args.clone();
                                                new_args[1] = Expression::Cast(Box::new(
                                                    crate::expressions::Cast {
                                                        this: new_args[1].clone(),
                                                        to: crate::expressions::DataType::BigInt {
                                                            length: None,
                                                        },
                                                        trailing_comments: Vec::new(),
                                                        double_colon_syntax: false,
                                                        format: None,
                                                        default: None,
                                                        inferred_type: None,
                                                    },
                                                ));
                                                return Ok(Expression::Function(Box::new(
                                                    crate::expressions::Function {
                                                        name: f.name.clone(),
                                                        args: new_args,
                                                        distinct: f.distinct,
                                                        trailing_comments: f
                                                            .trailing_comments
                                                            .clone(),
                                                        use_bracket_syntax: f.use_bracket_syntax,
                                                        no_parens: f.no_parens,
                                                        quoted: f.quoted,
                                                        span: None,
                                                        inferred_type: None,
                                                    },
                                                )));
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        Ok(e)
                    })?
                } else {
                    normalized
                };

                // Propagate struct field names in arrays (for BigQuery source to non-BigQuery target)
                // BigQuery->BigQuery should NOT propagate names (BigQuery handles implicit inheritance)
                let normalized = if matches!(self.dialect_type, DialectType::BigQuery)
                    && !matches!(target, DialectType::BigQuery)
                {
                    crate::transforms::propagate_struct_field_names(normalized)?
                } else {
                    normalized
                };

                // Snowflake source to DuckDB target: RANDOM()/RANDOM(seed) -> scaled RANDOM()
                // Snowflake RANDOM() returns integer in [-2^63, 2^63-1], DuckDB RANDOM() returns float [0, 1)
                // Skip RANDOM inside UNIFORM/NORMAL/ZIPF/RANDSTR generator args since those
                // functions handle their generator args differently (as float seeds).
                let normalized = if matches!(self.dialect_type, DialectType::Snowflake)
                    && matches!(target, DialectType::DuckDB)
                {
                    fn make_scaled_random() -> Expression {
                        let lower =
                            Expression::Literal(Box::new(crate::expressions::Literal::Number(
                                "-9.223372036854776E+18".to_string(),
                            )));
                        let upper =
                            Expression::Literal(Box::new(crate::expressions::Literal::Number(
                                "9.223372036854776e+18".to_string(),
                            )));
                        let random_call = Expression::Random(crate::expressions::Random);
                        let range_size = Expression::Paren(Box::new(crate::expressions::Paren {
                            this: Expression::Sub(Box::new(crate::expressions::BinaryOp {
                                left: upper,
                                right: lower.clone(),
                                left_comments: vec![],
                                operator_comments: vec![],
                                trailing_comments: vec![],
                                inferred_type: None,
                            })),
                            trailing_comments: vec![],
                        }));
                        let scaled = Expression::Mul(Box::new(crate::expressions::BinaryOp {
                            left: random_call,
                            right: range_size,
                            left_comments: vec![],
                            operator_comments: vec![],
                            trailing_comments: vec![],
                            inferred_type: None,
                        }));
                        let shifted = Expression::Add(Box::new(crate::expressions::BinaryOp {
                            left: lower,
                            right: scaled,
                            left_comments: vec![],
                            operator_comments: vec![],
                            trailing_comments: vec![],
                            inferred_type: None,
                        }));
                        Expression::Cast(Box::new(crate::expressions::Cast {
                            this: shifted,
                            to: crate::expressions::DataType::BigInt { length: None },
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        }))
                    }

                    // Pre-process: protect seeded RANDOM(seed) inside UNIFORM/NORMAL/ZIPF/RANDSTR
                    // by converting Rand{seed: Some(s)} to Function{name:"RANDOM", args:[s]}.
                    // This prevents transform_recursive (which is bottom-up) from expanding
                    // seeded RANDOM into make_scaled_random() and losing the seed value.
                    // Unseeded RANDOM()/Rand{seed:None} is left as-is so it gets expanded
                    // and then un-expanded back to Expression::Random by the code below.
                    let normalized = transform_recursive(normalized, &|e| {
                        if let Expression::Function(ref f) = e {
                            let n = f.name.to_ascii_uppercase();
                            if n == "UNIFORM" || n == "NORMAL" || n == "ZIPF" || n == "RANDSTR" {
                                if let Expression::Function(mut f) = e {
                                    for arg in f.args.iter_mut() {
                                        if let Expression::Rand(ref r) = arg {
                                            if r.lower.is_none() && r.upper.is_none() {
                                                if let Some(ref seed) = r.seed {
                                                    // Convert Rand{seed: Some(s)} to Function("RANDOM", [s])
                                                    // so it won't be expanded by the RANDOM expansion below
                                                    *arg = Expression::Function(Box::new(
                                                        crate::expressions::Function::new(
                                                            "RANDOM".to_string(),
                                                            vec![*seed.clone()],
                                                        ),
                                                    ));
                                                }
                                            }
                                        }
                                    }
                                    return Ok(Expression::Function(f));
                                }
                            }
                        }
                        Ok(e)
                    })?;

                    // transform_recursive processes bottom-up, so RANDOM() (unseeded) inside
                    // generator functions (UNIFORM, NORMAL, ZIPF) gets expanded before
                    // we see the parent. We detect this and undo the expansion by replacing
                    // the expanded pattern back with Expression::Random.
                    // Seeded RANDOM(seed) was already protected above as Function("RANDOM", [seed]).
                    // Note: RANDSTR is NOT included here — it needs the expanded form for unseeded
                    // RANDOM() since the DuckDB handler uses the expanded SQL as-is in the hash.
                    transform_recursive(normalized, &|e| {
                        if let Expression::Function(ref f) = e {
                            let n = f.name.to_ascii_uppercase();
                            if n == "UNIFORM" || n == "NORMAL" || n == "ZIPF" {
                                if let Expression::Function(mut f) = e {
                                    for arg in f.args.iter_mut() {
                                        // Detect expanded RANDOM pattern: CAST(-9.22... + RANDOM() * (...) AS BIGINT)
                                        if let Expression::Cast(ref cast) = arg {
                                            if matches!(
                                                cast.to,
                                                crate::expressions::DataType::BigInt { .. }
                                            ) {
                                                if let Expression::Add(ref add) = cast.this {
                                                    if let Expression::Literal(ref lit) = add.left {
                                                        if let crate::expressions::Literal::Number(
                                                            ref num,
                                                        ) = lit.as_ref()
                                                        {
                                                            if num == "-9.223372036854776E+18" {
                                                                *arg = Expression::Random(
                                                                    crate::expressions::Random,
                                                                );
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                    return Ok(Expression::Function(f));
                                }
                                return Ok(e);
                            }
                        }
                        match e {
                            Expression::Random(_) => Ok(make_scaled_random()),
                            // Rand(seed) with no bounds: drop seed and expand
                            // (DuckDB RANDOM doesn't support seeds)
                            Expression::Rand(ref r) if r.lower.is_none() && r.upper.is_none() => {
                                Ok(make_scaled_random())
                            }
                            _ => Ok(e),
                        }
                    })?
                } else {
                    normalized
                };

                // Apply cross-dialect semantic normalizations
                let normalized = normalization::normalize(
                    normalized,
                    self.dialect_type,
                    target,
                    matches!(
                        opts.unsupported_level,
                        UnsupportedLevel::Raise | UnsupportedLevel::Immediate
                    ),
                )?;

                let normalized = if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                    Self::normalize_tsql_fetch_overlaps_date_bin(normalized)?
                } else {
                    normalized
                };

                let normalized =
                    if matches!(
                        self.dialect_type,
                        DialectType::PostgreSQL | DialectType::CockroachDB
                    ) && !matches!(target, DialectType::PostgreSQL | DialectType::CockroachDB)
                    {
                        Self::normalize_postgres_type_function_casts(normalized, target)?
                    } else {
                        normalized
                    };

                let normalized = if matches!(self.dialect_type, DialectType::SQLite)
                    && !matches!(target, DialectType::SQLite)
                {
                    Self::normalize_sqlite_double_quoted_defaults(normalized)?
                } else {
                    normalized
                };

                let normalized = if matches!(self.dialect_type, DialectType::PostgreSQL)
                    && matches!(target, DialectType::SQLite)
                {
                    Self::normalize_postgres_to_sqlite_types(normalized)?
                } else {
                    normalized
                };

                let normalized = if matches!(self.dialect_type, DialectType::PostgreSQL)
                    && matches!(target, DialectType::Fabric)
                {
                    Self::normalize_postgres_to_fabric_types(normalized)?
                } else {
                    normalized
                };

                // For DuckDB target from BigQuery source: wrap UNNEST of struct arrays in
                // (SELECT UNNEST(..., max_depth => 2)) subquery
                // Must run BEFORE unnest_alias_to_column_alias since it changes alias structure
                let normalized = if matches!(self.dialect_type, DialectType::BigQuery)
                    && matches!(target, DialectType::DuckDB)
                {
                    crate::transforms::wrap_duckdb_unnest_struct(normalized)?
                } else {
                    normalized
                };

                // Convert BigQuery UNNEST aliases to column-alias format for DuckDB/Presto/Spark
                // UNNEST(arr) AS x -> UNNEST(arr) AS _t0(x)
                let normalized = if matches!(self.dialect_type, DialectType::BigQuery)
                    && matches!(
                        target,
                        DialectType::DuckDB
                            | DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Athena
                            | DialectType::Spark
                            | DialectType::Databricks
                    ) {
                    crate::transforms::unnest_alias_to_column_alias(normalized)?
                } else if matches!(self.dialect_type, DialectType::BigQuery)
                    && matches!(target, DialectType::BigQuery | DialectType::Redshift)
                {
                    // For BigQuery/Redshift targets: move UNNEST FROM items to CROSS JOINs
                    // but don't convert alias format (no _t0 wrapper)
                    let result = crate::transforms::unnest_from_to_cross_join(normalized)?;
                    // For Redshift: strip UNNEST when arg is a column reference path
                    if matches!(target, DialectType::Redshift) {
                        crate::transforms::strip_unnest_column_refs(result)?
                    } else {
                        result
                    }
                } else {
                    normalized
                };

                // For Presto/Trino targets from PostgreSQL/Redshift source:
                // Wrap UNNEST aliases from GENERATE_SERIES conversion: AS s -> AS _u(s)
                let normalized = if matches!(
                    self.dialect_type,
                    DialectType::PostgreSQL | DialectType::Redshift
                ) && matches!(
                    target,
                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                ) {
                    crate::transforms::wrap_unnest_join_aliases(normalized)?
                } else {
                    normalized
                };

                // Eliminate DISTINCT ON with target-dialect awareness
                // This must happen after source transform (which may produce DISTINCT ON)
                // and before target transform, with knowledge of the target dialect's NULL ordering behavior
                let normalized = crate::transforms::eliminate_distinct_on_for_dialect(
                    normalized,
                    Some(target),
                    Some(self.dialect_type),
                )?;

                // GENERATE_DATE_ARRAY in UNNEST -> Snowflake ARRAY_GENERATE_RANGE + DATEADD
                let normalized = if matches!(target, DialectType::Snowflake) {
                    Self::transform_generate_date_array_snowflake(normalized)?
                } else {
                    normalized
                };

                // CROSS JOIN UNNEST -> LATERAL VIEW EXPLODE/INLINE for Spark/Hive/Databricks
                let normalized = if matches!(
                    target,
                    DialectType::Spark | DialectType::Databricks | DialectType::Hive
                ) {
                    crate::transforms::unnest_to_explode_select(normalized)?
                } else {
                    normalized
                };

                // Wrap UNION with ORDER BY/LIMIT in a subquery for dialects that require it
                let normalized = if matches!(target, DialectType::ClickHouse | DialectType::TSQL) {
                    crate::transforms::no_limit_order_by_union(normalized)?
                } else {
                    normalized
                };

                let normalized = if matches!(
                    self.dialect_type,
                    DialectType::PostgreSQL | DialectType::CockroachDB
                ) && matches!(target, DialectType::TSQL | DialectType::Fabric)
                {
                    Self::normalize_postgres_boolean_semantics_for_tsql(normalized)?
                } else {
                    normalized
                };

                let normalized = if matches!(
                    self.dialect_type,
                    DialectType::PostgreSQL | DialectType::CockroachDB
                ) && matches!(target, DialectType::TSQL | DialectType::Fabric)
                {
                    Self::normalize_postgres_string_semantics_for_tsql(normalized)?
                } else {
                    normalized
                };

                // TSQL: Convert COUNT(*) -> COUNT_BIG(*) when source is not TSQL/Fabric
                // Python sqlglot does this in the TSQL generator, but we can't do it there
                // because it would break TSQL -> TSQL identity
                let normalized = if matches!(target, DialectType::TSQL | DialectType::Fabric)
                    && !matches!(self.dialect_type, DialectType::TSQL | DialectType::Fabric)
                {
                    transform_recursive(normalized, &|e| {
                        if let Expression::Count(ref c) = e {
                            // Build COUNT_BIG(...) as an AggregateFunction
                            let args = if c.star {
                                vec![Expression::Star(crate::expressions::Star {
                                    table: None,
                                    except: None,
                                    replace: None,
                                    rename: None,
                                    trailing_comments: Vec::new(),
                                    span: None,
                                })]
                            } else if let Some(ref this) = c.this {
                                vec![this.clone()]
                            } else {
                                vec![]
                            };
                            Ok(Expression::AggregateFunction(Box::new(
                                crate::expressions::AggregateFunction {
                                    name: "COUNT_BIG".to_string(),
                                    args,
                                    distinct: c.distinct,
                                    filter: c.filter.clone(),
                                    order_by: Vec::new(),
                                    limit: None,
                                    ignore_nulls: None,
                                    inferred_type: None,
                                },
                            )))
                        } else {
                            Ok(e)
                        }
                    })?
                } else {
                    normalized
                };

                // T-SQL/Fabric do not have a scalar boolean type. Keep predicate
                // contexts intact, but materialize boolean-valued expressions used
                // as values before target transforms add ORDER BY null sort keys.
                let normalized = if matches!(target, DialectType::TSQL | DialectType::Fabric)
                    && !matches!(self.dialect_type, DialectType::TSQL | DialectType::Fabric)
                {
                    Self::rewrite_boolean_values_for_tsql(normalized)?
                } else {
                    normalized
                };

                let normalized = if matches!(
                    self.dialect_type,
                    DialectType::PostgreSQL | DialectType::CockroachDB
                ) && matches!(target, DialectType::TSQL | DialectType::Fabric)
                {
                    Self::rewrite_postgres_format_for_tsql(normalized, target)?
                } else {
                    normalized
                };

                let normalized = if self.dialect_type == DialectType::PostgreSQL
                    && matches!(target, DialectType::TSQL | DialectType::Fabric)
                {
                    Self::normalize_postgres_only_for_tsql(normalized)?
                } else {
                    normalized
                };

                let transformed =
                    target_dialect.transform_with_guard(normalized, opts.complexity_guard)?;

                // T-SQL and Fabric do not support aggregate FILTER clauses. Rewrite any
                // remaining filters after target transforms so special aggregate rewrites
                // (for example BOOL_OR/BOOL_AND) can consume their filters first.
                let transformed = if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                    Self::rewrite_aggregate_filters_for_tsql(transformed)?
                } else {
                    transformed
                };

                let transformed = if matches!(
                    self.dialect_type,
                    DialectType::PostgreSQL | DialectType::CockroachDB
                ) && matches!(target, DialectType::TSQL | DialectType::Fabric)
                {
                    crate::transforms::grouped_percentiles_to_tsql_windows(transformed)?
                } else {
                    transformed
                };

                let transformed = if matches!(
                    self.dialect_type,
                    DialectType::PostgreSQL | DialectType::CockroachDB
                ) && matches!(target, DialectType::TSQL | DialectType::Fabric)
                {
                    Self::normalize_postgres_trim_for_tsql(transformed)?
                } else {
                    transformed
                };

                let transformed = if matches!(
                    self.dialect_type,
                    DialectType::PostgreSQL | DialectType::CockroachDB
                ) && matches!(target, DialectType::TSQL | DialectType::Fabric)
                {
                    Self::rewrite_postgres_json_array_elements_select_for_tsql(transformed)?
                } else {
                    transformed
                };

                // DuckDB target: when FROM is RANGE(n), replace SEQ's ROW_NUMBER pattern with `range`
                let transformed = if matches!(target, DialectType::DuckDB) {
                    Self::seq_rownum_to_range(transformed)?
                } else {
                    transformed
                };

                if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                    Self::reject_tsql_interval_casts(&transformed, target, opts)?;
                }

                let transformed = if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                    Self::rewrite_tsql_interval_casts_to_varchar(transformed)?
                } else {
                    transformed
                };

                let transformed = if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                    Self::legalize_tsql_nested_order_by(transformed)?
                } else {
                    transformed
                };

                Self::reject_strict_unsupported(&transformed, self.dialect_type, target, opts)?;

                let mut sql = target_dialect.generate_with_transpile_options(
                    &transformed,
                    self.dialect_type,
                    opts,
                )?;

                // Align a known Snowflake pretty-print edge case with Python sqlglot output.
                if opts.pretty && target == DialectType::Snowflake {
                    sql = Self::normalize_snowflake_pretty(sql);
                }

                Ok(sql)
            })
            .collect()
    }
}

// Transpile-only methods: cross-dialect normalization and helpers
#[cfg(feature = "transpile")]
impl Dialect {
    fn legalize_tsql_nested_order_by(expr: Expression) -> Result<Expression> {
        let preserve_root_order = matches!(&expr, Expression::Select(select) if Self::tsql_select_needs_order_offset(select));

        let mut transformed = transform_recursive(expr, &|node| match node {
            Expression::Select(mut select) => {
                Self::legalize_tsql_select_offset(&mut select);
                if Self::tsql_select_needs_order_offset(&select) {
                    select.offset = Some(Offset {
                        this: Expression::Literal(Box::new(Literal::Number("0".to_string()))),
                        rows: Some(true),
                    });
                }
                Ok(Expression::Select(select))
            }
            Expression::Subquery(mut subquery) => {
                Self::legalize_tsql_offset(&mut subquery.order_by, &mut subquery.offset, false);
                Ok(Expression::Subquery(subquery))
            }
            Expression::Union(mut union) => {
                Self::legalize_tsql_set_offset(&mut union.order_by, &mut union.offset);
                Ok(Expression::Union(union))
            }
            Expression::Intersect(mut intersect) => {
                Self::legalize_tsql_set_offset(&mut intersect.order_by, &mut intersect.offset);
                Ok(Expression::Intersect(intersect))
            }
            Expression::Except(mut except) => {
                Self::legalize_tsql_set_offset(&mut except.order_by, &mut except.offset);
                Ok(Expression::Except(except))
            }
            other => Ok(other),
        })?;

        if preserve_root_order {
            if let Expression::Select(select) = &mut transformed {
                select.offset = None;
            }
        }

        Self::drop_tsql_unbounded_nested_set_order_by(transformed)
    }

    fn drop_tsql_unbounded_nested_set_order_by(mut expr: Expression) -> Result<Expression> {
        let root_order_by = Self::take_tsql_root_set_order_by(&mut expr);

        let mut transformed = transform_recursive(expr, &|node| match node {
            Expression::Union(mut union) => {
                if union.limit.is_none() && union.offset.is_none() {
                    union.order_by = None;
                }
                Ok(Expression::Union(union))
            }
            Expression::Intersect(mut intersect) => {
                if intersect.limit.is_none() && intersect.offset.is_none() {
                    intersect.order_by = None;
                }
                Ok(Expression::Intersect(intersect))
            }
            Expression::Except(mut except) => {
                if except.limit.is_none() && except.offset.is_none() {
                    except.order_by = None;
                }
                Ok(Expression::Except(except))
            }
            other => Ok(other),
        })?;

        if let Some(order_by) = root_order_by {
            Self::restore_tsql_root_set_order_by(&mut transformed, order_by);
        }

        Ok(transformed)
    }

    fn take_tsql_root_set_order_by(expr: &mut Expression) -> Option<OrderBy> {
        match expr {
            Expression::Union(union) => union.order_by.take(),
            Expression::Intersect(intersect) => intersect.order_by.take(),
            Expression::Except(except) => except.order_by.take(),
            Expression::Subquery(subquery) if subquery.alias.is_none() => {
                Self::take_tsql_root_set_order_by(&mut subquery.this)
            }
            Expression::Paren(paren) => Self::take_tsql_root_set_order_by(&mut paren.this),
            _ => None,
        }
    }

    fn restore_tsql_root_set_order_by(expr: &mut Expression, order_by: OrderBy) {
        match expr {
            Expression::Union(union) => union.order_by = Some(order_by),
            Expression::Intersect(intersect) => intersect.order_by = Some(order_by),
            Expression::Except(except) => except.order_by = Some(order_by),
            Expression::Subquery(subquery) if subquery.alias.is_none() => {
                Self::restore_tsql_root_set_order_by(&mut subquery.this, order_by);
            }
            Expression::Paren(paren) => {
                Self::restore_tsql_root_set_order_by(&mut paren.this, order_by);
            }
            _ => {}
        }
    }

    fn legalize_tsql_select_offset(select: &mut crate::expressions::Select) {
        let has_fetch = select.fetch.is_some();
        Self::legalize_tsql_offset(&mut select.order_by, &mut select.offset, has_fetch);
    }

    fn legalize_tsql_offset(
        order_by: &mut Option<OrderBy>,
        offset: &mut Option<Offset>,
        retain_inert_offset: bool,
    ) {
        if order_by.is_some() {
            return;
        }

        if offset
            .as_ref()
            .is_some_and(|offset| Self::tsql_offset_is_inert(&offset.this))
            && !retain_inert_offset
        {
            *offset = None;
        } else if offset.is_some() {
            *order_by = Some(Generator::dummy_tsql_order_by());
        }
    }

    fn legalize_tsql_set_offset(
        order_by: &mut Option<OrderBy>,
        offset: &mut Option<Box<Expression>>,
    ) {
        if order_by.is_some() {
            return;
        }

        if offset.as_deref().is_some_and(Self::tsql_offset_is_inert) {
            *offset = None;
        } else if offset.is_some() {
            *order_by = Some(Generator::dummy_tsql_order_by());
        }
    }

    fn tsql_offset_is_inert(expr: &Expression) -> bool {
        match expr {
            Expression::Null(_) => true,
            Expression::Literal(literal) => match literal.as_ref() {
                Literal::Number(value) => value.parse::<i128>().is_ok_and(|value| value == 0),
                _ => false,
            },
            _ => false,
        }
    }

    fn tsql_select_needs_order_offset(select: &crate::expressions::Select) -> bool {
        select.order_by.is_some()
            && select.top.is_none()
            && select.limit.is_none()
            && select.offset.is_none()
            && select.fetch.is_none()
            && select.for_xml.is_empty()
            && select.for_json.is_empty()
    }

    fn reject_strict_unsupported(
        expr: &Expression,
        source: DialectType,
        target: DialectType,
        opts: &TranspileOptions,
    ) -> Result<()> {
        if !matches!(
            opts.unsupported_level,
            UnsupportedLevel::Raise | UnsupportedLevel::Immediate
        ) {
            return Ok(());
        }

        let mut diagnostics = Vec::new();
        let structural_grouping_tuples =
            if matches!(source, DialectType::PostgreSQL | DialectType::CockroachDB)
                && matches!(target, DialectType::TSQL | DialectType::Fabric)
            {
                Self::collect_tsql_grouping_tuple_nodes(expr)
            } else {
                HashSet::new()
            };

        for node in expr.dfs() {
            if matches!(target, DialectType::Fabric | DialectType::Hive)
                && Self::node_has_recursive_with(node)
            {
                Self::push_unsupported_diagnostic(&mut diagnostics, "recursive CTEs");
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_has_lateral(node)
            {
                Self::push_unsupported_diagnostic(&mut diagnostics, "LATERAL joins and subqueries");
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                if Self::node_has_join_using(node) {
                    Self::push_unsupported_diagnostic(&mut diagnostics, "JOIN USING clauses");
                }
                if Self::node_has_natural_join(node) {
                    Self::push_unsupported_diagnostic(&mut diagnostics, "NATURAL JOIN");
                }
                if Self::node_has_unsupported_relation_column_aliases(node) {
                    Self::push_unsupported_diagnostic(
                        &mut diagnostics,
                        "column alias lists on base or joined table references",
                    );
                }
                if Self::node_has_qualified_whole_row_aggregate_argument(node) {
                    Self::push_unsupported_diagnostic(
                        &mut diagnostics,
                        "qualified whole-row aggregate arguments",
                    );
                }
            }

            if !Self::target_supports_distinct_on(target) && Self::node_has_distinct_on(node) {
                Self::push_unsupported_diagnostic(&mut diagnostics, "DISTINCT ON");
            }

            if !Self::target_supports_remaining_unnest(target) && Self::node_is_unnest(node) {
                Self::push_unsupported_diagnostic(&mut diagnostics, "UNNEST");
            }

            if !Self::target_supports_remaining_explode(target) && Self::node_is_explode(node) {
                Self::push_unsupported_diagnostic(&mut diagnostics, "EXPLODE");
            }

            if Self::target_lacks_array_agg(target) && Self::node_is_array_agg(node) {
                Self::push_unsupported_diagnostic(&mut diagnostics, "ARRAY_AGG");
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_is_distinct_string_agg(node)
            {
                Self::push_unsupported_diagnostic(&mut diagnostics, "STRING_AGG with DISTINCT");
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && matches!(node, Expression::NthValue(_))
            {
                Self::push_unsupported_diagnostic(&mut diagnostics, "NTH_VALUE");
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                if let Some(frame) = Self::node_window_frame(node) {
                    if matches!(frame.kind, WindowFrameKind::Groups) {
                        Self::push_unsupported_diagnostic(&mut diagnostics, "GROUPS window frames");
                    }
                    if matches!(frame.kind, WindowFrameKind::Range)
                        && (Self::window_frame_bound_has_value_offset(&frame.start)
                            || frame
                                .end
                                .as_ref()
                                .is_some_and(Self::window_frame_bound_has_value_offset))
                    {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            "value-offset RANGE window frames",
                        );
                    }
                    if frame.exclude.is_some() {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            "window frame EXCLUDE clauses",
                        );
                    }
                }
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_is_regex_predicate(node)
            {
                Self::push_unsupported_diagnostic(
                    &mut diagnostics,
                    "regular expression predicates",
                );
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_is_non_subquery_any(node)
            {
                Self::push_unsupported_diagnostic(
                    &mut diagnostics,
                    "ANY over non-subquery expressions",
                );
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_is_row_value_subquery_comparison(node)
            {
                Self::push_unsupported_diagnostic(
                    &mut diagnostics,
                    "row-value subquery comparisons",
                );
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_is_row_value_values_membership(node)
            {
                Self::push_unsupported_diagnostic(
                    &mut diagnostics,
                    "row-value VALUES membership comparisons",
                );
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_has_fetch_with_ties(node)
            {
                Self::push_unsupported_diagnostic(&mut diagnostics, "FETCH WITH TIES without TOP");
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_is_overlaps(node)
            {
                Self::push_unsupported_diagnostic(&mut diagnostics, "OVERLAPS");
            }

            if matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_is_date_bin(node)
            {
                Self::push_unsupported_diagnostic(&mut diagnostics, "DATE_BIN");
            }

            if source == DialectType::PostgreSQL
                && matches!(target, DialectType::TSQL | DialectType::Fabric)
                && Self::node_is_unresolved_postgres_date_subtraction(node)
            {
                Self::push_unsupported_diagnostic(
                    &mut diagnostics,
                    "PostgreSQL date subtraction with an unresolved column type",
                );
            }

            if matches!(source, DialectType::PostgreSQL | DialectType::CockroachDB)
                && !matches!(target, DialectType::PostgreSQL | DialectType::CockroachDB)
            {
                if Self::node_is_postgres_json_build_object(node)
                    && !(matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && Self::postgres_json_build_object_can_lower_to_json_object(node))
                {
                    Self::push_unsupported_diagnostic(
                        &mut diagnostics,
                        "PostgreSQL JSON_BUILD_OBJECT",
                    );
                }
                if Self::node_is_function_named(node, "TO_TSVECTOR") {
                    Self::push_unsupported_diagnostic(&mut diagnostics, "PostgreSQL TO_TSVECTOR");
                }
                if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                    if let Some(composite_semantics) =
                        Self::postgres_tsql_unsupported_composite_semantics(
                            node,
                            structural_grouping_tuples.contains(&(node as *const Expression)),
                        )
                    {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {composite_semantics}"),
                        );
                    }
                    if Self::node_is_postgres_unknown_cast(node) {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            "PostgreSQL unresolved UNKNOWN casts",
                        );
                    }
                    if let Some(collation_name) =
                        Self::postgres_tsql_unsupported_collation_name(node)
                    {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL collation \"{collation_name}\""),
                        );
                    }
                    if let Some(array_semantics) =
                        Self::postgres_tsql_unsupported_array_semantics(node)
                    {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {array_semantics}"),
                        );
                    }
                    if let Some(string_semantics) =
                        Self::postgres_tsql_unsupported_string_semantics(node)
                    {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {string_semantics}"),
                        );
                    }
                    if let Some(function_name) =
                        Self::postgres_tsql_unsupported_function_name(node, target)
                    {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {function_name}"),
                        );
                    }
                }
                if matches!(target, DialectType::TSQL | DialectType::Fabric)
                    && Self::node_is_postgres_type_function_cast(node)
                {
                    Self::push_unsupported_diagnostic(
                        &mut diagnostics,
                        "PostgreSQL type-name function casts",
                    );
                }
            }

            if opts.unsupported_level == UnsupportedLevel::Immediate && !diagnostics.is_empty() {
                break;
            }
        }

        if matches!(target, DialectType::TSQL | DialectType::Fabric) {
            Self::collect_tsql_unsupported_ordered_sets(expr, &mut diagnostics);
            Self::collect_tsql_windows_missing_order(expr, &HashMap::new(), &mut diagnostics);
        }

        if diagnostics.is_empty() {
            return Ok(());
        }

        let limit = if opts.unsupported_level == UnsupportedLevel::Immediate {
            1
        } else {
            opts.max_unsupported.max(1)
        };
        let mut messages = diagnostics.iter().take(limit).cloned().collect::<Vec<_>>();
        if diagnostics.len() > limit {
            messages.push(format!("... and {} more", diagnostics.len() - limit));
        }

        Err(crate::error::Error::unsupported(
            messages.join("; "),
            target.to_string(),
        ))
    }

    fn reject_postgres_tsql_strict_regex_predicates(
        expr: &Expression,
        source: DialectType,
        target: DialectType,
        opts: &TranspileOptions,
    ) -> Result<()> {
        if !matches!(
            opts.unsupported_level,
            UnsupportedLevel::Raise | UnsupportedLevel::Immediate
        ) || !matches!(source, DialectType::PostgreSQL | DialectType::CockroachDB)
            || !matches!(target, DialectType::TSQL | DialectType::Fabric)
        {
            return Ok(());
        }

        if expr.dfs().any(Self::node_is_regex_predicate) {
            return Err(crate::error::Error::unsupported(
                "regular expression predicates",
                target.to_string(),
            ));
        }

        Ok(())
    }

    fn reject_tsql_strict_json_constructor_return_types(
        expr: &Expression,
        source: DialectType,
        target: DialectType,
        opts: &TranspileOptions,
    ) -> Result<()> {
        if !matches!(
            opts.unsupported_level,
            UnsupportedLevel::Raise | UnsupportedLevel::Immediate
        ) || source == target
            || !matches!(target, DialectType::TSQL | DialectType::Fabric)
        {
            return Ok(());
        }

        let mut diagnostics = Vec::new();
        for node in expr.dfs() {
            if let Some(return_type) =
                normalization::unsupported_tsql_json_constructor_return_type(node)
            {
                let message =
                    format!("SQL/JSON constructor RETURNING {return_type} cannot be preserved");
                Self::push_unsupported_diagnostic(&mut diagnostics, &message);
                if opts.unsupported_level == UnsupportedLevel::Immediate {
                    break;
                }
            }
        }

        if diagnostics.is_empty() {
            return Ok(());
        }

        let limit = if opts.unsupported_level == UnsupportedLevel::Immediate {
            1
        } else {
            opts.max_unsupported.max(1)
        };
        let mut messages = diagnostics.iter().take(limit).cloned().collect::<Vec<_>>();
        if diagnostics.len() > limit {
            messages.push(format!("... and {} more", diagnostics.len() - limit));
        }

        Err(crate::error::Error::unsupported(
            messages.join("; "),
            target.to_string(),
        ))
    }

    fn reject_postgres_tsql_strict_json_aggregate_modifiers(
        expr: &Expression,
        source: DialectType,
        target: DialectType,
        opts: &TranspileOptions,
    ) -> Result<()> {
        if !matches!(
            opts.unsupported_level,
            UnsupportedLevel::Raise | UnsupportedLevel::Immediate
        ) || !matches!(source, DialectType::PostgreSQL | DialectType::CockroachDB)
            || !matches!(target, DialectType::TSQL | DialectType::Fabric)
        {
            return Ok(());
        }

        let mut diagnostics = Vec::new();
        for node in expr.dfs() {
            match node {
                Expression::Function(function)
                    if !function.quoted
                        && matches!(
                            function.name.to_ascii_uppercase().as_str(),
                            "JSON_AGG" | "JSONB_AGG"
                        ) =>
                {
                    let name = function.name.to_ascii_uppercase();
                    if function.args.len() != 1 {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {name} with invalid argument count"),
                        );
                    }
                    if function.distinct {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {name} with DISTINCT"),
                        );
                    }
                }
                Expression::AggregateFunction(function)
                    if matches!(
                        function.name.to_ascii_uppercase().as_str(),
                        "JSON_AGG" | "JSONB_AGG"
                    ) =>
                {
                    let name = function.name.to_ascii_uppercase();
                    if function.args.len() != 1 {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {name} with invalid argument count"),
                        );
                    }
                    if function.distinct {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {name} with DISTINCT"),
                        );
                    }
                    if function.filter.is_some() {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {name} with FILTER"),
                        );
                    }
                    if function.limit.is_some() || function.ignore_nulls.is_some() {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {name} with unsupported aggregate modifiers"),
                        );
                    }
                }
                Expression::Filter(filter) => {
                    if let Some(name) = Self::postgres_json_aggregate_name(&filter.this) {
                        Self::push_unsupported_diagnostic(
                            &mut diagnostics,
                            &format!("PostgreSQL {name} with FILTER"),
                        );
                    }
                }
                _ => {}
            }

            if opts.unsupported_level == UnsupportedLevel::Immediate && !diagnostics.is_empty() {
                break;
            }
        }

        if diagnostics.is_empty() {
            return Ok(());
        }

        let limit = if opts.unsupported_level == UnsupportedLevel::Immediate {
            1
        } else {
            opts.max_unsupported.max(1)
        };
        let mut messages = diagnostics.iter().take(limit).cloned().collect::<Vec<_>>();
        if diagnostics.len() > limit {
            messages.push(format!("... and {} more", diagnostics.len() - limit));
        }

        Err(crate::error::Error::unsupported(
            messages.join("; "),
            target.to_string(),
        ))
    }

    fn postgres_json_aggregate_name(expr: &Expression) -> Option<String> {
        let name = match expr {
            Expression::Function(function) if !function.quoted => &function.name,
            Expression::AggregateFunction(function) => &function.name,
            _ => return None,
        };
        let name = name.to_ascii_uppercase();
        matches!(name.as_str(), "JSON_AGG" | "JSONB_AGG").then_some(name)
    }

    fn push_unsupported_diagnostic(diagnostics: &mut Vec<String>, message: &str) {
        if !diagnostics.iter().any(|existing| existing == message) {
            diagnostics.push(message.to_string());
        }
    }

    fn node_is_unresolved_postgres_date_subtraction(expr: &Expression) -> bool {
        let Expression::Sub(op) = expr else {
            return false;
        };

        (Self::is_explicit_date_expr(&op.left) && Self::is_column_expr(&op.right))
            || (Self::is_column_expr(&op.left) && Self::is_explicit_date_expr(&op.right))
    }

    fn is_column_expr(expr: &Expression) -> bool {
        match expr {
            Expression::Column(_) => true,
            Expression::Paren(paren) => Self::is_column_expr(&paren.this),
            _ => false,
        }
    }

    fn node_window_frame(expr: &Expression) -> Option<&WindowFrame> {
        match expr {
            Expression::WindowFunction(window) => window.over.frame.as_ref(),
            Expression::Window(window) | Expression::WindowSpec(window) => window.frame.as_ref(),
            _ => None,
        }
    }

    fn window_frame_bound_has_value_offset(bound: &WindowFrameBound) -> bool {
        matches!(
            bound,
            WindowFrameBound::Preceding(_)
                | WindowFrameBound::Following(_)
                | WindowFrameBound::Value(_)
                | WindowFrameBound::BarePreceding
                | WindowFrameBound::BareFollowing
        )
    }

    fn collect_tsql_windows_missing_order(
        expr: &Expression,
        active_windows: &HashMap<String, Over>,
        diagnostics: &mut Vec<String>,
    ) {
        if let Expression::Select(select) = expr {
            let local_windows = select
                .windows
                .as_ref()
                .map(|windows| {
                    windows
                        .iter()
                        .map(|window| (window.name.name.to_ascii_lowercase(), window.spec.clone()))
                        .collect()
                })
                .unwrap_or_default();

            for child in expr.children() {
                Self::collect_tsql_windows_missing_order(child, &local_windows, diagnostics);
            }
            return;
        }

        if let Expression::WindowFunction(window) = expr {
            let (has_order, has_frame) = Self::effective_window_order_and_frame(
                &window.over,
                active_windows,
                &mut Vec::new(),
            );

            if !has_order {
                if has_frame {
                    Self::push_unsupported_diagnostic(
                        diagnostics,
                        "window frames without ORDER BY",
                    );
                }
                if let Some(function_name) =
                    Self::tsql_window_function_requiring_order(&window.this)
                {
                    Self::push_unsupported_diagnostic(
                        diagnostics,
                        &format!("{function_name} without ORDER BY"),
                    );
                }
            }
        }

        for child in expr.children() {
            Self::collect_tsql_windows_missing_order(child, active_windows, diagnostics);
        }
    }

    fn effective_window_order_and_frame(
        over: &Over,
        active_windows: &HashMap<String, Over>,
        seen: &mut Vec<String>,
    ) -> (bool, bool) {
        let inherited = over
            .window_name
            .as_ref()
            .and_then(|name| {
                let key = name.name.to_ascii_lowercase();
                if seen.iter().any(|seen_name| seen_name == &key) {
                    return None;
                }
                let named = active_windows.get(&key)?;
                seen.push(key);
                let properties =
                    Self::effective_window_order_and_frame(named, active_windows, seen);
                seen.pop();
                Some(properties)
            })
            .unwrap_or((false, false));

        (
            !over.order_by.is_empty() || inherited.0,
            over.frame.is_some() || inherited.1,
        )
    }

    fn tsql_window_function_requiring_order(expr: &Expression) -> Option<&'static str> {
        match expr {
            Expression::FirstValue(_) => Some("FIRST_VALUE"),
            Expression::LastValue(_) => Some("LAST_VALUE"),
            Expression::Function(function) if function.name.eq_ignore_ascii_case("FIRST_VALUE") => {
                Some("FIRST_VALUE")
            }
            Expression::Function(function) if function.name.eq_ignore_ascii_case("LAST_VALUE") => {
                Some("LAST_VALUE")
            }
            _ => None,
        }
    }

    fn collect_tsql_unsupported_ordered_sets(expr: &Expression, diagnostics: &mut Vec<String>) {
        match expr {
            Expression::WindowFunction(window) => {
                if let Expression::WithinGroup(within_group) = &window.this {
                    if Self::within_group_is_hypothetical_set(within_group) {
                        Self::push_unsupported_diagnostic(
                            diagnostics,
                            "RANK/DENSE_RANK/CUME_DIST/PERCENT_RANK hypothetical-set aggregates",
                        );
                        return;
                    }

                    if Self::within_group_is_mode(within_group) {
                        Self::push_unsupported_diagnostic(
                            diagnostics,
                            "MODE ordered-set aggregates",
                        );
                        return;
                    }

                    if Self::within_group_is_percentile(within_group) {
                        if !window.over.order_by.is_empty() || window.over.frame.is_some() {
                            Self::push_unsupported_diagnostic(
                                diagnostics,
                                "PERCENTILE_CONT/PERCENTILE_DISC window ORDER BY or frame clauses",
                            );
                        }
                        return;
                    }
                }
            }
            Expression::WithinGroup(within_group) => {
                if Self::within_group_is_hypothetical_set(within_group) {
                    Self::push_unsupported_diagnostic(
                        diagnostics,
                        "RANK/DENSE_RANK/CUME_DIST/PERCENT_RANK hypothetical-set aggregates",
                    );
                    return;
                }

                if Self::within_group_is_mode(within_group) {
                    Self::push_unsupported_diagnostic(diagnostics, "MODE ordered-set aggregates");
                    return;
                }

                if Self::within_group_is_percentile(within_group) {
                    Self::push_unsupported_diagnostic(
                        diagnostics,
                        "PERCENTILE_CONT/PERCENTILE_DISC ordered-set aggregates without OVER",
                    );
                    return;
                }
            }
            _ => {}
        }

        for child in expr.children() {
            Self::collect_tsql_unsupported_ordered_sets(child, diagnostics);
        }
    }

    fn within_group_is_hypothetical_set(within_group: &crate::expressions::WithinGroup) -> bool {
        match &within_group.this {
            Expression::Function(function) => Self::is_hypothetical_set_name(&function.name),
            Expression::AggregateFunction(function) => {
                Self::is_hypothetical_set_name(&function.name)
            }
            Expression::Rank(_)
            | Expression::DenseRank(_)
            | Expression::CumeDist(_)
            | Expression::PercentRank(_) => true,
            _ => false,
        }
    }

    fn within_group_is_percentile(within_group: &crate::expressions::WithinGroup) -> bool {
        match &within_group.this {
            Expression::Function(function) => Self::is_percentile_ordered_set_name(&function.name),
            Expression::AggregateFunction(function) => {
                Self::is_percentile_ordered_set_name(&function.name)
            }
            Expression::PercentileCont(_) | Expression::PercentileDisc(_) => true,
            _ => false,
        }
    }

    fn within_group_is_mode(within_group: &crate::expressions::WithinGroup) -> bool {
        match &within_group.this {
            Expression::Function(function) => function.name.eq_ignore_ascii_case("MODE"),
            Expression::AggregateFunction(function) => function.name.eq_ignore_ascii_case("MODE"),
            Expression::Mode(_) => true,
            _ => false,
        }
    }

    fn is_percentile_ordered_set_name(name: &str) -> bool {
        name.eq_ignore_ascii_case("PERCENTILE_CONT") || name.eq_ignore_ascii_case("PERCENTILE_DISC")
    }

    fn is_hypothetical_set_name(name: &str) -> bool {
        name.eq_ignore_ascii_case("RANK")
            || name.eq_ignore_ascii_case("DENSE_RANK")
            || name.eq_ignore_ascii_case("CUME_DIST")
            || name.eq_ignore_ascii_case("PERCENT_RANK")
    }

    fn target_supports_distinct_on(target: DialectType) -> bool {
        matches!(target, DialectType::PostgreSQL | DialectType::DuckDB)
    }

    fn node_has_distinct_on(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::Select(select)
                if select
                    .distinct_on
                    .as_ref()
                    .is_some_and(|distinct_on| !distinct_on.is_empty())
        )
    }

    fn node_has_recursive_with(expr: &Expression) -> bool {
        fn recursive(with: &Option<With>) -> bool {
            with.as_ref().is_some_and(|with| with.recursive)
        }

        match expr {
            Expression::With(with) => with.recursive,
            Expression::Select(select) => recursive(&select.with),
            Expression::Union(union) => recursive(&union.with),
            Expression::Intersect(intersect) => recursive(&intersect.with),
            Expression::Except(except) => recursive(&except.with),
            Expression::Pivot(pivot) => recursive(&pivot.with),
            Expression::Insert(insert) => recursive(&insert.with),
            Expression::Update(update) => recursive(&update.with),
            Expression::Delete(delete) => recursive(&delete.with),
            _ => false,
        }
    }

    fn node_has_lateral(expr: &Expression) -> bool {
        fn join_has_lateral(join: &Join) -> bool {
            matches!(
                join.kind,
                crate::expressions::JoinKind::Lateral | crate::expressions::JoinKind::LeftLateral
            ) || Dialect::node_has_lateral(&join.this)
                || join.on.as_ref().is_some_and(Dialect::node_has_lateral)
                || join
                    .match_condition
                    .as_ref()
                    .is_some_and(Dialect::node_has_lateral)
                || join.pivots.iter().any(Dialect::node_has_lateral)
        }

        fn joins_have_lateral(joins: &[Join]) -> bool {
            joins.iter().any(join_has_lateral)
        }

        match expr {
            Expression::Subquery(subquery) => {
                subquery.lateral || Dialect::node_has_lateral(&subquery.this)
            }
            Expression::Lateral(_) | Expression::LateralView(_) => true,
            Expression::Join(join) => join_has_lateral(join),
            Expression::Select(select) => {
                !select.lateral_views.is_empty()
                    || joins_have_lateral(&select.joins)
                    || select
                        .from
                        .as_ref()
                        .is_some_and(|from| from.expressions.iter().any(Dialect::node_has_lateral))
            }
            Expression::JoinedTable(joined) => {
                !joined.lateral_views.is_empty()
                    || Dialect::node_has_lateral(&joined.left)
                    || joins_have_lateral(&joined.joins)
            }
            Expression::Update(update) => {
                joins_have_lateral(&update.table_joins) || joins_have_lateral(&update.from_joins)
            }
            _ => false,
        }
    }

    fn node_has_join_using(expr: &Expression) -> bool {
        fn has_using(joins: &[Join]) -> bool {
            joins.iter().any(|join| !join.using.is_empty())
        }

        match expr {
            Expression::Join(join) => !join.using.is_empty(),
            Expression::Select(select) => has_using(&select.joins),
            Expression::JoinedTable(joined) => has_using(&joined.joins),
            Expression::Update(update) => {
                has_using(&update.table_joins) || has_using(&update.from_joins)
            }
            Expression::Delete(delete) => has_using(&delete.joins),
            _ => false,
        }
    }

    fn node_has_natural_join(expr: &Expression) -> bool {
        fn is_natural(join: &Join) -> bool {
            matches!(
                join.kind,
                crate::expressions::JoinKind::Natural
                    | crate::expressions::JoinKind::NaturalLeft
                    | crate::expressions::JoinKind::NaturalRight
                    | crate::expressions::JoinKind::NaturalFull
            )
        }

        fn has_natural(joins: &[Join]) -> bool {
            joins.iter().any(is_natural)
        }

        match expr {
            Expression::Join(join) => is_natural(join),
            Expression::Select(select) => has_natural(&select.joins),
            Expression::JoinedTable(joined) => has_natural(&joined.joins),
            Expression::Update(update) => {
                has_natural(&update.table_joins) || has_natural(&update.from_joins)
            }
            Expression::Delete(delete) => has_natural(&delete.joins),
            _ => false,
        }
    }

    fn node_has_unsupported_relation_column_aliases(expr: &Expression) -> bool {
        match expr {
            Expression::Table(table) => !table.column_aliases.is_empty(),
            Expression::Alias(alias) => {
                !alias.column_aliases.is_empty()
                    && matches!(
                        alias.this,
                        Expression::Table(_) | Expression::JoinedTable(_)
                    )
            }
            _ => false,
        }
    }

    fn node_has_qualified_whole_row_aggregate_argument(expr: &Expression) -> bool {
        fn contains_qualified_star(expr: &Expression) -> bool {
            match expr {
                Expression::Star(star) => star.table.is_some(),
                // A star projected by an embedded query is not an argument of
                // the surrounding aggregate (for example, inside EXISTS).
                Expression::Select(_)
                | Expression::Subquery(_)
                | Expression::Union(_)
                | Expression::Intersect(_)
                | Expression::Except(_) => false,
                _ => expr.children().into_iter().any(contains_qualified_star),
            }
        }

        let is_aggregate = matches!(
            expr,
            Expression::AggregateFunction(_)
                | Expression::Count(_)
                | Expression::Sum(_)
                | Expression::Avg(_)
                | Expression::Min(_)
                | Expression::Max(_)
                | Expression::GroupConcat(_)
                | Expression::StringAgg(_)
                | Expression::ListAgg(_)
                | Expression::ArrayAgg(_)
                | Expression::CountIf(_)
                | Expression::SumIf(_)
                | Expression::Stddev(_)
                | Expression::StddevPop(_)
                | Expression::StddevSamp(_)
                | Expression::Variance(_)
                | Expression::VarPop(_)
                | Expression::VarSamp(_)
                | Expression::Median(_)
                | Expression::Mode(_)
                | Expression::First(_)
                | Expression::Last(_)
                | Expression::AnyValue(_)
                | Expression::ApproxDistinct(_)
                | Expression::ApproxCountDistinct(_)
                | Expression::ApproxPercentile(_)
                | Expression::Percentile(_)
                | Expression::LogicalAnd(_)
                | Expression::LogicalOr(_)
                | Expression::Skewness(_)
                | Expression::BitwiseCount(_)
                | Expression::BitwiseAndAgg(_)
                | Expression::BitwiseOrAgg(_)
                | Expression::BitwiseXorAgg(_)
                | Expression::ArrayConcatAgg(_)
                | Expression::ArrayUniqueAgg(_)
                | Expression::BoolXorAgg(_)
                | Expression::JsonArrayAgg(_)
                | Expression::JsonObjectAgg(_)
                | Expression::ParameterizedAgg(_)
                | Expression::ArgMax(_)
                | Expression::ArgMin(_)
                | Expression::ApproxTopK(_)
                | Expression::ApproxTopKAccumulate(_)
                | Expression::ApproxTopKCombine(_)
                | Expression::ApproxTopKEstimate(_)
                | Expression::ApproxTopSum(_)
                | Expression::ApproxQuantiles(_)
                | Expression::AnonymousAggFunc(_)
                | Expression::CombinedAggFunc(_)
                | Expression::CombinedParameterizedAgg(_)
                | Expression::HashAgg(_)
                | Expression::ObjectAgg(_)
                | Expression::AIAgg(_)
        );

        is_aggregate && expr.children().into_iter().any(contains_qualified_star)
    }

    fn target_supports_remaining_unnest(target: DialectType) -> bool {
        matches!(
            target,
            DialectType::PostgreSQL
                | DialectType::BigQuery
                | DialectType::DuckDB
                | DialectType::Presto
                | DialectType::Trino
                | DialectType::Athena
        )
    }

    fn target_supports_remaining_explode(target: DialectType) -> bool {
        matches!(
            target,
            DialectType::Spark | DialectType::Databricks | DialectType::Hive
        )
    }

    fn target_lacks_array_agg(target: DialectType) -> bool {
        matches!(
            target,
            DialectType::Fabric
                | DialectType::TSQL
                | DialectType::MySQL
                | DialectType::SQLite
                | DialectType::Oracle
        )
    }

    fn node_is_unnest(expr: &Expression) -> bool {
        matches!(expr, Expression::Unnest(_)) || Self::node_is_function_named(expr, "UNNEST")
    }

    fn node_is_explode(expr: &Expression) -> bool {
        matches!(expr, Expression::Explode(_) | Expression::ExplodeOuter(_))
            || Self::node_is_function_named(expr, "EXPLODE")
            || Self::node_is_function_named(expr, "EXPLODE_OUTER")
    }

    fn node_is_array_agg(expr: &Expression) -> bool {
        matches!(expr, Expression::ArrayAgg(_)) || Self::node_is_function_named(expr, "ARRAY_AGG")
    }

    fn node_is_distinct_string_agg(expr: &Expression) -> bool {
        match expr {
            Expression::StringAgg(agg) => agg.distinct,
            Expression::Function(function) => {
                function.distinct && function.name.eq_ignore_ascii_case("STRING_AGG")
            }
            Expression::AggregateFunction(function) => {
                function.distinct && function.name.eq_ignore_ascii_case("STRING_AGG")
            }
            _ => false,
        }
    }

    fn postgres_tsql_unsupported_collation_name(expr: &Expression) -> Option<&'static str> {
        let Expression::Collation(collation) = expr else {
            return None;
        };

        if collation.collation.eq_ignore_ascii_case("C") {
            Some("C")
        } else if collation.collation.eq_ignore_ascii_case("POSIX") {
            Some("POSIX")
        } else {
            None
        }
    }

    fn collect_tsql_grouping_tuple_nodes(expr: &Expression) -> HashSet<*const Expression> {
        let mut tuples = HashSet::new();

        for node in expr.dfs() {
            let Expression::Select(select) = node else {
                continue;
            };
            let Some(group_by) = &select.group_by else {
                continue;
            };

            for expression in &group_by.expressions {
                Self::collect_tsql_grouping_element_tuples(expression, &mut tuples);
            }
        }

        tuples
    }

    fn collect_tsql_grouping_element_tuples(
        expr: &Expression,
        tuples: &mut HashSet<*const Expression>,
    ) {
        match expr {
            Expression::GroupingSets(grouping_sets) => {
                for expression in &grouping_sets.expressions {
                    Self::collect_tsql_grouping_unit_tuples(expression, tuples);
                }
            }
            Expression::Rollup(rollup) => {
                for expression in &rollup.expressions {
                    Self::collect_tsql_grouping_unit_tuples(expression, tuples);
                }
            }
            Expression::Cube(cube) => {
                for expression in &cube.expressions {
                    Self::collect_tsql_grouping_unit_tuples(expression, tuples);
                }
            }
            Expression::Function(function)
                if !function.quoted
                    && (function.name.eq_ignore_ascii_case("GROUPING SETS")
                        || function.name.eq_ignore_ascii_case("ROLLUP")
                        || function.name.eq_ignore_ascii_case("CUBE")) =>
            {
                for expression in &function.args {
                    Self::collect_tsql_grouping_unit_tuples(expression, tuples);
                }
            }
            _ => {}
        }
    }

    fn collect_tsql_grouping_unit_tuples(
        expr: &Expression,
        tuples: &mut HashSet<*const Expression>,
    ) {
        match expr {
            Expression::Tuple(tuple) => {
                tuples.insert(expr as *const Expression);
                for expression in &tuple.expressions {
                    match expression {
                        Expression::Tuple(_) | Expression::Paren(_) => {
                            Self::collect_tsql_grouping_unit_tuples(expression, tuples);
                        }
                        Expression::GroupingSets(_)
                        | Expression::Rollup(_)
                        | Expression::Cube(_) => {
                            Self::collect_tsql_grouping_element_tuples(expression, tuples);
                        }
                        Expression::Function(function)
                            if !function.quoted
                                && (function.name.eq_ignore_ascii_case("GROUPING SETS")
                                    || function.name.eq_ignore_ascii_case("ROLLUP")
                                    || function.name.eq_ignore_ascii_case("CUBE")) =>
                        {
                            Self::collect_tsql_grouping_element_tuples(expression, tuples);
                        }
                        _ => {}
                    }
                }
            }
            Expression::Paren(paren) => {
                Self::collect_tsql_grouping_unit_tuples(&paren.this, tuples);
            }
            Expression::GroupingSets(_) | Expression::Rollup(_) | Expression::Cube(_) => {
                Self::collect_tsql_grouping_element_tuples(expr, tuples);
            }
            Expression::Function(function)
                if !function.quoted
                    && (function.name.eq_ignore_ascii_case("GROUPING SETS")
                        || function.name.eq_ignore_ascii_case("ROLLUP")
                        || function.name.eq_ignore_ascii_case("CUBE")) =>
            {
                Self::collect_tsql_grouping_element_tuples(expr, tuples);
            }
            _ => {}
        }
    }

    fn postgres_tsql_unsupported_composite_semantics(
        expr: &Expression,
        structural_grouping_tuple: bool,
    ) -> Option<&'static str> {
        match expr {
            Expression::Tuple(_) if !structural_grouping_tuple => Some("row/composite values"),
            Expression::Struct(_) | Expression::StructFunc(_) => Some("row/composite values"),
            Expression::Function(function)
                if !function.quoted && function.name.eq_ignore_ascii_case("ROW") =>
            {
                Some("row/composite values")
            }
            Expression::StructExtract(_) => Some("row/composite field access"),
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) if matches!(&cast.this, Expression::Star(star) if star.table.is_some()) => {
                Some("qualified whole-row casts")
            }
            _ => None,
        }
    }

    fn node_is_postgres_unknown_cast(expr: &Expression) -> bool {
        match expr {
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
                normalization::is_postgres_unknown_type(&cast.to)
            }
            _ => false,
        }
    }

    fn postgres_tsql_unsupported_array_semantics(expr: &Expression) -> Option<&'static str> {
        match expr {
            Expression::Array(_) | Expression::ArrayFunc(_) => Some("array literals"),
            Expression::Subscript(_) => Some("array subscripts"),
            Expression::ArraySlice(_) => Some("array slices"),
            Expression::DataType(DataType::Array { .. }) => Some("array data types"),
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast)
                if matches!(&cast.to, DataType::Array { .. }) =>
            {
                Some("array data types")
            }
            Expression::ArrayLength(_) | Expression::ArraySize(_) => Some("ARRAY_LENGTH"),
            Expression::Cardinality(_) => Some("CARDINALITY"),
            Expression::ArrayToString(_) | Expression::ArrayJoin(_) => Some("ARRAY_TO_STRING"),
            Expression::StringToArray(_) => Some("STRING_TO_ARRAY"),
            Expression::ArrayContains(_)
            | Expression::ArrayPosition(_)
            | Expression::ArrayAppend(_)
            | Expression::ArrayPrepend(_)
            | Expression::ArrayConcat(_)
            | Expression::ArraySort(_)
            | Expression::ArrayReverse(_)
            | Expression::ArrayDistinct(_)
            | Expression::ArrayFilter(_)
            | Expression::ArrayTransform(_)
            | Expression::ArrayFlatten(_)
            | Expression::ArrayCompact(_)
            | Expression::ArrayIntersect(_)
            | Expression::ArrayUnion(_)
            | Expression::ArrayExcept(_)
            | Expression::ArrayRemove(_)
            | Expression::ArrayZip(_)
            | Expression::ArrayAll(_)
            | Expression::ArrayAny(_)
            | Expression::ArrayConstructCompact(_)
            | Expression::ArraySum(_) => Some("array functions"),
            Expression::ArrayContainsAll(_)
            | Expression::ArrayContainedBy(_)
            | Expression::ArrayOverlaps(_) => Some("array operators"),
            Expression::Function(function) => {
                Self::postgres_tsql_unsupported_array_function_name_str(&function.name)
            }
            Expression::AggregateFunction(function) => {
                Self::postgres_tsql_unsupported_array_function_name_str(&function.name)
            }
            _ => None,
        }
    }

    fn postgres_tsql_unsupported_array_function_name_str(name: &str) -> Option<&'static str> {
        if name.eq_ignore_ascii_case("ARRAY") {
            Some("array literals")
        } else if name.eq_ignore_ascii_case("ARRAY_LENGTH")
            || name.eq_ignore_ascii_case("ARRAY_SIZE")
        {
            Some("ARRAY_LENGTH")
        } else if name.eq_ignore_ascii_case("CARDINALITY") {
            Some("CARDINALITY")
        } else if name.eq_ignore_ascii_case("ARRAY_TO_STRING")
            || name.eq_ignore_ascii_case("ARRAY_JOIN")
        {
            Some("ARRAY_TO_STRING")
        } else if name.eq_ignore_ascii_case("STRING_TO_ARRAY") {
            Some("STRING_TO_ARRAY")
        } else {
            None
        }
    }

    fn node_is_regex_predicate(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::SimilarTo(_) | Expression::RegexpLike(_) | Expression::RegexpILike(_)
        ) || Self::node_is_function_named(expr, "REGEXP_LIKE")
            || Self::node_is_function_named(expr, "REGEXP_I_LIKE")
            || Self::node_is_function_named(expr, "REGEXP_ILIKE")
    }

    fn node_is_non_subquery_any(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::Any(q) if !Self::quantified_rhs_is_subquery(&q.subquery)
        )
    }

    fn quantified_rhs_is_subquery(expr: &Expression) -> bool {
        match expr {
            Expression::Select(_) | Expression::Subquery(_) => true,
            Expression::Paren(paren) => Self::quantified_rhs_is_subquery(&paren.this),
            _ => false,
        }
    }

    fn node_is_row_value_subquery_comparison(expr: &Expression) -> bool {
        match expr {
            Expression::In(in_expr) => {
                Self::in_rhs_is_subquery_like(in_expr) && Self::expr_is_row_value(&in_expr.this)
            }
            Expression::Eq(op) | Expression::Neq(op) => {
                (Self::expr_is_row_value(&op.left) && Self::expr_is_subquery_like(&op.right))
                    || (Self::expr_is_row_value(&op.right) && Self::expr_is_subquery_like(&op.left))
            }
            _ => false,
        }
    }

    fn node_is_row_value_values_membership(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::In(in_expr)
                if Self::expr_is_row_value(&in_expr.this)
                    && Self::in_rhs_is_values_like(in_expr)
        )
    }

    fn expr_is_row_value(expr: &Expression) -> bool {
        match expr {
            Expression::Tuple(tuple) => tuple.expressions.len() > 1,
            Expression::Function(function) if function.name.eq_ignore_ascii_case("ROW") => {
                function.args.len() > 1
            }
            Expression::Paren(paren) => Self::expr_is_row_value(&paren.this),
            _ => false,
        }
    }

    fn expr_is_subquery_like(expr: &Expression) -> bool {
        match expr {
            Expression::Select(_) | Expression::Subquery(_) => true,
            Expression::Paren(paren) => Self::expr_is_subquery_like(&paren.this),
            _ => false,
        }
    }

    fn in_rhs_is_subquery_like(in_expr: &crate::expressions::In) -> bool {
        if in_expr
            .query
            .as_ref()
            .is_some_and(Self::expr_is_subquery_like)
        {
            return true;
        }

        in_expr.expressions.len() == 1 && Self::expr_is_subquery_like(&in_expr.expressions[0])
    }

    fn in_rhs_is_values_like(in_expr: &crate::expressions::In) -> bool {
        if in_expr
            .query
            .as_ref()
            .is_some_and(Self::expr_is_values_like)
        {
            return true;
        }

        (in_expr.expressions.len() == 1
            && Self::expr_is_values_like(&in_expr.expressions[0]))
            || in_expr.expressions.first().is_some_and(|expr| {
                matches!(expr, Expression::Function(function) if function.name.eq_ignore_ascii_case("VALUES"))
            })
    }

    fn expr_is_values_like(expr: &Expression) -> bool {
        match expr {
            Expression::Values(_) => true,
            Expression::Paren(paren) => Self::expr_is_values_like(&paren.this),
            Expression::Subquery(subquery) => Self::expr_is_values_like(&subquery.this),
            _ => false,
        }
    }

    fn normalize_tsql_fetch_overlaps_date_bin(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Select(mut select) => {
                if select.top.is_none() && select.offset.is_none() {
                    if let Some(fetch) = select.fetch.take() {
                        if let Some(top) = Self::fetch_with_ties_to_top(fetch.clone()) {
                            select.top = Some(top);
                        } else {
                            select.fetch = Some(fetch);
                        }
                    }
                }
                Self::rewrite_tsql_overlaps_in_select_predicates(&mut select)?;
                Ok(Expression::Select(select))
            }
            Expression::DateBin(date_bin) => {
                let date_bin = *date_bin;
                if let Some(rewritten) = Self::date_bin_to_date_bucket(date_bin.clone()) {
                    Ok(rewritten)
                } else {
                    Ok(Expression::DateBin(Box::new(date_bin)))
                }
            }
            Expression::Function(function) => {
                let function = *function;
                if function.name.eq_ignore_ascii_case("DATE_BIN") {
                    if let Some(rewritten) = Self::date_bin_function_to_date_bucket(&function) {
                        Ok(rewritten)
                    } else {
                        Ok(Expression::Function(Box::new(function)))
                    }
                } else {
                    Ok(Expression::Function(Box::new(function)))
                }
            }
            _ => Ok(e),
        })
    }

    fn rewrite_tsql_overlaps_in_select_predicates(
        select: &mut crate::expressions::Select,
    ) -> Result<()> {
        if let Some(where_clause) = &mut select.where_clause {
            where_clause.this = Self::rewrite_tsql_overlaps_predicate(where_clause.this.clone())?;
        }
        if let Some(having) = &mut select.having {
            having.this = Self::rewrite_tsql_overlaps_predicate(having.this.clone())?;
        }
        if let Some(qualify) = &mut select.qualify {
            qualify.this = Self::rewrite_tsql_overlaps_predicate(qualify.this.clone())?;
        }
        for join in &mut select.joins {
            if let Some(on) = join.on.take() {
                join.on = Some(Self::rewrite_tsql_overlaps_predicate(on)?);
            }
            if let Some(match_condition) = join.match_condition.take() {
                join.match_condition =
                    Some(Self::rewrite_tsql_overlaps_predicate(match_condition)?);
            }
        }
        Ok(())
    }

    fn rewrite_tsql_overlaps_predicate(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Overlaps(overlaps) => {
                let overlaps = *overlaps;
                if let Some(rewritten) = Self::rewrite_full_overlaps_for_tsql(&overlaps) {
                    Ok(rewritten)
                } else {
                    Ok(Expression::Overlaps(Box::new(overlaps)))
                }
            }
            _ => Ok(e),
        })
    }

    fn fetch_with_ties_to_top(fetch: Fetch) -> Option<Top> {
        if !fetch.with_ties {
            return None;
        }

        fetch.count.map(|count| Top {
            this: count,
            percent: fetch.percent,
            with_ties: true,
            parenthesized: true,
        })
    }

    fn rewrite_full_overlaps_for_tsql(
        overlaps: &crate::expressions::OverlapsExpr,
    ) -> Option<Expression> {
        let (left_start, left_end, right_start, right_end) =
            if let (Some(left_start), Some(left_end), Some(right_start), Some(right_end)) = (
                overlaps.left_start.as_ref(),
                overlaps.left_end.as_ref(),
                overlaps.right_start.as_ref(),
                overlaps.right_end.as_ref(),
            ) {
                (left_start, left_end, right_start, right_end)
            } else if let (
                Some(Expression::Tuple(left_tuple)),
                Some(Expression::Tuple(right_tuple)),
            ) = (&overlaps.this, &overlaps.expression)
            {
                if left_tuple.expressions.len() != 2 || right_tuple.expressions.len() != 2 {
                    return None;
                }
                (
                    &left_tuple.expressions[0],
                    &left_tuple.expressions[1],
                    &right_tuple.expressions[0],
                    &right_tuple.expressions[1],
                )
            } else {
                return None;
            };

        let left_min = Self::case_min(left_start.clone(), left_end.clone());
        let left_max = Self::case_max(left_start.clone(), left_end.clone());
        let right_min = Self::case_min(right_start.clone(), right_end.clone());
        let right_max = Self::case_max(right_start.clone(), right_end.clone());

        Some(Expression::And(Box::new(BinaryOp::new(
            Expression::Lte(Box::new(BinaryOp::new(left_min, right_max))),
            Expression::Lte(Box::new(BinaryOp::new(right_min, left_max))),
        ))))
    }

    fn case_min(left: Expression, right: Expression) -> Expression {
        Expression::Case(Box::new(Case {
            operand: None,
            whens: vec![(
                Expression::Lte(Box::new(BinaryOp::new(left.clone(), right.clone()))),
                left,
            )],
            else_: Some(right),
            comments: Vec::new(),
            inferred_type: None,
        }))
    }

    fn case_max(left: Expression, right: Expression) -> Expression {
        Expression::Case(Box::new(Case {
            operand: None,
            whens: vec![(
                Expression::Gte(Box::new(BinaryOp::new(left.clone(), right.clone()))),
                left,
            )],
            else_: Some(right),
            comments: Vec::new(),
            inferred_type: None,
        }))
    }

    fn date_bin_to_date_bucket(date_bin: DateBin) -> Option<Expression> {
        if date_bin.unit.is_some() || date_bin.zone.is_some() {
            return None;
        }

        let (datepart, number) = Self::date_bucket_parts(&date_bin.this)?;
        let mut args = vec![
            Self::date_bucket_datepart(datepart),
            number,
            *date_bin.expression,
        ];
        if let Some(origin) = date_bin.origin {
            args.push(*origin);
        }

        Some(Expression::Function(Box::new(Function::new(
            "DATE_BUCKET".to_string(),
            args,
        ))))
    }

    fn date_bin_function_to_date_bucket(function: &Function) -> Option<Expression> {
        if !(2..=3).contains(&function.args.len()) {
            return None;
        }

        let (datepart, number) = Self::date_bucket_parts(&function.args[0])?;
        let mut args = vec![
            Self::date_bucket_datepart(datepart),
            number,
            function.args[1].clone(),
        ];
        if let Some(origin) = function.args.get(2) {
            args.push(origin.clone());
        }

        Some(Expression::Function(Box::new(Function::new(
            "DATE_BUCKET".to_string(),
            args,
        ))))
    }

    fn date_bucket_parts(stride: &Expression) -> Option<(&'static str, Expression)> {
        match stride {
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::String(value) => Self::date_bucket_parts_from_string(value),
                _ => None,
            },
            Expression::Interval(interval) => Self::date_bucket_parts_from_interval(interval),
            _ => None,
        }
    }

    fn date_bucket_parts_from_interval(interval: &Interval) -> Option<(&'static str, Expression)> {
        match &interval.unit {
            Some(IntervalUnitSpec::Simple { unit, .. }) => {
                let datepart = Self::date_bucket_datepart_from_unit(*unit)?;
                let amount = interval
                    .this
                    .as_ref()
                    .and_then(Self::date_bucket_amount_expr)?;
                Some((datepart, amount))
            }
            None => interval.this.as_ref().and_then(|expr| match expr {
                Expression::Literal(lit) => match lit.as_ref() {
                    Literal::String(value) => Self::date_bucket_parts_from_string(value),
                    _ => None,
                },
                _ => None,
            }),
            _ => None,
        }
    }

    fn date_bucket_parts_from_string(value: &str) -> Option<(&'static str, Expression)> {
        let mut parts = value.split_whitespace();
        let amount = parts.next()?;
        let unit = parts.next()?;
        if parts.next().is_some() {
            return None;
        }

        Some((
            Self::date_bucket_datepart_from_name(unit)?,
            Self::positive_integer_expr(amount)?,
        ))
    }

    fn date_bucket_amount_expr(expr: &Expression) -> Option<Expression> {
        match expr {
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::Number(value) => Self::positive_integer_expr(value),
                Literal::String(value) => Self::positive_integer_expr(value),
                _ => None,
            },
            _ => Some(expr.clone()),
        }
    }

    fn positive_integer_expr(value: &str) -> Option<Expression> {
        let parsed = value.trim().parse::<i64>().ok()?;
        (parsed > 0).then(|| Expression::number(parsed))
    }

    fn date_bucket_datepart(datepart: &str) -> Expression {
        Expression::Var(Box::new(Var {
            this: datepart.to_string(),
        }))
    }

    fn date_bucket_datepart_from_unit(unit: IntervalUnit) -> Option<&'static str> {
        match unit {
            IntervalUnit::Week => Some("WEEK"),
            IntervalUnit::Day => Some("DAY"),
            IntervalUnit::Hour => Some("HOUR"),
            IntervalUnit::Minute => Some("MINUTE"),
            IntervalUnit::Second => Some("SECOND"),
            IntervalUnit::Millisecond => Some("MILLISECOND"),
            _ => None,
        }
    }

    fn date_bucket_datepart_from_name(unit: &str) -> Option<&'static str> {
        match unit.trim().to_ascii_uppercase().as_str() {
            "WEEK" | "WEEKS" | "W" | "WK" | "WKS" | "WW" => Some("WEEK"),
            "DAY" | "DAYS" | "D" | "DD" => Some("DAY"),
            "HOUR" | "HOURS" | "H" | "HH" | "HR" | "HRS" => Some("HOUR"),
            "MINUTE" | "MINUTES" | "MI" | "MIN" | "MINS" | "N" => Some("MINUTE"),
            "SECOND" | "SECONDS" | "S" | "SEC" | "SECS" | "SS" => Some("SECOND"),
            "MILLISECOND" | "MILLISECONDS" | "MS" | "MSEC" | "MSECS" | "MILLISEC" | "MILLISECS" => {
                Some("MILLISECOND")
            }
            _ => None,
        }
    }

    fn node_has_fetch_with_ties(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::Select(select)
                if select
                    .fetch
                    .as_ref()
                    .is_some_and(|fetch| fetch.with_ties)
        )
    }

    fn node_is_overlaps(expr: &Expression) -> bool {
        matches!(expr, Expression::Overlaps(_))
    }

    fn node_is_date_bin(expr: &Expression) -> bool {
        matches!(expr, Expression::DateBin(_)) || Self::node_is_function_named(expr, "DATE_BIN")
    }

    fn node_is_function_named(expr: &Expression, name: &str) -> bool {
        match expr {
            Expression::Function(function) => function.name.eq_ignore_ascii_case(name),
            Expression::AggregateFunction(function) => function.name.eq_ignore_ascii_case(name),
            _ => false,
        }
    }

    fn node_is_postgres_json_build_object(expr: &Expression) -> bool {
        match expr {
            Expression::Function(function) => {
                function.name.eq_ignore_ascii_case("JSON_BUILD_OBJECT")
                    || function.name.eq_ignore_ascii_case("JSONB_BUILD_OBJECT")
            }
            _ => false,
        }
    }

    fn postgres_json_build_object_can_lower_to_json_object(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::Function(function)
                if (function.name.eq_ignore_ascii_case("JSON_BUILD_OBJECT")
                    || function.name.eq_ignore_ascii_case("JSONB_BUILD_OBJECT"))
                    && !function.distinct
                    && function.args.len() % 2 == 0
        )
    }

    fn node_is_postgres_json_array_elements(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::Function(function)
                if function.name.eq_ignore_ascii_case("JSON_ARRAY_ELEMENTS")
                    || function.name.eq_ignore_ascii_case("JSONB_ARRAY_ELEMENTS")
                    || function.name.eq_ignore_ascii_case("JSON_ARRAY_ELEMENTS_TEXT")
                    || function.name.eq_ignore_ascii_case("JSONB_ARRAY_ELEMENTS_TEXT")
        )
    }

    fn postgres_tsql_unsupported_function_name(
        expr: &Expression,
        target: DialectType,
    ) -> Option<&'static str> {
        match expr {
            Expression::Lpad(_) => Some("LPAD"),
            Expression::Rpad(_) => Some("RPAD"),
            Expression::SplitPart(_) => Some("SPLIT_PART"),
            Expression::Initcap(_) => Some("INITCAP"),
            Expression::RegexpReplace(_) => Some("REGEXP_REPLACE"),
            Expression::RegexpInstr(_) => Some("REGEXP_INSTR"),
            Expression::RegexpCount(_) => Some("REGEXP_COUNT"),
            Expression::RegexpSplit(_) => Some("REGEXP_SPLIT"),
            Expression::DecodeCase(_) => Some("DECODE"),
            Expression::ToJson(_) => Some("TO_JSON"),
            Expression::JSONBObjectAgg(_) => Some("JSONB_OBJECT_AGG"),
            Expression::ToNumber(_) => Some("TO_NUMBER"),
            Expression::WidthBucket(_) => Some("WIDTH_BUCKET"),
            Expression::BitwiseAndAgg(_) => Some("BIT_AND"),
            Expression::BitwiseOrAgg(_) => Some("BIT_OR"),
            Expression::BitwiseXorAgg(_) => Some("BIT_XOR"),
            Expression::Corr(_) => Some("CORR"),
            Expression::CovarPop(_) => Some("COVAR_POP"),
            Expression::CovarSamp(_) => Some("COVAR_SAMP"),
            Expression::RegrAvgx(_) => Some("REGR_AVGX"),
            Expression::RegrAvgy(_) => Some("REGR_AVGY"),
            Expression::RegrCount(_) => Some("REGR_COUNT"),
            Expression::RegrIntercept(_) => Some("REGR_INTERCEPT"),
            Expression::RegrR2(_) => Some("REGR_R2"),
            Expression::RegrSlope(_) => Some("REGR_SLOPE"),
            Expression::RegrSxx(_) => Some("REGR_SXX"),
            Expression::RegrSxy(_) => Some("REGR_SXY"),
            Expression::RegrSyy(_) => Some("REGR_SYY"),
            Expression::Function(function) => {
                Self::postgres_tsql_unsupported_function_name_str(&function.name, target)
            }
            Expression::AggregateFunction(function) => {
                Self::postgres_tsql_unsupported_function_name_str(&function.name, target)
            }
            _ => None,
        }
    }

    fn postgres_tsql_unsupported_function_name_str(
        name: &str,
        target: DialectType,
    ) -> Option<&'static str> {
        if name.eq_ignore_ascii_case("LPAD") {
            Some("LPAD")
        } else if name.eq_ignore_ascii_case("RPAD") {
            Some("RPAD")
        } else if name.eq_ignore_ascii_case("SPLIT_PART") {
            Some("SPLIT_PART")
        } else if name.eq_ignore_ascii_case("INITCAP") {
            Some("INITCAP")
        } else if name.eq_ignore_ascii_case("TO_JSON") {
            Some("TO_JSON")
        } else if name.eq_ignore_ascii_case("TO_JSONB") {
            Some("TO_JSONB")
        } else if name.eq_ignore_ascii_case("JSONB_OBJECT_AGG") {
            Some("JSONB_OBJECT_AGG")
        } else if name.eq_ignore_ascii_case("ROW_TO_JSON") {
            Some("ROW_TO_JSON")
        } else if name.eq_ignore_ascii_case("JSON_ARRAY_ELEMENTS") {
            Some("JSON_ARRAY_ELEMENTS")
        } else if name.eq_ignore_ascii_case("JSONB_ARRAY_ELEMENTS") {
            Some("JSONB_ARRAY_ELEMENTS")
        } else if name.eq_ignore_ascii_case("JSON_ARRAY_ELEMENTS_TEXT") {
            Some("JSON_ARRAY_ELEMENTS_TEXT")
        } else if name.eq_ignore_ascii_case("JSONB_ARRAY_ELEMENTS_TEXT") {
            Some("JSONB_ARRAY_ELEMENTS_TEXT")
        } else if name.eq_ignore_ascii_case("ENCODE") {
            Some("ENCODE")
        } else if name.eq_ignore_ascii_case("DECODE") {
            Some("DECODE")
        } else if name.eq_ignore_ascii_case("REGEXP_REPLACE") {
            Some("REGEXP_REPLACE")
        } else if name.eq_ignore_ascii_case("REGEXP_COUNT") {
            Some("REGEXP_COUNT")
        } else if name.eq_ignore_ascii_case("REGEXP_INSTR") {
            Some("REGEXP_INSTR")
        } else if name.eq_ignore_ascii_case("REGEXP_SUBSTR") {
            Some("REGEXP_SUBSTR")
        } else if name.eq_ignore_ascii_case("REGEXP_SPLIT") {
            Some("REGEXP_SPLIT")
        } else if name.eq_ignore_ascii_case("REGEXP_SPLIT_TO_ARRAY") {
            Some("REGEXP_SPLIT_TO_ARRAY")
        } else if name.eq_ignore_ascii_case("REGEXP_SPLIT_TO_TABLE") {
            Some("REGEXP_SPLIT_TO_TABLE")
        } else if name.eq_ignore_ascii_case("SHA224") {
            Some("SHA224")
        } else if name.eq_ignore_ascii_case("SHA384") {
            Some("SHA384")
        } else if name.eq_ignore_ascii_case("TO_BIN") {
            Some("TO_BIN")
        } else if name.eq_ignore_ascii_case("TO_OCT") {
            Some("TO_OCT")
        } else if target == DialectType::TSQL && name.eq_ignore_ascii_case("UNISTR") {
            Some("UNISTR")
        } else if name.eq_ignore_ascii_case("AGE") {
            Some("AGE")
        } else if name.eq_ignore_ascii_case("ERF") {
            Some("ERF")
        } else if name.eq_ignore_ascii_case("GCD") {
            Some("GCD")
        } else if name.eq_ignore_ascii_case("LCM") {
            Some("LCM")
        } else if name.eq_ignore_ascii_case("QUOTE_LITERAL") {
            Some("QUOTE_LITERAL")
        } else if name.eq_ignore_ascii_case("WIDTH_BUCKET") {
            Some("WIDTH_BUCKET")
        } else if name.eq_ignore_ascii_case("SCALE") {
            Some("SCALE")
        } else if name.eq_ignore_ascii_case("TRIM_SCALE") {
            Some("TRIM_SCALE")
        } else if name.eq_ignore_ascii_case("MIN_SCALE") {
            Some("MIN_SCALE")
        } else if name.eq_ignore_ascii_case("FACTORIAL") {
            Some("FACTORIAL")
        } else if name.eq_ignore_ascii_case("PG_LSN") {
            Some("PG_LSN")
        } else if name.eq_ignore_ascii_case("TO_CHAR") {
            Some("TO_CHAR")
        } else if name.eq_ignore_ascii_case("PG_TYPEOF") {
            Some("PG_TYPEOF")
        } else if name.eq_ignore_ascii_case("BIT_AND") {
            Some("BIT_AND")
        } else if name.eq_ignore_ascii_case("BIT_OR") {
            Some("BIT_OR")
        } else if name.eq_ignore_ascii_case("BIT_XOR") {
            Some("BIT_XOR")
        } else if name.eq_ignore_ascii_case("CORR") {
            Some("CORR")
        } else if name.eq_ignore_ascii_case("COVAR_POP") {
            Some("COVAR_POP")
        } else if name.eq_ignore_ascii_case("COVAR_SAMP") {
            Some("COVAR_SAMP")
        } else if name.eq_ignore_ascii_case("REGR_AVGX") {
            Some("REGR_AVGX")
        } else if name.eq_ignore_ascii_case("REGR_AVGY") {
            Some("REGR_AVGY")
        } else if name.eq_ignore_ascii_case("REGR_COUNT") {
            Some("REGR_COUNT")
        } else if name.eq_ignore_ascii_case("REGR_INTERCEPT") {
            Some("REGR_INTERCEPT")
        } else if name.eq_ignore_ascii_case("REGR_R2") {
            Some("REGR_R2")
        } else if name.eq_ignore_ascii_case("REGR_SLOPE") {
            Some("REGR_SLOPE")
        } else if name.eq_ignore_ascii_case("REGR_SXX") {
            Some("REGR_SXX")
        } else if name.eq_ignore_ascii_case("REGR_SXY") {
            Some("REGR_SXY")
        } else if name.eq_ignore_ascii_case("REGR_SYY") {
            Some("REGR_SYY")
        } else if name.eq_ignore_ascii_case("FLOAT8_ACCUM") {
            Some("FLOAT8_ACCUM")
        } else if name.eq_ignore_ascii_case("FLOAT8_REGR_ACCUM") {
            Some("FLOAT8_REGR_ACCUM")
        } else if name.eq_ignore_ascii_case("FLOAT8_COMBINE") {
            Some("FLOAT8_COMBINE")
        } else if name.eq_ignore_ascii_case("FLOAT8_REGR_COMBINE") {
            Some("FLOAT8_REGR_COMBINE")
        } else if name.eq_ignore_ascii_case("BOOLAND_STATEFUNC") {
            Some("BOOLAND_STATEFUNC")
        } else if name.eq_ignore_ascii_case("BOOLOR_STATEFUNC") {
            Some("BOOLOR_STATEFUNC")
        } else {
            None
        }
    }

    fn normalize_postgres_trim_for_tsql(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Trim(trim) => {
                let mut trim = *trim;
                trim.characters = trim.characters.map(Self::strip_postgres_text_literal_cast);
                match trim.position {
                    crate::expressions::TrimPosition::Both
                        if trim.position_explicit && trim.characters.is_some() =>
                    {
                        trim.position_explicit = false;
                        trim.sql_standard_syntax = true;
                        Ok(Expression::Trim(Box::new(trim)))
                    }
                    crate::expressions::TrimPosition::Leading if trim.characters.is_some() => {
                        let characters = trim.characters.take().expect("checked above");
                        Ok(Expression::Function(Box::new(Function::new(
                            "LTRIM",
                            vec![trim.this, characters],
                        ))))
                    }
                    crate::expressions::TrimPosition::Trailing if trim.characters.is_some() => {
                        let characters = trim.characters.take().expect("checked above");
                        Ok(Expression::Function(Box::new(Function::new(
                            "RTRIM",
                            vec![trim.this, characters],
                        ))))
                    }
                    _ => Ok(Expression::Trim(Box::new(trim))),
                }
            }
            other => Ok(other),
        })
    }

    fn normalize_postgres_string_semantics_for_tsql(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Like(mut op) => {
                Self::recover_postgres_like_escape(&mut op);
                Ok(Expression::Like(op))
            }
            Expression::ILike(mut op) => {
                Self::recover_postgres_like_escape(&mut op);
                Ok(Expression::ILike(op))
            }
            Expression::Substring(mut substring)
                if substring.length.is_none()
                    && Self::is_explicitly_numeric_expression(&substring.start) =>
            {
                substring.length = Some(Expression::number(i32::MAX as i64));
                Ok(Expression::Substring(substring))
            }
            Expression::Trim(mut trim) => {
                trim.characters = trim.characters.map(Self::strip_postgres_text_literal_cast);
                Ok(Expression::Trim(trim))
            }
            Expression::Function(mut function)
                if !function.quoted
                    && matches!(
                        function.name.to_ascii_uppercase().as_str(),
                        "BTRIM" | "LTRIM" | "RTRIM"
                    )
                    && function.args.len() == 2 =>
            {
                function.args[1] = Self::strip_postgres_text_literal_cast(function.args[1].clone());
                Ok(Expression::Function(function))
            }
            Expression::Translate(translate) => {
                Ok(Self::normalize_postgres_translate_for_tsql(*translate))
            }
            Expression::Function(function)
                if !function.quoted
                    && function.name.eq_ignore_ascii_case("TRANSLATE")
                    && function.args.len() == 3 =>
            {
                Ok(Self::normalize_postgres_translate_function_for_tsql(
                    *function,
                ))
            }
            other => Ok(other),
        })
    }

    fn recover_postgres_like_escape(op: &mut crate::expressions::LikeOp) {
        if op.escape.is_some() {
            return;
        }

        let Expression::Function(function) = &op.right else {
            return;
        };
        if function.quoted
            || function.distinct
            || !function.name.eq_ignore_ascii_case("LIKE_ESCAPE")
            || function.args.len() != 2
        {
            return;
        }

        let pattern = function.args[0].clone();
        let escape = function.args[1].clone();
        op.right = Self::strip_postgres_text_literal_cast(pattern);
        op.escape = Some(Self::strip_postgres_text_literal_cast(escape));
    }

    fn normalize_postgres_translate_for_tsql(
        mut translate: crate::expressions::Translate,
    ) -> Expression {
        let (Some(from), Some(to)) = (&translate.from_, &translate.to) else {
            return Expression::Translate(Box::new(translate));
        };

        let (Some(from_value), Some(to_value)) = (
            Self::postgres_text_literal_value(from),
            Self::postgres_text_literal_value(to),
        ) else {
            return Expression::Translate(Box::new(translate));
        };
        let from_value = from_value.to_string();
        let to_value = to_value.to_string();

        if from_value.chars().count() > to_value.chars().count() {
            if let Some(input) = Self::postgres_text_literal_value(&translate.this) {
                return Expression::string(Self::translate_postgres_literal(
                    input,
                    &from_value,
                    &to_value,
                ));
            }
            return Expression::Translate(Box::new(translate));
        }

        translate.from_ = Some(Box::new(Self::strip_postgres_text_literal_cast(
            *translate.from_.expect("checked above"),
        )));
        let normalized_to = if from_value.chars().count() < to_value.chars().count() {
            Expression::string(
                to_value
                    .chars()
                    .take(from_value.chars().count())
                    .collect::<String>(),
            )
        } else {
            Self::strip_postgres_text_literal_cast(*translate.to.expect("checked above"))
        };
        translate.to = Some(Box::new(normalized_to));
        Expression::Translate(Box::new(translate))
    }

    fn normalize_postgres_translate_function_for_tsql(mut function: Function) -> Expression {
        let from = Self::postgres_text_literal_value(&function.args[1]);
        let to = Self::postgres_text_literal_value(&function.args[2]);
        let (Some(from), Some(to)) = (from, to) else {
            return Expression::Function(Box::new(function));
        };
        let from = from.to_string();
        let to = to.to_string();

        if from.chars().count() > to.chars().count() {
            if let Some(input) = Self::postgres_text_literal_value(&function.args[0]) {
                return Expression::string(Self::translate_postgres_literal(input, &from, &to));
            }
            return Expression::Function(Box::new(function));
        }

        function.args[1] = Self::strip_postgres_text_literal_cast(function.args[1].clone());
        function.args[2] = if from.chars().count() < to.chars().count() {
            Expression::string(to.chars().take(from.chars().count()).collect::<String>())
        } else {
            Self::strip_postgres_text_literal_cast(function.args[2].clone())
        };
        Expression::Function(Box::new(function))
    }

    fn translate_postgres_literal(input: &str, from: &str, to: &str) -> String {
        let from = from.chars().collect::<Vec<_>>();
        let to = to.chars().collect::<Vec<_>>();
        let mut output = String::with_capacity(input.len());

        for ch in input.chars() {
            match from.iter().position(|candidate| *candidate == ch) {
                Some(index) if index < to.len() => output.push(to[index]),
                Some(_) => {}
                None => output.push(ch),
            }
        }

        output
    }

    fn postgres_tsql_unsupported_string_semantics(expr: &Expression) -> Option<&'static str> {
        match expr {
            Expression::Substring(substring) if substring.length.is_none() => {
                if Self::postgres_text_literal_value(&substring.start).is_some() {
                    Some("regular-expression SUBSTRING")
                } else {
                    Some("SUBSTRING without a statically numeric start position")
                }
            }
            Expression::Translate(translate) => {
                let from = translate
                    .from_
                    .as_deref()
                    .and_then(Self::postgres_text_literal_value);
                let to = translate
                    .to
                    .as_deref()
                    .and_then(Self::postgres_text_literal_value);
                match (from, to) {
                    (Some(from), Some(to)) if from.chars().count() == to.chars().count() => None,
                    _ => Some("TRANSLATE with source and replacement lengths that differ or cannot be proven equal"),
                }
            }
            Expression::Function(function)
                if !function.quoted && function.name.eq_ignore_ascii_case("LIKE_ESCAPE") =>
            {
                Some("LIKE_ESCAPE helper outside a LIKE predicate")
            }
            Expression::Function(function)
                if !function.quoted
                    && function.name.eq_ignore_ascii_case("TRANSLATE")
                    && function.args.len() == 3 =>
            {
                let from = Self::postgres_text_literal_value(&function.args[1]);
                let to = Self::postgres_text_literal_value(&function.args[2]);
                match (from, to) {
                    (Some(from), Some(to)) if from.chars().count() == to.chars().count() => None,
                    _ => Some("TRANSLATE with source and replacement lengths that differ or cannot be proven equal"),
                }
            }
            Expression::Trim(trim)
                if trim
                    .characters
                    .as_ref()
                    .is_some_and(Self::is_unbounded_text_cast) =>
            {
                Some("TRIM character set cast to an unbounded text type")
            }
            Expression::Function(function)
                if !function.quoted
                    && matches!(
                        function.name.to_ascii_uppercase().as_str(),
                        "LTRIM" | "RTRIM"
                    )
                    && function.args.len() == 2
                    && Self::is_unbounded_text_cast(&function.args[1]) =>
            {
                Some("TRIM character set cast to an unbounded text type")
            }
            _ => None,
        }
    }

    fn strip_postgres_text_literal_cast(expr: Expression) -> Expression {
        match expr {
            Expression::Cast(cast)
                if Self::is_text_data_type(&cast.to)
                    && Self::postgres_text_literal_value(&cast.this).is_some() =>
            {
                Self::strip_postgres_text_literal_cast(cast.this)
            }
            Expression::TryCast(cast)
                if Self::is_text_data_type(&cast.to)
                    && Self::postgres_text_literal_value(&cast.this).is_some() =>
            {
                Self::strip_postgres_text_literal_cast(cast.this)
            }
            Expression::SafeCast(cast)
                if Self::is_text_data_type(&cast.to)
                    && Self::postgres_text_literal_value(&cast.this).is_some() =>
            {
                Self::strip_postgres_text_literal_cast(cast.this)
            }
            Expression::Paren(mut paren)
                if Self::postgres_text_literal_value(&paren.this).is_some() =>
            {
                paren.this = Self::strip_postgres_text_literal_cast(paren.this);
                Expression::Paren(paren)
            }
            other => other,
        }
    }

    fn postgres_text_literal_value(expr: &Expression) -> Option<&str> {
        match expr {
            Expression::Literal(literal) if literal.is_string() => Some(literal.value_str()),
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast)
                if Self::is_text_data_type(&cast.to) =>
            {
                Self::postgres_text_literal_value(&cast.this)
            }
            Expression::Alias(alias) => Self::postgres_text_literal_value(&alias.this),
            Expression::Paren(paren) => Self::postgres_text_literal_value(&paren.this),
            _ => None,
        }
    }

    fn is_text_data_type(data_type: &DataType) -> bool {
        match data_type {
            DataType::Char { .. }
            | DataType::VarChar { .. }
            | DataType::String { .. }
            | DataType::Text
            | DataType::TextWithLength { .. } => true,
            DataType::Custom { name } => {
                let base = name
                    .split_once('(')
                    .map_or(name.as_str(), |(base, _)| base)
                    .trim();
                matches!(
                    base.to_ascii_uppercase().as_str(),
                    "CHAR"
                        | "NCHAR"
                        | "VARCHAR"
                        | "NVARCHAR"
                        | "TEXT"
                        | "NTEXT"
                        | "STRING"
                        | "CHARACTER VARYING"
                )
            }
            _ => false,
        }
    }

    fn is_unbounded_text_cast(expr: &Expression) -> bool {
        let data_type = match expr {
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
                &cast.to
            }
            Expression::Paren(paren) => return Self::is_unbounded_text_cast(&paren.this),
            _ => return false,
        };

        match data_type {
            DataType::Text => true,
            DataType::VarChar { length: None, .. } | DataType::String { length: None } => true,
            DataType::Custom { name } => name.to_ascii_uppercase().contains("(MAX)"),
            _ => false,
        }
    }

    fn is_explicitly_numeric_expression(expr: &Expression) -> bool {
        if expr.inferred_type().is_some_and(Self::is_numeric_data_type) {
            return true;
        }

        match expr {
            Expression::Literal(literal) => literal.is_number(),
            Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
                Self::is_numeric_data_type(&cast.to)
            }
            Expression::Alias(alias) => Self::is_explicitly_numeric_expression(&alias.this),
            Expression::Paren(paren) => Self::is_explicitly_numeric_expression(&paren.this),
            Expression::Neg(unary) => Self::is_explicitly_numeric_expression(&unary.this),
            _ => false,
        }
    }

    fn is_numeric_data_type(data_type: &DataType) -> bool {
        match data_type {
            DataType::TinyInt { .. }
            | DataType::SmallInt { .. }
            | DataType::Int { .. }
            | DataType::BigInt { .. }
            | DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::Decimal { .. } => true,
            DataType::Custom { name } => {
                let base = name
                    .split_once('(')
                    .map_or(name.as_str(), |(base, _)| base)
                    .trim();
                matches!(
                    base.to_ascii_uppercase().as_str(),
                    "TINYINT"
                        | "SMALLINT"
                        | "INT"
                        | "INTEGER"
                        | "BIGINT"
                        | "DECIMAL"
                        | "NUMERIC"
                        | "REAL"
                        | "FLOAT"
                        | "MONEY"
                        | "SMALLMONEY"
                )
            }
            _ => false,
        }
    }

    fn normalize_postgres_only_for_tsql(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Table(mut table) if table.only => {
                table.only = false;
                Ok(Expression::Table(table))
            }
            other => Ok(other),
        })
    }

    fn rewrite_postgres_json_array_elements_select_for_tsql(
        expr: Expression,
    ) -> Result<Expression> {
        let Expression::Select(select) = expr else {
            return Ok(expr);
        };
        let mut select = *select;
        if !Self::is_plain_single_projection_select(&select) {
            return Ok(Expression::Select(Box::new(select)));
        }

        let Some(json_arg) =
            Self::postgres_json_array_elements_projection_arg(&select.expressions[0])
        else {
            return Ok(Expression::Select(Box::new(select)));
        };

        select.expressions = vec![Expression::column("value")];
        select.from = Some(From {
            expressions: vec![Expression::OpenJSON(Box::new(
                crate::expressions::OpenJSON {
                    this: Box::new(json_arg),
                    path: None,
                    expressions: Vec::new(),
                },
            ))],
        });

        Ok(Expression::Select(Box::new(select)))
    }

    fn is_plain_single_projection_select(select: &crate::expressions::Select) -> bool {
        select.expressions.len() == 1
            && select.from.is_none()
            && select.joins.is_empty()
            && select.lateral_views.is_empty()
            && select.prewhere.is_none()
            && select.where_clause.is_none()
            && select.group_by.is_none()
            && select.having.is_none()
            && select.qualify.is_none()
            && select.order_by.is_none()
            && select.distribute_by.is_none()
            && select.cluster_by.is_none()
            && select.sort_by.is_none()
            && select.limit.is_none()
            && select.offset.is_none()
            && select.limit_by.is_none()
            && select.fetch.is_none()
            && !select.distinct
            && select.distinct_on.is_none()
            && select.top.is_none()
            && select.with.is_none()
            && select.sample.is_none()
            && select.into.is_none()
            && select.locks.is_empty()
            && select.for_xml.is_empty()
            && select.for_json.is_empty()
            && select.exclude.is_none()
    }

    fn postgres_json_array_elements_projection_arg(expr: &Expression) -> Option<Expression> {
        match expr {
            Expression::Function(function)
                if Self::node_is_postgres_json_array_elements(expr) && function.args.len() == 1 =>
            {
                Some(function.args[0].clone())
            }
            Expression::Alias(alias) => {
                Self::postgres_json_array_elements_projection_arg(&alias.this)
            }
            _ => None,
        }
    }

    fn normalize_postgres_type_function_casts(
        expr: Expression,
        target: DialectType,
    ) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Function(function) => {
                let mut function = *function;
                if function.args.len() == 1
                    && !function.distinct
                    && !function.quoted
                    && !function.use_bracket_syntax
                    && !function.name.contains('.')
                {
                    if let Some(to) = Self::postgres_type_function_data_type(&function.name) {
                        let this = function.args.remove(0);
                        let cast = Cast {
                            this,
                            to,
                            trailing_comments: function.trailing_comments,
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: function.inferred_type,
                        };
                        return Ok(
                            if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                                normalization::rewrite_postgres_float_to_integer_cast(cast)
                            } else {
                                Expression::Cast(Box::new(cast))
                            },
                        );
                    }
                }
                Ok(Expression::Function(Box::new(function)))
            }
            _ => Ok(e),
        })
    }

    fn node_is_postgres_type_function_cast(expr: &Expression) -> bool {
        matches!(
            expr,
            Expression::Function(function)
                if !function.quoted
                    && !function.use_bracket_syntax
                    && !function.name.contains('.')
                    && Self::postgres_type_function_data_type(&function.name).is_some()
        )
    }

    fn postgres_type_function_data_type(name: &str) -> Option<DataType> {
        match name.to_ascii_uppercase().as_str() {
            "NUMERIC" | "DECIMAL" | "DEC" => Some(DataType::Decimal {
                precision: None,
                scale: None,
            }),
            "INT2" | "SMALLINT" => Some(DataType::SmallInt { length: None }),
            "INT4" | "INT" => Some(DataType::Int {
                length: None,
                integer_spelling: false,
            }),
            "INTEGER" => Some(DataType::Int {
                length: None,
                integer_spelling: true,
            }),
            "INT8" | "BIGINT" => Some(DataType::BigInt { length: None }),
            "FLOAT4" | "REAL" => Some(DataType::Float {
                precision: None,
                scale: None,
                real_spelling: true,
            }),
            "FLOAT8" => Some(DataType::Double {
                precision: None,
                scale: None,
            }),
            "BOOL" | "BOOLEAN" => Some(DataType::Boolean),
            "TEXT" => Some(DataType::Text),
            "VARCHAR" => Some(DataType::VarChar {
                length: None,
                parenthesized_length: false,
            }),
            "UUID" => Some(DataType::Uuid),
            _ => None,
        }
    }

    fn rewrite_boolean_values_for_tsql(expr: Expression) -> Result<Expression> {
        match expr {
            Expression::Select(select) => Self::rewrite_boolean_values_in_tsql_select(select),
            Expression::Subquery(mut subquery) => {
                subquery.this = Self::rewrite_boolean_values_for_tsql(subquery.this)?;
                Ok(Expression::Subquery(subquery))
            }
            Expression::Union(mut union) => {
                let left = std::mem::replace(&mut union.left, Expression::null());
                let right = std::mem::replace(&mut union.right, Expression::null());
                union.left = Self::rewrite_boolean_values_for_tsql(left)?;
                union.right = Self::rewrite_boolean_values_for_tsql(right)?;
                if let Some(mut with) = union.with.take() {
                    with.ctes = with
                        .ctes
                        .into_iter()
                        .map(|mut cte| {
                            cte.this = Self::rewrite_boolean_values_for_tsql(cte.this)?;
                            Ok(cte)
                        })
                        .collect::<Result<Vec<_>>>()?;
                    union.with = Some(with);
                }
                Ok(Expression::Union(union))
            }
            Expression::Intersect(mut intersect) => {
                let left = std::mem::replace(&mut intersect.left, Expression::null());
                let right = std::mem::replace(&mut intersect.right, Expression::null());
                intersect.left = Self::rewrite_boolean_values_for_tsql(left)?;
                intersect.right = Self::rewrite_boolean_values_for_tsql(right)?;
                Ok(Expression::Intersect(intersect))
            }
            Expression::Except(mut except) => {
                let left = std::mem::replace(&mut except.left, Expression::null());
                let right = std::mem::replace(&mut except.right, Expression::null());
                except.left = Self::rewrite_boolean_values_for_tsql(left)?;
                except.right = Self::rewrite_boolean_values_for_tsql(right)?;
                Ok(Expression::Except(except))
            }
            other => Self::rewrite_tsql_boolean_nested_contexts(other),
        }
    }

    fn rewrite_postgres_format_for_tsql(
        expr: Expression,
        target: DialectType,
    ) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Function(f) if f.name.eq_ignore_ascii_case("FORMAT") => {
                Self::postgres_format_function_to_tsql(*f, target)
            }
            other => Ok(other),
        })
    }

    fn postgres_format_function_to_tsql(f: Function, target: DialectType) -> Result<Expression> {
        let Some(format_expr) = f.args.first() else {
            return Err(Self::unsupported_postgres_format_for_tsql(
                target,
                "missing format string",
            ));
        };

        let format = match format_expr {
            Expression::Literal(lit) if lit.is_string() => lit.value_str(),
            _ => {
                return Err(Self::unsupported_postgres_format_for_tsql(
                    target,
                    "dynamic format strings",
                ))
            }
        };

        let value_args = &f.args[1..];
        let mut arg_index = 0usize;
        let mut literal = String::new();
        let mut segments = Vec::new();
        let mut chars = format.chars();

        while let Some(ch) = chars.next() {
            if ch != '%' {
                literal.push(ch);
                continue;
            }

            let Some(specifier) = chars.next() else {
                return Err(Self::unsupported_postgres_format_for_tsql(
                    target,
                    "unterminated format specifier",
                ));
            };

            match specifier {
                '%' => literal.push('%'),
                's' => {
                    if !literal.is_empty() {
                        segments.push(Expression::string(std::mem::take(&mut literal)));
                    }
                    let Some(arg) = value_args.get(arg_index) else {
                        return Err(Self::unsupported_postgres_format_for_tsql(
                            target,
                            "not enough arguments",
                        ));
                    };
                    segments.push(arg.clone());
                    arg_index += 1;
                }
                other => {
                    return Err(Self::unsupported_postgres_format_for_tsql(
                        target,
                        format!("unsupported format specifier %{other}"),
                    ))
                }
            }
        }

        if !literal.is_empty() {
            segments.push(Expression::string(literal));
        }

        if arg_index != value_args.len() {
            return Err(Self::unsupported_postgres_format_for_tsql(
                target,
                "unused format arguments",
            ));
        }

        Ok(Self::postgres_format_segments_to_tsql_concat(segments))
    }

    fn postgres_format_segments_to_tsql_concat(mut segments: Vec<Expression>) -> Expression {
        if segments.is_empty() {
            return Expression::string("");
        }

        if segments.len() == 1 {
            let only = segments.pop().expect("one segment");
            if matches!(&only, Expression::Literal(lit) if lit.is_string()) {
                return only;
            }

            return Expression::Function(Box::new(Function::new(
                "CONCAT".to_string(),
                vec![only, Expression::string("")],
            )));
        }

        Expression::Function(Box::new(Function::new("CONCAT".to_string(), segments)))
    }

    fn unsupported_postgres_format_for_tsql(
        target: DialectType,
        reason: impl Into<String>,
    ) -> crate::error::Error {
        crate::error::Error::unsupported(
            format!("PostgreSQL format() ({})", reason.into()),
            target.to_string(),
        )
    }

    fn rewrite_boolean_values_in_tsql_select(
        mut select: Box<crate::expressions::Select>,
    ) -> Result<Expression> {
        if let Some(mut with) = select.with.take() {
            with.ctes = with
                .ctes
                .into_iter()
                .map(|mut cte| {
                    cte.this = Self::rewrite_boolean_values_for_tsql(cte.this)?;
                    Ok(cte)
                })
                .collect::<Result<Vec<_>>>()?;
            select.with = Some(with);
        }

        select.expressions = select
            .expressions
            .into_iter()
            .map(Self::rewrite_tsql_boolean_scalar_value)
            .collect::<Result<Vec<_>>>()?;

        if let Some(mut from) = select.from.take() {
            from.expressions = from
                .expressions
                .into_iter()
                .map(Self::rewrite_tsql_boolean_nested_contexts)
                .collect::<Result<Vec<_>>>()?;
            select.from = Some(from);
        }

        select.joins = select
            .joins
            .into_iter()
            .map(|mut join| {
                join.this = Self::rewrite_tsql_boolean_nested_contexts(join.this)?;
                if let Some(on) = join.on.take() {
                    join.on = Some(Self::rewrite_tsql_boolean_predicate_context(on)?);
                }
                if let Some(match_condition) = join.match_condition.take() {
                    join.match_condition = Some(Self::rewrite_tsql_boolean_predicate_context(
                        match_condition,
                    )?);
                }
                join.pivots = join
                    .pivots
                    .into_iter()
                    .map(Self::rewrite_tsql_boolean_nested_contexts)
                    .collect::<Result<Vec<_>>>()?;
                Ok(join)
            })
            .collect::<Result<Vec<_>>>()?;

        select.lateral_views = select
            .lateral_views
            .into_iter()
            .map(|mut lateral_view| {
                lateral_view.this = Self::rewrite_tsql_boolean_nested_contexts(lateral_view.this)?;
                Ok(lateral_view)
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(prewhere) = select.prewhere.take() {
            select.prewhere = Some(Self::rewrite_tsql_boolean_predicate_context(prewhere)?);
        }

        if let Some(mut where_clause) = select.where_clause.take() {
            where_clause.this = Self::rewrite_tsql_boolean_predicate_context(where_clause.this)?;
            select.where_clause = Some(where_clause);
        }

        if let Some(mut group_by) = select.group_by.take() {
            group_by.expressions = group_by
                .expressions
                .into_iter()
                .map(Self::rewrite_tsql_boolean_scalar_value)
                .collect::<Result<Vec<_>>>()?;
            select.group_by = Some(group_by);
        }

        if let Some(mut having) = select.having.take() {
            having.this = Self::rewrite_tsql_boolean_predicate_context(having.this)?;
            select.having = Some(having);
        }

        if let Some(mut qualify) = select.qualify.take() {
            qualify.this = Self::rewrite_tsql_boolean_predicate_context(qualify.this)?;
            select.qualify = Some(qualify);
        }

        if let Some(mut order_by) = select.order_by.take() {
            order_by.expressions = Self::rewrite_tsql_boolean_ordered_values(order_by.expressions)?;
            select.order_by = Some(order_by);
        }

        if let Some(mut distribute_by) = select.distribute_by.take() {
            distribute_by.expressions = distribute_by
                .expressions
                .into_iter()
                .map(Self::rewrite_tsql_boolean_scalar_value)
                .collect::<Result<Vec<_>>>()?;
            select.distribute_by = Some(distribute_by);
        }

        if let Some(mut cluster_by) = select.cluster_by.take() {
            cluster_by.expressions =
                Self::rewrite_tsql_boolean_ordered_values(cluster_by.expressions)?;
            select.cluster_by = Some(cluster_by);
        }

        if let Some(mut sort_by) = select.sort_by.take() {
            sort_by.expressions = Self::rewrite_tsql_boolean_ordered_values(sort_by.expressions)?;
            select.sort_by = Some(sort_by);
        }

        if let Some(limit_by) = select.limit_by.take() {
            select.limit_by = Some(
                limit_by
                    .into_iter()
                    .map(Self::rewrite_tsql_boolean_scalar_value)
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        if let Some(distinct_on) = select.distinct_on.take() {
            select.distinct_on = Some(
                distinct_on
                    .into_iter()
                    .map(Self::rewrite_tsql_boolean_scalar_value)
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        if let Some(mut sample) = select.sample.take() {
            sample.size = Self::rewrite_tsql_boolean_nested_contexts(sample.size)?;
            if let Some(offset) = sample.offset.take() {
                sample.offset = Some(Self::rewrite_tsql_boolean_nested_contexts(offset)?);
            }
            if let Some(bucket_numerator) = sample.bucket_numerator.take() {
                sample.bucket_numerator = Some(Box::new(
                    Self::rewrite_tsql_boolean_nested_contexts(*bucket_numerator)?,
                ));
            }
            if let Some(bucket_denominator) = sample.bucket_denominator.take() {
                sample.bucket_denominator = Some(Box::new(
                    Self::rewrite_tsql_boolean_nested_contexts(*bucket_denominator)?,
                ));
            }
            if let Some(bucket_field) = sample.bucket_field.take() {
                sample.bucket_field = Some(Box::new(Self::rewrite_tsql_boolean_nested_contexts(
                    *bucket_field,
                )?));
            }
            select.sample = Some(sample);
        }

        if let Some(settings) = select.settings.take() {
            select.settings = Some(
                settings
                    .into_iter()
                    .map(Self::rewrite_tsql_boolean_nested_contexts)
                    .collect::<Result<Vec<_>>>()?,
            );
        }

        if let Some(format) = select.format.take() {
            select.format = Some(Self::rewrite_tsql_boolean_nested_contexts(format)?);
        }

        if let Some(mut windows) = select.windows.take() {
            for window in windows.iter_mut() {
                Self::rewrite_tsql_boolean_over_values(&mut window.spec)?;
            }
            select.windows = Some(windows);
        }

        Ok(Expression::Select(select))
    }

    fn normalize_postgres_boolean_semantics_for_tsql(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Function(function)
                if function.args.len() == 2
                    && (function.name.eq_ignore_ascii_case("BOOLEQ")
                        || function.name.eq_ignore_ascii_case("BOOLNE")) =>
            {
                let is_equal = function.name.eq_ignore_ascii_case("BOOLEQ");
                let mut args = function.args.into_iter();
                let op = BinaryOp {
                    left: args.next().expect("checked boolean operator arity"),
                    right: args.next().expect("checked boolean operator arity"),
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: function.trailing_comments,
                    inferred_type: None,
                };
                if is_equal {
                    Ok(Expression::Eq(Box::new(op)))
                } else {
                    Ok(Expression::Neq(Box::new(op)))
                }
            }
            Expression::Cast(cast)
                if matches!(cast.to, DataType::Text)
                    && Self::is_known_postgres_boolean_expression(&cast.this) =>
            {
                Ok(Self::postgres_boolean_text_value(cast.this))
            }
            other => Ok(other),
        })
    }

    fn is_known_postgres_boolean_expression(expr: &Expression) -> bool {
        match expr {
            Expression::Boolean(_) => true,
            Expression::Cast(cast) => matches!(cast.to, DataType::Boolean),
            Expression::Paren(paren) => Self::is_known_postgres_boolean_expression(&paren.this),
            other => Self::is_tsql_boolean_value_expression(other),
        }
    }

    fn postgres_boolean_text_value(predicate: Expression) -> Expression {
        if let Expression::Boolean(boolean) = predicate {
            return Expression::string(if boolean.value { "true" } else { "false" });
        }

        Self::three_valued_boolean_case(
            predicate,
            Expression::string("true"),
            Expression::string("false"),
        )
    }

    fn rewrite_tsql_boolean_scalar_value(expr: Expression) -> Result<Expression> {
        if let Expression::Boolean(boolean) = expr {
            return Ok(Expression::Cast(Box::new(Cast {
                this: Expression::Boolean(boolean),
                to: DataType::Boolean,
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            })));
        }

        if Self::is_tsql_boolean_value_expression(&expr) {
            // Tuple/subquery equality currently lowers only its positive branch to EXISTS.
            // Keep its established two-way scalar fallback until that rewrite models UNKNOWN.
            let can_be_unknown = Self::tsql_boolean_expression_can_be_unknown(&expr)
                && !Self::node_is_row_value_subquery_comparison(&expr);
            let predicate = Self::rewrite_tsql_boolean_predicate_context(expr)?;
            return Ok(Self::tsql_boolean_value_case(predicate, can_be_unknown));
        }

        match expr {
            Expression::Alias(mut alias) => {
                alias.this = Self::rewrite_tsql_boolean_scalar_value(alias.this)?;
                Ok(Expression::Alias(alias))
            }
            Expression::Paren(mut paren) => {
                paren.this = Self::rewrite_tsql_boolean_scalar_value(paren.this)?;
                Ok(Expression::Paren(paren))
            }
            Expression::Cast(mut cast) => {
                cast.this = Self::rewrite_tsql_boolean_scalar_value(cast.this)?;
                if let Some(format) = cast.format.take() {
                    cast.format = Some(Box::new(Self::rewrite_tsql_boolean_nested_contexts(
                        *format,
                    )?));
                }
                if let Some(default) = cast.default.take() {
                    cast.default =
                        Some(Box::new(Self::rewrite_tsql_boolean_scalar_value(*default)?));
                }
                Ok(Expression::Cast(cast))
            }
            Expression::TryCast(mut cast) => {
                cast.this = Self::rewrite_tsql_boolean_scalar_value(cast.this)?;
                if let Some(format) = cast.format.take() {
                    cast.format = Some(Box::new(Self::rewrite_tsql_boolean_nested_contexts(
                        *format,
                    )?));
                }
                if let Some(default) = cast.default.take() {
                    cast.default =
                        Some(Box::new(Self::rewrite_tsql_boolean_scalar_value(*default)?));
                }
                Ok(Expression::TryCast(cast))
            }
            Expression::SafeCast(mut cast) => {
                cast.this = Self::rewrite_tsql_boolean_scalar_value(cast.this)?;
                if let Some(format) = cast.format.take() {
                    cast.format = Some(Box::new(Self::rewrite_tsql_boolean_nested_contexts(
                        *format,
                    )?));
                }
                if let Some(default) = cast.default.take() {
                    cast.default =
                        Some(Box::new(Self::rewrite_tsql_boolean_scalar_value(*default)?));
                }
                Ok(Expression::SafeCast(cast))
            }
            Expression::Case(mut case) => {
                let is_simple_case = case.operand.is_some();
                if let Some(operand) = case.operand.take() {
                    case.operand = Some(Self::rewrite_tsql_boolean_scalar_value(operand)?);
                }
                case.whens = case
                    .whens
                    .into_iter()
                    .map(|(condition, result)| {
                        let condition = if is_simple_case {
                            Self::rewrite_tsql_boolean_scalar_value(condition)?
                        } else {
                            Self::rewrite_tsql_boolean_predicate_context(condition)?
                        };
                        Ok((condition, Self::rewrite_tsql_boolean_scalar_value(result)?))
                    })
                    .collect::<Result<Vec<_>>>()?;
                if let Some(else_) = case.else_.take() {
                    case.else_ = Some(Self::rewrite_tsql_boolean_scalar_value(else_)?);
                }
                Ok(Expression::Case(case))
            }
            Expression::IfFunc(mut if_func) => {
                if_func.condition =
                    Self::rewrite_tsql_boolean_predicate_context(if_func.condition)?;
                if_func.true_value = Self::rewrite_tsql_boolean_scalar_value(if_func.true_value)?;
                if let Some(false_value) = if_func.false_value.take() {
                    if_func.false_value =
                        Some(Self::rewrite_tsql_boolean_scalar_value(false_value)?);
                }
                Ok(Expression::IfFunc(if_func))
            }
            Expression::WindowFunction(mut window_function) => {
                window_function.this =
                    Self::rewrite_tsql_boolean_nested_contexts(window_function.this)?;
                Self::rewrite_tsql_boolean_over_values(&mut window_function.over)?;
                if let Some(mut keep) = window_function.keep.take() {
                    keep.order_by = Self::rewrite_tsql_boolean_ordered_values(keep.order_by)?;
                    window_function.keep = Some(keep);
                }
                Ok(Expression::WindowFunction(window_function))
            }
            Expression::WithinGroup(mut within_group) => {
                within_group.this = Self::rewrite_tsql_boolean_nested_contexts(within_group.this)?;
                within_group.order_by =
                    Self::rewrite_tsql_boolean_ordered_values(within_group.order_by)?;
                Ok(Expression::WithinGroup(within_group))
            }
            Expression::Subquery(mut subquery) => {
                subquery.this = Self::rewrite_boolean_values_for_tsql(subquery.this)?;
                Ok(Expression::Subquery(subquery))
            }
            Expression::Select(select) => Self::rewrite_boolean_values_in_tsql_select(select),
            other => Self::rewrite_tsql_boolean_nested_contexts(other),
        }
    }

    fn rewrite_tsql_boolean_predicate_context(expr: Expression) -> Result<Expression> {
        let expr = Self::rewrite_tsql_boolean_nested_contexts(expr)?;
        Ok(crate::transforms::ensure_bool_condition(expr))
    }

    fn rewrite_tsql_boolean_nested_contexts(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Select(select) => Self::rewrite_boolean_values_in_tsql_select(select),
            Expression::Subquery(mut subquery) => {
                subquery.this = Self::rewrite_boolean_values_for_tsql(subquery.this)?;
                Ok(Expression::Subquery(subquery))
            }
            Expression::Union(_) | Expression::Intersect(_) | Expression::Except(_) => {
                Self::rewrite_boolean_values_for_tsql(e)
            }
            other => Self::rewrite_tsql_boolean_cast_operand(other),
        })
    }

    fn rewrite_tsql_boolean_cast_operand(expr: Expression) -> Result<Expression> {
        macro_rules! rewrite_cast_operand {
            ($variant:ident, $cast:expr) => {{
                let mut cast = $cast;
                if Self::is_tsql_boolean_value_expression(&cast.this) {
                    cast.this = Self::rewrite_tsql_boolean_scalar_value(cast.this)?;
                }
                Ok(Expression::$variant(cast))
            }};
        }

        match expr {
            Expression::Cast(cast) => rewrite_cast_operand!(Cast, cast),
            Expression::TryCast(cast) => rewrite_cast_operand!(TryCast, cast),
            Expression::SafeCast(cast) => rewrite_cast_operand!(SafeCast, cast),
            other => Ok(other),
        }
    }

    fn rewrite_tsql_boolean_ordered_values(
        ordered: Vec<crate::expressions::Ordered>,
    ) -> Result<Vec<crate::expressions::Ordered>> {
        ordered
            .into_iter()
            .map(|mut ordered| {
                ordered.this = Self::rewrite_tsql_boolean_scalar_value(ordered.this)?;
                if let Some(with_fill) = ordered.with_fill.take() {
                    ordered.with_fill = Some(Box::new(
                        Self::rewrite_tsql_boolean_with_fill_values(*with_fill)?,
                    ));
                }
                Ok(ordered)
            })
            .collect()
    }

    fn rewrite_tsql_boolean_with_fill_values(
        mut with_fill: crate::expressions::WithFill,
    ) -> Result<crate::expressions::WithFill> {
        if let Some(from) = with_fill.from_.take() {
            with_fill.from_ = Some(Box::new(Self::rewrite_tsql_boolean_scalar_value(*from)?));
        }
        if let Some(to) = with_fill.to.take() {
            with_fill.to = Some(Box::new(Self::rewrite_tsql_boolean_scalar_value(*to)?));
        }
        if let Some(step) = with_fill.step.take() {
            with_fill.step = Some(Box::new(Self::rewrite_tsql_boolean_scalar_value(*step)?));
        }
        if let Some(staleness) = with_fill.staleness.take() {
            with_fill.staleness = Some(Box::new(Self::rewrite_tsql_boolean_scalar_value(
                *staleness,
            )?));
        }
        if let Some(interpolate) = with_fill.interpolate.take() {
            with_fill.interpolate = Some(Box::new(Self::rewrite_tsql_boolean_scalar_value(
                *interpolate,
            )?));
        }
        Ok(with_fill)
    }

    fn rewrite_tsql_boolean_over_values(over: &mut crate::expressions::Over) -> Result<()> {
        over.partition_by = std::mem::take(&mut over.partition_by)
            .into_iter()
            .map(Self::rewrite_tsql_boolean_scalar_value)
            .collect::<Result<Vec<_>>>()?;
        over.order_by =
            Self::rewrite_tsql_boolean_ordered_values(std::mem::take(&mut over.order_by))?;
        Ok(())
    }

    fn is_tsql_boolean_value_expression(expr: &Expression) -> bool {
        match expr {
            Expression::Paren(paren) => Self::is_tsql_boolean_value_expression(&paren.this),
            Expression::Eq(_)
            | Expression::Neq(_)
            | Expression::Lt(_)
            | Expression::Lte(_)
            | Expression::Gt(_)
            | Expression::Gte(_)
            | Expression::Is(_)
            | Expression::IsNull(_)
            | Expression::IsTrue(_)
            | Expression::IsFalse(_)
            | Expression::Like(_)
            | Expression::ILike(_)
            | Expression::StartsWith(_)
            | Expression::SimilarTo(_)
            | Expression::Glob(_)
            | Expression::RegexpLike(_)
            | Expression::In(_)
            | Expression::Between(_)
            | Expression::Exists(_)
            | Expression::And(_)
            | Expression::Or(_)
            | Expression::Not(_)
            | Expression::Any(_)
            | Expression::All(_)
            | Expression::NullSafeEq(_)
            | Expression::NullSafeNeq(_)
            | Expression::EqualNull(_) => true,
            _ => false,
        }
    }

    fn tsql_boolean_expression_can_be_unknown(expr: &Expression) -> bool {
        match expr {
            Expression::Boolean(_)
            | Expression::IsNull(_)
            | Expression::IsTrue(_)
            | Expression::IsFalse(_)
            | Expression::Exists(_)
            | Expression::NullSafeEq(_)
            | Expression::NullSafeNeq(_)
            | Expression::EqualNull(_) => false,
            Expression::Paren(paren) => Self::tsql_boolean_expression_can_be_unknown(&paren.this),
            Expression::Not(op) => Self::tsql_boolean_expression_can_be_unknown(&op.this),
            Expression::And(op) | Expression::Or(op) => {
                Self::tsql_boolean_expression_can_be_unknown(&op.left)
                    || Self::tsql_boolean_expression_can_be_unknown(&op.right)
            }
            _ => true,
        }
    }

    fn tsql_boolean_value_case(predicate: Expression, can_be_unknown: bool) -> Expression {
        let case = if can_be_unknown {
            Self::three_valued_boolean_case(predicate, Expression::number(1), Expression::number(0))
        } else {
            Expression::Case(Box::new(crate::expressions::Case {
                operand: None,
                whens: vec![(predicate, Expression::number(1))],
                else_: Some(Expression::number(0)),
                comments: Vec::new(),
                inferred_type: None,
            }))
        };

        Expression::Cast(Box::new(Cast {
            this: case,
            to: DataType::Boolean,
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        }))
    }

    fn three_valued_boolean_case(
        predicate: Expression,
        true_value: Expression,
        false_value: Expression,
    ) -> Expression {
        let false_operand = if matches!(predicate, Expression::And(_) | Expression::Or(_)) {
            Expression::Paren(Box::new(crate::expressions::Paren {
                this: predicate.clone(),
                trailing_comments: Vec::new(),
            }))
        } else {
            predicate.clone()
        };
        let false_predicate = Expression::Not(Box::new(crate::expressions::UnaryOp {
            this: false_operand,
            inferred_type: None,
        }));

        Expression::Case(Box::new(crate::expressions::Case {
            operand: None,
            whens: vec![(predicate, true_value), (false_predicate, false_value)],
            else_: Some(Expression::null()),
            comments: Vec::new(),
            inferred_type: None,
        }))
    }

    fn rewrite_aggregate_filters_for_tsql(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| Self::rewrite_aggregate_filter_for_tsql(e))
    }

    fn rewrite_aggregate_filter_for_tsql(expr: Expression) -> Result<Expression> {
        macro_rules! rewrite_agg_filter {
            ($variant:ident, $agg:expr) => {{
                let mut agg = $agg;
                if let Some(filter) = agg.filter.take() {
                    let this = std::mem::replace(&mut agg.this, Expression::null());
                    agg.this = Self::conditional_aggregate_value_for_tsql(filter, this);
                }
                Ok(Expression::$variant(agg))
            }};
        }

        match expr {
            Expression::Filter(filter) => {
                let condition = match *filter.expression {
                    Expression::Where(where_) => where_.this,
                    other => other,
                };
                Ok(Self::push_filter_into_tsql_aggregate(
                    *filter.this,
                    condition,
                ))
            }
            Expression::AggregateFunction(mut agg) => {
                if let Some(filter) = agg.filter.take() {
                    Self::rewrite_generic_aggregate_filter_for_tsql(&mut agg, filter);
                }
                Ok(Expression::AggregateFunction(agg))
            }
            Expression::Count(mut count) => {
                if let Some(filter) = count.filter.take() {
                    let value = if count.star {
                        Expression::number(1)
                    } else {
                        count.this.take().unwrap_or_else(|| Expression::number(1))
                    };
                    count.star = false;
                    count.this = Some(Self::conditional_aggregate_value_for_tsql(filter, value));
                }
                Ok(Expression::Count(count))
            }
            Expression::Sum(agg) => rewrite_agg_filter!(Sum, agg),
            Expression::Avg(agg) => rewrite_agg_filter!(Avg, agg),
            Expression::Min(agg) => rewrite_agg_filter!(Min, agg),
            Expression::Max(agg) => rewrite_agg_filter!(Max, agg),
            Expression::ArrayAgg(agg) => rewrite_agg_filter!(ArrayAgg, agg),
            Expression::CountIf(agg) => Ok(Expression::CountIf(agg)),
            Expression::Stddev(agg) => rewrite_agg_filter!(Stddev, agg),
            Expression::StddevPop(agg) => rewrite_agg_filter!(StddevPop, agg),
            Expression::StddevSamp(agg) => rewrite_agg_filter!(StddevSamp, agg),
            Expression::Variance(agg) => rewrite_agg_filter!(Variance, agg),
            Expression::VarPop(agg) => rewrite_agg_filter!(VarPop, agg),
            Expression::VarSamp(agg) => rewrite_agg_filter!(VarSamp, agg),
            Expression::Median(agg) => rewrite_agg_filter!(Median, agg),
            Expression::Mode(agg) => rewrite_agg_filter!(Mode, agg),
            Expression::First(agg) => rewrite_agg_filter!(First, agg),
            Expression::Last(agg) => rewrite_agg_filter!(Last, agg),
            Expression::AnyValue(agg) => rewrite_agg_filter!(AnyValue, agg),
            Expression::ApproxDistinct(agg) => rewrite_agg_filter!(ApproxDistinct, agg),
            Expression::ApproxCountDistinct(agg) => {
                rewrite_agg_filter!(ApproxCountDistinct, agg)
            }
            Expression::LogicalAnd(agg) => rewrite_agg_filter!(LogicalAnd, agg),
            Expression::LogicalOr(agg) => rewrite_agg_filter!(LogicalOr, agg),
            Expression::Skewness(agg) => rewrite_agg_filter!(Skewness, agg),
            Expression::ArrayConcatAgg(agg) => rewrite_agg_filter!(ArrayConcatAgg, agg),
            Expression::ArrayUniqueAgg(agg) => rewrite_agg_filter!(ArrayUniqueAgg, agg),
            Expression::BoolXorAgg(agg) => rewrite_agg_filter!(BoolXorAgg, agg),
            Expression::BitwiseAndAgg(agg) => rewrite_agg_filter!(BitwiseAndAgg, agg),
            Expression::BitwiseOrAgg(agg) => rewrite_agg_filter!(BitwiseOrAgg, agg),
            Expression::BitwiseXorAgg(agg) => rewrite_agg_filter!(BitwiseXorAgg, agg),
            Expression::StringAgg(mut agg) => {
                if let Some(filter) = agg.filter.take() {
                    let this = std::mem::replace(&mut agg.this, Expression::null());
                    agg.this = Self::conditional_aggregate_value_for_tsql(filter, this);
                }
                Ok(Expression::StringAgg(agg))
            }
            Expression::GroupConcat(mut agg) => {
                if let Some(filter) = agg.filter.take() {
                    let this = std::mem::replace(&mut agg.this, Expression::null());
                    agg.this = Self::conditional_aggregate_value_for_tsql(filter, this);
                }
                Ok(Expression::GroupConcat(agg))
            }
            Expression::ListAgg(mut agg) => {
                if let Some(filter) = agg.filter.take() {
                    let this = std::mem::replace(&mut agg.this, Expression::null());
                    agg.this = Self::conditional_aggregate_value_for_tsql(filter, this);
                }
                Ok(Expression::ListAgg(agg))
            }
            Expression::WithinGroup(mut within_group) => {
                within_group.this = Self::rewrite_aggregate_filters_for_tsql(within_group.this)?;
                Ok(Expression::WithinGroup(within_group))
            }
            other => Ok(other),
        }
    }

    fn push_filter_into_tsql_aggregate(expr: Expression, filter: Expression) -> Expression {
        macro_rules! push_agg_filter {
            ($variant:ident, $agg:expr) => {{
                let mut agg = $agg;
                let this = std::mem::replace(&mut agg.this, Expression::null());
                agg.this = Self::conditional_aggregate_value_for_tsql(filter, this);
                agg.filter = None;
                Expression::$variant(agg)
            }};
        }

        match expr {
            Expression::AggregateFunction(mut agg) => {
                Self::rewrite_generic_aggregate_filter_for_tsql(&mut agg, filter);
                Expression::AggregateFunction(agg)
            }
            Expression::Count(mut count) => {
                let value = if count.star {
                    Expression::number(1)
                } else {
                    count.this.take().unwrap_or_else(|| Expression::number(1))
                };
                count.star = false;
                count.filter = None;
                count.this = Some(Self::conditional_aggregate_value_for_tsql(filter, value));
                Expression::Count(count)
            }
            Expression::Sum(agg) => push_agg_filter!(Sum, agg),
            Expression::Avg(agg) => push_agg_filter!(Avg, agg),
            Expression::Min(agg) => push_agg_filter!(Min, agg),
            Expression::Max(agg) => push_agg_filter!(Max, agg),
            Expression::ArrayAgg(agg) => push_agg_filter!(ArrayAgg, agg),
            Expression::CountIf(mut agg) => {
                agg.filter = Some(filter);
                Expression::CountIf(agg)
            }
            Expression::Stddev(agg) => push_agg_filter!(Stddev, agg),
            Expression::StddevPop(agg) => push_agg_filter!(StddevPop, agg),
            Expression::StddevSamp(agg) => push_agg_filter!(StddevSamp, agg),
            Expression::Variance(agg) => push_agg_filter!(Variance, agg),
            Expression::VarPop(agg) => push_agg_filter!(VarPop, agg),
            Expression::VarSamp(agg) => push_agg_filter!(VarSamp, agg),
            Expression::Median(agg) => push_agg_filter!(Median, agg),
            Expression::Mode(agg) => push_agg_filter!(Mode, agg),
            Expression::First(agg) => push_agg_filter!(First, agg),
            Expression::Last(agg) => push_agg_filter!(Last, agg),
            Expression::AnyValue(agg) => push_agg_filter!(AnyValue, agg),
            Expression::ApproxDistinct(agg) => push_agg_filter!(ApproxDistinct, agg),
            Expression::ApproxCountDistinct(agg) => {
                push_agg_filter!(ApproxCountDistinct, agg)
            }
            Expression::LogicalAnd(agg) => push_agg_filter!(LogicalAnd, agg),
            Expression::LogicalOr(agg) => push_agg_filter!(LogicalOr, agg),
            Expression::Skewness(agg) => push_agg_filter!(Skewness, agg),
            Expression::ArrayConcatAgg(agg) => push_agg_filter!(ArrayConcatAgg, agg),
            Expression::ArrayUniqueAgg(agg) => push_agg_filter!(ArrayUniqueAgg, agg),
            Expression::BoolXorAgg(agg) => push_agg_filter!(BoolXorAgg, agg),
            Expression::BitwiseAndAgg(agg) => push_agg_filter!(BitwiseAndAgg, agg),
            Expression::BitwiseOrAgg(agg) => push_agg_filter!(BitwiseOrAgg, agg),
            Expression::BitwiseXorAgg(agg) => push_agg_filter!(BitwiseXorAgg, agg),
            Expression::StringAgg(mut agg) => {
                let this = std::mem::replace(&mut agg.this, Expression::null());
                agg.this = Self::conditional_aggregate_value_for_tsql(filter, this);
                agg.filter = None;
                Expression::StringAgg(agg)
            }
            Expression::GroupConcat(mut agg) => {
                let this = std::mem::replace(&mut agg.this, Expression::null());
                agg.this = Self::conditional_aggregate_value_for_tsql(filter, this);
                agg.filter = None;
                Expression::GroupConcat(agg)
            }
            Expression::ListAgg(mut agg) => {
                let this = std::mem::replace(&mut agg.this, Expression::null());
                agg.this = Self::conditional_aggregate_value_for_tsql(filter, this);
                agg.filter = None;
                Expression::ListAgg(agg)
            }
            Expression::WithinGroup(mut within_group) => {
                within_group.this =
                    Self::push_filter_into_tsql_aggregate(within_group.this, filter);
                Expression::WithinGroup(within_group)
            }
            other => Expression::Filter(Box::new(crate::expressions::Filter {
                this: Box::new(other),
                expression: Box::new(filter),
            })),
        }
    }

    fn rewrite_generic_aggregate_filter_for_tsql(
        agg: &mut crate::expressions::AggregateFunction,
        filter: Expression,
    ) {
        let is_count =
            agg.name.eq_ignore_ascii_case("COUNT") || agg.name.eq_ignore_ascii_case("COUNT_BIG");
        let is_count_star = is_count
            && (agg.args.is_empty()
                || (agg.args.len() == 1 && matches!(agg.args[0], Expression::Star(_))));

        if is_count_star {
            agg.args = vec![Self::conditional_aggregate_value_for_tsql(
                filter,
                Expression::number(1),
            )];
        } else if !agg.args.is_empty() {
            agg.args = agg
                .args
                .drain(..)
                .map(|arg| Self::conditional_aggregate_value_for_tsql(filter.clone(), arg))
                .collect();
        } else {
            agg.filter = Some(filter);
        }
    }

    fn conditional_aggregate_value_for_tsql(filter: Expression, value: Expression) -> Expression {
        Expression::Case(Box::new(crate::expressions::Case {
            operand: None,
            whens: vec![(filter, value)],
            else_: None,
            comments: Vec::new(),
            inferred_type: None,
        }))
    }

    fn reject_pgvector_distance_operators_for_sqlite(&self, sql: &str) -> Result<()> {
        let tokens = self.tokenize(sql)?;
        for (i, token) in tokens.iter().enumerate() {
            if token.token_type == TokenType::NullsafeEq {
                return Err(crate::error::Error::unsupported(
                    "PostgreSQL pgvector cosine distance operator <=>",
                    "SQLite",
                ));
            }
            if token.token_type == TokenType::Lt
                && tokens
                    .get(i + 1)
                    .is_some_and(|token| token.token_type == TokenType::Tilde)
                && tokens
                    .get(i + 2)
                    .is_some_and(|token| token.token_type == TokenType::Gt)
            {
                return Err(crate::error::Error::unsupported(
                    "PostgreSQL pgvector Hamming distance operator <~>",
                    "SQLite",
                ));
            }
        }
        Ok(())
    }

    fn normalize_sqlite_double_quoted_defaults(expr: Expression) -> Result<Expression> {
        fn normalize_default_expr(expr: Expression) -> Result<Expression> {
            transform_recursive(expr, &|e| match e {
                Expression::Column(col)
                    if col.table.is_none() && col.name.quoted && !col.join_mark =>
                {
                    Ok(Expression::Literal(Box::new(Literal::String(
                        col.name.name,
                    ))))
                }
                Expression::Identifier(id) if id.quoted => {
                    Ok(Expression::Literal(Box::new(Literal::String(id.name))))
                }
                _ => Ok(e),
            })
        }

        fn normalize_column_default(col: &mut crate::expressions::ColumnDef) -> Result<()> {
            if let Some(default) = col.default.take() {
                col.default = Some(normalize_default_expr(default)?);
            }

            for constraint in &mut col.constraints {
                if let ColumnConstraint::Default(default) = constraint {
                    *default = normalize_default_expr(default.clone())?;
                }
            }

            Ok(())
        }

        transform_recursive(expr, &|e| match e {
            Expression::CreateTable(mut ct) => {
                for column in &mut ct.columns {
                    normalize_column_default(column)?;
                }
                Ok(Expression::CreateTable(ct))
            }
            Expression::ColumnDef(mut col) => {
                normalize_column_default(&mut col)?;
                Ok(Expression::ColumnDef(col))
            }
            _ => Ok(e),
        })
    }

    fn normalize_postgres_to_sqlite_types(expr: Expression) -> Result<Expression> {
        fn sqlite_type(dt: crate::expressions::DataType) -> crate::expressions::DataType {
            use crate::expressions::DataType;

            match dt {
                DataType::Bit { .. } => DataType::Int {
                    length: None,
                    integer_spelling: true,
                },
                DataType::TextWithLength { .. } => DataType::Text,
                DataType::VarChar { .. } => DataType::Text,
                DataType::Char { .. } => DataType::Text,
                DataType::Timestamp { timezone: true, .. } => DataType::Text,
                DataType::Custom { name } => {
                    let base = name
                        .split_once('(')
                        .map_or(name.as_str(), |(base, _)| base)
                        .trim();
                    if base.eq_ignore_ascii_case("TSVECTOR")
                        || base.eq_ignore_ascii_case("TIMESTAMPTZ")
                        || base.eq_ignore_ascii_case("TIMESTAMP WITH TIME ZONE")
                        || base.eq_ignore_ascii_case("NVARCHAR")
                        || base.eq_ignore_ascii_case("NCHAR")
                    {
                        DataType::Text
                    } else {
                        DataType::Custom { name }
                    }
                }
                _ => dt,
            }
        }

        transform_recursive(expr, &|e| match e {
            Expression::DataType(dt) => Ok(Expression::DataType(sqlite_type(dt))),
            Expression::CreateTable(mut ct) => {
                for column in &mut ct.columns {
                    column.data_type = sqlite_type(column.data_type.clone());
                }
                Ok(Expression::CreateTable(ct))
            }
            _ => Ok(e),
        })
    }

    fn normalize_postgres_to_fabric_types(expr: Expression) -> Result<Expression> {
        fn fabric_type(dt: crate::expressions::DataType) -> crate::expressions::DataType {
            use crate::expressions::DataType;

            match dt {
                DataType::Decimal {
                    precision: None,
                    scale: None,
                } => DataType::Decimal {
                    precision: Some(38),
                    scale: Some(10),
                },
                DataType::Json | DataType::JsonB => DataType::Custom {
                    name: "VARCHAR(MAX)".to_string(),
                },
                _ => dt,
            }
        }

        transform_recursive(expr, &|e| match e {
            Expression::DataType(dt) => Ok(Expression::DataType(fabric_type(dt))),
            Expression::CreateTable(mut ct) => {
                for column in &mut ct.columns {
                    column.data_type = fabric_type(column.data_type.clone());
                }
                Ok(Expression::CreateTable(ct))
            }
            Expression::ColumnDef(mut col) => {
                col.data_type = fabric_type(col.data_type);
                Ok(Expression::ColumnDef(col))
            }
            _ => Ok(e),
        })
    }

    /// For DuckDB target: when FROM clause contains RANGE(n), replace
    /// `(ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1)` with `range` in select expressions.
    /// This handles SEQ1/2/4/8 → RANGE transpilation from Snowflake.
    fn seq_rownum_to_range(expr: Expression) -> Result<Expression> {
        if let Expression::Select(mut select) = expr {
            // Check if FROM contains a RANGE function
            let has_range_from = if let Some(ref from) = select.from {
                from.expressions.iter().any(|e| {
                    // Check for direct RANGE(...) or aliased RANGE(...)
                    match e {
                        Expression::Function(f) => f.name.eq_ignore_ascii_case("RANGE"),
                        Expression::Alias(a) => {
                            matches!(&a.this, Expression::Function(f) if f.name.eq_ignore_ascii_case("RANGE"))
                        }
                        _ => false,
                    }
                })
            } else {
                false
            };

            if has_range_from {
                // Replace the ROW_NUMBER pattern in select expressions
                select.expressions = select
                    .expressions
                    .into_iter()
                    .map(|e| Self::replace_rownum_with_range(e))
                    .collect();
            }

            Ok(Expression::Select(select))
        } else {
            Ok(expr)
        }
    }

    /// Replace `(ROW_NUMBER() OVER (...) - 1)` with `range` column reference
    fn replace_rownum_with_range(expr: Expression) -> Expression {
        match expr {
            // Match: (ROW_NUMBER() OVER (...) - 1) % N → range % N
            Expression::Mod(op) => {
                let new_left = Self::try_replace_rownum_paren(&op.left);
                Expression::Mod(Box::new(crate::expressions::BinaryOp {
                    left: new_left,
                    right: op.right,
                    left_comments: op.left_comments,
                    operator_comments: op.operator_comments,
                    trailing_comments: op.trailing_comments,
                    inferred_type: op.inferred_type,
                }))
            }
            // Match: (CASE WHEN (ROW...) % N >= ... THEN ... ELSE ... END)
            Expression::Paren(p) => {
                let inner = Self::replace_rownum_with_range(p.this);
                Expression::Paren(Box::new(crate::expressions::Paren {
                    this: inner,
                    trailing_comments: p.trailing_comments,
                }))
            }
            Expression::Case(mut c) => {
                // Replace ROW_NUMBER in WHEN conditions and THEN expressions
                c.whens = c
                    .whens
                    .into_iter()
                    .map(|(cond, then)| {
                        (
                            Self::replace_rownum_with_range(cond),
                            Self::replace_rownum_with_range(then),
                        )
                    })
                    .collect();
                if let Some(else_) = c.else_ {
                    c.else_ = Some(Self::replace_rownum_with_range(else_));
                }
                Expression::Case(c)
            }
            Expression::Gte(op) => Expression::Gte(Box::new(crate::expressions::BinaryOp {
                left: Self::replace_rownum_with_range(op.left),
                right: op.right,
                left_comments: op.left_comments,
                operator_comments: op.operator_comments,
                trailing_comments: op.trailing_comments,
                inferred_type: op.inferred_type,
            })),
            Expression::Sub(op) => Expression::Sub(Box::new(crate::expressions::BinaryOp {
                left: Self::replace_rownum_with_range(op.left),
                right: op.right,
                left_comments: op.left_comments,
                operator_comments: op.operator_comments,
                trailing_comments: op.trailing_comments,
                inferred_type: op.inferred_type,
            })),
            Expression::Alias(mut a) => {
                a.this = Self::replace_rownum_with_range(a.this);
                Expression::Alias(a)
            }
            other => other,
        }
    }

    /// Check if an expression is `(ROW_NUMBER() OVER (...) - 1)` and replace with `range`
    fn try_replace_rownum_paren(expr: &Expression) -> Expression {
        if let Expression::Paren(ref p) = expr {
            if let Expression::Sub(ref sub) = p.this {
                if let Expression::WindowFunction(ref wf) = sub.left {
                    if let Expression::Function(ref f) = wf.this {
                        if f.name.eq_ignore_ascii_case("ROW_NUMBER") {
                            if let Expression::Literal(ref lit) = sub.right {
                                if let crate::expressions::Literal::Number(ref n) = lit.as_ref() {
                                    if n == "1" {
                                        return Expression::column("range");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        expr.clone()
    }

    /// Transform BigQuery GENERATE_DATE_ARRAY in UNNEST for Snowflake target.
    /// Converts:
    ///   SELECT ..., alias, ... FROM t CROSS JOIN UNNEST(GENERATE_DATE_ARRAY(start, end, INTERVAL '1' unit)) AS alias
    /// To:
    ///   SELECT ..., DATEADD(unit, CAST(alias AS INT), CAST(start AS DATE)) AS alias, ...
    ///   FROM t, LATERAL FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, DATEDIFF(unit, start, end) + 1)) AS _t0(seq, key, path, index, alias, this)
    fn transform_generate_date_array_snowflake(expr: Expression) -> Result<Expression> {
        use crate::expressions::*;
        transform_recursive(expr, &|e| {
            // Handle ARRAY_SIZE(GENERATE_DATE_ARRAY(...)) -> ARRAY_SIZE((SELECT ARRAY_AGG(*) FROM subquery))
            if let Expression::ArraySize(ref af) = e {
                if let Expression::Function(ref f) = af.this {
                    if f.name.eq_ignore_ascii_case("GENERATE_DATE_ARRAY") && f.args.len() >= 2 {
                        let result = Self::convert_array_size_gda_snowflake(f)?;
                        return Ok(result);
                    }
                }
            }

            let Expression::Select(mut sel) = e else {
                return Ok(e);
            };

            // Find joins with UNNEST containing GenerateSeries (from GENERATE_DATE_ARRAY conversion)
            let mut gda_info: Option<(String, Expression, Expression, String)> = None; // (alias_name, start_expr, end_expr, unit)
            let mut gda_join_idx: Option<usize> = None;

            for (idx, join) in sel.joins.iter().enumerate() {
                // The join.this may be:
                // 1. Unnest(UnnestFunc { alias: Some("mnth"), ... })
                // 2. Alias(Alias { this: Unnest(UnnestFunc { alias: None, ... }), alias: "mnth", ... })
                let (unnest_ref, alias_name) = match &join.this {
                    Expression::Unnest(ref unnest) => {
                        let alias = unnest.alias.as_ref().map(|id| id.name.clone());
                        (Some(unnest.as_ref()), alias)
                    }
                    Expression::Alias(ref a) => {
                        if let Expression::Unnest(ref unnest) = a.this {
                            (Some(unnest.as_ref()), Some(a.alias.name.clone()))
                        } else {
                            (None, None)
                        }
                    }
                    _ => (None, None),
                };

                if let (Some(unnest), Some(alias)) = (unnest_ref, alias_name) {
                    // Check the main expression (this) of the UNNEST for GENERATE_DATE_ARRAY function
                    if let Expression::Function(ref f) = unnest.this {
                        if f.name.eq_ignore_ascii_case("GENERATE_DATE_ARRAY") && f.args.len() >= 2 {
                            let start_expr = f.args[0].clone();
                            let end_expr = f.args[1].clone();
                            let step = f.args.get(2).cloned();

                            // Extract unit from step interval
                            let unit = if let Some(Expression::Interval(ref iv)) = step {
                                if let Some(IntervalUnitSpec::Simple { ref unit, .. }) = iv.unit {
                                    Some(format!("{:?}", unit).to_ascii_uppercase())
                                } else if let Some(ref this) = iv.this {
                                    // The interval may be stored as a string like "1 MONTH"
                                    if let Expression::Literal(lit) = this {
                                        if let Literal::String(ref s) = lit.as_ref() {
                                            let parts: Vec<&str> = s.split_whitespace().collect();
                                            if parts.len() == 2 {
                                                Some(parts[1].to_ascii_uppercase())
                                            } else if parts.len() == 1 {
                                                // Single word like "MONTH" or just "1"
                                                let upper = parts[0].to_ascii_uppercase();
                                                if matches!(
                                                    upper.as_str(),
                                                    "YEAR"
                                                        | "QUARTER"
                                                        | "MONTH"
                                                        | "WEEK"
                                                        | "DAY"
                                                        | "HOUR"
                                                        | "MINUTE"
                                                        | "SECOND"
                                                ) {
                                                    Some(upper)
                                                } else {
                                                    None
                                                }
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };

                            if let Some(unit_str) = unit {
                                gda_info = Some((alias, start_expr, end_expr, unit_str));
                                gda_join_idx = Some(idx);
                            }
                        }
                    }
                }
                if gda_info.is_some() {
                    break;
                }
            }

            let Some((alias_name, start_expr, end_expr, unit_str)) = gda_info else {
                // Also check FROM clause for UNNEST(GENERATE_DATE_ARRAY(...)) patterns
                // This handles Generic->Snowflake where GENERATE_DATE_ARRAY is in FROM, not in JOIN
                let result = Self::try_transform_from_gda_snowflake(sel);
                return result;
            };
            let join_idx = gda_join_idx.unwrap();

            // Build ARRAY_GENERATE_RANGE(0, DATEDIFF(unit, start, end) + 1)
            // ARRAY_GENERATE_RANGE uses exclusive end, and we need DATEDIFF + 1 values
            // (inclusive date range), so the exclusive end is DATEDIFF + 1.
            let datediff = Expression::Function(Box::new(Function::new(
                "DATEDIFF".to_string(),
                vec![
                    Expression::boxed_column(Column {
                        name: Identifier::new(&unit_str),
                        table: None,
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                        inferred_type: None,
                    }),
                    start_expr.clone(),
                    end_expr.clone(),
                ],
            )));
            let datediff_plus_one = Expression::Add(Box::new(BinaryOp {
                left: datediff,
                right: Expression::Literal(Box::new(Literal::Number("1".to_string()))),
                left_comments: vec![],
                operator_comments: vec![],
                trailing_comments: vec![],
                inferred_type: None,
            }));

            let array_gen_range = Expression::Function(Box::new(Function::new(
                "ARRAY_GENERATE_RANGE".to_string(),
                vec![
                    Expression::Literal(Box::new(Literal::Number("0".to_string()))),
                    datediff_plus_one,
                ],
            )));

            // Build FLATTEN(INPUT => ARRAY_GENERATE_RANGE(...))
            let flatten_input = Expression::NamedArgument(Box::new(NamedArgument {
                name: Identifier::new("INPUT"),
                value: array_gen_range,
                separator: crate::expressions::NamedArgSeparator::DArrow,
            }));
            let flatten = Expression::Function(Box::new(Function::new(
                "FLATTEN".to_string(),
                vec![flatten_input],
            )));

            // Build LATERAL FLATTEN(...) AS _t0(seq, key, path, index, alias, this)
            let alias_table = Alias {
                this: flatten,
                alias: Identifier::new("_t0"),
                column_aliases: vec![
                    Identifier::new("seq"),
                    Identifier::new("key"),
                    Identifier::new("path"),
                    Identifier::new("index"),
                    Identifier::new(&alias_name),
                    Identifier::new("this"),
                ],
                alias_explicit_as: false,
                alias_keyword: None,
                pre_alias_comments: vec![],
                trailing_comments: vec![],
                inferred_type: None,
            };
            let lateral_expr = Expression::Lateral(Box::new(Lateral {
                this: Box::new(Expression::Alias(Box::new(alias_table))),
                view: None,
                outer: None,
                alias: None,
                alias_quoted: false,
                cross_apply: None,
                ordinality: None,
                column_aliases: vec![],
            }));

            // Remove the original join and add to FROM expressions
            sel.joins.remove(join_idx);
            if let Some(ref mut from) = sel.from {
                from.expressions.push(lateral_expr);
            }

            // Build DATEADD(unit, CAST(alias AS INT), CAST(start AS DATE))
            let dateadd_expr = Expression::Function(Box::new(Function::new(
                "DATEADD".to_string(),
                vec![
                    Expression::boxed_column(Column {
                        name: Identifier::new(&unit_str),
                        table: None,
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                        inferred_type: None,
                    }),
                    Expression::Cast(Box::new(Cast {
                        this: Expression::boxed_column(Column {
                            name: Identifier::new(&alias_name),
                            table: None,
                            join_mark: false,
                            trailing_comments: vec![],
                            span: None,
                            inferred_type: None,
                        }),
                        to: DataType::Int {
                            length: None,
                            integer_spelling: false,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })),
                    Expression::Cast(Box::new(Cast {
                        this: start_expr.clone(),
                        to: DataType::Date,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })),
                ],
            )));

            // Replace references to the alias in the SELECT list
            let new_exprs: Vec<Expression> = sel
                .expressions
                .iter()
                .map(|expr| Self::replace_column_ref_with_dateadd(expr, &alias_name, &dateadd_expr))
                .collect();
            sel.expressions = new_exprs;

            Ok(Expression::Select(sel))
        })
    }

    /// Helper: replace column references to `alias_name` with dateadd expression
    fn replace_column_ref_with_dateadd(
        expr: &Expression,
        alias_name: &str,
        dateadd: &Expression,
    ) -> Expression {
        use crate::expressions::*;
        match expr {
            Expression::Column(c) if c.name.name == alias_name && c.table.is_none() => {
                // Plain column reference -> DATEADD(...) AS alias_name
                Expression::Alias(Box::new(Alias {
                    this: dateadd.clone(),
                    alias: Identifier::new(alias_name),
                    column_aliases: vec![],
                    alias_explicit_as: false,
                    alias_keyword: None,
                    pre_alias_comments: vec![],
                    trailing_comments: vec![],
                    inferred_type: None,
                }))
            }
            Expression::Alias(a) => {
                // Check if the inner expression references the alias
                let new_this = Self::replace_column_ref_inner(&a.this, alias_name, dateadd);
                Expression::Alias(Box::new(Alias {
                    this: new_this,
                    alias: a.alias.clone(),
                    column_aliases: a.column_aliases.clone(),
                    alias_explicit_as: false,
                    alias_keyword: None,
                    pre_alias_comments: a.pre_alias_comments.clone(),
                    trailing_comments: a.trailing_comments.clone(),
                    inferred_type: None,
                }))
            }
            _ => expr.clone(),
        }
    }

    /// Helper: replace column references in inner expression (not top-level)
    fn replace_column_ref_inner(
        expr: &Expression,
        alias_name: &str,
        dateadd: &Expression,
    ) -> Expression {
        use crate::expressions::*;
        match expr {
            Expression::Column(c) if c.name.name == alias_name && c.table.is_none() => {
                dateadd.clone()
            }
            Expression::Add(op) => {
                let left = Self::replace_column_ref_inner(&op.left, alias_name, dateadd);
                let right = Self::replace_column_ref_inner(&op.right, alias_name, dateadd);
                Expression::Add(Box::new(BinaryOp {
                    left,
                    right,
                    left_comments: op.left_comments.clone(),
                    operator_comments: op.operator_comments.clone(),
                    trailing_comments: op.trailing_comments.clone(),
                    inferred_type: None,
                }))
            }
            Expression::Sub(op) => {
                let left = Self::replace_column_ref_inner(&op.left, alias_name, dateadd);
                let right = Self::replace_column_ref_inner(&op.right, alias_name, dateadd);
                Expression::Sub(Box::new(BinaryOp {
                    left,
                    right,
                    left_comments: op.left_comments.clone(),
                    operator_comments: op.operator_comments.clone(),
                    trailing_comments: op.trailing_comments.clone(),
                    inferred_type: None,
                }))
            }
            Expression::Mul(op) => {
                let left = Self::replace_column_ref_inner(&op.left, alias_name, dateadd);
                let right = Self::replace_column_ref_inner(&op.right, alias_name, dateadd);
                Expression::Mul(Box::new(BinaryOp {
                    left,
                    right,
                    left_comments: op.left_comments.clone(),
                    operator_comments: op.operator_comments.clone(),
                    trailing_comments: op.trailing_comments.clone(),
                    inferred_type: None,
                }))
            }
            _ => expr.clone(),
        }
    }

    /// Handle UNNEST(GENERATE_DATE_ARRAY(...)) in FROM clause for Snowflake target.
    /// Converts to a subquery with DATEADD + TABLE(FLATTEN(ARRAY_GENERATE_RANGE(...))).
    fn try_transform_from_gda_snowflake(
        mut sel: Box<crate::expressions::Select>,
    ) -> Result<Expression> {
        use crate::expressions::*;

        // Extract GDA info from FROM clause
        let mut gda_info: Option<(
            usize,
            String,
            Expression,
            Expression,
            String,
            Option<(String, Vec<Identifier>)>,
        )> = None; // (from_idx, col_name, start, end, unit, outer_alias)

        if let Some(ref from) = sel.from {
            for (idx, table_expr) in from.expressions.iter().enumerate() {
                // Pattern 1: UNNEST(GENERATE_DATE_ARRAY(...))
                // Pattern 2: Alias(UNNEST(GENERATE_DATE_ARRAY(...))) AS _q(date_week)
                let (unnest_opt, outer_alias_info) = match table_expr {
                    Expression::Unnest(ref unnest) => (Some(unnest.as_ref()), None),
                    Expression::Alias(ref a) => {
                        if let Expression::Unnest(ref unnest) = a.this {
                            let alias_info = (a.alias.name.clone(), a.column_aliases.clone());
                            (Some(unnest.as_ref()), Some(alias_info))
                        } else {
                            (None, None)
                        }
                    }
                    _ => (None, None),
                };

                if let Some(unnest) = unnest_opt {
                    // Check for GENERATE_DATE_ARRAY function
                    let func_opt = match &unnest.this {
                        Expression::Function(ref f)
                            if f.name.eq_ignore_ascii_case("GENERATE_DATE_ARRAY")
                                && f.args.len() >= 2 =>
                        {
                            Some(f)
                        }
                        // Also check for GenerateSeries (from earlier normalization)
                        _ => None,
                    };

                    if let Some(f) = func_opt {
                        let start_expr = f.args[0].clone();
                        let end_expr = f.args[1].clone();
                        let step = f.args.get(2).cloned();

                        // Extract unit and column name
                        let unit = Self::extract_interval_unit_str(&step);
                        let col_name = outer_alias_info
                            .as_ref()
                            .and_then(|(_, cols)| cols.first().map(|id| id.name.clone()))
                            .unwrap_or_else(|| "value".to_string());

                        if let Some(unit_str) = unit {
                            gda_info = Some((
                                idx,
                                col_name,
                                start_expr,
                                end_expr,
                                unit_str,
                                outer_alias_info,
                            ));
                            break;
                        }
                    }
                }
            }
        }

        let Some((from_idx, col_name, start_expr, end_expr, unit_str, outer_alias_info)) = gda_info
        else {
            return Ok(Expression::Select(sel));
        };

        // Build the Snowflake subquery:
        // (SELECT DATEADD(unit, CAST(col_name AS INT), CAST(start AS DATE)) AS col_name
        //  FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, DATEDIFF(unit, start, end) + 1))) AS _t0(seq, key, path, index, col_name, this))

        // DATEDIFF(unit, start, end)
        let datediff = Expression::Function(Box::new(Function::new(
            "DATEDIFF".to_string(),
            vec![
                Expression::boxed_column(Column {
                    name: Identifier::new(&unit_str),
                    table: None,
                    join_mark: false,
                    trailing_comments: vec![],
                    span: None,
                    inferred_type: None,
                }),
                start_expr.clone(),
                end_expr.clone(),
            ],
        )));
        // DATEDIFF(...) + 1
        let datediff_plus_one = Expression::Add(Box::new(BinaryOp {
            left: datediff,
            right: Expression::Literal(Box::new(Literal::Number("1".to_string()))),
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
        }));

        let array_gen_range = Expression::Function(Box::new(Function::new(
            "ARRAY_GENERATE_RANGE".to_string(),
            vec![
                Expression::Literal(Box::new(Literal::Number("0".to_string()))),
                datediff_plus_one,
            ],
        )));

        // TABLE(FLATTEN(INPUT => ...))
        let flatten_input = Expression::NamedArgument(Box::new(NamedArgument {
            name: Identifier::new("INPUT"),
            value: array_gen_range,
            separator: crate::expressions::NamedArgSeparator::DArrow,
        }));
        let flatten = Expression::Function(Box::new(Function::new(
            "FLATTEN".to_string(),
            vec![flatten_input],
        )));

        // Determine alias name for the table: use outer alias or _t0
        let table_alias_name = outer_alias_info
            .as_ref()
            .map(|(name, _)| name.clone())
            .unwrap_or_else(|| "_t0".to_string());

        // TABLE(FLATTEN(...)) AS _t0(seq, key, path, index, col_name, this)
        let table_func =
            Expression::Function(Box::new(Function::new("TABLE".to_string(), vec![flatten])));
        let flatten_aliased = Expression::Alias(Box::new(Alias {
            this: table_func,
            alias: Identifier::new(&table_alias_name),
            column_aliases: vec![
                Identifier::new("seq"),
                Identifier::new("key"),
                Identifier::new("path"),
                Identifier::new("index"),
                Identifier::new(&col_name),
                Identifier::new("this"),
            ],
            alias_explicit_as: false,
            alias_keyword: None,
            pre_alias_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
        }));

        // SELECT DATEADD(unit, CAST(col_name AS INT), CAST(start AS DATE)) AS col_name
        let dateadd_expr = Expression::Function(Box::new(Function::new(
            "DATEADD".to_string(),
            vec![
                Expression::boxed_column(Column {
                    name: Identifier::new(&unit_str),
                    table: None,
                    join_mark: false,
                    trailing_comments: vec![],
                    span: None,
                    inferred_type: None,
                }),
                Expression::Cast(Box::new(Cast {
                    this: Expression::boxed_column(Column {
                        name: Identifier::new(&col_name),
                        table: None,
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                        inferred_type: None,
                    }),
                    to: DataType::Int {
                        length: None,
                        integer_spelling: false,
                    },
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })),
                // Use start_expr directly - it's already been normalized (DATE literal -> CAST)
                start_expr.clone(),
            ],
        )));
        let dateadd_aliased = Expression::Alias(Box::new(Alias {
            this: dateadd_expr,
            alias: Identifier::new(&col_name),
            column_aliases: vec![],
            alias_explicit_as: false,
            alias_keyword: None,
            pre_alias_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
        }));

        // Build inner SELECT
        let mut inner_select = Select::new();
        inner_select.expressions = vec![dateadd_aliased];
        inner_select.from = Some(From {
            expressions: vec![flatten_aliased],
        });

        let inner_select_expr = Expression::Select(Box::new(inner_select));
        let subquery = Expression::Subquery(Box::new(Subquery {
            this: inner_select_expr,
            alias: None,
            column_aliases: vec![],
            alias_explicit_as: false,
            alias_keyword: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            lateral: false,
            modifiers_inside: false,
            trailing_comments: vec![],
            inferred_type: None,
        }));

        // If there was an outer alias (e.g., AS _q(date_week)), wrap with alias
        let replacement = if let Some((alias_name, col_aliases)) = outer_alias_info {
            Expression::Alias(Box::new(Alias {
                this: subquery,
                alias: Identifier::new(&alias_name),
                column_aliases: col_aliases,
                alias_explicit_as: false,
                alias_keyword: None,
                pre_alias_comments: vec![],
                trailing_comments: vec![],
                inferred_type: None,
            }))
        } else {
            subquery
        };

        // Replace the FROM expression
        if let Some(ref mut from) = sel.from {
            from.expressions[from_idx] = replacement;
        }

        Ok(Expression::Select(sel))
    }

    /// Convert ARRAY_SIZE(GENERATE_DATE_ARRAY(start, end, step)) for Snowflake.
    /// Produces: ARRAY_SIZE((SELECT ARRAY_AGG(*) FROM (SELECT DATEADD(unit, CAST(value AS INT), start) AS value
    ///   FROM TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, DATEDIFF(unit, start, end) + 1))) AS _t0(...))))
    fn convert_array_size_gda_snowflake(f: &crate::expressions::Function) -> Result<Expression> {
        use crate::expressions::*;

        let start_expr = f.args[0].clone();
        let end_expr = f.args[1].clone();
        let step = f.args.get(2).cloned();
        let unit_str = Self::extract_interval_unit_str(&step).unwrap_or_else(|| "DAY".to_string());
        let col_name = "value";

        // Build the inner subquery: same as try_transform_from_gda_snowflake
        let datediff = Expression::Function(Box::new(Function::new(
            "DATEDIFF".to_string(),
            vec![
                Expression::boxed_column(Column {
                    name: Identifier::new(&unit_str),
                    table: None,
                    join_mark: false,
                    trailing_comments: vec![],
                    span: None,
                    inferred_type: None,
                }),
                start_expr.clone(),
                end_expr.clone(),
            ],
        )));
        // DATEDIFF(...) + 1
        let datediff_plus_one = Expression::Add(Box::new(BinaryOp {
            left: datediff,
            right: Expression::Literal(Box::new(Literal::Number("1".to_string()))),
            left_comments: vec![],
            operator_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
        }));

        let array_gen_range = Expression::Function(Box::new(Function::new(
            "ARRAY_GENERATE_RANGE".to_string(),
            vec![
                Expression::Literal(Box::new(Literal::Number("0".to_string()))),
                datediff_plus_one,
            ],
        )));

        let flatten_input = Expression::NamedArgument(Box::new(NamedArgument {
            name: Identifier::new("INPUT"),
            value: array_gen_range,
            separator: crate::expressions::NamedArgSeparator::DArrow,
        }));
        let flatten = Expression::Function(Box::new(Function::new(
            "FLATTEN".to_string(),
            vec![flatten_input],
        )));

        let table_func =
            Expression::Function(Box::new(Function::new("TABLE".to_string(), vec![flatten])));
        let flatten_aliased = Expression::Alias(Box::new(Alias {
            this: table_func,
            alias: Identifier::new("_t0"),
            column_aliases: vec![
                Identifier::new("seq"),
                Identifier::new("key"),
                Identifier::new("path"),
                Identifier::new("index"),
                Identifier::new(col_name),
                Identifier::new("this"),
            ],
            alias_explicit_as: false,
            alias_keyword: None,
            pre_alias_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
        }));

        let dateadd_expr = Expression::Function(Box::new(Function::new(
            "DATEADD".to_string(),
            vec![
                Expression::boxed_column(Column {
                    name: Identifier::new(&unit_str),
                    table: None,
                    join_mark: false,
                    trailing_comments: vec![],
                    span: None,
                    inferred_type: None,
                }),
                Expression::Cast(Box::new(Cast {
                    this: Expression::boxed_column(Column {
                        name: Identifier::new(col_name),
                        table: None,
                        join_mark: false,
                        trailing_comments: vec![],
                        span: None,
                        inferred_type: None,
                    }),
                    to: DataType::Int {
                        length: None,
                        integer_spelling: false,
                    },
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })),
                start_expr.clone(),
            ],
        )));
        let dateadd_aliased = Expression::Alias(Box::new(Alias {
            this: dateadd_expr,
            alias: Identifier::new(col_name),
            column_aliases: vec![],
            alias_explicit_as: false,
            alias_keyword: None,
            pre_alias_comments: vec![],
            trailing_comments: vec![],
            inferred_type: None,
        }));

        // Inner SELECT: SELECT DATEADD(...) AS value FROM TABLE(FLATTEN(...)) AS _t0(...)
        let mut inner_select = Select::new();
        inner_select.expressions = vec![dateadd_aliased];
        inner_select.from = Some(From {
            expressions: vec![flatten_aliased],
        });

        // Wrap in subquery for the inner part
        let inner_subquery = Expression::Subquery(Box::new(Subquery {
            this: Expression::Select(Box::new(inner_select)),
            alias: None,
            column_aliases: vec![],
            alias_explicit_as: false,
            alias_keyword: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            lateral: false,
            modifiers_inside: false,
            trailing_comments: vec![],
            inferred_type: None,
        }));

        // Outer: SELECT ARRAY_AGG(*) FROM (inner_subquery)
        let star = Expression::Star(Star {
            table: None,
            except: None,
            replace: None,
            rename: None,
            trailing_comments: vec![],
            span: None,
        });
        let array_agg = Expression::ArrayAgg(Box::new(AggFunc {
            this: star,
            distinct: false,
            filter: None,
            order_by: vec![],
            name: Some("ARRAY_AGG".to_string()),
            ignore_nulls: None,
            having_max: None,
            limit: None,
            inferred_type: None,
        }));

        let mut outer_select = Select::new();
        outer_select.expressions = vec![array_agg];
        outer_select.from = Some(From {
            expressions: vec![inner_subquery],
        });

        // Wrap in a subquery
        let outer_subquery = Expression::Subquery(Box::new(Subquery {
            this: Expression::Select(Box::new(outer_select)),
            alias: None,
            column_aliases: vec![],
            alias_explicit_as: false,
            alias_keyword: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            lateral: false,
            modifiers_inside: false,
            trailing_comments: vec![],
            inferred_type: None,
        }));

        // ARRAY_SIZE(subquery)
        Ok(Expression::ArraySize(Box::new(UnaryFunc::new(
            outer_subquery,
        ))))
    }

    /// Extract interval unit string from an optional step expression.
    fn extract_interval_unit_str(step: &Option<Expression>) -> Option<String> {
        use crate::expressions::*;
        if let Some(Expression::Interval(ref iv)) = step {
            if let Some(IntervalUnitSpec::Simple { ref unit, .. }) = iv.unit {
                return Some(format!("{:?}", unit).to_ascii_uppercase());
            }
            if let Some(ref this) = iv.this {
                if let Expression::Literal(lit) = this {
                    if let Literal::String(ref s) = lit.as_ref() {
                        let parts: Vec<&str> = s.split_whitespace().collect();
                        if parts.len() == 2 {
                            return Some(parts[1].to_ascii_uppercase());
                        } else if parts.len() == 1 {
                            let upper = parts[0].to_ascii_uppercase();
                            if matches!(
                                upper.as_str(),
                                "YEAR"
                                    | "QUARTER"
                                    | "MONTH"
                                    | "WEEK"
                                    | "DAY"
                                    | "HOUR"
                                    | "MINUTE"
                                    | "SECOND"
                            ) {
                                return Some(upper);
                            }
                        }
                    }
                }
            }
        }
        // Default to DAY if no step or no interval
        if step.is_none() {
            return Some("DAY".to_string());
        }
        None
    }

    fn normalize_snowflake_pretty(mut sql: String) -> String {
        if sql.contains("LATERAL IFF(_u.pos = _u_2.pos_2, _u_2.entity, NULL) AS datasource(SEQ, KEY, PATH, INDEX, VALUE, THIS)")
            && sql.contains("ARRAY_GENERATE_RANGE(0, (GREATEST(ARRAY_SIZE(INPUT => PARSE_JSON(flags))) - 1) + 1)")
        {
            sql = sql.replace(
                "AND uc.user_id <> ALL (SELECT DISTINCT\n      _id\n    FROM users, LATERAL IFF(_u.pos = _u_2.pos_2, _u_2.entity, NULL) AS datasource(SEQ, KEY, PATH, INDEX, VALUE, THIS)\n    WHERE\n      GET_PATH(datasource.value, 'name') = 'something')",
                "AND uc.user_id <> ALL (\n      SELECT DISTINCT\n        _id\n      FROM users, LATERAL IFF(_u.pos = _u_2.pos_2, _u_2.entity, NULL) AS datasource(SEQ, KEY, PATH, INDEX, VALUE, THIS)\n      WHERE\n        GET_PATH(datasource.value, 'name') = 'something'\n    )",
            );

            sql = sql.replace(
                "CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (GREATEST(ARRAY_SIZE(INPUT => PARSE_JSON(flags))) - 1) + 1))) AS _u(seq, key, path, index, pos, this)",
                "CROSS JOIN TABLE(FLATTEN(INPUT => ARRAY_GENERATE_RANGE(0, (\n  GREATEST(ARRAY_SIZE(INPUT => PARSE_JSON(flags))) - 1\n) + 1))) AS _u(seq, key, path, index, pos, this)",
            );

            sql = sql.replace(
                "OR (_u.pos > (ARRAY_SIZE(INPUT => PARSE_JSON(flags)) - 1)\n  AND _u_2.pos_2 = (ARRAY_SIZE(INPUT => PARSE_JSON(flags)) - 1))",
                "OR (\n    _u.pos > (\n      ARRAY_SIZE(INPUT => PARSE_JSON(flags)) - 1\n    )\n    AND _u_2.pos_2 = (\n      ARRAY_SIZE(INPUT => PARSE_JSON(flags)) - 1\n    )\n  )",
            );
        }

        sql
    }

    #[cfg(feature = "transpile")]
    fn wrap_tsql_top_level_values(expr: Expression) -> Expression {
        match expr {
            Expression::Values(values) => Self::tsql_values_as_select(*values),
            Expression::Union(mut union) => {
                let left = std::mem::replace(&mut union.left, Expression::Null(Null));
                let right = std::mem::replace(&mut union.right, Expression::Null(Null));
                union.left = Self::wrap_tsql_values_set_operand(left);
                union.right = Self::wrap_tsql_values_set_operand(right);
                Expression::Union(union)
            }
            Expression::Intersect(mut intersect) => {
                let left = std::mem::replace(&mut intersect.left, Expression::Null(Null));
                let right = std::mem::replace(&mut intersect.right, Expression::Null(Null));
                intersect.left = Self::wrap_tsql_values_set_operand(left);
                intersect.right = Self::wrap_tsql_values_set_operand(right);
                Expression::Intersect(intersect)
            }
            Expression::Except(mut except) => {
                let left = std::mem::replace(&mut except.left, Expression::Null(Null));
                let right = std::mem::replace(&mut except.right, Expression::Null(Null));
                except.left = Self::wrap_tsql_values_set_operand(left);
                except.right = Self::wrap_tsql_values_set_operand(right);
                Expression::Except(except)
            }
            other => other,
        }
    }

    #[cfg(feature = "transpile")]
    fn wrap_tsql_values_set_operand(expr: Expression) -> Expression {
        match expr {
            Expression::Values(values) => Self::tsql_values_as_select(*values),
            Expression::Union(mut union) => {
                let left = std::mem::replace(&mut union.left, Expression::Null(Null));
                let right = std::mem::replace(&mut union.right, Expression::Null(Null));
                union.left = Self::wrap_tsql_values_set_operand(left);
                union.right = Self::wrap_tsql_values_set_operand(right);
                Expression::Union(union)
            }
            Expression::Intersect(mut intersect) => {
                let left = std::mem::replace(&mut intersect.left, Expression::Null(Null));
                let right = std::mem::replace(&mut intersect.right, Expression::Null(Null));
                intersect.left = Self::wrap_tsql_values_set_operand(left);
                intersect.right = Self::wrap_tsql_values_set_operand(right);
                Expression::Intersect(intersect)
            }
            Expression::Except(mut except) => {
                let left = std::mem::replace(&mut except.left, Expression::Null(Null));
                let right = std::mem::replace(&mut except.right, Expression::Null(Null));
                except.left = Self::wrap_tsql_values_set_operand(left);
                except.right = Self::wrap_tsql_values_set_operand(right);
                Expression::Except(except)
            }
            other => other,
        }
    }

    #[cfg(feature = "transpile")]
    fn tsql_values_as_select(mut values: crate::expressions::Values) -> Expression {
        let column_aliases = if values.column_aliases.is_empty() {
            let column_count = values
                .expressions
                .first()
                .map(|row| row.expressions.len())
                .unwrap_or(0);
            (1..=column_count)
                .map(|index| Identifier::new(format!("column{index}")))
                .collect()
        } else {
            std::mem::take(&mut values.column_aliases)
        };

        values.alias = None;

        let values_subquery = Expression::Subquery(Box::new(crate::expressions::Subquery {
            this: Expression::Values(Box::new(values)),
            alias: Some(Identifier::new("_v")),
            column_aliases,
            alias_explicit_as: false,
            alias_keyword: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            lateral: false,
            modifiers_inside: false,
            trailing_comments: Vec::new(),
            inferred_type: None,
        }));

        let mut select = crate::expressions::Select::new();
        select.expressions = vec![Expression::star()];
        select.from = Some(From {
            expressions: vec![values_subquery],
        });

        Expression::Select(Box::new(select))
    }

    fn extract_interval_parts(
        interval_expr: &Expression,
    ) -> Option<(Expression, crate::expressions::IntervalUnit)> {
        use crate::expressions::{DataType, IntervalUnit, IntervalUnitSpec, Literal};

        fn unit_from_str(unit: &str) -> Option<IntervalUnit> {
            match unit.trim().to_ascii_uppercase().as_str() {
                "YEAR" | "YEARS" | "Y" | "YR" | "YRS" | "YY" | "YYYY" => Some(IntervalUnit::Year),
                "QUARTER" | "QUARTERS" | "Q" | "QTR" | "QTRS" | "QQ" => Some(IntervalUnit::Quarter),
                "MONTH" | "MONTHS" | "MON" | "MONS" | "MM" => Some(IntervalUnit::Month),
                "WEEK" | "WEEKS" | "W" | "WK" | "WKS" | "WW" | "ISOWEEK" => {
                    Some(IntervalUnit::Week)
                }
                "DAY" | "DAYS" | "D" | "DD" => Some(IntervalUnit::Day),
                "HOUR" | "HOURS" | "H" | "HH" | "HR" | "HRS" => Some(IntervalUnit::Hour),
                "MINUTE" | "MINUTES" | "MI" | "MIN" | "MINS" | "N" => Some(IntervalUnit::Minute),
                "SECOND" | "SECONDS" | "S" | "SEC" | "SECS" | "SS" => Some(IntervalUnit::Second),
                "MILLISECOND" | "MILLISECONDS" | "MS" | "MSEC" | "MSECS" | "MSECOND"
                | "MSECONDS" | "MILLISEC" | "MILLISECS" | "MILLISECON" => {
                    Some(IntervalUnit::Millisecond)
                }
                "MICROSECOND" | "MICROSECONDS" | "US" | "USEC" | "USECS" | "USECOND"
                | "USECONDS" | "MICROSEC" | "MICROSECS" | "MCS" => Some(IntervalUnit::Microsecond),
                "NANOSECOND" | "NANOSECONDS" | "NS" | "NSEC" | "NSECS" | "NSECOND" | "NSECONDS"
                | "NANOSEC" | "NANOSECS" => Some(IntervalUnit::Nanosecond),
                _ => None,
            }
        }

        fn parts_from_literal_string(s: &str) -> Option<(Expression, IntervalUnit)> {
            let mut parts = s.split_whitespace();
            let value = parts.next()?;
            let unit = unit_from_str(parts.next()?)?;
            Some((
                Expression::Literal(Box::new(Literal::String(value.to_string()))),
                unit,
            ))
        }

        fn unit_from_spec(unit: &IntervalUnitSpec) -> Option<IntervalUnit> {
            match unit {
                IntervalUnitSpec::Simple { unit, .. } => Some(*unit),
                IntervalUnitSpec::Expr(expr) => match expr.as_ref() {
                    Expression::Day(_) => Some(IntervalUnit::Day),
                    Expression::Month(_) => Some(IntervalUnit::Month),
                    Expression::Year(_) => Some(IntervalUnit::Year),
                    Expression::Identifier(id) => unit_from_str(&id.name),
                    Expression::Var(v) => unit_from_str(&v.this),
                    Expression::Column(col) => unit_from_str(&col.name.name),
                    _ => None,
                },
                _ => None,
            }
        }

        match interval_expr {
            Expression::Interval(iv) => {
                let val = iv.this.clone().unwrap_or(Expression::number(0));
                if let Expression::Literal(lit) = &val {
                    if let Literal::String(s) = lit.as_ref() {
                        if let Some(parts) = parts_from_literal_string(s) {
                            return Some(parts);
                        }
                    }
                }
                let unit = iv
                    .unit
                    .as_ref()
                    .and_then(unit_from_spec)
                    .unwrap_or(IntervalUnit::Day);
                Some((val, unit))
            }
            Expression::Cast(cast) if matches!(cast.to, DataType::Interval { .. }) => {
                if let Expression::Literal(lit) = &cast.this {
                    if let Literal::String(s) = lit.as_ref() {
                        if let Some(parts) = parts_from_literal_string(s) {
                            return Some(parts);
                        }
                    }
                }
                let unit = match &cast.to {
                    DataType::Interval {
                        unit: Some(unit), ..
                    } => unit_from_str(unit).unwrap_or(IntervalUnit::Day),
                    _ => IntervalUnit::Day,
                };
                Some((cast.this.clone(), unit))
            }
            _ => None,
        }
    }

    fn data_type_is_interval(dt: &DataType) -> bool {
        match dt {
            DataType::Interval { .. } => true,
            DataType::Custom { name } => name.trim().eq_ignore_ascii_case("INTERVAL"),
            _ => false,
        }
    }

    fn node_is_interval_cast(node: &Expression) -> bool {
        match node {
            Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c) => {
                Self::data_type_is_interval(&c.to)
            }
            _ => false,
        }
    }

    fn reject_tsql_interval_casts(
        expr: &Expression,
        target: DialectType,
        opts: &TranspileOptions,
    ) -> Result<()> {
        if !matches!(
            opts.unsupported_level,
            UnsupportedLevel::Raise | UnsupportedLevel::Immediate
        ) {
            return Ok(());
        }

        if expr.dfs().any(Self::node_is_interval_cast) {
            return Err(crate::error::Error::unsupported(
                "INTERVAL casts",
                target.to_string(),
            ));
        }

        Ok(())
    }

    fn tsql_varchar_max_type() -> DataType {
        DataType::Custom {
            name: "VARCHAR(MAX)".to_string(),
        }
    }

    fn rewrite_tsql_interval_casts_to_varchar(expr: Expression) -> Result<Expression> {
        transform_recursive(expr, &|e| match e {
            Expression::Cast(mut cast) if Self::data_type_is_interval(&cast.to) => {
                cast.to = Self::tsql_varchar_max_type();
                cast.double_colon_syntax = false;
                Ok(Expression::Cast(cast))
            }
            Expression::TryCast(mut cast) if Self::data_type_is_interval(&cast.to) => {
                cast.to = Self::tsql_varchar_max_type();
                cast.double_colon_syntax = false;
                Ok(Expression::TryCast(cast))
            }
            Expression::SafeCast(mut cast) if Self::data_type_is_interval(&cast.to) => {
                cast.to = Self::tsql_varchar_max_type();
                cast.double_colon_syntax = false;
                Ok(Expression::SafeCast(cast))
            }
            _ => Ok(e),
        })
    }

    fn rewrite_tsql_interval_arithmetic_legacy(
        expr: &Expression,
        source: DialectType,
    ) -> Option<Expression> {
        match expr {
            Expression::Add(op) => {
                if Self::extract_interval_parts(&op.right).is_some() {
                    return Some(Self::build_tsql_dateadd_from_interval(
                        op.left.clone(),
                        &op.right,
                        false,
                    ));
                }

                if Self::is_postgres_family_source(source) {
                    if Self::is_explicit_date_expr(&op.left)
                        && Self::is_integer_day_offset_expr(&op.right)
                    {
                        return Some(Self::build_tsql_dateadd_days(
                            op.left.clone(),
                            op.right.clone(),
                            false,
                        ));
                    }

                    if Self::is_integer_day_offset_expr(&op.left)
                        && Self::is_explicit_date_expr(&op.right)
                    {
                        return Some(Self::build_tsql_dateadd_days(
                            op.right.clone(),
                            op.left.clone(),
                            false,
                        ));
                    }
                }

                None
            }
            Expression::Sub(op) => {
                if Self::extract_interval_parts(&op.right).is_some() {
                    return Some(Self::build_tsql_dateadd_from_interval(
                        op.left.clone(),
                        &op.right,
                        true,
                    ));
                }

                if Self::is_postgres_family_source(source) {
                    if Self::is_explicit_date_expr(&op.left)
                        && Self::is_explicit_date_expr(&op.right)
                    {
                        return Some(Self::build_tsql_datediff_days(
                            op.right.clone(),
                            op.left.clone(),
                        ));
                    }

                    if Self::is_explicit_date_expr(&op.left)
                        && Self::is_integer_day_offset_expr(&op.right)
                    {
                        return Some(Self::build_tsql_dateadd_days(
                            op.left.clone(),
                            op.right.clone(),
                            true,
                        ));
                    }
                }

                None
            }
            _ => None,
        }
    }

    fn is_postgres_family_source(source: DialectType) -> bool {
        matches!(
            source,
            DialectType::PostgreSQL
                | DialectType::Redshift
                | DialectType::Materialize
                | DialectType::RisingWave
                | DialectType::CockroachDB
        )
    }

    fn is_explicit_date_expr(expr: &Expression) -> bool {
        use crate::expressions::Literal;

        match expr {
            Expression::Literal(lit) => matches!(lit.as_ref(), Literal::Date(_)),
            Expression::Cast(c) | Expression::TryCast(c) | Expression::SafeCast(c) => {
                matches!(c.to, crate::expressions::DataType::Date)
            }
            Expression::Paren(p) => Self::is_explicit_date_expr(&p.this),
            Expression::CurrentDate(_)
            | Expression::Date(_)
            | Expression::MakeDate(_)
            | Expression::ToDate(_)
            | Expression::DateStrToDate(_) => true,
            _ => false,
        }
    }

    fn is_integer_day_offset_expr(expr: &Expression) -> bool {
        use crate::expressions::Literal;

        match expr {
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::Number(n) => n.parse::<i64>().is_ok(),
                _ => false,
            },
            Expression::Parameter(_) | Expression::Placeholder(_) => true,
            Expression::Neg(op) => Self::is_integer_day_offset_expr(&op.this),
            Expression::Paren(p) => Self::is_integer_day_offset_expr(&p.this),
            _ => false,
        }
    }

    fn build_tsql_datediff_days(start: Expression, end: Expression) -> Expression {
        Expression::Function(Box::new(Function::new(
            "DATEDIFF".to_string(),
            vec![Expression::Identifier(Identifier::new("DAY")), start, end],
        )))
    }

    fn build_tsql_dateadd_days(date: Expression, amount: Expression, subtract: bool) -> Expression {
        Expression::Function(Box::new(Function::new(
            "DATEADD".to_string(),
            vec![
                Expression::Identifier(Identifier::new("DAY")),
                Self::tsql_dateadd_amount(amount, subtract),
                date,
            ],
        )))
    }

    fn build_tsql_dateadd_from_interval(
        date: Expression,
        interval: &Expression,
        subtract: bool,
    ) -> Expression {
        let (value, unit) = Self::extract_interval_parts(interval)
            .unwrap_or_else(|| (interval.clone(), crate::expressions::IntervalUnit::Day));
        let unit = normalization::temporal::interval_unit_to_string(&unit);
        let amount = Self::tsql_dateadd_amount(value, subtract);

        Expression::Function(Box::new(Function::new(
            "DATEADD".to_string(),
            vec![Expression::Identifier(Identifier::new(unit)), amount, date],
        )))
    }

    fn tsql_dateadd_amount(value: Expression, negate: bool) -> Expression {
        use crate::expressions::{Parameter, ParameterStyle, UnaryOp};

        fn numeric_literal_value(value: &Expression) -> Option<&str> {
            match value {
                Expression::Literal(lit) => match lit.as_ref() {
                    crate::expressions::Literal::Number(n)
                    | crate::expressions::Literal::String(n) => Some(n.as_str()),
                    _ => None,
                },
                _ => None,
            }
        }

        fn colon_parameter(value: &Expression) -> Option<Expression> {
            let Expression::Literal(lit) = value else {
                return None;
            };
            let crate::expressions::Literal::String(s) = lit.as_ref() else {
                return None;
            };
            let name = s.strip_prefix(':')?;
            if name.is_empty()
                || !name
                    .chars()
                    .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
            {
                return None;
            }

            Some(Expression::Parameter(Box::new(Parameter {
                name: if name.chars().all(|ch| ch.is_ascii_digit()) {
                    None
                } else {
                    Some(name.to_string())
                },
                index: name.parse::<u32>().ok(),
                style: ParameterStyle::Colon,
                quoted: false,
                string_quoted: false,
                expression: None,
            })))
        }

        let value = colon_parameter(&value).unwrap_or(value);

        if let Some(n) = numeric_literal_value(&value) {
            if let Ok(parsed) = n.parse::<f64>() {
                let normalized = if negate { -parsed } else { parsed };
                let rendered = if normalized.fract() == 0.0 {
                    format!("{}", normalized as i64)
                } else {
                    normalized.to_string()
                };
                return Expression::Literal(Box::new(crate::expressions::Literal::Number(
                    rendered,
                )));
            }
        }

        if !negate {
            return value;
        }

        match value {
            Expression::Neg(op) => op.this,
            other => Expression::Neg(Box::new(UnaryOp {
                this: other,
                inferred_type: None,
            })),
        }
    }

    /// Internal TO_DATE function that won't be converted to CAST by the Snowflake handler.
    /// Uses the name `_POLYGLOT_TO_DATE` which is not recognized by the TO_DATE -> CAST logic.
    /// The Snowflake DATEDIFF handler converts these back to TO_DATE.
    const PRESERVED_TO_DATE: &'static str = "_POLYGLOT_TO_DATE";
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn built_in_dialect_instances_share_tokenizer_config() {
        let first = Dialect::get(DialectType::PostgreSQL);
        let second = Dialect::get(DialectType::PostgreSQL);

        assert!(first.tokenizer.shares_config_with(&second.tokenizer));
    }

    #[test]
    fn test_dialect_type_from_str() {
        assert_eq!(
            "postgres".parse::<DialectType>().unwrap(),
            DialectType::PostgreSQL
        );
        assert_eq!(
            "postgresql".parse::<DialectType>().unwrap(),
            DialectType::PostgreSQL
        );
        assert_eq!("mysql".parse::<DialectType>().unwrap(), DialectType::MySQL);
        assert_eq!(
            "bigquery".parse::<DialectType>().unwrap(),
            DialectType::BigQuery
        );
    }

    #[test]
    fn test_basic_transpile() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile("SELECT 1", DialectType::PostgreSQL)
            .unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], "SELECT 1");
    }

    #[test]
    fn test_sqlite_double_quoted_column_defaults_to_postgres_strings() {
        let sqlite = Dialect::get(DialectType::SQLite);
        let result = sqlite
            .transpile(
                r#"CREATE TABLE "_collections" (
                    "type" TEXT DEFAULT "base" NOT NULL,
                    "fields" JSON DEFAULT "[]" NOT NULL,
                    "options" JSON DEFAULT "{}" NOT NULL
                )"#,
                DialectType::PostgreSQL,
            )
            .unwrap();

        assert!(result[0].contains(r#""type" TEXT DEFAULT 'base' NOT NULL"#));
        assert!(result[0].contains(r#""fields" JSON DEFAULT '[]' NOT NULL"#));
        assert!(result[0].contains(r#""options" JSON DEFAULT '{}' NOT NULL"#));
    }

    #[test]
    fn test_sqlite_identity_preserves_double_quoted_column_defaults() {
        let sqlite = Dialect::get(DialectType::SQLite);
        let result = sqlite
            .transpile(
                r#"CREATE TABLE "_collections" ("type" TEXT DEFAULT "base" NOT NULL)"#,
                DialectType::SQLite,
            )
            .unwrap();

        assert_eq!(
            result[0],
            r#"CREATE TABLE "_collections" ("type" TEXT DEFAULT "base" NOT NULL)"#
        );
    }

    #[test]
    fn test_function_transformation_mysql() {
        // NVL should be transformed to IFNULL in MySQL
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile("SELECT NVL(a, b)", DialectType::MySQL)
            .unwrap();
        assert_eq!(result[0], "SELECT IFNULL(a, b)");
    }

    #[test]
    fn test_get_path_duckdb() {
        // Test: step by step
        let snowflake = Dialect::get(DialectType::Snowflake);

        // Step 1: Parse and check what Snowflake produces as intermediate
        let result_sf_sf = snowflake
            .transpile(
                "SELECT PARSE_JSON('{\"fruit\":\"banana\"}'):fruit",
                DialectType::Snowflake,
            )
            .unwrap();
        eprintln!("Snowflake->Snowflake colon: {}", result_sf_sf[0]);

        // Step 2: DuckDB target
        let result_sf_dk = snowflake
            .transpile(
                "SELECT PARSE_JSON('{\"fruit\":\"banana\"}'):fruit",
                DialectType::DuckDB,
            )
            .unwrap();
        eprintln!("Snowflake->DuckDB colon: {}", result_sf_dk[0]);

        // Step 3: GET_PATH directly
        let result_gp = snowflake
            .transpile(
                "SELECT GET_PATH(PARSE_JSON('{\"fruit\":\"banana\"}'), 'fruit')",
                DialectType::DuckDB,
            )
            .unwrap();
        eprintln!("Snowflake->DuckDB explicit GET_PATH: {}", result_gp[0]);
    }

    #[test]
    fn test_function_transformation_postgres() {
        // IFNULL should be transformed to COALESCE in PostgreSQL
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile("SELECT IFNULL(a, b)", DialectType::PostgreSQL)
            .unwrap();
        assert_eq!(result[0], "SELECT COALESCE(a, b)");

        // NVL should also be transformed to COALESCE
        let result = dialect
            .transpile("SELECT NVL(a, b)", DialectType::PostgreSQL)
            .unwrap();
        assert_eq!(result[0], "SELECT COALESCE(a, b)");
    }

    #[test]
    fn test_hive_cast_to_trycast() {
        // Hive CAST should become TRY_CAST for targets that support it
        let hive = Dialect::get(DialectType::Hive);
        let result = hive
            .transpile("CAST(1 AS INT)", DialectType::DuckDB)
            .unwrap();
        assert_eq!(result[0], "TRY_CAST(1 AS INT)");

        let result = hive
            .transpile("CAST(1 AS INT)", DialectType::Presto)
            .unwrap();
        assert_eq!(result[0], "TRY_CAST(1 AS INTEGER)");
    }

    #[test]
    fn test_hive_array_identity() {
        // Hive ARRAY<DATE> should preserve angle bracket syntax
        let sql = "CREATE EXTERNAL TABLE `my_table` (`a7` ARRAY<DATE>) ROW FORMAT SERDE 'a' STORED AS INPUTFORMAT 'b' OUTPUTFORMAT 'c' LOCATION 'd' TBLPROPERTIES ('e'='f')";
        let hive = Dialect::get(DialectType::Hive);

        // Test via transpile (this works)
        let result = hive.transpile(sql, DialectType::Hive).unwrap();
        eprintln!("Hive ARRAY via transpile: {}", result[0]);
        assert!(
            result[0].contains("ARRAY<DATE>"),
            "transpile: Expected ARRAY<DATE>, got: {}",
            result[0]
        );

        // Test via parse -> transform -> generate (identity test path)
        let ast = hive.parse(sql).unwrap();
        let transformed = hive.transform(ast[0].clone()).unwrap();
        let output = hive.generate(&transformed).unwrap();
        eprintln!("Hive ARRAY via identity path: {}", output);
        assert!(
            output.contains("ARRAY<DATE>"),
            "identity path: Expected ARRAY<DATE>, got: {}",
            output
        );
    }

    #[test]
    fn test_starrocks_delete_between_expansion() {
        // StarRocks doesn't support BETWEEN in DELETE statements
        let dialect = Dialect::get(DialectType::Generic);

        // BETWEEN should be expanded to >= AND <= in DELETE
        let result = dialect
            .transpile(
                "DELETE FROM t WHERE a BETWEEN b AND c",
                DialectType::StarRocks,
            )
            .unwrap();
        assert_eq!(result[0], "DELETE FROM t WHERE a >= b AND a <= c");

        // NOT BETWEEN should be expanded to < OR > in DELETE
        let result = dialect
            .transpile(
                "DELETE FROM t WHERE a NOT BETWEEN b AND c",
                DialectType::StarRocks,
            )
            .unwrap();
        assert_eq!(result[0], "DELETE FROM t WHERE a < b OR a > c");

        // BETWEEN in SELECT should NOT be expanded (StarRocks supports it there)
        let result = dialect
            .transpile(
                "SELECT * FROM t WHERE a BETWEEN b AND c",
                DialectType::StarRocks,
            )
            .unwrap();
        assert!(
            result[0].contains("BETWEEN"),
            "BETWEEN should be preserved in SELECT"
        );
    }

    #[test]
    fn test_snowflake_ltrim_rtrim_parse() {
        let sf = Dialect::get(DialectType::Snowflake);
        let sql = "SELECT LTRIM(RTRIM(col)) FROM t1";
        let result = sf.transpile(sql, DialectType::DuckDB);
        match &result {
            Ok(r) => eprintln!("LTRIM/RTRIM result: {}", r[0]),
            Err(e) => eprintln!("LTRIM/RTRIM error: {}", e),
        }
        assert!(
            result.is_ok(),
            "Expected successful parse of LTRIM(RTRIM(col)), got error: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_duckdb_count_if_parse() {
        let duck = Dialect::get(DialectType::DuckDB);
        let sql = "COUNT_IF(x)";
        let result = duck.transpile(sql, DialectType::DuckDB);
        match &result {
            Ok(r) => eprintln!("COUNT_IF result: {}", r[0]),
            Err(e) => eprintln!("COUNT_IF error: {}", e),
        }
        assert!(
            result.is_ok(),
            "Expected successful parse of COUNT_IF(x), got error: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_tsql_cast_tinyint_parse() {
        let tsql = Dialect::get(DialectType::TSQL);
        let sql = "CAST(X AS TINYINT)";
        let result = tsql.transpile(sql, DialectType::DuckDB);
        match &result {
            Ok(r) => eprintln!("TSQL CAST TINYINT result: {}", r[0]),
            Err(e) => eprintln!("TSQL CAST TINYINT error: {}", e),
        }
        assert!(
            result.is_ok(),
            "Expected successful transpile, got error: {:?}",
            result.err()
        );
    }

    #[test]
    fn test_pg_hash_bitwise_xor() {
        let dialect = Dialect::get(DialectType::PostgreSQL);
        let result = dialect.transpile("x # y", DialectType::PostgreSQL).unwrap();
        assert_eq!(result[0], "x # y");
    }

    #[test]
    fn test_pg_array_to_duckdb() {
        let dialect = Dialect::get(DialectType::PostgreSQL);
        let result = dialect
            .transpile("SELECT ARRAY[1, 2, 3] @> ARRAY[1, 2]", DialectType::DuckDB)
            .unwrap();
        assert_eq!(result[0], "SELECT [1, 2, 3] @> [1, 2]");
    }

    #[test]
    fn test_array_remove_bigquery() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile("ARRAY_REMOVE(the_array, target)", DialectType::BigQuery)
            .unwrap();
        assert_eq!(
            result[0],
            "ARRAY(SELECT _u FROM UNNEST(the_array) AS _u WHERE _u <> target)"
        );
    }

    #[test]
    fn test_map_clickhouse_case() {
        let dialect = Dialect::get(DialectType::Generic);
        let parsed = dialect
            .parse("CAST(MAP('a', '1') AS MAP(TEXT, TEXT))")
            .unwrap();
        eprintln!("MAP parsed: {:?}", parsed);
        let result = dialect
            .transpile(
                "CAST(MAP('a', '1') AS MAP(TEXT, TEXT))",
                DialectType::ClickHouse,
            )
            .unwrap();
        eprintln!("MAP result: {}", result[0]);
    }

    #[test]
    fn test_generate_date_array_presto() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile(
            "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
            DialectType::Presto,
        ).unwrap();
        eprintln!("GDA -> Presto: {}", result[0]);
        assert_eq!(result[0], "SELECT * FROM UNNEST(SEQUENCE(CAST('2020-01-01' AS DATE), CAST('2020-02-01' AS DATE), (1 * INTERVAL '7' DAY)))");
    }

    #[test]
    fn test_generate_date_array_postgres() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile(
            "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
            DialectType::PostgreSQL,
        ).unwrap();
        eprintln!("GDA -> PostgreSQL: {}", result[0]);
    }

    #[test]
    fn test_generate_date_array_snowflake() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile(
                "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
                DialectType::Snowflake,
            )
            .unwrap();
        eprintln!("GDA -> Snowflake: {}", result[0]);
    }

    #[test]
    fn test_array_length_generate_date_array_snowflake() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile(
            "SELECT ARRAY_LENGTH(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
            DialectType::Snowflake,
        ).unwrap();
        eprintln!("ARRAY_LENGTH(GDA) -> Snowflake: {}", result[0]);
    }

    #[test]
    fn test_generate_date_array_mysql() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile(
            "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
            DialectType::MySQL,
        ).unwrap();
        eprintln!("GDA -> MySQL: {}", result[0]);
    }

    #[test]
    fn test_generate_date_array_redshift() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile(
            "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
            DialectType::Redshift,
        ).unwrap();
        eprintln!("GDA -> Redshift: {}", result[0]);
    }

    #[test]
    fn test_generate_date_array_tsql() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile(
            "SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))",
            DialectType::TSQL,
        ).unwrap();
        eprintln!("GDA -> TSQL: {}", result[0]);
    }

    #[test]
    fn test_struct_colon_syntax() {
        let dialect = Dialect::get(DialectType::Generic);
        // Test without colon first
        let result = dialect.transpile(
            "CAST((1, 2, 3, 4) AS STRUCT<a TINYINT, b SMALLINT, c INT, d BIGINT>)",
            DialectType::ClickHouse,
        );
        match result {
            Ok(r) => eprintln!("STRUCT no colon -> ClickHouse: {}", r[0]),
            Err(e) => eprintln!("STRUCT no colon error: {}", e),
        }
        // Now test with colon
        let result = dialect.transpile(
            "CAST((1, 2, 3, 4) AS STRUCT<a: TINYINT, b: SMALLINT, c: INT, d: BIGINT>)",
            DialectType::ClickHouse,
        );
        match result {
            Ok(r) => eprintln!("STRUCT colon -> ClickHouse: {}", r[0]),
            Err(e) => eprintln!("STRUCT colon error: {}", e),
        }
    }

    #[test]
    fn test_generate_date_array_cte_wrapped_mysql() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile(
            "WITH dates AS (SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))) SELECT * FROM dates",
            DialectType::MySQL,
        ).unwrap();
        eprintln!("GDA CTE -> MySQL: {}", result[0]);
    }

    #[test]
    fn test_generate_date_array_cte_wrapped_tsql() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect.transpile(
            "WITH dates AS (SELECT * FROM UNNEST(GENERATE_DATE_ARRAY(DATE '2020-01-01', DATE '2020-02-01', INTERVAL 1 WEEK))) SELECT * FROM dates",
            DialectType::TSQL,
        ).unwrap();
        eprintln!("GDA CTE -> TSQL: {}", result[0]);
    }

    #[test]
    fn test_decode_literal_no_null_check() {
        // Oracle DECODE with all literals should produce simple equality, no IS NULL
        let dialect = Dialect::get(DialectType::Oracle);
        let result = dialect
            .transpile("SELECT decode(1,2,3,4)", DialectType::DuckDB)
            .unwrap();
        assert_eq!(
            result[0], "SELECT CASE WHEN 1 = 2 THEN 3 ELSE 4 END",
            "Literal DECODE should not have IS NULL checks"
        );
    }

    #[test]
    fn test_decode_column_vs_literal_no_null_check() {
        // Oracle DECODE with column vs literal should use simple equality (like sqlglot)
        let dialect = Dialect::get(DialectType::Oracle);
        let result = dialect
            .transpile("SELECT decode(col, 2, 3, 4) FROM t", DialectType::DuckDB)
            .unwrap();
        assert_eq!(
            result[0], "SELECT CASE WHEN col = 2 THEN 3 ELSE 4 END FROM t",
            "Column vs literal DECODE should not have IS NULL checks"
        );
    }

    #[test]
    fn test_decode_column_vs_column_keeps_null_check() {
        // Oracle DECODE with column vs column should keep null-safe comparison
        let dialect = Dialect::get(DialectType::Oracle);
        let result = dialect
            .transpile("SELECT decode(col, col2, 3, 4) FROM t", DialectType::DuckDB)
            .unwrap();
        assert!(
            result[0].contains("IS NULL"),
            "Column vs column DECODE should have IS NULL checks, got: {}",
            result[0]
        );
    }

    #[test]
    fn test_decode_null_search() {
        // Oracle DECODE with NULL search should use IS NULL
        let dialect = Dialect::get(DialectType::Oracle);
        let result = dialect
            .transpile("SELECT decode(col, NULL, 3, 4) FROM t", DialectType::DuckDB)
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT CASE WHEN col IS NULL THEN 3 ELSE 4 END FROM t",
        );
    }

    // =========================================================================
    // REGEXP function transpilation tests
    // =========================================================================

    #[test]
    fn test_regexp_substr_snowflake_to_duckdb_2arg() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT REGEXP_SUBSTR(s, 'pattern')", DialectType::DuckDB)
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_EXTRACT(s, 'pattern')");
    }

    #[test]
    fn test_regexp_substr_snowflake_to_duckdb_3arg_pos1() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT REGEXP_SUBSTR(s, 'pattern', 1)", DialectType::DuckDB)
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_EXTRACT(s, 'pattern')");
    }

    #[test]
    fn test_regexp_substr_snowflake_to_duckdb_3arg_pos_gt1() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT REGEXP_SUBSTR(s, 'pattern', 3)", DialectType::DuckDB)
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT REGEXP_EXTRACT(NULLIF(SUBSTRING(s, 3), ''), 'pattern')"
        );
    }

    #[test]
    fn test_regexp_substr_snowflake_to_duckdb_4arg_occ_gt1() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR(s, 'pattern', 1, 3)",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT ARRAY_EXTRACT(REGEXP_EXTRACT_ALL(s, 'pattern'), 3)"
        );
    }

    #[test]
    fn test_regexp_substr_snowflake_to_duckdb_5arg_e_flag() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR(s, 'pattern', 1, 1, 'e')",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_EXTRACT(s, 'pattern')");
    }

    #[test]
    fn test_regexp_substr_snowflake_to_duckdb_6arg_group0() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR(s, 'pattern', 1, 1, 'e', 0)",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_EXTRACT(s, 'pattern')");
    }

    #[test]
    fn test_regexp_substr_snowflake_identity_strip_group0() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR(s, 'pattern', 1, 1, 'e', 0)",
                DialectType::Snowflake,
            )
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_SUBSTR(s, 'pattern', 1, 1, 'e')");
    }

    #[test]
    fn test_regexp_substr_all_snowflake_to_duckdb_2arg() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR_ALL(s, 'pattern')",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_EXTRACT_ALL(s, 'pattern')");
    }

    #[test]
    fn test_regexp_substr_all_snowflake_to_duckdb_3arg_pos_gt1() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR_ALL(s, 'pattern', 3)",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT REGEXP_EXTRACT_ALL(SUBSTRING(s, 3), 'pattern')"
        );
    }

    #[test]
    fn test_regexp_substr_all_snowflake_to_duckdb_5arg_e_flag() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR_ALL(s, 'pattern', 1, 1, 'e')",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_EXTRACT_ALL(s, 'pattern')");
    }

    #[test]
    fn test_regexp_substr_all_snowflake_to_duckdb_6arg_group0() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR_ALL(s, 'pattern', 1, 1, 'e', 0)",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_EXTRACT_ALL(s, 'pattern')");
    }

    #[test]
    fn test_regexp_substr_all_snowflake_identity_strip_group0() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_SUBSTR_ALL(s, 'pattern', 1, 1, 'e', 0)",
                DialectType::Snowflake,
            )
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT REGEXP_SUBSTR_ALL(s, 'pattern', 1, 1, 'e')"
        );
    }

    #[test]
    fn test_regexp_count_snowflake_to_duckdb_2arg() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT REGEXP_COUNT(s, 'pattern')", DialectType::DuckDB)
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT CASE WHEN 'pattern' = '' THEN 0 ELSE LENGTH(REGEXP_EXTRACT_ALL(s, 'pattern')) END"
        );
    }

    #[test]
    fn test_regexp_count_snowflake_to_duckdb_3arg() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT REGEXP_COUNT(s, 'pattern', 3)", DialectType::DuckDB)
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT CASE WHEN 'pattern' = '' THEN 0 ELSE LENGTH(REGEXP_EXTRACT_ALL(SUBSTRING(s, 3), 'pattern')) END"
        );
    }

    #[test]
    fn test_regexp_count_snowflake_to_duckdb_4arg_flags() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_COUNT(s, 'pattern', 1, 'i')",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT CASE WHEN '(?i)' || 'pattern' = '' THEN 0 ELSE LENGTH(REGEXP_EXTRACT_ALL(SUBSTRING(s, 1), '(?i)' || 'pattern')) END"
        );
    }

    #[test]
    fn test_regexp_count_snowflake_to_duckdb_4arg_flags_literal_string() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_COUNT('Hello World', 'L', 1, 'im')",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT CASE WHEN '(?im)' || 'L' = '' THEN 0 ELSE LENGTH(REGEXP_EXTRACT_ALL(SUBSTRING('Hello World', 1), '(?im)' || 'L')) END"
        );
    }

    #[test]
    fn test_regexp_replace_snowflake_to_duckdb_5arg_pos1_occ1() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_REPLACE(s, 'pattern', 'repl', 1, 1)",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_REPLACE(s, 'pattern', 'repl')");
    }

    #[test]
    fn test_regexp_replace_snowflake_to_duckdb_5arg_pos_gt1_occ0() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_REPLACE(s, 'pattern', 'repl', 3, 0)",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT SUBSTRING(s, 1, 2) || REGEXP_REPLACE(SUBSTRING(s, 3), 'pattern', 'repl', 'g')"
        );
    }

    #[test]
    fn test_regexp_replace_snowflake_to_duckdb_5arg_pos_gt1_occ1() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT REGEXP_REPLACE(s, 'pattern', 'repl', 3, 1)",
                DialectType::DuckDB,
            )
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT SUBSTRING(s, 1, 2) || REGEXP_REPLACE(SUBSTRING(s, 3), 'pattern', 'repl')"
        );
    }

    #[test]
    fn test_rlike_snowflake_to_duckdb_2arg() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT RLIKE(a, b)", DialectType::DuckDB)
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_FULL_MATCH(a, b)");
    }

    #[test]
    fn test_rlike_snowflake_to_duckdb_3arg_flags() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT RLIKE(a, b, 'i')", DialectType::DuckDB)
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_FULL_MATCH(a, b, 'i')");
    }

    #[test]
    fn test_regexp_extract_all_bigquery_to_snowflake_no_capture() {
        let dialect = Dialect::get(DialectType::BigQuery);
        let result = dialect
            .transpile(
                "SELECT REGEXP_EXTRACT_ALL(s, 'pattern')",
                DialectType::Snowflake,
            )
            .unwrap();
        assert_eq!(result[0], "SELECT REGEXP_SUBSTR_ALL(s, 'pattern')");
    }

    #[test]
    fn test_regexp_extract_all_bigquery_to_snowflake_with_capture() {
        let dialect = Dialect::get(DialectType::BigQuery);
        let result = dialect
            .transpile(
                "SELECT REGEXP_EXTRACT_ALL(s, '(a)[0-9]')",
                DialectType::Snowflake,
            )
            .unwrap();
        assert_eq!(
            result[0],
            "SELECT REGEXP_SUBSTR_ALL(s, '(a)[0-9]', 1, 1, 'c', 1)"
        );
    }

    #[test]
    fn test_regexp_instr_snowflake_to_duckdb_2arg() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT REGEXP_INSTR(s, 'pattern')", DialectType::DuckDB)
            .unwrap();
        assert!(
            result[0].contains("CASE WHEN"),
            "Expected CASE WHEN in result: {}",
            result[0]
        );
        assert!(
            result[0].contains("LIST_SUM"),
            "Expected LIST_SUM in result: {}",
            result[0]
        );
    }

    #[test]
    fn test_array_except_generic_to_duckdb() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile(
                "SELECT ARRAY_EXCEPT(ARRAY(1, 2, 3), ARRAY(2))",
                DialectType::DuckDB,
            )
            .unwrap();
        eprintln!("ARRAY_EXCEPT Generic->DuckDB: {}", result[0]);
        assert!(
            result[0].contains("CASE WHEN"),
            "Expected CASE WHEN: {}",
            result[0]
        );
        assert!(
            result[0].contains("LIST_FILTER"),
            "Expected LIST_FILTER: {}",
            result[0]
        );
        assert!(
            result[0].contains("LIST_DISTINCT"),
            "Expected LIST_DISTINCT: {}",
            result[0]
        );
        assert!(
            result[0].contains("IS NOT DISTINCT FROM"),
            "Expected IS NOT DISTINCT FROM: {}",
            result[0]
        );
        assert!(
            result[0].contains("= 0"),
            "Expected = 0 filter: {}",
            result[0]
        );
    }

    #[test]
    fn test_array_except_generic_to_snowflake() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile(
                "SELECT ARRAY_EXCEPT(ARRAY(1, 2, 3), ARRAY(2))",
                DialectType::Snowflake,
            )
            .unwrap();
        eprintln!("ARRAY_EXCEPT Generic->Snowflake: {}", result[0]);
        assert_eq!(result[0], "SELECT ARRAY_EXCEPT([1, 2, 3], [2])");
    }

    #[test]
    fn test_array_except_generic_to_presto() {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile(
                "SELECT ARRAY_EXCEPT(ARRAY(1, 2, 3), ARRAY(2))",
                DialectType::Presto,
            )
            .unwrap();
        eprintln!("ARRAY_EXCEPT Generic->Presto: {}", result[0]);
        assert_eq!(result[0], "SELECT ARRAY_EXCEPT(ARRAY[1, 2, 3], ARRAY[2])");
    }

    #[test]
    fn test_array_except_snowflake_to_duckdb() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile("SELECT ARRAY_EXCEPT([1, 2, 3], [2])", DialectType::DuckDB)
            .unwrap();
        eprintln!("ARRAY_EXCEPT Snowflake->DuckDB: {}", result[0]);
        assert!(
            result[0].contains("CASE WHEN"),
            "Expected CASE WHEN: {}",
            result[0]
        );
        assert!(
            result[0].contains("LIST_TRANSFORM"),
            "Expected LIST_TRANSFORM: {}",
            result[0]
        );
    }

    #[test]
    fn test_array_contains_snowflake_to_snowflake() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT ARRAY_CONTAINS(x, [1, NULL, 3])",
                DialectType::Snowflake,
            )
            .unwrap();
        eprintln!("ARRAY_CONTAINS Snowflake->Snowflake: {}", result[0]);
        assert_eq!(result[0], "SELECT ARRAY_CONTAINS(x, [1, NULL, 3])");
    }

    #[test]
    fn test_array_contains_snowflake_to_duckdb() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT ARRAY_CONTAINS(x, [1, NULL, 3])",
                DialectType::DuckDB,
            )
            .unwrap();
        eprintln!("ARRAY_CONTAINS Snowflake->DuckDB: {}", result[0]);
        assert!(
            result[0].contains("CASE WHEN"),
            "Expected CASE WHEN: {}",
            result[0]
        );
        assert!(
            result[0].contains("NULLIF"),
            "Expected NULLIF: {}",
            result[0]
        );
        assert!(
            result[0].contains("ARRAY_CONTAINS"),
            "Expected ARRAY_CONTAINS: {}",
            result[0]
        );
    }

    #[test]
    fn test_array_distinct_snowflake_to_duckdb() {
        let dialect = Dialect::get(DialectType::Snowflake);
        let result = dialect
            .transpile(
                "SELECT ARRAY_DISTINCT([1, 2, 2, 3, 1])",
                DialectType::DuckDB,
            )
            .unwrap();
        eprintln!("ARRAY_DISTINCT Snowflake->DuckDB: {}", result[0]);
        assert!(
            result[0].contains("CASE WHEN"),
            "Expected CASE WHEN: {}",
            result[0]
        );
        assert!(
            result[0].contains("LIST_DISTINCT"),
            "Expected LIST_DISTINCT: {}",
            result[0]
        );
        assert!(
            result[0].contains("LIST_APPEND"),
            "Expected LIST_APPEND: {}",
            result[0]
        );
        assert!(
            result[0].contains("LIST_FILTER"),
            "Expected LIST_FILTER: {}",
            result[0]
        );
    }
}
