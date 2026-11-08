//! Structured execution-plan inspection, shared by the supported SQL dialects.

use super::*;
use crate::dialects::DialectType;

/// How an option is spelled. Consecutive parenthesized options form one list.
#[derive(
    polyglot_sql_ast_derive::AstNode, Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize,
)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
pub enum ExplainOptionSyntax {
    Keyword,
    CommaSeparated,
    Equals,
    Parenthesized,
    With,
    Using,
    As,
    Set,
    Into,
}

macro_rules! explain_options {
    ($($variant:ident => $name:literal),* $(,)?) => {
        /// Semantic option names, including native plan modes. Unknown extension
        /// options remain explicit and make execution classification conservative.
        #[derive(polyglot_sql_ast_derive::AstNode, Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
        #[cfg_attr(feature = "bindings", derive(TS))]
        #[serde(rename_all = "snake_case")]
        pub enum ExplainOptionKind { $($variant,)* Other(String) }

        impl ExplainOptionKind {
            pub fn from_name(name: &str) -> Self {
                match name.to_ascii_uppercase().as_str() {
                    "ANALYSE" => Self::Analyze,
                    $($name => Self::$variant,)*
                    _ => Self::Other(name.to_string()),
                }
            }
            pub fn name(&self) -> &str {
                match self { $(Self::$variant => $name,)* Self::Other(name) => name }
            }
        }
    }
}

explain_options! {
    Analyze => "ANALYZE", Verbose => "VERBOSE", Costs => "COSTS",
    Buffers => "BUFFERS", Settings => "SETTINGS", Wal => "WAL", Timing => "TIMING",
    Summary => "SUMMARY", Memory => "MEMORY", GenericPlan => "GENERIC_PLAN",
    Serialize => "SERIALIZE", Format => "FORMAT", Type => "TYPE", Sql => "SQL",
    Extended => "EXTENDED", Formatted => "FORMATTED", Cost => "COST", Codegen => "CODEGEN",
    QueryPlan => "QUERY PLAN", Plan => "PLAN", Ast => "AST", Syntax => "SYNTAX",
    QueryTree => "QUERY TREE", Pipeline => "PIPELINE", Estimate => "ESTIMATE",
    TableOverride => "TABLE OVERRIDE", CurrentTransaction => "CURRENT TRANSACTION",
    WhatIf => "WHATIF", Cbo => "CBO", Dependency => "DEPENDENCY", Authorization => "AUTHORIZATION",
    Locks => "LOCKS", Vectorization => "VECTORIZATION", Only => "ONLY",
    Detail => "DETAIL", Expression => "EXPRESSION", Debug => "DEBUG",
    Operator => "OPERATOR", Partitions => "PARTITIONS",
    Cpu => "CPU", Hints => "HINTS", Cluster => "CLUSTER", WithSkew => "WITH SKEW",
    Logical => "LOGICAL", Physical => "PHYSICAL", Local => "LOCAL", Annotated => "ANNOTATED",
    Json => "JSON", OptimizedPlan => "OPTIMIZED PLAN", RawPlan => "RAW PLAN",
    DecorrelatedPlan => "DECORRELATED PLAN", LocallyOptimizedPlan => "LOCALLY OPTIMIZED PLAN",
    PhysicalPlan => "PHYSICAL PLAN", Timestamp => "TIMESTAMP", FilterPushdown => "FILTER PUSHDOWN",
    Opt => "OPT", Types => "TYPES", Env => "ENV", Memo => "MEMO", Redact => "REDACT",
    Vec => "VEC", DistSql => "DISTSQL", Fingerprint => "FINGERPRINT", Trace => "TRACE",
    ShapePlan => "SHAPE PLAN", AllPlan => "ALL PLAN", MemoPlan => "MEMO PLAN",
    ParsedPlan => "PARSED PLAN", AnalyzedPlan => "ANALYZED PLAN", RewrittenPlan => "REWRITTEN PLAN",
    Duplicate => "DUPLICATE", Mbc => "MBC", Mpl => "MPL", Repro => "REPRO", Ui => "UI",
    Ttl => "TTL",
    Outfile => "INTO OUTFILE", InXml => "IN XML", NoDdlText => "NODDLTEXT",
    Static => "STATIC", Dynamic => "DYNAMIC", Virtual => "VIRTUAL",
    IncludingAllAttributes => "INCLUDING ALL ATTRIBUTES", IncludingAttributes => "INCLUDING ATTRIBUTES",
    ExcludingAttributes => "EXCLUDING ATTRIBUTES", WithImplementation => "WITH IMPLEMENTATION",
    WithoutImplementation => "WITHOUT IMPLEMENTATION", WithType => "WITH TYPE",
    StatementId => "STATEMENT_ID", StatementName => "STATEMENT_NAME", Destination => "DESTINATION",
    WithRecommendations => "WITH_RECOMMENDATIONS",
}

#[derive(polyglot_sql_ast_derive::AstNode, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct ExplainOption {
    pub kind: ExplainOptionKind,
    pub value: Option<Expression>,
    pub syntax: ExplainOptionSyntax,
}

impl ExplainOption {
    /// Interpret the boolean spellings accepted by PostgreSQL-style options.
    /// This does not imply that every engine honors a false ANALYZE argument.
    pub fn boolean_value(&self) -> Option<bool> {
        match &self.value {
            None => Some(true),
            Some(Expression::Boolean(value)) => Some(value.value),
            Some(Expression::Literal(value)) => {
                match value.value_str().to_ascii_uppercase().as_str() {
                    "TRUE" | "ON" | "1" => Some(true),
                    "FALSE" | "OFF" | "0" => Some(false),
                    _ => None,
                }
            }
            Some(Expression::Identifier(value)) => match value.name.to_ascii_uppercase().as_str() {
                "TRUE" | "ON" => Some(true),
                "FALSE" | "OFF" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }
}

/// Some engines inspect an existing plan or object rather than a SQL statement.
#[derive(polyglot_sql_ast_derive::AstNode, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
pub enum ExplainTarget {
    Statement(Box<Expression>),
    Connection(Box<Expression>),
    PlanCache {
        entry: Box<Expression>,
        variant: Option<Box<Expression>>,
    },
    Object {
        kind: String,
        name: Box<Expression>,
    },
    TableOverride {
        function: Box<Expression>,
        clauses: Vec<ExplainOption>,
    },
    CurrentTransaction,
    Cluster,
}

/// Whether EXPLAIN requests execution of its target, not a read-only guarantee.
/// Planning can itself have effects (for example Oracle's plan table writes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
#[serde(rename_all = "snake_case")]
pub enum ExplainExecution {
    PlanOnly,
    Executes,
    InspectExisting,
    Unknown,
}

#[derive(polyglot_sql_ast_derive::AstNode, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "bindings", derive(TS))]
pub struct Explain {
    #[ast(skip)]
    pub source_dialect: DialectType,
    pub target: ExplainTarget,
    pub options: Vec<ExplainOption>,
    /// PLAN FOR and related native syntaxes retain their explicit connector.
    pub for_target: bool,
    pub output_format: Option<Identifier>,
    /// Materialize can return the introspection SQL instead of running it.
    #[serde(default)]
    pub as_sql: bool,
    #[serde(default)]
    pub leading_comments: Vec<String>,
}

impl Explain {
    pub fn new(statement: Expression, source_dialect: DialectType) -> Self {
        Self {
            source_dialect,
            target: ExplainTarget::Statement(Box::new(statement)),
            options: Vec::new(),
            for_target: false,
            output_format: None,
            as_sql: false,
            leading_comments: Vec::new(),
        }
    }

    pub fn statement(&self) -> Option<&Expression> {
        match &self.target {
            ExplainTarget::Statement(statement) => Some(statement),
            _ => None,
        }
    }

    pub fn execution_mode(&self) -> ExplainExecution {
        if !matches!(self.target, ExplainTarget::Statement(_)) {
            return ExplainExecution::InspectExisting;
        }
        let mut analyze = false;
        for option in &self.options {
            if option.kind == ExplainOptionKind::Analyze {
                // DuckDB enables analysis by presence, even for ANALYZE FALSE.
                analyze = if self.source_dialect == DialectType::DuckDB {
                    true
                } else if let Some(value) = option.boolean_value() {
                    value
                } else {
                    return ExplainExecution::Unknown;
                };
            }
        }
        if analyze {
            return ExplainExecution::Executes;
        }
        if self
            .options
            .iter()
            .any(|o| matches!(o.kind, ExplainOptionKind::Other(_)))
            || matches!(
                self.statement(),
                Some(Expression::Raw(_) | Expression::Command(_))
            )
        {
            ExplainExecution::Unknown
        } else {
            ExplainExecution::PlanOnly
        }
    }
}
