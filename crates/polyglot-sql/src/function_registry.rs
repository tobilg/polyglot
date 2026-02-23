//! Centralized parser-facing function metadata.
//!
//! This module is intentionally small and focused on parseability flags that were
//! previously scattered across parser-local lists. Unknown `name(...)` functions
//! still parse via generic function fallback; this registry only captures
//! parser-special cases like no-paren and aggregate behavior.

use std::collections::{HashMap, HashSet};
use std::sync::LazyLock;

/// Metadata describing parser-specific function behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct FunctionSpec {
    pub name: &'static str,
    pub no_paren: bool,
    pub aggregate: bool,
}

/// Names that can be parsed as no-paren functions.
pub(crate) const NO_PAREN_FUNCTION_NAME_LIST: &[&str] = &[
    "CURRENT_DATE",
    "CURRENT_TIME",
    "CURRENT_TIMESTAMP",
    "CURRENT_DATETIME",
    "CURRENT_USER",
    "CURRENT_ROLE",
    "CURRENT_SCHEMA",
    "CURRENT_CATALOG",
    "LOCALTIME",
    "LOCALTIMESTAMP",
    "SYSTIMESTAMP",
    "GETDATE",
    "SYSDATE",
    "SYSDATETIME",
    "NOW",
    "UTC_DATE",
    "UTC_TIME",
    "UTC_TIMESTAMP",
    "SESSION_USER",
    "SYSTEM_USER",
    // Note: USER by itself is NOT a no-paren function in standard SQL - only CURRENT_USER is
    "PI",
    // MySQL/Databricks CURDATE
    "CURDATE",
];

/// Names that should use aggregate parsing behavior in generic-function fallback.
pub(crate) const AGGREGATE_FUNCTION_NAME_LIST: &[&str] = &[
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "ARRAY_AGG",
    "ARRAY_CONCAT_AGG",
    "STRING_AGG",
    "GROUP_CONCAT",
    "LISTAGG",
    "STDDEV",
    "STDDEV_POP",
    "STDDEV_SAMP",
    "VARIANCE",
    "VAR_POP",
    "VAR_SAMP",
    "BOOL_AND",
    "BOOL_OR",
    "EVERY",
    "BIT_AND",
    "BIT_OR",
    "BIT_XOR",
    "BITWISE_AND_AGG",
    "BITWISE_OR_AGG",
    "BITWISE_XOR_AGG",
    "CORR",
    "COVAR_POP",
    "COVAR_SAMP",
    "PERCENTILE_CONT",
    "PERCENTILE_DISC",
    "APPROX_COUNT_DISTINCT",
    "APPROX_DISTINCT",
    "APPROX_PERCENTILE",
    "COLLECT_LIST",
    "COLLECT_SET",
    "COUNT_IF",
    "COUNTIF",
    "SUM_IF",
    "SUMIF",
    "MEDIAN",
    "MODE",
    "FIRST",
    "LAST",
    "ANY_VALUE",
    "FIRST_VALUE",
    "LAST_VALUE",
    "JSON_ARRAYAGG",
    "JSON_OBJECTAGG",
    "JSONB_AGG",
    "JSONB_OBJECT_AGG",
    "JSON_AGG",
    "JSON_OBJECT_AGG",
    "XMLAGG",
    "LOGICAL_AND",
    "LOGICAL_OR",
    "ARG_MIN",
    "ARG_MAX",
    "ARGMIN",
    "ARGMAX",
    "MIN_BY",
    "MAX_BY",
    "REGR_SLOPE",
    "REGR_INTERCEPT",
    "REGR_COUNT",
    "REGR_R2",
    "REGR_AVGX",
    "REGR_AVGY",
    "REGR_SXX",
    "REGR_SYY",
    "REGR_SXY",
    "KURTOSIS",
    "SKEWNESS",
    "APPROX_QUANTILES",
    "APPROX_TOP_COUNT",
    "ENTROPY",
    "FAVG",
    "FSUM",
    "RESERVOIR_SAMPLE",
    "HISTOGRAM",
    "LIST",
    "ARBITRARY",
];

/// Consolidated metadata map keyed by uppercased function name.
pub(crate) static FUNCTION_SPECS: LazyLock<HashMap<&'static str, FunctionSpec>> =
    LazyLock::new(|| {
        let mut specs = HashMap::new();

        for &name in NO_PAREN_FUNCTION_NAME_LIST {
            specs.insert(
                name,
                FunctionSpec {
                    name,
                    no_paren: true,
                    aggregate: false,
                },
            );
        }

        for &name in AGGREGATE_FUNCTION_NAME_LIST {
            specs
                .entry(name)
                .and_modify(|spec| spec.aggregate = true)
                .or_insert(FunctionSpec {
                    name,
                    no_paren: false,
                    aggregate: true,
                });
        }

        specs
    });

/// Set of names that can be no-paren functions.
pub(crate) static NO_PAREN_FUNCTION_NAME_SET: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| {
        FUNCTION_SPECS
            .iter()
            .filter_map(|(name, spec)| spec.no_paren.then_some(*name))
            .collect()
    });

/// Set of names that should use aggregate parsing behavior.
pub(crate) static AGGREGATE_FUNCTION_NAME_SET: LazyLock<HashSet<&'static str>> =
    LazyLock::new(|| {
        FUNCTION_SPECS
            .iter()
            .filter_map(|(name, spec)| spec.aggregate.then_some(*name))
            .collect()
    });

/// High-level parse category for canonical typed function dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypedParseKind {
    AggregateLike,
    Unary,
    Binary,
    Conditional,
    CastLike,
    Variadic,
}

/// Typed-function metadata for canonicalization and future dispatch organization.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TypedFunctionSpec {
    pub canonical_name: &'static str,
    /// Optional alias spellings that canonicalize to `canonical_name`.
    /// Canonical-only entries can leave this empty.
    pub aliases: &'static [&'static str],
    pub parse_kind: TypedParseKind,
}

/// Canonical typed function specs with alias spellings.
pub(crate) const TYPED_FUNCTION_SPECS: &[TypedFunctionSpec] = &[
    TypedFunctionSpec {
        canonical_name: "COUNT_IF",
        aliases: &["COUNTIF"],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "STARTS_WITH",
        aliases: &["STARTSWITH"],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ENDS_WITH",
        aliases: &["ENDSWITH"],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "DAYOFWEEK",
        aliases: &["DAY_OF_WEEK"],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "DAYOFYEAR",
        aliases: &["DAY_OF_YEAR"],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "DAYOFMONTH",
        aliases: &["DAY_OF_MONTH"],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "WEEKOFYEAR",
        aliases: &["WEEK_OF_YEAR"],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "SUBSTRING",
        aliases: &["SUBSTR"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "DATE_PART",
        aliases: &["DATEPART"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "DATEADD",
        aliases: &["DATE_ADD", "TIMEADD", "TIMESTAMPADD"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "DATEDIFF",
        aliases: &["DATE_DIFF", "TIMEDIFF", "TIMESTAMPDIFF"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TRY_CAST",
        aliases: &["TRYCAST"],
        parse_kind: TypedParseKind::CastLike,
    },
    TypedFunctionSpec {
        canonical_name: "LENGTH",
        aliases: &["LEN", "CHAR_LENGTH", "CHARACTER_LENGTH"],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "LOWER",
        aliases: &["LCASE"],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "UPPER",
        aliases: &["UCASE"],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "CEIL",
        aliases: &["CEILING"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TO_NUMBER",
        aliases: &["TO_DECIMAL", "TO_NUMERIC"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TRY_TO_NUMBER",
        aliases: &["TRY_TO_DECIMAL", "TRY_TO_NUMERIC"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TIMESTAMP_FROM_PARTS",
        aliases: &["TIMESTAMPFROMPARTS"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TIMESTAMP_NTZ_FROM_PARTS",
        aliases: &["TIMESTAMPNTZFROMPARTS"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TIMESTAMP_LTZ_FROM_PARTS",
        aliases: &["TIMESTAMPLTZFROMPARTS"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TIMESTAMP_TZ_FROM_PARTS",
        aliases: &["TIMESTAMPTZFROMPARTS"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "DATE_FROM_PARTS",
        aliases: &["DATEFROMPARTS"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TIME_FROM_PARTS",
        aliases: &["TIMEFROMPARTS"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TYPEOF",
        aliases: &["TOTYPENAME"],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "SIN",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "COS",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "TAN",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ASIN",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ACOS",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ATAN",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "RADIANS",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "DEGREES",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ATAN2",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "YEAR",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "MONTH",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "DAY",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "HOUR",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "MINUTE",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "SECOND",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "DAYOFWEEK_ISO",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "QUARTER",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "EPOCH",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "EPOCH_MS",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_LENGTH",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_SIZE",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "CARDINALITY",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_REVERSE",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_DISTINCT",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_COMPACT",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "EXPLODE",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "EXPLODE_OUTER",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "MAP_FROM_ENTRIES",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "MAP_KEYS",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "MAP_VALUES",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "MAP_FROM_ARRAYS",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "MAP_CONTAINS_KEY",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ELEMENT_AT",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ABS",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "SQRT",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "EXP",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "LN",
        aliases: &[],
        parse_kind: TypedParseKind::Unary,
    },
    TypedFunctionSpec {
        canonical_name: "CONTAINS",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "MOD",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "POW",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ADD_MONTHS",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "MONTHS_BETWEEN",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "NEXT_DAY",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_CONTAINS",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_POSITION",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_APPEND",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_PREPEND",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_UNION",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_EXCEPT",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_REMOVE",
        aliases: &[],
        parse_kind: TypedParseKind::Binary,
    },
    TypedFunctionSpec {
        canonical_name: "RANDOM",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "RAND",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "PI",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "LAST_DAY",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "POSITION",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "STRPOS",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "LOCATE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "INSTR",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "NORMALIZE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "INITCAP",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "FLOOR",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "LOG",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "FLATTEN",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_INTERSECT",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "CURRENT_SCHEMAS",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "COALESCE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "IFNULL",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "NVL",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "NVL2",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "EXTRACT",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "STRUCT",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "CHAR",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "CHR",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "RANGE_N",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "XMLTABLE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "XMLELEMENT",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "XMLATTRIBUTES",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "XMLCOMMENT",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "MATCH",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TRANSFORM",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "CONVERT",
        aliases: &["TRY_CONVERT"],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TRIM",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "OVERLAY",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "COUNT",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "LIST",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "MAP",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "SUM",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "AVG",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "MIN",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "MAX",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_AGG",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "ARRAY_CONCAT_AGG",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "STDDEV",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "STDDEV_POP",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "STDDEV_SAMP",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "VARIANCE",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "VAR_POP",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "VAR_SAMP",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "MEDIAN",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "MODE",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "FIRST",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "LAST",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "ANY_VALUE",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "APPROX_DISTINCT",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "APPROX_COUNT_DISTINCT",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "BIT_AND",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "BIT_OR",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "BIT_XOR",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "STRING_AGG",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "GROUP_CONCAT",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "LISTAGG",
        aliases: &[],
        parse_kind: TypedParseKind::AggregateLike,
    },
    TypedFunctionSpec {
        canonical_name: "ROW_NUMBER",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "RANK",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "DENSE_RANK",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "PERCENT_RANK",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "CUME_DIST",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "NTILE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "LEAD",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "LAG",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "FIRST_VALUE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "LAST_VALUE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "NTH_VALUE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_EXTRACT",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_EXTRACT_SCALAR",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_QUERY",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_VALUE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_ARRAY_LENGTH",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_KEYS",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_TYPE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TO_JSON",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "PARSE_JSON",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_OBJECT",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_ARRAY",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_ARRAYAGG",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_OBJECTAGG",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "JSON_TABLE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "TRANSLATE",
        aliases: &[],
        parse_kind: TypedParseKind::Variadic,
    },
    TypedFunctionSpec {
        canonical_name: "IF",
        aliases: &["IIF", "IFF"],
        parse_kind: TypedParseKind::Conditional,
    },
];

/// Canonical typed function spec map.
pub(crate) static TYPED_FUNCTION_SPEC_BY_CANONICAL: LazyLock<
    HashMap<&'static str, &'static TypedFunctionSpec>,
> = LazyLock::new(|| {
    let mut map = HashMap::new();

    for spec in TYPED_FUNCTION_SPECS {
        if map.insert(spec.canonical_name, spec).is_some() {
            panic!(
                "duplicate canonical typed function spec '{}'",
                spec.canonical_name
            );
        }
    }

    map
});

/// Typed-function aliases normalized before `parse_typed_function` dispatch.
///
/// All keys and values are uppercased names.
pub(crate) static TYPED_FUNCTION_ALIAS_TO_CANONICAL: LazyLock<HashMap<&'static str, &'static str>> =
    LazyLock::new(|| {
        let mut map = HashMap::new();

        for spec in TYPED_FUNCTION_SPECS {
            for &alias in spec.aliases {
                if map.insert(alias, spec.canonical_name).is_some() {
                    panic!("duplicate typed function alias '{}'", alias);
                }
            }
        }

        map
    });

/// Registry metadata group for specialized typed-function parser families.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TypedDispatchGroup {
    AggregateFamily,
    WindowFamily,
    JsonFamily,
    TranslateTeradataFamily,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TypedDispatchGroupSpec {
    pub name: &'static str,
    pub group: TypedDispatchGroup,
}

pub(crate) const TYPED_DISPATCH_GROUP_SPECS: &[TypedDispatchGroupSpec] = &[
    // aggregate-like heavy family
    TypedDispatchGroupSpec {
        name: "COUNT",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "LIST",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "MAP",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "ARRAY",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "SUM",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "AVG",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "MIN",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "MAX",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "ARRAY_AGG",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "ARRAY_CONCAT_AGG",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "STDDEV",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "STDDEV_POP",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "STDDEV_SAMP",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "VARIANCE",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "VAR_POP",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "VAR_SAMP",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "MEDIAN",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "MODE",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "FIRST",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "LAST",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "ANY_VALUE",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "APPROX_DISTINCT",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "APPROX_COUNT_DISTINCT",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "BIT_AND",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "BIT_OR",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "BIT_XOR",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "STRING_AGG",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "GROUP_CONCAT",
        group: TypedDispatchGroup::AggregateFamily,
    },
    TypedDispatchGroupSpec {
        name: "LISTAGG",
        group: TypedDispatchGroup::AggregateFamily,
    },
    // window family
    TypedDispatchGroupSpec {
        name: "ROW_NUMBER",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "RANK",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "DENSE_RANK",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "PERCENT_RANK",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "CUME_DIST",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "NTILE",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "LEAD",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "LAG",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "FIRST_VALUE",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "LAST_VALUE",
        group: TypedDispatchGroup::WindowFamily,
    },
    TypedDispatchGroupSpec {
        name: "NTH_VALUE",
        group: TypedDispatchGroup::WindowFamily,
    },
    // JSON family
    TypedDispatchGroupSpec {
        name: "JSON_EXTRACT",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_EXTRACT_SCALAR",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_QUERY",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_VALUE",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_ARRAY_LENGTH",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_KEYS",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_TYPE",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "TO_JSON",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "PARSE_JSON",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_OBJECT",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_ARRAY",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_ARRAYAGG",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_OBJECTAGG",
        group: TypedDispatchGroup::JsonFamily,
    },
    TypedDispatchGroupSpec {
        name: "JSON_TABLE",
        group: TypedDispatchGroup::JsonFamily,
    },
    // Teradata-specific typed branch
    TypedDispatchGroupSpec {
        name: "TRANSLATE",
        group: TypedDispatchGroup::TranslateTeradataFamily,
    },
];

pub(crate) static TYPED_DISPATCH_GROUP_BY_NAME: LazyLock<
    HashMap<&'static str, TypedDispatchGroup>,
> = LazyLock::new(|| {
    let mut map = HashMap::new();

    for spec in TYPED_DISPATCH_GROUP_SPECS {
        if map.insert(spec.name, spec.group).is_some() {
            panic!("duplicate typed dispatch group name '{}'", spec.name);
        }
    }

    map
});

/// Parser dispatch behavior for phase-4 registry-driven typed-function handling.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParserDispatchBehavior {
    ExprListFunction,
    OptionalExprListFunction,
    FunctionArgumentsFunction,
    ZeroArgFunction,
    ExprListMaybeAggregateByFilter,
    ExprListMaybeAggregateByAggSuffix,
    HashLike,
    HllAggregate,
    PercentileAggregate,
    ExprListAggregate,
    UnaryAggregate,
    TranslateNonTeradata,
}

/// Parser dispatch metadata entry keyed by uppercased function name.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ParserDispatchSpec {
    pub name: &'static str,
    pub behavior: ParserDispatchBehavior,
}

/// Parser dispatch metadata currently migrated from parser-local phase-3 lists.
pub(crate) const PARSER_DISPATCH_SPECS: &[ParserDispatchSpec] = &[
    // expression-list function pass-through
    ParserDispatchSpec {
        name: "TRY_TO_DATE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_TO_TIMESTAMP",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_TO_TIME",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_TO_DOUBLE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_TO_BOOLEAN",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_TO_BINARY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_TO_GEOGRAPHY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_TO_GEOMETRY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_PARSE_JSON",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_TO_DECFLOAT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_BASE64_DECODE_BINARY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_BASE64_DECODE_STRING",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_HEX_DECODE_BINARY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TRY_HEX_DECODE_STRING",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_DATE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_TIMESTAMP",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_TIMESTAMP_NTZ",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_TIMESTAMP_LTZ",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_TIMESTAMP_TZ",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_TIME",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_CHAR",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_VARCHAR",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_DOUBLE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_BOOLEAN",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_BINARY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_VARIANT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_OBJECT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_ARRAY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_GEOGRAPHY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TO_GEOMETRY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TIME_SLICE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "MAP_CAT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "MAP_DELETE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "MAP_INSERT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "MAP_PICK",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "MAP_SIZE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "HEX_ENCODE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "HEX_DECODE_STRING",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "HEX_DECODE_BINARY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BASE64_ENCODE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BASE64_DECODE_STRING",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BASE64_DECODE_BINARY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "COMPRESS",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "DECOMPRESS_BINARY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "DECOMPRESS_STRING",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "STRTOK",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "SPLIT_PART",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "SOUNDEX",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "SOUNDEX_P123",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "RTRIMMED_LENGTH",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BIT_LENGTH",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "UNICODE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "JAROWINKLER_SIMILARITY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "EDITDISTANCE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "FACTORIAL",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "SQUARE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "CBRT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "SINH",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "COSH",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "TANH",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "ASINH",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "ACOSH",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "ATANH",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "WIDTH_BUCKET",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "NORMAL",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "UNIFORM",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "ZIPF",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BITAND",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BITOR",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BITXOR",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BITNOT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BITSHIFTLEFT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "BITSHIFTRIGHT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "GETBIT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "SETBIT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "REGEXP_LIKE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "RLIKE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "REGEXP_REPLACE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "REGEXP_SUBSTR",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "REGEXP_SUBSTR_ALL",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "REGEXP_INSTR",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "REGEXP_COUNT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "GET",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "LIKE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "ILIKE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "XMLGET",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "CHECK_XML",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "VECTOR_COSINE_SIMILARITY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "VECTOR_INNER_PRODUCT",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "VECTOR_L1_DISTANCE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "VECTOR_L2_DISTANCE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "WEEKISO",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "YEAROFWEEK",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "YEAROFWEEKISO",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "MONTHNAME",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "DAYNAME",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "PREVIOUS_DAY",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "CONVERT_TIMEZONE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "RANDSTR",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "PARSE_URL",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "PARSE_IP",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "IDENTIFIER",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "EQUAL_NULL",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "IS_NULL_VALUE",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "NULLIFZERO",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    ParserDispatchSpec {
        name: "ZEROIFNULL",
        behavior: ParserDispatchBehavior::ExprListFunction,
    },
    // optional-arg expression-list function pass-through
    ParserDispatchSpec {
        name: "GREATEST",
        behavior: ParserDispatchBehavior::OptionalExprListFunction,
    },
    ParserDispatchSpec {
        name: "LEAST",
        behavior: ParserDispatchBehavior::OptionalExprListFunction,
    },
    ParserDispatchSpec {
        name: "GREATEST_IGNORE_NULLS",
        behavior: ParserDispatchBehavior::OptionalExprListFunction,
    },
    ParserDispatchSpec {
        name: "LEAST_IGNORE_NULLS",
        behavior: ParserDispatchBehavior::OptionalExprListFunction,
    },
    ParserDispatchSpec {
        name: "GROUPING_ID",
        behavior: ParserDispatchBehavior::OptionalExprListFunction,
    },
    ParserDispatchSpec {
        name: "GROUPING",
        behavior: ParserDispatchBehavior::OptionalExprListFunction,
    },
    ParserDispatchSpec {
        name: "UUID_STRING",
        behavior: ParserDispatchBehavior::OptionalExprListFunction,
    },
    // function-argument parser path
    ParserDispatchSpec {
        name: "SEARCH",
        behavior: ParserDispatchBehavior::FunctionArgumentsFunction,
    },
    ParserDispatchSpec {
        name: "SEARCH_IP",
        behavior: ParserDispatchBehavior::FunctionArgumentsFunction,
    },
    ParserDispatchSpec {
        name: "STAR",
        behavior: ParserDispatchBehavior::FunctionArgumentsFunction,
    },
    // zero-arg function pass-through
    ParserDispatchSpec {
        name: "CURRENT_ACCOUNT",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_ACCOUNT_NAME",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_AVAILABLE_ROLES",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_CLIENT",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_IP_ADDRESS",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_DATABASE",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_SECONDARY_ROLES",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_SESSION",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_STATEMENT",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_VERSION",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_TRANSACTION",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_WAREHOUSE",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_ORGANIZATION_USER",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_REGION",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_ROLE",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_ROLE_TYPE",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    ParserDispatchSpec {
        name: "CURRENT_ORGANIZATION_NAME",
        behavior: ParserDispatchBehavior::ZeroArgFunction,
    },
    // filter-sensitive function/aggregate forms
    ParserDispatchSpec {
        name: "CHECK_JSON",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "JSON_EXTRACT_PATH_TEXT",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "GET_PATH",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "OBJECT_CONSTRUCT",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "OBJECT_CONSTRUCT_KEEP_NULL",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "OBJECT_INSERT",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "OBJECT_DELETE",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "OBJECT_PICK",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "ARRAY_CONSTRUCT",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "ARRAY_CONSTRUCT_COMPACT",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "ARRAY_SLICE",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    ParserDispatchSpec {
        name: "ARRAY_FLATTEN",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByFilter,
    },
    // _AGG suffix-sensitive function/aggregate forms
    ParserDispatchSpec {
        name: "AI_AGG",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "AI_SUMMARIZE_AGG",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "AI_CLASSIFY",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BITMAP_BUCKET_NUMBER",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BITMAP_CONSTRUCT_AGG",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BITMAP_COUNT",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BITMAP_OR_AGG",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BOOLAND",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BOOLOR",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BOOLXOR",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BOOLXOR_AGG",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BOOLAND_AGG",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    ParserDispatchSpec {
        name: "BOOLOR_AGG",
        behavior: ParserDispatchBehavior::ExprListMaybeAggregateByAggSuffix,
    },
    // behavior-specific handlers
    ParserDispatchSpec {
        name: "HASH",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "HASH_AGG",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "MD5",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "MD5_HEX",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "MD5_BINARY",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "SHA1",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "SHA1_HEX",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "SHA1_BINARY",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "SHA2",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "SHA2_HEX",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "SHA2_BINARY",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "MINHASH",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "FARM_FINGERPRINT",
        behavior: ParserDispatchBehavior::HashLike,
    },
    ParserDispatchSpec {
        name: "HLL",
        behavior: ParserDispatchBehavior::HllAggregate,
    },
    ParserDispatchSpec {
        name: "PERCENTILE",
        behavior: ParserDispatchBehavior::PercentileAggregate,
    },
    ParserDispatchSpec {
        name: "APPROX_TOP_K",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "APPROX_TOP_K_ACCUMULATE",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "APPROX_TOP_K_COMBINE",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "APPROX_TOP_K_ESTIMATE",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "APPROX_PERCENTILE_ACCUMULATE",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "APPROX_PERCENTILE_COMBINE",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "APPROX_PERCENTILE_ESTIMATE",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "OBJECT_AGG",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_AVGX",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_AVGY",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_COUNT",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_INTERCEPT",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_R2",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_SXX",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_SXY",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_SYY",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_SLOPE",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_VALX",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "REGR_VALY",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "CORR",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "COVAR_POP",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "COVAR_SAMP",
        behavior: ParserDispatchBehavior::ExprListAggregate,
    },
    ParserDispatchSpec {
        name: "KURTOSIS",
        behavior: ParserDispatchBehavior::UnaryAggregate,
    },
    ParserDispatchSpec {
        name: "SKEW",
        behavior: ParserDispatchBehavior::UnaryAggregate,
    },
    ParserDispatchSpec {
        name: "TRANSLATE",
        behavior: ParserDispatchBehavior::TranslateNonTeradata,
    },
];

/// Parser dispatch behavior map keyed by uppercased function name.
pub(crate) static PARSER_DISPATCH_BEHAVIOR_BY_NAME: LazyLock<
    HashMap<&'static str, ParserDispatchBehavior>,
> = LazyLock::new(|| {
    let mut map = HashMap::new();
    for spec in PARSER_DISPATCH_SPECS {
        if map.insert(spec.name, spec.behavior).is_some() {
            panic!("duplicate parser dispatch spec '{}'", spec.name);
        }
    }
    map
});

/// Returns parser dispatch behavior by uppercased function name.
pub(crate) fn parser_dispatch_behavior_by_name_upper(
    upper_name: &str,
) -> Option<ParserDispatchBehavior> {
    PARSER_DISPATCH_BEHAVIOR_BY_NAME.get(upper_name).copied()
}

/// Returns true if the given uppercased name can be parsed as a no-paren function.
pub(crate) fn is_no_paren_function_name_upper(upper_name: &str) -> bool {
    NO_PAREN_FUNCTION_NAME_SET.contains(upper_name)
}

/// Returns true if the given uppercased name should use aggregate parsing behavior.
pub(crate) fn is_aggregate_function_name_upper(upper_name: &str) -> bool {
    AGGREGATE_FUNCTION_NAME_SET.contains(upper_name)
}

/// Returns true if the given name (any casing) should use aggregate parsing behavior.
pub(crate) fn is_aggregate_function_name(name: &str) -> bool {
    let upper = name.to_uppercase();
    is_aggregate_function_name_upper(upper.as_str())
}

/// Returns typed function spec by canonical or alias uppercased name.
pub(crate) fn typed_function_spec_by_name_upper(
    upper_name: &str,
) -> Option<&'static TypedFunctionSpec> {
    let canonical_name = TYPED_FUNCTION_ALIAS_TO_CANONICAL
        .get(upper_name)
        .copied()
        .unwrap_or(upper_name);
    typed_function_spec_by_canonical_upper(canonical_name)
}

/// Returns typed function spec by canonical uppercased name.
pub(crate) fn typed_function_spec_by_canonical_upper(
    canonical_upper_name: &str,
) -> Option<&'static TypedFunctionSpec> {
    TYPED_FUNCTION_SPEC_BY_CANONICAL
        .get(canonical_upper_name)
        .copied()
}

/// Returns the canonical uppercased typed-function dispatch key for an uppercased name.
pub(crate) fn canonical_typed_function_name_upper<'a>(upper_name: &'a str) -> &'a str {
    typed_function_spec_by_name_upper(upper_name)
        .map(|spec| spec.canonical_name)
        .unwrap_or(upper_name)
}

/// Returns specialized typed parser dispatch group for canonical uppercased name.
pub(crate) fn typed_dispatch_group_by_name_upper(
    canonical_upper_name: &str,
) -> Option<TypedDispatchGroup> {
    TYPED_DISPATCH_GROUP_BY_NAME
        .get(canonical_upper_name)
        .copied()
}

#[cfg(test)]
mod tests {
    use super::{
        canonical_typed_function_name_upper, is_aggregate_function_name_upper,
        is_no_paren_function_name_upper, parser_dispatch_behavior_by_name_upper,
        typed_dispatch_group_by_name_upper, typed_function_spec_by_canonical_upper,
        typed_function_spec_by_name_upper, ParserDispatchBehavior, TypedDispatchGroup,
        TypedParseKind, AGGREGATE_FUNCTION_NAME_LIST, FUNCTION_SPECS, NO_PAREN_FUNCTION_NAME_LIST,
        PARSER_DISPATCH_SPECS, TYPED_DISPATCH_GROUP_SPECS, TYPED_FUNCTION_ALIAS_TO_CANONICAL,
        TYPED_FUNCTION_SPECS,
    };
    use crate::dialects::DialectType;
    use std::collections::HashSet;

    #[test]
    fn every_declared_name_has_metadata() {
        for &name in NO_PAREN_FUNCTION_NAME_LIST {
            let spec = FUNCTION_SPECS
                .get(name)
                .unwrap_or_else(|| panic!("missing spec for no-paren function '{}'", name));
            assert!(spec.no_paren, "expected no_paren=true for '{}'", name);
        }

        for &name in AGGREGATE_FUNCTION_NAME_LIST {
            let spec = FUNCTION_SPECS
                .get(name)
                .unwrap_or_else(|| panic!("missing spec for aggregate function '{}'", name));
            assert!(spec.aggregate, "expected aggregate=true for '{}'", name);
        }
    }

    #[test]
    fn known_lookup_examples_work() {
        assert!(is_no_paren_function_name_upper("CURRENT_TIMESTAMP"));
        assert!(is_no_paren_function_name_upper("CURDATE"));
        assert!(!is_no_paren_function_name_upper("FOO"));

        assert!(is_aggregate_function_name_upper("COUNT"));
        assert!(is_aggregate_function_name_upper("APPROX_PERCENTILE"));
        assert!(!is_aggregate_function_name_upper("COALESCE"));
    }

    #[test]
    fn no_duplicate_names_in_source_lists() {
        let no_paren_unique: HashSet<&str> = NO_PAREN_FUNCTION_NAME_LIST.iter().copied().collect();
        assert_eq!(
            no_paren_unique.len(),
            NO_PAREN_FUNCTION_NAME_LIST.len(),
            "no-paren source list has duplicates"
        );

        let aggregate_unique: HashSet<&str> =
            AGGREGATE_FUNCTION_NAME_LIST.iter().copied().collect();
        assert_eq!(
            aggregate_unique.len(),
            AGGREGATE_FUNCTION_NAME_LIST.len(),
            "aggregate source list has duplicates"
        );

        let parser_dispatch_unique: HashSet<&str> =
            PARSER_DISPATCH_SPECS.iter().map(|spec| spec.name).collect();
        assert_eq!(
            parser_dispatch_unique.len(),
            PARSER_DISPATCH_SPECS.len(),
            "parser-dispatch source list has duplicates"
        );
    }

    #[test]
    fn parser_handles_registered_no_paren_function_name() {
        let parsed = crate::parse("SELECT CURDATE", DialectType::MySQL)
            .expect("CURDATE should parse as no-paren function");
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn parser_handles_registered_aggregate_function_name() {
        let parsed = crate::parse(
            "SELECT ARBITRARY(x ORDER BY y LIMIT 1) FROM t",
            DialectType::Generic,
        )
        .expect("ARBITRARY should parse with aggregate ORDER BY/LIMIT clauses");
        assert_eq!(parsed.len(), 1);
    }

    #[test]
    fn typed_function_specs_are_self_consistent() {
        let mut canonical_names = HashSet::new();
        let mut all_names = HashSet::new();

        for spec in TYPED_FUNCTION_SPECS {
            assert!(
                canonical_names.insert(spec.canonical_name),
                "duplicate canonical typed function '{}'",
                spec.canonical_name
            );
            assert!(
                all_names.insert(spec.canonical_name),
                "canonical '{}' collides with an alias/canonical in the typed registry",
                spec.canonical_name
            );
            for &alias in spec.aliases {
                assert_ne!(
                    alias, spec.canonical_name,
                    "typed function '{}' must not list canonical as alias",
                    spec.canonical_name
                );
                assert!(
                    all_names.insert(alias),
                    "duplicate typed alias '{}' in registry",
                    alias
                );
            }
        }
    }

    #[test]
    fn typed_function_lookup_maps_are_consistent() {
        for spec in TYPED_FUNCTION_SPECS {
            let by_canonical = typed_function_spec_by_canonical_upper(spec.canonical_name)
                .unwrap_or_else(|| panic!("missing canonical spec '{}'", spec.canonical_name));
            assert_eq!(by_canonical.canonical_name, spec.canonical_name);

            let by_name = typed_function_spec_by_name_upper(spec.canonical_name)
                .unwrap_or_else(|| panic!("missing name lookup for '{}'", spec.canonical_name));
            assert_eq!(by_name.canonical_name, spec.canonical_name);

            for &alias in spec.aliases {
                let by_alias = typed_function_spec_by_name_upper(alias)
                    .unwrap_or_else(|| panic!("missing alias lookup for '{}'", alias));
                assert_eq!(
                    by_alias.canonical_name, spec.canonical_name,
                    "alias '{}' must resolve to canonical '{}'",
                    alias, spec.canonical_name
                );
            }
        }
    }

    #[test]
    fn typed_alias_map_targets_known_canonical_specs() {
        for (&alias, &canonical) in TYPED_FUNCTION_ALIAS_TO_CANONICAL.iter() {
            let canonical_spec =
                typed_function_spec_by_canonical_upper(canonical).unwrap_or_else(|| {
                    panic!(
                        "alias '{}' maps to unknown canonical '{}'",
                        alias, canonical
                    )
                });
            assert_eq!(canonical_spec.canonical_name, canonical);
            assert!(
                canonical_spec.aliases.contains(&alias),
                "alias '{}' missing from canonical spec '{}'",
                alias,
                canonical
            );
        }
    }

    #[test]
    fn typed_parse_kind_examples_are_stable() {
        let count_if = typed_function_spec_by_canonical_upper("COUNT_IF")
            .expect("COUNT_IF typed spec should exist");
        assert_eq!(count_if.parse_kind, TypedParseKind::AggregateLike);

        let iif = typed_function_spec_by_name_upper("IIF").expect("IIF alias should exist");
        assert_eq!(iif.canonical_name, "IF");
        assert_eq!(iif.parse_kind, TypedParseKind::Conditional);

        let contains = typed_function_spec_by_canonical_upper("CONTAINS")
            .expect("CONTAINS typed spec should exist");
        assert_eq!(contains.parse_kind, TypedParseKind::Binary);

        let sqrt =
            typed_function_spec_by_canonical_upper("SQRT").expect("SQRT typed spec should exist");
        assert_eq!(sqrt.parse_kind, TypedParseKind::Unary);

        let timeadd = typed_function_spec_by_name_upper("TIMEADD")
            .expect("TIMEADD alias should resolve to DATEADD");
        assert_eq!(timeadd.canonical_name, "DATEADD");
        assert_eq!(timeadd.parse_kind, TypedParseKind::Variadic);
    }

    #[test]
    fn typed_aliases_canonicalize_to_dispatch_keys() {
        assert_eq!(canonical_typed_function_name_upper("COUNTIF"), "COUNT_IF");
        assert_eq!(
            canonical_typed_function_name_upper("DAY_OF_MONTH"),
            "DAYOFMONTH"
        );
        assert_eq!(canonical_typed_function_name_upper("SUBSTR"), "SUBSTRING");
        assert_eq!(canonical_typed_function_name_upper("TRYCAST"), "TRY_CAST");
        assert_eq!(canonical_typed_function_name_upper("LEN"), "LENGTH");
        assert_eq!(canonical_typed_function_name_upper("LCASE"), "LOWER");
        assert_eq!(canonical_typed_function_name_upper("UCASE"), "UPPER");
        assert_eq!(canonical_typed_function_name_upper("CEILING"), "CEIL");
        assert_eq!(
            canonical_typed_function_name_upper("TO_DECIMAL"),
            "TO_NUMBER"
        );
        assert_eq!(
            canonical_typed_function_name_upper("TRY_TO_NUMERIC"),
            "TRY_TO_NUMBER"
        );
        assert_eq!(
            canonical_typed_function_name_upper("DATEFROMPARTS"),
            "DATE_FROM_PARTS"
        );
        assert_eq!(
            canonical_typed_function_name_upper("TIMESTAMPTZFROMPARTS"),
            "TIMESTAMP_TZ_FROM_PARTS"
        );
        assert_eq!(canonical_typed_function_name_upper("TOTYPENAME"), "TYPEOF");
        assert_eq!(canonical_typed_function_name_upper("IIF"), "IF");
        assert_eq!(canonical_typed_function_name_upper("IFF"), "IF");
        assert_eq!(
            canonical_typed_function_name_upper("NOT_AN_ALIAS"),
            "NOT_AN_ALIAS"
        );
    }

    #[test]
    fn parser_accepts_phase2_typed_alias_batch() {
        let cases = [
            ("SELECT COUNTIF(x) FROM t", DialectType::Generic),
            ("SELECT STARTSWITH(a, b) FROM t", DialectType::Generic),
            ("SELECT ENDSWITH(a, b) FROM t", DialectType::Generic),
            ("SELECT DAY_OF_WEEK(ts) FROM t", DialectType::Generic),
            ("SELECT DAY_OF_YEAR(ts) FROM t", DialectType::Generic),
            ("SELECT DAY_OF_MONTH(ts) FROM t", DialectType::Generic),
            ("SELECT WEEK_OF_YEAR(ts) FROM t", DialectType::Generic),
            ("SELECT SUBSTR('abc', 1, 2) FROM t", DialectType::Generic),
            ("SELECT DATEPART(day, ts) FROM t", DialectType::TSQL),
            (
                "SELECT DATE_ADD(ts, INTERVAL 1 DAY) FROM t",
                DialectType::Generic,
            ),
            ("SELECT DATE_DIFF(day, a, b) FROM t", DialectType::Generic),
            ("SELECT TRYCAST(x AS INT) FROM t", DialectType::Generic),
            ("SELECT LEN(name) FROM t", DialectType::Generic),
            ("SELECT CHAR_LENGTH(name) FROM t", DialectType::Generic),
            ("SELECT CHARACTER_LENGTH(name) FROM t", DialectType::Generic),
            ("SELECT LCASE(name) FROM t", DialectType::Generic),
            ("SELECT UCASE(name) FROM t", DialectType::Generic),
            ("SELECT CEILING(value) FROM t", DialectType::Generic),
            (
                "SELECT TO_DECIMAL(value, 10, 2) FROM t",
                DialectType::Generic,
            ),
            (
                "SELECT TO_NUMERIC(value, 10, 2) FROM t",
                DialectType::Generic,
            ),
            (
                "SELECT TRY_TO_DECIMAL(value, 10, 2) FROM t",
                DialectType::Generic,
            ),
            (
                "SELECT TRY_TO_NUMERIC(value, 10, 2) FROM t",
                DialectType::Generic,
            ),
            (
                "SELECT TIMESTAMPFROMPARTS(2024, 1, 1, 0, 0, 0) FROM t",
                DialectType::Generic,
            ),
            (
                "SELECT DATEFROMPARTS(2024, 1, 1) FROM t",
                DialectType::Generic,
            ),
            (
                "SELECT TIMEFROMPARTS(12, 0, 0, 0, 0) FROM t",
                DialectType::Generic,
            ),
            ("SELECT TOTYPENAME(x) FROM t", DialectType::ClickHouse),
            ("SELECT IIF(x > 0, 1, 0) FROM t", DialectType::TSQL),
            ("SELECT IFF(x > 0, 1, 0) FROM t", DialectType::Snowflake),
        ];

        for (sql, dialect) in cases {
            crate::parse(sql, dialect).unwrap_or_else(|e| {
                panic!("expected alias '{}' to parse in {:?}: {}", sql, dialect, e)
            });
        }
    }

    #[test]
    fn parser_dispatch_behavior_lookup_examples_work() {
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("TRY_TO_DATE"),
            Some(ParserDispatchBehavior::ExprListFunction)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("GROUPING_ID"),
            Some(ParserDispatchBehavior::OptionalExprListFunction)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("SEARCH"),
            Some(ParserDispatchBehavior::FunctionArgumentsFunction)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("CURRENT_ACCOUNT"),
            Some(ParserDispatchBehavior::ZeroArgFunction)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("HASH_AGG"),
            Some(ParserDispatchBehavior::HashLike)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("HLL"),
            Some(ParserDispatchBehavior::HllAggregate)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("PERCENTILE"),
            Some(ParserDispatchBehavior::PercentileAggregate)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("KURTOSIS"),
            Some(ParserDispatchBehavior::UnaryAggregate)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("TRANSLATE"),
            Some(ParserDispatchBehavior::TranslateNonTeradata)
        );
        assert_eq!(
            parser_dispatch_behavior_by_name_upper("NOT_A_KNOWN_FN"),
            None
        );
    }

    #[test]
    fn typed_dispatch_group_metadata_is_consistent() {
        for spec in TYPED_DISPATCH_GROUP_SPECS {
            let typed_spec =
                typed_function_spec_by_canonical_upper(spec.name).unwrap_or_else(|| {
                    panic!("dispatch-group name '{}' missing typed spec", spec.name)
                });
            assert_eq!(typed_spec.canonical_name, spec.name);
            assert_eq!(
                typed_dispatch_group_by_name_upper(spec.name),
                Some(spec.group),
                "dispatch-group lookup mismatch for '{}'",
                spec.name
            );
        }

        assert_eq!(
            typed_dispatch_group_by_name_upper("COUNT"),
            Some(TypedDispatchGroup::AggregateFamily)
        );
        assert_eq!(
            typed_dispatch_group_by_name_upper("ROW_NUMBER"),
            Some(TypedDispatchGroup::WindowFamily)
        );
        assert_eq!(
            typed_dispatch_group_by_name_upper("JSON_TABLE"),
            Some(TypedDispatchGroup::JsonFamily)
        );
        assert_eq!(
            typed_dispatch_group_by_name_upper("TRANSLATE"),
            Some(TypedDispatchGroup::TranslateTeradataFamily)
        );
        assert_eq!(typed_dispatch_group_by_name_upper("NOT_A_TYPED_FN"), None);
    }

    #[test]
    fn typed_dispatch_group_specs_match_intended_heavy_family_set() {
        let intended_heavy_family_names: HashSet<&str> = [
            "COUNT",
            "LIST",
            "MAP",
            "ARRAY",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "ARRAY_AGG",
            "ARRAY_CONCAT_AGG",
            "STDDEV",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "VARIANCE",
            "VAR_POP",
            "VAR_SAMP",
            "MEDIAN",
            "MODE",
            "FIRST",
            "LAST",
            "ANY_VALUE",
            "APPROX_DISTINCT",
            "APPROX_COUNT_DISTINCT",
            "BIT_AND",
            "BIT_OR",
            "BIT_XOR",
            "STRING_AGG",
            "GROUP_CONCAT",
            "LISTAGG",
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "PERCENT_RANK",
            "CUME_DIST",
            "NTILE",
            "LEAD",
            "LAG",
            "FIRST_VALUE",
            "LAST_VALUE",
            "NTH_VALUE",
            "JSON_EXTRACT",
            "JSON_EXTRACT_SCALAR",
            "JSON_QUERY",
            "JSON_VALUE",
            "JSON_ARRAY_LENGTH",
            "JSON_KEYS",
            "JSON_TYPE",
            "TO_JSON",
            "PARSE_JSON",
            "JSON_OBJECT",
            "JSON_ARRAY",
            "JSON_ARRAYAGG",
            "JSON_OBJECTAGG",
            "JSON_TABLE",
            "TRANSLATE",
        ]
        .into_iter()
        .collect();

        let spec_names: HashSet<&str> = TYPED_DISPATCH_GROUP_SPECS
            .iter()
            .map(|spec| spec.name)
            .collect();

        assert_eq!(
            spec_names, intended_heavy_family_names,
            "typed dispatch-group specs must match the intended heavy-family set exactly"
        );

        for &name in &spec_names {
            assert!(
                typed_function_spec_by_canonical_upper(name).is_some(),
                "dispatch-group name '{}' must exist in TYPED_FUNCTION_SPECS",
                name
            );
            assert!(
                typed_dispatch_group_by_name_upper(name).is_some(),
                "dispatch-group name '{}' must resolve via lookup",
                name
            );
        }
    }

    #[test]
    fn parser_typed_dispatch_uses_canonical_aliases_only() {
        let parser_src = include_str!("parser.rs");
        let dispatch_start = parser_src
            .find("fn parse_typed_aggregate_family(")
            .expect("phase-6 aggregate family parser start not found");
        let dispatch_end = parser_src[dispatch_start..]
            .find("/// Parse a generic function call")
            .map(|idx| dispatch_start + idx)
            .expect("generic function parser sentinel not found");
        let dispatch_block = &parser_src[dispatch_start..dispatch_end];

        for alias in TYPED_FUNCTION_ALIAS_TO_CANONICAL.keys() {
            let alias_literal = format!("\"{alias}\"");
            assert!(
                !dispatch_block.contains(&alias_literal),
                "alias '{alias}' must not appear in parse_typed_function dispatch; use canonicalization registry"
            );
        }
    }

    #[test]
    fn parser_typed_dispatch_top_level_names_have_typed_specs() {
        let parser_src = include_str!("parser.rs");
        let dispatch_start = parser_src
            .find("fn parse_typed_aggregate_family(")
            .expect("phase-6 aggregate family parser start not found");
        let dispatch_end = parser_src[dispatch_start..]
            .find("/// Parse a generic function call")
            .map(|idx| dispatch_start + idx)
            .expect("generic function parser sentinel not found");
        let dispatch_block = &parser_src[dispatch_start..dispatch_end];

        for line in dispatch_block.lines() {
            let Some(rest) = line.strip_prefix("            \"") else {
                continue;
            };

            for (idx, part) in rest.split('"').enumerate() {
                if idx % 2 == 0 {
                    continue;
                }
                if part.is_empty() {
                    continue;
                }
                if !part
                    .chars()
                    .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit() || c == '_')
                {
                    continue;
                }

                assert!(
                    typed_function_spec_by_canonical_upper(part).is_some(),
                    "dispatch arm '{part}' is missing from TYPED_FUNCTION_SPECS"
                );
            }
        }
    }

    #[test]
    fn phase6_families_are_not_in_parse_typed_fallback() {
        let parser_src = include_str!("parser.rs");
        let parse_start = parser_src
            .find("fn parse_typed_function(")
            .expect("parse_typed_function start not found");
        let parse_end = parser_src[parse_start..]
            .find("fn parse_typed_aggregate_family(")
            .map(|idx| parse_start + idx)
            .expect("parse_typed_aggregate_family start not found");
        let parse_block = &parser_src[parse_start..parse_end];

        assert!(
            !parse_block.contains("match canonical_upper_name {"),
            "parse_typed_function fallback should not directly dispatch typed families"
        );

        let phase6_names = [
            "COUNT",
            "LIST",
            "MAP",
            "ARRAY",
            "SUM",
            "AVG",
            "MIN",
            "MAX",
            "ARRAY_AGG",
            "ARRAY_CONCAT_AGG",
            "STDDEV",
            "STDDEV_POP",
            "STDDEV_SAMP",
            "VARIANCE",
            "VAR_POP",
            "VAR_SAMP",
            "MEDIAN",
            "MODE",
            "FIRST",
            "LAST",
            "ANY_VALUE",
            "APPROX_DISTINCT",
            "APPROX_COUNT_DISTINCT",
            "BIT_AND",
            "BIT_OR",
            "BIT_XOR",
            "STRING_AGG",
            "GROUP_CONCAT",
            "LISTAGG",
            "ROW_NUMBER",
            "RANK",
            "DENSE_RANK",
            "PERCENT_RANK",
            "CUME_DIST",
            "NTILE",
            "LEAD",
            "LAG",
            "FIRST_VALUE",
            "LAST_VALUE",
            "NTH_VALUE",
            "JSON_EXTRACT",
            "JSON_EXTRACT_SCALAR",
            "JSON_QUERY",
            "JSON_VALUE",
            "JSON_ARRAY_LENGTH",
            "JSON_KEYS",
            "JSON_TYPE",
            "TO_JSON",
            "PARSE_JSON",
            "JSON_OBJECT",
            "JSON_ARRAY",
            "JSON_ARRAYAGG",
            "JSON_OBJECTAGG",
            "JSON_TABLE",
            "TRANSLATE",
        ];

        for name in phase6_names {
            let needle = format!("\"{name}\"");
            assert!(
                !parse_block.contains(&needle),
                "phase-6 family name '{name}' must not be directly handled in parse_typed_function"
            );
        }
    }

    #[test]
    fn phase6_registry_helpers_are_wired_from_registry_dispatch() {
        let parser_src = include_str!("parser.rs");
        let registry_start = parser_src
            .find("fn try_parse_registry_typed_function(")
            .expect("try_parse_registry_typed_function start not found");
        let registry_end = parser_src[registry_start..]
            .find("fn try_parse_registry_grouped_typed_family(")
            .map(|idx| registry_start + idx)
            .expect("phase6 helper start not found");
        let registry_block = &parser_src[registry_start..registry_end];

        let helper_calls = ["try_parse_registry_grouped_typed_family("];

        for helper in helper_calls {
            assert!(
                registry_block.contains(helper),
                "registry dispatch must invoke helper '{helper}'"
            );
        }

        let helper_start = parser_src
            .find("fn try_parse_registry_grouped_typed_family(")
            .expect("phase6 helper start not found");
        let helper_end = parser_src[helper_start..]
            .find("fn make_unquoted_function(")
            .map(|idx| helper_start + idx)
            .expect("make_unquoted_function start not found");
        let helper_block = &parser_src[helper_start..helper_end];

        assert!(
            helper_block.contains("typed_dispatch_group_by_name_upper(canonical_upper_name)"),
            "phase-6 helper must route via typed dispatch-group metadata"
        );
        assert!(
            helper_block.contains("parse_typed_aggregate_family("),
            "phase-6 helper must call aggregate family parser"
        );
        assert!(
            helper_block.contains("parse_typed_window_family("),
            "phase-6 helper must call window family parser"
        );
        assert!(
            helper_block.contains("parse_typed_json_family("),
            "phase-6 helper must call json family parser"
        );
        assert!(
            helper_block.contains("parse_typed_translate_teradata_family("),
            "phase-6 helper must call teradata translate family parser"
        );
    }
}
