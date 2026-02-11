//! DuckDB Dialect
//!
//! DuckDB-specific transformations based on sqlglot patterns.
//! Key features:
//! - Modern SQL analytics database with PostgreSQL-like syntax
//! - LIST type for arrays
//! - STRUCT support with dot access
//! - EPOCH_MS / EPOCH for timestamps
//! - EXCLUDE / REPLACE in SELECT
//! - Rich array/list functions

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{Alias, BinaryOp, Case, Cast, CeilFunc, Column, DataType, Expression, Function, FunctionParameter, Identifier, Interval, IntervalUnit, IntervalUnitSpec, JsonExtractFunc, JSONPath, JSONPathKey, JSONPathRoot, JSONPathSubscript, Literal, Paren, Struct, Subquery, UnaryFunc, VarArgFunc, WindowFunction};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Normalize a JSON path for DuckDB arrow syntax.
/// Converts string keys like 'foo' to '$.foo' and numeric indexes like 0 to '$[0]'.
/// This matches Python sqlglot's to_json_path() behavior.
fn normalize_json_path(path: Expression) -> Expression {
    match &path {
        // String literal: 'foo' -> JSONPath with $.foo
        Expression::Literal(Literal::String(s)) => {
            // Skip paths that are already normalized (start with $ or /)
            // Also skip JSON pointer syntax and back-of-list syntax [#-i]
            if s.starts_with('$') || s.starts_with('/') || s.contains("[#") {
                return path;
            }
            // Create JSONPath expression: $.key
            Expression::JSONPath(Box::new(JSONPath {
                expressions: vec![
                    Expression::JSONPathRoot(JSONPathRoot),
                    Expression::JSONPathKey(Box::new(JSONPathKey {
                        this: Box::new(Expression::Literal(Literal::String(s.clone()))),
                    })),
                ],
                escape: None,
            }))
        }
        // Number literal: 0 -> JSONPath with $[0]
        Expression::Literal(Literal::Number(n)) => {
            // Create JSONPath expression: $[n]
            Expression::JSONPath(Box::new(JSONPath {
                expressions: vec![
                    Expression::JSONPathRoot(JSONPathRoot),
                    Expression::JSONPathSubscript(Box::new(JSONPathSubscript {
                        this: Box::new(Expression::Literal(Literal::Number(n.clone()))),
                    })),
                ],
                escape: None,
            }))
        }
        // Already a JSONPath or other expression - return as is
        _ => path,
    }
}

/// Helper to wrap JSON arrow expressions in parentheses when they appear
/// in contexts that require it (Binary, In, Not expressions)
/// This matches Python sqlglot's WRAPPED_JSON_EXTRACT_EXPRESSIONS behavior
fn wrap_if_json_arrow(expr: Expression) -> Expression {
    match &expr {
        Expression::JsonExtract(f) if f.arrow_syntax => {
            Expression::Paren(Box::new(Paren {
                this: expr,
                trailing_comments: Vec::new(),
            }))
        }
        Expression::JsonExtractScalar(f) if f.arrow_syntax => {
            Expression::Paren(Box::new(Paren {
                this: expr,
                trailing_comments: Vec::new(),
            }))
        }
        _ => expr,
    }
}

/// DuckDB dialect
pub struct DuckDBDialect;

impl DialectImpl for DuckDBDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::DuckDB
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // DuckDB uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // DuckDB supports nested comments
        config.nested_comments = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::DuckDB),
            // DuckDB-specific settings from Python sqlglot
            parameter_token: "$",
            named_placeholder_token: "$",
            join_hints: false,
            table_hints: false,
            query_hints: false,
            limit_fetch_style: crate::generator::LimitFetchStyle::Limit,
            struct_delimiter: ("(", ")"),
            rename_table_with_db: false,
            nvl2_supported: false,
            semi_anti_join_with_side: false,
            tablesample_keywords: "TABLESAMPLE",
            tablesample_seed_keyword: "REPEATABLE",
            last_day_supports_date_part: false,
            json_key_value_pair_sep: ",",
            ignore_nulls_in_func: true,
            json_path_bracketed_key_supported: false,
            supports_create_table_like: false,
            multi_arg_distinct: false,
            quantified_no_paren_space: true,
            can_implement_array_any: true,
            supports_to_number: false,
            supports_window_exclude: true,
            copy_has_into_keyword: false,
            star_except: "EXCLUDE",
            pad_fill_pattern_is_required: true,
            array_concat_is_var_len: false,
            array_size_dim_required: None,
            normalize_extract_date_parts: true,
            supports_like_quantifiers: false,
            // DuckDB supports TRY_CAST
            try_supported: true,
            // DuckDB uses curly brace notation for struct literals: {'a': 1}
            struct_curly_brace_notation: true,
            // DuckDB uses bracket-only notation for arrays: [1, 2, 3]
            array_bracket_only: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // ===== Data Type Mappings =====
            Expression::DataType(dt) => self.transform_data_type(dt),

            // ===== Operator transformations =====
            // BitwiseXor -> XOR() function in DuckDB
            Expression::BitwiseXor(op) => Ok(Expression::Function(Box::new(crate::expressions::Function::new(
                "XOR",
                vec![op.left, op.right],
            )))),

            // ===== Array/List syntax =====
            // ARRAY[1, 2, 3] -> [1, 2, 3] in DuckDB (bracket notation preferred)
            Expression::ArrayFunc(mut f) => {
                f.bracket_notation = true;
                Ok(Expression::ArrayFunc(f))
            }

            // IFNULL -> COALESCE in DuckDB
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE in DuckDB
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Coalesce with original_name (e.g., IFNULL parsed as Coalesce) -> clear original_name
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // GROUP_CONCAT -> LISTAGG in DuckDB
            Expression::GroupConcat(f) => Ok(Expression::ListAgg(Box::new(crate::expressions::ListAggFunc {
                this: f.this,
                separator: f.separator,
                on_overflow: None,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
            }))),

            // LISTAGG is native in DuckDB - keep as-is
            Expression::ListAgg(f) => Ok(Expression::ListAgg(f)),

            // STRING_AGG -> LISTAGG in DuckDB (normalize to LISTAGG)
            Expression::StringAgg(f) => Ok(Expression::ListAgg(Box::new(crate::expressions::ListAggFunc {
                this: f.this,
                separator: f.separator,
                on_overflow: None,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
            }))),

            // TryCast -> TRY_CAST (DuckDB supports TRY_CAST)
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TRY_CAST in DuckDB
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // ILIKE is native to DuckDB (PostgreSQL-compatible)
            Expression::ILike(op) => Ok(Expression::ILike(op)),

            // EXPLODE -> UNNEST in DuckDB
            Expression::Explode(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // UNNEST is native to DuckDB
            Expression::Unnest(f) => Ok(Expression::Unnest(f)),

            // ArrayContainedBy (<@) -> ArrayContainsAll (@>) with swapped operands
            // a <@ b becomes b @> a
            Expression::ArrayContainedBy(op) => Ok(Expression::ArrayContainsAll(Box::new(BinaryOp {
                left: op.right,
                right: op.left,
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }))),

            // DATE_ADD -> date + INTERVAL in DuckDB
            Expression::DateAdd(f) => {
                // Reconstruct INTERVAL expression from value and unit
                let interval_expr = if matches!(&f.interval, Expression::Interval(_)) {
                    f.interval
                } else {
                    Expression::Interval(Box::new(Interval {
                        this: Some(f.interval),
                        unit: Some(IntervalUnitSpec::Simple { unit: f.unit, use_plural: false }),
                    }))
                };
                Ok(Expression::Add(Box::new(BinaryOp {
                    left: f.this,
                    right: interval_expr,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // DATE_SUB -> date - INTERVAL in DuckDB
            Expression::DateSub(f) => {
                // Reconstruct INTERVAL expression from value and unit
                let interval_expr = if matches!(&f.interval, Expression::Interval(_)) {
                    f.interval
                } else {
                    Expression::Interval(Box::new(Interval {
                        this: Some(f.interval),
                        unit: Some(IntervalUnitSpec::Simple { unit: f.unit, use_plural: false }),
                    }))
                };
                Ok(Expression::Sub(Box::new(BinaryOp {
                    left: f.this,
                    right: interval_expr,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // GenerateSeries with 1 arg -> GENERATE_SERIES(0, n)
            Expression::GenerateSeries(mut f) => {
                // If only end is set (no start), add 0 as start
                if f.start.is_none() && f.end.is_some() {
                    f.start = Some(Box::new(Expression::number(0)));
                }
                Ok(Expression::GenerateSeries(f))
            }

            // ===== Array/List functions =====
            // ArrayAppend -> LIST_APPEND
            Expression::ArrayAppend(f) => Ok(Expression::Function(Box::new(Function::new(
                "LIST_APPEND".to_string(),
                vec![f.this, f.expression],
            )))),

            // ArrayPrepend -> LIST_PREPEND
            Expression::ArrayPrepend(f) => Ok(Expression::Function(Box::new(Function::new(
                "LIST_PREPEND".to_string(),
                vec![f.this, f.expression],
            )))),

            // ArrayUniqueAgg -> LIST
            Expression::ArrayUniqueAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "LIST".to_string(),
                vec![f.this],
            )))),

            // Split -> STR_SPLIT
            Expression::Split(f) => Ok(Expression::Function(Box::new(Function::new(
                "STR_SPLIT".to_string(),
                vec![f.this, f.delimiter],
            )))),

            // RANDOM is native to DuckDB
            Expression::Random(_) => Ok(Expression::Random(crate::expressions::Random)),

            // Rand with seed -> keep as Rand so NORMAL/UNIFORM handlers can extract the seed
            // Rand without seed -> Random
            Expression::Rand(r) => {
                if r.seed.is_some() {
                    Ok(Expression::Rand(r))
                } else {
                    Ok(Expression::Random(crate::expressions::Random))
                }
            }

            // ===== Boolean aggregates =====
            // LogicalAnd -> BOOL_AND with CAST to BOOLEAN
            Expression::LogicalAnd(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOL_AND".to_string(),
                vec![Expression::Cast(Box::new(crate::expressions::Cast {
                    this: f.this,
                    to: crate::expressions::DataType::Boolean,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }))],
            )))),

            // LogicalOr -> BOOL_OR with CAST to BOOLEAN
            Expression::LogicalOr(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOL_OR".to_string(),
                vec![Expression::Cast(Box::new(crate::expressions::Cast {
                    this: f.this,
                    to: crate::expressions::DataType::Boolean,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }))],
            )))),

            // ===== Approximate functions =====
            // ApproxDistinct -> APPROX_COUNT_DISTINCT
            Expression::ApproxDistinct(f) => Ok(Expression::Function(Box::new(Function::new(
                "APPROX_COUNT_DISTINCT".to_string(),
                vec![f.this],
            )))),

            // ===== Variance =====
            // VarPop -> VAR_POP
            Expression::VarPop(f) => Ok(Expression::Function(Box::new(Function::new(
                "VAR_POP".to_string(),
                vec![f.this],
            )))),

            // ===== Date/time functions =====
            // DayOfMonth -> DAYOFMONTH
            Expression::DayOfMonth(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFMONTH".to_string(),
                vec![f.this],
            )))),

            // DayOfWeek -> DAYOFWEEK
            Expression::DayOfWeek(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFWEEK".to_string(),
                vec![f.this],
            )))),

            // DayOfWeekIso -> ISODOW
            Expression::DayOfWeekIso(f) => Ok(Expression::Function(Box::new(Function::new(
                "ISODOW".to_string(),
                vec![f.this],
            )))),

            // DayOfYear -> DAYOFYEAR
            Expression::DayOfYear(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFYEAR".to_string(),
                vec![f.this],
            )))),

            // WeekOfYear -> WEEKOFYEAR
            Expression::WeekOfYear(f) => Ok(Expression::Function(Box::new(Function::new(
                "WEEKOFYEAR".to_string(),
                vec![f.this],
            )))),

            // ===== Time conversion functions =====
            // TimeStrToUnix -> EPOCH
            Expression::TimeStrToUnix(f) => Ok(Expression::Function(Box::new(Function::new(
                "EPOCH".to_string(),
                vec![f.this],
            )))),

            // TimeToUnix -> EPOCH
            Expression::TimeToUnix(f) => Ok(Expression::Function(Box::new(Function::new(
                "EPOCH".to_string(),
                vec![f.this],
            )))),

            // UnixMicros -> EPOCH_US
            Expression::UnixMicros(f) => Ok(Expression::Function(Box::new(Function::new(
                "EPOCH_US".to_string(),
                vec![f.this],
            )))),

            // UnixMillis -> EPOCH_MS
            Expression::UnixMillis(f) => Ok(Expression::Function(Box::new(Function::new(
                "EPOCH_MS".to_string(),
                vec![f.this],
            )))),

            // TimestampDiff -> DATE_DIFF
            Expression::TimestampDiff(f) => Ok(Expression::Function(Box::new(Function::new(
                "DATE_DIFF".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ===== Hash functions =====
            // SHA -> SHA1
            Expression::SHA(f) => Ok(Expression::Function(Box::new(Function::new(
                "SHA1".to_string(),
                vec![f.this],
            )))),

            // MD5Digest -> UNHEX(MD5(...))
            Expression::MD5Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "UNHEX".to_string(),
                vec![*f.this],
            )))),

            // SHA1Digest -> UNHEX
            Expression::SHA1Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "UNHEX".to_string(),
                vec![f.this],
            )))),

            // SHA2Digest -> UNHEX
            Expression::SHA2Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "UNHEX".to_string(),
                vec![*f.this],
            )))),

            // ===== Vector/Distance functions =====
            // CosineDistance -> LIST_COSINE_DISTANCE
            Expression::CosineDistance(f) => Ok(Expression::Function(Box::new(Function::new(
                "LIST_COSINE_DISTANCE".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // EuclideanDistance -> LIST_DISTANCE
            Expression::EuclideanDistance(f) => Ok(Expression::Function(Box::new(Function::new(
                "LIST_DISTANCE".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ===== Numeric checks =====
            // IsInf -> ISINF
            Expression::IsInf(f) => Ok(Expression::Function(Box::new(Function::new(
                "ISINF".to_string(),
                vec![f.this],
            )))),

            // IsNan -> ISNAN
            Expression::IsNan(f) => Ok(Expression::Function(Box::new(Function::new(
                "ISNAN".to_string(),
                vec![f.this],
            )))),

            // ===== Pattern matching =====
            // RegexpLike (~) -> REGEXP_FULL_MATCH in DuckDB
            Expression::RegexpLike(f) => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_FULL_MATCH".to_string(),
                vec![f.this, f.pattern],
            )))),


            // ===== Time functions =====
            // CurrentTime -> CURRENT_TIME (no parens in DuckDB)
            Expression::CurrentTime(_) => Ok(Expression::Function(Box::new(Function {
                name: "CURRENT_TIME".to_string(),
                args: vec![],
                distinct: false,
                trailing_comments: vec![],
                use_bracket_syntax: false,
                no_parens: true,
                quoted: false,
            }))),

            // ===== Return statement =====
            // ReturnStmt -> just output the inner expression
            Expression::ReturnStmt(e) => Ok(*e),

            // ===== DDL Column Constraints =====
            // CommentColumnConstraint -> ignored (DuckDB doesn't support column comments this way)
            Expression::CommentColumnConstraint(_) => Ok(Expression::Literal(
                crate::expressions::Literal::String(String::new()),
            )),

            // JsonExtract -> use arrow syntax (->) in DuckDB with normalized JSON path
            Expression::JsonExtract(mut f) => {
                f.arrow_syntax = true;
                f.path = normalize_json_path(f.path);
                Ok(Expression::JsonExtract(f))
            }

            // JsonExtractScalar -> use arrow syntax (->>) in DuckDB with normalized JSON path
            Expression::JsonExtractScalar(mut f) => {
                f.arrow_syntax = true;
                f.path = normalize_json_path(f.path);
                Ok(Expression::JsonExtractScalar(f))
            }

            // CARDINALITY -> ARRAY_LENGTH in DuckDB
            Expression::Cardinality(f) => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_LENGTH".to_string(),
                vec![f.this],
            )))),

            // ADD_MONTHS(date, n) -> convert to Function and handle in transform_function
            Expression::AddMonths(f) => {
                let func = Function::new("ADD_MONTHS".to_string(), vec![f.this, f.expression]);
                self.transform_function(func)
            }

            // NEXT_DAY(date, day) -> convert to Function and handle in transform_function
            Expression::NextDay(f) => {
                let func = Function::new("NEXT_DAY".to_string(), vec![f.this, f.expression]);
                self.transform_function(func)
            }

            // LAST_DAY(date, unit) -> convert to Function and handle in transform_function
            Expression::LastDay(f) => {
                if let Some(unit) = f.unit {
                    let unit_str = match unit {
                        crate::expressions::DateTimeField::Year => "YEAR",
                        crate::expressions::DateTimeField::Month => "MONTH",
                        crate::expressions::DateTimeField::Quarter => "QUARTER",
                        crate::expressions::DateTimeField::Week => "WEEK",
                        crate::expressions::DateTimeField::Day => "DAY",
                        _ => "MONTH",
                    };
                    let func = Function::new("LAST_DAY".to_string(), vec![f.this, Expression::Identifier(Identifier { name: unit_str.to_string(), quoted: false, trailing_comments: Vec::new() })]);
                    self.transform_function(func)
                } else {
                    // Single arg LAST_DAY - pass through
                    Ok(Expression::Function(Box::new(Function::new("LAST_DAY".to_string(), vec![f.this]))))
                }
            }

            // DAYNAME(expr) -> STRFTIME(expr, '%a')
            Expression::Dayname(d) => {
                Ok(Expression::Function(Box::new(Function::new("STRFTIME".to_string(), vec![
                    *d.this,
                    Expression::Literal(Literal::String("%a".to_string())),
                ]))))
            }

            // MONTHNAME(expr) -> STRFTIME(expr, '%b')
            Expression::Monthname(d) => {
                Ok(Expression::Function(Box::new(Function::new("STRFTIME".to_string(), vec![
                    *d.this,
                    Expression::Literal(Literal::String("%b".to_string())),
                ]))))
            }

            // FLOOR(x, scale) -> ROUND(FLOOR(x * POWER(10, scale)) / POWER(10, scale), scale)
            Expression::Floor(f) if f.scale.is_some() => {
                let x = f.this;
                let scale = f.scale.unwrap();
                let needs_cast = match &scale {
                    Expression::Literal(Literal::Number(n)) => n.contains('.'),
                    _ => false,
                };
                let int_scale = if needs_cast {
                    Expression::Cast(Box::new(Cast { this: scale.clone(), to: DataType::Int { length: None, integer_spelling: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))
                } else {
                    scale.clone()
                };
                let power_10 = Expression::Function(Box::new(Function::new("POWER".to_string(), vec![Expression::number(10), int_scale.clone()])));
                let x_paren = match &x { Expression::Add(_) | Expression::Sub(_) | Expression::Mul(_) | Expression::Div(_) => Expression::Paren(Box::new(Paren { this: x, trailing_comments: Vec::new() })), _ => x };
                let multiplied = Expression::Mul(Box::new(BinaryOp { left: x_paren, right: power_10.clone(), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                let floored = Expression::Function(Box::new(Function::new("FLOOR".to_string(), vec![multiplied])));
                let divided = Expression::Div(Box::new(BinaryOp { left: floored, right: power_10, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![divided, int_scale]))))
            }

            // CEIL(x, scale) -> ROUND(CEIL(x * POWER(10, scale)) / POWER(10, scale), scale)
            Expression::Ceil(f) if f.decimals.is_some() => {
                let x = f.this;
                let scale = f.decimals.unwrap();
                let needs_cast = match &scale {
                    Expression::Literal(Literal::Number(n)) => n.contains('.'),
                    _ => false,
                };
                let int_scale = if needs_cast {
                    Expression::Cast(Box::new(Cast { this: scale.clone(), to: DataType::Int { length: None, integer_spelling: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))
                } else {
                    scale.clone()
                };
                let power_10 = Expression::Function(Box::new(Function::new("POWER".to_string(), vec![Expression::number(10), int_scale.clone()])));
                let x_paren = match &x { Expression::Add(_) | Expression::Sub(_) | Expression::Mul(_) | Expression::Div(_) => Expression::Paren(Box::new(Paren { this: x, trailing_comments: Vec::new() })), _ => x };
                let multiplied = Expression::Mul(Box::new(BinaryOp { left: x_paren, right: power_10.clone(), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                let ceiled = Expression::Function(Box::new(Function::new("CEIL".to_string(), vec![multiplied])));
                let divided = Expression::Div(Box::new(BinaryOp { left: ceiled, right: power_10, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![divided, int_scale]))))
            }

            // ParseJson: handled by generator (outputs JSON() for DuckDB)

            // TABLE(GENERATOR(ROWCOUNT => n)) -> RANGE(n) in DuckDB
            // The TABLE() wrapper around GENERATOR is parsed as TableArgument
            Expression::TableArgument(ta) if ta.prefix.to_uppercase() == "TABLE" => {
                // Check if inner is a GENERATOR or RANGE function
                match ta.this {
                    Expression::Function(ref f) if f.name.to_uppercase() == "RANGE" => {
                        // Already converted to RANGE, unwrap TABLE()
                        Ok(ta.this)
                    }
                    Expression::Function(ref f) if f.name.to_uppercase() == "GENERATOR" => {
                        // GENERATOR(ROWCOUNT => n) -> RANGE(n)
                        let mut rowcount = None;
                        for arg in &f.args {
                            if let Expression::NamedArgument(na) = arg {
                                if na.name.name.to_uppercase() == "ROWCOUNT" {
                                    rowcount = Some(na.value.clone());
                                }
                            }
                        }
                        if let Some(n) = rowcount {
                            Ok(Expression::Function(Box::new(Function::new("RANGE".to_string(), vec![n]))))
                        } else {
                            Ok(Expression::TableArgument(ta))
                        }
                    }
                    _ => Ok(Expression::TableArgument(ta)),
                }
            }

            // JSONExtract (variant_extract/colon accessor) -> arrow syntax in DuckDB
            Expression::JSONExtract(e) if e.variant_extract.is_some() => {
                let path = match *e.expression {
                    Expression::Literal(Literal::String(s)) => {
                        // Convert bracket notation ["key"] to quoted dot notation ."key"
                        let s = Self::convert_bracket_to_quoted_path(&s);
                        let normalized = if s.starts_with('$') {
                            s
                        } else if s.starts_with('[') {
                            format!("${}", s)
                        } else {
                            format!("$.{}", s)
                        };
                        Expression::Literal(Literal::String(normalized))
                    }
                    other => other,
                };
                Ok(Expression::JsonExtract(Box::new(JsonExtractFunc {
                    this: *e.this,
                    path,
                    returning: None,
                    arrow_syntax: true,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

            // X'ABCD' -> UNHEX('ABCD') in DuckDB
            Expression::Literal(Literal::HexString(s)) => {
                Ok(Expression::Function(Box::new(Function::new(
                    "UNHEX".to_string(),
                    vec![Expression::Literal(Literal::String(s))],
                ))))
            }

            // b'a' -> CAST(e'a' AS BLOB) in DuckDB
            Expression::Literal(Literal::ByteString(s)) => {
                Ok(Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::EscapeString(s)),
                    to: DataType::VarBinary { length: None },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // CAST(x AS DECIMAL) -> CAST(x AS DECIMAL(18, 3)) in DuckDB (default precision)
            // Exception: CAST(a // b AS DECIMAL) from DIV conversion keeps bare DECIMAL
            Expression::Cast(mut c) => {
                if matches!(&c.to, DataType::Decimal { precision: None, .. })
                    && !matches!(&c.this, Expression::IntDiv(_))
                {
                    c.to = DataType::Decimal {
                        precision: Some(18),
                        scale: Some(3),
                    };
                }
                let transformed_this = self.transform_expr(c.this)?;
                c.this = transformed_this;
                Ok(Expression::Cast(c))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // WindowFunction with CASE-wrapped CORR: re-wrap so OVER is inside CASE
            // Pattern: WindowFunction { this: CASE(ISNAN(CORR), NULL, CORR), over }
            // Expected: CASE(ISNAN(WindowFunction(CORR, over)), NULL, WindowFunction(CORR, over))
            Expression::WindowFunction(wf) => {
                if let Expression::Case(case_box) = wf.this {
                    let case = *case_box;
                    // Detect the ISNAN(CORR) -> NULL pattern
                    if case.whens.len() == 1 && matches!(&case.else_, Some(Expression::AggregateFunction(ref af)) if af.name.to_uppercase() == "CORR") {
                        // Re-wrap: put the OVER on each CORR inside the CASE
                        let over = wf.over;
                        let new_else = case.else_.map(|e| {
                            Expression::WindowFunction(Box::new(WindowFunction { this: e, over: over.clone(), keep: None }))
                        });
                        let new_whens = case.whens.into_iter().map(|(when_cond, when_result)| {
                            // wrap the ISNAN arg (which is CORR) with OVER
                            let new_cond = if let Expression::Function(func) = when_cond {
                                if func.name.to_uppercase() == "ISNAN" && func.args.len() == 1 {
                                    let inner = func.args.into_iter().next().unwrap();
                                    let windowed = Expression::WindowFunction(Box::new(WindowFunction { this: inner, over: over.clone(), keep: None }));
                                    Expression::Function(Box::new(Function::new("ISNAN".to_string(), vec![windowed])))
                                } else {
                                    Expression::Function(func)
                                }
                            } else {
                                when_cond
                            };
                            (new_cond, when_result)
                        }).collect();
                        Ok(Expression::Case(Box::new(Case { operand: None, whens: new_whens, else_: new_else })))
                    } else {
                        Ok(Expression::WindowFunction(Box::new(WindowFunction { this: Expression::Case(Box::new(case)), over: wf.over, keep: wf.keep })))
                    }
                } else {
                    Ok(Expression::WindowFunction(wf))
                }
            }

            // ===== Context-aware JSON arrow wrapping =====
            // When JSON arrow expressions appear in Binary/In/Not contexts,
            // they need to be wrapped in parentheses for correct precedence.
            // This matches Python sqlglot's WRAPPED_JSON_EXTRACT_EXPRESSIONS behavior.

            // Binary operators that need JSON wrapping
            Expression::Eq(op) => Ok(Expression::Eq(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Neq(op) => Ok(Expression::Neq(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Lt(op) => Ok(Expression::Lt(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Lte(op) => Ok(Expression::Lte(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Gt(op) => Ok(Expression::Gt(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Gte(op) => Ok(Expression::Gte(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::And(op) => Ok(Expression::And(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Or(op) => Ok(Expression::Or(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Add(op) => Ok(Expression::Add(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Sub(op) => Ok(Expression::Sub(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Mul(op) => Ok(Expression::Mul(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Div(op) => Ok(Expression::Div(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Mod(op) => Ok(Expression::Mod(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),
            Expression::Concat(op) => Ok(Expression::Concat(Box::new(BinaryOp {
                left: wrap_if_json_arrow(op.left),
                right: wrap_if_json_arrow(op.right),
                ..*op
            }))),

            // In expression - wrap the this part if it's JSON arrow
            // Also transform `expr NOT IN (list)` to `NOT (expr) IN (list)` for DuckDB
            Expression::In(mut i) => {
                i.this = wrap_if_json_arrow(i.this);
                if i.not {
                    // Transform `expr NOT IN (list)` to `NOT (expr) IN (list)`
                    i.not = false;
                    Ok(Expression::Not(Box::new(crate::expressions::UnaryOp {
                        this: Expression::In(i),
                    })))
                } else {
                    Ok(Expression::In(i))
                }
            }

            // Not expression - wrap the this part if it's JSON arrow
            Expression::Not(mut n) => {
                n.this = wrap_if_json_arrow(n.this);
                Ok(Expression::Not(n))
            }

            // WithinGroup: PERCENTILE_CONT/DISC WITHIN GROUP (ORDER BY ...) -> QUANTILE_CONT/DISC(col, quantile ORDER BY ...)
            Expression::WithinGroup(wg) => {
                match &wg.this {
                    Expression::PercentileCont(p) => {
                        let column = wg.order_by.first().map(|o| o.this.clone()).unwrap_or_else(|| p.this.clone());
                        let percentile = p.percentile.clone();
                        let filter = p.filter.clone();
                        Ok(Expression::AggregateFunction(Box::new(crate::expressions::AggregateFunction {
                            name: "QUANTILE_CONT".to_string(),
                            args: vec![column, percentile],
                            distinct: false,
                            filter,
                            order_by: wg.order_by,
                            limit: None,
                            ignore_nulls: None,
                        })))
                    }
                    Expression::PercentileDisc(p) => {
                        let column = wg.order_by.first().map(|o| o.this.clone()).unwrap_or_else(|| p.this.clone());
                        let percentile = p.percentile.clone();
                        let filter = p.filter.clone();
                        Ok(Expression::AggregateFunction(Box::new(crate::expressions::AggregateFunction {
                            name: "QUANTILE_DISC".to_string(),
                            args: vec![column, percentile],
                            distinct: false,
                            filter,
                            order_by: wg.order_by,
                            limit: None,
                            ignore_nulls: None,
                        })))
                    }
                    // Handle case where inner is AggregateFunction with PERCENTILE_CONT/DISC name
                    Expression::AggregateFunction(af) if af.name == "PERCENTILE_CONT" || af.name == "PERCENTILE_DISC" => {
                        let new_name = if af.name == "PERCENTILE_CONT" { "QUANTILE_CONT" } else { "QUANTILE_DISC" };
                        let column = wg.order_by.first().map(|o| o.this.clone());
                        let quantile = af.args.first().cloned();
                        match (column, quantile) {
                            (Some(col), Some(q)) => {
                                Ok(Expression::AggregateFunction(Box::new(crate::expressions::AggregateFunction {
                                    name: new_name.to_string(),
                                    args: vec![col, q],
                                    distinct: false,
                                    filter: af.filter.clone(),
                                    order_by: wg.order_by,
                                    limit: None,
                                    ignore_nulls: None,
                                })))
                            }
                            _ => Ok(Expression::WithinGroup(wg)),
                        }
                    }
                    _ => Ok(Expression::WithinGroup(wg)),
                }
            }

            // ===== DuckDB @ prefix operator → ABS() =====
            // In DuckDB, @expr means ABS(expr)
            // Parser creates Column with name "@col" — strip the @ and wrap in ABS()
            Expression::Column(ref c) if c.name.name.starts_with('@') && c.table.is_none() => {
                let col_name = &c.name.name[1..]; // strip leading @
                Ok(Expression::Abs(Box::new(UnaryFunc {
                    this: Expression::Column(Column {
                        name: Identifier::new(col_name),
                        table: None,
                        join_mark: false,
                        trailing_comments: Vec::new(),
                    }),
                    original_name: None,
                })))
            }

            // ===== SELECT-level transforms =====
            // DuckDB colon alias syntax: `foo: bar` → `bar AS foo`
            // Parser creates JSONExtract(this=foo, expression='bar', variant_extract=true)
            // which needs to become Alias(this=Column(bar), alias=foo)
            Expression::Select(mut select) => {
                select.expressions = select.expressions.into_iter().map(|e| {
                    match e {
                        Expression::JSONExtract(ref je) if je.variant_extract.is_some() => {
                            // JSONExtract(this=alias_name, expression='value', variant_extract=true) → value AS alias_name
                            let alias_ident = match je.this.as_ref() {
                                Expression::Identifier(ident) => Some(ident.clone()),
                                Expression::Column(col) if col.table.is_none() => Some(col.name.clone()),
                                _ => None,
                            };
                            let value_expr = match je.expression.as_ref() {
                                Expression::Literal(Literal::String(s)) => {
                                    // Convert string path to column reference
                                    if s.contains('.') {
                                        // t.col → Column { name: col, table: t }
                                        let parts: Vec<&str> = s.splitn(2, '.').collect();
                                        Some(Expression::Column(Column {
                                            name: Identifier::new(parts[1]),
                                            table: Some(Identifier::new(parts[0])),
                                            join_mark: false,
                                            trailing_comments: Vec::new(),
                                        }))
                                    } else {
                                        Some(Expression::Column(Column {
                                            name: Identifier::new(s.as_str()),
                                            table: None,
                                            join_mark: false,
                                            trailing_comments: Vec::new(),
                                        }))
                                    }
                                }
                                _ => None,
                            };

                            if let (Some(alias), Some(value)) = (alias_ident, value_expr) {
                                Expression::Alias(Box::new(Alias {
                                    this: value,
                                    alias,
                                    column_aliases: Vec::new(),
                                    pre_alias_comments: Vec::new(),
                                    trailing_comments: Vec::new(),
                                }))
                            } else {
                                e
                            }
                        }
                        _ => e,
                    }
                }).collect();

                // ===== DuckDB comma-join with UNNEST → JOIN ON TRUE =====
                // Transform FROM t1, UNNEST(...) AS t2 → FROM t1 JOIN UNNEST(...) AS t2 ON TRUE
                if let Some(ref mut from) = select.from {
                    if from.expressions.len() > 1 {
                        // Check if any expression after the first is UNNEST or Alias wrapping UNNEST
                        let mut new_from_exprs = Vec::new();
                        let mut new_joins = Vec::new();

                        for (idx, expr) in from.expressions.drain(..).enumerate() {
                            if idx == 0 {
                                // First expression stays in FROM
                                new_from_exprs.push(expr);
                            } else {
                                // Check if this is UNNEST or Alias(UNNEST)
                                let is_unnest = match &expr {
                                    Expression::Unnest(_) => true,
                                    Expression::Alias(a) => matches!(a.this, Expression::Unnest(_)),
                                    _ => false,
                                };

                                if is_unnest {
                                    // Convert to JOIN ON TRUE
                                    new_joins.push(crate::expressions::Join {
                                        this: expr,
                                        on: Some(Expression::Boolean(crate::expressions::BooleanLiteral { value: true })),
                                        using: Vec::new(),
                                        kind: crate::expressions::JoinKind::Inner,
                                        use_inner_keyword: false,
                                        use_outer_keyword: false,
                                        deferred_condition: false,
                                        join_hint: None,
                                        match_condition: None,
                                        pivots: Vec::new(),
                                    });
                                } else {
                                    // Keep non-UNNEST expressions in FROM (comma-separated)
                                    new_from_exprs.push(expr);
                                }
                            }
                        }

                        from.expressions = new_from_exprs;

                        // Prepend the new joins before any existing joins
                        new_joins.append(&mut select.joins);
                        select.joins = new_joins;
                    }
                }

                Ok(Expression::Select(select))
            }

            // ===== INTERVAL splitting =====
            // DuckDB requires INTERVAL '1' HOUR format, not INTERVAL '1 hour'
            // When we have INTERVAL 'value unit' (single string with embedded unit),
            // split it into INTERVAL 'value' UNIT
            Expression::Interval(interval) => {
                self.transform_interval(*interval)
            }

            // DuckDB CREATE FUNCTION (macro syntax): strip param types, suppress RETURNS
            Expression::CreateFunction(mut cf) => {
                // Strip parameter data types (DuckDB macros don't use types)
                cf.parameters = cf.parameters.into_iter().map(|p| {
                    FunctionParameter {
                        name: p.name,
                        data_type: DataType::Custom { name: String::new() },
                        mode: None,
                        default: p.default,
                    }
                }).collect();

                // For DuckDB macro syntax: suppress RETURNS output
                // Use a marker in returns_table_body to signal TABLE keyword in body
                let was_table_return = cf.returns_table_body.is_some()
                    || matches!(&cf.return_type, Some(DataType::Custom { ref name }) if name == "TABLE");
                cf.return_type = None;
                if was_table_return {
                    // Use empty marker to signal TABLE return without outputting RETURNS
                    cf.returns_table_body = Some(String::new());
                } else {
                    cf.returns_table_body = None;
                }

                Ok(Expression::CreateFunction(cf))
            }

            // ===== Snowflake-specific expression type transforms =====

            // IFF(cond, true_val, false_val) -> CASE WHEN cond THEN true_val ELSE false_val END
            Expression::IfFunc(f) => {
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.condition, f.true_value)],
                    else_: f.false_value,
                })))
            }

            // VAR_SAMP -> VARIANCE in DuckDB
            Expression::VarSamp(f) => Ok(Expression::Function(Box::new(Function::new(
                "VARIANCE".to_string(),
                vec![f.this],
            )))),

            // NVL2(expr, val_if_not_null, val_if_null) -> CASE WHEN expr IS NOT NULL THEN val_if_not_null ELSE val_if_null END
            Expression::Nvl2(f) => {
                let condition = Expression::IsNull(Box::new(crate::expressions::IsNull {
                    this: f.this,
                    not: true,
                    postfix_form: false,
                }));
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(condition, f.true_value)],
                    else_: Some(f.false_value),
                })))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl DuckDBDialect {
    /// Extract a numeric value from a literal expression, if possible
    fn extract_number_value(expr: &Expression) -> Option<f64> {
        match expr {
            Expression::Literal(Literal::Number(n)) => n.parse::<f64>().ok(),
            _ => None,
        }
    }

    /// Convert an expression to a SQL string for template-based transformations
    fn expr_to_sql(expr: &Expression) -> String {
        crate::generator::Generator::sql(expr).unwrap_or_default()
    }

    /// Extract the seed expression for random-based function emulations.
    /// Returns (seed_sql, is_random_no_seed) where:
    /// - For RANDOM(): ("RANDOM()", true)
    /// - For RANDOM(seed): ("seed", false) - extracts the seed
    /// - For literal seed: ("seed_value", false)
    fn extract_seed_info(gen: &Expression) -> (String, bool) {
        match gen {
            Expression::Function(func) if func.name.to_uppercase() == "RANDOM" => {
                if func.args.is_empty() {
                    ("RANDOM()".to_string(), true)
                } else {
                    // RANDOM(seed) -> extract the seed
                    (Self::expr_to_sql(&func.args[0]), false)
                }
            }
            Expression::Rand(r) => {
                if let Some(ref seed) = r.seed {
                    // RANDOM(seed) / RAND(seed) -> extract the seed
                    (Self::expr_to_sql(seed), false)
                } else {
                    ("RANDOM()".to_string(), true)
                }
            }
            Expression::Random(_) => {
                ("RANDOM()".to_string(), true)
            }
            _ => (Self::expr_to_sql(gen), false),
        }
    }

    /// Parse a SQL template string and wrap it in a Subquery (parenthesized expression).
    /// Uses a thread with larger stack to handle deeply nested template SQL in debug builds.
    fn parse_as_subquery(sql: &str) -> Result<Expression> {
        let sql_owned = sql.to_string();
        let handle = std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024) // 16MB stack for complex templates
            .spawn(move || {
                match crate::parser::Parser::parse_sql(&sql_owned) {
                    Ok(stmts) => {
                        if let Some(stmt) = stmts.into_iter().next() {
                            Ok(Expression::Subquery(Box::new(Subquery {
                                this: stmt,
                                alias: None,
                                column_aliases: Vec::new(),
                                order_by: None,
                                limit: None,
                                offset: None,
                                distribute_by: None,
                                sort_by: None,
                                cluster_by: None,
                                lateral: false,
                                modifiers_inside: false,
                                trailing_comments: Vec::new(),
                            })))
                        } else {
                            Err(crate::error::Error::Generate("Failed to parse template SQL".to_string()))
                        }
                    }
                    Err(e) => Err(e),
                }
            })
            .map_err(|e| crate::error::Error::Internal(format!("Failed to spawn parser thread: {}", e)))?;

        handle.join()
            .map_err(|_| crate::error::Error::Internal("Parser thread panicked".to_string()))?
    }

    /// Convert bracket notation ["key with spaces"] to quoted dot notation ."key with spaces"
    /// in JSON path strings. This is needed because Snowflake uses bracket notation for keys
    /// with special characters, but DuckDB uses quoted dot notation.
    fn convert_bracket_to_quoted_path(path: &str) -> String {
        let mut result = String::new();
        let mut chars = path.chars().peekable();
        while let Some(c) = chars.next() {
            if c == '[' && chars.peek() == Some(&'"') {
                // Found [" - start of bracket notation
                chars.next(); // consume "
                let mut key = String::new();
                while let Some(kc) = chars.next() {
                    if kc == '"' && chars.peek() == Some(&']') {
                        chars.next(); // consume ]
                        break;
                    }
                    key.push(kc);
                }
                // Convert to quoted dot notation: ."key"
                if !result.is_empty() && !result.ends_with('.') {
                    result.push('.');
                }
                result.push('"');
                result.push_str(&key);
                result.push('"');
            } else {
                result.push(c);
            }
        }
        result
    }

    /// Transform data types according to DuckDB TYPE_MAPPING
    fn transform_data_type(&self, dt: crate::expressions::DataType) -> Result<Expression> {
        use crate::expressions::DataType;
        let transformed = match dt {
            // BINARY -> VarBinary (DuckDB generator maps VarBinary to BLOB)
            DataType::Binary { .. } => DataType::VarBinary { length: None },
            // BLOB -> VarBinary (DuckDB generator maps VarBinary to BLOB)
            // This matches Python sqlglot's DuckDB parser mapping BLOB -> VARBINARY
            DataType::Blob => DataType::VarBinary { length: None },
            // CHAR -> TEXT
            DataType::Char { .. } => DataType::Text,
            // VARCHAR -> TEXT
            DataType::VarChar { .. } => DataType::Text,
            // FLOAT -> REAL (use real_spelling flag so generator can decide)
            DataType::Float { precision, scale, .. } => DataType::Float {
                precision, scale, real_spelling: true,
            },
            // JSONB -> JSON
            DataType::JsonB => DataType::Json,
            // Handle Custom type aliases used in DuckDB
            DataType::Custom { ref name } => {
                let upper = name.to_uppercase();
                match upper.as_str() {
                    // INT64 -> BIGINT
                    "INT64" | "INT8" => DataType::BigInt { length: None },
                    // INT32, INT4, SIGNED -> INT
                    "INT32" | "INT4" | "SIGNED" => DataType::Int { length: None, integer_spelling: false },
                    // INT16 -> SMALLINT
                    "INT16" => DataType::SmallInt { length: None },
                    // INT1 -> TINYINT
                    "INT1" => DataType::TinyInt { length: None },
                    // HUGEINT -> INT128
                    "HUGEINT" => DataType::Custom { name: "INT128".to_string() },
                    // UHUGEINT -> UINT128
                    "UHUGEINT" => DataType::Custom { name: "UINT128".to_string() },
                    // BPCHAR -> TEXT
                    "BPCHAR" => DataType::Text,
                    // CHARACTER VARYING, CHAR VARYING -> TEXT
                    "CHARACTER VARYING" | "CHAR VARYING" => DataType::Text,
                    // FLOAT4, REAL -> REAL
                    "FLOAT4" => DataType::Custom { name: "REAL".to_string() },
                    // LOGICAL -> BOOLEAN
                    "LOGICAL" => DataType::Boolean,
                    // TIMESTAMPNTZ / TIMESTAMP_NTZ -> TIMESTAMP
                    "TIMESTAMPNTZ" | "TIMESTAMP_NTZ" => DataType::Timestamp { precision: None, timezone: false },
                    // TIMESTAMP_US -> TIMESTAMP (DuckDB's default timestamp is microsecond precision)
                    "TIMESTAMP_US" => DataType::Timestamp { precision: None, timezone: false },
                    // TIMESTAMPLTZ / TIMESTAMPTZ / TIMESTAMP_LTZ / TIMESTAMP_TZ -> TIMESTAMPTZ
                    "TIMESTAMPLTZ" | "TIMESTAMP_LTZ" | "TIMESTAMPTZ" | "TIMESTAMP_TZ" => DataType::Timestamp { precision: None, timezone: true },
                    // DECFLOAT -> DECIMAL(38, 5) in DuckDB
                    "DECFLOAT" => DataType::Decimal { precision: Some(38), scale: Some(5) },
                    // Keep other custom types as-is
                    _ => dt,
                }
            }
            // Keep all other types as-is
            other => other,
        };
        Ok(Expression::DataType(transformed))
    }

    /// Transform interval to split embedded value+unit strings (e.g., '1 hour' -> '1' HOUR)
    /// DuckDB requires INTERVAL 'value' UNIT format, not INTERVAL 'value unit' format
    fn transform_interval(&self, interval: Interval) -> Result<Expression> {
        // Only transform if:
        // 1. There's a string literal value
        // 2. There's no unit already specified
        if interval.unit.is_some() {
            // Already has a unit, keep as-is
            return Ok(Expression::Interval(Box::new(interval)));
        }

        if let Some(Expression::Literal(Literal::String(ref s))) = interval.this {
            // Try to parse the string as "value unit" format
            if let Some((value, unit)) = Self::parse_interval_string(s) {
                // Create new interval with separated value and unit
                return Ok(Expression::Interval(Box::new(Interval {
                    this: Some(Expression::Literal(Literal::String(value.to_string()))),
                    unit: Some(IntervalUnitSpec::Simple {
                        unit,
                        use_plural: false, // DuckDB uses singular form
                    }),
                })));
            }
        }

        // No transformation needed
        Ok(Expression::Interval(Box::new(interval)))
    }

    /// Parse an interval string like "1 hour" into (value, unit)
    /// Returns None if the string doesn't match the expected format
    fn parse_interval_string(s: &str) -> Option<(&str, IntervalUnit)> {
        let s = s.trim();

        // Find where the number ends and the unit begins
        // Number can be: optional -, digits, optional decimal point, more digits
        let mut num_end = 0;
        let mut chars = s.chars().peekable();

        // Skip leading minus
        if chars.peek() == Some(&'-') {
            chars.next();
            num_end += 1;
        }

        // Skip digits
        while let Some(&c) = chars.peek() {
            if c.is_ascii_digit() {
                chars.next();
                num_end += 1;
            } else {
                break;
            }
        }

        // Skip optional decimal point and more digits
        if chars.peek() == Some(&'.') {
            chars.next();
            num_end += 1;
            while let Some(&c) = chars.peek() {
                if c.is_ascii_digit() {
                    chars.next();
                    num_end += 1;
                } else {
                    break;
                }
            }
        }

        if num_end == 0 || (num_end == 1 && s.starts_with('-')) {
            return None; // No number found
        }

        let value = &s[..num_end];
        let rest = s[num_end..].trim();

        // Rest should be alphabetic (the unit)
        if rest.is_empty() || !rest.chars().all(|c| c.is_ascii_alphabetic()) {
            return None;
        }

        // Map unit string to IntervalUnit
        let unit = match rest.to_uppercase().as_str() {
            "YEAR" | "YEARS" | "Y" => IntervalUnit::Year,
            "MONTH" | "MONTHS" | "MON" | "MONS" => IntervalUnit::Month,
            "DAY" | "DAYS" | "D" => IntervalUnit::Day,
            "HOUR" | "HOURS" | "H" | "HR" | "HRS" => IntervalUnit::Hour,
            "MINUTE" | "MINUTES" | "MIN" | "MINS" | "M" => IntervalUnit::Minute,
            "SECOND" | "SECONDS" | "SEC" | "SECS" | "S" => IntervalUnit::Second,
            "MILLISECOND" | "MILLISECONDS" | "MS" => IntervalUnit::Millisecond,
            "MICROSECOND" | "MICROSECONDS" | "US" => IntervalUnit::Microsecond,
            "QUARTER" | "QUARTERS" | "Q" => IntervalUnit::Quarter,
            "WEEK" | "WEEKS" | "W" => IntervalUnit::Week,
            _ => return None, // Unknown unit
        };

        Some((value, unit))
    }

    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL -> COALESCE
            "IFNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // NVL -> COALESCE
            "NVL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // ISNULL -> COALESCE
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // GROUP_CONCAT -> LISTAGG in DuckDB
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // LISTAGG is native to DuckDB
            "LISTAGG" => Ok(Expression::Function(Box::new(f))),

            // STRING_AGG -> LISTAGG in DuckDB
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // SUBSTR -> SUBSTRING (both work)
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // FLATTEN -> UNNEST in DuckDB
            "FLATTEN" => Ok(Expression::Function(Box::new(Function::new(
                "UNNEST".to_string(),
                f.args,
            )))),

            // EXPLODE -> UNNEST
            "EXPLODE" => Ok(Expression::Function(Box::new(Function::new(
                "UNNEST".to_string(),
                f.args,
            )))),

            // NOW -> CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // CURRENT_DATE is native
            "CURRENT_DATE" => Ok(Expression::CurrentDate(crate::expressions::CurrentDate)),

            // TO_DATE with 1 arg -> CAST(x AS DATE)
            "TO_DATE" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Cast(Box::new(Cast {
                    this: arg,
                    to: DataType::Date,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // TO_TIMESTAMP is native in DuckDB (kept as-is for identity)

            // DATE_FORMAT -> STRFTIME in DuckDB with format conversion
            "DATE_FORMAT" if f.args.len() >= 2 => {
                let mut args = f.args;
                args[1] = Self::convert_format_to_duckdb(&args[1]);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRFTIME".to_string(),
                    args,
                ))))
            }

            // DATE_PARSE -> STRPTIME in DuckDB with format conversion
            "DATE_PARSE" if f.args.len() >= 2 => {
                let mut args = f.args;
                args[1] = Self::convert_format_to_duckdb(&args[1]);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPTIME".to_string(),
                    args,
                ))))
            }

            // FORMAT_DATE -> STRFTIME in DuckDB
            "FORMAT_DATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                args[1] = Self::convert_format_to_duckdb(&args[1]);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRFTIME".to_string(),
                    args,
                ))))
            }

            // TO_CHAR -> STRFTIME in DuckDB
            "TO_CHAR" if f.args.len() >= 2 => {
                let mut args = f.args;
                args[1] = Self::convert_format_to_duckdb(&args[1]);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRFTIME".to_string(),
                    args,
                ))))
            }

            // EPOCH_MS is native to DuckDB
            "EPOCH_MS" => Ok(Expression::Function(Box::new(f))),

            // EPOCH -> EPOCH (native)
            "EPOCH" => Ok(Expression::Function(Box::new(f))),

            // FROM_UNIXTIME -> TO_TIMESTAMP in DuckDB
            "FROM_UNIXTIME" if !f.args.is_empty() => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_TIMESTAMP".to_string(),
                    f.args,
                ))))
            }

            // UNIX_TIMESTAMP -> EPOCH
            "UNIX_TIMESTAMP" => Ok(Expression::Function(Box::new(Function::new(
                "EPOCH".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT -> arrow operator (->)
            "JSON_EXTRACT" if f.args.len() == 2 => {
                let mut args = f.args;
                let path = args.pop().unwrap();
                let this = args.pop().unwrap();
                Ok(Expression::JsonExtract(Box::new(JsonExtractFunc {
                    this,
                    path,
                    returning: None,
                    arrow_syntax: true,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

            // JSON_EXTRACT_STRING -> arrow operator (->>)
            "JSON_EXTRACT_STRING" if f.args.len() == 2 => {
                let mut args = f.args;
                let path = args.pop().unwrap();
                let this = args.pop().unwrap();
                Ok(Expression::JsonExtractScalar(Box::new(JsonExtractFunc {
                    this,
                    path,
                    returning: None,
                    arrow_syntax: true,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

            // ARRAY_CONSTRUCT -> list_value or [a, b, c] syntax
            "ARRAY_CONSTRUCT" => Ok(Expression::Function(Box::new(Function::new(
                "list_value".to_string(),
                f.args,
            )))),

            // ARRAY -> list_value
            // ARRAY -> list_value for non-subquery args, keep ARRAY for subquery args
            "ARRAY" => {
                // Check if any arg contains a query (subquery)
                let has_query = f.args.iter().any(|a| matches!(a, Expression::Subquery(_) | Expression::Select(_)));
                if has_query {
                    // Keep as ARRAY() for subquery args
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY".to_string(),
                        f.args,
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "list_value".to_string(),
                        f.args,
                    ))))
                }
            }

            // LIST_VALUE -> Array literal notation [...]
            "LIST_VALUE" => Ok(Expression::Array(Box::new(crate::expressions::Array {
                expressions: f.args,
            }))),

            // ARRAY_AGG -> LIST in DuckDB (or array_agg which is also supported)
            "ARRAY_AGG" => Ok(Expression::Function(Box::new(Function::new(
                "list".to_string(),
                f.args,
            )))),

            // LIST_CONTAINS / ARRAY_CONTAINS -> keep normalized form
            "LIST_CONTAINS" | "ARRAY_CONTAINS" => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_CONTAINS".to_string(),
                f.args,
            )))),

            // ARRAY_SIZE/CARDINALITY -> ARRAY_LENGTH in DuckDB
            "ARRAY_SIZE" | "CARDINALITY" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_LENGTH".to_string(),
                    f.args,
                ))))
            }

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // CEILING -> CEIL (both work)
            "CEILING" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // LOGICAL_OR -> BOOL_OR with CAST to BOOLEAN
            "LOGICAL_OR" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "BOOL_OR".to_string(),
                    vec![Expression::Cast(Box::new(crate::expressions::Cast {
                        this: arg,
                        to: crate::expressions::DataType::Boolean,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))],
                ))))
            }

            // LOGICAL_AND -> BOOL_AND with CAST to BOOLEAN
            "LOGICAL_AND" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "BOOL_AND".to_string(),
                    vec![Expression::Cast(Box::new(crate::expressions::Cast {
                        this: arg,
                        to: crate::expressions::DataType::Boolean,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))],
                ))))
            }

            // REGEXP_LIKE -> REGEXP_MATCHES in DuckDB
            "REGEXP_LIKE" => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_MATCHES".to_string(),
                f.args,
            )))),

            // POSITION is native
            "POSITION" => Ok(Expression::Function(Box::new(f))),

            // SPLIT -> STR_SPLIT in DuckDB
            "SPLIT" => Ok(Expression::Function(Box::new(Function::new(
                "STR_SPLIT".to_string(),
                f.args,
            )))),

            // STRING_SPLIT -> STR_SPLIT in DuckDB
            "STRING_SPLIT" => Ok(Expression::Function(Box::new(Function::new(
                "STR_SPLIT".to_string(),
                f.args,
            )))),

            // STRTOK_TO_ARRAY -> STR_SPLIT
            "STRTOK_TO_ARRAY" => Ok(Expression::Function(Box::new(Function::new(
                "STR_SPLIT".to_string(),
                f.args,
            )))),

            // REGEXP_SPLIT -> STR_SPLIT_REGEX in DuckDB
            "REGEXP_SPLIT" => Ok(Expression::Function(Box::new(Function::new(
                "STR_SPLIT_REGEX".to_string(),
                f.args,
            )))),

            // EDITDIST3 -> LEVENSHTEIN in DuckDB
            "EDITDIST3" => Ok(Expression::Function(Box::new(Function::new(
                "LEVENSHTEIN".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT_PATH -> arrow operator (->)
            "JSON_EXTRACT_PATH" if f.args.len() >= 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let path = args.remove(0);
                Ok(Expression::JsonExtract(Box::new(JsonExtractFunc {
                    this,
                    path,
                    returning: None,
                    arrow_syntax: true,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

            // JSON_EXTRACT_PATH_TEXT -> arrow operator (->>)
            "JSON_EXTRACT_PATH_TEXT" if f.args.len() >= 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let path = args.remove(0);
                Ok(Expression::JsonExtractScalar(Box::new(JsonExtractFunc {
                    this,
                    path,
                    returning: None,
                    arrow_syntax: true,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

            // DATE_ADD(date, interval) -> date + interval in DuckDB
            "DATE_ADD" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let interval = args.remove(0);
                Ok(Expression::Add(Box::new(BinaryOp {
                    left: date,
                    right: interval,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // DATE_SUB(date, interval) -> date - interval in DuckDB
            "DATE_SUB" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let interval = args.remove(0);
                Ok(Expression::Sub(Box::new(BinaryOp {
                    left: date,
                    right: interval,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // RANGE(n) -> RANGE(0, n) in DuckDB
            "RANGE" if f.args.len() == 1 => {
                let mut new_args = vec![Expression::number(0)];
                new_args.extend(f.args);
                Ok(Expression::Function(Box::new(Function::new(
                    "RANGE".to_string(),
                    new_args,
                ))))
            }

            // GENERATE_SERIES(n) -> GENERATE_SERIES(0, n) in DuckDB
            "GENERATE_SERIES" if f.args.len() == 1 => {
                let mut new_args = vec![Expression::number(0)];
                new_args.extend(f.args);
                Ok(Expression::Function(Box::new(Function::new(
                    "GENERATE_SERIES".to_string(),
                    new_args,
                ))))
            }

            // REGEXP_EXTRACT(str, pattern, 0) -> REGEXP_EXTRACT(str, pattern) in DuckDB
            // Drop the group argument when it's 0 (default)
            "REGEXP_EXTRACT" if f.args.len() == 3 => {
                // Check if the third argument is 0
                let drop_group = match &f.args[2] {
                    Expression::Literal(Literal::Number(n)) => n == "0",
                    _ => false,
                };
                if drop_group {
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_EXTRACT".to_string(),
                        vec![f.args[0].clone(), f.args[1].clone()],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // STRUCT_PACK(a := 1, b := 2) -> {'a': 1, 'b': 2} (DuckDB struct literal)
            "STRUCT_PACK" => {
                let mut fields = Vec::new();
                for arg in f.args {
                    match arg {
                        Expression::NamedArgument(na) => {
                            fields.push((Some(na.name.name.clone()), na.value));
                        }
                        // Non-named arguments get positional keys
                        other => {
                            fields.push((None, other));
                        }
                    }
                }
                Ok(Expression::Struct(Box::new(Struct { fields })))
            }

            // REPLACE with 2 args -> add empty string 3rd arg
            "REPLACE" if f.args.len() == 2 => {
                let mut args = f.args;
                args.push(Expression::Literal(Literal::String(String::new())));
                Ok(Expression::Function(Box::new(Function::new(
                    "REPLACE".to_string(),
                    args,
                ))))
            }

            // TO_UNIXTIME -> EPOCH in DuckDB
            "TO_UNIXTIME" => Ok(Expression::Function(Box::new(Function::new(
                "EPOCH".to_string(),
                f.args,
            )))),

            // FROM_ISO8601_TIMESTAMP -> CAST(x AS TIMESTAMPTZ) in DuckDB
            "FROM_ISO8601_TIMESTAMP" if f.args.len() == 1 => {
                use crate::expressions::{Cast, DataType};
                Ok(Expression::Cast(Box::new(Cast {
                    this: f.args.into_iter().next().unwrap(),
                    to: DataType::Timestamp { precision: None, timezone: true },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // APPROX_DISTINCT -> APPROX_COUNT_DISTINCT in DuckDB
            "APPROX_DISTINCT" => {
                // Drop the accuracy parameter (second arg) if present
                let args = if f.args.len() > 1 {
                    vec![f.args.into_iter().next().unwrap()]
                } else {
                    f.args
                };
                Ok(Expression::Function(Box::new(Function::new(
                    "APPROX_COUNT_DISTINCT".to_string(),
                    args,
                ))))
            }

            // ARRAY_SORT is native to DuckDB (but drop the lambda comparator)
            "ARRAY_SORT" => {
                let args = vec![f.args.into_iter().next().unwrap()];
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_SORT".to_string(),
                    args,
                )))
                )
            }

            // TO_UTF8 -> ENCODE in DuckDB
            "TO_UTF8" => Ok(Expression::Function(Box::new(Function::new(
                "ENCODE".to_string(),
                f.args,
            )))),

            // FROM_UTF8 -> DECODE in DuckDB
            "FROM_UTF8" => Ok(Expression::Function(Box::new(Function::new(
                "DECODE".to_string(),
                f.args,
            )))),

            // ARBITRARY -> ANY_VALUE in DuckDB
            "ARBITRARY" => Ok(Expression::Function(Box::new(Function::new(
                "ANY_VALUE".to_string(),
                f.args,
            )))),

            // MAX_BY -> ARG_MAX in DuckDB
            "MAX_BY" => Ok(Expression::Function(Box::new(Function::new(
                "ARG_MAX".to_string(),
                f.args,
            )))),

            // MIN_BY -> ARG_MIN in DuckDB
            "MIN_BY" => Ok(Expression::Function(Box::new(Function::new(
                "ARG_MIN".to_string(),
                f.args,
            )))),

            // ===== Snowflake-specific function transforms =====

            "IFF" if f.args.len() == 3 => {
                let mut args = f.args;
                let cond = args.remove(0);
                let true_val = args.remove(0);
                let false_val = args.remove(0);
                Ok(Expression::Case(Box::new(Case { operand: None, whens: vec![(cond, true_val)], else_: Some(false_val) })))
            }
            "SKEW" => Ok(Expression::Function(Box::new(Function::new("SKEWNESS".to_string(), f.args)))),
            "VAR_SAMP" => Ok(Expression::Function(Box::new(Function::new("VARIANCE".to_string(), f.args)))),
            "VARIANCE_POP" => Ok(Expression::Function(Box::new(Function::new("VAR_POP".to_string(), f.args)))),
            "REGR_VALX" if f.args.len() == 2 => {
                let mut args = f.args; let y = args.remove(0); let x = args.remove(0);
                Ok(Expression::Case(Box::new(Case { operand: None, whens: vec![(Expression::IsNull(Box::new(crate::expressions::IsNull { this: y, not: false, postfix_form: false })), Expression::Cast(Box::new(Cast { this: Expression::Null(crate::expressions::Null), to: DataType::Double { precision: None, scale: None }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))], else_: Some(x) })))
            }
            "REGR_VALY" if f.args.len() == 2 => {
                let mut args = f.args; let y = args.remove(0); let x = args.remove(0);
                Ok(Expression::Case(Box::new(Case { operand: None, whens: vec![(Expression::IsNull(Box::new(crate::expressions::IsNull { this: x, not: false, postfix_form: false })), Expression::Cast(Box::new(Cast { this: Expression::Null(crate::expressions::Null), to: DataType::Double { precision: None, scale: None }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))], else_: Some(y) })))
            }
            "BOOLNOT" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Not(Box::new(crate::expressions::UnaryOp { this: Expression::Paren(Box::new(Paren { this: arg, trailing_comments: Vec::new() })) })))
            }
            "BITMAP_BIT_POSITION" if f.args.len() == 1 => {
                let n = f.args.into_iter().next().unwrap();
                let case_expr = Expression::Case(Box::new(Case { operand: None, whens: vec![(Expression::Gt(Box::new(BinaryOp { left: n.clone(), right: Expression::number(0), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), Expression::Sub(Box::new(BinaryOp { left: n.clone(), right: Expression::number(1), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))], else_: Some(Expression::Abs(Box::new(UnaryFunc { this: n, original_name: None }))) }));
                Ok(Expression::Mod(Box::new(BinaryOp { left: Expression::Paren(Box::new(Paren { this: case_expr, trailing_comments: Vec::new() })), right: Expression::number(32768), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
            }
            // GREATEST/LEAST - pass through (null-wrapping is handled by source dialect transforms)
            "GREATEST" | "LEAST" => Ok(Expression::Function(Box::new(f))),
            "GREATEST_IGNORE_NULLS" => Ok(Expression::Greatest(Box::new(VarArgFunc { expressions: f.args, original_name: None }))),
            "LEAST_IGNORE_NULLS" => Ok(Expression::Least(Box::new(VarArgFunc { expressions: f.args, original_name: None }))),
            "PARSE_JSON" => Ok(Expression::Function(Box::new(Function::new("JSON".to_string(), f.args)))),
            "OBJECT_CONSTRUCT_KEEP_NULL" => {
                // OBJECT_CONSTRUCT_KEEP_NULL -> JSON_OBJECT (preserves NULLs)
                Ok(Expression::Function(Box::new(Function::new("JSON_OBJECT".to_string(), f.args))))
            }
            "OBJECT_CONSTRUCT" => {
                // Convert to DuckDB struct literal: {'key1': val1, 'key2': val2}
                let args = f.args;
                if args.is_empty() {
                    // Empty OBJECT_CONSTRUCT() -> STRUCT_PACK() (no args)
                    Ok(Expression::Function(Box::new(Function::new("STRUCT_PACK".to_string(), vec![]))))
                } else {
                    // Build struct literal from key-value pairs
                    let mut fields = Vec::new();
                    let mut i = 0;
                    while i + 1 < args.len() {
                        let key = &args[i];
                        let value = args[i + 1].clone();
                        let key_name = match key {
                            Expression::Literal(Literal::String(s)) => Some(s.clone()),
                            _ => None,
                        };
                        fields.push((key_name, value));
                        i += 2;
                    }
                    Ok(Expression::Struct(Box::new(Struct { fields })))
                }
            }
            "IS_NULL_VALUE" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Eq(Box::new(BinaryOp { left: Expression::Function(Box::new(Function::new("JSON_TYPE".to_string(), vec![arg]))), right: Expression::Literal(Literal::String("NULL".to_string())), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
            }
            "TRY_TO_DOUBLE" | "TRY_TO_NUMBER" | "TRY_TO_NUMERIC" | "TRY_TO_DECIMAL" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::TryCast(Box::new(Cast { this: arg, to: DataType::Double { precision: None, scale: None }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "TRY_TO_TIME" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::TryCast(Box::new(Cast { this: arg, to: DataType::Time { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "TRY_TO_TIME" if f.args.len() == 2 => {
                let mut args = f.args; let value = args.remove(0);
                let fmt = self.convert_snowflake_time_format(args.remove(0));
                Ok(Expression::TryCast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("TRY_STRPTIME".to_string(), vec![value, fmt]))), to: DataType::Time { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "TRY_TO_TIMESTAMP" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::TryCast(Box::new(Cast { this: arg, to: DataType::Timestamp { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "TRY_TO_TIMESTAMP" if f.args.len() == 2 => {
                let mut args = f.args; let value = args.remove(0);
                let fmt = self.convert_snowflake_time_format(args.remove(0));
                Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("TRY_STRPTIME".to_string(), vec![value, fmt]))), to: DataType::Timestamp { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "TRY_TO_DATE" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::TryCast(Box::new(Cast { this: arg, to: DataType::Date, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "DAYOFWEEKISO" | "DAYOFWEEK_ISO" => Ok(Expression::Function(Box::new(Function::new("ISODOW".to_string(), f.args)))),
            "YEAROFWEEK" | "YEAROFWEEKISO" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Extract(Box::new(crate::expressions::ExtractFunc { this: arg, field: crate::expressions::DateTimeField::Custom("ISOYEAR".to_string()) })))
            }
            "WEEKISO" => Ok(Expression::Function(Box::new(Function::new("WEEKOFYEAR".to_string(), f.args)))),
            "TIME_FROM_PARTS" | "TIMEFROMPARTS" if f.args.len() == 3 => {
                let args_ref = &f.args;
                // Check if all args are in-range literals: h < 24, m < 60, s < 60
                let all_in_range = if let (
                    Some(h_val), Some(m_val), Some(s_val)
                ) = (
                    Self::extract_number_value(&args_ref[0]),
                    Self::extract_number_value(&args_ref[1]),
                    Self::extract_number_value(&args_ref[2]),
                ) {
                    h_val >= 0.0 && h_val < 24.0 && m_val >= 0.0 && m_val < 60.0 && s_val >= 0.0 && s_val < 60.0
                } else {
                    false
                };
                if all_in_range {
                    // Use MAKE_TIME for normal values
                    Ok(Expression::Function(Box::new(Function::new("MAKE_TIME".to_string(), f.args))))
                } else {
                    // TIME_FROM_PARTS(h, m, s) -> CAST('00:00:00' AS TIME) + INTERVAL ((h * 3600) + (m * 60) + s) SECOND
                    // Use arithmetic approach to handle out-of-range values (e.g., 100 minutes)
                    let mut args = f.args; let h = args.remove(0); let m = args.remove(0); let s = args.remove(0);
                    let seconds_expr = Expression::Add(Box::new(BinaryOp { left: Expression::Add(Box::new(BinaryOp { left: Expression::Paren(Box::new(Paren { this: Expression::Mul(Box::new(BinaryOp { left: h, right: Expression::number(3600), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), trailing_comments: Vec::new() })), right: Expression::Paren(Box::new(Paren { this: Expression::Mul(Box::new(BinaryOp { left: m, right: Expression::number(60), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), trailing_comments: Vec::new() })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), right: s, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                    let base_time = Expression::Cast(Box::new(Cast { this: Expression::Literal(Literal::String("00:00:00".to_string())), to: DataType::Time { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }));
                    Ok(Expression::Add(Box::new(BinaryOp { left: base_time, right: Expression::Interval(Box::new(Interval { this: Some(Expression::Paren(Box::new(crate::expressions::Paren { this: seconds_expr, trailing_comments: Vec::new() }))), unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Second, use_plural: false }) })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
                }
            }
            "TIME_FROM_PARTS" | "TIMEFROMPARTS" if f.args.len() == 4 => {
                let mut args = f.args; let h = args.remove(0); let m = args.remove(0); let s = args.remove(0); let ns = args.remove(0);
                let seconds_expr = Expression::Add(Box::new(BinaryOp { left: Expression::Add(Box::new(BinaryOp { left: Expression::Add(Box::new(BinaryOp { left: Expression::Paren(Box::new(Paren { this: Expression::Mul(Box::new(BinaryOp { left: h, right: Expression::number(3600), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), trailing_comments: Vec::new() })), right: Expression::Paren(Box::new(Paren { this: Expression::Mul(Box::new(BinaryOp { left: m, right: Expression::number(60), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), trailing_comments: Vec::new() })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), right: s, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), right: Expression::Paren(Box::new(Paren { this: Expression::Div(Box::new(BinaryOp { left: ns, right: Expression::Literal(Literal::Number("1000000000.0".to_string())), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), trailing_comments: Vec::new() })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                let base_time = Expression::Cast(Box::new(Cast { this: Expression::Literal(Literal::String("00:00:00".to_string())), to: DataType::Time { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }));
                Ok(Expression::Add(Box::new(BinaryOp { left: base_time, right: Expression::Interval(Box::new(Interval { this: Some(Expression::Paren(Box::new(crate::expressions::Paren { this: seconds_expr, trailing_comments: Vec::new() }))), unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Second, use_plural: false }) })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
            }
            "TIMESTAMP_FROM_PARTS" | "TIMESTAMPFROMPARTS" if f.args.len() == 6 => Ok(Expression::Function(Box::new(Function::new("MAKE_TIMESTAMP".to_string(), f.args)))),
            "TIMESTAMP_FROM_PARTS" | "TIMESTAMPFROMPARTS" | "TIMESTAMP_NTZ_FROM_PARTS" if f.args.len() == 2 => {
                let mut args = f.args; let d = args.remove(0); let t = args.remove(0);
                Ok(Expression::Add(Box::new(BinaryOp { left: d, right: t, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
            }
            "TIMESTAMP_LTZ_FROM_PARTS" if f.args.len() == 6 => {
                Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("MAKE_TIMESTAMP".to_string(), f.args))), to: DataType::Timestamp { precision: None, timezone: true }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "TIMESTAMP_TZ_FROM_PARTS" if f.args.len() == 8 => {
                let mut args = f.args;
                let ts_args = vec![args.remove(0), args.remove(0), args.remove(0), args.remove(0), args.remove(0), args.remove(0)];
                let _nano = args.remove(0); let tz = args.remove(0);
                Ok(Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone { this: Expression::Function(Box::new(Function::new("MAKE_TIMESTAMP".to_string(), ts_args))), zone: tz })))
            }
            "BOOLAND_AGG" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new("BOOL_AND".to_string(), vec![Expression::Cast(Box::new(Cast { this: arg, to: DataType::Boolean, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))]))))
            }
            "BOOLOR_AGG" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new("BOOL_OR".to_string(), vec![Expression::Cast(Box::new(Cast { this: arg, to: DataType::Boolean, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))]))))
            }
            "NVL2" if f.args.len() == 3 => {
                let mut args = f.args; let a = args.remove(0); let b = args.remove(0); let c = args.remove(0);
                Ok(Expression::Case(Box::new(Case { operand: None, whens: vec![(Expression::Not(Box::new(crate::expressions::UnaryOp { this: Expression::IsNull(Box::new(crate::expressions::IsNull { this: a, not: false, postfix_form: false })) })), b)], else_: Some(c) })))
            }
            "EQUAL_NULL" if f.args.len() == 2 => {
                let mut args = f.args; let a = args.remove(0); let b = args.remove(0);
                Ok(Expression::NullSafeEq(Box::new(BinaryOp { left: a, right: b, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
            }
            "EDITDISTANCE" => Ok(Expression::Function(Box::new(Function::new("LEVENSHTEIN".to_string(), f.args)))),
            "BITAND" if f.args.len() == 2 => {
                let mut args = f.args;
                let left = args.remove(0);
                let right = args.remove(0);
                // Wrap shift expressions in parentheses for correct precedence
                let wrap = |e: Expression| -> Expression {
                    match &e {
                        Expression::BitwiseLeftShift(_) | Expression::BitwiseRightShift(_) =>
                            Expression::Paren(Box::new(Paren { this: e, trailing_comments: Vec::new() })),
                        _ => e,
                    }
                };
                Ok(Expression::BitwiseAnd(Box::new(BinaryOp { left: wrap(left), right: wrap(right), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
            }
            "BITOR" if f.args.len() == 2 => {
                let mut args = f.args;
                let left = args.remove(0);
                let right = args.remove(0);
                // Wrap shift expressions in parentheses for correct precedence
                let wrap = |e: Expression| -> Expression {
                    match &e {
                        Expression::BitwiseLeftShift(_) | Expression::BitwiseRightShift(_) =>
                            Expression::Paren(Box::new(Paren { this: e, trailing_comments: Vec::new() })),
                        _ => e,
                    }
                };
                Ok(Expression::BitwiseOr(Box::new(BinaryOp { left: wrap(left), right: wrap(right), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
            }
            "BITXOR" if f.args.len() == 2 => { let mut args = f.args; Ok(Expression::BitwiseXor(Box::new(BinaryOp { left: args.remove(0), right: args.remove(0), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))) }
            "BITNOT" if f.args.len() == 1 => { let arg = f.args.into_iter().next().unwrap(); Ok(Expression::BitwiseNot(Box::new(crate::expressions::UnaryOp { this: Expression::Paren(Box::new(Paren { this: arg, trailing_comments: Vec::new() })) }))) }
            "BITSHIFTLEFT" if f.args.len() == 2 => {
                let mut args = f.args; let a = args.remove(0); let b = args.remove(0);
                // Check if first arg is BINARY/BLOB type (e.g., X'002A'::BINARY)
                let is_binary = if let Expression::Cast(ref c) = a {
                    matches!(&c.to, DataType::Binary { .. } | DataType::VarBinary { .. } | DataType::Blob)
                        || matches!(&c.to, DataType::Custom { name } if name == "BLOB")
                } else { false };
                if is_binary {
                    // CAST(CAST(a AS BIT) << b AS BLOB)
                    let cast_to_bit = Expression::Cast(Box::new(Cast { this: a, to: DataType::Custom { name: "BIT".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }));
                    let shift = Expression::BitwiseLeftShift(Box::new(BinaryOp { left: cast_to_bit, right: b, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                    Ok(Expression::Cast(Box::new(Cast { this: shift, to: DataType::Custom { name: "BLOB".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
                } else {
                    Ok(Expression::BitwiseLeftShift(Box::new(BinaryOp { left: Expression::Cast(Box::new(Cast { this: a, to: DataType::Custom { name: "INT128".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })), right: b, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
                }
            }
            "BITSHIFTRIGHT" if f.args.len() == 2 => {
                let mut args = f.args; let a = args.remove(0); let b = args.remove(0);
                // Check if first arg is BINARY/BLOB type (e.g., X'002A'::BINARY)
                let is_binary = if let Expression::Cast(ref c) = a {
                    matches!(&c.to, DataType::Binary { .. } | DataType::VarBinary { .. } | DataType::Blob)
                        || matches!(&c.to, DataType::Custom { name } if name == "BLOB")
                } else { false };
                if is_binary {
                    // CAST(CAST(a AS BIT) >> b AS BLOB)
                    let cast_to_bit = Expression::Cast(Box::new(Cast { this: a, to: DataType::Custom { name: "BIT".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }));
                    let shift = Expression::BitwiseRightShift(Box::new(BinaryOp { left: cast_to_bit, right: b, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                    Ok(Expression::Cast(Box::new(Cast { this: shift, to: DataType::Custom { name: "BLOB".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
                } else {
                    Ok(Expression::BitwiseRightShift(Box::new(BinaryOp { left: Expression::Cast(Box::new(Cast { this: a, to: DataType::Custom { name: "INT128".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })), right: b, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
                }
            }
            "SQUARE" if f.args.len() == 1 => { let arg = f.args.into_iter().next().unwrap(); Ok(Expression::Function(Box::new(Function::new("POWER".to_string(), vec![arg, Expression::number(2)])))) }
            "UUID_STRING" => Ok(Expression::Function(Box::new(Function::new("UUID".to_string(), vec![])))),
            "ENDSWITH" => Ok(Expression::Function(Box::new(Function::new("ENDS_WITH".to_string(), f.args)))),
            // REGEXP_REPLACE: 'g' flag is handled by cross_dialect_normalize for source dialects
            // that default to global replacement (e.g., Snowflake). DuckDB defaults to first-match,
            // so no 'g' flag needed for DuckDB identity or PostgreSQL->DuckDB.
            "REGEXP_REPLACE" if f.args.len() == 2 => {
                // 2-arg form (subject, pattern) -> add empty replacement
                let mut args = f.args; args.push(Expression::Literal(Literal::String(String::new())));
                Ok(Expression::Function(Box::new(Function::new("REGEXP_REPLACE".to_string(), args))))
            }
            "DIV0" if f.args.len() == 2 => {
                let mut args = f.args; let a = args.remove(0); let b = args.remove(0);
                Ok(Expression::Case(Box::new(Case { operand: None, whens: vec![(Expression::And(Box::new(BinaryOp { left: Expression::Eq(Box::new(BinaryOp { left: b.clone(), right: Expression::number(0), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), right: Expression::Not(Box::new(crate::expressions::UnaryOp { this: Expression::IsNull(Box::new(crate::expressions::IsNull { this: a.clone(), not: false, postfix_form: false })) })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), Expression::number(0))], else_: Some(Expression::Div(Box::new(BinaryOp { left: a, right: b, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))) })))
            }
            "DIV0NULL" if f.args.len() == 2 => {
                let mut args = f.args; let a = args.remove(0); let b = args.remove(0);
                Ok(Expression::Case(Box::new(Case { operand: None, whens: vec![(Expression::Or(Box::new(BinaryOp { left: Expression::Eq(Box::new(BinaryOp { left: b.clone(), right: Expression::number(0), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), right: Expression::IsNull(Box::new(crate::expressions::IsNull { this: b.clone(), not: false, postfix_form: false })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), Expression::number(0))], else_: Some(Expression::Div(Box::new(BinaryOp { left: a, right: b, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))) })))
            }
            "ZEROIFNULL" if f.args.len() == 1 => {
                let x = f.args.into_iter().next().unwrap();
                Ok(Expression::Case(Box::new(Case { operand: None, whens: vec![(Expression::IsNull(Box::new(crate::expressions::IsNull { this: x.clone(), not: false, postfix_form: false })), Expression::number(0))], else_: Some(x) })))
            }
            "NULLIFZERO" if f.args.len() == 1 => {
                let x = f.args.into_iter().next().unwrap();
                Ok(Expression::Case(Box::new(Case { operand: None, whens: vec![(Expression::Eq(Box::new(BinaryOp { left: x.clone(), right: Expression::number(0), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), Expression::Null(crate::expressions::Null))], else_: Some(x) })))
            }
            "TO_DOUBLE" if f.args.len() == 1 => { let arg = f.args.into_iter().next().unwrap(); Ok(Expression::Cast(Box::new(Cast { this: arg, to: DataType::Double { precision: None, scale: None }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))) }
            "DATE" if f.args.len() == 1 => { let arg = f.args.into_iter().next().unwrap(); Ok(Expression::Cast(Box::new(Cast { this: arg, to: DataType::Date, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))) }
            "DATE" if f.args.len() == 2 => {
                let mut args = f.args; let value = args.remove(0); let fmt = self.convert_snowflake_date_format(args.remove(0));
                Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("STRPTIME".to_string(), vec![value, fmt]))), to: DataType::Date, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "SYSDATE" => Ok(Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone { this: Expression::CurrentTimestamp(crate::expressions::CurrentTimestamp { precision: None, sysdate: false }), zone: Expression::Literal(Literal::String("UTC".to_string())) }))),
            "HEX_DECODE_BINARY" => Ok(Expression::Function(Box::new(Function::new("UNHEX".to_string(), f.args)))),
            "CONVERT_TIMEZONE" if f.args.len() == 3 => {
                let mut args = f.args; let src_tz = args.remove(0); let tgt_tz = args.remove(0); let ts = args.remove(0);
                let cast_ts = Expression::Cast(Box::new(Cast { this: ts, to: DataType::Timestamp { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }));
                Ok(Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone { this: Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone { this: cast_ts, zone: src_tz })), zone: tgt_tz })))
            }
            "CONVERT_TIMEZONE" if f.args.len() == 2 => {
                let mut args = f.args; let tgt_tz = args.remove(0); let ts = args.remove(0);
                let cast_ts = Expression::Cast(Box::new(Cast { this: ts, to: DataType::Timestamp { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }));
                Ok(Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone { this: cast_ts, zone: tgt_tz })))
            }
            "DATE_PART" | "DATEPART" if f.args.len() == 2 => self.transform_date_part(f.args),
            "DATEADD" | "TIMEADD" if f.args.len() == 3 => self.transform_dateadd(f.args),
            "TIMESTAMPADD" if f.args.len() == 3 => self.transform_dateadd(f.args),
            "DATEDIFF" | "TIMEDIFF" if f.args.len() == 3 => self.transform_datediff(f.args),
            "TIMESTAMPDIFF" if f.args.len() == 3 => self.transform_datediff(f.args),
            "CORR" if f.args.len() == 2 => {
                // DuckDB handles NaN natively - no ISNAN wrapping needed
                Ok(Expression::Function(Box::new(f)))
            }
            "TO_TIMESTAMP" | "TO_TIMESTAMP_NTZ" if f.args.len() == 2 => {
                let mut args = f.args; let value = args.remove(0); let second_arg = args.remove(0);
                match &second_arg {
                    Expression::Literal(Literal::Number(_)) => Ok(Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone { this: Expression::Function(Box::new(Function::new("TO_TIMESTAMP".to_string(), vec![Expression::Div(Box::new(BinaryOp { left: value, right: Expression::Function(Box::new(Function::new("POWER".to_string(), vec![Expression::number(10), second_arg]))), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))]))), zone: Expression::Literal(Literal::String("UTC".to_string())) }))),
                    _ => { let fmt = self.convert_snowflake_time_format(second_arg); Ok(Expression::Function(Box::new(Function::new("STRPTIME".to_string(), vec![value, fmt])))) }
                }
            }
            "TO_TIME" if f.args.len() == 1 => { let arg = f.args.into_iter().next().unwrap(); Ok(Expression::Cast(Box::new(Cast { this: arg, to: DataType::Time { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))) }
            "TO_TIME" if f.args.len() == 2 => {
                let mut args = f.args; let value = args.remove(0); let fmt = self.convert_snowflake_time_format(args.remove(0));
                Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("STRPTIME".to_string(), vec![value, fmt]))), to: DataType::Time { precision: None, timezone: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            "TO_DATE" if f.args.len() == 2 => {
                let mut args = f.args; let value = args.remove(0); let fmt = self.convert_snowflake_date_format(args.remove(0));
                Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("STRPTIME".to_string(), vec![value, fmt]))), to: DataType::Date, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
            }
            // LAST_DAY with 2 args handled by comprehensive handler below

            // SAFE_DIVIDE(x, y) -> CASE WHEN y <> 0 THEN x / y ELSE NULL END
            "SAFE_DIVIDE" if f.args.len() == 2 => {
                let mut args = f.args;
                let x = args.remove(0);
                let y = args.remove(0);
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(
                        Expression::Neq(Box::new(BinaryOp {
                            left: y.clone(),
                            right: Expression::number(0),
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                        })),
                        Expression::Div(Box::new(BinaryOp {
                            left: x,
                            right: y,
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                        })),
                    )],
                    else_: Some(Expression::Null(crate::expressions::Null)),
                })))
            }

            // TO_HEX(x) -> LOWER(HEX(x)) in DuckDB (BigQuery TO_HEX returns lowercase)
            "TO_HEX" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Lower(Box::new(UnaryFunc::new(
                    Expression::Function(Box::new(Function::new("HEX".to_string(), vec![arg]))),
                ))))
            }

            // EDIT_DISTANCE -> LEVENSHTEIN in DuckDB
            "EDIT_DISTANCE" if f.args.len() >= 2 => {
                // Only use the first two args (drop max_distance kwarg)
                let mut args = f.args;
                let a = args.remove(0);
                let b = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "LEVENSHTEIN".to_string(),
                    vec![a, b],
                ))))
            }

            // UNIX_DATE(d) -> DATE_DIFF('DAY', CAST('1970-01-01' AS DATE), d) in DuckDB
            "UNIX_DATE" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::Literal(Literal::String("DAY".to_string())),
                        Expression::Cast(Box::new(Cast {
                            this: Expression::Literal(Literal::String("1970-01-01".to_string())),
                            to: DataType::Date,
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        })),
                        arg,
                    ],
                ))))
            }

            // TIMESTAMP(x) -> CAST(x AS TIMESTAMPTZ) in DuckDB
            "TIMESTAMP" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Cast(Box::new(Cast {
                    this: arg,
                    to: DataType::Custom { name: "TIMESTAMPTZ".to_string() },
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }

            // TIME(h, m, s) -> MAKE_TIME(h, m, s) in DuckDB
            "TIME" if f.args.len() == 3 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "MAKE_TIME".to_string(),
                    f.args,
                ))))
            }

            // DATE(y, m, d) -> MAKE_DATE(y, m, d) in DuckDB
            "DATE" if f.args.len() == 3 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "MAKE_DATE".to_string(),
                    f.args,
                ))))
            }

            // DATETIME(y, m, d, h, min, sec) -> MAKE_TIMESTAMP(y, m, d, h, min, sec) in DuckDB
            "DATETIME" if f.args.len() == 6 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "MAKE_TIMESTAMP".to_string(),
                    f.args,
                ))))
            }

            // PARSE_TIMESTAMP(fmt, x) -> STRPTIME(x, fmt) in DuckDB (swap args)
            "PARSE_TIMESTAMP" if f.args.len() >= 2 => {
                let mut args = f.args;
                let fmt = args.remove(0);
                let value = args.remove(0);
                // Convert BigQuery format to DuckDB strptime format
                let duckdb_fmt = self.convert_bq_to_strptime_format(fmt);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPTIME".to_string(),
                    vec![value, duckdb_fmt],
                ))))
            }

            // BOOLAND(a, b) -> ((a) AND (b))
            "BOOLAND" if f.args.len() == 2 => {
                let mut args = f.args;
                let a = args.remove(0);
                let b = args.remove(0);
                Ok(Expression::Paren(Box::new(Paren {
                    this: Expression::And(Box::new(BinaryOp {
                        left: Expression::Paren(Box::new(Paren { this: a, trailing_comments: Vec::new() })),
                        right: Expression::Paren(Box::new(Paren { this: b, trailing_comments: Vec::new() })),
                        left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                    })),
                    trailing_comments: Vec::new(),
                })))
            }

            // BOOLOR(a, b) -> ((a) OR (b))
            "BOOLOR" if f.args.len() == 2 => {
                let mut args = f.args;
                let a = args.remove(0);
                let b = args.remove(0);
                Ok(Expression::Paren(Box::new(Paren {
                    this: Expression::Or(Box::new(BinaryOp {
                        left: Expression::Paren(Box::new(Paren { this: a, trailing_comments: Vec::new() })),
                        right: Expression::Paren(Box::new(Paren { this: b, trailing_comments: Vec::new() })),
                        left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                    })),
                    trailing_comments: Vec::new(),
                })))
            }

            // DECODE(expr, search1, result1, ..., default) -> CASE WHEN expr = search1 OR (expr IS NULL AND search1 IS NULL) THEN result1 ... ELSE default END
            "DECODE" if f.args.len() >= 3 => {
                let mut args = f.args;
                let expr = args.remove(0);
                let mut whens = Vec::new();
                let mut else_expr = None;
                while args.len() >= 2 {
                    let search = args.remove(0);
                    let result = args.remove(0);
                    // Wrap complex expressions in parens for = comparison
                    let needs_paren_expr = matches!(&expr, Expression::Eq(_) | Expression::Neq(_) | Expression::Gt(_) | Expression::Lt(_) | Expression::Gte(_) | Expression::Lte(_) | Expression::And(_) | Expression::Or(_));
                    let needs_paren_search = matches!(&search, Expression::Eq(_) | Expression::Neq(_) | Expression::Gt(_) | Expression::Lt(_) | Expression::Gte(_) | Expression::Lte(_) | Expression::And(_) | Expression::Or(_));
                    let eq_left = if needs_paren_expr { Expression::Paren(Box::new(Paren { this: expr.clone(), trailing_comments: Vec::new() })) } else { expr.clone() };
                    let eq_right = if needs_paren_search { Expression::Paren(Box::new(Paren { this: search.clone(), trailing_comments: Vec::new() })) } else { search.clone() };
                    let is_null_expr = if needs_paren_expr { Expression::Paren(Box::new(Paren { this: expr.clone(), trailing_comments: Vec::new() })) } else { expr.clone() };
                    let is_null_search = if needs_paren_search { Expression::Paren(Box::new(Paren { this: search.clone(), trailing_comments: Vec::new() })) } else { search.clone() };
                    let condition = Expression::Or(Box::new(BinaryOp {
                        left: Expression::Eq(Box::new(BinaryOp { left: eq_left, right: eq_right, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })),
                        right: Expression::Paren(Box::new(Paren {
                            this: Expression::And(Box::new(BinaryOp {
                                left: Expression::IsNull(Box::new(crate::expressions::IsNull { this: is_null_expr, not: false, postfix_form: false })),
                                right: Expression::IsNull(Box::new(crate::expressions::IsNull { this: is_null_search, not: false, postfix_form: false })),
                                left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                            })),
                            trailing_comments: Vec::new(),
                        })),
                        left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                    }));
                    whens.push((condition, result));
                }
                if !args.is_empty() {
                    else_expr = Some(args.remove(0));
                }
                Ok(Expression::Case(Box::new(Case { operand: None, whens, else_: else_expr })))
            }

            // TRY_TO_BOOLEAN -> CASE WHEN UPPER(CAST(x AS TEXT)) = 'ON' THEN TRUE WHEN ... = 'OFF' THEN FALSE ELSE TRY_CAST(x AS BOOLEAN) END
            "TRY_TO_BOOLEAN" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                let cast_text = Expression::Cast(Box::new(Cast { this: arg.clone(), to: DataType::Text, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }));
                let upper_text = Expression::Upper(Box::new(UnaryFunc::new(cast_text)));
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![
                        (Expression::Eq(Box::new(BinaryOp { left: upper_text.clone(), right: Expression::Literal(Literal::String("ON".to_string())), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), Expression::Boolean(crate::expressions::BooleanLiteral { value: true })),
                        (Expression::Eq(Box::new(BinaryOp { left: upper_text, right: Expression::Literal(Literal::String("OFF".to_string())), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), Expression::Boolean(crate::expressions::BooleanLiteral { value: false })),
                    ],
                    else_: Some(Expression::TryCast(Box::new(Cast { this: arg, to: DataType::Boolean, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))),
                })))
            }

            // TO_BOOLEAN -> complex CASE expression
            "TO_BOOLEAN" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                let cast_text = Expression::Cast(Box::new(Cast { this: arg.clone(), to: DataType::Text, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }));
                let upper_text = Expression::Upper(Box::new(UnaryFunc::new(cast_text)));
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![
                        (Expression::Eq(Box::new(BinaryOp { left: upper_text.clone(), right: Expression::Literal(Literal::String("ON".to_string())), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), Expression::Boolean(crate::expressions::BooleanLiteral { value: true })),
                        (Expression::Eq(Box::new(BinaryOp { left: upper_text, right: Expression::Literal(Literal::String("OFF".to_string())), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })), Expression::Boolean(crate::expressions::BooleanLiteral { value: false })),
                        (Expression::Or(Box::new(BinaryOp {
                            left: Expression::Function(Box::new(Function::new("ISNAN".to_string(), vec![Expression::TryCast(Box::new(Cast { this: arg.clone(), to: DataType::Custom { name: "REAL".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))]))),
                            right: Expression::Function(Box::new(Function::new("ISINF".to_string(), vec![Expression::TryCast(Box::new(Cast { this: arg.clone(), to: DataType::Custom { name: "REAL".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))]))),
                            left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                        })), Expression::Function(Box::new(Function::new("ERROR".to_string(), vec![Expression::Literal(Literal::String("TO_BOOLEAN: Non-numeric values NaN and INF are not supported".to_string()))])))),
                    ],
                    else_: Some(Expression::Cast(Box::new(Cast { this: arg, to: DataType::Boolean, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))),
                })))
            }

            // OBJECT_INSERT(obj, key, value) -> STRUCT_INSERT(obj, key := value)
            // Special case: OBJECT_INSERT(OBJECT_CONSTRUCT(), key, value) -> STRUCT_PACK(key := value)
            "OBJECT_INSERT" if f.args.len() == 3 => {
                let mut args = f.args;
                let obj = args.remove(0);
                let key = args.remove(0);
                let value = args.remove(0);
                // Extract key string for named arg
                let key_name = match &key {
                    Expression::Literal(Literal::String(s)) => s.clone(),
                    _ => "key".to_string(),
                };
                let named_arg = Expression::NamedArgument(Box::new(crate::expressions::NamedArgument {
                    name: Identifier::new(&key_name),
                    value,
                    separator: crate::expressions::NamedArgSeparator::ColonEq,
                }));
                // Check if the inner object is an empty STRUCT_PACK or OBJECT_CONSTRUCT
                let is_empty_struct = match &obj {
                    Expression::Struct(s) if s.fields.is_empty() => true,
                    Expression::Function(f) => {
                        let n = f.name.to_uppercase();
                        (n == "STRUCT_PACK" || n == "OBJECT_CONSTRUCT") && f.args.is_empty()
                    }
                    _ => false,
                };
                if is_empty_struct {
                    // Collapse: OBJECT_INSERT(empty, key, value) -> STRUCT_PACK(key := value)
                    Ok(Expression::Function(Box::new(Function::new(
                        "STRUCT_PACK".to_string(),
                        vec![named_arg],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "STRUCT_INSERT".to_string(),
                        vec![obj, named_arg],
                    ))))
                }
            }

            // GET(array_or_obj, key) -> array[key+1] for arrays, obj -> '$.key' for objects
            "GET" if f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let key = args.remove(0);
                match &key {
                    // String key -> JSON extract (object access)
                    Expression::Literal(Literal::String(s)) => {
                        let json_path = format!("$.{}", s);
                        Ok(Expression::JsonExtract(Box::new(JsonExtractFunc {
                            this,
                            path: Expression::Literal(Literal::String(json_path)),
                            returning: None,
                            arrow_syntax: true,
                            hash_arrow_syntax: false,
                            wrapper_option: None,
                            quotes_option: None,
                            on_scalar_string: false,
                            on_error: None,
                        })))
                    }
                    // Numeric key -> array subscript
                    // For MAP access: key is used as-is (map[key])
                    // For ARRAY access: Snowflake is 0-based, DuckDB is 1-based, so add 1
                    Expression::Literal(Literal::Number(n)) => {
                        let idx: i64 = n.parse().unwrap_or(0);
                        let is_map = matches!(&this, Expression::Cast(c) if matches!(c.to, DataType::Map { .. }));
                        let index_val = if is_map { idx } else { idx + 1 };
                        Ok(Expression::Subscript(Box::new(crate::expressions::Subscript {
                            this,
                            index: Expression::number(index_val),
                        })))
                    }
                    _ => {
                        // Unknown key type - use JSON arrow
                        Ok(Expression::JsonExtract(Box::new(JsonExtractFunc {
                            this,
                            path: Expression::JSONPath(Box::new(JSONPath { expressions: vec![Expression::JSONPathRoot(JSONPathRoot), Expression::JSONPathKey(Box::new(JSONPathKey { this: Box::new(key) }))], escape: None })),
                            returning: None,
                            arrow_syntax: true,
                            hash_arrow_syntax: false,
                            wrapper_option: None,
                            quotes_option: None,
                            on_scalar_string: false,
                            on_error: None,
                        })))
                    }
                }
            }

            // GET_PATH(obj, path) -> obj -> json_path in DuckDB
            "GET_PATH" if f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let path = args.remove(0);
                // Convert Snowflake path to JSONPath
                let json_path = match &path {
                    Expression::Literal(Literal::String(s)) => {
                        // Convert bracket notation ["key"] to quoted dot notation ."key"
                        let s = Self::convert_bracket_to_quoted_path(s);
                        // Convert Snowflake path (e.g., 'attr[0].name' or '[0].attr') to JSON path ($.attr[0].name or $[0].attr)
                        let normalized = if s.starts_with('$') {
                            s
                        } else if s.starts_with('[') {
                            format!("${}", s)
                        } else {
                            format!("$.{}", s)
                        };
                        Expression::Literal(Literal::String(normalized))
                    }
                    _ => path,
                };
                Ok(Expression::JsonExtract(Box::new(JsonExtractFunc {
                    this,
                    path: json_path,
                    returning: None,
                    arrow_syntax: true,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

            // BASE64_ENCODE(x) -> TO_BASE64(x)
            "BASE64_ENCODE" if f.args.len() == 1 => {
                Ok(Expression::Function(Box::new(Function::new("TO_BASE64".to_string(), f.args))))
            }

            // BASE64_ENCODE(x, max_line_length) -> RTRIM(REGEXP_REPLACE(TO_BASE64(x), '(.{N})', '\1' || CHR(10), 'g'), CHR(10))
            "BASE64_ENCODE" if f.args.len() >= 2 => {
                let mut args = f.args;
                let x = args.remove(0);
                let line_len = args.remove(0);
                let line_len_str = match &line_len {
                    Expression::Literal(Literal::Number(n)) => n.clone(),
                    _ => "76".to_string(),
                };
                let to_base64 = Expression::Function(Box::new(Function::new("TO_BASE64".to_string(), vec![x])));
                let pattern = format!("(.{{{}}})", line_len_str);
                let chr_10 = Expression::Function(Box::new(Function::new("CHR".to_string(), vec![Expression::number(10)])));
                let replacement = Expression::Concat(Box::new(BinaryOp {
                    left: Expression::Literal(Literal::String("\\1".to_string())),
                    right: chr_10.clone(),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));
                let regexp_replace = Expression::Function(Box::new(Function::new("REGEXP_REPLACE".to_string(), vec![
                    to_base64,
                    Expression::Literal(Literal::String(pattern)),
                    replacement,
                    Expression::Literal(Literal::String("g".to_string())),
                ])));
                Ok(Expression::Function(Box::new(Function::new("RTRIM".to_string(), vec![regexp_replace, chr_10]))))
            }

            // TRY_TO_DATE with 2 args -> CAST(CAST(TRY_STRPTIME(value, fmt) AS TIMESTAMP) AS DATE)
            "TRY_TO_DATE" if f.args.len() == 2 => {
                let mut args = f.args;
                let value = args.remove(0);
                let fmt = self.convert_snowflake_date_format(args.remove(0));
                Ok(Expression::Cast(Box::new(Cast {
                    this: Expression::Cast(Box::new(Cast {
                        this: Expression::Function(Box::new(Function::new("TRY_STRPTIME".to_string(), vec![value, fmt]))),
                        to: DataType::Timestamp { precision: None, timezone: false },
                        trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                    })),
                    to: DataType::Date,
                    trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                })))
            }

            // REGEXP_REPLACE with 4 args: check if 4th arg is a number (Snowflake position) or flags (DuckDB native)
            // REGEXP_REPLACE with 4 args: check if 4th is a string flag (DuckDB native) or a numeric position
            "REGEXP_REPLACE" if f.args.len() == 4 => {
                let is_snowflake_position = matches!(&f.args[3], Expression::Literal(Literal::Number(_)));
                if is_snowflake_position {
                    // Snowflake form: REGEXP_REPLACE(subject, pattern, replacement, position) -> add 'g' flag
                    let mut args = f.args;
                    let subject = args.remove(0);
                    let pattern = args.remove(0);
                    let replacement = args.remove(0);
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_REPLACE".to_string(),
                        vec![subject, pattern, replacement, Expression::Literal(Literal::String("g".to_string()))],
                    ))))
                } else {
                    // DuckDB native form (string flags) or pass through
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // REGEXP_REPLACE with 5+ args -> Snowflake form: (subject, pattern, replacement, position, occurrence, params)
            "REGEXP_REPLACE" if f.args.len() >= 5 => {
                let mut args = f.args;
                let subject = args.remove(0);
                let pattern = args.remove(0);
                let replacement = args.remove(0);
                let _position = args.remove(0);
                let occurrence = if !args.is_empty() { Some(args.remove(0)) } else { None };
                let params = if !args.is_empty() { Some(args.remove(0)) } else { None };

                let mut flags = String::new();
                if let Some(Expression::Literal(Literal::String(p))) = &params {
                    flags = p.clone();
                }
                let is_global = match &occurrence {
                    Some(Expression::Literal(Literal::Number(n))) => n == "0",
                    None => true,
                    _ => false,
                };
                if is_global && !flags.contains('g') {
                    flags.push('g');
                }

                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_REPLACE".to_string(),
                    vec![subject, pattern, replacement, Expression::Literal(Literal::String(flags))],
                ))))
            }

            // ROUND with named args (EXPR =>, SCALE =>, ROUNDING_MODE =>)
            "ROUND" if f.args.iter().any(|a| matches!(a, Expression::NamedArgument(_))) => {
                let mut expr_val = None;
                let mut scale_val = None;
                let mut rounding_mode = None;
                for arg in &f.args {
                    if let Expression::NamedArgument(na) = arg {
                        match na.name.name.to_uppercase().as_str() {
                            "EXPR" => expr_val = Some(na.value.clone()),
                            "SCALE" => scale_val = Some(na.value.clone()),
                            "ROUNDING_MODE" => rounding_mode = Some(na.value.clone()),
                            _ => {}
                        }
                    }
                }
                if let Some(expr) = expr_val {
                    let scale = scale_val.unwrap_or(Expression::number(0));
                    let is_half_to_even = match &rounding_mode {
                        Some(Expression::Literal(Literal::String(s))) => s == "HALF_TO_EVEN",
                        _ => false,
                    };
                    if is_half_to_even {
                        Ok(Expression::Function(Box::new(Function::new("ROUND_EVEN".to_string(), vec![expr, scale]))))
                    } else {
                        Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![expr, scale]))))
                    }
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // ROUND(x, scale, 'HALF_TO_EVEN') -> ROUND_EVEN(x, scale)
            // ROUND(x, scale, 'HALF_AWAY_FROM_ZERO') -> ROUND(x, scale)
            "ROUND" if f.args.len() == 3 => {
                let mut args = f.args;
                let x = args.remove(0);
                let scale = args.remove(0);
                let mode = args.remove(0);
                let is_half_to_even = match &mode {
                    Expression::Literal(Literal::String(s)) => s == "HALF_TO_EVEN",
                    _ => false,
                };
                if is_half_to_even {
                    Ok(Expression::Function(Box::new(Function::new("ROUND_EVEN".to_string(), vec![x, scale]))))
                } else {
                    // HALF_AWAY_FROM_ZERO is default in DuckDB, just drop the mode
                    Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![x, scale]))))
                }
            }

            // ROUND(x, scale) where scale is non-integer -> ROUND(x, CAST(scale AS INT))
            "ROUND" if f.args.len() == 2 => {
                let mut args = f.args;
                let x = args.remove(0);
                let scale = args.remove(0);
                let needs_cast = match &scale {
                    Expression::Literal(Literal::Number(n)) => n.contains('.'),
                    Expression::Cast(_) => {
                        // Already has a CAST - wrap in another CAST to INT
                        true
                    }
                    _ => false,
                };
                if needs_cast {
                    Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![x, Expression::Cast(Box::new(Cast { this: scale, to: DataType::Int { length: None, integer_spelling: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))]))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![x, scale]))))
                }
            }

            // FLOOR(x, scale) -> ROUND(FLOOR(x * POWER(10, scale)) / POWER(10, scale), scale)
            "FLOOR" if f.args.len() == 2 => {
                let mut args = f.args;
                let x = args.remove(0);
                let scale = args.remove(0);
                // Check if scale needs CAST to INT
                let needs_cast = match &scale {
                    Expression::Literal(Literal::Number(n)) => n.contains('.'),
                    _ => false,
                };
                let int_scale = if needs_cast {
                    Expression::Cast(Box::new(Cast { this: scale.clone(), to: DataType::Int { length: None, integer_spelling: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))
                } else {
                    scale.clone()
                };
                let power_10 = Expression::Function(Box::new(Function::new("POWER".to_string(), vec![Expression::number(10), int_scale.clone()])));
                let x_paren = match &x { Expression::Add(_) | Expression::Sub(_) | Expression::Mul(_) | Expression::Div(_) => Expression::Paren(Box::new(Paren { this: x, trailing_comments: Vec::new() })), _ => x };
                let multiplied = Expression::Mul(Box::new(BinaryOp { left: x_paren, right: power_10.clone(), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                let floored = Expression::Function(Box::new(Function::new("FLOOR".to_string(), vec![multiplied])));
                let divided = Expression::Div(Box::new(BinaryOp { left: floored, right: power_10, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![divided, int_scale]))))
            }

            // CEIL(x, scale) -> ROUND(CEIL(x * POWER(10, scale)) / POWER(10, scale), scale)
            "CEIL" | "CEILING" if f.args.len() == 2 => {
                let mut args = f.args;
                let x = args.remove(0);
                let scale = args.remove(0);
                let needs_cast = match &scale {
                    Expression::Literal(Literal::Number(n)) => n.contains('.'),
                    _ => false,
                };
                let int_scale = if needs_cast {
                    Expression::Cast(Box::new(Cast { this: scale.clone(), to: DataType::Int { length: None, integer_spelling: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))
                } else {
                    scale.clone()
                };
                let power_10 = Expression::Function(Box::new(Function::new("POWER".to_string(), vec![Expression::number(10), int_scale.clone()])));
                let x_paren = match &x { Expression::Add(_) | Expression::Sub(_) | Expression::Mul(_) | Expression::Div(_) => Expression::Paren(Box::new(Paren { this: x, trailing_comments: Vec::new() })), _ => x };
                let multiplied = Expression::Mul(Box::new(BinaryOp { left: x_paren, right: power_10.clone(), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                let ceiled = Expression::Function(Box::new(Function::new("CEIL".to_string(), vec![multiplied])));
                let divided = Expression::Div(Box::new(BinaryOp { left: ceiled, right: power_10, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![divided, int_scale]))))
            }

            // ADD_MONTHS(date, n) -> CASE WHEN LAST_DAY(date) = date THEN LAST_DAY(date + INTERVAL n MONTH) ELSE date + INTERVAL n MONTH END
            "ADD_MONTHS" if f.args.len() == 2 => {
                let mut args = f.args;
                let date_expr_raw = args.remove(0);
                let months_expr = args.remove(0);

                // Track whether the raw expression was a string literal
                let was_string_literal = matches!(&date_expr_raw, Expression::Literal(Literal::String(_)));

                // Wrap string literals in CAST(... AS TIMESTAMP) for DuckDB
                let date_expr = match &date_expr_raw {
                    Expression::Literal(Literal::String(_)) => {
                        Expression::Cast(Box::new(Cast {
                            this: date_expr_raw,
                            to: DataType::Timestamp { precision: None, timezone: false },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        }))
                    }
                    _ => date_expr_raw,
                };

                // Determine the type of the date expression for outer CAST
                // But NOT if the CAST was added by us (for string literal wrapping)
                let date_type = if was_string_literal {
                    None
                } else {
                    match &date_expr {
                        Expression::Cast(c) => Some(c.to.clone()),
                        _ => None,
                    }
                };

                // Determine interval expression - for non-integer months, use TO_MONTHS(CAST(ROUND(n) AS INT))
                let is_non_integer_months = match &months_expr {
                    Expression::Literal(Literal::Number(n)) => n.contains('.'),
                    Expression::Neg(_) => {
                        if let Expression::Neg(um) = &months_expr {
                            matches!(&um.this, Expression::Literal(Literal::Number(n)) if n.contains('.'))
                        } else { false }
                    }
                    // Cast to DECIMAL type means non-integer months
                    Expression::Cast(c) => matches!(&c.to, DataType::Decimal { .. }),
                    _ => false,
                };

                let is_negative = match &months_expr {
                    Expression::Neg(_) => true,
                    Expression::Literal(Literal::Number(n)) => n.starts_with('-'),
                    _ => false,
                };
                let is_null = matches!(&months_expr, Expression::Null(_));

                let interval_expr = if is_non_integer_months {
                    // For non-integer: TO_MONTHS(CAST(ROUND(n) AS INT))
                    Expression::Function(Box::new(Function::new("TO_MONTHS".to_string(), vec![
                        Expression::Cast(Box::new(Cast {
                            this: Expression::Function(Box::new(Function::new("ROUND".to_string(), vec![months_expr.clone()]))),
                            to: DataType::Int { length: None, integer_spelling: false },
                            trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                        }))
                    ])))
                } else if is_negative || is_null {
                    // For negative or NULL: INTERVAL (n) MONTH
                    Expression::Interval(Box::new(Interval {
                        this: Some(Expression::Paren(Box::new(Paren { this: months_expr.clone(), trailing_comments: Vec::new() }))),
                        unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Month, use_plural: false }),
                    }))
                } else {
                    // For positive integer: INTERVAL n MONTH
                    Expression::Interval(Box::new(Interval {
                        this: Some(months_expr.clone()),
                        unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Month, use_plural: false }),
                    }))
                };

                let date_plus_interval = Expression::Add(Box::new(BinaryOp {
                    left: date_expr.clone(),
                    right: interval_expr.clone(),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(
                        Expression::Eq(Box::new(BinaryOp {
                            left: Expression::Function(Box::new(Function::new("LAST_DAY".to_string(), vec![date_expr.clone()]))),
                            right: date_expr.clone(),
                            left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                        })),
                        Expression::Function(Box::new(Function::new("LAST_DAY".to_string(), vec![date_plus_interval.clone()]))),
                    )],
                    else_: Some(date_plus_interval),
                }));

                // Wrap in CAST if date had explicit type
                if let Some(dt) = date_type {
                    Ok(Expression::Cast(Box::new(Cast {
                        this: case_expr,
                        to: dt,
                        trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                    })))
                } else {
                    Ok(case_expr)
                }
            }

            // TIME_SLICE(date, n, 'UNIT') -> TIME_BUCKET(INTERVAL n UNIT, date)
            // TIME_SLICE(date, n, 'UNIT', 'END') -> TIME_BUCKET(INTERVAL n UNIT, date) + INTERVAL n UNIT
            "TIME_SLICE" if f.args.len() >= 3 => {
                let mut args = f.args;
                let date_expr = args.remove(0);
                let n = args.remove(0);
                let unit_str = args.remove(0);
                let alignment = if !args.is_empty() { Some(args.remove(0)) } else { None };

                // Extract unit string
                let unit = match &unit_str {
                    Expression::Literal(Literal::String(s)) => s.to_uppercase(),
                    Expression::Column(c) => c.name.name.to_uppercase(),
                    Expression::Identifier(i) => i.name.to_uppercase(),
                    _ => "DAY".to_string(),
                };

                let interval_unit = match unit.as_str() {
                    "YEAR" => IntervalUnit::Year,
                    "QUARTER" => IntervalUnit::Quarter,
                    "MONTH" => IntervalUnit::Month,
                    "WEEK" => IntervalUnit::Week,
                    "DAY" => IntervalUnit::Day,
                    "HOUR" => IntervalUnit::Hour,
                    "MINUTE" => IntervalUnit::Minute,
                    "SECOND" => IntervalUnit::Second,
                    _ => IntervalUnit::Day,
                };

                let interval = Expression::Interval(Box::new(Interval {
                    this: Some(n.clone()),
                    unit: Some(IntervalUnitSpec::Simple { unit: interval_unit.clone(), use_plural: false }),
                }));

                let time_bucket = Expression::Function(Box::new(Function::new("TIME_BUCKET".to_string(), vec![interval.clone(), date_expr.clone()])));

                let is_end = match &alignment {
                    Some(Expression::Literal(Literal::String(s))) => s.to_uppercase() == "END",
                    _ => false,
                };

                // Determine if date is a DATE type (needs CAST)
                let is_date_type = match &date_expr {
                    Expression::Cast(c) => matches!(&c.to, DataType::Date),
                    _ => false,
                };

                if is_end {
                    let bucket_plus = Expression::Add(Box::new(BinaryOp {
                        left: time_bucket,
                        right: Expression::Interval(Box::new(Interval {
                            this: Some(n),
                            unit: Some(IntervalUnitSpec::Simple { unit: interval_unit, use_plural: false }),
                        })),
                        left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                    }));
                    if is_date_type {
                        Ok(Expression::Cast(Box::new(Cast {
                            this: bucket_plus,
                            to: DataType::Date,
                            trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                        })))
                    } else {
                        Ok(bucket_plus)
                    }
                } else {
                    Ok(time_bucket)
                }
            }

            // DATE_FROM_PARTS(year, month, day) -> CAST(MAKE_DATE(year, 1, 1) + INTERVAL (month - 1) MONTH + INTERVAL (day - 1) DAY AS DATE)
            "DATE_FROM_PARTS" | "DATEFROMPARTS" if f.args.len() == 3 => {
                let mut args = f.args;
                let year = args.remove(0);
                let month = args.remove(0);
                let day = args.remove(0);

                let make_date = Expression::Function(Box::new(Function::new("MAKE_DATE".to_string(), vec![year, Expression::number(1), Expression::number(1)])));

                // Wrap compound expressions in parens to get ((expr) - 1) instead of (expr - 1)
                let month_wrapped = match &month {
                    Expression::Add(_) | Expression::Sub(_) | Expression::Mul(_) | Expression::Div(_) => {
                        Expression::Paren(Box::new(Paren { this: month, trailing_comments: Vec::new() }))
                    }
                    _ => month,
                };
                let day_wrapped = match &day {
                    Expression::Add(_) | Expression::Sub(_) | Expression::Mul(_) | Expression::Div(_) => {
                        Expression::Paren(Box::new(Paren { this: day, trailing_comments: Vec::new() }))
                    }
                    _ => day,
                };
                let month_minus_1 = Expression::Sub(Box::new(BinaryOp { left: month_wrapped, right: Expression::number(1), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                let month_interval = Expression::Interval(Box::new(Interval {
                    this: Some(Expression::Paren(Box::new(Paren { this: month_minus_1, trailing_comments: Vec::new() }))),
                    unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Month, use_plural: false }),
                }));

                let day_minus_1 = Expression::Sub(Box::new(BinaryOp { left: day_wrapped, right: Expression::number(1), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                let day_interval = Expression::Interval(Box::new(Interval {
                    this: Some(Expression::Paren(Box::new(Paren { this: day_minus_1, trailing_comments: Vec::new() }))),
                    unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Day, use_plural: false }),
                }));

                let result = Expression::Add(Box::new(BinaryOp {
                    left: Expression::Add(Box::new(BinaryOp {
                        left: make_date,
                        right: month_interval,
                        left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                    })),
                    right: day_interval,
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                Ok(Expression::Cast(Box::new(Cast {
                    this: result,
                    to: DataType::Date,
                    trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                })))
            }

            // NEXT_DAY(date, 'day_name') -> complex expression using ISODOW
            "NEXT_DAY" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let day_name = args.remove(0);

                // Parse day name to ISO day number (1=Monday..7=Sunday)
                let day_num = match &day_name {
                    Expression::Literal(Literal::String(s)) => {
                        let upper = s.to_uppercase();
                        if upper.starts_with("MO") { Some(1) }
                        else if upper.starts_with("TU") { Some(2) }
                        else if upper.starts_with("WE") { Some(3) }
                        else if upper.starts_with("TH") { Some(4) }
                        else if upper.starts_with("FR") { Some(5) }
                        else if upper.starts_with("SA") { Some(6) }
                        else if upper.starts_with("SU") { Some(7) }
                        else { None }
                    }
                    _ => None,
                };

                let target_day_expr = if let Some(n) = day_num {
                    Expression::number(n)
                } else {
                    // Dynamic day name: CASE WHEN STARTS_WITH(UPPER(day_column), 'MO') THEN 1 ... END
                    Expression::Case(Box::new(Case {
                        operand: None,
                        whens: vec![
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("MO".to_string()))]))), Expression::number(1)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("TU".to_string()))]))), Expression::number(2)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("WE".to_string()))]))), Expression::number(3)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("TH".to_string()))]))), Expression::number(4)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("FR".to_string()))]))), Expression::number(5)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("SA".to_string()))]))), Expression::number(6)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name))), Expression::Literal(Literal::String("SU".to_string()))]))), Expression::number(7)),
                        ],
                        else_: None,
                    }))
                };

                let isodow = Expression::Function(Box::new(Function::new("ISODOW".to_string(), vec![date.clone()])));
                // ((target_day - ISODOW(date) + 6) % 7) + 1
                let diff = Expression::Add(Box::new(BinaryOp {
                    left: Expression::Paren(Box::new(Paren {
                        this: Expression::Mod(Box::new(BinaryOp {
                            left: Expression::Paren(Box::new(Paren {
                                this: Expression::Add(Box::new(BinaryOp {
                                    left: Expression::Paren(Box::new(Paren {
                                        this: Expression::Sub(Box::new(BinaryOp { left: target_day_expr, right: isodow, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })),
                                        trailing_comments: Vec::new(),
                                    })),
                                    right: Expression::number(6),
                                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                                })),
                                trailing_comments: Vec::new(),
                            })),
                            right: Expression::number(7),
                            left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                        })),
                        trailing_comments: Vec::new(),
                    })),
                    right: Expression::number(1),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                let result = Expression::Add(Box::new(BinaryOp {
                    left: date,
                    right: Expression::Interval(Box::new(Interval {
                        this: Some(Expression::Paren(Box::new(Paren { this: diff, trailing_comments: Vec::new() }))),
                        unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Day, use_plural: false }),
                    })),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                Ok(Expression::Cast(Box::new(Cast {
                    this: result,
                    to: DataType::Date,
                    trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                })))
            }

            // PREVIOUS_DAY(date, 'day_name') -> complex expression using ISODOW
            "PREVIOUS_DAY" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let day_name = args.remove(0);

                let day_num = match &day_name {
                    Expression::Literal(Literal::String(s)) => {
                        let upper = s.to_uppercase();
                        if upper.starts_with("MO") { Some(1) }
                        else if upper.starts_with("TU") { Some(2) }
                        else if upper.starts_with("WE") { Some(3) }
                        else if upper.starts_with("TH") { Some(4) }
                        else if upper.starts_with("FR") { Some(5) }
                        else if upper.starts_with("SA") { Some(6) }
                        else if upper.starts_with("SU") { Some(7) }
                        else { None }
                    }
                    _ => None,
                };

                let target_day_expr = if let Some(n) = day_num {
                    Expression::number(n)
                } else {
                    Expression::Case(Box::new(Case {
                        operand: None,
                        whens: vec![
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("MO".to_string()))]))), Expression::number(1)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("TU".to_string()))]))), Expression::number(2)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("WE".to_string()))]))), Expression::number(3)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("TH".to_string()))]))), Expression::number(4)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("FR".to_string()))]))), Expression::number(5)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name.clone()))), Expression::Literal(Literal::String("SA".to_string()))]))), Expression::number(6)),
                            (Expression::Function(Box::new(Function::new("STARTS_WITH".to_string(), vec![Expression::Upper(Box::new(UnaryFunc::new(day_name))), Expression::Literal(Literal::String("SU".to_string()))]))), Expression::number(7)),
                        ],
                        else_: None,
                    }))
                };

                let isodow = Expression::Function(Box::new(Function::new("ISODOW".to_string(), vec![date.clone()])));
                // ((ISODOW(date) - target_day + 6) % 7) + 1
                let diff = Expression::Add(Box::new(BinaryOp {
                    left: Expression::Paren(Box::new(Paren {
                        this: Expression::Mod(Box::new(BinaryOp {
                            left: Expression::Paren(Box::new(Paren {
                                this: Expression::Add(Box::new(BinaryOp {
                                    left: Expression::Paren(Box::new(Paren {
                                        this: Expression::Sub(Box::new(BinaryOp { left: isodow, right: target_day_expr, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })),
                                        trailing_comments: Vec::new(),
                                    })),
                                    right: Expression::number(6),
                                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                                })),
                                trailing_comments: Vec::new(),
                            })),
                            right: Expression::number(7),
                            left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                        })),
                        trailing_comments: Vec::new(),
                    })),
                    right: Expression::number(1),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                let result = Expression::Sub(Box::new(BinaryOp {
                    left: date,
                    right: Expression::Interval(Box::new(Interval {
                        this: Some(Expression::Paren(Box::new(Paren { this: diff, trailing_comments: Vec::new() }))),
                        unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Day, use_plural: false }),
                    })),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                Ok(Expression::Cast(Box::new(Cast {
                    this: result,
                    to: DataType::Date,
                    trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                })))
            }

            // LAST_DAY(date, YEAR) -> MAKE_DATE(EXTRACT(YEAR FROM date), 12, 31)
            // LAST_DAY(date, QUARTER) -> LAST_DAY(MAKE_DATE(EXTRACT(YEAR FROM date), EXTRACT(QUARTER FROM date) * 3, 1))
            // LAST_DAY(date, WEEK) -> CAST(date + INTERVAL ((7 - EXTRACT(DAYOFWEEK FROM date)) % 7) DAY AS DATE)
            "LAST_DAY" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let unit = args.remove(0);
                let unit_str = match &unit {
                    Expression::Column(c) => c.name.name.to_uppercase(),
                    Expression::Identifier(i) => i.name.to_uppercase(),
                    _ => String::new(),
                };

                match unit_str.as_str() {
                    "MONTH" => Ok(Expression::Function(Box::new(Function::new("LAST_DAY".to_string(), vec![date])))),
                    "YEAR" => {
                        Ok(Expression::Function(Box::new(Function::new("MAKE_DATE".to_string(), vec![
                            Expression::Extract(Box::new(crate::expressions::ExtractFunc { this: date, field: crate::expressions::DateTimeField::Year })),
                            Expression::number(12),
                            Expression::number(31),
                        ]))))
                    }
                    "QUARTER" => {
                        let year = Expression::Extract(Box::new(crate::expressions::ExtractFunc { this: date.clone(), field: crate::expressions::DateTimeField::Year }));
                        let quarter_month = Expression::Mul(Box::new(BinaryOp {
                            left: Expression::Extract(Box::new(crate::expressions::ExtractFunc { this: date, field: crate::expressions::DateTimeField::Custom("QUARTER".to_string()) })),
                            right: Expression::number(3),
                            left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                        }));
                        let make_date = Expression::Function(Box::new(Function::new("MAKE_DATE".to_string(), vec![year, quarter_month, Expression::number(1)])));
                        Ok(Expression::Function(Box::new(Function::new("LAST_DAY".to_string(), vec![make_date]))))
                    }
                    "WEEK" => {
                        let dow = Expression::Extract(Box::new(crate::expressions::ExtractFunc { this: date.clone(), field: crate::expressions::DateTimeField::Custom("DAYOFWEEK".to_string()) }));
                        let diff = Expression::Mod(Box::new(BinaryOp {
                            left: Expression::Paren(Box::new(Paren {
                                this: Expression::Sub(Box::new(BinaryOp { left: Expression::number(7), right: dow, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })),
                                trailing_comments: Vec::new(),
                            })),
                            right: Expression::number(7),
                            left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                        }));
                        let result = Expression::Add(Box::new(BinaryOp {
                            left: date,
                            right: Expression::Interval(Box::new(Interval {
                                this: Some(Expression::Paren(Box::new(Paren { this: diff, trailing_comments: Vec::new() }))),
                                unit: Some(IntervalUnitSpec::Simple { unit: IntervalUnit::Day, use_plural: false }),
                            })),
                            left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                        }));
                        Ok(Expression::Cast(Box::new(Cast { this: result, to: DataType::Date, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })))
                    }
                    _ => Ok(Expression::Function(Box::new(Function::new("LAST_DAY".to_string(), vec![date, unit])))),
                }
            }

            // SEQ1/SEQ2/SEQ4/SEQ8 -> (ROW_NUMBER() OVER (ORDER BY 1 NULLS FIRST) - 1) % range
            "SEQ1" | "SEQ2" | "SEQ4" | "SEQ8" => {
                let (range, half): (u128, u128) = match name_upper.as_str() {
                    "SEQ1" => (256, 128),
                    "SEQ2" => (65536, 32768),
                    "SEQ4" => (4294967296, 2147483648),
                    "SEQ8" => (18446744073709551616, 9223372036854775808),
                    _ => unreachable!("sequence type already matched in caller"),
                };

                let is_signed = match f.args.first() {
                    Some(Expression::Literal(Literal::Number(n))) => n == "1",
                    _ => false,
                };

                let row_num = Expression::Sub(Box::new(BinaryOp {
                    left: Expression::WindowFunction(Box::new(crate::expressions::WindowFunction {
                        this: Expression::Function(Box::new(Function::new("ROW_NUMBER".to_string(), vec![]))),
                        over: crate::expressions::Over {
                            window_name: None,
                            partition_by: vec![],
                            order_by: vec![crate::expressions::Ordered {
                                this: Expression::number(1),
                                desc: false,
                                nulls_first: Some(true),
                                explicit_asc: false,
                                with_fill: None,
                            }],
                            frame: None,
                            alias: None,
                        },
                        keep: None,
                    })),
                    right: Expression::number(1),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                let modded = Expression::Mod(Box::new(BinaryOp {
                    left: Expression::Paren(Box::new(Paren { this: row_num, trailing_comments: Vec::new() })),
                    right: Expression::Literal(Literal::Number(range.to_string())),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                if is_signed {
                    // CASE WHEN val >= half THEN val - range ELSE val END
                    let cond = Expression::Gte(Box::new(BinaryOp {
                        left: modded.clone(),
                        right: Expression::Literal(Literal::Number(half.to_string())),
                        left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                    }));
                    let signed_val = Expression::Sub(Box::new(BinaryOp {
                        left: modded.clone(),
                        right: Expression::Literal(Literal::Number(range.to_string())),
                        left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                    }));
                    Ok(Expression::Paren(Box::new(Paren {
                        this: Expression::Case(Box::new(Case {
                            operand: None,
                            whens: vec![(cond, signed_val)],
                            else_: Some(modded),
                        })),
                        trailing_comments: Vec::new(),
                    })))
                } else {
                    Ok(modded)
                }
            }

            // TABLE(fn) -> fn (unwrap TABLE wrapper for DuckDB)
            // Also handles TABLE(GENERATOR(ROWCOUNT => n)) -> RANGE(n) directly
            "TABLE" if f.args.len() == 1 => {
                let inner = f.args.into_iter().next().unwrap();
                // If inner is GENERATOR, transform it to RANGE
                if let Expression::Function(ref gen_f) = inner {
                    if gen_f.name.to_uppercase() == "GENERATOR" {
                        let mut rowcount = None;
                        for arg in &gen_f.args {
                            if let Expression::NamedArgument(na) = arg {
                                if na.name.name.to_uppercase() == "ROWCOUNT" {
                                    rowcount = Some(na.value.clone());
                                }
                            }
                        }
                        if let Some(n) = rowcount {
                            return Ok(Expression::Function(Box::new(Function::new("RANGE".to_string(), vec![n]))));
                        }
                    }
                }
                Ok(inner)
            }

            // GENERATOR(ROWCOUNT => n) -> RANGE(n) in DuckDB
            "GENERATOR" => {
                let mut rowcount = None;
                for arg in &f.args {
                    if let Expression::NamedArgument(na) = arg {
                        if na.name.name.to_uppercase() == "ROWCOUNT" {
                            rowcount = Some(na.value.clone());
                        }
                    }
                }
                if let Some(n) = rowcount {
                    Ok(Expression::Function(Box::new(Function::new("RANGE".to_string(), vec![n]))))
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // UNIFORM(low, high, gen) -> CAST(FLOOR(low + RANDOM() * (high - low + 1)) AS BIGINT)
            // or with seed: CAST(FLOOR(low + (ABS(HASH(seed)) % 1000000) / 1000000.0 * (high - low + 1)) AS BIGINT)
            "UNIFORM" if f.args.len() == 3 => {
                let mut args = f.args;
                let low = args.remove(0);
                let high = args.remove(0);
                let gen = args.remove(0);

                let range = Expression::Add(Box::new(BinaryOp {
                    left: Expression::Sub(Box::new(BinaryOp { left: high, right: low.clone(), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })),
                    right: Expression::number(1),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));

                // Check if gen is RANDOM() (function) or a literal seed
                let random_val = match &gen {
                    Expression::Rand(_) | Expression::Random(_) => {
                        // RANDOM() - use directly
                        Expression::Function(Box::new(Function::new("RANDOM".to_string(), vec![])))
                    }
                    Expression::Function(func) if func.name.to_uppercase() == "RANDOM" => {
                        // RANDOM(seed) or RANDOM() - just use RANDOM()
                        Expression::Function(Box::new(Function::new("RANDOM".to_string(), vec![])))
                    }
                    _ => {
                        // Seed-based: (ABS(HASH(seed)) % 1000000) / 1000000.0
                        let hash = Expression::Function(Box::new(Function::new("HASH".to_string(), vec![gen])));
                        let abs_hash = Expression::Abs(Box::new(UnaryFunc::new(hash)));
                        let modded = Expression::Mod(Box::new(BinaryOp { left: abs_hash, right: Expression::number(1000000), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                        let paren_modded = Expression::Paren(Box::new(Paren { this: modded, trailing_comments: Vec::new() }));
                        Expression::Div(Box::new(BinaryOp { left: paren_modded, right: Expression::Literal(Literal::Number("1000000.0".to_string())), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))
                    }
                };

                let inner = Expression::Function(Box::new(Function::new("FLOOR".to_string(), vec![
                    Expression::Add(Box::new(BinaryOp {
                        left: low,
                        right: Expression::Mul(Box::new(BinaryOp { left: random_val, right: Expression::Paren(Box::new(Paren { this: range, trailing_comments: Vec::new() })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })),
                        left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                    }))
                ])));

                Ok(Expression::Cast(Box::new(Cast {
                    this: inner,
                    to: DataType::BigInt { length: None },
                    trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None,
                })))
            }

            // NORMAL(mean, stddev, gen) -> Box-Muller transform
            // mean + (stddev * SQRT(-2 * LN(GREATEST(u1, 1e-10))) * COS(2 * PI() * u2))
            // where u1 and u2 are uniform random values derived from gen
            "NORMAL" if f.args.len() == 3 => {
                let mut args = f.args;
                let mean = args.remove(0);
                let stddev = args.remove(0);
                let gen = args.remove(0);

                // Helper to create seed-based random: (ABS(HASH(seed)) % 1000000) / 1000000.0
                let make_seed_random = |seed: Expression| -> Expression {
                    let hash = Expression::Function(Box::new(Function::new("HASH".to_string(), vec![seed])));
                    let abs_hash = Expression::Abs(Box::new(UnaryFunc::new(hash)));
                    let modded = Expression::Mod(Box::new(BinaryOp { left: abs_hash, right: Expression::number(1000000), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                    let paren_modded = Expression::Paren(Box::new(Paren { this: modded, trailing_comments: Vec::new() }));
                    Expression::Div(Box::new(BinaryOp { left: paren_modded, right: Expression::Literal(Literal::Number("1000000.0".to_string())), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))
                };

                // Determine u1 and u2 based on gen type
                let is_random_no_seed = match &gen {
                    Expression::Random(_) => true,
                    Expression::Rand(r) => r.seed.is_none(),
                    _ => false,
                };
                let (u1, u2) = if is_random_no_seed {
                    // RANDOM() -> u1 = RANDOM(), u2 = RANDOM()
                    let u1 = Expression::Function(Box::new(Function::new("RANDOM".to_string(), vec![])));
                    let u2 = Expression::Function(Box::new(Function::new("RANDOM".to_string(), vec![])));
                    (u1, u2)
                } else {
                    // Seed-based: extract the seed value
                    let seed = match gen {
                        Expression::Rand(r) => r.seed.map(|s| *s).unwrap_or(Expression::number(0)),
                        Expression::Function(func) if func.name.to_uppercase() == "RANDOM" => {
                            if func.args.len() == 1 {
                                func.args.into_iter().next().unwrap()
                            } else {
                                Expression::number(0)
                            }
                        }
                        other => other,
                    };
                    let u1 = make_seed_random(seed.clone());
                    let seed_plus_1 = Expression::Add(Box::new(BinaryOp { left: seed, right: Expression::number(1), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }));
                    let u2 = make_seed_random(seed_plus_1);
                    (u1, u2)
                };

                // GREATEST(u1, 1e-10)
                let greatest = Expression::Greatest(Box::new(VarArgFunc {
                    expressions: vec![u1, Expression::Literal(Literal::Number("1e-10".to_string()))],
                    original_name: None,
                }));

                // SQRT(-2 * LN(GREATEST(u1, 1e-10)))
                let neg2 = Expression::Neg(Box::new(crate::expressions::UnaryOp { this: Expression::number(2) }));
                let ln_greatest = Expression::Function(Box::new(Function::new("LN".to_string(), vec![greatest])));
                let neg2_times_ln = Expression::Mul(Box::new(BinaryOp {
                    left: neg2,
                    right: ln_greatest,
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));
                let sqrt_part = Expression::Function(Box::new(Function::new("SQRT".to_string(), vec![neg2_times_ln])));

                // COS(2 * PI() * u2)
                let pi = Expression::Function(Box::new(Function::new("PI".to_string(), vec![])));
                let two_pi = Expression::Mul(Box::new(BinaryOp {
                    left: Expression::number(2),
                    right: pi,
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));
                let two_pi_u2 = Expression::Mul(Box::new(BinaryOp {
                    left: two_pi,
                    right: u2,
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));
                let cos_part = Expression::Function(Box::new(Function::new("COS".to_string(), vec![two_pi_u2])));

                // stddev * sqrt_part * cos_part
                let stddev_times_sqrt = Expression::Mul(Box::new(BinaryOp {
                    left: stddev,
                    right: sqrt_part,
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));
                let inner = Expression::Mul(Box::new(BinaryOp {
                    left: stddev_times_sqrt,
                    right: cos_part,
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                }));
                let paren_inner = Expression::Paren(Box::new(Paren { this: inner, trailing_comments: Vec::new() }));

                // mean + (inner)
                Ok(Expression::Add(Box::new(BinaryOp {
                    left: mean,
                    right: paren_inner,
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                })))
            }

            // DATE_TRUNC: DuckDB supports natively, just pass through
            // (DuckDB returns the correct type automatically)

            // BITOR/BITAND with BITSHIFT need parenthesization
            // This is handled via the BITOR/BITAND transforms which create BitwiseOr/BitwiseAnd
            // The issue is operator precedence: BITOR(BITSHIFTLEFT(a, b), BITSHIFTLEFT(c, d))
            // should generate (a << b) | (c << d), not a << b | c << d

            // ZIPF(s, n, gen) -> CTE-based emulation for DuckDB
            "ZIPF" if f.args.len() == 3 => {
                let mut args = f.args;
                let s_expr = args.remove(0);
                let n_expr = args.remove(0);
                let gen_expr = args.remove(0);

                let s_sql = Self::expr_to_sql(&s_expr);
                let n_sql = Self::expr_to_sql(&n_expr);
                let (seed_sql, is_random) = Self::extract_seed_info(&gen_expr);

                let rand_sql = if is_random {
                    format!("SELECT {} AS r", seed_sql)
                } else {
                    format!("SELECT (ABS(HASH({})) % 1000000) / 1000000.0 AS r", seed_sql)
                };

                let template = format!(
                    "WITH rand AS ({}), weights AS (SELECT i, 1.0 / POWER(i, {}) AS w FROM RANGE(1, {} + 1) AS t(i)), cdf AS (SELECT i, SUM(w) OVER (ORDER BY i NULLS FIRST) / SUM(w) OVER () AS p FROM weights) SELECT MIN(i) FROM cdf WHERE p >= (SELECT r FROM rand)",
                    rand_sql, s_sql, n_sql
                );

                Self::parse_as_subquery(&template)
            }

            // RANDSTR(len, gen) -> subquery-based emulation for DuckDB
            "RANDSTR" if f.args.len() == 2 => {
                let mut args = f.args;
                let len_expr = args.remove(0);
                let gen_expr = args.remove(0);

                let len_sql = Self::expr_to_sql(&len_expr);
                let (seed_sql, is_random) = Self::extract_seed_info(&gen_expr);

                let random_value_sql = if is_random {
                    format!("(ABS(HASH(i + {})) % 1000) / 1000.0", seed_sql)
                } else {
                    format!("(ABS(HASH(i + {})) % 1000) / 1000.0", seed_sql)
                };

                let template = format!(
                    "SELECT LISTAGG(SUBSTRING('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz', 1 + CAST(FLOOR(random_value * 62) AS INT), 1), '') FROM (SELECT {} AS random_value FROM RANGE({}) AS t(i))",
                    random_value_sql, len_sql
                );

                Self::parse_as_subquery(&template)
            }

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }

    /// Convert Snowflake date format to DuckDB strptime format
    fn convert_snowflake_date_format(&self, fmt: Expression) -> Expression {
        match fmt {
            Expression::Literal(Literal::String(s)) => {
                let converted = Self::snowflake_to_strptime(&s);
                Expression::Literal(Literal::String(converted))
            }
            _ => fmt,
        }
    }

    /// Convert Snowflake time format to DuckDB strptime format
    fn convert_snowflake_time_format(&self, fmt: Expression) -> Expression {
        match fmt {
            Expression::Literal(Literal::String(s)) => {
                let converted = Self::snowflake_to_strptime(&s);
                Expression::Literal(Literal::String(converted))
            }
            _ => fmt,
        }
    }

    /// Token-based conversion from Snowflake format strings (both original and normalized) to DuckDB strptime format.
    /// Handles both uppercase Snowflake originals (YYYY, MM, DD) and normalized lowercase forms (yyyy, mm, DD).
    fn snowflake_to_strptime(s: &str) -> String {
        let mut result = String::new();
        let chars: Vec<char> = s.chars().collect();
        let len = chars.len();
        let mut i = 0;
        while i < len {
            let remaining = &s[i..];
            let remaining_upper: String = remaining.chars().take(8).collect::<String>().to_uppercase();

            // Compound patterns first
            if remaining_upper.starts_with("HH24MISS") {
                result.push_str("%H%M%S");
                i += 8;
            } else if remaining_upper.starts_with("MMMM") {
                result.push_str("%B");
                i += 4;
            } else if remaining_upper.starts_with("YYYY") {
                result.push_str("%Y");
                i += 4;
            } else if remaining_upper.starts_with("YY") {
                result.push_str("%y");
                i += 2;
            } else if remaining_upper.starts_with("MON") {
                result.push_str("%b");
                i += 3;
            } else if remaining_upper.starts_with("HH24") {
                result.push_str("%H");
                i += 4;
            } else if remaining_upper.starts_with("HH12") {
                result.push_str("%I");
                i += 4;
            } else if remaining_upper.starts_with("HH") {
                result.push_str("%I");
                i += 2;
            } else if remaining_upper.starts_with("MISS") {
                result.push_str("%M%S");
                i += 4;
            } else if remaining_upper.starts_with("MI") {
                result.push_str("%M");
                i += 2;
            } else if remaining_upper.starts_with("MM") {
                result.push_str("%m");
                i += 2;
            } else if remaining_upper.starts_with("DD") {
                result.push_str("%d");
                i += 2;
            } else if remaining_upper.starts_with("DY") {
                result.push_str("%a");
                i += 2;
            } else if remaining_upper.starts_with("SS") {
                result.push_str("%S");
                i += 2;
            } else if remaining_upper.starts_with("FF") {
                // FF with optional digit (FF, FF1-FF9)
                // %f = microseconds (6 digits, FF1-FF6), %n = nanoseconds (9 digits, FF7-FF9)
                let ff_pos = i + 2;
                if ff_pos < len && chars[ff_pos].is_ascii_digit() {
                    let digit = chars[ff_pos].to_digit(10).unwrap_or(6);
                    if digit >= 7 {
                        result.push_str("%n");
                    } else {
                        result.push_str("%f");
                    }
                    i += 3; // skip FF + digit
                } else {
                    result.push_str("%f");
                    i += 2;
                }
            } else if remaining_upper.starts_with("PM") || remaining_upper.starts_with("AM") {
                result.push_str("%p");
                i += 2;
            } else if remaining_upper.starts_with("TZH") {
                result.push_str("%z");
                i += 3;
            } else if remaining_upper.starts_with("TZM") {
                // TZM is part of timezone, skip
                i += 3;
            } else {
                result.push(chars[i]);
                i += 1;
            }
        }
        result
    }

    /// Convert BigQuery format string to DuckDB strptime format
    /// BigQuery: %E6S -> DuckDB: %S.%f (seconds with microseconds)
    fn convert_bq_to_strptime_format(&self, fmt: Expression) -> Expression {
        match fmt {
            Expression::Literal(Literal::String(s)) => {
                let converted = s
                    .replace("%E6S", "%S.%f")
                    .replace("%E*S", "%S.%f");
                Expression::Literal(Literal::String(converted))
            }
            _ => fmt,
        }
    }

    /// Transform DATE_PART(unit, expr) for DuckDB
    fn transform_date_part(&self, args: Vec<Expression>) -> Result<Expression> {
        let mut args = args;
        let unit_expr = args.remove(0);
        let date_expr = args.remove(0);
        let unit_name = match &unit_expr {
            Expression::Column(c) => c.name.name.to_uppercase(),
            Expression::Identifier(i) => i.name.to_uppercase(),
            Expression::Literal(Literal::String(s)) => s.to_uppercase(),
            _ => return Ok(Expression::Function(Box::new(Function::new("DATE_PART".to_string(), vec![unit_expr, date_expr])))),
        };
        match unit_name.as_str() {
            "EPOCH_SECOND" | "EPOCH" => Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("EPOCH".to_string(), vec![date_expr]))), to: DataType::BigInt { length: None }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))),
            "EPOCH_MILLISECOND" | "EPOCH_MILLISECONDS" => Ok(Expression::Function(Box::new(Function::new("EPOCH_MS".to_string(), vec![date_expr])))),
            "EPOCH_MICROSECOND" | "EPOCH_MICROSECONDS" => Ok(Expression::Function(Box::new(Function::new("EPOCH_US".to_string(), vec![date_expr])))),
            "EPOCH_NANOSECOND" | "EPOCH_NANOSECONDS" => Ok(Expression::Function(Box::new(Function::new("EPOCH_NS".to_string(), vec![date_expr])))),
            "DAYOFWEEKISO" | "DAYOFWEEK_ISO" => Ok(Expression::Extract(Box::new(crate::expressions::ExtractFunc { this: date_expr, field: crate::expressions::DateTimeField::Custom("ISODOW".to_string()) }))),
            "YEAROFWEEK" | "YEAROFWEEKISO" => Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("STRFTIME".to_string(), vec![date_expr, Expression::Literal(Literal::String("%G".to_string()))]))), to: DataType::Int { length: None, integer_spelling: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))),
            "WEEKISO" => Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("STRFTIME".to_string(), vec![date_expr, Expression::Literal(Literal::String("%V".to_string()))]))), to: DataType::Int { length: None, integer_spelling: false }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))),
            "NANOSECOND" | "NANOSECONDS" | "NS" => Ok(Expression::Cast(Box::new(Cast { this: Expression::Function(Box::new(Function::new("STRFTIME".to_string(), vec![Expression::Cast(Box::new(Cast { this: date_expr, to: DataType::Custom { name: "TIMESTAMP_NS".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })), Expression::Literal(Literal::String("%n".to_string()))]))), to: DataType::BigInt { length: None }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))),
            "DAYOFMONTH" => Ok(Expression::Extract(Box::new(crate::expressions::ExtractFunc { this: date_expr, field: crate::expressions::DateTimeField::Day }))),
            _ => {
                let field = match unit_name.as_str() {
                    "YEAR" | "YY" | "YYYY" => crate::expressions::DateTimeField::Year,
                    "MONTH" | "MON" | "MM" => crate::expressions::DateTimeField::Month,
                    "DAY" | "DD" | "D" => crate::expressions::DateTimeField::Day,
                    "HOUR" | "HH" => crate::expressions::DateTimeField::Hour,
                    "MINUTE" | "MI" | "MIN" => crate::expressions::DateTimeField::Minute,
                    "SECOND" | "SEC" | "SS" => crate::expressions::DateTimeField::Second,
                    "MILLISECOND" | "MS" => crate::expressions::DateTimeField::Millisecond,
                    "MICROSECOND" | "US" => crate::expressions::DateTimeField::Microsecond,
                    "QUARTER" | "QTR" => crate::expressions::DateTimeField::Quarter,
                    "WEEK" | "WK" => crate::expressions::DateTimeField::Week,
                    "DAYOFWEEK" | "DOW" => crate::expressions::DateTimeField::DayOfWeek,
                    "DAYOFYEAR" | "DOY" => crate::expressions::DateTimeField::DayOfYear,
                    "TIMEZONE_HOUR" => crate::expressions::DateTimeField::TimezoneHour,
                    "TIMEZONE_MINUTE" => crate::expressions::DateTimeField::TimezoneMinute,
                    _ => crate::expressions::DateTimeField::Custom(unit_name),
                };
                Ok(Expression::Extract(Box::new(crate::expressions::ExtractFunc { this: date_expr, field })))
            }
        }
    }

    /// Transform DATEADD(unit, amount, date) for DuckDB
    fn transform_dateadd(&self, args: Vec<Expression>) -> Result<Expression> {
        let mut args = args;
        let unit_expr = args.remove(0);
        let amount = args.remove(0);
        let date = args.remove(0);
        let unit_name = match &unit_expr {
            Expression::Column(c) => c.name.name.to_uppercase(),
            Expression::Identifier(i) => i.name.to_uppercase(),
            Expression::Literal(Literal::String(s)) => s.to_uppercase(),
            _ => String::new(),
        };
        if unit_name == "NANOSECOND" || unit_name == "NS" {
            let epoch_ns = Expression::Function(Box::new(Function::new("EPOCH_NS".to_string(), vec![Expression::Cast(Box::new(Cast { this: date, to: DataType::Custom { name: "TIMESTAMP_NS".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))])));
            return Ok(Expression::Function(Box::new(Function::new("MAKE_TIMESTAMP_NS".to_string(), vec![Expression::Add(Box::new(BinaryOp { left: epoch_ns, right: amount, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))]))));
        }
        let (interval_unit, multiplied_amount) = match unit_name.as_str() {
            "YEAR" | "YY" | "YYYY" => (IntervalUnit::Year, amount),
            "MONTH" | "MON" | "MM" => (IntervalUnit::Month, amount),
            "DAY" | "DD" | "D" => (IntervalUnit::Day, amount),
            "HOUR" | "HH" => (IntervalUnit::Hour, amount),
            "MINUTE" | "MI" | "MIN" => (IntervalUnit::Minute, amount),
            "SECOND" | "SEC" | "SS" => (IntervalUnit::Second, amount),
            "MILLISECOND" | "MS" => (IntervalUnit::Millisecond, amount),
            "MICROSECOND" | "US" => (IntervalUnit::Microsecond, amount),
            "WEEK" | "WK" => (IntervalUnit::Day, Expression::Mul(Box::new(BinaryOp { left: amount, right: Expression::number(7), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))),
            "QUARTER" | "QTR" => (IntervalUnit::Month, Expression::Mul(Box::new(BinaryOp { left: amount, right: Expression::number(3), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() }))),
            _ => (IntervalUnit::Day, amount),
        };
        Ok(Expression::Add(Box::new(BinaryOp { left: date, right: Expression::Interval(Box::new(Interval { this: Some(multiplied_amount), unit: Some(IntervalUnitSpec::Simple { unit: interval_unit, use_plural: false }) })), left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })))
    }

    /// Transform DATEDIFF(unit, start, end) for DuckDB
    fn transform_datediff(&self, args: Vec<Expression>) -> Result<Expression> {
        let mut args = args;
        let unit_expr = args.remove(0);
        let start = args.remove(0);
        let end = args.remove(0);
        let unit_name = match &unit_expr {
            Expression::Column(c) => c.name.name.to_uppercase(),
            Expression::Identifier(i) => i.name.to_uppercase(),
            Expression::Literal(Literal::String(s)) => s.to_uppercase(),
            _ => String::new(),
        };
        if unit_name == "NANOSECOND" || unit_name == "NS" {
            let epoch_end = Expression::Function(Box::new(Function::new("EPOCH_NS".to_string(), vec![Expression::Cast(Box::new(Cast { this: end, to: DataType::Custom { name: "TIMESTAMP_NS".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))])));
            let epoch_start = Expression::Function(Box::new(Function::new("EPOCH_NS".to_string(), vec![Expression::Cast(Box::new(Cast { this: start, to: DataType::Custom { name: "TIMESTAMP_NS".to_string() }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))])));
            return Ok(Expression::Sub(Box::new(BinaryOp { left: epoch_end, right: epoch_start, left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new() })));
        }
        if unit_name == "WEEK" || unit_name == "WK" {
            let trunc_start = Expression::Function(Box::new(Function::new("DATE_TRUNC".to_string(), vec![Expression::Literal(Literal::String("WEEK".to_string())), Expression::Cast(Box::new(Cast { this: start, to: DataType::Date, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))])));
            let trunc_end = Expression::Function(Box::new(Function::new("DATE_TRUNC".to_string(), vec![Expression::Literal(Literal::String("WEEK".to_string())), Expression::Cast(Box::new(Cast { this: end, to: DataType::Date, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None }))])));
            return Ok(Expression::Function(Box::new(Function::new("DATE_DIFF".to_string(), vec![Expression::Literal(Literal::String("WEEK".to_string())), trunc_start, trunc_end]))));
        }
        let cast_if_string = |e: Expression| -> Expression { match &e { Expression::Literal(Literal::String(_)) => Expression::Cast(Box::new(Cast { this: e, to: DataType::Date, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })), _ => e } };
        let start = cast_if_string(start);
        let end = cast_if_string(end);
        Ok(Expression::Function(Box::new(Function::new("DATE_DIFF".to_string(), vec![Expression::Literal(Literal::String(unit_name)), start, end]))))
    }

    fn transform_aggregate_function(
        &self,
        f: Box<crate::expressions::AggregateFunction>,
    ) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // GROUP_CONCAT -> LISTAGG
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // LISTAGG is native to DuckDB
            "LISTAGG" => Ok(Expression::AggregateFunction(f)),

            // STRING_AGG -> LISTAGG
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "LISTAGG".to_string(),
                f.args,
            )))),

            // ARRAY_AGG -> list (or array_agg, both work)
            "ARRAY_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "list".to_string(),
                f.args,
            )))),

            // LOGICAL_OR -> BOOL_OR with CAST to BOOLEAN
            "LOGICAL_OR" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "BOOL_OR".to_string(),
                    vec![Expression::Cast(Box::new(crate::expressions::Cast {
                        this: arg,
                        to: crate::expressions::DataType::Boolean,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))],
                ))))
            }

            // LOGICAL_AND -> BOOL_AND with CAST to BOOLEAN
            "LOGICAL_AND" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "BOOL_AND".to_string(),
                    vec![Expression::Cast(Box::new(crate::expressions::Cast {
                        this: arg,
                        to: crate::expressions::DataType::Boolean,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))],
                ))))
            }

            // SKEW -> SKEWNESS
            "SKEW" => Ok(Expression::Function(Box::new(Function::new(
                "SKEWNESS".to_string(),
                f.args,
            )))),

            // REGR_VALX(y, x) -> CASE WHEN y IS NULL THEN CAST(NULL AS DOUBLE) ELSE x END
            "REGR_VALX" if f.args.len() == 2 => {
                let mut args = f.args;
                let y = args.remove(0);
                let x = args.remove(0);
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(
                        Expression::IsNull(Box::new(crate::expressions::IsNull { this: y, not: false, postfix_form: false })),
                        Expression::Cast(Box::new(Cast { this: Expression::Null(crate::expressions::Null), to: DataType::Double { precision: None, scale: None }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })),
                    )],
                    else_: Some(x),
                })))
            }

            // REGR_VALY(y, x) -> CASE WHEN x IS NULL THEN CAST(NULL AS DOUBLE) ELSE y END
            "REGR_VALY" if f.args.len() == 2 => {
                let mut args = f.args;
                let y = args.remove(0);
                let x = args.remove(0);
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(
                        Expression::IsNull(Box::new(crate::expressions::IsNull { this: x, not: false, postfix_form: false })),
                        Expression::Cast(Box::new(Cast { this: Expression::Null(crate::expressions::Null), to: DataType::Double { precision: None, scale: None }, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })),
                    )],
                    else_: Some(y),
                })))
            }

            // BOOLAND_AGG -> BOOL_AND(CAST(arg AS BOOLEAN))
            "BOOLAND_AGG" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "BOOL_AND".to_string(),
                    vec![Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Boolean,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))],
                ))))
            }

            // BOOLOR_AGG -> BOOL_OR(CAST(arg AS BOOLEAN))
            "BOOLOR_AGG" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "BOOL_OR".to_string(),
                    vec![Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Boolean,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))],
                ))))
            }

            // BOOLXOR_AGG(c) -> COUNT_IF(CAST(c AS BOOLEAN)) = 1
            "BOOLXOR_AGG" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Eq(Box::new(BinaryOp {
                    left: Expression::Function(Box::new(Function::new("COUNT_IF".to_string(), vec![
                        Expression::Cast(Box::new(Cast { this: arg, to: DataType::Boolean, trailing_comments: Vec::new(), double_colon_syntax: false, format: None, default: None })),
                    ]))),
                    right: Expression::number(1),
                    left_comments: Vec::new(), operator_comments: Vec::new(), trailing_comments: Vec::new(),
                })))
            }

            // MAX_BY -> ARG_MAX
            "MAX_BY" if f.args.len() == 2 => {
                Ok(Expression::AggregateFunction(Box::new(crate::expressions::AggregateFunction {
                    name: "ARG_MAX".to_string(),
                    ..(*f)
                })))
            }

            // MIN_BY -> ARG_MIN
            "MIN_BY" if f.args.len() == 2 => {
                Ok(Expression::AggregateFunction(Box::new(crate::expressions::AggregateFunction {
                    name: "ARG_MIN".to_string(),
                    ..(*f)
                })))
            }

            // CORR - pass through (DuckDB handles NaN natively)
            "CORR" if f.args.len() == 2 => {
                Ok(Expression::AggregateFunction(f))
            }

            // BITMAP_CONSTRUCT_AGG(v) -> complex DuckDB subquery emulation
            "BITMAP_CONSTRUCT_AGG" if f.args.len() == 1 => {
                let v_sql = Self::expr_to_sql(&f.args[0]);

                let template = format!(
                    "SELECT CASE WHEN l IS NULL OR LENGTH(l) = 0 THEN NULL WHEN LENGTH(l) <> LENGTH(LIST_FILTER(l, __v -> __v BETWEEN 0 AND 32767)) THEN NULL WHEN LENGTH(l) < 5 THEN UNHEX(PRINTF('%04X', LENGTH(l)) || h || REPEAT('00', GREATEST(0, 4 - LENGTH(l)) * 2)) ELSE UNHEX('08000000000000000000' || h) END FROM (SELECT l, COALESCE(LIST_REDUCE(LIST_TRANSFORM(l, __x -> PRINTF('%02X%02X', CAST(__x AS INT) & 255, (CAST(__x AS INT) >> 8) & 255)), (__a, __b) -> __a || __b, ''), '') AS h FROM (SELECT LIST_SORT(LIST_DISTINCT(LIST({v}) FILTER(WHERE NOT {v} IS NULL))) AS l))",
                    v = v_sql
                );

                Self::parse_as_subquery(&template)
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    /// Convert Presto/MySQL format string to DuckDB format string
    /// DuckDB uses strftime/strptime C-style format specifiers
    /// Key difference: %i (Presto minutes) -> %M (DuckDB minutes)
    fn convert_format_to_duckdb(expr: &Expression) -> Expression {
        if let Expression::Literal(Literal::String(s)) = expr {
            let duckdb_fmt = Self::presto_to_duckdb_format(s);
            Expression::Literal(Literal::String(duckdb_fmt))
        } else {
            expr.clone()
        }
    }

    /// Convert Presto format specifiers to DuckDB strftime format
    fn presto_to_duckdb_format(fmt: &str) -> String {
        let mut result = String::new();
        let chars: Vec<char> = fmt.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            if chars[i] == '%' && i + 1 < chars.len() {
                match chars[i + 1] {
                    'i' => {
                        // Presto %i (minutes) -> DuckDB %M (minutes)
                        result.push_str("%M");
                        i += 2;
                    }
                    'T' => {
                        // Presto %T (time shorthand %H:%M:%S)
                        result.push_str("%H:%M:%S");
                        i += 2;
                    }
                    'F' => {
                        // Presto %F (date shorthand %Y-%m-%d)
                        result.push_str("%Y-%m-%d");
                        i += 2;
                    }
                    _ => {
                        result.push('%');
                        result.push(chars[i + 1]);
                        i += 2;
                    }
                }
            } else {
                result.push(chars[i]);
                i += 1;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::Dialect;

    fn transpile_to_duckdb(sql: &str) -> String {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile_to(sql, DialectType::DuckDB)
            .expect("Transpile failed");
        result[0].clone()
    }

    #[test]
    fn test_ifnull_to_coalesce() {
        let result = transpile_to_duckdb("SELECT IFNULL(a, b)");
        assert!(
            result.contains("COALESCE"),
            "Expected COALESCE, got: {}",
            result
        );
    }

    #[test]
    fn test_nvl_to_coalesce() {
        let result = transpile_to_duckdb("SELECT NVL(a, b)");
        assert!(
            result.contains("COALESCE"),
            "Expected COALESCE, got: {}",
            result
        );
    }

    #[test]
    fn test_basic_select() {
        let result = transpile_to_duckdb("SELECT a, b FROM users WHERE id = 1");
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM users"));
    }

    #[test]
    fn test_group_concat_to_listagg() {
        let result = transpile_to_duckdb("SELECT GROUP_CONCAT(name)");
        assert!(
            result.contains("LISTAGG"),
            "Expected LISTAGG, got: {}",
            result
        );
    }

    #[test]
    fn test_listagg_preserved() {
        let result = transpile_to_duckdb("SELECT LISTAGG(name)");
        assert!(
            result.contains("LISTAGG"),
            "Expected LISTAGG, got: {}",
            result
        );
    }

    #[test]
    fn test_date_format_to_strftime() {
        let result = transpile_to_duckdb("SELECT DATE_FORMAT(d, '%Y-%m-%d')");
        // Generator uppercases function names
        assert!(
            result.to_uppercase().contains("STRFTIME"),
            "Expected STRFTIME, got: {}",
            result
        );
    }

    #[test]
    fn test_regexp_like_to_regexp_matches() {
        let result = transpile_to_duckdb("SELECT REGEXP_LIKE(name, 'pattern')");
        // Generator uppercases function names
        assert!(
            result.to_uppercase().contains("REGEXP_MATCHES"),
            "Expected REGEXP_MATCHES, got: {}",
            result
        );
    }

    #[test]
    fn test_double_quote_identifiers() {
        // DuckDB uses double quotes for identifiers
        let dialect = Dialect::get(DialectType::DuckDB);
        let config = dialect.generator_config();
        assert_eq!(config.identifier_quote, '"');
    }

    /// Helper for DuckDB identity tests (parse with DuckDB, generate with DuckDB)
    fn duckdb_identity(sql: &str) -> String {
        let dialect = Dialect::get(DialectType::DuckDB);
        let ast = dialect.parse(sql).expect("Parse failed");
        let transformed = dialect.transform(ast[0].clone()).expect("Transform failed");
        dialect.generate(&transformed).expect("Generate failed")
    }

    #[test]
    fn test_interval_quoting() {
        // Test 137: INTERVAL value should be quoted for DuckDB
        let result = duckdb_identity(
            "SELECT DATE_ADD(CAST('2020-01-01' AS DATE), INTERVAL 1 DAY)",
        );
        assert_eq!(
            result,
            "SELECT CAST('2020-01-01' AS DATE) + INTERVAL '1' DAY",
            "Interval value should be quoted as string"
        );
    }

    #[test]
    fn test_struct_pack_to_curly_brace() {
        // Test 221: STRUCT_PACK should become curly brace notation
        let result = duckdb_identity(
            "CAST([STRUCT_PACK(a := 1)] AS STRUCT(a BIGINT)[])",
        );
        assert_eq!(
            result,
            "CAST([{'a': 1}] AS STRUCT(a BIGINT)[])",
            "STRUCT_PACK should be transformed to curly brace notation"
        );
    }

    #[test]
    fn test_struct_pack_nested() {
        // Test 220: Nested STRUCT_PACK
        let result = duckdb_identity(
            "CAST([[STRUCT_PACK(a := 1)]] AS STRUCT(a BIGINT)[][])",
        );
        assert_eq!(
            result,
            "CAST([[{'a': 1}]] AS STRUCT(a BIGINT)[][])",
            "Nested STRUCT_PACK should be transformed"
        );
    }

    #[test]
    fn test_struct_pack_cast() {
        // Test 222: STRUCT_PACK with :: cast
        let result = duckdb_identity(
            "STRUCT_PACK(a := 'b')::json",
        );
        assert_eq!(
            result,
            "CAST({'a': 'b'} AS JSON)",
            "STRUCT_PACK with cast should be transformed"
        );
    }

    #[test]
    fn test_list_value_to_bracket() {
        // Test 309: LIST_VALUE should become bracket notation
        let result = duckdb_identity(
            "SELECT LIST_VALUE(1)[i]",
        );
        assert_eq!(
            result,
            "SELECT [1][i]",
            "LIST_VALUE should be transformed to bracket notation"
        );
    }

    #[test]
    fn test_list_value_in_struct_literal() {
        // Test 310: LIST_VALUE inside struct literal
        let result = duckdb_identity(
            "{'x': LIST_VALUE(1)[i]}",
        );
        assert_eq!(
            result,
            "{'x': [1][i]}",
            "LIST_VALUE inside struct literal should be transformed"
        );
    }

    #[test]
    fn test_struct_pack_simple() {
        // Simple STRUCT_PACK without nesting
        let result = duckdb_identity("SELECT STRUCT_PACK(a := 1)");
        eprintln!("STRUCT_PACK result: {}", result);
        assert!(result.contains("{"), "Expected curly brace, got: {}", result);
    }

    #[test]
    fn test_not_in_position() {
        // Test 78: NOT IN should become NOT (...) IN (...)
        // DuckDB prefers `NOT (expr) IN (list)` over `expr NOT IN (list)`
        let result = duckdb_identity(
            "SELECT col FROM t WHERE JSON_EXTRACT_STRING(col, '$.id') NOT IN ('b')",
        );
        assert_eq!(
            result,
            "SELECT col FROM t WHERE NOT (col ->> '$.id') IN ('b')",
            "NOT IN should have NOT moved outside and JSON expression wrapped"
        );
    }

    #[test]
    fn test_unnest_comma_join_to_join_on_true() {
        // Test 310: Comma-join with UNNEST should become JOIN ... ON TRUE
        let result = duckdb_identity(
            "WITH _data AS (SELECT [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}] AS col) SELECT t.col['b'] FROM _data, UNNEST(_data.col) AS t(col) WHERE t.col['a'] = 1",
        );
        assert_eq!(
            result,
            "WITH _data AS (SELECT [{'a': 1, 'b': 2}, {'a': 2, 'b': 3}] AS col) SELECT t.col['b'] FROM _data JOIN UNNEST(_data.col) AS t(col) ON TRUE WHERE t.col['a'] = 1",
            "Comma-join with UNNEST should become JOIN ON TRUE"
        );
    }
}
