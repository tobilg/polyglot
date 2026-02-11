//! Snowflake Dialect
//!
//! Snowflake-specific transformations based on sqlglot patterns.
//! Key differences:
//! - TRY_ prefix for safe operations (TRY_CAST, TRY_TO_NUMBER)
//! - FLATTEN for unnesting arrays
//! - QUALIFY clause support
//! - ARRAY_CONSTRUCT, OBJECT_CONSTRUCT for arrays/objects
//! - Variant type handling
//! - Default case-insensitive identifiers (unquoted)

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{AggFunc, BinaryOp, Cast, CeilFunc, DataType, Expression, Function, IntervalUnit, ListAggFunc, Literal, UnaryFunc, VarArgFunc};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Convert IntervalUnit to string for Snowflake syntax
fn interval_unit_to_str(unit: &IntervalUnit) -> String {
    match unit {
        IntervalUnit::Year => "YEAR".to_string(),
        IntervalUnit::Quarter => "QUARTER".to_string(),
        IntervalUnit::Month => "MONTH".to_string(),
        IntervalUnit::Week => "WEEK".to_string(),
        IntervalUnit::Day => "DAY".to_string(),
        IntervalUnit::Hour => "HOUR".to_string(),
        IntervalUnit::Minute => "MINUTE".to_string(),
        IntervalUnit::Second => "SECOND".to_string(),
        IntervalUnit::Millisecond => "MILLISECOND".to_string(),
        IntervalUnit::Microsecond => "MICROSECOND".to_string(),
    }
}

/// Snowflake dialect
pub struct SnowflakeDialect;

impl DialectImpl for SnowflakeDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Snowflake
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Snowflake uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // Snowflake supports $$ string literals
        config.quotes.insert("$$".to_string(), "$$".to_string());
        // Snowflake does NOT support nested comments (per Python sqlglot)
        config.nested_comments = false;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Snowflake),
            // Snowflake-specific settings from Python sqlglot
            parameter_token: "$",
            matched_by_source: false,
            single_string_interval: true,
            join_hints: false,
            table_hints: false,
            query_hints: false,
            aggregate_filter_supported: false,
            supports_table_copy: false,
            collate_is_func: true,
            limit_only_literals: true,
            json_key_value_pair_sep: ",",
            insert_overwrite: " OVERWRITE INTO",
            struct_delimiter: ("(", ")"),
            copy_params_are_wrapped: false,
            copy_params_eq_required: true,
            star_except: "EXCLUDE",
            supports_exploding_projections: false,
            array_concat_is_var_len: false,
            supports_convert_timezone: true,
            except_intersect_support_all_clause: false,
            supports_median: true,
            array_size_name: "ARRAY_SIZE",
            supports_decode_case: true,
            is_bool_allowed: false,
            // Snowflake supports TRY_ prefix operations
            try_supported: true,
            // Snowflake supports NVL2
            nvl2_supported: true,
            // Snowflake uses FLATTEN for unnest
            unnest_with_ordinality: false,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // ===== Data Type Mappings =====
            Expression::DataType(dt) => self.transform_data_type(dt),

            // ===== NOT IN transformation =====
            // Snowflake treats `value NOT IN (subquery)` as `VALUE <> ALL (subquery)`
            // See: https://docs.snowflake.com/en/sql-reference/functions/in
            Expression::In(in_expr) if in_expr.not && in_expr.query.is_some() => {
                // Transform NOT IN (subquery) -> <> ALL (subquery)
                let subquery = in_expr.query.unwrap();
                Ok(Expression::All(Box::new(crate::expressions::QuantifiedExpr {
                    this: in_expr.this,
                    subquery,
                    op: Some(crate::expressions::QuantifiedOp::Neq),
                })))
            }

            // NOT IN (values) -> NOT x IN (values)
            Expression::In(in_expr) if in_expr.not => {
                // Transform NOT x IN (values) by wrapping the In expression with not=false inside a Not
                let in_without_not = crate::expressions::In {
                    this: in_expr.this,
                    expressions: in_expr.expressions,
                    query: in_expr.query,
                    not: false,
                    global: in_expr.global,
                    unnest: in_expr.unnest,
                };
                Ok(Expression::Not(Box::new(crate::expressions::UnaryOp {
                    this: Expression::In(Box::new(in_without_not)),
                })))
            }

            // ===== Interval unit expansion =====
            // Expand abbreviated units in interval string values (e.g., '1 w' -> '1 WEEK')
            Expression::Interval(interval) => {
                self.transform_interval(*interval)
            }

            // ===== Null handling =====
            // IFNULL -> COALESCE (both work in Snowflake, but COALESCE is standard)
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE (both work in Snowflake, but COALESCE is standard)
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Coalesce with original_name (e.g., IFNULL parsed as Coalesce) -> clear original_name
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // GROUP_CONCAT -> LISTAGG in Snowflake
            Expression::GroupConcat(f) => Ok(Expression::ListAgg(Box::new(ListAggFunc {
                this: f.this,
                separator: f.separator,
                on_overflow: None,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
            }))),

            // ===== Cast operations =====
            // CAST(x AS GEOGRAPHY) -> TO_GEOGRAPHY(x)
            // CAST(x AS GEOMETRY) -> TO_GEOMETRY(x)
            Expression::Cast(c) => {
                use crate::expressions::DataType;
                // First, recursively transform the inner expression
                let transformed_this = self.transform_expr(c.this)?;
                match &c.to {
                    DataType::Geography { .. } => {
                        Ok(Expression::Function(Box::new(Function::new(
                            "TO_GEOGRAPHY".to_string(),
                            vec![transformed_this],
                        ))))
                    }
                    DataType::Geometry { .. } => {
                        Ok(Expression::Function(Box::new(Function::new(
                            "TO_GEOMETRY".to_string(),
                            vec![transformed_this],
                        ))))
                    }
                    _ => {
                        // Transform the data type
                        let transformed_dt = match self.transform_data_type(c.to.clone())? {
                            Expression::DataType(dt) => dt,
                            _ => c.to.clone(),
                        };
                        Ok(Expression::Cast(Box::new(Cast {
                            this: transformed_this,
                            to: transformed_dt,
                            double_colon_syntax: false,  // Normalize :: to CAST()
                            trailing_comments: c.trailing_comments,
                            format: c.format,
                            default: c.default,
                        })))
                    }
                }
            }

            // TryCast stays as TryCast (Snowflake supports TRY_CAST)
            // Recursively transform the inner expression
            Expression::TryCast(c) => {
                let transformed_this = self.transform_expr(c.this)?;
                Ok(Expression::TryCast(Box::new(Cast {
                    this: transformed_this,
                    to: c.to,
                    double_colon_syntax: false,  // Normalize :: to CAST()
                    trailing_comments: c.trailing_comments,
                    format: c.format,
                    default: c.default,
                })))
            }

            // SafeCast -> Cast in Snowflake (Snowflake CAST is safe by default)
            // Also convert TIMESTAMP to TIMESTAMPTZ (BigQuery TIMESTAMP = tz-aware)
            Expression::SafeCast(c) => {
                let to = match c.to {
                    DataType::Timestamp { .. } => DataType::Custom { name: "TIMESTAMPTZ".to_string() },
                    DataType::Custom { name } if name.eq_ignore_ascii_case("TIMESTAMP") => {
                        DataType::Custom { name: "TIMESTAMPTZ".to_string() }
                    }
                    other => other,
                };
                let transformed_this = self.transform_expr(c.this)?;
                Ok(Expression::Cast(Box::new(Cast {
                    this: transformed_this,
                    to,
                    double_colon_syntax: c.double_colon_syntax,
                    trailing_comments: c.trailing_comments,
                    format: c.format,
                    default: c.default,
                })))
            }

            // ===== Typed Literals -> CAST =====
            // TIMESTAMP '...' -> CAST('...' AS TIMESTAMP)
            Expression::Literal(Literal::Timestamp(s)) => {
                Ok(Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::String(s)),
                    to: DataType::Timestamp { precision: None, timezone: false },
                    double_colon_syntax: false,
                    trailing_comments: Vec::new(),
                    format: None,
                    default: None,
                })))
            }

            // DATE '...' -> CAST('...' AS DATE)
            Expression::Literal(Literal::Date(s)) => {
                Ok(Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::String(s)),
                    to: DataType::Date,
                    double_colon_syntax: false,
                    trailing_comments: Vec::new(),
                    format: None,
                    default: None,
                })))
            }

            // TIME '...' -> CAST('...' AS TIME)
            Expression::Literal(Literal::Time(s)) => {
                Ok(Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::String(s)),
                    to: DataType::Time { precision: None, timezone: false },
                    double_colon_syntax: false,
                    trailing_comments: Vec::new(),
                    format: None,
                    default: None,
                })))
            }

            // DATETIME '...' -> CAST('...' AS DATETIME)
            Expression::Literal(Literal::Datetime(s)) => {
                Ok(Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::String(s)),
                    to: DataType::Custom { name: "DATETIME".to_string() },
                    double_colon_syntax: false,
                    trailing_comments: Vec::new(),
                    format: None,
                    default: None,
                })))
            }

            // ===== Pattern matching =====
            // ILIKE is native to Snowflake (no transformation needed)
            Expression::ILike(op) => Ok(Expression::ILike(op)),

            // ===== Array operations =====
            // EXPLODE -> FLATTEN in Snowflake
            Expression::Explode(f) => Ok(Expression::Function(Box::new(Function::new(
                "FLATTEN".to_string(),
                vec![f.this],
            )))),

            // ExplodeOuter -> FLATTEN with OUTER => TRUE
            Expression::ExplodeOuter(f) => Ok(Expression::Function(Box::new(Function::new(
                "FLATTEN".to_string(),
                vec![f.this],
            )))),

            // UNNEST -> TABLE(FLATTEN(INPUT => x)) AS _t0(seq, key, path, index, value, this)
            Expression::Unnest(f) => {
                // Create INPUT => x named argument
                let input_arg = Expression::NamedArgument(Box::new(crate::expressions::NamedArgument {
                    name: crate::expressions::Identifier::new("INPUT"),
                    value: f.this,
                    separator: crate::expressions::NamedArgSeparator::DArrow,
                }));

                // Create FLATTEN(INPUT => x)
                let flatten = Expression::Function(Box::new(Function::new(
                    "FLATTEN".to_string(),
                    vec![input_arg],
                )));

                // Wrap in TABLE(...)
                let table_func = Expression::TableFromRows(Box::new(crate::expressions::TableFromRows {
                    this: Box::new(flatten),
                    alias: None,
                    joins: vec![],
                    pivots: None,
                    sample: None,
                }));

                // Add alias _t0(seq, key, path, index, value, this)
                Ok(Expression::Alias(Box::new(crate::expressions::Alias {
                    this: table_func,
                    alias: crate::expressions::Identifier::new("_t0"),
                    column_aliases: vec![
                        crate::expressions::Identifier::new("seq"),
                        crate::expressions::Identifier::new("key"),
                        crate::expressions::Identifier::new("path"),
                        crate::expressions::Identifier::new("index"),
                        crate::expressions::Identifier::new("value"),
                        crate::expressions::Identifier::new("this"),
                    ],
                    pre_alias_comments: vec![],
                    trailing_comments: vec![],
                })))
            }

            // Array constructor:
            // - If bracket notation ([1, 2, 3]), preserve it in Snowflake
            // - If ARRAY[...] syntax, convert to ARRAY_CONSTRUCT
            Expression::ArrayFunc(arr) => {
                if arr.bracket_notation {
                    // Keep bracket notation in Snowflake
                    Ok(Expression::ArrayFunc(arr))
                } else {
                    // Convert ARRAY[...] to ARRAY_CONSTRUCT
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_CONSTRUCT".to_string(),
                        arr.expressions,
                    ))))
                }
            }

            // ArrayConcat -> ARRAY_CAT
            Expression::ArrayConcat(f) => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_CAT".to_string(),
                f.expressions,
            )))),

            // ArrayConcatAgg -> ARRAY_FLATTEN
            Expression::ArrayConcatAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_FLATTEN".to_string(),
                vec![f.this],
            )))),

            // ArrayContains -> ARRAY_CONTAINS
            Expression::ArrayContains(f) => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_CONTAINS".to_string(),
                vec![f.this, f.expression],
            )))),

            // ArrayIntersect -> ARRAY_INTERSECTION
            Expression::ArrayIntersect(f) => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_INTERSECTION".to_string(),
                vec![f.this, f.expression],
            )))),

            // SortArray -> ARRAY_SORT
            Expression::ArraySort(f) => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_SORT".to_string(),
                vec![f.this],
            )))),

            // StringToArray -> STRTOK_TO_ARRAY
            Expression::StringToArray(f) => {
                let mut args = vec![*f.this];
                if let Some(expr) = f.expression {
                    args.push(*expr);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "STRTOK_TO_ARRAY".to_string(),
                    args,
                ))))
            }

            // ===== Bitwise operations =====
            // BitwiseOr -> BITOR
            Expression::BitwiseOr(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITOR".to_string(),
                vec![f.left, f.right],
            )))),

            // BitwiseXor -> BITXOR
            Expression::BitwiseXor(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITXOR".to_string(),
                vec![f.left, f.right],
            )))),

            // BitwiseAnd -> BITAND
            Expression::BitwiseAnd(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITAND".to_string(),
                vec![f.left, f.right],
            )))),

            // BitwiseNot -> BITNOT
            Expression::BitwiseNot(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITNOT".to_string(),
                vec![f.this],
            )))),

            // BitwiseLeftShift -> BITSHIFTLEFT
            Expression::BitwiseLeftShift(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITSHIFTLEFT".to_string(),
                vec![f.left, f.right],
            )))),

            // BitwiseRightShift -> BITSHIFTRIGHT
            Expression::BitwiseRightShift(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITSHIFTRIGHT".to_string(),
                vec![f.left, f.right],
            )))),

            // BitwiseAndAgg -> BITAND_AGG
            Expression::BitwiseAndAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITAND_AGG".to_string(),
                vec![f.this],
            )))),

            // BitwiseOrAgg -> BITOR_AGG
            Expression::BitwiseOrAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITOR_AGG".to_string(),
                vec![f.this],
            )))),

            // BitwiseXorAgg -> BITXOR_AGG
            Expression::BitwiseXorAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BITXOR_AGG".to_string(),
                vec![f.this],
            )))),

            // ===== Boolean aggregates =====
            // LogicalAnd -> BOOLAND_AGG
            Expression::LogicalAnd(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOLAND_AGG".to_string(),
                vec![f.this],
            )))),

            // LogicalOr -> BOOLOR_AGG
            Expression::LogicalOr(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOLOR_AGG".to_string(),
                vec![f.this],
            )))),

            // Booland -> BOOLAND
            Expression::Booland(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOLAND".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // Boolor -> BOOLOR
            Expression::Boolor(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOLOR".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // Xor -> BOOLXOR
            Expression::Xor(f) => {
                let mut args = Vec::new();
                if let Some(this) = f.this {
                    args.push(*this);
                }
                if let Some(expr) = f.expression {
                    args.push(*expr);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "BOOLXOR".to_string(),
                    args,
                ))))
            }

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

            // DayOfWeekIso -> DAYOFWEEKISO
            Expression::DayOfWeekIso(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFWEEKISO".to_string(),
                vec![f.this],
            )))),

            // DayOfYear -> DAYOFYEAR
            Expression::DayOfYear(f) => Ok(Expression::Function(Box::new(Function::new(
                "DAYOFYEAR".to_string(),
                vec![f.this],
            )))),

            // WeekOfYear -> WEEK (matches Python SQLGlot behavior)
            Expression::WeekOfYear(f) => Ok(Expression::Function(Box::new(Function::new(
                "WEEK".to_string(),
                vec![f.this],
            )))),

            // YearOfWeek -> YEAROFWEEK
            Expression::YearOfWeek(f) => Ok(Expression::Function(Box::new(Function::new(
                "YEAROFWEEK".to_string(),
                vec![f.this],
            )))),

            // YearOfWeekIso -> YEAROFWEEKISO
            Expression::YearOfWeekIso(f) => Ok(Expression::Function(Box::new(Function::new(
                "YEAROFWEEKISO".to_string(),
                vec![f.this],
            )))),

            // ByteLength -> OCTET_LENGTH
            Expression::ByteLength(f) => Ok(Expression::Function(Box::new(Function::new(
                "OCTET_LENGTH".to_string(),
                vec![f.this],
            )))),

            // TimestampDiff -> TIMESTAMPDIFF
            Expression::TimestampDiff(f) => {
                let mut args = vec![];
                // If unit is set (from cross-dialect normalize), use unit as first arg, this as second, expression as third
                if let Some(ref unit_str) = f.unit {
                    args.push(Expression::Identifier(crate::expressions::Identifier::new(unit_str.clone())));
                    args.push(*f.this);
                    args.push(*f.expression);
                } else {
                    args.push(*f.this);
                    args.push(*f.expression);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMPDIFF".to_string(),
                    args,
                ))))
            }

            // TimestampAdd -> TIMESTAMPADD
            Expression::TimestampAdd(f) => {
                let mut args = vec![];
                if let Some(ref unit_str) = f.unit {
                    args.push(Expression::Identifier(crate::expressions::Identifier::new(unit_str.clone())));
                    args.push(*f.this);
                    args.push(*f.expression);
                } else {
                    args.push(*f.this);
                    args.push(*f.expression);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMPADD".to_string(),
                    args,
                ))))
            }

            // ToArray -> TO_ARRAY
            Expression::ToArray(f) => Ok(Expression::Function(Box::new(Function::new(
                "TO_ARRAY".to_string(),
                vec![f.this],
            )))),

            // DateAdd -> DATEADD (with unit, amount, date order)
            Expression::DateAdd(f) => {
                let unit_str = interval_unit_to_str(&f.unit);
                let unit = Expression::Identifier(crate::expressions::Identifier {
                    name: unit_str,
                    quoted: false,
                    trailing_comments: Vec::new(),
                });
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![unit, f.interval, f.this],
                ))))
            }

            // DateSub -> DATEADD with negated amount: val * -1
            Expression::DateSub(f) => {
                let unit_str = interval_unit_to_str(&f.unit);
                let unit = Expression::Identifier(crate::expressions::Identifier {
                    name: unit_str,
                    quoted: false,
                    trailing_comments: Vec::new(),
                });
                // Negate using val * -1 format (matching Python sqlglot output)
                let neg_expr = Expression::Mul(Box::new(crate::expressions::BinaryOp::new(
                    f.interval,
                    Expression::Neg(Box::new(crate::expressions::UnaryOp { this: Expression::number(1) })),
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![unit, neg_expr, f.this],
                ))))
            }

            // DateDiff -> DATEDIFF
            Expression::DateDiff(f) => {
                let unit_str = interval_unit_to_str(&f.unit.unwrap_or(crate::expressions::IntervalUnit::Day));
                let unit = Expression::Identifier(crate::expressions::Identifier {
                    name: unit_str,
                    quoted: false,
                    trailing_comments: Vec::new(),
                });
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    vec![unit, f.expression, f.this],
                ))))
            }

            // ===== String functions =====
            // StringAgg -> LISTAGG in Snowflake
            Expression::StringAgg(f) => {
                let mut args = vec![f.this.clone()];
                if let Some(separator) = &f.separator {
                    args.push(separator.clone());
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "LISTAGG".to_string(),
                    args,
                ))))
            }

            // StartsWith -> STARTSWITH
            Expression::StartsWith(f) => Ok(Expression::Function(Box::new(Function::new(
                "STARTSWITH".to_string(),
                vec![f.this, f.expression],
            )))),

            // EndsWith -> keep as EndsWith AST node; generator outputs per-dialect
            Expression::EndsWith(f) => Ok(Expression::EndsWith(f)),

            // Stuff -> INSERT
            Expression::Stuff(f) => {
                let mut args = vec![*f.this];
                if let Some(start) = f.start {
                    args.push(*start);
                }
                if let Some(length) = f.length {
                    args.push(Expression::number(length));
                }
                args.push(*f.expression);
                Ok(Expression::Function(Box::new(Function::new(
                    "INSERT".to_string(),
                    args,
                ))))
            }

            // ===== Hash functions =====
            // SHA -> SHA1
            Expression::SHA(f) => Ok(Expression::Function(Box::new(Function::new(
                "SHA1".to_string(),
                vec![f.this],
            )))),

            // SHA1Digest -> SHA1_BINARY
            Expression::SHA1Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "SHA1_BINARY".to_string(),
                vec![f.this],
            )))),

            // SHA2Digest -> SHA2_BINARY
            Expression::SHA2Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "SHA2_BINARY".to_string(),
                vec![*f.this],
            )))),

            // MD5Digest -> MD5_BINARY
            Expression::MD5Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "MD5_BINARY".to_string(),
                vec![*f.this],
            )))),

            // MD5NumberLower64 -> MD5_NUMBER_LOWER64
            Expression::MD5NumberLower64(f) => Ok(Expression::Function(Box::new(Function::new(
                "MD5_NUMBER_LOWER64".to_string(),
                vec![f.this],
            )))),

            // MD5NumberUpper64 -> MD5_NUMBER_UPPER64
            Expression::MD5NumberUpper64(f) => Ok(Expression::Function(Box::new(Function::new(
                "MD5_NUMBER_UPPER64".to_string(),
                vec![f.this],
            )))),

            // ===== Vector functions =====
            // CosineDistance -> VECTOR_COSINE_SIMILARITY
            Expression::CosineDistance(f) => Ok(Expression::Function(Box::new(Function::new(
                "VECTOR_COSINE_SIMILARITY".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // DotProduct -> VECTOR_INNER_PRODUCT
            Expression::DotProduct(f) => Ok(Expression::Function(Box::new(Function::new(
                "VECTOR_INNER_PRODUCT".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // EuclideanDistance -> VECTOR_L2_DISTANCE
            Expression::EuclideanDistance(f) => Ok(Expression::Function(Box::new(Function::new(
                "VECTOR_L2_DISTANCE".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ManhattanDistance -> VECTOR_L1_DISTANCE
            Expression::ManhattanDistance(f) => Ok(Expression::Function(Box::new(Function::new(
                "VECTOR_L1_DISTANCE".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ===== JSON/Struct functions =====
            // JSONFormat -> TO_JSON
            Expression::JSONFormat(f) => {
                let mut args = Vec::new();
                if let Some(this) = f.this {
                    args.push(*this);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_JSON".to_string(),
                    args,
                ))))
            }

            // JSONKeys -> OBJECT_KEYS
            Expression::JSONKeys(f) => Ok(Expression::Function(Box::new(Function::new(
                "OBJECT_KEYS".to_string(),
                vec![*f.this],
            )))),

            // GetExtract -> GET
            Expression::GetExtract(f) => Ok(Expression::Function(Box::new(Function::new(
                "GET".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // StarMap -> OBJECT_CONSTRUCT
            Expression::StarMap(f) => Ok(Expression::Function(Box::new(Function::new(
                "OBJECT_CONSTRUCT".to_string(),
                vec![f.this, f.expression],
            )))),

            // LowerHex -> TO_CHAR
            Expression::LowerHex(f) => Ok(Expression::Function(Box::new(Function::new(
                "TO_CHAR".to_string(),
                vec![f.this],
            )))),

            // Skewness -> SKEW
            Expression::Skewness(f) => Ok(Expression::Function(Box::new(Function::new(
                "SKEW".to_string(),
                vec![f.this],
            )))),

            // StPoint -> ST_MAKEPOINT
            Expression::StPoint(f) => Ok(Expression::Function(Box::new(Function::new(
                "ST_MAKEPOINT".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // FromTimeZone -> CONVERT_TIMEZONE
            Expression::FromTimeZone(f) => Ok(Expression::Function(Box::new(Function::new(
                "CONVERT_TIMEZONE".to_string(),
                vec![*f.this],
            )))),

            // ===== Conversion functions =====
            // Unhex -> HEX_DECODE_BINARY
            Expression::Unhex(f) => Ok(Expression::Function(Box::new(Function::new(
                "HEX_DECODE_BINARY".to_string(),
                vec![*f.this],
            )))),

            // UnixToTime -> TO_TIMESTAMP
            Expression::UnixToTime(f) => {
                let mut args = vec![*f.this];
                if let Some(scale) = f.scale {
                    args.push(Expression::number(scale));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_TIMESTAMP".to_string(),
                    args,
                ))))
            }

            // ===== Conditional =====
            // IfFunc -> keep as IfFunc with IFF name for Snowflake
            Expression::IfFunc(f) => {
                Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                    condition: f.condition,
                    true_value: f.true_value,
                    false_value: Some(f.false_value.unwrap_or(Expression::Null(crate::expressions::Null))),
                    original_name: Some("IFF".to_string()),
                })))
            }

            // ===== Aggregate functions =====
            // ApproxDistinct -> APPROX_COUNT_DISTINCT
            Expression::ApproxDistinct(f) => Ok(Expression::Function(Box::new(Function::new(
                "APPROX_COUNT_DISTINCT".to_string(),
                vec![f.this],
            )))),

            // ArgMax -> MAX_BY
            Expression::ArgMax(f) => Ok(Expression::Function(Box::new(Function::new(
                "MAX_BY".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ArgMin -> MIN_BY
            Expression::ArgMin(f) => Ok(Expression::Function(Box::new(Function::new(
                "MIN_BY".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ===== Random =====
            // RANDOM is native to Snowflake - keep as-is
            Expression::Random(_) => Ok(Expression::Random(crate::expressions::Random)),

            // Rand - keep as-is (generator outputs RANDOM for Snowflake)
            Expression::Rand(r) => Ok(Expression::Rand(r)),

            // ===== UUID =====
            // Uuid -> keep as Uuid node; generator will output UUID_STRING for Snowflake
            Expression::Uuid(u) => Ok(Expression::Uuid(u)),

            // ===== Map/Object =====
            // Map -> OBJECT_CONSTRUCT
            Expression::Map(f) => Ok(Expression::Function(Box::new(Function::new(
                "OBJECT_CONSTRUCT".to_string(),
                f.keys
                    .into_iter()
                    .zip(f.values.into_iter())
                    .flat_map(|(k, v)| vec![k, v])
                    .collect(),
            )))),

            // MapFunc (curly brace syntax) -> OBJECT_CONSTRUCT
            Expression::MapFunc(f) => Ok(Expression::Function(Box::new(Function::new(
                "OBJECT_CONSTRUCT".to_string(),
                f.keys
                    .into_iter()
                    .zip(f.values.into_iter())
                    .flat_map(|(k, v)| vec![k, v])
                    .collect(),
            )))),

            // VarMap -> OBJECT_CONSTRUCT
            Expression::VarMap(f) => Ok(Expression::Function(Box::new(Function::new(
                "OBJECT_CONSTRUCT".to_string(),
                f.keys
                    .into_iter()
                    .zip(f.values.into_iter())
                    .flat_map(|(k, v)| vec![k, v])
                    .collect(),
            )))),

            // ===== JSON =====
            // JSONObject -> OBJECT_CONSTRUCT_KEEP_NULL
            Expression::JsonObject(f) => Ok(Expression::Function(Box::new(Function::new(
                "OBJECT_CONSTRUCT_KEEP_NULL".to_string(),
                f.pairs.into_iter().flat_map(|(k, v)| vec![k, v]).collect(),
            )))),

            // JSONExtractScalar -> JSON_EXTRACT_PATH_TEXT
            Expression::JsonExtractScalar(f) => Ok(Expression::Function(Box::new(Function::new(
                "JSON_EXTRACT_PATH_TEXT".to_string(),
                vec![f.this, f.path],
            )))),

            // ===== Struct =====
            // Struct -> OBJECT_CONSTRUCT
            Expression::Struct(f) => Ok(Expression::Function(Box::new(Function::new(
                "OBJECT_CONSTRUCT".to_string(),
                f.fields
                    .into_iter()
                    .flat_map(|(name, expr)| {
                        let key = match name {
                            Some(n) => Expression::string(n),
                            None => Expression::Null(crate::expressions::Null),
                        };
                        vec![key, expr]
                    })
                    .collect(),
            )))),

            // ===== JSON Path =====
            // JSONPathRoot -> empty string ($ is implicit in Snowflake)
            Expression::JSONPathRoot(_) => Ok(Expression::Literal(crate::expressions::Literal::String(
                String::new(),
            ))),

            // ===== VarSamp -> VARIANCE (Snowflake) =====
            // Snowflake uses VARIANCE instead of VAR_SAMP
            Expression::VarSamp(agg) => {
                Ok(Expression::Variance(agg))
            }

            // ===== VarPop -> keep as VarPop =====
            // The generator handles dialect-specific naming (VARIANCE_POP for Snowflake)
            Expression::VarPop(agg) => Ok(Expression::VarPop(agg)),

            // ===== EXTRACT -> DATE_PART =====
            // Snowflake uses DATE_PART instead of EXTRACT
            Expression::Extract(f) => {
                use crate::expressions::DateTimeField;
                // Recursively transform the inner expression (e.g., CAST(... AS TIMESTAMP_NTZ) -> CAST(... AS TIMESTAMPNTZ))
                let transformed_this = self.transform_expr(f.this)?;
                let field_name = match &f.field {
                    DateTimeField::Year => "YEAR",
                    DateTimeField::Month => "MONTH",
                    DateTimeField::Day => "DAY",
                    DateTimeField::Hour => "HOUR",
                    DateTimeField::Minute => "MINUTE",
                    DateTimeField::Second => "SECOND",
                    DateTimeField::Millisecond => "MILLISECOND",
                    DateTimeField::Microsecond => "MICROSECOND",
                    DateTimeField::Week => "WEEK",
                    DateTimeField::WeekWithModifier(m) => return Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PART".to_string(),
                        vec![
                            Expression::Identifier(crate::expressions::Identifier {
                                name: format!("WEEK({})", m),
                                quoted: false,
                                trailing_comments: Vec::new(),
                            }),
                            transformed_this,
                        ],
                    )))),
                    DateTimeField::DayOfWeek => "DAYOFWEEK",
                    DateTimeField::DayOfYear => "DAYOFYEAR",
                    DateTimeField::Quarter => "QUARTER",
                    DateTimeField::Epoch => "EPOCH",
                    DateTimeField::Timezone => "TIMEZONE",
                    DateTimeField::TimezoneHour => "TIMEZONE_HOUR",
                    DateTimeField::TimezoneMinute => "TIMEZONE_MINUTE",
                    DateTimeField::Date => "DATE",
                    DateTimeField::Time => "TIME",
                    DateTimeField::Custom(s) => {
                        // Map common EXTRACT field names to Snowflake DATE_PART names
                        match s.to_uppercase().as_str() {
                            "DAYOFMONTH" => "DAY",
                            "DOW" => "DAYOFWEEK",
                            "DOY" => "DAYOFYEAR",
                            "ISODOW" => "DAYOFWEEKISO",
                            "EPOCH_SECOND" | "EPOCH_SECONDS" => "EPOCH_SECOND",
                            "EPOCH_MILLISECOND" | "EPOCH_MILLISECONDS" => "EPOCH_MILLISECOND",
                            "EPOCH_MICROSECOND" | "EPOCH_MICROSECONDS" => "EPOCH_MICROSECOND",
                            "EPOCH_NANOSECOND" | "EPOCH_NANOSECONDS" => "EPOCH_NANOSECOND",
                            _ => return {
                                let field_ident = Expression::Identifier(crate::expressions::Identifier {
                                    name: s.to_string(),
                                    quoted: false,
                                    trailing_comments: Vec::new(),
                                });
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_PART".to_string(),
                                    vec![field_ident, transformed_this],
                                ))))
                            },
                        }
                    }
                };
                let field_ident = Expression::Identifier(crate::expressions::Identifier {
                    name: field_name.to_string(),
                    quoted: false,
                    trailing_comments: Vec::new(),
                });
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_PART".to_string(),
                    vec![field_ident, transformed_this],
                ))))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // SUM - recursively transform inner expression
            Expression::Sum(mut agg) => {
                agg.this = self.transform_expr(agg.this)?;
                Ok(Expression::Sum(agg))
            }

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Handle NamedArgument - recursively transform the value
            Expression::NamedArgument(na) => {
                let transformed_value = self.transform_expr(na.value)?;
                Ok(Expression::NamedArgument(Box::new(crate::expressions::NamedArgument {
                    name: na.name,
                    value: transformed_value,
                    separator: na.separator,
                })))
            }

            // Handle CreateTable - transform column data types and default/computed expressions
            Expression::CreateTable(mut ct) => {
                for col in &mut ct.columns {
                    if let Expression::DataType(new_dt) = self.transform_data_type(col.data_type.clone())? {
                        col.data_type = new_dt;
                    }
                    // Also transform computed/default expressions (e.g., AS (parse_json(x):COL3::number))
                    if let Some(default_expr) = col.default.take() {
                        col.default = Some(self.transform_expr(default_expr)?);
                    }
                    // Transform expressions in column constraints (computed columns)
                    for constraint in &mut col.constraints {
                        if let crate::expressions::ColumnConstraint::ComputedColumn(cc) = constraint {
                            let transformed = self.transform_expr(*cc.expression.clone())?;
                            cc.expression = Box::new(transformed);
                        }
                    }
                }

                // For EXTERNAL tables, convert with_properties to Raw properties
                // with proper Snowflake formatting (no WITH wrapper, specific key casing)
                if ct.table_modifier.as_deref() == Some("EXTERNAL") && !ct.with_properties.is_empty() {
                    for (key, value) in ct.with_properties.drain(..) {
                        let formatted = Self::format_external_table_property(&key, &value);
                        ct.properties.push(Expression::Raw(crate::expressions::Raw { sql: formatted }));
                    }
                }

                Ok(Expression::CreateTable(ct))
            }

            // Handle AlterTable - transform column data types in ADD operations
            Expression::AlterTable(mut at) => {
                for action in &mut at.actions {
                    if let crate::expressions::AlterTableAction::AddColumn { column, .. } = action {
                        if let Expression::DataType(new_dt) = self.transform_data_type(column.data_type.clone())? {
                            column.data_type = new_dt;
                        }
                    }
                }
                Ok(Expression::AlterTable(at))
            }

            // Handle Table reference - transform HistoricalData (AT/BEFORE time travel clauses)
            Expression::Table(mut t) => {
                if let Some(when) = t.when.take() {
                    // Recursively transform the expression inside HistoricalData
                    let transformed_expr = self.transform_expr(*when.expression)?;
                    t.when = Some(Box::new(crate::expressions::HistoricalData {
                        this: when.this,
                        kind: when.kind,
                        expression: Box::new(transformed_expr),
                    }));
                }
                Ok(Expression::Table(t))
            }

            // Handle Subscript - recursively transform inner expression
            Expression::Subscript(s) => {
                let transformed_this = self.transform_expr(s.this)?;
                let transformed_index = self.transform_expr(s.index)?;
                Ok(Expression::Subscript(Box::new(crate::expressions::Subscript {
                    this: transformed_this,
                    index: transformed_index,
                })))
            }

            // Recursively transform parenthesized expressions
            Expression::Paren(p) => {
                let transformed = self.transform_expr(p.this)?;
                Ok(Expression::Paren(Box::new(crate::expressions::Paren {
                    this: transformed,
                    trailing_comments: p.trailing_comments,
                })))
            }

            // ===== ORDER BY null ordering normalization =====
            // Snowflake is nulls_are_large: ASC defaults to NULLS LAST, DESC defaults to NULLS FIRST
            // Fill in implicit nulls_first so target dialects can properly strip/add as needed
            Expression::Select(mut select) => {
                if let Some(ref mut order) = select.order_by {
                    for ord in &mut order.expressions {
                        if ord.nulls_first.is_none() {
                            ord.nulls_first = Some(ord.desc);
                        }
                    }
                }
                Ok(Expression::Select(select))
            }

            // Fill in NULLS ordering for window function ORDER BY clauses
            Expression::WindowFunction(mut wf) => {
                for ord in &mut wf.over.order_by {
                    if ord.nulls_first.is_none() {
                        ord.nulls_first = Some(ord.desc);
                    }
                }
                Ok(Expression::WindowFunction(wf))
            }

            // Also handle Expression::Window (WindowSpec)
            Expression::Window(mut w) => {
                for ord in &mut w.order_by {
                    if ord.nulls_first.is_none() {
                        ord.nulls_first = Some(ord.desc);
                    }
                }
                Ok(Expression::Window(w))
            }

            // LATERAL FLATTEN: add default column aliases (SEQ, KEY, PATH, INDEX, VALUE, THIS)
            Expression::Lateral(mut lat) => {
                // Check if the inner expression is a FLATTEN function
                let is_flatten = match lat.this.as_ref() {
                    Expression::Function(f) => f.name.to_uppercase() == "FLATTEN",
                    _ => false,
                };
                if is_flatten && lat.column_aliases.is_empty() {
                    // Add default column aliases
                    lat.column_aliases = vec![
                        "SEQ".to_string(), "KEY".to_string(), "PATH".to_string(),
                        "INDEX".to_string(), "VALUE".to_string(), "THIS".to_string(),
                    ];
                    // If no alias, add _flattened
                    if lat.alias.is_none() {
                        lat.alias = Some("_flattened".to_string());
                    }
                }
                Ok(Expression::Lateral(lat))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl SnowflakeDialect {
    /// Format a Snowflake external table property for output.
    /// Some properties like LOCATION and FILE_FORMAT are uppercased keywords.
    fn format_external_table_property(key: &str, value: &str) -> String {
        let lower_key = key.to_lowercase();
        match lower_key.as_str() {
            "location" => format!("LOCATION={}", value),
            "file_format" => {
                // Format file_format value: remove spaces around =, uppercase booleans
                let formatted_value = Self::format_file_format_value(value);
                format!("FILE_FORMAT={}", formatted_value)
            }
            _ => format!("{}={}", key, value),
        }
    }

    /// Format file_format property value:
    /// - Remove spaces around = signs
    /// - Uppercase boolean values (false -> FALSE, true -> TRUE)
    fn format_file_format_value(value: &str) -> String {
        if !value.starts_with('(') {
            return value.to_string();
        }
        // Strip outer parens, process inner key=value pairs
        let inner = value[1..value.len() - 1].trim();
        // Parse space-separated key=value pairs (may have spaces around =)
        let mut result = String::from("(");
        let mut parts: Vec<String> = Vec::new();
        // Split by whitespace and reconstruct key=value pairs
        let tokens: Vec<&str> = inner.split_whitespace().collect();
        let mut i = 0;
        while i < tokens.len() {
            let token = tokens[i];
            if i + 2 < tokens.len() && tokens[i + 1] == "=" {
                // key = value pattern
                let val = Self::format_property_value(tokens[i + 2]);
                parts.push(format!("{}={}", token, val));
                i += 3;
            } else if token.contains('=') {
                // key=value already joined
                let eq_pos = token.find('=').unwrap();
                let k = &token[..eq_pos];
                let v = Self::format_property_value(&token[eq_pos + 1..]);
                parts.push(format!("{}={}", k, v));
                i += 1;
            } else {
                parts.push(token.to_string());
                i += 1;
            }
        }
        result.push_str(&parts.join(" "));
        result.push(')');
        result
    }

    /// Format a property value - uppercase boolean literals
    fn format_property_value(value: &str) -> String {
        match value.to_lowercase().as_str() {
            "true" => "TRUE".to_string(),
            "false" => "FALSE".to_string(),
            _ => value.to_string(),
        }
    }

    /// Transform data types according to Snowflake TYPE_MAPPING
    fn transform_data_type(&self, dt: crate::expressions::DataType) -> Result<Expression> {
        use crate::expressions::DataType;
        let transformed = match dt {
            // TEXT -> VARCHAR
            DataType::Text => DataType::VarChar { length: None, parenthesized_length: false },
            // STRUCT -> OBJECT
            DataType::Struct { fields, .. } => {
                // Snowflake uses OBJECT for struct types
                let _ = fields; // Snowflake OBJECT doesn't preserve field names in the same way
                DataType::Custom {
                    name: "OBJECT".to_string(),
                }
            }
            // Custom type transformations
            DataType::Custom { name } => {
                let upper_name = name.to_uppercase();
                match upper_name.as_str() {
                    // NVARCHAR -> VARCHAR (SQL Server type)
                    "NVARCHAR" | "NCHAR" | "NATIONAL CHARACTER VARYING" | "NATIONAL CHAR" => {
                        DataType::VarChar { length: None, parenthesized_length: false }
                    }
                    // STRING -> VARCHAR (Snowflake accepts both, but normalizes to VARCHAR)
                    "STRING" => DataType::VarChar { length: None, parenthesized_length: false },
                    // BIGDECIMAL -> DOUBLE
                    "BIGDECIMAL" => DataType::Double { precision: None, scale: None },
                    // NESTED -> OBJECT
                    "NESTED" => DataType::Custom {
                        name: "OBJECT".to_string(),
                    },
                    // BYTEINT -> INT
                    "BYTEINT" => DataType::Int { length: None, integer_spelling: false },
                    // CHAR VARYING -> VARCHAR
                    "CHAR VARYING" | "CHARACTER VARYING" => DataType::VarChar { length: None, parenthesized_length: false },
                    // SQL_DOUBLE -> DOUBLE
                    "SQL_DOUBLE" => DataType::Double { precision: None, scale: None },
                    // SQL_VARCHAR -> VARCHAR
                    "SQL_VARCHAR" => DataType::VarChar { length: None, parenthesized_length: false },
                    // TIMESTAMP_NTZ -> TIMESTAMPNTZ (normalize underscore form)
                    "TIMESTAMP_NTZ" => DataType::Custom { name: "TIMESTAMPNTZ".to_string() },
                    // TIMESTAMP_LTZ -> TIMESTAMPLTZ (normalize underscore form)
                    "TIMESTAMP_LTZ" => DataType::Custom { name: "TIMESTAMPLTZ".to_string() },
                    // TIMESTAMP_TZ -> TIMESTAMPTZ (normalize underscore form)
                    "TIMESTAMP_TZ" => DataType::Custom { name: "TIMESTAMPTZ".to_string() },
                    // NCHAR VARYING -> VARCHAR
                    "NCHAR VARYING" => DataType::VarChar { length: None, parenthesized_length: false },
                    // NUMBER -> DECIMAL(38, 0) (Snowflake's default NUMBER is DECIMAL(38, 0))
                    "NUMBER" => DataType::Decimal { precision: Some(38), scale: Some(0) },
                    _ if name.starts_with("NUMBER(") => {
                        // NUMBER(precision, scale) -> DECIMAL(precision, scale)
                        // Parse: "NUMBER(38, 0)" -> precision=38, scale=0
                        let inner = &name[7..name.len()-1]; // strip "NUMBER(" and ")"
                        let parts: Vec<&str> = inner.split(',').map(|s| s.trim()).collect();
                        let precision = parts.first().and_then(|p| p.parse::<u32>().ok());
                        let scale = parts.get(1).and_then(|s| s.parse::<u32>().ok());
                        DataType::Decimal { precision, scale }
                    }
                    _ => DataType::Custom { name },
                }
            }
            // DECIMAL without precision -> DECIMAL(38, 0) (Snowflake default)
            DataType::Decimal { precision: None, scale: None } => DataType::Decimal { precision: Some(38), scale: Some(0) },
            // FLOAT -> DOUBLE (Snowflake FLOAT is actually 64-bit DOUBLE)
            DataType::Float { .. } => DataType::Double { precision: None, scale: None },
            // Keep all other types as-is (Snowflake is quite flexible)
            other => other,
        };
        Ok(Expression::DataType(transformed))
    }

    /// Map date part abbreviation to canonical form (from Python SQLGlot DATE_PART_MAPPING)
    fn map_date_part(abbr: &str) -> Option<&'static str> {
        match abbr.to_uppercase().as_str() {
            // Year
            "Y" | "YY" | "YYY" | "YYYY" | "YR" | "YEARS" | "YRS" => Some("YEAR"),
            // Month
            "MM" | "MON" | "MONS" | "MONTHS" => Some("MONTH"),
            // Day
            "D" | "DD" | "DAYS" | "DAYOFMONTH" => Some("DAY"),
            // Day of week
            "DAY OF WEEK" | "WEEKDAY" | "DOW" | "DW" => Some("DAYOFWEEK"),
            "WEEKDAY_ISO" | "DOW_ISO" | "DW_ISO" | "DAYOFWEEK_ISO" => Some("DAYOFWEEKISO"),
            // Day of year
            "DAY OF YEAR" | "DOY" | "DY" => Some("DAYOFYEAR"),
            // Week
            "W" | "WK" | "WEEKOFYEAR" | "WOY" | "WY" => Some("WEEK"),
            "WEEK_ISO" | "WEEKOFYEARISO" | "WEEKOFYEAR_ISO" => Some("WEEKISO"),
            // Quarter
            "Q" | "QTR" | "QTRS" | "QUARTERS" => Some("QUARTER"),
            // Hour
            "H" | "HH" | "HR" | "HOURS" | "HRS" => Some("HOUR"),
            // Minute (note: 'M' could be minute in some contexts, but we keep it simple)
            "MI" | "MIN" | "MINUTES" | "MINS" => Some("MINUTE"),
            // Second
            "S" | "SEC" | "SECONDS" | "SECS" => Some("SECOND"),
            // Millisecond
            "MS" | "MSEC" | "MSECS" | "MSECOND" | "MSECONDS" | "MILLISEC" | "MILLISECS" | "MILLISECON" | "MILLISECONDS" => Some("MILLISECOND"),
            // Microsecond
            "US" | "USEC" | "USECS" | "MICROSEC" | "MICROSECS" | "USECOND" | "USECONDS" | "MICROSECONDS" => Some("MICROSECOND"),
            // Nanosecond
            "NS" | "NSEC" | "NANOSEC" | "NSECOND" | "NSECONDS" | "NANOSECS" => Some("NANOSECOND"),
            // Epoch variants
            "EPOCH_SECOND" | "EPOCH_SECONDS" => Some("EPOCH_SECOND"),
            "EPOCH_MILLISECOND" | "EPOCH_MILLISECONDS" => Some("EPOCH_MILLISECOND"),
            "EPOCH_MICROSECOND" | "EPOCH_MICROSECONDS" => Some("EPOCH_MICROSECOND"),
            "EPOCH_NANOSECOND" | "EPOCH_NANOSECONDS" => Some("EPOCH_NANOSECOND"),
            // Timezone
            "TZH" => Some("TIMEZONE_HOUR"),
            "TZM" => Some("TIMEZONE_MINUTE"),
            // Decade
            "DEC" | "DECS" | "DECADES" => Some("DECADE"),
            // Millennium
            "MIL" | "MILS" | "MILLENIA" => Some("MILLENNIUM"),
            // Century
            "C" | "CENT" | "CENTS" | "CENTURIES" => Some("CENTURY"),
            // No mapping needed (already canonical or unknown)
            _ => None,
        }
    }

    /// Transform a date part identifier/expression using the mapping
    fn transform_date_part_arg(&self, expr: Expression) -> Expression {
        match &expr {
            // Handle string literal: 'minute' -> minute (unquoted identifier, preserving case)
            Expression::Literal(crate::expressions::Literal::String(s)) => {
                Expression::Identifier(crate::expressions::Identifier {
                    name: s.clone(),
                    quoted: false,
                    trailing_comments: Vec::new(),
                })
            }
            // Handle Identifier (rare case)
            Expression::Identifier(id) => {
                if let Some(canonical) = Self::map_date_part(&id.name) {
                    Expression::Identifier(crate::expressions::Identifier {
                        name: canonical.to_string(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                    })
                } else {
                    // No mapping needed, keep original (Python sqlglot preserves case)
                    expr
                }
            }
            // Handle Column (more common - parser treats unqualified names as columns)
            Expression::Column(col) if col.table.is_none() => {
                if let Some(canonical) = Self::map_date_part(&col.name.name) {
                    Expression::Identifier(crate::expressions::Identifier {
                        name: canonical.to_string(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                    })
                } else {
                    // No mapping needed, keep original (Python sqlglot preserves case)
                    expr
                }
            }
            _ => expr,
        }
    }

    /// Like transform_date_part_arg but only handles Identifier/Column, never String literals.
    /// Used for native Snowflake DATE_PART where string args should stay as strings.
    fn transform_date_part_arg_identifiers_only(&self, expr: Expression) -> Expression {
        match &expr {
            Expression::Identifier(id) => {
                if let Some(canonical) = Self::map_date_part(&id.name) {
                    Expression::Identifier(crate::expressions::Identifier {
                        name: canonical.to_string(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                    })
                } else {
                    expr
                }
            }
            Expression::Column(col) if col.table.is_none() => {
                if let Some(canonical) = Self::map_date_part(&col.name.name) {
                    Expression::Identifier(crate::expressions::Identifier {
                        name: canonical.to_string(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                    })
                } else {
                    expr
                }
            }
            _ => expr,
        }
    }

    /// Transform JSON path for Snowflake GET_PATH function
    /// - Convert colon notation to dot notation (y[0]:z -> y[0].z)
    /// - Wrap unsafe keys in brackets ($id -> ["$id"])
    fn transform_json_path(path: &str) -> String {
        // Check if path is just a single key that needs bracket wrapping
        // A safe identifier is alphanumeric + underscore, starting with letter/underscore
        fn is_safe_identifier(s: &str) -> bool {
            if s.is_empty() {
                return false;
            }
            let mut chars = s.chars();
            match chars.next() {
                Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
                _ => return false,
            }
            chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        }

        // Simple path: just a key like "$id" or "field"
        // If no dots, brackets, or colons, it's a simple key
        if !path.contains('.') && !path.contains('[') && !path.contains(':') {
            if is_safe_identifier(path) {
                return path.to_string();
            } else {
                // Wrap unsafe key in bracket notation
                return format!("[\"{}\"]", path);
            }
        }

        // Complex path: replace colons with dots
        // e.g., y[0]:z -> y[0].z
        let result = path.replace(':', ".");
        result
    }

    /// Transform interval to expand abbreviated units (e.g., 'w' -> 'WEEK')
    fn transform_interval(&self, interval: crate::expressions::Interval) -> Result<Expression> {
        use crate::expressions::{Interval, Literal};

        // Unit abbreviation mapping (from Python SQLGlot UNABBREVIATED_UNIT_NAME)
        fn expand_unit(abbr: &str) -> &'static str {
            match abbr.to_uppercase().as_str() {
                "D" => "DAY",
                "H" => "HOUR",
                "M" => "MINUTE",
                "MS" => "MILLISECOND",
                "NS" => "NANOSECOND",
                "Q" => "QUARTER",
                "S" => "SECOND",
                "US" => "MICROSECOND",
                "W" => "WEEK",
                "Y" => "YEAR",
                // Full forms (normalize to singular, uppercase)
                "WEEK" | "WEEKS" => "WEEK",
                "DAY" | "DAYS" => "DAY",
                "HOUR" | "HOURS" => "HOUR",
                "MINUTE" | "MINUTES" => "MINUTE",
                "SECOND" | "SECONDS" => "SECOND",
                "MONTH" | "MONTHS" => "MONTH",
                "YEAR" | "YEARS" => "YEAR",
                "QUARTER" | "QUARTERS" => "QUARTER",
                "MILLISECOND" | "MILLISECONDS" => "MILLISECOND",
                "MICROSECOND" | "MICROSECONDS" => "MICROSECOND",
                "NANOSECOND" | "NANOSECONDS" => "NANOSECOND",
                _ => "", // Unknown unit, return empty to indicate no match
            }
        }

        /// Parse an interval string like "1 w" into (value, unit)
        fn parse_interval_string(s: &str) -> Option<(&str, &str)> {
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

            Some((value, rest))
        }

        // Check if the interval value is a string literal with embedded value+unit
        if let Some(Expression::Literal(Literal::String(ref s))) = interval.this {
            if let Some((value, unit)) = parse_interval_string(s) {
                let expanded = expand_unit(unit);
                if !expanded.is_empty() {
                    // Construct new string with expanded unit
                    let new_value = format!("{} {}", value, expanded);

                    return Ok(Expression::Interval(Box::new(Interval {
                        this: Some(Expression::Literal(Literal::String(new_value))),
                        unit: None, // Unit is now part of the string (SINGLE_STRING_INTERVAL style)
                    })));
                }
            }
        }

        // No transformation needed
        Ok(Expression::Interval(Box::new(interval)))
    }

    fn transform_function(&self, f: Function) -> Result<Expression> {
        // First, recursively transform all function arguments
        let transformed_args: Vec<Expression> = f
            .args
            .into_iter()
            .map(|arg| self.transform_expr(arg))
            .collect::<Result<Vec<_>>>()?;

        let f = Function {
            name: f.name,
            args: transformed_args,
            distinct: f.distinct,
            trailing_comments: f.trailing_comments,
            use_bracket_syntax: f.use_bracket_syntax,
            no_parens: f.no_parens,
            quoted: f.quoted,
        };

        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL -> COALESCE (standardize to COALESCE)
            "IFNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // NVL -> COALESCE (both work in Snowflake, but COALESCE is standard per SQLGlot)
            "NVL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // NVL2 is native to Snowflake
            "NVL2" => Ok(Expression::Function(Box::new(f))),

            // GROUP_CONCAT -> LISTAGG in Snowflake
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG in Snowflake
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "LISTAGG".to_string(),
                f.args,
            )))),

            // SUBSTR -> SUBSTRING (both work in Snowflake)
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // UNNEST -> FLATTEN
            "UNNEST" => Ok(Expression::Function(Box::new(Function::new(
                "FLATTEN".to_string(),
                f.args,
            )))),

            // EXPLODE -> FLATTEN
            "EXPLODE" => Ok(Expression::Function(Box::new(Function::new(
                "FLATTEN".to_string(),
                f.args,
            )))),

            // CURRENT_DATE is native
            "CURRENT_DATE" => Ok(Expression::CurrentDate(crate::expressions::CurrentDate)),

            // NOW -> CURRENT_TIMESTAMP (preserving parens style)
            "NOW" => Ok(Expression::Function(Box::new(Function {
                name: "CURRENT_TIMESTAMP".to_string(),
                args: f.args,
                distinct: false,
                trailing_comments: Vec::new(),
                use_bracket_syntax: false,
                no_parens: f.no_parens, quoted: false,
            }))),

            // GETDATE -> CURRENT_TIMESTAMP (preserving parens style)
            "GETDATE" => Ok(Expression::Function(Box::new(Function {
                name: "CURRENT_TIMESTAMP".to_string(),
                args: f.args,
                distinct: false,
                trailing_comments: Vec::new(),
                use_bracket_syntax: false,
                no_parens: f.no_parens, quoted: false,
            }))),

            // CURRENT_TIMESTAMP - always output with parens in Snowflake
            // Note: LOCALTIMESTAMP converts to CURRENT_TIMESTAMP without parens,
            // but explicit CURRENT_TIMESTAMP calls should have parens
            "CURRENT_TIMESTAMP" if f.args.is_empty() => Ok(Expression::Function(Box::new(Function {
                name: "CURRENT_TIMESTAMP".to_string(),
                args: Vec::new(),
                distinct: false,
                trailing_comments: Vec::new(),
                use_bracket_syntax: false,
                no_parens: false,  // Always output with parens
                quoted: false,
            }))),

            // TO_DATE with single string arg that looks like a date -> CAST(arg AS DATE)
            // Per Python SQLGlot: TO_DATE('2013-04-05') -> CAST('2013-04-05' AS DATE)
            // But TO_DATE('12345') stays as is (doesn't look like a date)
            "TO_DATE" => {
                if f.args.len() == 1 {
                    if let Expression::Literal(crate::expressions::Literal::String(s)) = &f.args[0] {
                        // Check if the string looks like a date (contains dashes like 2013-04-05)
                        if s.contains('-') && s.len() >= 8 && s.len() <= 12 {
                            return Ok(Expression::Cast(Box::new(Cast {
                                this: f.args.into_iter().next().unwrap(),
                                to: crate::expressions::DataType::Date,
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                            })));
                        }
                    }
                }
                // Normalize format string (2nd arg) if present
                let mut args = f.args;
                if args.len() >= 2 {
                    args[1] = Self::normalize_format_arg(args[1].clone());
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_DATE".to_string(),
                    args,
                ))))
            }

            // TO_TIME with single string arg -> CAST(arg AS TIME)
            "TO_TIME" => {
                if f.args.len() == 1 {
                    if let Expression::Literal(crate::expressions::Literal::String(_)) = &f.args[0] {
                        return Ok(Expression::Cast(Box::new(Cast {
                            this: f.args.into_iter().next().unwrap(),
                            to: crate::expressions::DataType::Time { precision: None, timezone: false },
                            double_colon_syntax: false,
                            trailing_comments: Vec::new(),
                            format: None,
                            default: None,
                        })));
                    }
                }
                // Normalize format string (2nd arg) if present
                let mut args = f.args;
                if args.len() >= 2 {
                    args[1] = Self::normalize_format_arg(args[1].clone());
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_TIME".to_string(),
                    args,
                ))))
            }

            // TO_TIMESTAMP: Snowflake has multiple forms:
            // 1. TO_TIMESTAMP('datetime_string') -> CAST('...' AS TIMESTAMP)
            // 2. TO_TIMESTAMP('epoch_string') -> UnixToTime(epoch_string)
            // 3. TO_TIMESTAMP(number) -> UnixToTime(number)
            // 4. TO_TIMESTAMP(number, scale) where scale is int -> UnixToTime(number, scale)
            // 5. TO_TIMESTAMP(string, format) where format is string -> StrToTime(string, format)
            "TO_TIMESTAMP" => {
                let args = f.args;
                if args.len() == 1 {
                    let arg = &args[0];
                    match arg {
                        Expression::Literal(Literal::String(s)) if Self::looks_like_datetime(s) => {
                            // Case 1: datetime string -> CAST AS TIMESTAMP
                            return Ok(Expression::Cast(Box::new(Cast {
                                this: args.into_iter().next().unwrap(),
                                to: DataType::Timestamp { precision: None, timezone: false },
                                double_colon_syntax: false,
                                trailing_comments: vec![],
                                format: None,
                                default: None,
                            })));
                        }
                        Expression::Literal(Literal::String(s)) if Self::looks_like_epoch(s) => {
                            // Case 2: epoch number as string -> UnixToTime
                            return Ok(Expression::UnixToTime(Box::new(crate::expressions::UnixToTime {
                                this: Box::new(args.into_iter().next().unwrap()),
                                scale: None,
                                zone: None,
                                hours: None,
                                minutes: None,
                                format: None,
                                target_type: None,
                            })));
                        }
                        Expression::Literal(Literal::Number(_)) | Expression::Neg(_) => {
                            // Case 3: number -> UnixToTime
                            return Ok(Expression::UnixToTime(Box::new(crate::expressions::UnixToTime {
                                this: Box::new(args.into_iter().next().unwrap()),
                                scale: None,
                                zone: None,
                                hours: None,
                                minutes: None,
                                format: None,
                                target_type: None,
                            })));
                        }
                        _ => {
                            // Unknown single arg, keep as function
                            return Ok(Expression::Function(Box::new(Function::new(
                                "TO_TIMESTAMP".to_string(),
                                args,
                            ))));
                        }
                    }
                } else if args.len() == 2 {
                    let second_arg = &args[1];
                    // Check if second arg is an integer (scale) or a format string
                    let is_int_scale = match second_arg {
                        Expression::Literal(Literal::Number(n)) => n.parse::<i64>().is_ok(),
                        _ => false,
                    };

                    if is_int_scale {
                        // Case 4: TO_TIMESTAMP(number, scale) -> UnixToTime
                        let mut args_iter = args.into_iter();
                        let value = args_iter.next().unwrap();
                        let scale_expr = args_iter.next().unwrap();
                        let scale = if let Expression::Literal(Literal::Number(n)) = &scale_expr {
                            n.parse::<i64>().ok()
                        } else {
                            None
                        };
                        return Ok(Expression::UnixToTime(Box::new(crate::expressions::UnixToTime {
                            this: Box::new(value),
                            scale,
                            zone: None,
                            hours: None,
                            minutes: None,
                            format: None,
                            target_type: None,
                        })));
                    } else {
                        // Case 5: TO_TIMESTAMP(string, format) -> StrToTime
                        let mut args_iter = args.into_iter();
                        let value = args_iter.next().unwrap();
                        let format_expr = args_iter.next().unwrap();
                        let format_str = match &format_expr {
                            Expression::Literal(Literal::String(s)) => s.clone(),
                            _ => {
                                // Non-string format, keep as function
                                return Ok(Expression::Function(Box::new(Function::new(
                                    "TO_TIMESTAMP".to_string(),
                                    vec![value, format_expr],
                                ))));
                            }
                        };
                        // Normalize Snowflake format to target-neutral
                        let normalized_format = Self::normalize_snowflake_format(&format_str);
                        return Ok(Expression::StrToTime(Box::new(crate::expressions::StrToTime {
                            this: Box::new(value),
                            format: normalized_format,
                            zone: None,
                            safe: None,
                            target_type: None,
                        })));
                    }
                }
                // More than 2 args or other cases, keep as function
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_TIMESTAMP".to_string(),
                    args,
                ))))
            }

            // TO_CHAR is native to Snowflake
            "TO_CHAR" => Ok(Expression::Function(Box::new(f))),

            // ROUND with named args: ROUND(EXPR => x, SCALE => y, ROUNDING_MODE => z)
            // -> ROUND(x, y) or ROUND(x, y, z)
            "ROUND" if f.args.iter().any(|a| matches!(a, Expression::NamedArgument(_))) => {
                let mut expr_val = None;
                let mut scale_val = None;
                let mut rounding_mode_val = None;
                for arg in &f.args {
                    if let Expression::NamedArgument(na) = arg {
                        match na.name.name.to_uppercase().as_str() {
                            "EXPR" => expr_val = Some(na.value.clone()),
                            "SCALE" => scale_val = Some(na.value.clone()),
                            "ROUNDING_MODE" => rounding_mode_val = Some(na.value.clone()),
                            _ => {}
                        }
                    }
                }
                if let Some(expr) = expr_val {
                    let mut args = vec![expr];
                    if let Some(scale) = scale_val {
                        args.push(scale);
                    }
                    if let Some(mode) = rounding_mode_val {
                        args.push(mode);
                    }
                    Ok(Expression::Function(Box::new(Function::new("ROUND".to_string(), args))))
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }

            // DATE_FORMAT -> TO_CHAR in Snowflake
            // Also converts strftime format to Snowflake format and wraps first arg in CAST AS TIMESTAMP
            "DATE_FORMAT" => {
                let mut args = f.args;
                // Wrap first arg in CAST AS TIMESTAMP if it's a string literal
                if !args.is_empty() {
                    if matches!(&args[0], Expression::Literal(Literal::String(_))) {
                        args[0] = Expression::Cast(Box::new(crate::expressions::Cast {
                            this: args[0].clone(),
                            to: DataType::Timestamp { precision: None, timezone: false },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        }));
                    }
                }
                // Convert strftime format to Snowflake format
                if args.len() >= 2 {
                    if let Expression::Literal(Literal::String(ref fmt)) = args[1] {
                        let sf_fmt = strftime_to_snowflake_format(fmt);
                        args[1] = Expression::Literal(Literal::String(sf_fmt));
                    }
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_CHAR".to_string(),
                    args,
                ))))
            }

            // ARRAY -> ARRAY_CONSTRUCT
            "ARRAY" => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_CONSTRUCT".to_string(),
                f.args,
            )))),

            // STRUCT -> OBJECT_CONSTRUCT
            // Convert STRUCT(value AS name, ...) to OBJECT_CONSTRUCT('name', value, ...)
            "STRUCT" => {
                let mut oc_args = Vec::new();
                for arg in f.args {
                    match arg {
                        Expression::Alias(a) => {
                            // Named field: value AS name -> 'name', value
                            oc_args.push(Expression::Literal(crate::expressions::Literal::String(a.alias.name.clone())));
                            oc_args.push(a.this);
                        }
                        other => {
                            // Unnamed field: just pass through
                            oc_args.push(other);
                        }
                    }
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "OBJECT_CONSTRUCT".to_string(),
                    oc_args,
                ))))
            }

            // JSON_EXTRACT -> GET_PATH or GET in Snowflake
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(Function::new(
                "GET_PATH".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT_SCALAR -> JSON_EXTRACT_PATH_TEXT
            "JSON_EXTRACT_SCALAR" => Ok(Expression::Function(Box::new(Function::new(
                "JSON_EXTRACT_PATH_TEXT".to_string(),
                f.args,
            )))),

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

            // CHARINDEX -> POSITION or CHARINDEX (native)
            "CHARINDEX" => Ok(Expression::Function(Box::new(f))),

            // SPLIT is native to Snowflake - keep as-is
            "SPLIT" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_AGG is native to Snowflake
            "ARRAY_AGG" => Ok(Expression::Function(Box::new(f))),

            // PARSE_JSON for JSON parsing
            "JSON_PARSE" | "PARSE_JSON" => Ok(Expression::Function(Box::new(Function::new(
                "PARSE_JSON".to_string(),
                f.args,
            )))),

            // RAND -> Rand (to use RANDOM in Snowflake)
            "RAND" => {
                let seed = f.args.first().cloned().map(Box::new);
                Ok(Expression::Rand(Box::new(crate::expressions::Rand { seed, lower: None, upper: None })))
            }

            // SHA -> SHA1
            "SHA" => Ok(Expression::Function(Box::new(Function::new(
                "SHA1".to_string(),
                f.args,
            )))),

            // APPROX_COUNT_DISTINCT is native
            "APPROX_DISTINCT" => Ok(Expression::Function(Box::new(Function::new(
                "APPROX_COUNT_DISTINCT".to_string(),
                f.args,
            )))),

            // GEN_RANDOM_UUID/UUID -> Uuid AST node
            "GEN_RANDOM_UUID" | "UUID" => Ok(Expression::Uuid(Box::new(crate::expressions::Uuid { this: None, name: None, is_string: None }))),

            // NEWID -> Uuid AST node
            "NEWID" => Ok(Expression::Uuid(Box::new(crate::expressions::Uuid { this: None, name: None, is_string: None }))),

            // UUID_STRING -> Uuid AST node (without args only; with args keep as Function for identity)
            "UUID_STRING" => {
                if f.args.is_empty() {
                    Ok(Expression::Uuid(Box::new(crate::expressions::Uuid { this: None, name: None, is_string: None })))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "UUID_STRING".to_string(),
                        f.args,
                    ))))
                }
            }

            // IF -> IFF (convert to IfFunc AST node)
            "IF" if f.args.len() >= 2 => {
                let mut args = f.args;
                let condition = args.remove(0);
                let true_val = args.remove(0);
                let false_val = if !args.is_empty() { Some(args.remove(0)) } else { None };
                Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                    condition,
                    true_value: true_val,
                    false_value: Some(false_val.unwrap_or(Expression::Null(crate::expressions::Null))),
                    original_name: Some("IFF".to_string()),
                })))
            }

            // SQUARE(x) -> POWER(x, 2)
            "SQUARE" if f.args.len() == 1 => {
                let x = f.args.into_iter().next().unwrap();
                Ok(Expression::Power(Box::new(crate::expressions::BinaryFunc { original_name: None,
                    this: x,
                    expression: Expression::number(2),
                })))
            }

            // POW(x, y) -> POWER(x, y)
            "POW" if f.args.len() == 2 => {
                let mut args = f.args.into_iter();
                let x = args.next().unwrap();
                let y = args.next().unwrap();
                Ok(Expression::Power(Box::new(crate::expressions::BinaryFunc { original_name: None,
                    this: x,
                    expression: y,
                })))
            }

            // MOD(x, y) -> x % y (modulo operator)
            "MOD" if f.args.len() == 2 => {
                let mut args = f.args.into_iter();
                let x = args.next().unwrap();
                let y = args.next().unwrap();
                Ok(Expression::Mod(Box::new(crate::expressions::BinaryOp {
                    left: x,
                    right: y,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // APPROXIMATE_JACCARD_INDEX -> APPROXIMATE_SIMILARITY
            "APPROXIMATE_JACCARD_INDEX" => Ok(Expression::Function(Box::new(Function::new(
                "APPROXIMATE_SIMILARITY".to_string(),
                f.args,
            )))),

            // ARRAY_CONSTRUCT -> Array with bracket notation in Snowflake
            "ARRAY_CONSTRUCT" => Ok(Expression::ArrayFunc(Box::new(crate::expressions::ArrayConstructor {
                expressions: f.args,
                bracket_notation: true,
                use_list_keyword: false,
            }))),

            // APPROX_TOP_K - add default k=1 if not provided
            "APPROX_TOP_K" if f.args.len() == 1 => {
                let mut args = f.args;
                args.push(Expression::number(1));
                Ok(Expression::Function(Box::new(Function::new(
                    "APPROX_TOP_K".to_string(),
                    args,
                ))))
            }

            // TO_DECIMAL, TO_NUMERIC -> TO_NUMBER
            "TO_DECIMAL" | "TO_NUMERIC" => Ok(Expression::Function(Box::new(Function::new(
                "TO_NUMBER".to_string(),
                f.args,
            )))),

            // TRY_TO_DECIMAL, TRY_TO_NUMERIC -> TRY_TO_NUMBER
            "TRY_TO_DECIMAL" | "TRY_TO_NUMERIC" => Ok(Expression::Function(Box::new(Function::new(
                "TRY_TO_NUMBER".to_string(),
                f.args,
            )))),

            // STDDEV_SAMP -> STDDEV
            "STDDEV_SAMP" => Ok(Expression::Function(Box::new(Function::new(
                "STDDEV".to_string(),
                f.args,
            )))),

            // STRTOK -> SPLIT_PART (with default delimiter and position)
            "STRTOK" if f.args.len() >= 1 => {
                let mut args = f.args;
                // Add default delimiter (space) if missing
                if args.len() == 1 {
                    args.push(Expression::string(" ".to_string()));
                }
                // Add default position (1) if missing
                if args.len() == 2 {
                    args.push(Expression::number(1));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "SPLIT_PART".to_string(),
                    args,
                ))))
            }

            // WEEKOFYEAR -> WEEK
            "WEEKOFYEAR" => Ok(Expression::Function(Box::new(Function::new(
                "WEEK".to_string(),
                f.args,
            )))),

            // LIKE(col, pattern, escape) -> col LIKE pattern ESCAPE escape
            "LIKE" if f.args.len() >= 2 => {
                let mut args = f.args.into_iter();
                let left = args.next().unwrap();
                let right = args.next().unwrap();
                let escape = args.next();
                Ok(Expression::Like(Box::new(crate::expressions::LikeOp {
                    left,
                    right,
                    escape,
                    quantifier: None,
                })))
            }

            // ILIKE(col, pattern, escape) -> col ILIKE pattern ESCAPE escape
            "ILIKE" if f.args.len() >= 2 => {
                let mut args = f.args.into_iter();
                let left = args.next().unwrap();
                let right = args.next().unwrap();
                let escape = args.next();
                Ok(Expression::ILike(Box::new(crate::expressions::LikeOp {
                    left,
                    right,
                    escape,
                    quantifier: None,
                })))
            }

            // RLIKE -> REGEXP_LIKE
            "RLIKE" if f.args.len() >= 2 => {
                let mut args = f.args.into_iter();
                let left = args.next().unwrap();
                let pattern = args.next().unwrap();
                let flags = args.next();
                Ok(Expression::RegexpLike(Box::new(crate::expressions::RegexpFunc {
                    this: left,
                    pattern,
                    flags,
                })))
            }

            // IFF -> convert to IfFunc AST node for proper cross-dialect handling
            "IFF" if f.args.len() >= 2 => {
                let mut args = f.args;
                let condition = args.remove(0);
                let true_value = args.remove(0);
                let false_value = if !args.is_empty() { Some(args.remove(0)) } else { None };
                Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                    condition,
                    true_value,
                    false_value,
                    original_name: Some("IFF".to_string()),
                })))
            }

            // TIMESTAMP_NTZ_FROM_PARTS, TIMESTAMPFROMPARTS, TIMESTAMPNTZFROMPARTS -> TIMESTAMP_FROM_PARTS
            "TIMESTAMP_NTZ_FROM_PARTS" | "TIMESTAMPFROMPARTS" | "TIMESTAMPNTZFROMPARTS" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP_FROM_PARTS".to_string(),
                    f.args,
                ))))
            }

            // TIMESTAMPLTZFROMPARTS -> TIMESTAMP_LTZ_FROM_PARTS
            "TIMESTAMPLTZFROMPARTS" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP_LTZ_FROM_PARTS".to_string(),
                    f.args,
                ))))
            }

            // TIMESTAMPTZFROMPARTS -> TIMESTAMP_TZ_FROM_PARTS
            "TIMESTAMPTZFROMPARTS" => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP_TZ_FROM_PARTS".to_string(),
                    f.args,
                ))))
            }

            // DATEADD with 3 args - transform the unit (first arg) using date part mapping
            "DATEADD" if f.args.len() >= 1 => {
                let mut args = f.args;
                args[0] = self.transform_date_part_arg(args[0].clone());
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    args,
                ))))
            }

            // DATEDIFF with 3 args - transform the unit (first arg) using date part mapping
            // Also convert _POLYGLOT_TO_DATE back to TO_DATE (from cross-dialect normalize)
            "DATEDIFF" if f.args.len() >= 1 => {
                let mut args = f.args;
                args[0] = self.transform_date_part_arg(args[0].clone());
                // Convert _POLYGLOT_TO_DATE back to TO_DATE for date args
                // (_POLYGLOT_TO_DATE is an internal marker from cross-dialect normalize)
                for i in 1..args.len() {
                    if let Expression::Function(ref func) = args[i] {
                        if func.name == "_POLYGLOT_TO_DATE" {
                            let inner_args = func.args.clone();
                            args[i] = Expression::Function(Box::new(Function::new(
                                "TO_DATE".to_string(),
                                inner_args,
                            )));
                        }
                    }
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    args,
                ))))
            }

            // TIMEDIFF -> DATEDIFF
            "TIMEDIFF" => Ok(Expression::Function(Box::new(Function::new(
                "DATEDIFF".to_string(),
                f.args,
            )))),

            // TIMESTAMPDIFF -> DATEDIFF
            "TIMESTAMPDIFF" => Ok(Expression::Function(Box::new(Function::new(
                "DATEDIFF".to_string(),
                f.args,
            )))),

            // TIMESTAMPADD -> DATEADD
            "TIMESTAMPADD" => Ok(Expression::Function(Box::new(Function::new(
                "DATEADD".to_string(),
                f.args,
            )))),

            // TIMEADD -> preserve it
            "TIMEADD" => Ok(Expression::Function(Box::new(f))),

            // DATE_FROM_PARTS, DATEFROMPARTS -> DATE_FROM_PARTS
            "DATEFROMPARTS" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FROM_PARTS".to_string(),
                f.args,
            )))),

            // TIME_FROM_PARTS, TIMEFROMPARTS -> TIME_FROM_PARTS
            "TIMEFROMPARTS" => Ok(Expression::Function(Box::new(Function::new(
                "TIME_FROM_PARTS".to_string(),
                f.args,
            )))),

            // DAYOFWEEK -> DAYOFWEEK (preserve)
            "DAYOFWEEK" => Ok(Expression::Function(Box::new(f))),

            // DAYOFMONTH -> DAYOFMONTH (preserve)
            "DAYOFMONTH" => Ok(Expression::Function(Box::new(f))),

            // DAYOFYEAR -> DAYOFYEAR (preserve)
            "DAYOFYEAR" => Ok(Expression::Function(Box::new(f))),

            // MONTHNAME -> Monthname AST node (abbreviated=true for Snowflake)
            // Target dialects can then convert to their native form
            "MONTHNAME" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Monthname(Box::new(crate::expressions::Monthname {
                    this: Box::new(arg),
                    abbreviated: Some(Box::new(Expression::Literal(Literal::String("true".to_string())))),
                })))
            }

            // DAYNAME -> Dayname AST node (abbreviated=true for Snowflake)
            // Target dialects can then convert to their native form
            "DAYNAME" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Dayname(Box::new(crate::expressions::Dayname {
                    this: Box::new(arg),
                    abbreviated: Some(Box::new(Expression::Literal(Literal::String("true".to_string())))),
                })))
            }

            // BOOLAND_AGG/BOOL_AND/LOGICAL_AND -> LogicalAnd AST node
            "BOOLAND_AGG" | "BOOL_AND" | "LOGICAL_AND" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::LogicalAnd(Box::new(AggFunc {
                    this: arg,
                    distinct: false,
                    filter: None,
                    order_by: Vec::new(),
                    name: Some("BOOLAND_AGG".to_string()),
                    ignore_nulls: None,
                    having_max: None,
                    limit: None,
                })))
            }

            // BOOLOR_AGG/BOOL_OR/LOGICAL_OR -> LogicalOr AST node
            "BOOLOR_AGG" | "BOOL_OR" | "LOGICAL_OR" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::LogicalOr(Box::new(AggFunc {
                    this: arg,
                    distinct: false,
                    filter: None,
                    order_by: Vec::new(),
                    name: Some("BOOLOR_AGG".to_string()),
                    ignore_nulls: None,
                    having_max: None,
                    limit: None,
                })))
            }

            // SKEW -> Skewness AST node for proper cross-dialect handling
            "SKEW" | "SKEWNESS" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Skewness(Box::new(AggFunc {
                    this: arg,
                    distinct: false,
                    filter: None,
                    order_by: Vec::new(),
                    name: Some("SKEW".to_string()),
                    ignore_nulls: None,
                    having_max: None,
                    limit: None,
                })))
            }

            // VAR_SAMP -> VARIANCE (Snowflake uses VARIANCE for sample variance)
            "VAR_SAMP" => Ok(Expression::Function(Box::new(Function::new(
                "VARIANCE".to_string(),
                f.args,
            )))),

            // VAR_POP -> VARIANCE_POP
            "VAR_POP" => Ok(Expression::Function(Box::new(Function::new(
                "VARIANCE_POP".to_string(),
                f.args,
            )))),

            // DATE(str) -> TO_DATE(str) (single-arg form)
            "DATE" if f.args.len() == 1 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_DATE".to_string(),
                    f.args,
                ))))
            }
            // DATE(str, format) -> TO_DATE(str, normalized_format)
            // Python SQLGlot normalizes DATE(...) to TO_DATE(...) for formatted variants.
            // But _POLYGLOT_DATE(str, format) stays as DATE() (from BigQuery PARSE_DATE conversion)
            "DATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                args[1] = Self::normalize_format_arg(args[1].clone());
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_DATE".to_string(),
                    args,
                ))))
            }
            // Internal marker from BigQuery PARSE_DATE -> Snowflake conversion
            // _POLYGLOT_DATE stays as DATE() (not converted to TO_DATE)
            "_POLYGLOT_DATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                args[1] = Self::normalize_format_arg(args[1].clone());
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE".to_string(),
                    args,
                ))))
            }

            // DESCRIBE/DESC normalization
            "DESCRIBE" => Ok(Expression::Function(Box::new(f))),

            // MD5 -> MD5 (preserve) but MD5_HEX -> MD5
            "MD5_HEX" => Ok(Expression::Function(Box::new(Function::new(
                "MD5".to_string(),
                f.args,
            )))),

            // SHA1_HEX -> SHA1
            "SHA1_HEX" => Ok(Expression::Function(Box::new(Function::new(
                "SHA1".to_string(),
                f.args,
            )))),

            // SHA2_HEX -> SHA2
            "SHA2_HEX" => Ok(Expression::Function(Box::new(Function::new(
                "SHA2".to_string(),
                f.args,
            )))),

            // EDITDISTANCE -> EDITDISTANCE (preserve Snowflake name)
            "LEVENSHTEIN" => Ok(Expression::Function(Box::new(Function::new(
                "EDITDISTANCE".to_string(),
                f.args,
            )))),

            // BIT_NOT -> BITNOT
            "BIT_NOT" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "BITNOT".to_string(),
                f.args,
            )))),

            // BIT_AND -> BITAND
            "BIT_AND" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "BITAND".to_string(),
                f.args,
            )))),

            // BIT_OR -> BITOR
            "BIT_OR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "BITOR".to_string(),
                f.args,
            )))),

            // BIT_XOR -> BITXOR
            "BIT_XOR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "BITXOR".to_string(),
                f.args,
            )))),

            // BIT_SHIFTLEFT -> BITSHIFTLEFT
            "BIT_SHIFTLEFT" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "BITSHIFTLEFT".to_string(),
                f.args,
            )))),

            // BIT_SHIFTRIGHT -> BITSHIFTRIGHT
            "BIT_SHIFTRIGHT" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "BITSHIFTRIGHT".to_string(),
                f.args,
            )))),

            // SYSTIMESTAMP -> CURRENT_TIMESTAMP (preserving parens style)
            "SYSTIMESTAMP" => Ok(Expression::Function(Box::new(Function {
                name: "CURRENT_TIMESTAMP".to_string(),
                args: f.args,
                distinct: false,
                trailing_comments: Vec::new(),
                use_bracket_syntax: false,
                no_parens: f.no_parens, quoted: false,
            }))),

            // LOCALTIMESTAMP -> CURRENT_TIMESTAMP (preserving parens style)
            "LOCALTIMESTAMP" => {
                Ok(Expression::Function(Box::new(Function {
                    name: "CURRENT_TIMESTAMP".to_string(),
                    args: f.args,
                    distinct: false,
                    trailing_comments: Vec::new(),
                    use_bracket_syntax: false,
                    no_parens: f.no_parens, quoted: false,
                })))
            }

            // CEILING -> CEIL
            "CEILING" => Ok(Expression::Function(Box::new(Function::new(
                "CEIL".to_string(),
                f.args,
            )))),

            // LOG without base -> LN
            "LOG" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LN".to_string(),
                f.args,
            )))),

            // REGEXP_SUBSTR_ALL -> REGEXP_EXTRACT_ALL
            "REGEXP_SUBSTR_ALL" => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_EXTRACT_ALL".to_string(),
                f.args,
            )))),

            // GET_PATH - transform path argument:
            // - Convert colon notation to dot notation (y[0]:z -> y[0].z)
            // - Wrap unsafe keys in brackets ($id -> ["$id"])
            "GET_PATH" if f.args.len() >= 2 => {
                let mut args = f.args;
                // Transform the path argument (second argument)
                if let Expression::Literal(crate::expressions::Literal::String(path)) = &args[1] {
                    let transformed = Self::transform_json_path(path);
                    args[1] = Expression::Literal(crate::expressions::Literal::String(transformed));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "GET_PATH".to_string(),
                    args,
                ))))
            }
            "GET_PATH" => Ok(Expression::Function(Box::new(f))),

            // FLATTEN is native to Snowflake
            "FLATTEN" => Ok(Expression::Function(Box::new(f))),

            // DATE_TRUNC - transform unit to quoted string
            // DATE_TRUNC(yr, x) -> DATE_TRUNC('YEAR', x)
            "DATE_TRUNC" if f.args.len() >= 1 => {
                let mut args = f.args;
                // Transform the unit to canonical form and convert to string literal
                let unit_name = match &args[0] {
                    Expression::Identifier(id) => Some(id.name.as_str()),
                    Expression::Column(col) if col.table.is_none() => Some(col.name.name.as_str()),
                    _ => None,
                };
                if let Some(name) = unit_name {
                    let canonical = Self::map_date_part(name).unwrap_or(name);
                    args[0] = Expression::Literal(crate::expressions::Literal::String(canonical.to_uppercase()));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_TRUNC".to_string(),
                    args,
                ))))
            }

            // DATE_PART - transform unit argument
            // DATE_PART(yyy, x) -> DATE_PART(YEAR, x)
            // Only convert string literals to identifiers when the second arg is a typed literal
            // (e.g., TIMESTAMP '...', DATE '...'), indicating the function came from another dialect.
            // For native Snowflake DATE_PART('month', CAST(...)), preserve the string as-is.
            "DATE_PART" if f.args.len() >= 1 => {
                let mut args = f.args;
                let from_typed_literal = args.len() >= 2 && matches!(&args[1],
                    Expression::Literal(crate::expressions::Literal::Timestamp(_))
                    | Expression::Literal(crate::expressions::Literal::Date(_))
                    | Expression::Literal(crate::expressions::Literal::Time(_))
                    | Expression::Literal(crate::expressions::Literal::Datetime(_))
                );
                if from_typed_literal {
                    args[0] = self.transform_date_part_arg(args[0].clone());
                } else {
                    // For non-typed-literal cases, only normalize identifiers/columns
                    // (don't convert string literals to identifiers)
                    args[0] = self.transform_date_part_arg_identifiers_only(args[0].clone());
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_PART".to_string(),
                    args,
                ))))
            }

            // OBJECT_CONSTRUCT is native to Snowflake
            "OBJECT_CONSTRUCT" => Ok(Expression::Function(Box::new(f))),

            // OBJECT_CONSTRUCT_KEEP_NULL is native to Snowflake
            "OBJECT_CONSTRUCT_KEEP_NULL" => Ok(Expression::Function(Box::new(f))),

            // DESC -> DESCRIBE
            "DESC" => Ok(Expression::Function(Box::new(Function::new(
                "DESCRIBE".to_string(),
                f.args,
            )))),

            // RLIKE -> REGEXP_LIKE
            "RLIKE" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_LIKE".to_string(),
                f.args,
            )))),

            // TRANSFORM function - handle typed lambda parameters
            // For typed lambdas like `a int -> a + 1`, we need to:
            // 1. Remove the type annotation from the parameter
            // 2. Wrap all references to the parameter in the body with CAST(param AS type)
            "TRANSFORM" => {
                let transformed_args: Vec<Expression> = f.args.into_iter().map(|arg| {
                    if let Expression::Lambda(lambda) = arg {
                        self.transform_typed_lambda(*lambda)
                    } else {
                        arg
                    }
                }).collect();
                Ok(Expression::Function(Box::new(Function::new(
                    "TRANSFORM".to_string(),
                    transformed_args,
                ))))
            }

            // SEARCH function - convert to Search expression with canonical parameter ordering
            "SEARCH" if f.args.len() >= 2 => {
                let mut args = f.args.into_iter();
                let this = Box::new(args.next().unwrap());
                let expression = Box::new(args.next().unwrap());

                let mut analyzer: Option<Box<Expression>> = None;
                let mut search_mode: Option<Box<Expression>> = None;

                // Parse remaining named arguments
                for arg in args {
                    if let Expression::NamedArgument(na) = &arg {
                        let name_upper = na.name.name.to_uppercase();
                        match name_upper.as_str() {
                            "ANALYZER" => analyzer = Some(Box::new(arg)),
                            "SEARCH_MODE" => search_mode = Some(Box::new(arg)),
                            _ => {}
                        }
                    }
                }

                Ok(Expression::Search(Box::new(crate::expressions::Search {
                    this,
                    expression,
                    json_scope: None,
                    analyzer,
                    analyzer_options: None,
                    search_mode,
                })))
            }

            // ODBC CONVERT function: CONVERT(value, SQL_TYPE) -> CAST(value AS TYPE)
            // This handles the { fn CONVERT(...) } ODBC escape sequence syntax
            "CONVERT" if f.args.len() == 2 => {
                let value = f.args.get(0).cloned().unwrap();
                let type_arg = f.args.get(1).cloned().unwrap();

                // Check if second argument is a SQL_ type identifier
                if let Expression::Column(col) = &type_arg {
                    let type_name = col.name.name.to_uppercase();
                    let data_type = match type_name.as_str() {
                        "SQL_DOUBLE" => Some(DataType::Double { precision: None, scale: None }),
                        "SQL_VARCHAR" => Some(DataType::VarChar { length: None, parenthesized_length: false }),
                        "SQL_INTEGER" | "SQL_INT" => Some(DataType::Int { length: None, integer_spelling: false }),
                        "SQL_BIGINT" => Some(DataType::BigInt { length: None }),
                        "SQL_SMALLINT" => Some(DataType::SmallInt { length: None }),
                        "SQL_FLOAT" => Some(DataType::Float { precision: None, scale: None, real_spelling: false }),
                        "SQL_REAL" => Some(DataType::Float { precision: None, scale: None, real_spelling: true }),
                        "SQL_DECIMAL" => Some(DataType::Decimal { precision: None, scale: None }),
                        "SQL_DATE" => Some(DataType::Date),
                        "SQL_TIME" => Some(DataType::Time { precision: None, timezone: false }),
                        "SQL_TIMESTAMP" => Some(DataType::Timestamp { precision: None, timezone: false }),
                        _ => None,
                    };

                    if let Some(dt) = data_type {
                        return Ok(Expression::Cast(Box::new(Cast {
                            this: value,
                            to: dt,
                            double_colon_syntax: false,
                            trailing_comments: vec![],
                            format: None,
                            default: None,
                        })));
                    }
                }
                // If not a SQL_ type, keep as regular CONVERT function
                Ok(Expression::Function(Box::new(f)))
            }

            // TO_TIMESTAMP_TZ: single string arg -> CAST(... AS TIMESTAMPTZ), otherwise keep as function
            // Per Python sqlglot: _build_datetime converts TO_TIMESTAMP_TZ('string') to CAST('string' AS TIMESTAMPTZ)
            "TO_TIMESTAMP_TZ" => {
                if f.args.len() == 1 {
                    if let Expression::Literal(crate::expressions::Literal::String(_)) = &f.args[0] {
                        return Ok(Expression::Cast(Box::new(Cast {
                            this: f.args.into_iter().next().unwrap(),
                            to: DataType::Custom { name: "TIMESTAMPTZ".to_string() },
                            double_colon_syntax: false,
                            trailing_comments: vec![],
                            format: None,
                            default: None,
                        })));
                    }
                }
                Ok(Expression::Function(Box::new(f)))
            }

            // TO_TIMESTAMP_NTZ: single string arg -> CAST(... AS TIMESTAMPNTZ), otherwise keep as function
            "TO_TIMESTAMP_NTZ" => {
                if f.args.len() == 1 {
                    if let Expression::Literal(crate::expressions::Literal::String(_)) = &f.args[0] {
                        return Ok(Expression::Cast(Box::new(Cast {
                            this: f.args.into_iter().next().unwrap(),
                            to: DataType::Custom { name: "TIMESTAMPNTZ".to_string() },
                            double_colon_syntax: false,
                            trailing_comments: vec![],
                            format: None,
                            default: None,
                        })));
                    }
                }
                Ok(Expression::Function(Box::new(f)))
            }

            // TO_TIMESTAMP_LTZ: single string arg -> CAST(... AS TIMESTAMPLTZ), otherwise keep as function
            "TO_TIMESTAMP_LTZ" => {
                if f.args.len() == 1 {
                    if let Expression::Literal(crate::expressions::Literal::String(_)) = &f.args[0] {
                        return Ok(Expression::Cast(Box::new(Cast {
                            this: f.args.into_iter().next().unwrap(),
                            to: DataType::Custom { name: "TIMESTAMPLTZ".to_string() },
                            double_colon_syntax: false,
                            trailing_comments: vec![],
                            format: None,
                            default: None,
                        })));
                    }
                }
                Ok(Expression::Function(Box::new(f)))
            }

            // UNIFORM -> keep as-is (Snowflake-specific)
            "UNIFORM" => Ok(Expression::Function(Box::new(f))),

            // REPLACE with 2 args -> add empty string 3rd arg
            "REPLACE" if f.args.len() == 2 => {
                let mut args = f.args;
                args.push(Expression::Literal(crate::expressions::Literal::String(String::new())));
                Ok(Expression::Function(Box::new(Function::new(
                    "REPLACE".to_string(),
                    args,
                ))))
            }

            // ARBITRARY -> ANY_VALUE in Snowflake
            "ARBITRARY" => Ok(Expression::Function(Box::new(Function::new(
                "ANY_VALUE".to_string(),
                f.args,
            )))),

            // SAFE_DIVIDE(x, y) -> IFF(y <> 0, x / y, NULL)
            "SAFE_DIVIDE" if f.args.len() == 2 => {
                let mut args = f.args;
                let x = args.remove(0);
                let y = args.remove(0);
                Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                    condition: Expression::Neq(Box::new(BinaryOp {
                        left: y.clone(),
                        right: Expression::number(0),
                        left_comments: Vec::new(),
                        operator_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    })),
                    true_value: Expression::Div(Box::new(BinaryOp {
                        left: x,
                        right: y,
                        left_comments: Vec::new(),
                        operator_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    })),
                    false_value: Some(Expression::Null(crate::expressions::Null)),
                    original_name: Some("IFF".to_string()),
                })))
            }

            // TIMESTAMP(x) -> CAST(x AS TIMESTAMPTZ) in Snowflake
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

            // TIMESTAMP(x, tz) -> CONVERT_TIMEZONE(tz, CAST(x AS TIMESTAMP)) in Snowflake
            "TIMESTAMP" if f.args.len() == 2 => {
                let mut args = f.args;
                let value = args.remove(0);
                let tz = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "CONVERT_TIMEZONE".to_string(),
                    vec![
                        tz,
                        Expression::Cast(Box::new(Cast {
                            this: value,
                            to: DataType::Timestamp { precision: None, timezone: false },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        })),
                    ],
                ))))
            }

            // TIME(h, m, s) -> TIME_FROM_PARTS(h, m, s) in Snowflake
            "TIME" if f.args.len() == 3 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIME_FROM_PARTS".to_string(),
                    f.args,
                ))))
            }

            // DIV0(x, y) -> IFF(y = 0 AND NOT x IS NULL, 0, x / y)
            "DIV0" if f.args.len() == 2 => {
                let mut args = f.args;
                let x = args.remove(0);
                let y = args.remove(0);
                // Need parens around complex expressions
                let x_expr = Self::maybe_paren(x.clone());
                let y_expr = Self::maybe_paren(y.clone());
                Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                    condition: Expression::And(Box::new(BinaryOp::new(
                        Expression::Eq(Box::new(BinaryOp::new(
                            y_expr.clone(),
                            Expression::number(0),
                        ))),
                        Expression::Not(Box::new(crate::expressions::UnaryOp {
                            this: Expression::IsNull(Box::new(crate::expressions::IsNull {
                                this: x_expr.clone(),
                                not: false,
                                postfix_form: false,
                            })),
                        })),
                    ))),
                    true_value: Expression::number(0),
                    false_value: Some(Expression::Div(Box::new(BinaryOp::new(x_expr, y_expr)))),
                    original_name: Some("IFF".to_string()),
                })))
            }

            // DIV0NULL(x, y) -> IFF(y = 0 OR y IS NULL, 0, x / y)
            "DIV0NULL" if f.args.len() == 2 => {
                let mut args = f.args;
                let x = args.remove(0);
                let y = args.remove(0);
                let x_expr = Self::maybe_paren(x.clone());
                let y_expr = Self::maybe_paren(y.clone());
                Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                    condition: Expression::Or(Box::new(BinaryOp::new(
                        Expression::Eq(Box::new(BinaryOp::new(
                            y_expr.clone(),
                            Expression::number(0),
                        ))),
                        Expression::IsNull(Box::new(crate::expressions::IsNull {
                            this: y_expr.clone(),
                            not: false,
                            postfix_form: false,
                        })),
                    ))),
                    true_value: Expression::number(0),
                    false_value: Some(Expression::Div(Box::new(BinaryOp::new(x_expr, y_expr)))),
                    original_name: Some("IFF".to_string()),
                })))
            }

            // ZEROIFNULL(x) -> IFF(x IS NULL, 0, x)
            "ZEROIFNULL" if f.args.len() == 1 => {
                let x = f.args.into_iter().next().unwrap();
                Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                    condition: Expression::IsNull(Box::new(crate::expressions::IsNull {
                        this: x.clone(),
                        not: false,
                        postfix_form: false,
                    })),
                    true_value: Expression::number(0),
                    false_value: Some(x),
                    original_name: Some("IFF".to_string()),
                })))
            }

            // NULLIFZERO(x) -> IFF(x = 0, NULL, x)
            "NULLIFZERO" if f.args.len() == 1 => {
                let x = f.args.into_iter().next().unwrap();
                Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                    condition: Expression::Eq(Box::new(BinaryOp::new(
                        x.clone(),
                        Expression::number(0),
                    ))),
                    true_value: Expression::Null(crate::expressions::Null),
                    false_value: Some(x),
                    original_name: Some("IFF".to_string()),
                })))
            }

            // TRY_TO_TIME('string') -> TRY_CAST('string' AS TIME) when single string arg
            "TRY_TO_TIME" => {
                if f.args.len() == 1 {
                    if let Expression::Literal(crate::expressions::Literal::String(_)) = &f.args[0] {
                        return Ok(Expression::TryCast(Box::new(Cast {
                            this: f.args.into_iter().next().unwrap(),
                            to: crate::expressions::DataType::Time { precision: None, timezone: false },
                            double_colon_syntax: false,
                            trailing_comments: Vec::new(),
                            format: None,
                            default: None,
                        })));
                    }
                }
                // Normalize format string (2nd arg) if present
                let mut args = f.args;
                if args.len() >= 2 {
                    args[1] = Self::normalize_format_arg(args[1].clone());
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TRY_TO_TIME".to_string(),
                    args,
                ))))
            }

            // TRY_TO_TIMESTAMP('string') -> TRY_CAST('string' AS TIMESTAMP) when single string arg
            // Convert if the string is NOT a pure numeric/epoch value
            "TRY_TO_TIMESTAMP" => {
                if f.args.len() == 1 {
                    if let Expression::Literal(crate::expressions::Literal::String(s)) = &f.args[0] {
                        if !Self::looks_like_epoch(s) {
                            return Ok(Expression::TryCast(Box::new(Cast {
                                this: f.args.into_iter().next().unwrap(),
                                to: DataType::Timestamp { precision: None, timezone: false },
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                            })));
                        }
                    }
                }
                // Normalize format string (2nd arg) if present
                let mut args = f.args;
                if args.len() >= 2 {
                    args[1] = Self::normalize_format_arg(args[1].clone());
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TRY_TO_TIMESTAMP".to_string(),
                    args,
                ))))
            }

            // TRY_TO_DATE('string') -> TRY_CAST('string' AS DATE) when single string arg
            "TRY_TO_DATE" => {
                if f.args.len() == 1 {
                    if let Expression::Literal(crate::expressions::Literal::String(s)) = &f.args[0] {
                        // Only convert if the string looks like a date
                        if s.contains('-') && s.len() >= 8 && s.len() <= 12 {
                            return Ok(Expression::TryCast(Box::new(Cast {
                                this: f.args.into_iter().next().unwrap(),
                                to: crate::expressions::DataType::Date,
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                            })));
                        }
                    }
                }
                // Normalize format string (2nd arg) if present
                let mut args = f.args;
                if args.len() >= 2 {
                    args[1] = Self::normalize_format_arg(args[1].clone());
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TRY_TO_DATE".to_string(),
                    args,
                ))))
            }

            // TRY_TO_DOUBLE -> keep as TRY_TO_DOUBLE in Snowflake (native function)
            "TRY_TO_DOUBLE" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_REPLACE with 2 args -> add empty string replacement
            "REGEXP_REPLACE" if f.args.len() == 2 => {
                let mut args = f.args;
                args.push(Expression::Literal(crate::expressions::Literal::String(String::new())));
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_REPLACE".to_string(),
                    args,
                ))))
            }

            // LAST_DAY(x, MONTH) -> LAST_DAY(x) in Snowflake (strip MONTH default)
            "LAST_DAY" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let unit = args.remove(0);
                let unit_str = match &unit {
                    Expression::Column(c) => c.name.name.to_uppercase(),
                    Expression::Identifier(i) => i.name.to_uppercase(),
                    _ => String::new(),
                };
                if unit_str == "MONTH" {
                    Ok(Expression::Function(Box::new(Function::new("LAST_DAY".to_string(), vec![date]))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new("LAST_DAY".to_string(), vec![date, unit]))))
                }
            }

            // EXTRACT('field', expr) function-call syntax -> DATE_PART('field', expr)
            "EXTRACT" if f.args.len() == 2 => {
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_PART".to_string(),
                    f.args,
                ))))
            }

            // ENDS_WITH/ENDSWITH -> EndsWith AST node
            "ENDS_WITH" | "ENDSWITH" if f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let expr = args.remove(0);
                Ok(Expression::EndsWith(Box::new(crate::expressions::BinaryFunc {
                    original_name: None,
                    this,
                    expression: expr,
                })))
            }

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }

    /// Check if a string looks like a datetime (contains date separators, not just digits)
    fn looks_like_datetime(s: &str) -> bool {
        // A datetime string typically contains dashes, colons, or spaces
        // A numeric/epoch string is just digits (possibly with a dot)
        s.contains('-') || s.contains(':') || s.contains(' ') || s.contains('/')
    }

    /// Check if a string looks like an epoch number (only digits, possibly with a dot)
    fn looks_like_epoch(s: &str) -> bool {
        !s.is_empty() && s.chars().all(|c| c.is_ascii_digit() || c == '.')
    }

    /// Wrap an expression in parentheses if it's a complex expression (binary op, etc.)
    fn maybe_paren(expr: Expression) -> Expression {
        match &expr {
            Expression::Sub(_) | Expression::Add(_) | Expression::Mul(_) | Expression::Div(_) => {
                Expression::Paren(Box::new(crate::expressions::Paren {
                    this: expr,
                    trailing_comments: Vec::new(),
                }))
            }
            _ => expr,
        }
    }

    /// Normalize Snowflake date/time format strings to canonical lowercase form.
    /// YYYY -> yyyy, MM -> mm, DD -> DD (stays), HH24 -> hh24, HH12 -> hh12,
    /// MI -> mi, SS -> ss, FF -> ff, AM/PM -> pm, quoted "T" -> T
    fn normalize_snowflake_format(format: &str) -> String {
        let mut result = String::new();
        let chars: Vec<char> = format.chars().collect();
        let mut i = 0;
        while i < chars.len() {
            // Handle quoted strings like "T" -> T
            if chars[i] == '"' {
                i += 1;
                while i < chars.len() && chars[i] != '"' {
                    result.push(chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    i += 1; // skip closing quote
                }
                continue;
            }

            let remaining = &format[i..];
            let remaining_upper = remaining.to_uppercase();

            // Multi-char patterns (check longest first)
            if remaining_upper.starts_with("YYYY") {
                result.push_str("yyyy");
                i += 4;
            } else if remaining_upper.starts_with("YY") {
                result.push_str("yy");
                i += 2;
            } else if remaining_upper.starts_with("MMMM") {
                result.push_str("mmmm");
                i += 4;
            } else if remaining_upper.starts_with("MON") {
                result.push_str("mon");
                i += 3;
            } else if remaining_upper.starts_with("MM") {
                result.push_str("mm");
                i += 2;
            } else if remaining_upper.starts_with("DD") {
                result.push_str("DD");
                i += 2;
            } else if remaining_upper.starts_with("DY") {
                result.push_str("dy");
                i += 2;
            } else if remaining_upper.starts_with("HH24") {
                result.push_str("hh24");
                i += 4;
            } else if remaining_upper.starts_with("HH12") {
                result.push_str("hh12");
                i += 4;
            } else if remaining_upper.starts_with("HH") {
                result.push_str("hh");
                i += 2;
            } else if remaining_upper.starts_with("MISS") {
                // MISS = MI + SS
                result.push_str("miss");
                i += 4;
            } else if remaining_upper.starts_with("MI") {
                result.push_str("mi");
                i += 2;
            } else if remaining_upper.starts_with("SS") {
                result.push_str("ss");
                i += 2;
            } else if remaining_upper.starts_with("FF") {
                // FF followed by a digit (FF1-FF9) keeps the digit
                let ff_len = 2;
                let digit = if i + ff_len < chars.len() && chars[i + ff_len].is_ascii_digit() {
                    let d = chars[i + ff_len];
                    Some(d)
                } else {
                    None
                };
                if let Some(d) = digit {
                    result.push_str("ff");
                    result.push(d);
                    i += 3;
                } else {
                    // Plain FF -> ff9
                    result.push_str("ff9");
                    i += 2;
                }
            } else if remaining_upper.starts_with("AM") || remaining_upper.starts_with("PM") {
                result.push_str("pm");
                i += 2;
            } else if remaining_upper.starts_with("TZH") {
                result.push_str("tzh");
                i += 3;
            } else if remaining_upper.starts_with("TZM") {
                result.push_str("tzm");
                i += 3;
            } else {
                // Keep separators and other characters as-is
                result.push(chars[i]);
                i += 1;
            }
        }
        result
    }

    /// Normalize format string argument if it's a string literal
    fn normalize_format_arg(expr: Expression) -> Expression {
        if let Expression::Literal(crate::expressions::Literal::String(s)) = &expr {
            let normalized = Self::normalize_snowflake_format(s);
            Expression::Literal(crate::expressions::Literal::String(normalized))
        } else {
            expr
        }
    }

    /// Transform a lambda with typed parameters for Snowflake
    /// For `a int -> a + a + 1`, transforms to `a -> CAST(a AS INT) + CAST(a AS INT) + 1`
    fn transform_typed_lambda(&self, lambda: crate::expressions::LambdaExpr) -> Expression {
        use crate::expressions::{LambdaExpr, DataType};
        use std::collections::HashMap;

        // Build mapping of parameter names to their types
        let mut param_types: HashMap<String, DataType> = HashMap::new();
        for (i, param) in lambda.parameters.iter().enumerate() {
            if let Some(Some(dt)) = lambda.parameter_types.get(i) {
                param_types.insert(param.name.to_uppercase(), dt.clone());
            }
        }

        // If no typed parameters, return lambda unchanged
        if param_types.is_empty() {
            return Expression::Lambda(Box::new(lambda));
        }

        // Transform the body by replacing parameter references with CAST expressions
        let transformed_body = self.replace_lambda_params_with_cast(lambda.body, &param_types);

        // Return new lambda without type annotations (they're now embedded in CAST)
        Expression::Lambda(Box::new(LambdaExpr {
            parameters: lambda.parameters,
            body: transformed_body,
            colon: lambda.colon,
            parameter_types: Vec::new(), // Clear type annotations
        }))
    }

    /// Recursively replace column/identifier references to typed lambda parameters with CAST expressions
    fn replace_lambda_params_with_cast(
        &self,
        expr: Expression,
        param_types: &std::collections::HashMap<String, crate::expressions::DataType>,
    ) -> Expression {
        use crate::expressions::{Cast, BinaryOp, Paren};

        match expr {
            // Column reference - check if it matches a typed parameter
            Expression::Column(col) if col.table.is_none() => {
                let name_upper = col.name.name.to_uppercase();
                if let Some(dt) = param_types.get(&name_upper) {
                    // Wrap in CAST
                    Expression::Cast(Box::new(Cast {
                        this: Expression::Column(col),
                        to: dt.clone(),
                        double_colon_syntax: false,
                        trailing_comments: Vec::new(),
                        format: None,
                        default: None,
                    }))
                } else {
                    Expression::Column(col)
                }
            }

            // Identifier reference - check if it matches a typed parameter
            Expression::Identifier(id) => {
                let name_upper = id.name.to_uppercase();
                if let Some(dt) = param_types.get(&name_upper) {
                    // Wrap in CAST
                    Expression::Cast(Box::new(Cast {
                        this: Expression::Identifier(id),
                        to: dt.clone(),
                        double_colon_syntax: false,
                        trailing_comments: Vec::new(),
                        format: None,
                        default: None,
                    }))
                } else {
                    Expression::Identifier(id)
                }
            }

            // Binary operations - recursively transform both sides
            Expression::Add(op) => Expression::Add(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Sub(op) => Expression::Sub(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Mul(op) => Expression::Mul(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Div(op) => Expression::Div(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Mod(op) => Expression::Mod(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),

            // Parenthesized expression
            Expression::Paren(p) => Expression::Paren(Box::new(Paren {
                this: self.replace_lambda_params_with_cast(p.this, param_types),
                trailing_comments: p.trailing_comments,
            })),

            // Function calls - transform arguments
            Expression::Function(mut f) => {
                f.args = f.args.into_iter()
                    .map(|arg| self.replace_lambda_params_with_cast(arg, param_types))
                    .collect();
                Expression::Function(f)
            }

            // Comparison operators
            Expression::Eq(op) => Expression::Eq(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Neq(op) => Expression::Neq(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Lt(op) => Expression::Lt(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Lte(op) => Expression::Lte(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Gt(op) => Expression::Gt(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Gte(op) => Expression::Gte(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),

            // And/Or
            Expression::And(op) => Expression::And(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),
            Expression::Or(op) => Expression::Or(Box::new(BinaryOp::new(
                self.replace_lambda_params_with_cast(op.left, param_types),
                self.replace_lambda_params_with_cast(op.right, param_types),
            ))),

            // Other expressions - return unchanged
            other => other,
        }
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

            // STRING_AGG -> LISTAGG
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "LISTAGG".to_string(),
                f.args,
            )))),

            // APPROX_DISTINCT -> APPROX_COUNT_DISTINCT
            "APPROX_DISTINCT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("APPROX_COUNT_DISTINCT".to_string(), f.args),
            ))),

            // BIT_AND -> BITAND_AGG
            "BIT_AND" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "BITAND_AGG".to_string(),
                f.args,
            )))),

            // BIT_OR -> BITOR_AGG
            "BIT_OR" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "BITOR_AGG".to_string(),
                f.args,
            )))),

            // BIT_XOR -> BITXOR_AGG
            "BIT_XOR" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "BITXOR_AGG".to_string(),
                f.args,
            )))),

            // BOOL_AND/BOOLAND_AGG/LOGICAL_AND -> LogicalAnd AST node
            "BOOL_AND" | "LOGICAL_AND" | "BOOLAND_AGG" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::LogicalAnd(Box::new(AggFunc {
                    this: arg,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: Some("BOOLAND_AGG".to_string()),
                    ignore_nulls: None,
                    having_max: None,
                    limit: None,
                })))
            }

            // BOOL_OR/BOOLOR_AGG/LOGICAL_OR -> LogicalOr AST node
            "BOOL_OR" | "LOGICAL_OR" | "BOOLOR_AGG" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::LogicalOr(Box::new(AggFunc {
                    this: arg,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: Some("BOOLOR_AGG".to_string()),
                    ignore_nulls: None,
                    having_max: None,
                    limit: None,
                })))
            }

            // APPROX_TOP_K - add default k=1 if only one argument
            "APPROX_TOP_K" if f.args.len() == 1 => {
                let mut args = f.args;
                args.push(Expression::number(1));
                Ok(Expression::AggregateFunction(Box::new(
                    crate::expressions::AggregateFunction {
                        name: "APPROX_TOP_K".to_string(),
                        args,
                        distinct: f.distinct,
                        filter: f.filter,
                        order_by: Vec::new(),
                        limit: None,
                        ignore_nulls: None,
                    },
                )))
            }

            // SKEW/SKEWNESS -> Skewness AST node
            "SKEW" | "SKEWNESS" if !f.args.is_empty() => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Skewness(Box::new(AggFunc {
                    this: arg,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: Some("SKEW".to_string()),
                    ignore_nulls: None,
                    having_max: None,
                    limit: None,
                })))
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}

/// Convert strftime format specifiers to Snowflake format specifiers
fn strftime_to_snowflake_format(fmt: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        if chars[i] == '%' && i + 1 < chars.len() {
            match chars[i + 1] {
                'Y' => { result.push_str("yyyy"); i += 2; }
                'y' => { result.push_str("yy"); i += 2; }
                'm' => { result.push_str("mm"); i += 2; }
                'd' => { result.push_str("DD"); i += 2; }
                'H' => { result.push_str("hh24"); i += 2; }
                'M' => { result.push_str("mmmm"); i += 2; } // %M = full month name
                'i' => { result.push_str("mi"); i += 2; }
                'S' | 's' => { result.push_str("ss"); i += 2; }
                'f' => { result.push_str("ff"); i += 2; }
                'w' => { result.push_str("dy"); i += 2; } // day of week number
                'a' => { result.push_str("DY"); i += 2; } // abbreviated day name
                'b' => { result.push_str("mon"); i += 2; } // abbreviated month name
                'T' => { result.push_str("hh24:mi:ss"); i += 2; } // time shorthand
                _ => { result.push(chars[i]); result.push(chars[i + 1]); i += 2; }
            }
        } else {
            result.push(chars[i]);
            i += 1;
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::Dialect;

    fn transpile_to_snowflake(sql: &str) -> String {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile_to(sql, DialectType::Snowflake)
            .expect("Transpile failed");
        result[0].clone()
    }

    #[test]
    fn test_ifnull_to_coalesce() {
        let result = transpile_to_snowflake("SELECT IFNULL(a, b)");
        assert!(
            result.contains("COALESCE"),
            "Expected COALESCE, got: {}",
            result
        );
    }

    #[test]
    fn test_basic_select() {
        let result = transpile_to_snowflake("SELECT a, b FROM users WHERE id = 1");
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM users"));
    }

    #[test]
    fn test_group_concat_to_listagg() {
        let result = transpile_to_snowflake("SELECT GROUP_CONCAT(name)");
        assert!(
            result.contains("LISTAGG"),
            "Expected LISTAGG, got: {}",
            result
        );
    }

    #[test]
    fn test_string_agg_to_listagg() {
        let result = transpile_to_snowflake("SELECT STRING_AGG(name)");
        assert!(
            result.contains("LISTAGG"),
            "Expected LISTAGG, got: {}",
            result
        );
    }

    #[test]
    fn test_array_to_array_construct() {
        let result = transpile_to_snowflake("SELECT ARRAY(1, 2, 3)");
        assert!(
            result.contains("ARRAY_CONSTRUCT"),
            "Expected ARRAY_CONSTRUCT, got: {}",
            result
        );
    }

    #[test]
    fn test_double_quote_identifiers() {
        // Snowflake uses double quotes for identifiers
        let dialect = SnowflakeDialect;
        let config = dialect.generator_config();
        assert_eq!(config.identifier_quote, '"');
    }
}
