//! PostgreSQL Dialect
//!
//! PostgreSQL-specific transformations based on sqlglot patterns.
//! Comprehensive batch translation from Python sqlglot postgres.py
//!
//! Key differences from other dialects:
//! - TRY_CAST not supported (falls back to CAST)
//! - RANDOM() instead of RAND()
//! - STRING_AGG instead of GROUP_CONCAT
//! - Bitwise XOR is # operator
//! - BOOL_AND/BOOL_OR for logical aggregates
//! - GEN_RANDOM_UUID() for UUID generation
//! - UNNEST instead of EXPLODE
//! - Type mappings: TINYINT→SMALLINT, FLOAT→REAL, DOUBLE→DOUBLE PRECISION, etc.
//! - RegexpLike uses ~ operator, RegexpILike uses ~* operator
//! - JSONB operators: #>, #>>, ?, ?|, ?&

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    AggFunc, BinaryOp, BooleanLiteral, Case, Cast, CeilFunc, DataType, DateTimeField,
    Expression, ExtractFunc, Function, Interval, IntervalUnit, IntervalUnitSpec,
    Join, JoinKind, Literal, Paren, UnaryFunc, VarArgFunc,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

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

/// PostgreSQL dialect
pub struct PostgresDialect;

impl DialectImpl for PostgresDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::PostgreSQL
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        use crate::tokens::TokenType;
        let mut config = TokenizerConfig::default();
        // PostgreSQL supports $$ string literals (heredoc strings)
        config.quotes.insert("$$".to_string(), "$$".to_string());
        // PostgreSQL uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // Nested comments supported
        config.nested_comments = true;
        // PostgreSQL treats EXEC as a generic command (not TSQL EXEC statement)
        // Note: EXECUTE is kept as-is since it's used in GRANT/REVOKE EXECUTE ON FUNCTION
        config.keywords.insert("EXEC".to_string(), TokenType::Command);
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::PostgreSQL),
            // PostgreSQL uses TIMESTAMPTZ shorthand
            tz_to_with_time_zone: false,
            // PostgreSQL prefers INTERVAL '1 day' syntax
            single_string_interval: true,
            // TABLESAMPLE uses REPEATABLE in PostgreSQL
            tablesample_seed_keyword: "REPEATABLE",
            // PostgreSQL doesn't support NVL2
            nvl2_supported: false,
            // PostgreSQL uses $ for parameters
            parameter_token: "$",
            // PostgreSQL uses % for named placeholders
            named_placeholder_token: "%",
            // PostgreSQL supports SELECT INTO
            supports_select_into: true,
            // PostgreSQL: USING btree(col) without space before parens
            index_using_no_space: true,
            // PostgreSQL supports UNLOGGED tables
            supports_unlogged_tables: true,
            // PostgreSQL doesn't support multi-arg DISTINCT
            multi_arg_distinct: false,
            // PostgreSQL uses ANY(subquery) without space
            quantified_no_paren_space: true,
            // PostgreSQL supports window EXCLUDE clause
            supports_window_exclude: true,
            // PostgreSQL normalizes single-bound window frames to BETWEEN form
            normalize_window_frame_between: true,
            // PostgreSQL COPY doesn't use INTO keyword
            copy_has_into_keyword: false,
            // PostgreSQL ARRAY_SIZE requires dimension argument
            array_size_dim_required: Some(true),
            // PostgreSQL supports BETWEEN flags
            supports_between_flags: true,
            // PostgreSQL doesn't support hints
            join_hints: false,
            table_hints: false,
            query_hints: false,
            // PostgreSQL supports locking reads
            locking_reads_supported: true,
            // PostgreSQL doesn't rename tables with DB
            rename_table_with_db: false,
            // PostgreSQL can implement array any
            can_implement_array_any: true,
            // PostgreSQL ARRAY_CONCAT is not var-len
            array_concat_is_var_len: false,
            // PostgreSQL doesn't support MEDIAN
            supports_median: false,
            // PostgreSQL requires JSON type for extraction
            json_type_required_for_extraction: true,
            // PostgreSQL LIKE property inside schema
            like_property_inside_schema: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // ============================================
            // DATA TYPE MAPPINGS (from TYPE_MAPPING)
            // These are handled specially - transform DataType variants
            // ============================================
            Expression::DataType(dt) => self.transform_data_type(dt),

            // ============================================
            // NULL HANDLING
            // ============================================
            // IFNULL -> COALESCE in PostgreSQL
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE in PostgreSQL
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Coalesce with original_name (e.g., IFNULL parsed as Coalesce) -> clear original_name
            // so it outputs as COALESCE instead of the original function name
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // ============================================
            // CAST OPERATIONS
            // ============================================
            // TryCast -> CAST (PostgreSQL doesn't support TRY_CAST)
            Expression::TryCast(c) => Ok(Expression::Cast(c)),

            // SafeCast -> CAST (PostgreSQL doesn't support safe casts)
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // ============================================
            // RANDOM
            // ============================================
            // RAND -> RANDOM in PostgreSQL
            Expression::Rand(r) => {
                // PostgreSQL's RANDOM() doesn't take a seed argument
                let _ = r.seed; // Ignore seed
                Ok(Expression::Random(crate::expressions::Random))
            }

            // ============================================
            // UUID
            // ============================================
            // Uuid -> GEN_RANDOM_UUID in PostgreSQL
            Expression::Uuid(_) => Ok(Expression::Function(Box::new(Function::new(
                "GEN_RANDOM_UUID".to_string(),
                vec![],
            )))),

            // ============================================
            // ARRAY OPERATIONS
            // ============================================
            // EXPLODE -> UNNEST in PostgreSQL
            Expression::Explode(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // ExplodeOuter -> UNNEST in PostgreSQL
            Expression::ExplodeOuter(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // ArrayConcat -> ARRAY_CAT in PostgreSQL
            Expression::ArrayConcat(f) => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_CAT".to_string(),
                f.expressions,
            )))),

            // ArrayPrepend -> ARRAY_PREPEND in PostgreSQL (note: args swapped from other dialects)
            Expression::ArrayPrepend(f) => Ok(Expression::Function(Box::new(Function::new(
                "ARRAY_PREPEND".to_string(),
                vec![f.expression, f.this], // PostgreSQL: ARRAY_PREPEND(element, array)
            )))),

            // ============================================
            // BITWISE OPERATIONS
            // ============================================
            // BitwiseXor -> # operator in PostgreSQL
            Expression::BitwiseXor(f) => {
                // Use a special marker that generator will recognize
                Ok(Expression::Function(Box::new(Function::new(
                    "__PG_BITWISE_XOR__".to_string(),
                    vec![f.left, f.right],
                ))))
            }

            // BitwiseAndAgg -> BIT_AND
            Expression::BitwiseAndAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_AND".to_string(),
                vec![f.this],
            )))),

            // BitwiseOrAgg -> BIT_OR
            Expression::BitwiseOrAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_OR".to_string(),
                vec![f.this],
            )))),

            // BitwiseXorAgg -> BIT_XOR
            Expression::BitwiseXorAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_XOR".to_string(),
                vec![f.this],
            )))),

            // ============================================
            // BOOLEAN AGGREGATES
            // ============================================
            // LogicalAnd -> BOOL_AND
            Expression::LogicalAnd(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOL_AND".to_string(),
                vec![f.this],
            )))),

            // LogicalOr -> BOOL_OR
            Expression::LogicalOr(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOL_OR".to_string(),
                vec![f.this],
            )))),

            // Xor -> PostgreSQL bool_xor pattern: a <> b for boolean values
            Expression::Xor(f) => {
                if let (Some(a), Some(b)) = (f.this, f.expression) {
                    Ok(Expression::Neq(Box::new(BinaryOp {
                        left: *a,
                        right: *b,
                        left_comments: Vec::new(),
                        operator_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    })))
                } else {
                    Ok(Expression::Boolean(BooleanLiteral { value: false }))
                }
            }

            // ============================================
            // ARRAY OPERATORS
            // ============================================
            // ArrayContainedBy (<@) -> ArrayContainsAll (@>) with swapped operands
            // a <@ b -> b @> a (PostgreSQL prefers @> syntax)
            Expression::ArrayContainedBy(op) => Ok(Expression::ArrayContainsAll(Box::new(BinaryOp {
                left: op.right,
                right: op.left,
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
            }))),

            // ============================================
            // REGEXP OPERATIONS (PostgreSQL uses ~ and ~* operators)
            // ============================================
            // RegexpLike -> keep as-is, generator handles ~ operator output
            Expression::RegexpLike(f) => {
                // Generator will output as: expr ~ pattern
                Ok(Expression::RegexpLike(f))
            }

            // ============================================
            // DATE/TIME FUNCTIONS
            // ============================================
            // DateAdd -> date + INTERVAL in PostgreSQL
            Expression::DateAdd(f) => {
                let interval_expr = Expression::Interval(Box::new(Interval {
                    this: Some(f.interval),
                    unit: Some(IntervalUnitSpec::Simple {
                        unit: f.unit,
                        use_plural: false,
                    }),
                }));
                Ok(Expression::Add(Box::new(BinaryOp {
                    left: f.this,
                    right: interval_expr,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // DateSub -> date - INTERVAL in PostgreSQL
            Expression::DateSub(f) => {
                let interval_expr = Expression::Interval(Box::new(Interval {
                    this: Some(f.interval),
                    unit: Some(IntervalUnitSpec::Simple {
                        unit: f.unit,
                        use_plural: false,
                    }),
                }));
                Ok(Expression::Sub(Box::new(BinaryOp {
                    left: f.this,
                    right: interval_expr,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // DateDiff -> Complex PostgreSQL pattern using AGE/EXTRACT
            Expression::DateDiff(f) => {
                // For PostgreSQL, DATEDIFF is converted to EXTRACT(epoch FROM ...) pattern
                // matching the 3-arg string-based DATEDIFF handler below
                let unit = f.unit.unwrap_or(IntervalUnit::Day);

                // Helper: CAST(expr AS TIMESTAMP)
                let cast_ts = |e: Expression| -> Expression {
                    Expression::Cast(Box::new(Cast {
                        this: e,
                        to: DataType::Timestamp { precision: None, timezone: false },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))
                };

                // Helper: CAST(expr AS BIGINT)
                let cast_bigint = |e: Expression| -> Expression {
                    Expression::Cast(Box::new(Cast {
                        this: e,
                        to: DataType::BigInt { length: None },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))
                };

                // Clone end/start for reuse
                let end_expr = f.this;
                let start = f.expression;

                // Helper: end_ts - start_ts
                let ts_diff = || -> Expression {
                    Expression::Sub(Box::new(BinaryOp::new(
                        cast_ts(end_expr.clone()),
                        cast_ts(start.clone()),
                    )))
                };

                // Helper: AGE(end_ts, start_ts)
                let age_call = || -> Expression {
                    Expression::Function(Box::new(Function::new(
                        "AGE".to_string(),
                        vec![cast_ts(end_expr.clone()), cast_ts(start.clone())],
                    )))
                };

                // Helper: EXTRACT(field FROM expr)
                let extract = |field: DateTimeField, from: Expression| -> Expression {
                    Expression::Extract(Box::new(ExtractFunc {
                        this: from,
                        field,
                    }))
                };

                // Helper: number literal
                let num = |n: i64| -> Expression {
                    Expression::Literal(Literal::Number(n.to_string()))
                };

                let epoch_field = DateTimeField::Custom("epoch".to_string());

                let result = match unit {
                    IntervalUnit::Microsecond => {
                        let epoch = extract(epoch_field, ts_diff());
                        cast_bigint(Expression::Mul(Box::new(BinaryOp::new(epoch, num(1000000)))))
                    }
                    IntervalUnit::Millisecond => {
                        let epoch = extract(epoch_field, ts_diff());
                        cast_bigint(Expression::Mul(Box::new(BinaryOp::new(epoch, num(1000)))))
                    }
                    IntervalUnit::Second => {
                        let epoch = extract(epoch_field, ts_diff());
                        cast_bigint(epoch)
                    }
                    IntervalUnit::Minute => {
                        let epoch = extract(epoch_field, ts_diff());
                        cast_bigint(Expression::Div(Box::new(BinaryOp::new(epoch, num(60)))))
                    }
                    IntervalUnit::Hour => {
                        let epoch = extract(epoch_field, ts_diff());
                        cast_bigint(Expression::Div(Box::new(BinaryOp::new(epoch, num(3600)))))
                    }
                    IntervalUnit::Day => {
                        let epoch = extract(epoch_field, ts_diff());
                        cast_bigint(Expression::Div(Box::new(BinaryOp::new(epoch, num(86400)))))
                    }
                    IntervalUnit::Week => {
                        let diff_parens = Expression::Paren(Box::new(Paren {
                            this: ts_diff(),
                            trailing_comments: Vec::new(),
                        }));
                        let days = extract(DateTimeField::Custom("days".to_string()), diff_parens);
                        cast_bigint(Expression::Div(Box::new(BinaryOp::new(days, num(7)))))
                    }
                    IntervalUnit::Month => {
                        let year_part = extract(DateTimeField::Custom("year".to_string()), age_call());
                        let month_part = extract(DateTimeField::Custom("month".to_string()), age_call());
                        let year_months = Expression::Mul(Box::new(BinaryOp::new(year_part, num(12))));
                        cast_bigint(Expression::Add(Box::new(BinaryOp::new(year_months, month_part))))
                    }
                    IntervalUnit::Quarter => {
                        let year_part = extract(DateTimeField::Custom("year".to_string()), age_call());
                        let month_part = extract(DateTimeField::Custom("month".to_string()), age_call());
                        let year_quarters = Expression::Mul(Box::new(BinaryOp::new(year_part, num(4))));
                        let month_quarters = Expression::Div(Box::new(BinaryOp::new(month_part, num(3))));
                        cast_bigint(Expression::Add(Box::new(BinaryOp::new(year_quarters, month_quarters))))
                    }
                    IntervalUnit::Year => {
                        cast_bigint(extract(DateTimeField::Custom("year".to_string()), age_call()))
                    }
                };
                Ok(result)
            }

            // UnixToTime -> TO_TIMESTAMP
            Expression::UnixToTime(f) => Ok(Expression::Function(Box::new(Function::new(
                "TO_TIMESTAMP".to_string(),
                vec![*f.this],
            )))),

            // TimeToUnix -> DATE_PART('epoch', ...) in PostgreSQL
            Expression::TimeToUnix(f) => Ok(Expression::Function(Box::new(Function::new(
                "DATE_PART".to_string(),
                vec![Expression::string("epoch"), f.this],
            )))),

            // StrToTime -> TO_TIMESTAMP in PostgreSQL
            Expression::ToTimestamp(f) => {
                let mut args = vec![f.this];
                if let Some(fmt) = f.format {
                    args.push(fmt);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_TIMESTAMP".to_string(),
                    args,
                ))))
            }

            // StrToDate -> TO_DATE in PostgreSQL
            Expression::ToDate(f) => {
                let mut args = vec![f.this];
                if let Some(fmt) = f.format {
                    args.push(fmt);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_DATE".to_string(),
                    args,
                ))))
            }

            // TimestampTrunc -> DATE_TRUNC
            Expression::TimestampTrunc(f) => {
                // Convert DateTimeField to string expression for DATE_TRUNC
                let unit_str = format!("{:?}", f.unit).to_lowercase();
                let args = vec![Expression::string(&unit_str), f.this];
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_TRUNC".to_string(),
                    args,
                ))))
            }

            // TimeFromParts -> MAKE_TIME
            Expression::TimeFromParts(f) => {
                let mut args = Vec::new();
                if let Some(h) = f.hour {
                    args.push(*h);
                }
                if let Some(m) = f.min {
                    args.push(*m);
                }
                if let Some(s) = f.sec {
                    args.push(*s);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "MAKE_TIME".to_string(),
                    args,
                ))))
            }

            // TimestampFromParts -> MAKE_TIMESTAMP
            Expression::MakeTimestamp(f) => {
                // MakeTimestampFunc has direct Expression fields, not Options
                let args = vec![f.year, f.month, f.day, f.hour, f.minute, f.second];
                Ok(Expression::Function(Box::new(Function::new(
                    "MAKE_TIMESTAMP".to_string(),
                    args,
                ))))
            }

            // ============================================
            // STRING FUNCTIONS
            // ============================================
            // StringAgg is native to PostgreSQL - keep as-is
            Expression::StringAgg(f) => Ok(Expression::StringAgg(f)),

            // GroupConcat -> STRING_AGG in PostgreSQL
            Expression::GroupConcat(f) => {
                let mut args = vec![f.this.clone()];
                if let Some(sep) = f.separator.clone() {
                    args.push(sep);
                } else {
                    args.push(Expression::string(","));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "STRING_AGG".to_string(),
                    args,
                ))))
            }

            // StrPosition -> POSITION function
            Expression::Position(f) => {
                // PostgreSQL: POSITION(substring IN string)
                // Keep as Position, generator handles it
                Ok(Expression::Position(f))
            }

            // ============================================
            // AGGREGATE FUNCTIONS
            // ============================================
            // CountIf -> SUM(CASE WHEN condition THEN 1 ELSE 0 END) in PostgreSQL
            Expression::CountIf(f) => {
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.this.clone(), Expression::number(1))],
                    else_: Some(Expression::number(0)),
                }));
                Ok(Expression::Sum(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // AnyValue -> keep as ANY_VALUE for PostgreSQL (supported since PG 16)
            Expression::AnyValue(f) => Ok(Expression::AnyValue(f)),

            // Variance -> VAR_SAMP in PostgreSQL
            Expression::Variance(f) => Ok(Expression::Function(Box::new(Function::new(
                "VAR_SAMP".to_string(),
                vec![f.this],
            )))),

            // VarPop -> VAR_POP in PostgreSQL
            Expression::VarPop(f) => Ok(Expression::Function(Box::new(Function::new(
                "VAR_POP".to_string(),
                vec![f.this],
            )))),

            // ============================================
            // JSON FUNCTIONS
            // ============================================
            // JSONExtract -> use arrow syntax (->) in PostgreSQL for simple literal paths
            // Complex paths (like column references) should use JSON_EXTRACT_PATH function
            Expression::JsonExtract(mut f) => {
                // Only use arrow syntax for simple literal paths (string or non-negative number)
                // Complex expressions like column references should use function form
                f.arrow_syntax = Self::is_simple_json_path(&f.path);
                Ok(Expression::JsonExtract(f))
            }

            // JSONExtractScalar -> use arrow syntax (->>) in PostgreSQL for simple paths
            // Complex paths (like negative indices) should use JSON_EXTRACT_PATH_TEXT function
            // #>> (hash_arrow_syntax) stays as #>>
            Expression::JsonExtractScalar(mut f) => {
                if !f.hash_arrow_syntax {
                    // Only use arrow syntax for simple literal paths (string or non-negative number)
                    // Complex expressions like Neg(-1) should use function form
                    f.arrow_syntax = Self::is_simple_json_path(&f.path);
                }
                Ok(Expression::JsonExtractScalar(f))
            }

            // ParseJson: handled by generator (outputs CAST(x AS JSON) for PostgreSQL)

            // JSONObjectAgg -> JSON_OBJECT_AGG
            Expression::JsonObjectAgg(f) => {
                // JsonObjectAggFunc has key and value as Expression, not Option
                let args = vec![f.key, f.value];
                Ok(Expression::Function(Box::new(Function::new(
                    "JSON_OBJECT_AGG".to_string(),
                    args,
                ))))
            }

            // JSONArrayAgg -> JSON_AGG
            Expression::JsonArrayAgg(f) => Ok(Expression::Function(Box::new(Function::new(
                "JSON_AGG".to_string(),
                vec![f.this],
            )))),

            // JSONPathRoot -> empty string ($ is implicit in PostgreSQL)
            Expression::JSONPathRoot(_) => {
                Ok(Expression::Literal(Literal::String(String::new())))
            }

            // ============================================
            // MISC FUNCTIONS
            // ============================================
            // IntDiv -> DIV in PostgreSQL
            Expression::IntDiv(f) => Ok(Expression::Function(Box::new(Function::new(
                "DIV".to_string(),
                vec![f.this, f.expression],
            )))),

            // Unicode -> ASCII in PostgreSQL
            Expression::Unicode(f) => Ok(Expression::Function(Box::new(Function::new(
                "ASCII".to_string(),
                vec![f.this],
            )))),

            // LastDay -> Complex expression (PostgreSQL doesn't have LAST_DAY)
            Expression::LastDay(f) => {
                // (DATE_TRUNC('month', date) + INTERVAL '1 month' - INTERVAL '1 day')::DATE
                let truncated = Expression::Function(Box::new(Function::new(
                    "DATE_TRUNC".to_string(),
                    vec![Expression::string("month"), f.this.clone()],
                )));
                let plus_month = Expression::Add(Box::new(BinaryOp {
                    left: truncated,
                    right: Expression::Interval(Box::new(Interval {
                        this: Some(Expression::string("1")),
                        unit: Some(IntervalUnitSpec::Simple {
                            unit: IntervalUnit::Month,
                            use_plural: false,
                        }),
                    })),
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));
                let minus_day = Expression::Sub(Box::new(BinaryOp {
                    left: plus_month,
                    right: Expression::Interval(Box::new(Interval {
                        this: Some(Expression::string("1")),
                        unit: Some(IntervalUnitSpec::Simple {
                            unit: IntervalUnit::Day,
                            use_plural: false,
                        }),
                    })),
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));
                Ok(Expression::Cast(Box::new(Cast {
                    this: minus_day,
                    to: DataType::Date,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: true, // Use PostgreSQL :: syntax
                    format: None,
                    default: None,
                })))
            }

            // GenerateSeries is native to PostgreSQL
            Expression::GenerateSeries(f) => Ok(Expression::GenerateSeries(f)),

            // ExplodingGenerateSeries -> GENERATE_SERIES
            Expression::ExplodingGenerateSeries(f) => {
                let mut args = vec![f.start, f.stop];
                if let Some(step) = f.step {
                    args.push(step); // step is Expression, not Box<Expression>
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "GENERATE_SERIES".to_string(),
                    args,
                ))))
            }

            // ============================================
            // SESSION/TIME FUNCTIONS (no parentheses in PostgreSQL)
            // ============================================
            // CurrentTimestamp -> CURRENT_TIMESTAMP (no parens)
            Expression::CurrentTimestamp(_) => Ok(Expression::Function(Box::new(Function {
                name: "CURRENT_TIMESTAMP".to_string(),
                args: vec![],
                distinct: false,
                trailing_comments: vec![],
                use_bracket_syntax: false,
                no_parens: true,
                quoted: false,
            }))),

            // CurrentUser -> CURRENT_USER (no parens)
            Expression::CurrentUser(_) => Ok(Expression::Function(Box::new(Function::new(
                "CURRENT_USER".to_string(),
                vec![],
            )))),

            // CurrentDate -> CURRENT_DATE (no parens)
            Expression::CurrentDate(_) => Ok(Expression::Function(Box::new(Function {
                name: "CURRENT_DATE".to_string(),
                args: vec![],
                distinct: false,
                trailing_comments: vec![],
                use_bracket_syntax: false,
                no_parens: true,
                quoted: false,
            }))),

            // ============================================
            // JOIN TRANSFORMATIONS
            // ============================================
            // CROSS APPLY -> CROSS JOIN LATERAL in PostgreSQL
            Expression::Join(join) if join.kind == JoinKind::CrossApply => {
                Ok(Expression::Join(Box::new(Join {
                    this: join.this,
                    on: None,
                    using: join.using,
                    kind: JoinKind::Lateral,
                    use_inner_keyword: false,
                    use_outer_keyword: false,
                    deferred_condition: false,
                    join_hint: None,
                    match_condition: None,
                    pivots: join.pivots,
                })))
            }

            // OUTER APPLY -> LEFT JOIN LATERAL ... ON TRUE in PostgreSQL
            Expression::Join(join) if join.kind == JoinKind::OuterApply => {
                Ok(Expression::Join(Box::new(Join {
                    this: join.this,
                    on: Some(Expression::Boolean(BooleanLiteral { value: true })),
                    using: join.using,
                    kind: JoinKind::LeftLateral,
                    use_inner_keyword: false,
                    use_outer_keyword: false,
                    deferred_condition: false,
                    join_hint: None,
                    match_condition: None,
                    pivots: join.pivots,
                })))
            }

            // ============================================
            // GENERIC FUNCTION TRANSFORMATIONS
            // ============================================
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

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

            // In expression - wrap the this part if it's JSON arrow
            Expression::In(mut i) => {
                i.this = wrap_if_json_arrow(i.this);
                Ok(Expression::In(i))
            }

            // Not expression - wrap the this part if it's JSON arrow
            Expression::Not(mut n) => {
                n.this = wrap_if_json_arrow(n.this);
                Ok(Expression::Not(n))
            }

            // MERGE: qualifier stripping is handled by the generator (dialect-aware)
            // PostgreSQL generator strips qualifiers, Snowflake generator keeps them
            Expression::Merge(m) => Ok(Expression::Merge(m)),

            // JSONExtract with variant_extract (Databricks colon syntax) -> JSON_EXTRACT_PATH
            Expression::JSONExtract(je) if je.variant_extract.is_some() => {
                // Convert path from bracketed format to simple key
                // e.g., '["fr''uit"]' -> 'fr''uit'
                let path = match *je.expression {
                    Expression::Literal(Literal::String(s)) => {
                        // Strip bracketed JSON path format: ["key"] -> key
                        let cleaned = if s.starts_with("[\"") && s.ends_with("\"]") {
                            s[2..s.len() - 2].to_string()
                        } else {
                            s
                        };
                        Expression::Literal(Literal::String(cleaned))
                    }
                    other => other,
                };
                Ok(Expression::Function(Box::new(Function::new(
                    "JSON_EXTRACT_PATH".to_string(),
                    vec![*je.this, path],
                ))))
            }

            // TRIM(str, chars) -> TRIM(chars FROM str) for PostgreSQL SQL standard syntax
            Expression::Trim(t) if !t.sql_standard_syntax && t.characters.is_some() => {
                Ok(Expression::Trim(Box::new(crate::expressions::TrimFunc {
                    this: t.this,
                    characters: t.characters,
                    position: t.position,
                    sql_standard_syntax: true,
                    position_explicit: t.position_explicit,
                })))
            }

            // b'a' -> CAST(e'a' AS BYTEA) for PostgreSQL
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

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl PostgresDialect {
    /// Check if a JSON path expression is "simple" (string literal or non-negative integer)
    /// Simple paths can use arrow syntax (->>) in PostgreSQL
    /// Complex paths (like negative indices) should use JSON_EXTRACT_PATH_TEXT function
    fn is_simple_json_path(path: &Expression) -> bool {
        match path {
            // String literals are always simple
            Expression::Literal(Literal::String(_)) => true,
            // Non-negative integer literals are simple
            Expression::Literal(Literal::Number(n)) => {
                // Check if it's non-negative
                !n.starts_with('-')
            }
            // JSONPath expressions are simple (they're already parsed paths)
            Expression::JSONPath(_) => true,
            // Everything else (Neg, function calls, etc.) is complex
            _ => false,
        }
    }

    /// Transform data types according to PostgreSQL TYPE_MAPPING
    fn transform_data_type(&self, dt: DataType) -> Result<Expression> {
        let transformed = match dt {
            // TINYINT -> SMALLINT
            DataType::TinyInt { .. } => DataType::SmallInt { length: None },

            // FLOAT -> DOUBLE PRECISION (Python sqlglot tokenizes FLOAT as DOUBLE)
            DataType::Float { .. } => DataType::Custom {
                name: "DOUBLE PRECISION".to_string(),
            },

            // DOUBLE -> DOUBLE PRECISION
            DataType::Double { .. } => DataType::Custom {
                name: "DOUBLE PRECISION".to_string(),
            },

            // BINARY -> BYTEA
            DataType::Binary { .. } => DataType::Custom {
                name: "BYTEA".to_string(),
            },

            // VARBINARY -> BYTEA
            DataType::VarBinary { .. } => DataType::Custom {
                name: "BYTEA".to_string(),
            },

            // BLOB -> BYTEA
            DataType::Blob => DataType::Custom {
                name: "BYTEA".to_string(),
            },

            // Custom type normalizations
            DataType::Custom { ref name } => {
                let upper = name.to_uppercase();
                match upper.as_str() {
                    // INT8 -> BIGINT (PostgreSQL alias)
                    "INT8" => DataType::BigInt { length: None },
                    // FLOAT8 -> DOUBLE PRECISION (PostgreSQL alias)
                    "FLOAT8" => DataType::Custom {
                        name: "DOUBLE PRECISION".to_string(),
                    },
                    // FLOAT4 -> REAL (PostgreSQL alias)
                    "FLOAT4" => DataType::Custom {
                        name: "REAL".to_string(),
                    },
                    // INT4 -> INTEGER (PostgreSQL alias)
                    "INT4" => DataType::Int { length: None, integer_spelling: false },
                    // INT2 -> SMALLINT (PostgreSQL alias)
                    "INT2" => DataType::SmallInt { length: None },
                    _ => dt,
                }
            }

            // Keep all other types as-is
            other => other,
        };
        Ok(Expression::DataType(transformed))
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

            // ISNULL (SQL Server) -> COALESCE
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: f.args,
            }))),

            // GROUP_CONCAT -> STRING_AGG in PostgreSQL
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("STRING_AGG".to_string(), f.args),
            ))),

            // SUBSTR -> SUBSTRING (standard SQL)
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // RAND -> RANDOM in PostgreSQL
            "RAND" => Ok(Expression::Random(crate::expressions::Random)),

            // CEILING -> CEIL (both work in PostgreSQL, but CEIL is preferred)
            "CEILING" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // LEN -> LENGTH in PostgreSQL
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc {
                this: f.args.into_iter().next().unwrap(),
                original_name: None,
            }))),

            // CHAR_LENGTH -> LENGTH in PostgreSQL
            "CHAR_LENGTH" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc {
                this: f.args.into_iter().next().unwrap(),
                original_name: None,
            }))),

            // CHARACTER_LENGTH -> LENGTH in PostgreSQL
            "CHARACTER_LENGTH" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc {
                this: f.args.into_iter().next().unwrap(),
                original_name: None,
            }))),

            // CHARINDEX -> POSITION in PostgreSQL
            // CHARINDEX(substring, string) -> POSITION(substring IN string)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Position(Box::new(
                    crate::expressions::PositionFunc {
                        substring,
                        string,
                        start: args.pop(),
                    },
                )))
            }

            // GETDATE -> CURRENT_TIMESTAMP in PostgreSQL
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // SYSDATETIME -> CURRENT_TIMESTAMP in PostgreSQL
            "SYSDATETIME" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // NOW -> CURRENT_TIMESTAMP in PostgreSQL (NOW() is also valid)
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // NEWID -> GEN_RANDOM_UUID in PostgreSQL
            "NEWID" => Ok(Expression::Function(Box::new(Function::new(
                "GEN_RANDOM_UUID".to_string(),
                vec![],
            )))),

            // UUID -> GEN_RANDOM_UUID in PostgreSQL
            "UUID" => Ok(Expression::Function(Box::new(Function::new(
                "GEN_RANDOM_UUID".to_string(),
                vec![],
            )))),

            // UNNEST is native to PostgreSQL
            "UNNEST" => Ok(Expression::Function(Box::new(f))),

            // GENERATE_SERIES is native to PostgreSQL
            "GENERATE_SERIES" => Ok(Expression::Function(Box::new(f))),

            // SHA256 -> SHA256 in PostgreSQL (via pgcrypto extension)
            "SHA256" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "SHA256".to_string(),
                f.args,
            )))),

            // SHA2 -> SHA256/SHA512 based on length argument
            "SHA2" if f.args.len() == 2 => {
                // SHA2(data, length) -> SHA256/SHA384/SHA512
                let args = f.args;
                let data = args[0].clone();
                // Default to SHA256 - would need runtime inspection for exact mapping
                Ok(Expression::Function(Box::new(Function::new(
                    "SHA256".to_string(),
                    vec![data],
                ))))
            }

            // LEVENSHTEIN is native to PostgreSQL (fuzzystrmatch extension)
            "LEVENSHTEIN" => Ok(Expression::Function(Box::new(f))),

            // EDITDISTANCE -> LEVENSHTEIN_LESS_EQUAL (with max distance) or LEVENSHTEIN
            "EDITDISTANCE" if f.args.len() == 3 => Ok(Expression::Function(Box::new(Function::new(
                "LEVENSHTEIN_LESS_EQUAL".to_string(),
                f.args,
            )))),
            "EDITDISTANCE" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "LEVENSHTEIN".to_string(),
                f.args,
            )))),

            // TRIM(value, chars) -> TRIM(chars FROM value) for Postgres
            "TRIM" if f.args.len() == 2 => {
                let value = f.args[0].clone();
                let chars = f.args[1].clone();
                Ok(Expression::Trim(Box::new(crate::expressions::TrimFunc {
                    this: value,
                    characters: Some(chars),
                    position: crate::expressions::TrimPosition::Both,
                    sql_standard_syntax: true,
                    position_explicit: false,
                })))
            }

            // DATEDIFF(unit, start, end) -> PostgreSQL EXTRACT/AGE patterns
            "DATEDIFF" if f.args.len() >= 2 => {
                let mut args = f.args;
                if args.len() == 2 {
                    // 2-arg form: DATEDIFF(start, end) -> AGE(start, end)
                    let first = args.remove(0);
                    let second = args.remove(0);
                    Ok(Expression::Function(Box::new(Function::new(
                        "AGE".to_string(),
                        vec![first, second],
                    ))))
                } else {
                    // 3-arg form: DATEDIFF(unit, start, end)
                    let unit_expr = args.remove(0);
                    let start = args.remove(0);
                    let end_expr = args.remove(0);

                    // Extract unit name from identifier or column
                    let unit_name = match &unit_expr {
                        Expression::Identifier(id) => id.name.to_uppercase(),
                        Expression::Column(col) if col.table.is_none() => col.name.name.to_uppercase(),
                        _ => "DAY".to_string(),
                    };

                    // Helper: CAST(expr AS TIMESTAMP)
                    let cast_ts = |e: Expression| -> Expression {
                        Expression::Cast(Box::new(Cast {
                            this: e,
                            to: DataType::Timestamp { precision: None, timezone: false },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        }))
                    };

                    // Helper: CAST(expr AS BIGINT)
                    let cast_bigint = |e: Expression| -> Expression {
                        Expression::Cast(Box::new(Cast {
                            this: e,
                            to: DataType::BigInt { length: None },
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                        }))
                    };

                    let end_ts = cast_ts(end_expr.clone());
                    let start_ts = cast_ts(start.clone());

                    // Helper: end_ts - start_ts
                    let ts_diff = || -> Expression {
                        Expression::Sub(Box::new(BinaryOp::new(
                            cast_ts(end_expr.clone()),
                            cast_ts(start.clone()),
                        )))
                    };

                    // Helper: AGE(end_ts, start_ts)
                    let age_call = || -> Expression {
                        Expression::Function(Box::new(Function::new(
                            "AGE".to_string(),
                            vec![cast_ts(end_expr.clone()), cast_ts(start.clone())],
                        )))
                    };

                    // Helper: EXTRACT(field FROM expr)
                    let extract = |field: DateTimeField, from: Expression| -> Expression {
                        Expression::Extract(Box::new(ExtractFunc {
                            this: from,
                            field,
                        }))
                    };

                    // Helper: number literal
                    let num = |n: i64| -> Expression {
                        Expression::Literal(Literal::Number(n.to_string()))
                    };

                    // Use Custom DateTimeField for lowercase output (PostgreSQL convention)
                    let epoch_field = DateTimeField::Custom("epoch".to_string());

                    let result = match unit_name.as_str() {
                        "MICROSECOND" => {
                            // CAST(EXTRACT(epoch FROM end_ts - start_ts) * 1000000 AS BIGINT)
                            let epoch = extract(epoch_field, ts_diff());
                            cast_bigint(Expression::Mul(Box::new(BinaryOp::new(epoch, num(1000000)))))
                        }
                        "MILLISECOND" => {
                            let epoch = extract(epoch_field, ts_diff());
                            cast_bigint(Expression::Mul(Box::new(BinaryOp::new(epoch, num(1000)))))
                        }
                        "SECOND" => {
                            let epoch = extract(epoch_field, ts_diff());
                            cast_bigint(epoch)
                        }
                        "MINUTE" => {
                            let epoch = extract(epoch_field, ts_diff());
                            cast_bigint(Expression::Div(Box::new(BinaryOp::new(epoch, num(60)))))
                        }
                        "HOUR" => {
                            let epoch = extract(epoch_field, ts_diff());
                            cast_bigint(Expression::Div(Box::new(BinaryOp::new(epoch, num(3600)))))
                        }
                        "DAY" => {
                            let epoch = extract(epoch_field, ts_diff());
                            cast_bigint(Expression::Div(Box::new(BinaryOp::new(epoch, num(86400)))))
                        }
                        "WEEK" => {
                            // CAST(EXTRACT(days FROM (end_ts - start_ts)) / 7 AS BIGINT)
                            let diff_parens = Expression::Paren(Box::new(Paren {
                                this: ts_diff(),
                                trailing_comments: Vec::new(),
                            }));
                            let days = extract(DateTimeField::Custom("days".to_string()), diff_parens);
                            cast_bigint(Expression::Div(Box::new(BinaryOp::new(days, num(7)))))
                        }
                        "MONTH" => {
                            // CAST(EXTRACT(year FROM AGE(...)) * 12 + EXTRACT(month FROM AGE(...)) AS BIGINT)
                            let year_part = extract(DateTimeField::Custom("year".to_string()), age_call());
                            let month_part = extract(DateTimeField::Custom("month".to_string()), age_call());
                            let year_months = Expression::Mul(Box::new(BinaryOp::new(year_part, num(12))));
                            cast_bigint(Expression::Add(Box::new(BinaryOp::new(year_months, month_part))))
                        }
                        "QUARTER" => {
                            // CAST(EXTRACT(year FROM AGE(...)) * 4 + EXTRACT(month FROM AGE(...)) / 3 AS BIGINT)
                            let year_part = extract(DateTimeField::Custom("year".to_string()), age_call());
                            let month_part = extract(DateTimeField::Custom("month".to_string()), age_call());
                            let year_quarters = Expression::Mul(Box::new(BinaryOp::new(year_part, num(4))));
                            let month_quarters = Expression::Div(Box::new(BinaryOp::new(month_part, num(3))));
                            cast_bigint(Expression::Add(Box::new(BinaryOp::new(year_quarters, month_quarters))))
                        }
                        "YEAR" => {
                            // CAST(EXTRACT(year FROM AGE(...)) AS BIGINT)
                            cast_bigint(extract(DateTimeField::Custom("year".to_string()), age_call()))
                        }
                        _ => {
                            // Fallback: simple AGE
                            Expression::Function(Box::new(Function::new(
                                "AGE".to_string(),
                                vec![end_ts, start_ts],
                            )))
                        }
                    };
                    Ok(result)
                }
            }

            // TIMESTAMPDIFF -> AGE or EXTRACT pattern
            "TIMESTAMPDIFF" if f.args.len() >= 3 => {
                let mut args = f.args;
                let _unit = args.remove(0); // Unit (ignored, AGE returns full interval)
                let start = args.remove(0);
                let end = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "AGE".to_string(),
                    vec![end, start],
                ))))
            }

            // FROM_UNIXTIME -> TO_TIMESTAMP
            "FROM_UNIXTIME" => Ok(Expression::Function(Box::new(Function::new(
                "TO_TIMESTAMP".to_string(),
                f.args,
            )))),

            // UNIX_TIMESTAMP -> EXTRACT(EPOCH FROM ...)
            "UNIX_TIMESTAMP" if f.args.len() == 1 => {
                let arg = f.args.into_iter().next().unwrap();
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_PART".to_string(),
                    vec![Expression::string("epoch"), arg],
                ))))
            }

            // UNIX_TIMESTAMP() with no args -> EXTRACT(EPOCH FROM CURRENT_TIMESTAMP)
            "UNIX_TIMESTAMP" if f.args.is_empty() => {
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_PART".to_string(),
                    vec![
                        Expression::string("epoch"),
                        Expression::CurrentTimestamp(crate::expressions::CurrentTimestamp {
                            precision: None,
                            sysdate: false,
                        }),
                    ],
                ))))
            }

            // DATEADD -> date + interval pattern
            "DATEADD" if f.args.len() == 3 => {
                // DATEADD(unit, count, date) -> date + interval 'count unit'
                // This is a simplified version - full impl would construct proper interval
                let mut args = f.args;
                let _unit = args.remove(0);
                let count = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Add(Box::new(BinaryOp {
                    left: date,
                    right: count,
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                })))
            }

            // INSTR -> POSITION (simplified)
            "INSTR" if f.args.len() >= 2 => {
                let mut args = f.args;
                let string = args.remove(0);
                let substring = args.remove(0);
                Ok(Expression::Position(Box::new(
                    crate::expressions::PositionFunc {
                        substring,
                        string,
                        start: args.pop(),
                    },
                )))
            }

            // CONCAT_WS is native to PostgreSQL
            "CONCAT_WS" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_REPLACE: pass through without adding 'g' flag
            // The 'g' flag handling is managed by cross_dialect_normalize based on source dialect's default behavior
            "REGEXP_REPLACE" if f.args.len() == 3 || f.args.len() == 4 => {
                Ok(Expression::Function(Box::new(f)))
            }
            // 6 args from Snowflake: (subject, pattern, replacement, position, occurrence, params)
            // If occurrence is 0 (global), append 'g' to flags
            "REGEXP_REPLACE" if f.args.len() == 6 => {
                let is_global = match &f.args[4] {
                    Expression::Literal(crate::expressions::Literal::Number(n)) => n == "0",
                    _ => false,
                };
                if is_global {
                    let subject = f.args[0].clone();
                    let pattern = f.args[1].clone();
                    let replacement = f.args[2].clone();
                    let position = f.args[3].clone();
                    let occurrence = f.args[4].clone();
                    let params = &f.args[5];
                    let mut flags = if let Expression::Literal(crate::expressions::Literal::String(s)) = params {
                        s.clone()
                    } else {
                        String::new()
                    };
                    if !flags.contains('g') {
                        flags.push('g');
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_REPLACE".to_string(),
                        vec![subject, pattern, replacement, position, occurrence, Expression::Literal(crate::expressions::Literal::String(flags))],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(f)))
                }
            }
            // Default: pass through
            "REGEXP_REPLACE" => Ok(Expression::Function(Box::new(f))),

            // Pass through everything else
            _ => Ok(Expression::Function(Box::new(f))),
        }
    }

    fn transform_aggregate_function(
        &self,
        f: Box<crate::expressions::AggregateFunction>,
    ) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // COUNT_IF -> SUM(CASE WHEN...)
            "COUNT_IF" if !f.args.is_empty() => {
                let condition = f.args.into_iter().next().unwrap();
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(condition, Expression::number(1))],
                    else_: Some(Expression::number(0)),
                }));
                Ok(Expression::Sum(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // GROUP_CONCAT -> STRING_AGG
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("STRING_AGG".to_string(), f.args),
            ))),

            // STDEV -> STDDEV in PostgreSQL
            "STDEV" if !f.args.is_empty() => Ok(Expression::Stddev(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                this: f.args.into_iter().next().unwrap(),
                distinct: f.distinct,
                filter: f.filter,
                order_by: Vec::new(),
                name: None,
                limit: None,
            }))),

            // STDEVP -> STDDEV_POP in PostgreSQL
            "STDEVP" if !f.args.is_empty() => Ok(Expression::StddevPop(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                this: f.args.into_iter().next().unwrap(),
                distinct: f.distinct,
                filter: f.filter,
                order_by: Vec::new(),
                name: None,
                limit: None,
            }))),

            // VAR -> VAR_SAMP in PostgreSQL
            "VAR" if !f.args.is_empty() => Ok(Expression::VarSamp(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                this: f.args.into_iter().next().unwrap(),
                distinct: f.distinct,
                filter: f.filter,
                order_by: Vec::new(),
                name: None,
                limit: None,
            }))),

            // VARP -> VAR_POP in PostgreSQL
            "VARP" if !f.args.is_empty() => Ok(Expression::VarPop(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                this: f.args.into_iter().next().unwrap(),
                distinct: f.distinct,
                filter: f.filter,
                order_by: Vec::new(),
                name: None,
                limit: None,
            }))),

            // BIT_AND is native to PostgreSQL
            "BIT_AND" => Ok(Expression::AggregateFunction(f)),

            // BIT_OR is native to PostgreSQL
            "BIT_OR" => Ok(Expression::AggregateFunction(f)),

            // BIT_XOR is native to PostgreSQL
            "BIT_XOR" => Ok(Expression::AggregateFunction(f)),

            // BOOL_AND is native to PostgreSQL
            "BOOL_AND" => Ok(Expression::AggregateFunction(f)),

            // BOOL_OR is native to PostgreSQL
            "BOOL_OR" => Ok(Expression::AggregateFunction(f)),

            // VARIANCE -> VAR_SAMP in PostgreSQL
            "VARIANCE" if !f.args.is_empty() => Ok(Expression::VarSamp(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                this: f.args.into_iter().next().unwrap(),
                distinct: f.distinct,
                filter: f.filter,
                order_by: Vec::new(),
                name: None,
                limit: None,
            }))),

            // LOGICAL_OR -> BOOL_OR in PostgreSQL
            "LOGICAL_OR" if !f.args.is_empty() => {
                let mut new_agg = f.clone();
                new_agg.name = "BOOL_OR".to_string();
                Ok(Expression::AggregateFunction(new_agg))
            }

            // LOGICAL_AND -> BOOL_AND in PostgreSQL
            "LOGICAL_AND" if !f.args.is_empty() => {
                let mut new_agg = f.clone();
                new_agg.name = "BOOL_AND".to_string();
                Ok(Expression::AggregateFunction(new_agg))
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::Dialect;

    fn transpile_to_postgres(sql: &str) -> String {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile_to(sql, DialectType::PostgreSQL)
            .expect("Transpile failed");
        result[0].clone()
    }

    #[test]
    fn test_ifnull_to_coalesce() {
        let result = transpile_to_postgres("SELECT IFNULL(a, b)");
        assert!(
            result.contains("COALESCE"),
            "Expected COALESCE, got: {}",
            result
        );
    }

    #[test]
    fn test_nvl_to_coalesce() {
        let result = transpile_to_postgres("SELECT NVL(a, b)");
        assert!(
            result.contains("COALESCE"),
            "Expected COALESCE, got: {}",
            result
        );
    }

    #[test]
    fn test_rand_to_random() {
        let result = transpile_to_postgres("SELECT RAND()");
        assert!(
            result.contains("RANDOM"),
            "Expected RANDOM, got: {}",
            result
        );
    }

    #[test]
    fn test_basic_select() {
        let result = transpile_to_postgres("SELECT a, b FROM users WHERE id = 1");
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM users"));
    }

    #[test]
    fn test_len_to_length() {
        let result = transpile_to_postgres("SELECT LEN(name)");
        assert!(
            result.contains("LENGTH"),
            "Expected LENGTH, got: {}",
            result
        );
    }

    #[test]
    fn test_getdate_to_current_timestamp() {
        let result = transpile_to_postgres("SELECT GETDATE()");
        assert!(
            result.contains("CURRENT_TIMESTAMP"),
            "Expected CURRENT_TIMESTAMP, got: {}",
            result
        );
    }

    #[test]
    fn test_substr_to_substring() {
        let result = transpile_to_postgres("SELECT SUBSTR(name, 1, 3)");
        assert!(
            result.contains("SUBSTRING"),
            "Expected SUBSTRING, got: {}",
            result
        );
    }

    #[test]
    fn test_group_concat_to_string_agg() {
        let result = transpile_to_postgres("SELECT GROUP_CONCAT(name)");
        assert!(
            result.contains("STRING_AGG"),
            "Expected STRING_AGG, got: {}",
            result
        );
    }

    #[test]
    fn test_double_quote_identifiers() {
        // PostgreSQL uses double quotes for identifiers
        let dialect = PostgresDialect;
        let config = dialect.generator_config();
        assert_eq!(config.identifier_quote, '"');
    }

    #[test]
    fn test_char_length_to_length() {
        let result = transpile_to_postgres("SELECT CHAR_LENGTH(name)");
        assert!(
            result.contains("LENGTH"),
            "Expected LENGTH, got: {}",
            result
        );
    }

    #[test]
    fn test_character_length_to_length() {
        let result = transpile_to_postgres("SELECT CHARACTER_LENGTH(name)");
        assert!(
            result.contains("LENGTH"),
            "Expected LENGTH, got: {}",
            result
        );
    }

    /// Helper for PostgreSQL identity tests (parse and regenerate with PostgreSQL dialect)
    fn identity_postgres(sql: &str) -> String {
        let dialect = Dialect::get(DialectType::PostgreSQL);
        let exprs = dialect.parse(sql).expect("Parse failed");
        let transformed = dialect.transform(exprs[0].clone()).expect("Transform failed");
        dialect.generate(&transformed).expect("Generate failed")
    }

    #[test]
    fn test_json_extract_with_column_path() {
        // When the path is a column reference (not a literal), should use function form
        let result = identity_postgres("json_data.data -> field_ids.field_id");
        assert!(
            result.contains("JSON_EXTRACT_PATH"),
            "Expected JSON_EXTRACT_PATH for column path, got: {}",
            result
        );
    }

    #[test]
    fn test_json_extract_scalar_with_negative_index() {
        // When the path is a negative index, should use JSON_EXTRACT_PATH_TEXT function
        let result = identity_postgres("x::JSON -> 'duration' ->> -1");
        assert!(
            result.contains("JSON_EXTRACT_PATH_TEXT"),
            "Expected JSON_EXTRACT_PATH_TEXT for negative index, got: {}",
            result
        );
        // The first -> should still be arrow syntax since 'duration' is a string literal
        assert!(
            result.contains("->"),
            "Expected -> for string literal path, got: {}",
            result
        );
    }

    #[test]
    fn test_json_extract_with_string_literal() {
        // When the path is a string literal, should keep arrow syntax
        let result = identity_postgres("data -> 'key'");
        assert!(
            result.contains("->"),
            "Expected -> for string literal path, got: {}",
            result
        );
        assert!(
            !result.contains("JSON_EXTRACT_PATH"),
            "Should NOT use function form for string literal, got: {}",
            result
        );
    }
}
