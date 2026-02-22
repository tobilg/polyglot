//! BigQuery Dialect
//!
//! BigQuery-specific transformations based on sqlglot patterns.
//! Key differences:
//! - Uses backticks for identifiers (especially for project.dataset.table)
//! - SAFE_ prefix for safe operations
//! - Different date/time function names (DATE_DIFF, FORMAT_DATE, PARSE_DATE)
//! - STRUCT and ARRAY syntax differences
//! - No ILIKE support
//! - QUALIFY clause support

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    Alias, BinaryOp, CeilFunc, Column, Exists, Expression, From, Function, FunctionBody,
    Identifier, JsonExtractFunc, LikeOp, Literal, Select, SplitFunc, StringAggFunc, UnaryFunc,
    UnnestFunc, VarArgFunc, Where,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// BigQuery dialect
pub struct BigQueryDialect;

impl DialectImpl for BigQueryDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::BigQuery
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // BigQuery uses backticks for identifiers, NOT double quotes
        // Remove double quote from identifiers (it's in the default config)
        config.identifiers.remove(&'"');
        config.identifiers.insert('`', '`');
        // BigQuery supports double quotes for strings (in addition to single quotes)
        config.quotes.insert("\"".to_string(), "\"".to_string());
        // BigQuery supports triple-quoted strings
        config.quotes.insert("'''".to_string(), "'''".to_string());
        config
            .quotes
            .insert("\"\"\"".to_string(), "\"\"\"".to_string());
        // BigQuery supports backslash escaping in strings
        config.string_escapes = vec!['\'', '\\'];
        // In BigQuery, b'...' is a byte string (bytes), not a bit string (binary digits)
        config.b_prefix_is_byte_string = true;
        // BigQuery supports hex number strings like 0xA, 0xFF
        config.hex_number_strings = true;
        // BigQuery: 0xA represents integer 10 (not binary/blob)
        config.hex_string_is_integer_type = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::{IdentifierQuoteStyle, NormalizeFunctions};
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::BigQuery),
            // BigQuery doesn't normalize function names (Python: NORMALIZE_FUNCTIONS = False)
            normalize_functions: NormalizeFunctions::None,
            // BigQuery-specific settings from Python sqlglot
            interval_allows_plural_form: false,
            join_hints: false,
            query_hints: false,
            table_hints: false,
            limit_fetch_style: crate::generator::LimitFetchStyle::Limit,
            rename_table_with_db: false,
            nvl2_supported: false,
            unnest_with_ordinality: false,
            collate_is_func: true,
            limit_only_literals: true,
            supports_table_alias_columns: false,
            unpivot_aliases_are_identifiers: false,
            json_key_value_pair_sep: ",",
            null_ordering_supported: false,
            ignore_nulls_in_func: true,
            json_path_single_quote_escape: true,
            can_implement_array_any: true,
            supports_to_number: false,
            named_placeholder_token: "@",
            hex_func: "TO_HEX",
            with_properties_prefix: "OPTIONS",
            supports_exploding_projections: false,
            except_intersect_support_all_clause: false,
            supports_unix_seconds: true,
            // BigQuery uses SAFE_ prefix for safe operations
            try_supported: true,
            // BigQuery does not support SEMI/ANTI JOIN syntax
            semi_anti_join_with_side: false,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // ===== Data Type Mappings =====
            Expression::DataType(dt) => self.transform_data_type(dt),

            // ===== Null handling =====
            // IFNULL is native to BigQuery - keep as-is for identity
            Expression::IfNull(f) => Ok(Expression::IfNull(f)),

            // NVL -> IFNULL in BigQuery (BigQuery uses IFNULL, not NVL)
            Expression::Nvl(f) => Ok(Expression::IfNull(f)),

            // Coalesce stays as Coalesce
            Expression::Coalesce(f) => Ok(Expression::Coalesce(f)),

            // ===== String aggregation =====
            // GROUP_CONCAT -> STRING_AGG in BigQuery
            Expression::GroupConcat(f) => Ok(Expression::StringAgg(Box::new(StringAggFunc {
                this: f.this,
                separator: f.separator,
                order_by: f.order_by,
                distinct: f.distinct,
                filter: f.filter,
                limit: None,
            }))),

            // ===== Cast operations =====
            // Cast data types are transformed by transform_recursive in mod.rs
            // which calls transform_data_type via transform_expr(Expression::DataType(...))

            // TryCast -> SafeCast in BigQuery with type transformation
            Expression::TryCast(c) => {
                let transformed_type = match self.transform_data_type(c.to)? {
                    Expression::DataType(dt) => dt,
                    _ => return Err(crate::error::Error::parse("Expected DataType", 0, 0)),
                };
                Ok(Expression::SafeCast(Box::new(crate::expressions::Cast {
                    this: c.this,
                    to: transformed_type,
                    trailing_comments: c.trailing_comments,
                    double_colon_syntax: c.double_colon_syntax,
                    format: c.format,
                    default: c.default,
                })))
            }

            // ===== Pattern matching =====
            // ILIKE -> LOWER() LIKE LOWER() in BigQuery (no ILIKE support)
            Expression::ILike(op) => {
                let lower_left = Expression::Lower(Box::new(UnaryFunc::new(op.left)));
                let lower_right = Expression::Lower(Box::new(UnaryFunc::new(op.right)));
                Ok(Expression::Like(Box::new(LikeOp {
                    left: lower_left,
                    right: lower_right,
                    escape: op.escape,
                    quantifier: op.quantifier,
                })))
            }

            // RegexpLike -> REGEXP_CONTAINS in BigQuery
            Expression::RegexpLike(f) => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_CONTAINS".to_string(),
                vec![f.this, f.pattern],
            )))),

            // ===== Array operations =====
            // EXPLODE -> UNNEST in BigQuery
            Expression::Explode(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // ExplodeOuter -> UNNEST with LEFT JOIN semantics
            Expression::ExplodeOuter(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // GenerateSeries -> GENERATE_ARRAY in BigQuery
            Expression::GenerateSeries(f) => {
                let mut args = Vec::new();
                if let Some(start) = f.start {
                    args.push(*start);
                }
                if let Some(end) = f.end {
                    args.push(*end);
                }
                if let Some(step) = f.step {
                    args.push(*step);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "GENERATE_ARRAY".to_string(),
                    args,
                ))))
            }

            // ===== Bitwise operations =====
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

            // BitwiseCount -> BIT_COUNT
            Expression::BitwiseCount(f) => Ok(Expression::Function(Box::new(Function::new(
                "BIT_COUNT".to_string(),
                vec![f.this],
            )))),

            // ByteLength -> BYTE_LENGTH
            Expression::ByteLength(f) => Ok(Expression::Function(Box::new(Function::new(
                "BYTE_LENGTH".to_string(),
                vec![f.this],
            )))),

            // IntDiv -> DIV
            Expression::IntDiv(f) => Ok(Expression::Function(Box::new(Function::new(
                "DIV".to_string(),
                vec![f.this, f.expression],
            )))),

            // Int64 -> INT64
            Expression::Int64(f) => Ok(Expression::Function(Box::new(Function::new(
                "INT64".to_string(),
                vec![f.this],
            )))),

            // ===== Random =====
            // RANDOM -> RAND in BigQuery
            Expression::Random(_) => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // ===== UUID =====
            // Uuid -> GENERATE_UUID in BigQuery
            Expression::Uuid(_) => Ok(Expression::Function(Box::new(Function::new(
                "GENERATE_UUID".to_string(),
                vec![],
            )))),

            // ===== Approximate functions =====
            // ApproxDistinct -> APPROX_COUNT_DISTINCT
            Expression::ApproxDistinct(f) => Ok(Expression::Function(Box::new(Function::new(
                "APPROX_COUNT_DISTINCT".to_string(),
                vec![f.this],
            )))),

            // ArgMax -> MAX_BY in BigQuery
            Expression::ArgMax(f) => Ok(Expression::Function(Box::new(Function::new(
                "MAX_BY".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ArgMin -> MIN_BY in BigQuery
            Expression::ArgMin(f) => Ok(Expression::Function(Box::new(Function::new(
                "MIN_BY".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // ===== Conditional =====
            // CountIf -> COUNTIF in BigQuery
            Expression::CountIf(f) => Ok(Expression::Function(Box::new(Function::new(
                "COUNTIF".to_string(),
                vec![f.this],
            )))),

            // ===== String functions =====
            // StringAgg -> STRING_AGG in BigQuery - keep as-is to preserve ORDER BY
            Expression::StringAgg(f) => Ok(Expression::StringAgg(f)),

            // ===== Conversion =====
            // Unhex -> FROM_HEX
            Expression::Unhex(f) => Ok(Expression::Function(Box::new(Function::new(
                "FROM_HEX".to_string(),
                vec![*f.this],
            )))),

            // UnixToTime -> TIMESTAMP_SECONDS/MILLIS/MICROS based on scale
            Expression::UnixToTime(f) => {
                let scale = f.scale.unwrap_or(0);
                match scale {
                    0 => Ok(Expression::Function(Box::new(Function::new(
                        "TIMESTAMP_SECONDS".to_string(),
                        vec![*f.this],
                    )))),
                    3 => Ok(Expression::Function(Box::new(Function::new(
                        "TIMESTAMP_MILLIS".to_string(),
                        vec![*f.this],
                    )))),
                    6 => Ok(Expression::Function(Box::new(Function::new(
                        "TIMESTAMP_MICROS".to_string(),
                        vec![*f.this],
                    )))),
                    _ => {
                        // TIMESTAMP_SECONDS(CAST(value / POWER(10, scale) AS INT64))
                        let div_expr =
                            Expression::Div(Box::new(crate::expressions::BinaryOp::new(
                                *f.this,
                                Expression::Function(Box::new(Function::new(
                                    "POWER".to_string(),
                                    vec![Expression::number(10), Expression::number(scale)],
                                ))),
                            )));
                        let cast_expr = Expression::Cast(Box::new(crate::expressions::Cast {
                            this: div_expr,
                            to: crate::expressions::DataType::Custom {
                                name: "INT64".to_string(),
                            },
                            double_colon_syntax: false,
                            trailing_comments: vec![],
                            format: None,
                            default: None,
                        }));
                        Ok(Expression::Function(Box::new(Function::new(
                            "TIMESTAMP_SECONDS".to_string(),
                            vec![cast_expr],
                        ))))
                    }
                }
            }

            // ===== Date/time =====
            // DateDiff -> DATE_DIFF in BigQuery
            Expression::DateDiff(f) => {
                // BigQuery: DATE_DIFF(date1, date2, part)
                let unit_str = match f.unit {
                    Some(crate::expressions::IntervalUnit::Year) => "YEAR",
                    Some(crate::expressions::IntervalUnit::Quarter) => "QUARTER",
                    Some(crate::expressions::IntervalUnit::Month) => "MONTH",
                    Some(crate::expressions::IntervalUnit::Week) => "WEEK",
                    Some(crate::expressions::IntervalUnit::Day) => "DAY",
                    Some(crate::expressions::IntervalUnit::Hour) => "HOUR",
                    Some(crate::expressions::IntervalUnit::Minute) => "MINUTE",
                    Some(crate::expressions::IntervalUnit::Second) => "SECOND",
                    Some(crate::expressions::IntervalUnit::Millisecond) => "MILLISECOND",
                    Some(crate::expressions::IntervalUnit::Microsecond) => "MICROSECOND",
                    Some(crate::expressions::IntervalUnit::Nanosecond) => "NANOSECOND",
                    None => "DAY",
                };
                let unit = Expression::Identifier(crate::expressions::Identifier {
                    name: unit_str.to_string(),
                    quoted: false,
                    trailing_comments: Vec::new(),
                });
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![f.this, f.expression, unit],
                ))))
            }

            // ===== Variance =====
            // VarPop -> VAR_POP
            Expression::VarPop(f) => Ok(Expression::Function(Box::new(Function::new(
                "VAR_POP".to_string(),
                vec![f.this],
            )))),

            // ===== Hash functions =====
            // SHA -> SHA1
            Expression::SHA(f) => Ok(Expression::Function(Box::new(Function::new(
                "SHA1".to_string(),
                vec![f.this],
            )))),

            // SHA1Digest -> SHA1
            Expression::SHA1Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "SHA1".to_string(),
                vec![f.this],
            )))),

            // MD5Digest -> MD5
            Expression::MD5Digest(f) => Ok(Expression::Function(Box::new(Function::new(
                "MD5".to_string(),
                vec![*f.this],
            )))),

            // ===== Type conversion =====
            // JSONBool -> BOOL
            Expression::JSONBool(f) => Ok(Expression::Function(Box::new(Function::new(
                "BOOL".to_string(),
                vec![f.this],
            )))),

            // StringFunc -> STRING
            Expression::StringFunc(f) => Ok(Expression::Function(Box::new(Function::new(
                "STRING".to_string(),
                vec![*f.this],
            )))),

            // ===== Date/time from parts =====
            // DateFromUnixDate -> DATE_FROM_UNIX_DATE
            Expression::DateFromUnixDate(f) => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FROM_UNIX_DATE".to_string(),
                vec![f.this],
            )))),

            // UnixDate -> UNIX_DATE
            Expression::UnixDate(f) => Ok(Expression::Function(Box::new(Function::new(
                "UNIX_DATE".to_string(),
                vec![f.this],
            )))),

            // TimestampDiff -> TIMESTAMP_DIFF
            Expression::TimestampDiff(f) => Ok(Expression::Function(Box::new(Function::new(
                "TIMESTAMP_DIFF".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // FromTimeZone -> DATETIME
            Expression::FromTimeZone(f) => Ok(Expression::Function(Box::new(Function::new(
                "DATETIME".to_string(),
                vec![*f.this],
            )))),

            // TsOrDsToDatetime -> DATETIME
            Expression::TsOrDsToDatetime(f) => Ok(Expression::Function(Box::new(Function::new(
                "DATETIME".to_string(),
                vec![f.this],
            )))),

            // TsOrDsToTimestamp -> TIMESTAMP
            Expression::TsOrDsToTimestamp(f) => Ok(Expression::Function(Box::new(Function::new(
                "TIMESTAMP".to_string(),
                vec![f.this],
            )))),

            // ===== IfFunc -> IF in BigQuery =====
            Expression::IfFunc(f) => {
                let mut args = vec![f.condition, f.true_value];
                if let Some(false_val) = f.false_value {
                    args.push(false_val);
                } else {
                    args.push(Expression::Null(crate::expressions::Null));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "IF".to_string(),
                    args,
                ))))
            }

            // ===== HexString -> FROM_HEX =====
            Expression::HexStringExpr(f) => Ok(Expression::Function(Box::new(Function::new(
                "FROM_HEX".to_string(),
                vec![*f.this],
            )))),

            // ===== Additional auto-generated transforms from Python sqlglot =====
            // ApproxTopK -> APPROX_TOP_COUNT
            Expression::ApproxTopK(f) => {
                let mut args = vec![*f.this];
                if let Some(expr) = f.expression {
                    args.push(*expr);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "APPROX_TOP_COUNT".to_string(),
                    args,
                ))))
            }

            // SafeDivide -> SAFE_DIVIDE
            Expression::SafeDivide(f) => Ok(Expression::Function(Box::new(Function::new(
                "SAFE_DIVIDE".to_string(),
                vec![*f.this, *f.expression],
            )))),

            // JSONKeysAtDepth -> JSON_KEYS
            Expression::JSONKeysAtDepth(f) => Ok(Expression::Function(Box::new(Function::new(
                "JSON_KEYS".to_string(),
                vec![*f.this],
            )))),

            // JSONValueArray -> JSON_VALUE_ARRAY
            Expression::JSONValueArray(f) => Ok(Expression::Function(Box::new(Function::new(
                "JSON_VALUE_ARRAY".to_string(),
                vec![*f.this],
            )))),

            // DateFromParts -> DATE
            Expression::DateFromParts(f) => {
                let mut args = Vec::new();
                if let Some(y) = f.year {
                    args.push(*y);
                }
                if let Some(m) = f.month {
                    args.push(*m);
                }
                if let Some(d) = f.day {
                    args.push(*d);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE".to_string(),
                    args,
                ))))
            }

            // SPLIT: BigQuery defaults to comma separator when none provided
            // SPLIT(foo) -> SPLIT(foo, ',')
            Expression::Split(f) => {
                // Check if delimiter is empty or a placeholder - add default comma
                let delimiter = match &f.delimiter {
                    Expression::Literal(Literal::String(s)) if s.is_empty() => {
                        Expression::Literal(Literal::String(",".to_string()))
                    }
                    _ => f.delimiter,
                };
                Ok(Expression::Split(Box::new(SplitFunc {
                    this: f.this,
                    delimiter,
                })))
            }

            // Cast: Transform the target type according to BigQuery TYPE_MAPPING
            // Special case: CAST to JSON -> PARSE_JSON in BigQuery
            Expression::Cast(c) => {
                use crate::expressions::DataType;
                // Check if casting to JSON - use PARSE_JSON instead
                // Handle both DataType::Json/JsonB and DataType::Custom { name: "JSON" }
                // (parser creates Custom for type literals like JSON 'string')
                let is_json = matches!(c.to, DataType::Json | DataType::JsonB)
                    || matches!(&c.to, DataType::Custom { name } if name.eq_ignore_ascii_case("JSON") || name.eq_ignore_ascii_case("JSONB"));
                if is_json {
                    return Ok(Expression::ParseJson(Box::new(UnaryFunc::new(c.this))));
                }
                let transformed_type = match self.transform_data_type(c.to)? {
                    Expression::DataType(dt) => dt,
                    _ => return Err(crate::error::Error::parse("Expected DataType", 0, 0)),
                };
                Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                    this: c.this,
                    to: transformed_type,
                    trailing_comments: c.trailing_comments,
                    double_colon_syntax: c.double_colon_syntax,
                    format: c.format,
                    default: c.default,
                })))
            }

            // SafeCast: Transform the target type according to BigQuery TYPE_MAPPING
            Expression::SafeCast(c) => {
                let transformed_type = match self.transform_data_type(c.to)? {
                    Expression::DataType(dt) => dt,
                    _ => return Err(crate::error::Error::parse("Expected DataType", 0, 0)),
                };
                Ok(Expression::SafeCast(Box::new(crate::expressions::Cast {
                    this: c.this,
                    to: transformed_type,
                    trailing_comments: c.trailing_comments,
                    double_colon_syntax: c.double_colon_syntax,
                    format: c.format,
                    default: c.default,
                })))
            }

            // ===== SELECT-level transforms =====
            // BigQuery: GROUP BY expression → alias when both GROUP BY and ORDER BY exist
            Expression::Select(mut select) => {
                if select.group_by.is_some() && select.order_by.is_some() {
                    // Build map: expression → alias name for aliased projections
                    let aliases: Vec<(Expression, Identifier)> = select
                        .expressions
                        .iter()
                        .filter_map(|e| {
                            if let Expression::Alias(a) = e {
                                Some((a.this.clone(), a.alias.clone()))
                            } else {
                                None
                            }
                        })
                        .collect();

                    if let Some(ref mut group_by) = select.group_by {
                        for grouped in group_by.expressions.iter_mut() {
                            // Skip numeric indices (already aliased)
                            if matches!(grouped, Expression::Literal(Literal::Number(_))) {
                                continue;
                            }
                            // Check if this GROUP BY expression matches a SELECT alias
                            for (expr, alias_ident) in &aliases {
                                if grouped == expr {
                                    *grouped = Expression::Column(Column {
                                        name: alias_ident.clone(),
                                        table: None,
                                        join_mark: false,
                                        trailing_comments: Vec::new(),
                                    });
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(Expression::Select(select))
            }

            // ===== ArrayContains → EXISTS(SELECT 1 FROM UNNEST(arr) AS _col WHERE _col = val) =====
            Expression::ArrayContains(f) => {
                let array_expr = f.this;
                let value_expr = f.expression;

                // Build: SELECT 1 FROM UNNEST(array) AS _col WHERE _col = value
                let unnest = Expression::Unnest(Box::new(UnnestFunc {
                    this: array_expr,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                }));
                let aliased_unnest = Expression::Alias(Box::new(Alias {
                    this: unnest,
                    alias: Identifier::new("_col"),
                    column_aliases: Vec::new(),
                    pre_alias_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));
                let col_ref = Expression::Column(Column {
                    name: Identifier::new("_col"),
                    table: None,
                    join_mark: false,
                    trailing_comments: Vec::new(),
                });
                let where_clause = Where {
                    this: Expression::Eq(Box::new(BinaryOp {
                        left: col_ref,
                        right: value_expr,
                        left_comments: Vec::new(),
                        operator_comments: Vec::new(),
                        trailing_comments: Vec::new(),
                    })),
                };
                let inner_select = Expression::Select(Box::new(Select {
                    expressions: vec![Expression::Literal(Literal::Number("1".to_string()))],
                    from: Some(From {
                        expressions: vec![aliased_unnest],
                    }),
                    where_clause: Some(where_clause),
                    ..Default::default()
                }));
                Ok(Expression::Exists(Box::new(Exists {
                    this: inner_select,
                    not: false,
                })))
            }

            // ===== JSON_OBJECT array form → key-value pairs =====
            // BigQuery "signature 2": JSON_OBJECT(['a', 'b'], [10, NULL]) → JSON_OBJECT('a', 10, 'b', NULL)
            Expression::JsonObject(mut f) => {
                if f.pairs.len() == 1 {
                    // Extract expressions from both Array and ArrayFunc variants
                    let keys_exprs = match &f.pairs[0].0 {
                        Expression::Array(arr) => Some(&arr.expressions),
                        Expression::ArrayFunc(arr) => Some(&arr.expressions),
                        _ => None,
                    };
                    let vals_exprs = match &f.pairs[0].1 {
                        Expression::Array(arr) => Some(&arr.expressions),
                        Expression::ArrayFunc(arr) => Some(&arr.expressions),
                        _ => None,
                    };
                    if let (Some(keys), Some(vals)) = (keys_exprs, vals_exprs) {
                        if keys.len() == vals.len() {
                            let new_pairs: Vec<(Expression, Expression)> = keys
                                .iter()
                                .zip(vals.iter())
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();
                            f.pairs = new_pairs;
                        }
                    }
                }
                Ok(Expression::JsonObject(f))
            }

            // ===== MOD function: unwrap unnecessary Paren from first argument =====
            // BigQuery normalizes MOD((a + 1), b) -> MOD(a + 1, b)
            Expression::ModFunc(mut f) => {
                // Unwrap Paren from first argument if present
                if let Expression::Paren(paren) = f.this {
                    f.this = paren.this;
                }
                Ok(Expression::ModFunc(f))
            }

            // JSONExtract with variant_extract (Snowflake colon syntax) -> JSON_EXTRACT
            Expression::JSONExtract(e) if e.variant_extract.is_some() => {
                let path = match *e.expression {
                    Expression::Literal(Literal::String(s)) => {
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
                Ok(Expression::Function(Box::new(Function::new(
                    "JSON_EXTRACT".to_string(),
                    vec![*e.this, path],
                ))))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // MethodCall: Handle SAFE.PARSE_DATE, SAFE.PARSE_DATETIME, SAFE.PARSE_TIMESTAMP
            // These are parsed as MethodCall(this=SAFE, method=PARSE_DATE, args=[...])
            Expression::MethodCall(mc) => self.transform_method_call(*mc),

            // CreateFunction: Convert RETURNS TABLE(...) to RETURNS TABLE <...> for BigQuery
            // and set is_table_function = true for TABLE FUNCTION syntax
            Expression::CreateFunction(mut cf) => {
                if let Some(ref mut rtb) = cf.returns_table_body {
                    if rtb.starts_with("TABLE (") || rtb.starts_with("TABLE(") {
                        // Convert TABLE (...) to TABLE <...> with BigQuery types
                        let inner = if rtb.starts_with("TABLE (") {
                            &rtb["TABLE (".len()..rtb.len() - 1]
                        } else {
                            &rtb["TABLE(".len()..rtb.len() - 1]
                        };
                        // Convert common types to BigQuery equivalents
                        let converted = inner
                            .replace(" INT,", " INT64,")
                            .replace(" INT)", " INT64)")
                            .replace(" INTEGER,", " INT64,")
                            .replace(" INTEGER)", " INT64)")
                            .replace(" FLOAT,", " FLOAT64,")
                            .replace(" FLOAT)", " FLOAT64)")
                            .replace(" BOOLEAN,", " BOOL,")
                            .replace(" BOOLEAN)", " BOOL)")
                            .replace(" VARCHAR", " STRING")
                            .replace(" TEXT", " STRING");
                        // Handle trailing type (no comma, no paren)
                        let converted = if converted.ends_with(" INT") {
                            format!("{}{}", &converted[..converted.len() - 4], " INT64")
                        } else {
                            converted
                        };
                        *rtb = format!("TABLE <{}>", converted);
                        cf.is_table_function = true;
                    }
                }
                // Convert string literal body to expression body for BigQuery TABLE FUNCTIONs only
                if cf.is_table_function {
                    if let Some(ref body) = cf.body {
                        if matches!(body, FunctionBody::StringLiteral(_)) {
                            if let Some(FunctionBody::StringLiteral(sql)) = cf.body.take() {
                                // Parse the SQL string into an expression
                                if let Ok(parsed) = crate::parser::Parser::parse_sql(&sql) {
                                    if let Some(stmt) = parsed.into_iter().next() {
                                        cf.body = Some(FunctionBody::Expression(stmt));
                                    } else {
                                        cf.body = Some(FunctionBody::StringLiteral(sql));
                                    }
                                } else {
                                    cf.body = Some(FunctionBody::StringLiteral(sql));
                                }
                            }
                        }
                    }
                }
                Ok(Expression::CreateFunction(cf))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl BigQueryDialect {
    /// Transform data types according to BigQuery TYPE_MAPPING
    fn transform_data_type(&self, dt: crate::expressions::DataType) -> Result<Expression> {
        use crate::expressions::DataType;
        let transformed = match dt {
            // BIGINT -> INT64
            DataType::BigInt { .. } => DataType::Custom {
                name: "INT64".to_string(),
            },
            // INT -> INT64
            DataType::Int { .. } => DataType::Custom {
                name: "INT64".to_string(),
            },
            // SMALLINT -> INT64
            DataType::SmallInt { .. } => DataType::Custom {
                name: "INT64".to_string(),
            },
            // TINYINT -> INT64
            DataType::TinyInt { .. } => DataType::Custom {
                name: "INT64".to_string(),
            },
            // FLOAT -> FLOAT64
            DataType::Float { .. } => DataType::Custom {
                name: "FLOAT64".to_string(),
            },
            // DOUBLE -> FLOAT64
            DataType::Double { .. } => DataType::Custom {
                name: "FLOAT64".to_string(),
            },
            // BOOLEAN -> BOOL
            DataType::Boolean => DataType::Custom {
                name: "BOOL".to_string(),
            },
            // CHAR -> STRING
            DataType::Char { .. } => DataType::Custom {
                name: "STRING".to_string(),
            },
            // VARCHAR -> STRING
            DataType::VarChar { .. } => DataType::Custom {
                name: "STRING".to_string(),
            },
            // TEXT -> STRING
            DataType::Text => DataType::Custom {
                name: "STRING".to_string(),
            },
            // STRING(n) -> STRING (BigQuery doesn't support length for STRING)
            DataType::String { .. } => DataType::Custom {
                name: "STRING".to_string(),
            },
            // BINARY -> BYTES
            DataType::Binary { .. } => DataType::Custom {
                name: "BYTES".to_string(),
            },
            // VARBINARY -> BYTES
            DataType::VarBinary { .. } => DataType::Custom {
                name: "BYTES".to_string(),
            },
            // BLOB -> BYTES
            DataType::Blob => DataType::Custom {
                name: "BYTES".to_string(),
            },
            // DECIMAL -> NUMERIC (BigQuery strips precision in CAST context)
            DataType::Decimal { .. } => DataType::Custom {
                name: "NUMERIC".to_string(),
            },
            // For BigQuery identity: preserve TIMESTAMP/DATETIME as Custom types
            // This avoids the issue where parsed TIMESTAMP (timezone: false) would
            // be converted to DATETIME by the generator
            DataType::Timestamp {
                timezone: false, ..
            } => DataType::Custom {
                name: "TIMESTAMP".to_string(),
            },
            DataType::Timestamp { timezone: true, .. } => DataType::Custom {
                name: "TIMESTAMP".to_string(),
            },
            // UUID -> STRING (BigQuery doesn't have native UUID type)
            DataType::Uuid => DataType::Custom {
                name: "STRING".to_string(),
            },
            // RECORD -> STRUCT in BigQuery
            DataType::Custom { ref name } if name.eq_ignore_ascii_case("RECORD") => {
                DataType::Custom {
                    name: "STRUCT".to_string(),
                }
            }
            // TIMESTAMPTZ (custom) -> TIMESTAMP
            DataType::Custom { ref name } if name.eq_ignore_ascii_case("TIMESTAMPTZ") => {
                DataType::Custom {
                    name: "TIMESTAMP".to_string(),
                }
            }
            // BYTEINT (custom) -> INT64
            DataType::Custom { ref name } if name.eq_ignore_ascii_case("BYTEINT") => {
                DataType::Custom {
                    name: "INT64".to_string(),
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
            // IFNULL -> COALESCE (both work in BigQuery)
            "IFNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // NVL -> COALESCE
            "NVL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // ISNULL -> COALESCE
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // GROUP_CONCAT -> STRING_AGG in BigQuery
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("STRING_AGG".to_string(), f.args),
            ))),

            // SUBSTR -> SUBSTRING (both work)
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // RANDOM -> RAND
            "RANDOM" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // CURRENT_DATE -> CURRENT_DATE() in BigQuery
            // Keep as Function when it has args (e.g., CURRENT_DATE('UTC'))
            "CURRENT_DATE" if f.args.is_empty() => {
                Ok(Expression::CurrentDate(crate::expressions::CurrentDate))
            }
            "CURRENT_DATE" => Ok(Expression::Function(Box::new(Function {
                name: "CURRENT_DATE".to_string(),
                args: f.args,
                distinct: false,
                trailing_comments: Vec::new(),
                use_bracket_syntax: false,
                no_parens: false,
                quoted: false,
            }))),

            // NOW -> CURRENT_TIMESTAMP in BigQuery
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // TO_DATE -> PARSE_DATE in BigQuery
            "TO_DATE" => Ok(Expression::Function(Box::new(Function::new(
                "PARSE_DATE".to_string(),
                f.args,
            )))),

            // TO_TIMESTAMP -> PARSE_TIMESTAMP in BigQuery
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(Function::new(
                "PARSE_TIMESTAMP".to_string(),
                f.args,
            )))),

            // TO_TIME -> TIME in BigQuery
            "TO_TIME" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "TIME".to_string(),
                f.args,
            )))),

            // DATE_FORMAT -> FORMAT_DATE in BigQuery (argument order may differ)
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(Function::new(
                "FORMAT_DATE".to_string(),
                f.args,
            )))),

            // POSITION -> STRPOS in BigQuery
            // BigQuery uses STRPOS(string, substring)
            "POSITION" if f.args.len() == 2 => {
                let mut args = f.args;
                // Swap arguments: POSITION(sub IN str) -> STRPOS(str, sub)
                let first = args.remove(0);
                let second = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    vec![second, first],
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

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // ARRAY_LENGTH -> ARRAY_LENGTH (native)
            // CARDINALITY -> ARRAY_LENGTH
            "CARDINALITY" if f.args.len() == 1 => Ok(Expression::ArrayLength(Box::new(
                UnaryFunc::new(f.args.into_iter().next().unwrap()),
            ))),

            // UNNEST is native to BigQuery

            // GENERATE_SERIES -> GENERATE_ARRAY in BigQuery
            "GENERATE_SERIES" => Ok(Expression::Function(Box::new(Function::new(
                "GENERATE_ARRAY".to_string(),
                f.args,
            )))),

            // APPROX_COUNT_DISTINCT -> APPROX_COUNT_DISTINCT (native)
            // APPROX_DISTINCT -> APPROX_COUNT_DISTINCT
            "APPROX_DISTINCT" => Ok(Expression::Function(Box::new(Function::new(
                "APPROX_COUNT_DISTINCT".to_string(),
                f.args,
            )))),

            // COUNT_IF -> COUNTIF in BigQuery
            "COUNT_IF" => Ok(Expression::Function(Box::new(Function::new(
                "COUNTIF".to_string(),
                f.args,
            )))),

            // SHA1 -> SHA1 (native), SHA -> SHA1
            "SHA" => Ok(Expression::Function(Box::new(Function::new(
                "SHA1".to_string(),
                f.args,
            )))),

            // SHA256/SHA2 -> SHA256
            "SHA2" => Ok(Expression::Function(Box::new(Function::new(
                "SHA256".to_string(),
                f.args,
            )))),

            // MD5 in BigQuery returns bytes, often combined with TO_HEX
            // TO_HEX(MD5(x)) pattern
            "MD5" => Ok(Expression::Function(Box::new(Function::new(
                "MD5".to_string(),
                f.args,
            )))),

            // VARIANCE/VAR_SAMP -> VAR_SAMP (native)
            // VAR_POP -> VAR_POP (native)

            // DATEADD(unit, amount, date) → DATE_ADD(date, INTERVAL amount unit) for BigQuery
            "DATEADD" if f.args.len() == 3 => {
                let mut args = f.args;
                let unit_expr = args.remove(0);
                let amount = args.remove(0);
                let date = args.remove(0);
                // Convert unit identifier to IntervalUnit
                let unit_name = match &unit_expr {
                    Expression::Identifier(id) => id.name.to_uppercase(),
                    _ => "DAY".to_string(),
                };
                let unit = match unit_name.as_str() {
                    "YEAR" | "YEARS" | "YY" | "YYYY" => crate::expressions::IntervalUnit::Year,
                    "QUARTER" | "QUARTERS" | "QQ" | "Q" => {
                        crate::expressions::IntervalUnit::Quarter
                    }
                    "MONTH" | "MONTHS" | "MM" | "M" => crate::expressions::IntervalUnit::Month,
                    "WEEK" | "WEEKS" | "WK" | "WW" => crate::expressions::IntervalUnit::Week,
                    "DAY" | "DAYS" | "DD" | "D" | "DAYOFMONTH" => {
                        crate::expressions::IntervalUnit::Day
                    }
                    "HOUR" | "HOURS" | "HH" => crate::expressions::IntervalUnit::Hour,
                    "MINUTE" | "MINUTES" | "MI" | "N" => crate::expressions::IntervalUnit::Minute,
                    "SECOND" | "SECONDS" | "SS" | "S" => crate::expressions::IntervalUnit::Second,
                    "MILLISECOND" | "MILLISECONDS" | "MS" => {
                        crate::expressions::IntervalUnit::Millisecond
                    }
                    "MICROSECOND" | "MICROSECONDS" | "US" => {
                        crate::expressions::IntervalUnit::Microsecond
                    }
                    _ => crate::expressions::IntervalUnit::Day,
                };
                Ok(Expression::DateAdd(Box::new(
                    crate::expressions::DateAddFunc {
                        this: date,
                        interval: amount,
                        unit,
                    },
                )))
            }
            "DATE_ADD" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_ADD".to_string(),
                f.args,
            )))),

            // DATE_DIFF in BigQuery (native)
            "DATEDIFF" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_DIFF".to_string(),
                f.args,
            )))),

            // TIMESTAMP_DIFF in BigQuery
            "TIMESTAMPDIFF" => Ok(Expression::Function(Box::new(Function::new(
                "TIMESTAMP_DIFF".to_string(),
                f.args,
            )))),

            // TIME -> TIME (native)
            // DATETIME -> DATETIME (native)

            // SAFE_DIVIDE -> SAFE_DIVIDE (native)

            // NEWID/UUID -> GENERATE_UUID
            "NEWID" | "UUID" => Ok(Expression::Function(Box::new(Function::new(
                "GENERATE_UUID".to_string(),
                vec![],
            )))),

            // LEVENSHTEIN -> EDIT_DISTANCE (BigQuery naming)
            "LEVENSHTEIN" => Ok(Expression::Function(Box::new(Function::new(
                "EDIT_DISTANCE".to_string(),
                f.args,
            )))),

            // UNIX_TIMESTAMP -> UNIX_SECONDS
            "UNIX_TIMESTAMP" => Ok(Expression::Function(Box::new(Function::new(
                "UNIX_SECONDS".to_string(),
                f.args,
            )))),

            // FROM_UNIXTIME -> TIMESTAMP_SECONDS
            "FROM_UNIXTIME" => Ok(Expression::Function(Box::new(Function::new(
                "TIMESTAMP_SECONDS".to_string(),
                f.args,
            )))),

            // CHAR_LENGTH / CHARACTER_LENGTH -> LENGTH
            "CHAR_LENGTH" | "CHARACTER_LENGTH" => Ok(Expression::Function(Box::new(
                Function::new("LENGTH".to_string(), f.args),
            ))),

            // OCTET_LENGTH -> BYTE_LENGTH in BigQuery
            "OCTET_LENGTH" => Ok(Expression::Function(Box::new(Function::new(
                "BYTE_LENGTH".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT_STRING_ARRAY -> JSON_VALUE_ARRAY in BigQuery
            "JSON_EXTRACT_STRING_ARRAY" => Ok(Expression::Function(Box::new(Function::new(
                "JSON_VALUE_ARRAY".to_string(),
                f.args,
            )))),

            // INSTR is native to BigQuery

            // SPLIT: BigQuery defaults to comma separator when none provided
            // SPLIT(foo) -> SPLIT(foo, ',')
            "SPLIT" if f.args.len() == 1 => {
                let mut args = f.args;
                args.push(Expression::Literal(Literal::String(",".to_string())));
                Ok(Expression::Split(Box::new(SplitFunc {
                    this: args.remove(0),
                    delimiter: args.remove(0),
                })))
            }

            // SPLIT with two args - convert to Split expression
            "SPLIT" if f.args.len() == 2 => {
                let mut args = f.args;
                Ok(Expression::Split(Box::new(SplitFunc {
                    this: args.remove(0),
                    delimiter: args.remove(0),
                })))
            }

            // REGEXP_SUBSTR -> REGEXP_EXTRACT in BigQuery (strip extra Snowflake args)
            "REGEXP_SUBSTR" if f.args.len() >= 2 => {
                // BigQuery REGEXP_EXTRACT supports (subject, pattern, pos, occ) max 4 args
                let args = if f.args.len() > 4 {
                    f.args[..4].to_vec()
                } else {
                    f.args
                };
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_EXTRACT".to_string(),
                    args,
                ))))
            }
            "REGEXP_SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_EXTRACT".to_string(),
                f.args,
            )))),

            // REGEXP_REPLACE - strip extra Snowflake-specific args
            "REGEXP_REPLACE" if f.args.len() > 3 => {
                let args = f.args[..3].to_vec();
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_REPLACE".to_string(),
                    args,
                ))))
            }

            // OBJECT_CONSTRUCT_KEEP_NULL -> JSON_OBJECT
            "OBJECT_CONSTRUCT_KEEP_NULL" => Ok(Expression::Function(Box::new(Function::new(
                "JSON_OBJECT".to_string(),
                f.args,
            )))),

            // EDITDISTANCE -> EDIT_DISTANCE with named max_distance parameter
            "EDITDISTANCE" if f.args.len() == 3 => {
                let col1 = f.args[0].clone();
                let col2 = f.args[1].clone();
                let max_dist = f.args[2].clone();
                Ok(Expression::Function(Box::new(Function::new(
                    "EDIT_DISTANCE".to_string(),
                    vec![
                        col1,
                        col2,
                        Expression::NamedArgument(Box::new(crate::expressions::NamedArgument {
                            name: crate::expressions::Identifier::new("max_distance".to_string()),
                            value: max_dist,
                            separator: crate::expressions::NamedArgSeparator::DArrow,
                        })),
                    ],
                ))))
            }
            "EDITDISTANCE" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("EDIT_DISTANCE".to_string(), f.args),
            ))),

            // HEX_DECODE_BINARY -> FROM_HEX
            "HEX_DECODE_BINARY" => Ok(Expression::Function(Box::new(Function::new(
                "FROM_HEX".to_string(),
                f.args,
            )))),

            // BigQuery format string normalization for PARSE_DATE/DATETIME/TIMESTAMP functions
            // %Y-%m-%d -> %F and %H:%M:%S -> %T
            "PARSE_DATE"
            | "PARSE_DATETIME"
            | "PARSE_TIMESTAMP"
            | "SAFE.PARSE_DATE"
            | "SAFE.PARSE_DATETIME"
            | "SAFE.PARSE_TIMESTAMP" => {
                let args = self.normalize_time_format_args(f.args);
                Ok(Expression::Function(Box::new(Function {
                    name: f.name,
                    args,
                    distinct: f.distinct,
                    no_parens: f.no_parens,
                    trailing_comments: f.trailing_comments,
                    quoted: f.quoted,
                    use_bracket_syntax: f.use_bracket_syntax,
                })))
            }

            // GET_PATH(obj, path) -> JSON_EXTRACT(obj, json_path) in BigQuery
            "GET_PATH" if f.args.len() == 2 => {
                let mut args = f.args;
                let this = args.remove(0);
                let path = args.remove(0);
                let json_path = match &path {
                    Expression::Literal(Literal::String(s)) => {
                        let normalized = if s.starts_with('$') {
                            s.clone()
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
                    arrow_syntax: false,
                    hash_arrow_syntax: false,
                    wrapper_option: None,
                    quotes_option: None,
                    on_scalar_string: false,
                    on_error: None,
                })))
            }

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
            // GROUP_CONCAT -> STRING_AGG
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("STRING_AGG".to_string(), f.args),
            ))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    /// Transform MethodCall expressions
    /// Handles SAFE.PARSE_DATE, SAFE.PARSE_DATETIME, SAFE.PARSE_TIMESTAMP format normalization
    fn transform_method_call(&self, mc: crate::expressions::MethodCall) -> Result<Expression> {
        use crate::expressions::MethodCall;

        // Check if this is SAFE.PARSE_DATE/DATETIME/TIMESTAMP
        if let Expression::Column(ref col) = mc.this {
            if col.name.name.eq_ignore_ascii_case("SAFE") {
                let method_upper = mc.method.name.to_uppercase();
                if method_upper == "PARSE_DATE"
                    || method_upper == "PARSE_DATETIME"
                    || method_upper == "PARSE_TIMESTAMP"
                {
                    // Normalize the format string in the first argument
                    let args = self.normalize_time_format_args(mc.args);
                    return Ok(Expression::MethodCall(Box::new(MethodCall {
                        this: mc.this,
                        method: mc.method,
                        args,
                    })));
                }
            }
        }

        // Pass through all other method calls
        Ok(Expression::MethodCall(Box::new(mc)))
    }

    /// Normalize time format strings in function arguments
    /// BigQuery normalizes: %Y-%m-%d -> %F, %H:%M:%S -> %T
    fn normalize_time_format_args(&self, args: Vec<Expression>) -> Vec<Expression> {
        args.into_iter()
            .enumerate()
            .map(|(i, arg)| {
                // Only transform the first argument (the format string)
                if i == 0 {
                    if let Expression::Literal(Literal::String(s)) = arg {
                        let normalized = self.normalize_time_format(&s);
                        return Expression::Literal(Literal::String(normalized));
                    }
                }
                arg
            })
            .collect()
    }

    /// Normalize a time format string according to BigQuery conventions
    /// %Y-%m-%d -> %F (ISO date)
    /// %H:%M:%S -> %T (time)
    fn normalize_time_format(&self, format: &str) -> String {
        format.replace("%Y-%m-%d", "%F").replace("%H:%M:%S", "%T")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dialects::Dialect;

    fn transpile_to_bigquery(sql: &str) -> String {
        let dialect = Dialect::get(DialectType::Generic);
        let result = dialect
            .transpile_to(sql, DialectType::BigQuery)
            .expect("Transpile failed");
        result[0].clone()
    }

    #[test]
    fn test_ifnull_identity() {
        // Generic -> BigQuery: IFNULL is normalized to COALESCE (matching sqlglot behavior)
        let result = transpile_to_bigquery("SELECT IFNULL(a, b)");
        assert!(
            result.contains("COALESCE"),
            "Expected COALESCE, got: {}",
            result
        );
    }

    #[test]
    fn test_nvl_to_ifnull() {
        // NVL is converted to IFNULL in BigQuery
        let result = transpile_to_bigquery("SELECT NVL(a, b)");
        assert!(
            result.contains("IFNULL"),
            "Expected IFNULL, got: {}",
            result
        );
    }

    #[test]
    fn test_try_cast_to_safe_cast() {
        let result = transpile_to_bigquery("SELECT TRY_CAST(a AS INT)");
        assert!(
            result.contains("SAFE_CAST"),
            "Expected SAFE_CAST, got: {}",
            result
        );
    }

    #[test]
    fn test_random_to_rand() {
        let result = transpile_to_bigquery("SELECT RANDOM()");
        assert!(result.contains("RAND"), "Expected RAND, got: {}", result);
    }

    #[test]
    fn test_basic_select() {
        let result = transpile_to_bigquery("SELECT a, b FROM users WHERE id = 1");
        assert!(result.contains("SELECT"));
        assert!(result.contains("FROM users"));
    }

    #[test]
    fn test_group_concat_to_string_agg() {
        let result = transpile_to_bigquery("SELECT GROUP_CONCAT(name)");
        assert!(
            result.contains("STRING_AGG"),
            "Expected STRING_AGG, got: {}",
            result
        );
    }

    #[test]
    fn test_generate_series_to_generate_array() {
        let result = transpile_to_bigquery("SELECT GENERATE_SERIES(1, 10)");
        assert!(
            result.contains("GENERATE_ARRAY"),
            "Expected GENERATE_ARRAY, got: {}",
            result
        );
    }

    #[test]
    fn test_backtick_identifiers() {
        // BigQuery uses backticks for identifiers
        let dialect = BigQueryDialect;
        let config = dialect.generator_config();
        assert_eq!(config.identifier_quote, '`');
    }

    fn bigquery_identity(sql: &str, expected: &str) {
        let dialect = Dialect::get(DialectType::BigQuery);
        let ast = dialect.parse(sql).expect("Parse failed");
        let transformed = dialect.transform(ast[0].clone()).expect("Transform failed");
        let result = dialect.generate(&transformed).expect("Generate failed");
        assert_eq!(result, expected, "SQL: {}", sql);
    }

    #[test]
    fn test_cast_char_to_string() {
        bigquery_identity("CAST(x AS CHAR)", "CAST(x AS STRING)");
    }

    #[test]
    fn test_cast_varchar_to_string() {
        bigquery_identity("CAST(x AS VARCHAR)", "CAST(x AS STRING)");
    }

    #[test]
    fn test_cast_nchar_to_string() {
        bigquery_identity("CAST(x AS NCHAR)", "CAST(x AS STRING)");
    }

    #[test]
    fn test_cast_nvarchar_to_string() {
        bigquery_identity("CAST(x AS NVARCHAR)", "CAST(x AS STRING)");
    }

    #[test]
    fn test_cast_timestamptz_to_timestamp() {
        bigquery_identity("CAST(x AS TIMESTAMPTZ)", "CAST(x AS TIMESTAMP)");
    }

    #[test]
    fn test_cast_record_to_struct() {
        bigquery_identity("CAST(x AS RECORD)", "CAST(x AS STRUCT)");
    }

    #[test]
    fn test_json_literal_to_parse_json() {
        // JSON 'string' literal syntax should be converted to PARSE_JSON()
        bigquery_identity(
            "SELECT JSON '\"foo\"' AS json_data",
            "SELECT PARSE_JSON('\"foo\"') AS json_data",
        );
    }

    #[test]
    fn test_grant_as_alias_not_quoted() {
        // GRANT is not a reserved keyword in BigQuery, should not be backtick-quoted
        bigquery_identity(
            "SELECT GRANT FROM (SELECT 'input' AS GRANT)",
            "SELECT GRANT FROM (SELECT 'input' AS GRANT)",
        );
    }

    #[test]
    fn test_timestamp_literal_to_cast() {
        // TIMESTAMP 'value' literal should be converted to CAST('value' AS TIMESTAMP)
        bigquery_identity(
            "CREATE VIEW `d.v` OPTIONS (expiration_timestamp=TIMESTAMP '2020-01-02T04:05:06.007Z') AS SELECT 1 AS c",
            "CREATE VIEW `d.v` OPTIONS (expiration_timestamp=CAST('2020-01-02T04:05:06.007Z' AS TIMESTAMP)) AS SELECT 1 AS c"
        );
    }

    #[test]
    fn test_date_literal_to_cast_in_extract() {
        // Issue 1: DATE literal should become CAST syntax in BigQuery
        bigquery_identity(
            "EXTRACT(WEEK(THURSDAY) FROM DATE '2013-12-25')",
            "EXTRACT(WEEK(THURSDAY) FROM CAST('2013-12-25' AS DATE))",
        );
    }

    #[test]
    fn test_json_object_with_json_literals() {
        // Issue 2: JSON literals in JSON_OBJECT should use PARSE_JSON, not CAST AS JSON
        bigquery_identity(
            "SELECT JSON_OBJECT('a', JSON '10') AS json_data",
            "SELECT JSON_OBJECT('a', PARSE_JSON('10')) AS json_data",
        );
    }

    // NOTE: MOD paren unwrapping is tested in the conformance tests (sqlglot_dialect_identity).
    // The unit test version was removed due to stack overflow in debug builds (deep recursion).
    // Test case: MOD((a + 1), b) -> MOD(a + 1, b)

    #[test]
    fn test_safe_parse_date_format_normalization() {
        // SAFE.PARSE_DATE format string normalization: %Y-%m-%d -> %F
        bigquery_identity(
            "SAFE.PARSE_DATE('%Y-%m-%d', '2024-01-15')",
            "SAFE.PARSE_DATE('%F', '2024-01-15')",
        );
    }

    #[test]
    fn test_safe_parse_datetime_format_normalization() {
        // SAFE.PARSE_DATETIME format string normalization: %Y-%m-%d %H:%M:%S -> %F %T
        bigquery_identity(
            "SAFE.PARSE_DATETIME('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:00')",
            "SAFE.PARSE_DATETIME('%F %T', '2024-01-15 10:30:00')",
        );
    }

    #[test]
    fn test_safe_parse_timestamp_format_normalization() {
        // SAFE.PARSE_TIMESTAMP format string normalization: %Y-%m-%d %H:%M:%S -> %F %T
        bigquery_identity(
            "SAFE.PARSE_TIMESTAMP('%Y-%m-%d %H:%M:%S', '2024-01-15 10:30:00')",
            "SAFE.PARSE_TIMESTAMP('%F %T', '2024-01-15 10:30:00')",
        );
    }

    #[test]
    fn test_datetime_literal_to_cast() {
        // DATETIME 'value' literal should be converted to CAST('value' AS DATETIME)
        bigquery_identity(
            "LAST_DAY(DATETIME '2008-11-10 15:30:00', WEEK(SUNDAY))",
            "LAST_DAY(CAST('2008-11-10 15:30:00' AS DATETIME), WEEK)",
        );
    }

    #[test]
    fn test_last_day_week_modifier_stripped() {
        // WEEK(SUNDAY) should become WEEK in BigQuery LAST_DAY function
        bigquery_identity("LAST_DAY(col, WEEK(MONDAY))", "LAST_DAY(col, WEEK)");
    }
}
