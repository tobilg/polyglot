//! Hive Dialect
//!
//! Hive-specific transformations based on sqlglot patterns.
//! Key differences:
//! - No TRY_CAST (must use CAST)
//! - No ILIKE support (use LOWER + LIKE)
//! - UNNEST -> EXPLODE
//! - Uses backticks for identifiers
//! - COLLECT_LIST for array aggregation
//! - CONCAT_WS for string aggregation
//! - SIZE for array length
//! - Different date functions (FROM_UNIXTIME, UNIX_TIMESTAMP, TO_DATE)
//! - No recursive CTEs

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    BinaryOp, CeilFunc, DateTimeField, Expression, ExtractFunc, Function, LikeOp, Literal, Paren,
    UnaryFunc, VarArgFunc,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Hive dialect
pub struct HiveDialect;

impl DialectImpl for HiveDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Hive
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Hive uses backticks for identifiers (NOT double quotes)
        config.identifiers.clear();
        config.identifiers.insert('`', '`');
        // Hive uses double quotes as string delimiters (QUOTES = ["'", '"'] in Python sqlglot)
        config.quotes.insert("\"".to_string(), "\"".to_string());
        // Hive uses backslash escapes in strings (STRING_ESCAPES = ["\\"])
        config.string_escapes.push('\\');
        // Hive supports DIV keyword for integer division
        config
            .keywords
            .insert("DIV".to_string(), crate::tokens::TokenType::Div);
        // Hive numeric literal suffixes: 1L -> BIGINT, 1S -> SMALLINT, etc.
        config
            .numeric_literals
            .insert("L".to_string(), "BIGINT".to_string());
        config
            .numeric_literals
            .insert("S".to_string(), "SMALLINT".to_string());
        config
            .numeric_literals
            .insert("Y".to_string(), "TINYINT".to_string());
        config
            .numeric_literals
            .insert("D".to_string(), "DOUBLE".to_string());
        config
            .numeric_literals
            .insert("F".to_string(), "FLOAT".to_string());
        config
            .numeric_literals
            .insert("BD".to_string(), "DECIMAL".to_string());
        // Hive allows identifiers to start with digits (e.g., 1a, 1_a)
        config.identifiers_can_start_with_digit = true;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::Hive),
            // Hive uses colon separator in STRUCT field definitions: STRUCT<field_name: TYPE>
            struct_field_sep: ": ",
            // Hive places alias after the TABLESAMPLE clause
            alias_post_tablesample: true,
            join_hints: false,
            identifiers_can_start_with_digit: true,
            // Hive uses COMMENT 'value' without = sign
            schema_comment_with_eq: false,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Hive (or NVL which is also supported)
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE (NVL is actually supported in Hive, but COALESCE is standard)
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // TryCast -> CAST is now handled by the generator (dialects without TRY_CAST output CAST)
            // This allows TryCast to be preserved when Hive is the source and target supports TRY_CAST

            // SafeCast -> CAST in Hive
            Expression::SafeCast(c) => Ok(Expression::Cast(c)),

            // ILIKE -> LOWER() LIKE LOWER() in Hive (no ILIKE support)
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

            // UNNEST -> EXPLODE in Hive
            Expression::Unnest(f) => Ok(Expression::Explode(Box::new(UnaryFunc::new(f.this)))),

            // EXPLODE is native to Hive
            Expression::Explode(f) => Ok(Expression::Explode(f)),

            // ExplodeOuter is supported in Hive (OUTER EXPLODE)
            Expression::ExplodeOuter(f) => Ok(Expression::ExplodeOuter(f)),

            // RANDOM is native to Hive
            Expression::Random(_) => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // Rand is native to Hive
            Expression::Rand(r) => Ok(Expression::Rand(r)),

            // || (Concat) -> CONCAT in Hive
            Expression::Concat(op) => Ok(Expression::Function(Box::new(Function::new(
                "CONCAT".to_string(),
                vec![op.left, op.right],
            )))),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl HiveDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // LOG(x) -> LN(x) in Hive (single-arg LOG is natural log)
            "LOG" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LN".to_string(),
                f.args,
            )))),

            // IFNULL -> COALESCE
            "IFNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // NVL -> COALESCE (NVL is supported but COALESCE is standard)
            "NVL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // ISNULL -> COALESCE
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // GROUP_CONCAT -> Not directly supported in Hive
            // Use CONCAT_WS with COLLECT_LIST
            "GROUP_CONCAT" if !f.args.is_empty() => {
                // For simple cases, this would be: CONCAT_WS(sep, COLLECT_LIST(col))
                // But we'll just return a function placeholder
                Ok(Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    f.args,
                ))))
            }

            // STRING_AGG -> CONCAT_WS + COLLECT_LIST
            "STRING_AGG" if !f.args.is_empty() => {
                // Similar to GROUP_CONCAT
                Ok(Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    f.args,
                ))))
            }

            // LISTAGG -> CONCAT_WS + COLLECT_LIST
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // SUBSTRING is native to Hive (also SUBSTR)
            "SUBSTRING" | "SUBSTR" => Ok(Expression::Function(Box::new(f))),

            // LENGTH is native to Hive
            "LENGTH" => Ok(Expression::Function(Box::new(f))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // RANDOM -> RAND
            "RANDOM" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // RAND is native to Hive
            "RAND" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // NOW -> CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // CURRENT_TIMESTAMP is native
            "CURRENT_TIMESTAMP" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // CURRENT_DATE is native
            "CURRENT_DATE" => Ok(Expression::CurrentDate(crate::expressions::CurrentDate)),

            // TO_DATE is native to Hive
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // TO_TIMESTAMP -> CAST to TIMESTAMP or use FROM_UNIXTIME
            "TO_TIMESTAMP" if f.args.len() == 1 => {
                // Simple case: just cast to timestamp
                Ok(Expression::Function(Box::new(Function::new(
                    "CAST".to_string(),
                    f.args,
                ))))
            }

            // DATE_FORMAT is native to Hive
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(f))),

            // strftime -> DATE_FORMAT
            "STRFTIME" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FORMAT".to_string(),
                f.args,
            )))),

            // TO_CHAR -> DATE_FORMAT
            "TO_CHAR" => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FORMAT".to_string(),
                f.args,
            )))),

            // DATE_TRUNC -> TRUNC in Hive
            "DATE_TRUNC" => Ok(Expression::Function(Box::new(Function::new(
                "TRUNC".to_string(),
                f.args,
            )))),

            // TRUNC is native to Hive (for date truncation)
            "TRUNC" => Ok(Expression::Function(Box::new(f))),

            // EXTRACT is native to Hive (with some limitations)
            "EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // DATEPART -> Use specific functions or EXTRACT
            "DATEPART" => Ok(Expression::Function(Box::new(Function::new(
                "EXTRACT".to_string(),
                f.args,
            )))),

            // UNIX_TIMESTAMP is native to Hive
            "UNIX_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // FROM_UNIXTIME is native to Hive
            "FROM_UNIXTIME" => Ok(Expression::Function(Box::new(f))),

            // POSITION -> LOCATE in Hive
            "POSITION" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "LOCATE".to_string(),
                f.args,
            )))),

            // STRPOS -> LOCATE (with reversed args)
            "STRPOS" if f.args.len() == 2 => {
                let mut args = f.args;
                let first = args.remove(0);
                let second = args.remove(0);
                // LOCATE(substr, str) in Hive
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    vec![second, first],
                ))))
            }

            // CHARINDEX -> LOCATE (with reversed args)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // LOCATE(substr, str, [start]) in Hive
                let mut locate_args = vec![substring, string];
                if !args.is_empty() {
                    locate_args.push(args.remove(0));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    locate_args,
                ))))
            }

            // INSTR -> LOCATE
            "INSTR" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "LOCATE".to_string(),
                f.args,
            )))),

            // LOCATE is native to Hive
            "LOCATE" => Ok(Expression::Function(Box::new(f))),

            // CEILING -> CEIL
            "CEILING" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // CEIL is native to Hive
            "CEIL" if f.args.len() == 1 => Ok(Expression::Ceil(Box::new(CeilFunc {
                this: f.args.into_iter().next().unwrap(),
                decimals: None,
                to: None,
            }))),

            // UNNEST -> EXPLODE
            "UNNEST" => Ok(Expression::Function(Box::new(Function::new(
                "EXPLODE".to_string(),
                f.args,
            )))),

            // FLATTEN -> EXPLODE
            "FLATTEN" => Ok(Expression::Function(Box::new(Function::new(
                "EXPLODE".to_string(),
                f.args,
            )))),

            // ARRAY_AGG -> COLLECT_LIST
            "ARRAY_AGG" => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // COLLECT_LIST is native to Hive
            "COLLECT_LIST" => Ok(Expression::Function(Box::new(f))),

            // COLLECT_SET is native to Hive (unique values)
            "COLLECT_SET" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_LENGTH -> SIZE in Hive
            "ARRAY_LENGTH" | "ARRAY_SIZE" | "CARDINALITY" => Ok(Expression::Function(Box::new(
                Function::new("SIZE".to_string(), f.args),
            ))),

            // SIZE is native to Hive
            "SIZE" => Ok(Expression::Function(Box::new(f))),

            // SPLIT is native to Hive (returns array)
            "SPLIT" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_REPLACE - strip extra Snowflake-specific args (position, occurrence, params)
            "REGEXP_REPLACE" if f.args.len() > 3 => {
                let args = f.args[..3].to_vec();
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_REPLACE".to_string(),
                    args,
                ))))
            }
            // REGEXP_REPLACE is native to Hive
            "REGEXP_REPLACE" => Ok(Expression::Function(Box::new(f))),

            // REGEXP_SUBSTR -> REGEXP_EXTRACT (strip extra args)
            "REGEXP_SUBSTR" if f.args.len() >= 2 => {
                let subject = f.args[0].clone();
                let pattern = f.args[1].clone();
                let group = if f.args.len() >= 6 {
                    let g = &f.args[5];
                    if matches!(g, Expression::Literal(crate::expressions::Literal::Number(n)) if n == "1")
                    {
                        None
                    } else {
                        Some(g.clone())
                    }
                } else {
                    None
                };
                let mut args = vec![subject, pattern];
                if let Some(g) = group {
                    args.push(g);
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_EXTRACT".to_string(),
                    args,
                ))))
            }

            // REGEXP_EXTRACT is native to Hive
            "REGEXP_EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // RLIKE is native to Hive (regex matching)
            "RLIKE" | "REGEXP_LIKE" => Ok(Expression::Function(Box::new(Function::new(
                "RLIKE".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT -> GET_JSON_OBJECT
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(Function::new(
                "GET_JSON_OBJECT".to_string(),
                f.args,
            )))),

            // JSON_EXTRACT_SCALAR -> GET_JSON_OBJECT
            "JSON_EXTRACT_SCALAR" => Ok(Expression::Function(Box::new(Function::new(
                "GET_JSON_OBJECT".to_string(),
                f.args,
            )))),

            // GET_JSON_OBJECT is native to Hive
            "GET_JSON_OBJECT" => Ok(Expression::Function(Box::new(f))),

            // PARSE_JSON -> Not directly available in Hive
            // Would need FROM_JSON in newer versions
            "PARSE_JSON" => Ok(Expression::Function(Box::new(Function::new(
                "FROM_JSON".to_string(),
                f.args,
            )))),

            // TO_JSON is native in newer Hive versions
            "TO_JSON" => Ok(Expression::Function(Box::new(f))),

            // DATEDIFF is native to Hive (but only for days)
            "DATEDIFF" => Ok(Expression::Function(Box::new(f))),

            // DATE_ADD is native to Hive
            "DATE_ADD" => Ok(Expression::Function(Box::new(f))),

            // DATE_SUB -> DATE_ADD with negated interval in Hive
            // DATE_SUB(date, n) becomes DATE_ADD(date, (n) * -1)
            "DATE_SUB" if f.args.len() == 2 => {
                let mut args = f.args;
                let date_arg = args.remove(0);
                let interval_arg = args.remove(0);

                // Wrap in parens only if the interval is a complex expression (not literal/column)
                let effective_interval = match &interval_arg {
                    Expression::Literal(_) | Expression::Column(_) | Expression::Identifier(_) => {
                        interval_arg
                    }
                    _ => Expression::Paren(Box::new(Paren {
                        this: interval_arg,
                        trailing_comments: Vec::new(),
                    })),
                };

                // Negate the interval: val * -1
                let negated_interval = Expression::Mul(Box::new(BinaryOp {
                    left: effective_interval,
                    right: Expression::Literal(Literal::Number("-1".to_string())),
                    left_comments: Vec::new(),
                    operator_comments: Vec::new(),
                    trailing_comments: Vec::new(),
                }));

                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![date_arg, negated_interval],
                ))))
            }

            // ADD_MONTHS is native to Hive
            "ADD_MONTHS" => Ok(Expression::Function(Box::new(f))),

            // MONTHS_BETWEEN is native to Hive
            "MONTHS_BETWEEN" => Ok(Expression::Function(Box::new(f))),

            // NVL is native to Hive
            "NVL" => Ok(Expression::Function(Box::new(f))),

            // NVL2 is native to Hive
            "NVL2" => Ok(Expression::Function(Box::new(f))),

            // MAP is native to Hive
            "MAP" => Ok(Expression::Function(Box::new(f))),

            // ARRAY is native to Hive
            "ARRAY" => Ok(Expression::Function(Box::new(f))),

            // STRUCT is native to Hive
            "STRUCT" => Ok(Expression::Function(Box::new(f))),

            // NAMED_STRUCT is native to Hive
            "NAMED_STRUCT" => Ok(Expression::Function(Box::new(f))),

            // DATE_PART(part, expr) -> EXTRACT(part FROM expr)
            "DATE_PART" if f.args.len() == 2 => {
                let mut args = f.args;
                let part = args.remove(0);
                let expr = args.remove(0);
                if let Some(field) = hive_expr_to_datetime_field(&part) {
                    Ok(Expression::Extract(Box::new(ExtractFunc {
                        this: expr,
                        field,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PART".to_string(),
                        vec![part, expr],
                    ))))
                }
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
            // GROUP_CONCAT -> COLLECT_LIST (then needs CONCAT_WS)
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("COLLECT_LIST".to_string(), f.args),
            ))),

            // STRING_AGG -> COLLECT_LIST
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("COLLECT_LIST".to_string(), f.args),
            ))),

            // LISTAGG -> COLLECT_LIST
            "LISTAGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // ARRAY_AGG -> COLLECT_LIST
            "ARRAY_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }
}

/// Convert an expression (string literal or identifier) to a DateTimeField for Hive
fn hive_expr_to_datetime_field(expr: &Expression) -> Option<DateTimeField> {
    let name = match expr {
        Expression::Literal(Literal::String(s)) => s.to_uppercase(),
        Expression::Identifier(id) => id.name.to_uppercase(),
        Expression::Column(col) if col.table.is_none() => col.name.name.to_uppercase(),
        _ => return None,
    };
    match name.as_str() {
        "YEAR" | "Y" | "YY" | "YYY" | "YYYY" | "YR" | "YEARS" | "YRS" => Some(DateTimeField::Year),
        "MONTH" | "MM" | "MON" | "MONS" | "MONTHS" => Some(DateTimeField::Month),
        "DAY" | "D" | "DD" | "DAYS" | "DAYOFMONTH" => Some(DateTimeField::Day),
        "HOUR" | "H" | "HH" | "HR" | "HOURS" | "HRS" => Some(DateTimeField::Hour),
        "MINUTE" | "MI" | "MIN" | "MINUTES" | "MINS" => Some(DateTimeField::Minute),
        "SECOND" | "S" | "SEC" | "SECONDS" | "SECS" => Some(DateTimeField::Second),
        "MILLISECOND" | "MS" | "MSEC" | "MILLISECONDS" => Some(DateTimeField::Millisecond),
        "MICROSECOND" | "US" | "USEC" | "MICROSECONDS" => Some(DateTimeField::Microsecond),
        "DOW" | "DAYOFWEEK" | "DAYOFWEEK_ISO" | "DW" => Some(DateTimeField::DayOfWeek),
        "DOY" | "DAYOFYEAR" => Some(DateTimeField::DayOfYear),
        "WEEK" | "W" | "WK" | "WEEKOFYEAR" | "WOY" => Some(DateTimeField::Week),
        "QUARTER" | "Q" | "QTR" | "QTRS" | "QUARTERS" => Some(DateTimeField::Quarter),
        "EPOCH" | "EPOCH_SECOND" | "EPOCH_SECONDS" => Some(DateTimeField::Epoch),
        "TIMEZONE" | "TIMEZONE_HOUR" | "TZH" => Some(DateTimeField::TimezoneHour),
        "TIMEZONE_MINUTE" | "TZM" => Some(DateTimeField::TimezoneMinute),
        _ => Some(DateTimeField::Custom(name)),
    }
}
