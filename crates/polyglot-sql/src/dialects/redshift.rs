//! Redshift Dialect
//!
//! Redshift-specific transformations based on sqlglot patterns.
//! Redshift is based on PostgreSQL but has some differences.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    AggFunc, Case, Cast, DataType, Expression, Function, Limit, RegexpFunc, VarArgFunc,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Redshift dialect
pub struct RedshiftDialect;

impl DialectImpl for RedshiftDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Redshift
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        use crate::tokens::TokenType;
        let mut config = TokenizerConfig::default();
        // Redshift uses double quotes for identifiers (like PostgreSQL)
        config.identifiers.insert('"', '"');
        // Redshift does NOT support nested comments
        config.nested_comments = false;
        // MINUS is an alias for EXCEPT in Redshift
        config
            .keywords
            .insert("MINUS".to_string(), TokenType::Except);
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Redshift),
            supports_column_join_marks: true,
            locking_reads_supported: false,
            tz_to_with_time_zone: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Redshift
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL is native in Redshift, but we standardize to COALESCE for consistency
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // TryCast -> TRY_CAST (Redshift supports TRY_CAST via CONVERT)
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TRY_CAST in Redshift
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // ILIKE is native in Redshift (inherited from PostgreSQL)
            Expression::ILike(op) => Ok(Expression::ILike(op)),

            // CountIf -> SUM(CASE WHEN condition THEN 1 ELSE 0 END)
            Expression::CountIf(f) => {
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(f.this.clone(), Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
                }));
                Ok(Expression::Sum(Box::new(AggFunc {
                    ignore_nulls: None,
                    having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // EXPLODE is not supported in Redshift
            Expression::Explode(_) => Ok(expr),

            // ExplodeOuter is not supported in Redshift
            Expression::ExplodeOuter(_) => Ok(expr),

            // UNNEST is supported in Redshift (but limited)
            Expression::Unnest(_) => Ok(expr),

            // RAND -> RANDOM in Redshift (like PostgreSQL)
            Expression::Rand(r) => {
                let _ = r.seed;
                Ok(Expression::Random(crate::expressions::Random))
            }

            // DateAdd -> DATEADD(unit, count, date) in Redshift
            Expression::DateAdd(f) => {
                let unit_str = match f.unit {
                    crate::expressions::IntervalUnit::Year => "YEAR",
                    crate::expressions::IntervalUnit::Quarter => "QUARTER",
                    crate::expressions::IntervalUnit::Month => "MONTH",
                    crate::expressions::IntervalUnit::Week => "WEEK",
                    crate::expressions::IntervalUnit::Day => "DAY",
                    crate::expressions::IntervalUnit::Hour => "HOUR",
                    crate::expressions::IntervalUnit::Minute => "MINUTE",
                    crate::expressions::IntervalUnit::Second => "SECOND",
                    crate::expressions::IntervalUnit::Millisecond => "MILLISECOND",
                    crate::expressions::IntervalUnit::Microsecond => "MICROSECOND",
                    crate::expressions::IntervalUnit::Nanosecond => "NANOSECOND",
                };
                let unit = Expression::Identifier(crate::expressions::Identifier {
                    name: unit_str.to_string(),
                    quoted: false,
                    trailing_comments: Vec::new(),
                    span: None,
                });
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![unit, f.interval, f.this],
                ))))
            }

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations
            Expression::Cast(c) => self.transform_cast(*c),

            // CONVERT -> CAST in Redshift
            Expression::Convert(c) => Ok(Expression::Cast(Box::new(Cast {
                this: c.this,
                to: c.to,
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
            }))),

            // SELECT TOP n -> SELECT ... LIMIT n in Redshift (PostgreSQL-style)
            Expression::Select(mut select) => {
                if let Some(top) = select.top.take() {
                    // Only convert simple TOP (not TOP PERCENT or WITH TIES)
                    if !top.percent && !top.with_ties {
                        // Convert TOP to LIMIT
                        select.limit = Some(Limit {
                            this: top.this,
                            percent: false,
                            comments: Vec::new(),
                        });
                    } else {
                        // Restore TOP if it has PERCENT or WITH TIES (not supported as LIMIT)
                        select.top = Some(top);
                    }
                }
                Ok(Expression::Select(select))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl RedshiftDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL -> COALESCE
            "IFNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // NVL -> COALESCE (supports 2+ args)
            "NVL" if f.args.len() >= 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // ISNULL -> COALESCE
            "ISNULL" if f.args.len() == 2 => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: f.args,
            }))),

            // GETDATE is native to Redshift
            "GETDATE" => Ok(Expression::Function(Box::new(Function::new(
                "GETDATE".to_string(),
                vec![],
            )))),

            // NOW -> GETDATE in Redshift
            "NOW" => Ok(Expression::Function(Box::new(Function::new(
                "GETDATE".to_string(),
                vec![],
            )))),

            // RAND -> RANDOM in Redshift
            "RAND" => Ok(Expression::Random(crate::expressions::Random)),

            // GROUP_CONCAT -> LISTAGG in Redshift
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG in Redshift
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // LISTAGG is native in Redshift
            "LISTAGG" => Ok(Expression::Function(Box::new(f))),

            // SUBSTR -> SUBSTRING
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // LEN is native in Redshift
            "LEN" => Ok(Expression::Function(Box::new(f))),

            // LENGTH -> LEN in Redshift
            "LENGTH" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "LEN".to_string(),
                f.args,
            )))),

            // CHARINDEX is native in Redshift
            "CHARINDEX" => Ok(Expression::Function(Box::new(f))),

            // POSITION -> CHARINDEX in Redshift (with swapped args)
            "POSITION" if f.args.len() == 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // CHARINDEX(substring, string)
                Ok(Expression::Function(Box::new(Function::new(
                    "CHARINDEX".to_string(),
                    vec![substring, string],
                ))))
            }

            // STRPOS -> CHARINDEX in Redshift
            "STRPOS" if f.args.len() == 2 => {
                let args = f.args;
                // STRPOS(string, substring) -> CHARINDEX(substring, string)
                let string = args[0].clone();
                let substring = args[1].clone();
                Ok(Expression::Function(Box::new(Function::new(
                    "CHARINDEX".to_string(),
                    vec![substring, string],
                ))))
            }

            // INSTR -> CHARINDEX in Redshift
            "INSTR" if f.args.len() >= 2 => {
                let mut args = f.args;
                let string = args.remove(0);
                let substring = args.remove(0);
                // INSTR(string, substring) -> CHARINDEX(substring, string)
                Ok(Expression::Function(Box::new(Function::new(
                    "CHARINDEX".to_string(),
                    vec![substring, string],
                ))))
            }

            // LOCATE -> CHARINDEX in Redshift
            "LOCATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // LOCATE(substring, string) -> CHARINDEX(substring, string)
                Ok(Expression::Function(Box::new(Function::new(
                    "CHARINDEX".to_string(),
                    vec![substring, string],
                ))))
            }

            // ARRAY_LENGTH -> ARRAY_UPPER / custom in Redshift
            // Redshift doesn't have ARRAY_LENGTH, arrays are limited
            "ARRAY_LENGTH" => Ok(Expression::Function(Box::new(f))),

            // SIZE -> not directly supported
            "SIZE" => Ok(Expression::Function(Box::new(f))),

            // TO_DATE -> TO_DATE (native in Redshift)
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // TO_TIMESTAMP -> TO_TIMESTAMP (native in Redshift)
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT -> TO_CHAR in Redshift
            "DATE_FORMAT" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(
                Function::new("TO_CHAR".to_string(), f.args),
            ))),

            // strftime -> TO_CHAR in Redshift
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_CHAR".to_string(),
                    vec![date, format],
                ))))
            }

            // TO_CHAR is native in Redshift
            "TO_CHAR" => Ok(Expression::Function(Box::new(f))),

            // LEVENSHTEIN -> not directly supported
            "LEVENSHTEIN" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT -> JSON_EXTRACT_PATH_TEXT in Redshift
            "JSON_EXTRACT" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT_PATH_TEXT".to_string(), f.args),
            ))),

            // JSON_EXTRACT_SCALAR -> JSON_EXTRACT_PATH_TEXT in Redshift
            "JSON_EXTRACT_SCALAR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT_PATH_TEXT".to_string(), f.args),
            ))),

            // GET_JSON_OBJECT -> JSON_EXTRACT_PATH_TEXT in Redshift
            "GET_JSON_OBJECT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT_PATH_TEXT".to_string(), f.args),
            ))),

            // COLLECT_LIST -> not directly supported (limited array support)
            "COLLECT_LIST" => Ok(Expression::Function(Box::new(f))),

            // COLLECT_SET -> not directly supported
            "COLLECT_SET" => Ok(Expression::Function(Box::new(f))),

            // RLIKE -> REGEXP_MATCHES in Redshift (or SIMILAR TO)
            "RLIKE" if f.args.len() == 2 => {
                // Redshift uses ~ for regex matching
                let mut args = f.args;
                let string = args.remove(0);
                let pattern = args.remove(0);
                Ok(Expression::RegexpLike(Box::new(RegexpFunc {
                    this: string,
                    pattern,
                    flags: None,
                })))
            }

            // REGEXP -> RegexpLike
            "REGEXP" if f.args.len() == 2 => {
                let mut args = f.args;
                let string = args.remove(0);
                let pattern = args.remove(0);
                Ok(Expression::RegexpLike(Box::new(RegexpFunc {
                    this: string,
                    pattern,
                    flags: None,
                })))
            }

            // REGEXP_LIKE -> native in Redshift (PostgreSQL-compatible)
            "REGEXP_LIKE" => Ok(Expression::Function(Box::new(f))),

            // ADD_MONTHS -> DATEADD in Redshift
            "ADD_MONTHS" if f.args.len() == 2 => {
                let mut args = f.args;
                let date = args.remove(0);
                let months = args.remove(0);
                // DATEADD(month, num_months, date)
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![Expression::identifier("month"), months, date],
                ))))
            }

            // DATEDIFF is native in Redshift
            "DATEDIFF" => Ok(Expression::Function(Box::new(f))),

            // DATE_DIFF -> DATEDIFF in Redshift
            "DATE_DIFF" => Ok(Expression::Function(Box::new(Function::new(
                "DATEDIFF".to_string(),
                f.args,
            )))),

            // DATEADD is native in Redshift
            "DATEADD" => Ok(Expression::Function(Box::new(f))),

            // DATE_ADD -> DATEADD in Redshift
            "DATE_ADD" => Ok(Expression::Function(Box::new(Function::new(
                "DATEADD".to_string(),
                f.args,
            )))),

            // SPLIT_TO_ARRAY is native in Redshift
            "SPLIT_TO_ARRAY" => Ok(Expression::Function(Box::new(f))),

            // STRING_TO_ARRAY -> SPLIT_TO_ARRAY in Redshift
            "STRING_TO_ARRAY" if f.args.len() >= 1 => Ok(Expression::Function(Box::new(
                Function::new("SPLIT_TO_ARRAY".to_string(), f.args),
            ))),

            // SPLIT -> SPLIT_TO_ARRAY in Redshift
            "SPLIT" if f.args.len() >= 1 => Ok(Expression::Function(Box::new(Function::new(
                "SPLIT_TO_ARRAY".to_string(),
                f.args,
            )))),

            // STRTOL is native in Redshift (string to long/base conversion)
            "STRTOL" => Ok(Expression::Function(Box::new(f))),

            // FROM_BASE -> STRTOL in Redshift
            "FROM_BASE" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "STRTOL".to_string(),
                f.args,
            )))),

            // CONVERT_TIMEZONE(target_tz, timestamp) -> CONVERT_TIMEZONE('UTC', target_tz, timestamp)
            "CONVERT_TIMEZONE" if f.args.len() == 2 => {
                let mut new_args = vec![Expression::string("UTC")];
                new_args.extend(f.args);
                Ok(Expression::Function(Box::new(Function::new(
                    "CONVERT_TIMEZONE".to_string(),
                    new_args,
                ))))
            }
            // 3-arg form stays as-is
            "CONVERT_TIMEZONE" => Ok(Expression::Function(Box::new(f))),

            // CONVERT(type, expr) -> CAST(expr AS type)
            "CONVERT" if f.args.len() == 2 => {
                let type_expr = &f.args[0];
                let value_expr = f.args[1].clone();

                // Extract type name from the first argument (it's likely a Column or Identifier)
                let type_name = match type_expr {
                    Expression::Column(c) => c.name.name.clone(),
                    Expression::Identifier(i) => i.name.clone(),
                    _ => return Ok(Expression::Function(Box::new(f))), // Can't handle, pass through
                };

                // Map type name to DataType
                let data_type = match type_name.to_uppercase().as_str() {
                    "INT" | "INTEGER" => DataType::Int {
                        length: None,
                        integer_spelling: false,
                    },
                    "BIGINT" => DataType::BigInt { length: None },
                    "SMALLINT" => DataType::SmallInt { length: None },
                    "TINYINT" => DataType::TinyInt { length: None },
                    "VARCHAR" => DataType::VarChar {
                        length: None,
                        parenthesized_length: false,
                    },
                    "CHAR" => DataType::Char { length: None },
                    "FLOAT" | "REAL" => DataType::Float {
                        precision: None,
                        scale: None,
                        real_spelling: false,
                    },
                    "DOUBLE" => DataType::Double {
                        precision: None,
                        scale: None,
                    },
                    "BOOLEAN" | "BOOL" => DataType::Boolean,
                    "DATE" => DataType::Date,
                    "TIMESTAMP" => DataType::Timestamp {
                        precision: None,
                        timezone: false,
                    },
                    "TEXT" => DataType::Text,
                    "DECIMAL" | "NUMERIC" => DataType::Decimal {
                        precision: None,
                        scale: None,
                    },
                    _ => return Ok(Expression::Function(Box::new(f))), // Unknown type, pass through
                };

                Ok(Expression::Cast(Box::new(Cast {
                    this: value_expr,
                    to: data_type,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
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
            // COUNT_IF -> SUM(CASE WHEN...)
            "COUNT_IF" if !f.args.is_empty() => {
                let condition = f.args.into_iter().next().unwrap();
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(condition, Expression::number(1))],
                    else_: Some(Expression::number(0)),
                    comments: Vec::new(),
                }));
                Ok(Expression::Sum(Box::new(AggFunc {
                    ignore_nulls: None,
                    having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    name: None,
                    limit: None,
                })))
            }

            // ANY_VALUE is native in Redshift
            "ANY_VALUE" => Ok(Expression::AggregateFunction(f)),

            // GROUP_CONCAT -> LISTAGG in Redshift
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG in Redshift
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // LISTAGG is native in Redshift
            "LISTAGG" => Ok(Expression::AggregateFunction(f)),

            // STDDEV is native in Redshift
            "STDDEV" => Ok(Expression::AggregateFunction(f)),

            // VARIANCE is native in Redshift
            "VARIANCE" => Ok(Expression::AggregateFunction(f)),

            // MEDIAN is native in Redshift
            "MEDIAN" => Ok(Expression::AggregateFunction(f)),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // Redshift type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }
}
