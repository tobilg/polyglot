//! Trino Dialect
//!
//! Trino-specific transformations based on sqlglot patterns.
//! Trino is largely compatible with Presto but has some differences.

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    AggFunc, AggregateFunction, Case, Cast, DataType, Expression, Function, IntervalUnit, IntervalUnitSpec, LikeOp, Literal, UnaryFunc, VarArgFunc,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Trino dialect
pub struct TrinoDialect;

impl DialectImpl for TrinoDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Trino
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Trino uses double quotes for identifiers
        config.identifiers.insert('"', '"');
        // Trino does NOT support nested comments
        config.nested_comments = false;
        // Trino does NOT support QUALIFY - it's a valid identifier
        // (unlike Snowflake, BigQuery, DuckDB which have QUALIFY clause)
        config.keywords.remove("QUALIFY");
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '"',
            identifier_quote_style: IdentifierQuoteStyle::DOUBLE_QUOTE,
            dialect: Some(DialectType::Trino),
            limit_only_literals: true,
            tz_to_with_time_zone: true,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Trino
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE in Trino
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc { original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // Coalesce with original_name (e.g., IFNULL parsed as Coalesce) -> clear original_name
            Expression::Coalesce(mut f) => {
                f.original_name = None;
                Ok(Expression::Coalesce(f))
            }

            // TryCast stays as TryCast (Trino supports TRY_CAST)
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TRY_CAST in Trino
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // ILike -> LOWER() LIKE LOWER() (Trino doesn't support ILIKE)
            Expression::ILike(op) => {
                let lower_left = Expression::Lower(Box::new(UnaryFunc::new(op.left.clone())));
                let lower_right = Expression::Lower(Box::new(UnaryFunc::new(op.right.clone())));
                Ok(Expression::Like(Box::new(LikeOp {
                    left: lower_left,
                    right: lower_right,
                    escape: op.escape,
                    quantifier: op.quantifier.clone(),
                })))
            }

            // CountIf is native in Trino (keep as-is)
            Expression::CountIf(f) => Ok(Expression::CountIf(f)),

            // EXPLODE -> UNNEST in Trino
            Expression::Explode(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // ExplodeOuter -> UNNEST in Trino
            Expression::ExplodeOuter(f) => Ok(Expression::Unnest(Box::new(
                crate::expressions::UnnestFunc {
                    this: f.this,
                    expressions: Vec::new(),
                    with_ordinality: false,
                    alias: None,
                    offset_alias: None,
                },
            ))),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // Cast transformations
            Expression::Cast(c) => self.transform_cast(*c),

            // TRIM: Convert comma syntax TRIM(str, chars) to SQL standard TRIM(chars FROM str)
            // Trino requires SQL standard syntax for TRIM with characters
            Expression::Trim(mut f) => {
                if !f.sql_standard_syntax && f.characters.is_some() {
                    // Convert from TRIM(str, chars) to TRIM(chars FROM str)
                    f.sql_standard_syntax = true;
                }
                Ok(Expression::Trim(f))
            }

            // LISTAGG: Add default separator ',' if none is specified (Trino style)
            Expression::ListAgg(mut f) => {
                if f.separator.is_none() {
                    f.separator = Some(Expression::Literal(Literal::String(",".to_string())));
                }
                Ok(Expression::ListAgg(f))
            }

            // Interval: Split compound string intervals like INTERVAL '1 day' into INTERVAL '1' DAY
            Expression::Interval(mut interval) => {
                if interval.unit.is_none() {
                    if let Some(Expression::Literal(Literal::String(ref s))) = interval.this {
                        if let Some((value, unit)) = Self::parse_compound_interval(s) {
                            interval.this = Some(Expression::Literal(Literal::String(value)));
                            interval.unit = Some(unit);
                        }
                    }
                }
                Ok(Expression::Interval(interval))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl TrinoDialect {
    /// Parse a compound interval string like "1 day" into (value, unit_spec).
    /// Returns None if the string doesn't match a known pattern.
    fn parse_compound_interval(s: &str) -> Option<(String, IntervalUnitSpec)> {
        let s = s.trim();
        let parts: Vec<&str> = s.split_whitespace().collect();
        if parts.len() != 2 {
            return None;
        }
        let value = parts[0].to_string();
        let unit = match parts[1].to_uppercase().as_str() {
            "YEAR" | "YEARS" => IntervalUnit::Year,
            "MONTH" | "MONTHS" => IntervalUnit::Month,
            "DAY" | "DAYS" => IntervalUnit::Day,
            "HOUR" | "HOURS" => IntervalUnit::Hour,
            "MINUTE" | "MINUTES" => IntervalUnit::Minute,
            "SECOND" | "SECONDS" => IntervalUnit::Second,
            "MILLISECOND" | "MILLISECONDS" => IntervalUnit::Millisecond,
            "MICROSECOND" | "MICROSECONDS" => IntervalUnit::Microsecond,
            _ => return None,
        };
        Some((value, IntervalUnitSpec::Simple { unit, use_plural: false }))
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

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // NOW -> CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp { precision: None, sysdate: false },
            )),

            // RAND -> RANDOM in Trino
            "RAND" => Ok(Expression::Function(Box::new(Function::new(
                "RANDOM".to_string(),
                vec![],
            )))),

            // GROUP_CONCAT -> LISTAGG in Trino (Trino supports LISTAGG)
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG in Trino
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // LISTAGG is native in Trino
            "LISTAGG" => Ok(Expression::Function(Box::new(f))),

            // SUBSTR -> SUBSTRING
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // CHARINDEX -> STRPOS in Trino (with swapped args)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    vec![string, substring],
                ))))
            }

            // INSTR -> STRPOS
            "INSTR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "STRPOS".to_string(),
                f.args,
            )))),

            // LOCATE -> STRPOS in Trino (with swapped args)
            "LOCATE" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "STRPOS".to_string(),
                    vec![string, substring],
                ))))
            }

            // ARRAY_LENGTH -> CARDINALITY in Trino
            "ARRAY_LENGTH" if f.args.len() == 1 => Ok(Expression::Function(Box::new(
                Function::new("CARDINALITY".to_string(), f.args),
            ))),

            // SIZE -> CARDINALITY in Trino
            "SIZE" if f.args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
                "CARDINALITY".to_string(),
                f.args,
            )))),

            // ARRAY_CONTAINS -> CONTAINS in Trino
            "ARRAY_CONTAINS" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("CONTAINS".to_string(), f.args),
            ))),

            // TO_DATE -> CAST to DATE or DATE_PARSE
            "TO_DATE" if !f.args.is_empty() => {
                if f.args.len() == 1 {
                    Ok(Expression::Cast(Box::new(Cast {
                        this: f.args.into_iter().next().unwrap(),
                        to: DataType::Date,
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PARSE".to_string(),
                        f.args,
                    ))))
                }
            }

            // TO_TIMESTAMP -> CAST or DATE_PARSE
            "TO_TIMESTAMP" if !f.args.is_empty() => {
                if f.args.len() == 1 {
                    Ok(Expression::Cast(Box::new(Cast {
                        this: f.args.into_iter().next().unwrap(),
                        to: DataType::Timestamp { precision: None, timezone: false },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    })))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_PARSE".to_string(),
                        f.args,
                    ))))
                }
            }

            // strftime -> DATE_FORMAT in Trino
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, format],
                ))))
            }

            // TO_CHAR -> DATE_FORMAT in Trino
            "TO_CHAR" if f.args.len() >= 2 => Ok(Expression::Function(Box::new(Function::new(
                "DATE_FORMAT".to_string(),
                f.args,
            )))),

            // LEVENSHTEIN -> LEVENSHTEIN_DISTANCE in Trino
            "LEVENSHTEIN" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LEVENSHTEIN_DISTANCE".to_string(), f.args),
            ))),

            // GET_JSON_OBJECT -> JSON_EXTRACT_SCALAR in Trino
            "GET_JSON_OBJECT" if f.args.len() == 2 => Ok(Expression::Function(Box::new(
                Function::new("JSON_EXTRACT_SCALAR".to_string(), f.args),
            ))),

            // COLLECT_LIST -> ARRAY_AGG
            "COLLECT_LIST" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("ARRAY_AGG".to_string(), f.args),
            ))),

            // COLLECT_SET -> ARRAY_DISTINCT(ARRAY_AGG())
            "COLLECT_SET" if !f.args.is_empty() => {
                let array_agg = Expression::Function(Box::new(Function::new(
                    "ARRAY_AGG".to_string(),
                    f.args,
                )));
                Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_DISTINCT".to_string(),
                    vec![array_agg],
                ))))
            }

            // RLIKE -> REGEXP_LIKE in Trino
            "RLIKE" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_LIKE".to_string(),
                f.args,
            )))),

            // REGEXP -> REGEXP_LIKE in Trino
            "REGEXP" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "REGEXP_LIKE".to_string(),
                f.args,
            )))),

            // ARRAY_SUM -> REDUCE in Trino (complex transformation)
            // For simplicity, we'll use a different approach
            "ARRAY_SUM" if f.args.len() == 1 => {
                // This is a complex transformation in Presto/Trino
                // ARRAY_SUM(arr) -> REDUCE(arr, 0, (s, x) -> s + x, s -> s)
                // For now, pass through and let user handle it
                Ok(Expression::Function(Box::new(f)))
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
                Ok(Expression::Sum(Box::new(AggFunc { ignore_nulls: None, having_max: None,
                    this: case_expr,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                name: None,
                limit: None,
                })))
            }

            // ANY_VALUE -> ARBITRARY in Trino
            "ANY_VALUE" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "ARBITRARY".to_string(),
                f.args,
            )))),

            // GROUP_CONCAT -> LISTAGG in Trino
            "GROUP_CONCAT" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // STRING_AGG -> LISTAGG in Trino
            "STRING_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("LISTAGG".to_string(), f.args),
            ))),

            // VAR -> VAR_POP in Trino
            "VAR" if !f.args.is_empty() => Ok(Expression::AggregateFunction(Box::new(
                AggregateFunction {
                    name: "VAR_POP".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                },
            ))),

            // VARIANCE -> VAR_SAMP in Trino
            "VARIANCE" if !f.args.is_empty() => Ok(Expression::AggregateFunction(Box::new(
                AggregateFunction {
                    name: "VAR_SAMP".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                },
            ))),

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // Trino type mappings are handled in the generator
        Ok(Expression::Cast(Box::new(c)))
    }
}
