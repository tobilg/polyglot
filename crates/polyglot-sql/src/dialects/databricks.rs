//! Databricks Dialect
//!
//! Databricks-specific transformations based on sqlglot patterns.
//! Databricks extends Spark SQL with additional features:
//! - Colon operator for JSON extraction (col:path)
//! - DATEADD/DATEDIFF with specific syntax
//! - NULL type mapped to VOID
//! - Native REGEXP_LIKE and TRY_CAST support

use super::{DialectImpl, DialectType};
use crate::error::Result;
use crate::expressions::{
    AggregateFunction, Cast, DataType, Expression, Function, JSONExtract, Literal, UnaryFunc,
    VarArgFunc,
};
use crate::generator::GeneratorConfig;
use crate::tokens::TokenizerConfig;

/// Databricks dialect
pub struct DatabricksDialect;

impl DialectImpl for DatabricksDialect {
    fn dialect_type(&self) -> DialectType {
        DialectType::Databricks
    }

    fn tokenizer_config(&self) -> TokenizerConfig {
        let mut config = TokenizerConfig::default();
        // Databricks uses backticks for identifiers (NOT double quotes)
        config.identifiers.clear();
        config.identifiers.insert('`', '`');
        // Databricks (like Hive/Spark) uses double quotes as string delimiters
        config.quotes.insert("\"".to_string(), "\"".to_string());
        // Databricks uses backslash escapes in strings (inherited from Hive/Spark)
        config.string_escapes.push('\\');
        // Databricks supports DIV keyword for integer division
        config
            .keywords
            .insert("DIV".to_string(), crate::tokens::TokenType::Div);
        // Databricks numeric literal suffixes (same as Hive/Spark)
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
        // Databricks allows identifiers to start with digits (like Hive/Spark)
        config.identifiers_can_start_with_digit = true;
        // Databricks (like Spark): STRING_ESCAPES_ALLOWED_IN_RAW_STRINGS = False
        // Backslashes in raw strings are always literal (no escape processing)
        config.string_escapes_allowed_in_raw_strings = false;
        config
    }

    fn generator_config(&self) -> GeneratorConfig {
        use crate::generator::IdentifierQuoteStyle;
        GeneratorConfig {
            identifier_quote: '`',
            identifier_quote_style: IdentifierQuoteStyle::BACKTICK,
            dialect: Some(DialectType::Databricks),
            struct_field_sep: ": ",
            create_function_return_as: false,
            tablesample_seed_keyword: "REPEATABLE",
            identifiers_can_start_with_digit: true,
            // Databricks uses COMMENT 'value' without = sign
            schema_comment_with_eq: false,
            ..Default::default()
        }
    }

    fn transform_expr(&self, expr: Expression) -> Result<Expression> {
        match expr {
            // IFNULL -> COALESCE in Databricks
            Expression::IfNull(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // NVL -> COALESCE in Databricks
            Expression::Nvl(f) => Ok(Expression::Coalesce(Box::new(VarArgFunc {
                original_name: None,
                expressions: vec![f.this, f.expression],
            }))),

            // TryCast is native in Databricks
            Expression::TryCast(c) => Ok(Expression::TryCast(c)),

            // SafeCast -> TRY_CAST in Databricks
            Expression::SafeCast(c) => Ok(Expression::TryCast(c)),

            // ILIKE is native in Databricks (Spark 3+)
            Expression::ILike(op) => Ok(Expression::ILike(op)),

            // UNNEST -> EXPLODE in Databricks
            Expression::Unnest(f) => Ok(Expression::Explode(Box::new(UnaryFunc::new(f.this)))),

            // EXPLODE is native to Databricks
            Expression::Explode(f) => Ok(Expression::Explode(f)),

            // ExplodeOuter is supported
            Expression::ExplodeOuter(f) => Ok(Expression::ExplodeOuter(f)),

            // RANDOM -> RAND in Databricks
            Expression::Random(_) => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // Rand is native
            Expression::Rand(r) => Ok(Expression::Rand(r)),

            // || (Concat) -> CONCAT in Databricks
            Expression::Concat(op) => Ok(Expression::Function(Box::new(Function::new(
                "CONCAT".to_string(),
                vec![op.left, op.right],
            )))),

            // RegexpLike is native in Databricks
            Expression::RegexpLike(op) => Ok(Expression::RegexpLike(op)),

            // Cast with typed literal: TIMESTAMP 'x'::TYPE -> CAST(CAST('x' AS TYPE) AS TIMESTAMP)
            // This is a complex sqlglot transformation where:
            // 1. The inner typed literal (e.g., TIMESTAMP 'x') becomes CAST('x' AS <target_type>)
            // 2. The outer result is wrapped in CAST(... AS <original_literal_type>)
            Expression::Cast(c) => self.transform_cast(*c),

            // Generic function transformations
            Expression::Function(f) => self.transform_function(*f),

            // Generic aggregate function transformations
            Expression::AggregateFunction(f) => self.transform_aggregate_function(f),

            // DateSub -> DATE_ADD(date, -val) in Databricks
            Expression::DateSub(f) => {
                // Convert string literals to numbers (interval values are often stored as strings)
                let val = match f.interval {
                    Expression::Literal(crate::expressions::Literal::String(s))
                        if s.parse::<i64>().is_ok() =>
                    {
                        Expression::Literal(crate::expressions::Literal::Number(s))
                    }
                    other => other,
                };
                let neg_val = Expression::Neg(Box::new(crate::expressions::UnaryOp { this: val }));
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![f.this, neg_val],
                ))))
            }

            // Pass through everything else
            _ => Ok(expr),
        }
    }
}

impl DatabricksDialect {
    fn transform_function(&self, f: Function) -> Result<Expression> {
        let name_upper = f.name.to_uppercase();
        match name_upper.as_str() {
            // IFNULL -> COALESCE
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

            // ROW -> STRUCT (no auto-naming for cross-dialect conversion)
            "ROW" => Ok(Expression::Function(Box::new(Function::new(
                "STRUCT".to_string(),
                f.args,
            )))),

            // GETDATE -> CURRENT_TIMESTAMP
            "GETDATE" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // NOW -> CURRENT_TIMESTAMP
            "NOW" => Ok(Expression::CurrentTimestamp(
                crate::expressions::CurrentTimestamp {
                    precision: None,
                    sysdate: false,
                },
            )),

            // CURDATE -> CURRENT_DATE
            "CURDATE" => Ok(Expression::CurrentDate(crate::expressions::CurrentDate)),

            // CURRENT_DATE() with parens -> CURRENT_DATE (no parens)
            "CURRENT_DATE" if f.args.is_empty() => {
                Ok(Expression::CurrentDate(crate::expressions::CurrentDate))
            }

            // RANDOM -> RAND
            "RANDOM" => Ok(Expression::Rand(Box::new(crate::expressions::Rand {
                seed: None,
                lower: None,
                upper: None,
            }))),

            // GROUP_CONCAT -> COLLECT_LIST + ARRAY_JOIN
            "GROUP_CONCAT" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let collect_list = Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list],
                    ))))
                }
            }

            // STRING_AGG -> COLLECT_LIST + ARRAY_JOIN in Databricks
            "STRING_AGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let collect_list = Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list],
                    ))))
                }
            }

            // LISTAGG -> COLLECT_LIST + ARRAY_JOIN
            "LISTAGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let collect_list = Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list],
                    ))))
                }
            }

            // ARRAY_AGG -> COLLECT_LIST in Databricks
            "ARRAY_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // SUBSTR -> SUBSTRING
            "SUBSTR" => Ok(Expression::Function(Box::new(Function::new(
                "SUBSTRING".to_string(),
                f.args,
            )))),

            // LEN -> LENGTH
            "LEN" if f.args.len() == 1 => Ok(Expression::Length(Box::new(UnaryFunc::new(
                f.args.into_iter().next().unwrap(),
            )))),

            // CHARINDEX -> LOCATE (with swapped args, like Spark)
            "CHARINDEX" if f.args.len() >= 2 => {
                let mut args = f.args;
                let substring = args.remove(0);
                let string = args.remove(0);
                // LOCATE(substring, string)
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    vec![substring, string],
                ))))
            }

            // POSITION -> LOCATE
            "POSITION" if f.args.len() == 2 => {
                let args = f.args;
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    args,
                ))))
            }

            // STRPOS -> LOCATE (with same arg order)
            "STRPOS" if f.args.len() == 2 => {
                let args = f.args;
                let string = args[0].clone();
                let substring = args[1].clone();
                // STRPOS(string, substring) -> LOCATE(substring, string)
                Ok(Expression::Function(Box::new(Function::new(
                    "LOCATE".to_string(),
                    vec![substring, string],
                ))))
            }

            // INSTR is native in Databricks
            "INSTR" => Ok(Expression::Function(Box::new(f))),

            // LOCATE is native in Databricks
            "LOCATE" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_LENGTH -> SIZE
            "ARRAY_LENGTH" if f.args.len() == 1 => Ok(Expression::Function(Box::new(
                Function::new("SIZE".to_string(), f.args),
            ))),

            // CARDINALITY -> SIZE
            "CARDINALITY" if f.args.len() == 1 => Ok(Expression::Function(Box::new(
                Function::new("SIZE".to_string(), f.args),
            ))),

            // SIZE is native
            "SIZE" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_CONTAINS is native in Databricks
            "ARRAY_CONTAINS" => Ok(Expression::Function(Box::new(f))),

            // CONTAINS -> ARRAY_CONTAINS in Databricks (for array operations)
            // But keep CONTAINS for string contains (from CONTAINS_SUBSTR transpilation)
            "CONTAINS" if f.args.len() == 2 => {
                // Check if this is a string CONTAINS (LOWER() args pattern from CONTAINS_SUBSTR)
                let is_string_contains = matches!(&f.args[0], Expression::Lower(_))
                    && matches!(&f.args[1], Expression::Lower(_));
                if is_string_contains {
                    Ok(Expression::Function(Box::new(f)))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_CONTAINS".to_string(),
                        f.args,
                    ))))
                }
            }

            // TO_DATE is native in Databricks
            "TO_DATE" => Ok(Expression::Function(Box::new(f))),

            // TO_TIMESTAMP is native in Databricks
            "TO_TIMESTAMP" => Ok(Expression::Function(Box::new(f))),

            // DATE_FORMAT is native in Databricks
            "DATE_FORMAT" => Ok(Expression::Function(Box::new(f))),

            // strftime -> DATE_FORMAT in Databricks
            "STRFTIME" if f.args.len() >= 2 => {
                let mut args = f.args;
                let format = args.remove(0);
                let date = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATE_FORMAT".to_string(),
                    vec![date, format],
                ))))
            }

            // TO_CHAR is supported natively in Databricks (unlike Spark)
            "TO_CHAR" => Ok(Expression::Function(Box::new(f))),

            // DATE_TRUNC is native in Databricks
            "DATE_TRUNC" => Ok(Expression::Function(Box::new(f))),

            // DATEADD is native in Databricks - uppercase the unit if present
            "DATEADD" => {
                let transformed_args = self.uppercase_first_arg_if_identifier(f.args);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    transformed_args,
                ))))
            }

            // DATE_ADD -> DATEADD in Databricks (2-arg form only)
            // 2-arg with interval: DATE_ADD(date, interval) -> DATEADD(DAY, interval, date)
            // 2-arg with number: DATE_ADD(date, -2) -> keep as DATE_ADD(date, -2)
            // 3-arg: DATE_ADD(unit, amount, date) -> keep as DATE_ADD(UNIT, amount, date)
            "DATE_ADD" => {
                if f.args.len() == 2 {
                    let is_simple_number = matches!(
                        &f.args[1],
                        Expression::Literal(crate::expressions::Literal::Number(_))
                            | Expression::Neg(_)
                    );
                    if is_simple_number {
                        // Keep as DATE_ADD(date, num_days)
                        Ok(Expression::Function(Box::new(Function::new(
                            "DATE_ADD".to_string(),
                            f.args,
                        ))))
                    } else {
                        let mut args = f.args;
                        let date = args.remove(0);
                        let interval = args.remove(0);
                        let unit = Expression::Identifier(crate::expressions::Identifier {
                            name: "DAY".to_string(),
                            quoted: false,
                            trailing_comments: Vec::new(),
                            span: None,
                        });
                        Ok(Expression::Function(Box::new(Function::new(
                            "DATEADD".to_string(),
                            vec![unit, interval, date],
                        ))))
                    }
                } else {
                    let transformed_args = self.uppercase_first_arg_if_identifier(f.args);
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_ADD".to_string(),
                        transformed_args,
                    ))))
                }
            }

            // DATEDIFF is native in Databricks - uppercase the unit if present
            // 2-arg: DATEDIFF(end, start) -> DATEDIFF(DAY, start, end)
            // 3-arg: DATEDIFF(unit, start, end) -> DATEDIFF(UNIT, start, end)
            "DATEDIFF" => {
                if f.args.len() == 2 {
                    let mut args = f.args;
                    let end_date = args.remove(0);
                    let start_date = args.remove(0);
                    let unit = Expression::Identifier(crate::expressions::Identifier {
                        name: "DAY".to_string(),
                        quoted: false,
                        trailing_comments: Vec::new(),
                        span: None,
                    });
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATEDIFF".to_string(),
                        vec![unit, start_date, end_date],
                    ))))
                } else {
                    let transformed_args = self.uppercase_first_arg_if_identifier(f.args);
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATEDIFF".to_string(),
                        transformed_args,
                    ))))
                }
            }

            // DATE_DIFF -> DATEDIFF with uppercased unit
            "DATE_DIFF" => {
                let transformed_args = self.uppercase_first_arg_if_identifier(f.args);
                Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    transformed_args,
                ))))
            }

            // JSON_EXTRACT -> Use colon operator in generation, but keep as function for now
            "JSON_EXTRACT" => Ok(Expression::Function(Box::new(f))),

            // JSON_EXTRACT_SCALAR -> same handling
            "JSON_EXTRACT_SCALAR" => Ok(Expression::Function(Box::new(f))),

            // GET_JSON_OBJECT -> colon syntax in Databricks
            // GET_JSON_OBJECT(col, '$.path') becomes col:path
            "GET_JSON_OBJECT" if f.args.len() == 2 => {
                let mut args = f.args;
                let col = args.remove(0);
                let path_arg = args.remove(0);

                // Extract and strip the $. prefix from the path
                let path_expr = match &path_arg {
                    Expression::Literal(crate::expressions::Literal::String(s)) => {
                        // Strip leading '$.' if present
                        let stripped = if s.starts_with("$.") {
                            &s[2..]
                        } else if s.starts_with("$") {
                            &s[1..]
                        } else {
                            s.as_str()
                        };
                        Expression::Literal(crate::expressions::Literal::String(
                            stripped.to_string(),
                        ))
                    }
                    _ => path_arg,
                };

                Ok(Expression::JSONExtract(Box::new(JSONExtract {
                    this: Box::new(col),
                    expression: Box::new(path_expr),
                    only_json_types: None,
                    expressions: Vec::new(),
                    variant_extract: Some(Box::new(Expression::true_())),
                    json_query: None,
                    option: None,
                    quote: None,
                    on_condition: None,
                    requires_json: None,
                })))
            }

            // FROM_JSON is native in Databricks
            "FROM_JSON" => Ok(Expression::Function(Box::new(f))),

            // PARSE_JSON is native in Databricks
            "PARSE_JSON" => Ok(Expression::Function(Box::new(f))),

            // COLLECT_LIST is native in Databricks
            "COLLECT_LIST" => Ok(Expression::Function(Box::new(f))),

            // COLLECT_SET is native in Databricks
            "COLLECT_SET" => Ok(Expression::Function(Box::new(f))),

            // RLIKE is native in Databricks
            "RLIKE" => Ok(Expression::Function(Box::new(f))),

            // REGEXP -> RLIKE in Databricks
            "REGEXP" if f.args.len() == 2 => Ok(Expression::Function(Box::new(Function::new(
                "RLIKE".to_string(),
                f.args,
            )))),

            // REGEXP_LIKE is native in Databricks
            "REGEXP_LIKE" => Ok(Expression::Function(Box::new(f))),

            // LEVENSHTEIN is native in Databricks
            "LEVENSHTEIN" => Ok(Expression::Function(Box::new(f))),

            // SEQUENCE is native (for GENERATE_SERIES)
            "GENERATE_SERIES" if !f.args.is_empty() => Ok(Expression::Function(Box::new(
                Function::new("SEQUENCE".to_string(), f.args),
            ))),

            // SEQUENCE is native
            "SEQUENCE" => Ok(Expression::Function(Box::new(f))),

            // FLATTEN is native in Databricks
            "FLATTEN" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_SORT is native
            "ARRAY_SORT" => Ok(Expression::Function(Box::new(f))),

            // ARRAY_DISTINCT is native
            "ARRAY_DISTINCT" => Ok(Expression::Function(Box::new(f))),

            // TRANSFORM is native (for array transformation)
            "TRANSFORM" => Ok(Expression::Function(Box::new(f))),

            // FILTER is native (for array filtering)
            "FILTER" => Ok(Expression::Function(Box::new(f))),

            // FROM_UTC_TIMESTAMP - wrap first argument in CAST(... AS TIMESTAMP) if not already
            "FROM_UTC_TIMESTAMP" if f.args.len() >= 2 => {
                let mut args = f.args;
                let first_arg = args.remove(0);

                // Check if first arg is already a Cast to TIMESTAMP
                let wrapped_arg = if self.is_cast_to_timestamp(&first_arg) {
                    first_arg
                } else {
                    // Wrap in CAST(... AS TIMESTAMP)
                    Expression::Cast(Box::new(Cast {
                        this: first_arg,
                        to: DataType::Timestamp {
                            precision: None,
                            timezone: false,
                        },
                        trailing_comments: Vec::new(),
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                    }))
                };

                let mut new_args = vec![wrapped_arg];
                new_args.extend(args);

                Ok(Expression::Function(Box::new(Function::new(
                    "FROM_UTC_TIMESTAMP".to_string(),
                    new_args,
                ))))
            }

            // UNIFORM(low, high, RANDOM(seed)) -> UNIFORM(low, high, seed) or UNIFORM(low, high)
            "UNIFORM" if f.args.len() == 3 => {
                let mut args = f.args;
                let low = args.remove(0);
                let high = args.remove(0);
                let gen = args.remove(0);
                match gen {
                    Expression::Function(func) if func.name.to_uppercase() == "RANDOM" => {
                        if func.args.len() == 1 {
                            // RANDOM(seed) -> extract seed
                            let seed = func.args.into_iter().next().unwrap();
                            Ok(Expression::Function(Box::new(Function::new(
                                "UNIFORM".to_string(),
                                vec![low, high, seed],
                            ))))
                        } else {
                            // RANDOM() -> drop gen arg
                            Ok(Expression::Function(Box::new(Function::new(
                                "UNIFORM".to_string(),
                                vec![low, high],
                            ))))
                        }
                    }
                    Expression::Rand(r) => {
                        if let Some(seed) = r.seed {
                            Ok(Expression::Function(Box::new(Function::new(
                                "UNIFORM".to_string(),
                                vec![low, high, *seed],
                            ))))
                        } else {
                            Ok(Expression::Function(Box::new(Function::new(
                                "UNIFORM".to_string(),
                                vec![low, high],
                            ))))
                        }
                    }
                    _ => Ok(Expression::Function(Box::new(Function::new(
                        "UNIFORM".to_string(),
                        vec![low, high, gen],
                    )))),
                }
            }

            // REGEXP_SUBSTR(subject, pattern, ...) -> REGEXP_EXTRACT(subject, pattern)
            "REGEXP_SUBSTR" if f.args.len() >= 2 => {
                let subject = f.args[0].clone();
                let pattern = f.args[1].clone();
                Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_EXTRACT".to_string(),
                    vec![subject, pattern],
                ))))
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
            // COUNT_IF is native in Databricks (Spark 3+)
            "COUNT_IF" => Ok(Expression::AggregateFunction(f)),

            // ANY_VALUE is native in Databricks (Spark 3+)
            "ANY_VALUE" => Ok(Expression::AggregateFunction(f)),

            // GROUP_CONCAT -> COLLECT_LIST + ARRAY_JOIN
            "GROUP_CONCAT" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let collect_list = Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list],
                    ))))
                }
            }

            // STRING_AGG -> COLLECT_LIST + ARRAY_JOIN
            "STRING_AGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let collect_list = Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list],
                    ))))
                }
            }

            // LISTAGG -> COLLECT_LIST + ARRAY_JOIN
            "LISTAGG" if !f.args.is_empty() => {
                let mut args = f.args;
                let first = args.remove(0);
                let separator = args.pop();
                let collect_list = Expression::Function(Box::new(Function::new(
                    "COLLECT_LIST".to_string(),
                    vec![first],
                )));
                if let Some(sep) = separator {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list, sep],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_JOIN".to_string(),
                        vec![collect_list],
                    ))))
                }
            }

            // ARRAY_AGG -> COLLECT_LIST
            "ARRAY_AGG" if !f.args.is_empty() => Ok(Expression::Function(Box::new(Function::new(
                "COLLECT_LIST".to_string(),
                f.args,
            )))),

            // STDDEV is native in Databricks
            "STDDEV" => Ok(Expression::AggregateFunction(f)),

            // VARIANCE is native in Databricks
            "VARIANCE" => Ok(Expression::AggregateFunction(f)),

            // APPROX_COUNT_DISTINCT is native in Databricks
            "APPROX_COUNT_DISTINCT" => Ok(Expression::AggregateFunction(f)),

            // APPROX_DISTINCT -> APPROX_COUNT_DISTINCT
            "APPROX_DISTINCT" if !f.args.is_empty() => {
                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    name: "APPROX_COUNT_DISTINCT".to_string(),
                    args: f.args,
                    distinct: f.distinct,
                    filter: f.filter,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                })))
            }

            // Pass through everything else
            _ => Ok(Expression::AggregateFunction(f)),
        }
    }

    /// Transform Cast expressions - handles typed literals being cast
    ///
    /// When we have a typed literal (TIMESTAMP 'x', DATE 'x', TIME 'x') being cast to another type,
    /// Databricks/Spark transforms it as follows:
    ///
    /// `TIMESTAMP 'x'::TYPE` -> `CAST(CAST('x' AS TYPE) AS TIMESTAMP)`
    ///
    /// This reverses the types - the inner cast is to the target type,
    /// the outer cast is to the original literal type.
    fn transform_cast(&self, c: Cast) -> Result<Expression> {
        // Check if the inner expression is a typed literal
        match &c.this {
            // TIMESTAMP 'value'::TYPE -> CAST(CAST('value' AS TYPE) AS TIMESTAMP)
            Expression::Literal(Literal::Timestamp(value)) => {
                // Create inner cast: CAST('value' AS target_type)
                let inner_cast = Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::String(value.clone())),
                    to: c.to,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                // Create outer cast: CAST(inner_cast AS TIMESTAMP)
                Ok(Expression::Cast(Box::new(Cast {
                    this: inner_cast,
                    to: DataType::Timestamp {
                        precision: None,
                        timezone: false,
                    },
                    trailing_comments: c.trailing_comments,
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }
            // DATE 'value'::TYPE -> CAST(CAST('value' AS TYPE) AS DATE)
            Expression::Literal(Literal::Date(value)) => {
                let inner_cast = Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::String(value.clone())),
                    to: c.to,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                Ok(Expression::Cast(Box::new(Cast {
                    this: inner_cast,
                    to: DataType::Date,
                    trailing_comments: c.trailing_comments,
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }
            // TIME 'value'::TYPE -> CAST(CAST('value' AS TYPE) AS TIME)
            Expression::Literal(Literal::Time(value)) => {
                let inner_cast = Expression::Cast(Box::new(Cast {
                    this: Expression::Literal(Literal::String(value.clone())),
                    to: c.to,
                    trailing_comments: Vec::new(),
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                }));
                Ok(Expression::Cast(Box::new(Cast {
                    this: inner_cast,
                    to: DataType::Time {
                        precision: None,
                        timezone: false,
                    },
                    trailing_comments: c.trailing_comments,
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                })))
            }
            // For all other cases, pass through the Cast unchanged
            _ => Ok(Expression::Cast(Box::new(c))),
        }
    }

    /// Check if an expression is a CAST to TIMESTAMP
    fn is_cast_to_timestamp(&self, expr: &Expression) -> bool {
        if let Expression::Cast(cast) = expr {
            matches!(cast.to, DataType::Timestamp { .. })
        } else {
            false
        }
    }

    /// Helper to uppercase the first argument if it's an identifier or column (for DATEDIFF, DATEADD units)
    fn uppercase_first_arg_if_identifier(&self, mut args: Vec<Expression>) -> Vec<Expression> {
        use crate::expressions::Identifier;
        if !args.is_empty() {
            match &args[0] {
                Expression::Identifier(id) => {
                    args[0] = Expression::Identifier(Identifier {
                        name: id.name.to_uppercase(),
                        quoted: id.quoted,
                        trailing_comments: id.trailing_comments.clone(),
                        span: None,
                    });
                }
                Expression::Column(col) if col.table.is_none() => {
                    // Unqualified column name like "day" should be treated as a unit
                    args[0] = Expression::Identifier(Identifier {
                        name: col.name.name.to_uppercase(),
                        quoted: col.name.quoted,
                        trailing_comments: col.name.trailing_comments.clone(),
                        span: None,
                    });
                }
                _ => {}
            }
        }
        args
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Dialect;

    #[test]
    fn test_timestamp_literal_cast() {
        // TIMESTAMP 'value'::DATE -> CAST(CAST('value' AS DATE) AS TIMESTAMP)
        // This is test [47] in the Databricks dialect identity fixtures
        let sql = "SELECT TIMESTAMP '2025-04-29 18.47.18'::DATE";
        let expected = "SELECT CAST(CAST('2025-04-29 18.47.18' AS DATE) AS TIMESTAMP)";

        let d = Dialect::get(DialectType::Databricks);
        let ast = d.parse(sql).expect("Parse failed");
        let transformed = d.transform(ast[0].clone()).expect("Transform failed");
        let output = d.generate(&transformed).expect("Generate failed");

        assert_eq!(
            output, expected,
            "Timestamp literal cast transformation failed"
        );
    }

    #[test]
    fn test_from_utc_timestamp_wraps_column() {
        // Test [48]: FROM_UTC_TIMESTAMP(foo, 'timezone') -> FROM_UTC_TIMESTAMP(CAST(foo AS TIMESTAMP), 'timezone')
        let sql = "SELECT DATE_FORMAT(CAST(FROM_UTC_TIMESTAMP(foo, 'America/Los_Angeles') AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS foo FROM t";
        let expected = "SELECT DATE_FORMAT(CAST(FROM_UTC_TIMESTAMP(CAST(foo AS TIMESTAMP), 'America/Los_Angeles') AS TIMESTAMP), 'yyyy-MM-dd HH:mm:ss') AS foo FROM t";

        let d = Dialect::get(DialectType::Databricks);
        let ast = d.parse(sql).expect("Parse failed");
        let transformed = d.transform(ast[0].clone()).expect("Transform failed");
        let output = d.generate(&transformed).expect("Generate failed");

        assert_eq!(output, expected, "FROM_UTC_TIMESTAMP transformation failed");
    }

    #[test]
    fn test_from_utc_timestamp_keeps_existing_cast() {
        // Test [50]: FROM_UTC_TIMESTAMP(x::TIMESTAMP, tz) -> FROM_UTC_TIMESTAMP(CAST(x AS TIMESTAMP), tz)
        // When already cast to TIMESTAMP, keep it but convert :: syntax to CAST()
        let sql = "FROM_UTC_TIMESTAMP(x::TIMESTAMP, tz)";
        let expected = "FROM_UTC_TIMESTAMP(CAST(x AS TIMESTAMP), tz)";

        let d = Dialect::get(DialectType::Databricks);
        let ast = d.parse(sql).expect("Parse failed");
        let transformed = d.transform(ast[0].clone()).expect("Transform failed");
        let output = d.generate(&transformed).expect("Generate failed");

        assert_eq!(
            output, expected,
            "FROM_UTC_TIMESTAMP with existing CAST failed"
        );
    }
}
