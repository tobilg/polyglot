use super::{normalize, operators, temporal, types, NormalizationContext, RewriteOutcome};
use crate::dialects::{
    duckdb_to_bigquery_format, duckdb_to_presto_format, is_default_presto_date_format,
    is_default_presto_timestamp_format, normalize_presto_format, presto_to_bigquery_format,
    presto_to_duckdb_format, presto_to_java_format, Dialect, DialectType,
};
use crate::error::Result;
use crate::expressions::*;
use crate::generator::Generator;

#[derive(Debug)]
pub(super) enum Action {
    GreatestLeastNull,
    BigQueryFunctionNormalize,
    BigQueryToHexBare,
    BigQueryToHexLower,
    BigQueryToHexUpper,
    BigQueryLastDayStripUnit,
    GenericFunctionNormalize,
    NvlClearOriginal,
    CurrentUserParens,
    EscapeStringNormalize,
    SnowflakeIntervalFormat,
    MysqlNullsLastRewrite,
    FilterToIff,
    StrPositionExpand,
    CurrentUserSparkParens,
    ConcatCoalesceWrap,
    PostgresSingleValueConcatToTsql,
    CbrtToPower,
    MinMaxToLeastGreatest,
    Nvl2Expand,
    IfnullToCoalesce,
    IsAsciiConvert,
    StrPositionConvert,
    DecodeSimplify,
}

pub(super) fn rewrite(
    action: Action,
    expression: Expression,
    context: &NormalizationContext,
) -> Result<RewriteOutcome> {
    let source = context.source;
    let target = context.target;
    let e = expression;
    let expression = (|| -> Result<Expression> {
        match action {
            Action::GreatestLeastNull => {
                let f = if let Expression::Function(f) = e {
                    *f
                } else {
                    unreachable!("action only triggered for Function expressions")
                };
                let mut null_checks: Vec<Expression> = f
                    .args
                    .iter()
                    .map(|a| {
                        Expression::IsNull(Box::new(IsNull {
                            this: a.clone(),
                            not: false,
                            postfix_form: false,
                        }))
                    })
                    .collect();
                let condition = if null_checks.len() == 1 {
                    null_checks.remove(0)
                } else {
                    let first = null_checks.remove(0);
                    null_checks.into_iter().fold(first, |acc, check| {
                        Expression::Or(Box::new(BinaryOp::new(acc, check)))
                    })
                };
                Ok(Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(condition, Expression::Null(Null))],
                    else_: Some(Expression::Function(Box::new(Function::new(
                        f.name, f.args,
                    )))),
                    comments: Vec::new(),
                    inferred_type: None,
                })))
            }

            Action::BigQueryFunctionNormalize => normalize_bigquery_function(e, source, target),

            Action::BigQueryToHexBare => {
                // Not used anymore - handled directly in normalize_bigquery_function
                Ok(e)
            }

            Action::BigQueryToHexLower => {
                if let Expression::Lower(uf) = e {
                    match uf.this {
                        // BQ->BQ: LOWER(TO_HEX(x)) -> TO_HEX(x)
                        Expression::Function(f)
                            if matches!(target, DialectType::BigQuery) && f.name == "TO_HEX" =>
                        {
                            Ok(Expression::Function(f))
                        }
                        // LOWER(LOWER(HEX/TO_HEX(x))) patterns
                        Expression::Lower(inner_uf) => {
                            if matches!(target, DialectType::BigQuery) {
                                // BQ->BQ: extract TO_HEX
                                if let Expression::Function(f) = inner_uf.this {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_HEX".to_string(),
                                        f.args,
                                    ))))
                                } else {
                                    Ok(Expression::Lower(inner_uf))
                                }
                            } else {
                                // Flatten: LOWER(LOWER(x)) -> LOWER(x)
                                Ok(Expression::Lower(inner_uf))
                            }
                        }
                        other => Ok(Expression::Lower(Box::new(crate::expressions::UnaryFunc {
                            this: other,
                            original_name: None,
                            inferred_type: None,
                        }))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::BigQueryToHexUpper => {
                // UPPER(LOWER(HEX(x))) -> HEX(x) (UPPER cancels LOWER, HEX is already uppercase)
                // UPPER(LOWER(TO_HEX(x))) -> TO_HEX(x) for Presto/Trino
                if let Expression::Upper(uf) = e {
                    if let Expression::Lower(inner_uf) = uf.this {
                        // For BQ->BQ: UPPER(TO_HEX(x)) should stay as UPPER(TO_HEX(x))
                        if matches!(target, DialectType::BigQuery) {
                            // Restore TO_HEX name in inner function
                            if let Expression::Function(f) = inner_uf.this {
                                let restored = Expression::Function(Box::new(Function::new(
                                    "TO_HEX".to_string(),
                                    f.args,
                                )));
                                Ok(Expression::Upper(Box::new(
                                    crate::expressions::UnaryFunc::new(restored),
                                )))
                            } else {
                                Ok(Expression::Upper(inner_uf))
                            }
                        } else {
                            // Extract the inner HEX/TO_HEX function (UPPER(LOWER(x)) = x when HEX is uppercase)
                            Ok(inner_uf.this)
                        }
                    } else {
                        Ok(Expression::Upper(uf))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::BigQueryLastDayStripUnit => {
                if let Expression::LastDay(mut ld) = e {
                    ld.unit = None; // Strip the unit (MONTH is default)
                    match target {
                        DialectType::PostgreSQL => {
                            // LAST_DAY(date) -> CAST(DATE_TRUNC('MONTH', date) + INTERVAL '1 MONTH' - INTERVAL '1 DAY' AS DATE)
                            let date_trunc = Expression::Function(Box::new(Function::new(
                                "DATE_TRUNC".to_string(),
                                vec![
                                    Expression::Literal(Box::new(
                                        crate::expressions::Literal::String("MONTH".to_string()),
                                    )),
                                    ld.this.clone(),
                                ],
                            )));
                            let plus_month =
                                Expression::Add(Box::new(crate::expressions::BinaryOp::new(
                                    date_trunc,
                                    Expression::Interval(Box::new(crate::expressions::Interval {
                                        this: Some(Expression::Literal(Box::new(
                                            crate::expressions::Literal::String(
                                                "1 MONTH".to_string(),
                                            ),
                                        ))),
                                        unit: None,
                                    })),
                                )));
                            let minus_day =
                                Expression::Sub(Box::new(crate::expressions::BinaryOp::new(
                                    plus_month,
                                    Expression::Interval(Box::new(crate::expressions::Interval {
                                        this: Some(Expression::Literal(Box::new(
                                            crate::expressions::Literal::String(
                                                "1 DAY".to_string(),
                                            ),
                                        ))),
                                        unit: None,
                                    })),
                                )));
                            Ok(Expression::Cast(Box::new(Cast {
                                this: minus_day,
                                to: DataType::Date,
                                trailing_comments: vec![],
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        DialectType::Presto => {
                            // LAST_DAY(date) -> LAST_DAY_OF_MONTH(date)
                            Ok(Expression::Function(Box::new(Function::new(
                                "LAST_DAY_OF_MONTH".to_string(),
                                vec![ld.this],
                            ))))
                        }
                        DialectType::ClickHouse => {
                            // ClickHouse LAST_DAY(CAST(x AS Nullable(DATE)))
                            // Need to wrap the DATE type in Nullable
                            let nullable_date = match ld.this {
                                Expression::Cast(mut c) => {
                                    c.to = DataType::Nullable {
                                        inner: Box::new(DataType::Date),
                                    };
                                    Expression::Cast(c)
                                }
                                other => other,
                            };
                            ld.this = nullable_date;
                            Ok(Expression::LastDay(ld))
                        }
                        _ => Ok(Expression::LastDay(ld)),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::GenericFunctionNormalize => {
                // Helper closure to convert ARBITRARY to target-specific function
                fn convert_arbitrary(arg: Expression, target: DialectType) -> Expression {
                    let name = match target {
                        DialectType::ClickHouse => "any",
                        DialectType::TSQL | DialectType::SQLite => "MAX",
                        DialectType::Hive => "FIRST",
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            "ARBITRARY"
                        }
                        _ => "ANY_VALUE",
                    };
                    Expression::Function(Box::new(Function::new(name.to_string(), vec![arg])))
                }

                if let Expression::Function(f) = e {
                    let name = f.name.to_ascii_uppercase();
                    match name.as_str() {
                        "ARBITRARY" if f.args.len() == 1 => {
                            let arg = f.args.into_iter().next().unwrap();
                            Ok(convert_arbitrary(arg, target))
                        }
                        "TO_NUMBER" if f.args.len() == 1 => {
                            let arg = f.args.into_iter().next().unwrap();
                            match target {
                                DialectType::Oracle | DialectType::Snowflake => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_NUMBER".to_string(),
                                        vec![arg],
                                    ))))
                                }
                                _ => Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                                    this: arg,
                                    to: crate::expressions::DataType::Double {
                                        precision: None,
                                        scale: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))),
                            }
                        }
                        "AGGREGATE" if f.args.len() >= 3 => match target {
                            DialectType::DuckDB
                            | DialectType::Hive
                            | DialectType::Presto
                            | DialectType::Trino => Ok(Expression::Function(Box::new(
                                Function::new("REDUCE".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // REGEXP_MATCHES(x, y) -> RegexpLike for most targets, keep as-is for DuckDB
                        "REGEXP_MATCHES" if f.args.len() >= 2 => {
                            if matches!(target, DialectType::DuckDB) {
                                Ok(Expression::Function(f))
                            } else {
                                let mut args = f.args;
                                let this = args.remove(0);
                                let pattern = args.remove(0);
                                let flags = if args.is_empty() {
                                    None
                                } else {
                                    Some(args.remove(0))
                                };
                                Ok(Expression::RegexpLike(Box::new(
                                    crate::expressions::RegexpFunc {
                                        this,
                                        pattern,
                                        flags,
                                    },
                                )))
                            }
                        }
                        // REGEXP_FULL_MATCH (Hive REGEXP) -> RegexpLike
                        "REGEXP_FULL_MATCH" if f.args.len() >= 2 => {
                            if matches!(target, DialectType::DuckDB) {
                                Ok(Expression::Function(f))
                            } else {
                                let mut args = f.args;
                                let this = args.remove(0);
                                let pattern = args.remove(0);
                                let flags = if args.is_empty() {
                                    None
                                } else {
                                    Some(args.remove(0))
                                };
                                Ok(Expression::RegexpLike(Box::new(
                                    crate::expressions::RegexpFunc {
                                        this,
                                        pattern,
                                        flags,
                                    },
                                )))
                            }
                        }
                        // STRUCT_EXTRACT(x, 'field') -> x.field (StructExtract expression)
                        "STRUCT_EXTRACT" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            let field_expr = args.remove(0);
                            // Extract string literal to get field name
                            let field_name = match &field_expr {
                                Expression::Literal(lit)
                                    if matches!(
                                        lit.as_ref(),
                                        crate::expressions::Literal::String(_)
                                    ) =>
                                {
                                    let crate::expressions::Literal::String(s) = lit.as_ref()
                                    else {
                                        unreachable!()
                                    };
                                    s.clone()
                                }
                                Expression::Identifier(id) => id.name.clone(),
                                _ => {
                                    return Ok(Expression::Function(Box::new(Function::new(
                                        "STRUCT_EXTRACT".to_string(),
                                        vec![this, field_expr],
                                    ))))
                                }
                            };
                            Ok(Expression::StructExtract(Box::new(
                                crate::expressions::StructExtractFunc {
                                    this,
                                    field: crate::expressions::Identifier::new(field_name),
                                },
                            )))
                        }
                        // LIST_FILTER([4,5,6], x -> x > 4) -> FILTER(ARRAY(4,5,6), x -> x > 4)
                        "LIST_FILTER" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::DuckDB => "LIST_FILTER",
                                _ => "FILTER",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // LIST_TRANSFORM(x, y -> y + 1) -> TRANSFORM(x, y -> y + 1)
                        "LIST_TRANSFORM" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::DuckDB => "LIST_TRANSFORM",
                                _ => "TRANSFORM",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // LIST_SORT(x) -> LIST_SORT(x) for DuckDB, ARRAY_SORT(x) for Presto/Trino, SORT_ARRAY(x) for others
                        "LIST_SORT" if f.args.len() >= 1 => {
                            let name = match target {
                                DialectType::DuckDB => "LIST_SORT",
                                DialectType::Presto | DialectType::Trino => "ARRAY_SORT",
                                _ => "SORT_ARRAY",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // LIST_REVERSE_SORT(x) -> SORT_ARRAY(x, FALSE) for Spark/Hive, ARRAY_SORT(x, lambda) for Presto
                        "LIST_REVERSE_SORT" if f.args.len() >= 1 => {
                            match target {
                                DialectType::DuckDB => Ok(Expression::Function(Box::new(
                                    Function::new("ARRAY_REVERSE_SORT".to_string(), f.args),
                                ))),
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => {
                                    let mut args = f.args;
                                    args.push(Expression::Identifier(
                                        crate::expressions::Identifier::new("FALSE"),
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SORT_ARRAY".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)
                                    let arr = f.args.into_iter().next().unwrap();
                                    let lambda = Expression::Lambda(Box::new(
                                        crate::expressions::LambdaExpr {
                                            parameters: vec![
                                                crate::expressions::Identifier::new("a"),
                                                crate::expressions::Identifier::new("b"),
                                            ],
                                            body: Expression::Case(Box::new(Case {
                                                operand: None,
                                                whens: vec![
                                                    (
                                                        Expression::Lt(Box::new(BinaryOp::new(
                                                            Expression::Identifier(
                                                                crate::expressions::Identifier::new(
                                                                    "a",
                                                                ),
                                                            ),
                                                            Expression::Identifier(
                                                                crate::expressions::Identifier::new(
                                                                    "b",
                                                                ),
                                                            ),
                                                        ))),
                                                        Expression::number(1),
                                                    ),
                                                    (
                                                        Expression::Gt(Box::new(BinaryOp::new(
                                                            Expression::Identifier(
                                                                crate::expressions::Identifier::new(
                                                                    "a",
                                                                ),
                                                            ),
                                                            Expression::Identifier(
                                                                crate::expressions::Identifier::new(
                                                                    "b",
                                                                ),
                                                            ),
                                                        ))),
                                                        Expression::Literal(Box::new(
                                                            Literal::Number("-1".to_string()),
                                                        )),
                                                    ),
                                                ],
                                                else_: Some(Expression::number(0)),
                                                comments: Vec::new(),
                                                inferred_type: None,
                                            })),
                                            colon: false,
                                            parameter_types: Vec::new(),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "ARRAY_SORT".to_string(),
                                        vec![arr, lambda],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "LIST_REVERSE_SORT".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // SPLIT_TO_ARRAY(x) with 1 arg -> add default ',' separator and rename
                        "SPLIT_TO_ARRAY" if f.args.len() == 1 => {
                            let mut args = f.args;
                            args.push(Expression::string(","));
                            let name = match target {
                                DialectType::DuckDB => "STR_SPLIT",
                                DialectType::Presto | DialectType::Trino => "SPLIT",
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "SPLIT",
                                DialectType::PostgreSQL => "STRING_TO_ARRAY",
                                DialectType::Redshift => "SPLIT_TO_ARRAY",
                                _ => "SPLIT",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                args,
                            ))))
                        }
                        // SPLIT_TO_ARRAY(x, sep) with 2 args -> rename based on target
                        "SPLIT_TO_ARRAY" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::DuckDB => "STR_SPLIT",
                                DialectType::Presto | DialectType::Trino => "SPLIT",
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "SPLIT",
                                DialectType::PostgreSQL => "STRING_TO_ARRAY",
                                DialectType::Redshift => "SPLIT_TO_ARRAY",
                                _ => "SPLIT",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // STRING_TO_ARRAY/STR_SPLIT -> target-specific split function
                        "STRING_TO_ARRAY" | "STR_SPLIT" if f.args.len() >= 2 => {
                            let name = match target {
                                DialectType::DuckDB => "STR_SPLIT",
                                DialectType::Presto | DialectType::Trino => "SPLIT",
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "SPLIT",
                                DialectType::Doris | DialectType::StarRocks => "SPLIT_BY_STRING",
                                DialectType::TSQL | DialectType::Fabric
                                    if name == "STRING_TO_ARRAY" =>
                                {
                                    "STRING_TO_ARRAY"
                                }
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    "STRING_TO_ARRAY"
                                }
                                _ => "SPLIT",
                            };
                            // For Spark/Hive, SPLIT uses regex - need to escape literal with \Q...\E
                            if matches!(
                                target,
                                DialectType::Spark | DialectType::Databricks | DialectType::Hive
                            ) {
                                let mut args = f.args;
                                let x = args.remove(0);
                                let sep = args.remove(0);
                                // Wrap separator in CONCAT('\\Q', sep, '\\E')
                                let escaped_sep = Expression::Function(Box::new(Function::new(
                                    "CONCAT".to_string(),
                                    vec![Expression::string("\\Q"), sep, Expression::string("\\E")],
                                )));
                                Ok(Expression::Function(Box::new(Function::new(
                                    name.to_string(),
                                    vec![x, escaped_sep],
                                ))))
                            } else {
                                Ok(Expression::Function(Box::new(Function::new(
                                    name.to_string(),
                                    f.args,
                                ))))
                            }
                        }
                        // STR_SPLIT_REGEX(x, 'a') / REGEXP_SPLIT(x, 'a') -> target-specific regex split
                        "STR_SPLIT_REGEX" | "REGEXP_SPLIT" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::DuckDB => "STR_SPLIT_REGEX",
                                DialectType::Presto | DialectType::Trino => "REGEXP_SPLIT",
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "SPLIT",
                                _ => "REGEXP_SPLIT",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // SPLIT(str, delim) from Snowflake -> DuckDB with CASE wrapper
                        "SPLIT"
                            if f.args.len() == 2
                                && matches!(source, DialectType::Snowflake)
                                && matches!(target, DialectType::DuckDB) =>
                        {
                            let mut args = f.args;
                            let str_arg = args.remove(0);
                            let delim_arg = args.remove(0);

                            // STR_SPLIT(str, delim) as the base
                            let base_func = Expression::Function(Box::new(Function::new(
                                "STR_SPLIT".to_string(),
                                vec![str_arg.clone(), delim_arg.clone()],
                            )));

                            // [str] - array with single element
                            let array_with_input =
                                Expression::Array(Box::new(crate::expressions::Array {
                                    expressions: vec![str_arg],
                                }));

                            // CASE
                            //   WHEN delim IS NULL THEN NULL
                            //   WHEN delim = '' THEN [str]
                            //   ELSE STR_SPLIT(str, delim)
                            // END
                            Ok(Expression::Case(Box::new(Case {
                                operand: None,
                                whens: vec![
                                    (
                                        Expression::Is(Box::new(BinaryOp {
                                            left: delim_arg.clone(),
                                            right: Expression::Null(Null),
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        })),
                                        Expression::Null(Null),
                                    ),
                                    (
                                        Expression::Eq(Box::new(BinaryOp {
                                            left: delim_arg,
                                            right: Expression::string(""),
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        })),
                                        array_with_input,
                                    ),
                                ],
                                else_: Some(base_func),
                                comments: vec![],
                                inferred_type: None,
                            })))
                        }
                        // SPLIT(x, sep) from Presto/StarRocks/Doris -> target-specific split with regex escaping for Hive/Spark
                        "SPLIT"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::Presto
                                        | DialectType::Trino
                                        | DialectType::Athena
                                        | DialectType::StarRocks
                                        | DialectType::Doris
                                )
                                && matches!(
                                    target,
                                    DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Hive
                                ) =>
                        {
                            // Presto/StarRocks SPLIT is literal, Hive/Spark SPLIT is regex
                            let mut args = f.args;
                            let x = args.remove(0);
                            let sep = args.remove(0);
                            let escaped_sep = Expression::Function(Box::new(Function::new(
                                "CONCAT".to_string(),
                                vec![Expression::string("\\Q"), sep, Expression::string("\\E")],
                            )));
                            Ok(Expression::Function(Box::new(Function::new(
                                "SPLIT".to_string(),
                                vec![x, escaped_sep],
                            ))))
                        }
                        // SUBSTRINGINDEX -> SUBSTRING_INDEX (ClickHouse camelCase to standard)
                        // For ClickHouse target, preserve original name to maintain camelCase
                        "SUBSTRINGINDEX" => {
                            let name = if matches!(target, DialectType::ClickHouse) {
                                f.name.clone()
                            } else {
                                "SUBSTRING_INDEX".to_string()
                            };
                            Ok(Expression::Function(Box::new(Function::new(name, f.args))))
                        }
                        // ARRAY_LENGTH/SIZE/CARDINALITY -> target-specific array length function
                        "ARRAY_LENGTH" | "SIZE" | "CARDINALITY" => {
                            // DuckDB source CARDINALITY -> DuckDB target: keep as CARDINALITY (used for maps)
                            if name == "CARDINALITY"
                                && matches!(source, DialectType::DuckDB)
                                && matches!(target, DialectType::DuckDB)
                            {
                                return Ok(Expression::Function(f));
                            }
                            // Get the array argument (first arg, drop dimension args)
                            let mut args = f.args;
                            let arr = if args.is_empty() {
                                return Ok(Expression::Function(Box::new(Function::new(
                                    name.to_string(),
                                    args,
                                ))));
                            } else {
                                args.remove(0)
                            };
                            let name = match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "SIZE",
                                DialectType::Presto | DialectType::Trino => "CARDINALITY",
                                DialectType::BigQuery => "ARRAY_LENGTH",
                                DialectType::DuckDB => {
                                    // DuckDB: use ARRAY_LENGTH with all args
                                    let mut all_args = vec![arr];
                                    all_args.extend(args);
                                    return Ok(Expression::Function(Box::new(Function::new(
                                        "ARRAY_LENGTH".to_string(),
                                        all_args,
                                    ))));
                                }
                                DialectType::PostgreSQL
                                | DialectType::Redshift
                                | DialectType::TSQL
                                | DialectType::Fabric => {
                                    // Keep ARRAY_LENGTH with dimension args when there is
                                    // no safe target-specific array representation.
                                    let mut all_args = vec![arr];
                                    all_args.extend(args);
                                    return Ok(Expression::Function(Box::new(Function::new(
                                        "ARRAY_LENGTH".to_string(),
                                        all_args,
                                    ))));
                                }
                                DialectType::ClickHouse => "LENGTH",
                                _ => "ARRAY_LENGTH",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                vec![arr],
                            ))))
                        }
                        // TO_VARIANT(x) -> CAST(x AS VARIANT) for DuckDB
                        "TO_VARIANT" if f.args.len() == 1 => match target {
                            DialectType::DuckDB => {
                                let arg = f.args.into_iter().next().unwrap();
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: arg,
                                    to: DataType::Custom {
                                        name: "VARIANT".to_string(),
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // JSON_GROUP_ARRAY(x) -> JSON_AGG(x) for PostgreSQL
                        "JSON_GROUP_ARRAY" if f.args.len() == 1 => match target {
                            DialectType::PostgreSQL => Ok(Expression::Function(Box::new(
                                Function::new("JSON_AGG".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // JSON_GROUP_OBJECT(key, value) -> JSON_OBJECT_AGG(key, value) for PostgreSQL
                        "JSON_GROUP_OBJECT" if f.args.len() == 2 => match target {
                            DialectType::PostgreSQL => Ok(Expression::Function(Box::new(
                                Function::new("JSON_OBJECT_AGG".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // UNICODE(x) -> target-specific codepoint function
                        "UNICODE" if f.args.len() == 1 => {
                            match target {
                                DialectType::SQLite | DialectType::DuckDB => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "UNICODE".to_string(),
                                        f.args,
                                    ))))
                                }
                                DialectType::Oracle => {
                                    // ASCII(UNISTR(x))
                                    let inner = Expression::Function(Box::new(Function::new(
                                        "UNISTR".to_string(),
                                        f.args,
                                    )));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "ASCII".to_string(),
                                        vec![inner],
                                    ))))
                                }
                                DialectType::MySQL => {
                                    // ORD(CONVERT(x USING utf32))
                                    let arg = f.args.into_iter().next().unwrap();
                                    let convert_expr = Expression::ConvertToCharset(Box::new(
                                        crate::expressions::ConvertToCharset {
                                            this: Box::new(arg),
                                            dest: Some(Box::new(Expression::Identifier(
                                                crate::expressions::Identifier::new("utf32"),
                                            ))),
                                            source: None,
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "ORD".to_string(),
                                        vec![convert_expr],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "ASCII".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // XOR(a, b, ...) -> a XOR b XOR ... for MySQL, BITWISE_XOR for Presto/Trino, # for PostgreSQL, ^ for BigQuery
                        "XOR" if f.args.len() >= 2 => {
                            match target {
                                DialectType::ClickHouse => {
                                    // ClickHouse: keep as xor() function with lowercase name
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "xor".to_string(),
                                        f.args,
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino => {
                                    if f.args.len() == 2 {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "BITWISE_XOR".to_string(),
                                            f.args,
                                        ))))
                                    } else {
                                        // Nest: BITWISE_XOR(BITWISE_XOR(a, b), c)
                                        let mut args = f.args;
                                        let first = args.remove(0);
                                        let second = args.remove(0);
                                        let mut result =
                                            Expression::Function(Box::new(Function::new(
                                                "BITWISE_XOR".to_string(),
                                                vec![first, second],
                                            )));
                                        for arg in args {
                                            result = Expression::Function(Box::new(Function::new(
                                                "BITWISE_XOR".to_string(),
                                                vec![result, arg],
                                            )));
                                        }
                                        Ok(result)
                                    }
                                }
                                DialectType::MySQL
                                | DialectType::SingleStore
                                | DialectType::Doris
                                | DialectType::StarRocks => {
                                    // Convert XOR(a, b, c) -> Expression::Xor with expressions list
                                    let args = f.args;
                                    Ok(Expression::Xor(Box::new(crate::expressions::Xor {
                                        this: None,
                                        expression: None,
                                        expressions: args,
                                    })))
                                }
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    // PostgreSQL: a # b (hash operator for XOR)
                                    let mut args = f.args;
                                    let first = args.remove(0);
                                    let second = args.remove(0);
                                    let mut result = Expression::BitwiseXor(Box::new(
                                        BinaryOp::new(first, second),
                                    ));
                                    for arg in args {
                                        result = Expression::BitwiseXor(Box::new(BinaryOp::new(
                                            result, arg,
                                        )));
                                    }
                                    Ok(result)
                                }
                                DialectType::DuckDB => {
                                    // DuckDB: keep as XOR function (DuckDB ^ is Power, not XOR)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "XOR".to_string(),
                                        f.args,
                                    ))))
                                }
                                DialectType::BigQuery => {
                                    // BigQuery: a ^ b (caret operator for XOR)
                                    let mut args = f.args;
                                    let first = args.remove(0);
                                    let second = args.remove(0);
                                    let mut result = Expression::BitwiseXor(Box::new(
                                        BinaryOp::new(first, second),
                                    ));
                                    for arg in args {
                                        result = Expression::BitwiseXor(Box::new(BinaryOp::new(
                                            result, arg,
                                        )));
                                    }
                                    Ok(result)
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "XOR".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // ARRAY_REVERSE_SORT(x) -> SORT_ARRAY(x, FALSE) for Spark/Hive, ARRAY_SORT(x, lambda) for Presto
                        "ARRAY_REVERSE_SORT" if f.args.len() >= 1 => {
                            match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => {
                                    let mut args = f.args;
                                    args.push(Expression::Identifier(
                                        crate::expressions::Identifier::new("FALSE"),
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SORT_ARRAY".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // ARRAY_SORT(x, (a, b) -> CASE WHEN a < b THEN 1 WHEN a > b THEN -1 ELSE 0 END)
                                    let arr = f.args.into_iter().next().unwrap();
                                    let lambda = Expression::Lambda(Box::new(
                                        crate::expressions::LambdaExpr {
                                            parameters: vec![
                                                Identifier::new("a"),
                                                Identifier::new("b"),
                                            ],
                                            colon: false,
                                            parameter_types: Vec::new(),
                                            body: Expression::Case(Box::new(Case {
                                                operand: None,
                                                whens: vec![
                                                    (
                                                        Expression::Lt(Box::new(BinaryOp::new(
                                                            Expression::Identifier(
                                                                Identifier::new("a"),
                                                            ),
                                                            Expression::Identifier(
                                                                Identifier::new("b"),
                                                            ),
                                                        ))),
                                                        Expression::number(1),
                                                    ),
                                                    (
                                                        Expression::Gt(Box::new(BinaryOp::new(
                                                            Expression::Identifier(
                                                                Identifier::new("a"),
                                                            ),
                                                            Expression::Identifier(
                                                                Identifier::new("b"),
                                                            ),
                                                        ))),
                                                        Expression::Neg(Box::new(
                                                            crate::expressions::UnaryOp {
                                                                this: Expression::number(1),
                                                                inferred_type: None,
                                                            },
                                                        )),
                                                    ),
                                                ],
                                                else_: Some(Expression::number(0)),
                                                comments: Vec::new(),
                                                inferred_type: None,
                                            })),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "ARRAY_SORT".to_string(),
                                        vec![arr, lambda],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "ARRAY_REVERSE_SORT".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // ENCODE(x) -> ENCODE(x, 'utf-8') for Spark/Hive, TO_UTF8(x) for Presto
                        "ENCODE" if f.args.len() == 1 => match target {
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                let mut args = f.args;
                                args.push(Expression::string("utf-8"));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "ENCODE".to_string(),
                                    args,
                                ))))
                            }
                            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "TO_UTF8".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "ENCODE".to_string(),
                                f.args,
                            )))),
                        },
                        // DECODE(x) -> DECODE(x, 'utf-8') for Spark/Hive, FROM_UTF8(x) for Presto
                        "DECODE" if f.args.len() == 1 => match target {
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                let mut args = f.args;
                                args.push(Expression::string("utf-8"));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DECODE".to_string(),
                                    args,
                                ))))
                            }
                            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "FROM_UTF8".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "DECODE".to_string(),
                                f.args,
                            )))),
                        },
                        // QUANTILE(x, p) -> PERCENTILE(x, p) for Spark/Hive
                        "QUANTILE" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "PERCENTILE",
                                DialectType::Presto | DialectType::Trino => "APPROX_PERCENTILE",
                                DialectType::BigQuery => "PERCENTILE_CONT",
                                _ => "QUANTILE",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // QUANTILE_CONT(x, q) -> PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY x) for PostgreSQL/Snowflake
                        "QUANTILE_CONT" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let column = args.remove(0);
                            let quantile = args.remove(0);
                            match target {
                                DialectType::DuckDB => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "QUANTILE_CONT".to_string(),
                                        vec![column, quantile],
                                    ))))
                                }
                                DialectType::PostgreSQL
                                | DialectType::Redshift
                                | DialectType::Snowflake => {
                                    // PERCENTILE_CONT(q) WITHIN GROUP (ORDER BY x)
                                    let inner = Expression::PercentileCont(Box::new(
                                        crate::expressions::PercentileFunc {
                                            this: column.clone(),
                                            percentile: quantile,
                                            order_by: None,
                                            filter: None,
                                        },
                                    ));
                                    Ok(Expression::WithinGroup(Box::new(
                                        crate::expressions::WithinGroup {
                                            this: inner,
                                            order_by: vec![crate::expressions::Ordered {
                                                this: column,
                                                desc: false,
                                                nulls_first: None,
                                                explicit_asc: false,
                                                with_fill: None,
                                            }],
                                        },
                                    )))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "QUANTILE_CONT".to_string(),
                                    vec![column, quantile],
                                )))),
                            }
                        }
                        // QUANTILE_DISC(x, q) -> PERCENTILE_DISC(q) WITHIN GROUP (ORDER BY x) for PostgreSQL/Snowflake
                        "QUANTILE_DISC" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let column = args.remove(0);
                            let quantile = args.remove(0);
                            match target {
                                DialectType::DuckDB => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "QUANTILE_DISC".to_string(),
                                        vec![column, quantile],
                                    ))))
                                }
                                DialectType::PostgreSQL
                                | DialectType::Redshift
                                | DialectType::Snowflake => {
                                    // PERCENTILE_DISC(q) WITHIN GROUP (ORDER BY x)
                                    let inner = Expression::PercentileDisc(Box::new(
                                        crate::expressions::PercentileFunc {
                                            this: column.clone(),
                                            percentile: quantile,
                                            order_by: None,
                                            filter: None,
                                        },
                                    ));
                                    Ok(Expression::WithinGroup(Box::new(
                                        crate::expressions::WithinGroup {
                                            this: inner,
                                            order_by: vec![crate::expressions::Ordered {
                                                this: column,
                                                desc: false,
                                                nulls_first: None,
                                                explicit_asc: false,
                                                with_fill: None,
                                            }],
                                        },
                                    )))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "QUANTILE_DISC".to_string(),
                                    vec![column, quantile],
                                )))),
                            }
                        }
                        // PERCENTILE_APPROX(x, p) / APPROX_PERCENTILE(x, p) -> target-specific
                        "PERCENTILE_APPROX" | "APPROX_PERCENTILE" if f.args.len() >= 2 => {
                            let name = match target {
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    "APPROX_PERCENTILE"
                                }
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "PERCENTILE_APPROX",
                                DialectType::DuckDB => "APPROX_QUANTILE",
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    "PERCENTILE_CONT"
                                }
                                _ => &f.name,
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // EPOCH(x) -> UNIX_TIMESTAMP(x) for Spark/Hive
                        "EPOCH" if f.args.len() == 1 => {
                            let name = match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "UNIX_TIMESTAMP",
                                DialectType::Presto | DialectType::Trino => "TO_UNIXTIME",
                                _ => "EPOCH",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // EPOCH_MS(x) -> target-specific epoch milliseconds conversion
                        "EPOCH_MS" if f.args.len() == 1 => {
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TIMESTAMP_MILLIS".to_string(),
                                        f.args,
                                    ))))
                                }
                                DialectType::Hive => {
                                    // Hive: FROM_UNIXTIME(x / 1000)
                                    let arg = f.args.into_iter().next().unwrap();
                                    let div_expr = Expression::Div(Box::new(
                                        crate::expressions::BinaryOp::new(
                                            arg,
                                            Expression::number(1000),
                                        ),
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "FROM_UNIXTIME".to_string(),
                                        vec![div_expr],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "FROM_UNIXTIME".to_string(),
                                        vec![Expression::Div(Box::new(
                                            crate::expressions::BinaryOp::new(
                                                f.args.into_iter().next().unwrap(),
                                                Expression::number(1000),
                                            ),
                                        ))],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "EPOCH_MS".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // HASHBYTES('algorithm', x) -> target-specific hash function
                        "HASHBYTES" if f.args.len() == 2 => {
                            // Keep HASHBYTES as-is for TSQL target
                            if matches!(target, DialectType::TSQL) {
                                return Ok(Expression::Function(f));
                            }
                            let algo_expr = &f.args[0];
                            let algo = match algo_expr {
                                Expression::Literal(lit)
                                    if matches!(
                                        lit.as_ref(),
                                        crate::expressions::Literal::String(_)
                                    ) =>
                                {
                                    let crate::expressions::Literal::String(s) = lit.as_ref()
                                    else {
                                        unreachable!()
                                    };
                                    s.to_ascii_uppercase()
                                }
                                _ => return Ok(Expression::Function(f)),
                            };
                            let data_arg = f.args.into_iter().nth(1).unwrap();
                            match algo.as_str() {
                                "SHA1" => {
                                    let name = match target {
                                        DialectType::Spark | DialectType::Databricks => "SHA",
                                        DialectType::Hive => "SHA1",
                                        _ => "SHA1",
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        name.to_string(),
                                        vec![data_arg],
                                    ))))
                                }
                                "SHA2_256" => Ok(Expression::Function(Box::new(Function::new(
                                    "SHA2".to_string(),
                                    vec![data_arg, Expression::number(256)],
                                )))),
                                "SHA2_512" => Ok(Expression::Function(Box::new(Function::new(
                                    "SHA2".to_string(),
                                    vec![data_arg, Expression::number(512)],
                                )))),
                                "MD5" => Ok(Expression::Function(Box::new(Function::new(
                                    "MD5".to_string(),
                                    vec![data_arg],
                                )))),
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "HASHBYTES".to_string(),
                                    vec![Expression::string(&algo), data_arg],
                                )))),
                            }
                        }
                        // JSON_EXTRACT_PATH(json, key1, key2, ...) -> target-specific JSON extraction
                        "JSON_EXTRACT_PATH" | "JSON_EXTRACT_PATH_TEXT" if f.args.len() >= 2 => {
                            let is_text = name == "JSON_EXTRACT_PATH_TEXT";
                            let mut args = f.args;
                            let json_expr = args.remove(0);
                            // Build JSON path from remaining keys: $.key1.key2 or $.key1[0]
                            let mut json_path = "$".to_string();
                            for a in &args {
                                match a {
                                    Expression::Literal(lit)
                                        if matches!(
                                            lit.as_ref(),
                                            crate::expressions::Literal::String(_)
                                        ) =>
                                    {
                                        let crate::expressions::Literal::String(s) = lit.as_ref()
                                        else {
                                            unreachable!()
                                        };
                                        // Numeric string keys become array indices: [0]
                                        if s.chars().all(|c| c.is_ascii_digit()) {
                                            json_path.push('[');
                                            json_path.push_str(s);
                                            json_path.push(']');
                                        } else {
                                            json_path.push('.');
                                            json_path.push_str(s);
                                        }
                                    }
                                    _ => {
                                        json_path.push_str(".?");
                                    }
                                }
                            }
                            match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "GET_JSON_OBJECT".to_string(),
                                        vec![json_expr, Expression::string(&json_path)],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino => {
                                    let func_name = if is_text {
                                        "JSON_EXTRACT_SCALAR"
                                    } else {
                                        "JSON_EXTRACT"
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        vec![json_expr, Expression::string(&json_path)],
                                    ))))
                                }
                                DialectType::BigQuery | DialectType::MySQL => {
                                    let func_name = if is_text {
                                        "JSON_EXTRACT_SCALAR"
                                    } else {
                                        "JSON_EXTRACT"
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        vec![json_expr, Expression::string(&json_path)],
                                    ))))
                                }
                                DialectType::PostgreSQL | DialectType::Materialize => {
                                    // Keep as JSON_EXTRACT_PATH_TEXT / JSON_EXTRACT_PATH for PostgreSQL/Materialize
                                    let func_name = if is_text {
                                        "JSON_EXTRACT_PATH_TEXT"
                                    } else {
                                        "JSON_EXTRACT_PATH"
                                    };
                                    let mut new_args = vec![json_expr];
                                    new_args.extend(args);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        new_args,
                                    ))))
                                }
                                DialectType::DuckDB | DialectType::SQLite => {
                                    // Use -> for JSON_EXTRACT_PATH, ->> for JSON_EXTRACT_PATH_TEXT
                                    if is_text {
                                        Ok(Expression::JsonExtractScalar(Box::new(
                                            crate::expressions::JsonExtractFunc {
                                                this: json_expr,
                                                path: Expression::string(&json_path),
                                                returning: None,
                                                arrow_syntax: true,
                                                hash_arrow_syntax: false,
                                                wrapper_option: None,
                                                quotes_option: None,
                                                on_scalar_string: false,
                                                on_error: None,
                                            },
                                        )))
                                    } else {
                                        Ok(Expression::JsonExtract(Box::new(
                                            crate::expressions::JsonExtractFunc {
                                                this: json_expr,
                                                path: Expression::string(&json_path),
                                                returning: None,
                                                arrow_syntax: true,
                                                hash_arrow_syntax: false,
                                                wrapper_option: None,
                                                quotes_option: None,
                                                on_scalar_string: false,
                                                on_error: None,
                                            },
                                        )))
                                    }
                                }
                                DialectType::Redshift => {
                                    // Keep as JSON_EXTRACT_PATH_TEXT for Redshift
                                    let mut new_args = vec![json_expr];
                                    new_args.extend(args);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "JSON_EXTRACT_PATH_TEXT".to_string(),
                                        new_args,
                                    ))))
                                }
                                DialectType::TSQL | DialectType::Fabric => {
                                    // ISNULL(JSON_QUERY(json, '$.path'), JSON_VALUE(json, '$.path'))
                                    let jq = Expression::Function(Box::new(Function::new(
                                        "JSON_QUERY".to_string(),
                                        vec![json_expr.clone(), Expression::string(&json_path)],
                                    )));
                                    let jv = Expression::Function(Box::new(Function::new(
                                        "JSON_VALUE".to_string(),
                                        vec![json_expr, Expression::string(&json_path)],
                                    )));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "ISNULL".to_string(),
                                        vec![jq, jv],
                                    ))))
                                }
                                DialectType::ClickHouse => {
                                    let func_name = if is_text {
                                        "JSONExtractString"
                                    } else {
                                        "JSONExtractRaw"
                                    };
                                    let mut new_args = vec![json_expr];
                                    new_args.extend(args);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        new_args,
                                    ))))
                                }
                                _ => {
                                    let func_name = if is_text {
                                        "JSON_EXTRACT_SCALAR"
                                    } else {
                                        "JSON_EXTRACT"
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        vec![json_expr, Expression::string(&json_path)],
                                    ))))
                                }
                            }
                        }
                        // APPROX_DISTINCT(x) -> APPROX_COUNT_DISTINCT(x) for Spark/Hive/BigQuery
                        "APPROX_DISTINCT" if f.args.len() >= 1 => {
                            let name = match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive
                                | DialectType::BigQuery => "APPROX_COUNT_DISTINCT",
                                _ => "APPROX_DISTINCT",
                            };
                            let mut args = f.args;
                            // Hive doesn't support the accuracy parameter
                            if name == "APPROX_COUNT_DISTINCT"
                                && matches!(target, DialectType::Hive)
                            {
                                args.truncate(1);
                            }
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                args,
                            ))))
                        }
                        // REGEXP_EXTRACT(x, pattern) - normalize default group index
                        "REGEXP_EXTRACT" if f.args.len() == 2 => {
                            // Determine source default group index
                            let source_default = match source {
                                DialectType::Presto | DialectType::Trino | DialectType::DuckDB => 0,
                                _ => 1, // Hive/Spark/Databricks default = 1
                            };
                            // Determine target default group index
                            let target_default = match target {
                                DialectType::Presto
                                | DialectType::Trino
                                | DialectType::DuckDB
                                | DialectType::BigQuery => 0,
                                DialectType::Snowflake => {
                                    // Snowflake uses REGEXP_SUBSTR
                                    return Ok(Expression::Function(Box::new(Function::new(
                                        "REGEXP_SUBSTR".to_string(),
                                        f.args,
                                    ))));
                                }
                                _ => 1, // Hive/Spark/Databricks default = 1
                            };
                            if source_default != target_default {
                                let mut args = f.args;
                                args.push(Expression::number(source_default));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT".to_string(),
                                    args,
                                ))))
                            } else {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT".to_string(),
                                    f.args,
                                ))))
                            }
                        }
                        // RLIKE(str, pattern) -> RegexpLike expression (generates as target-specific form)
                        "RLIKE" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let str_expr = args.remove(0);
                            let pattern = args.remove(0);
                            match target {
                                DialectType::DuckDB => {
                                    // REGEXP_MATCHES(str, pattern)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "REGEXP_MATCHES".to_string(),
                                        vec![str_expr, pattern],
                                    ))))
                                }
                                _ => {
                                    // Convert to RegexpLike which generates as RLIKE/~/REGEXP_LIKE per dialect
                                    Ok(Expression::RegexpLike(Box::new(
                                        crate::expressions::RegexpFunc {
                                            this: str_expr,
                                            pattern,
                                            flags: None,
                                        },
                                    )))
                                }
                            }
                        }
                        // EOMONTH(date[, month_offset]) -> target-specific
                        "EOMONTH" if f.args.len() >= 1 => {
                            let mut args = f.args;
                            let date_arg = args.remove(0);
                            let month_offset = if !args.is_empty() {
                                Some(args.remove(0))
                            } else {
                                None
                            };

                            // Helper: wrap date in CAST to DATE
                            let cast_to_date = |e: Expression| -> Expression {
                                Expression::Cast(Box::new(Cast {
                                    this: e,
                                    to: DataType::Date,
                                    trailing_comments: vec![],
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))
                            };

                            match target {
                                DialectType::TSQL | DialectType::Fabric => {
                                    // TSQL: EOMONTH(CAST(date AS DATE)) or EOMONTH(DATEADD(MONTH, offset, CAST(date AS DATE)))
                                    let date = cast_to_date(date_arg);
                                    let date = if let Some(offset) = month_offset {
                                        Expression::Function(Box::new(Function::new(
                                            "DATEADD".to_string(),
                                            vec![
                                                Expression::Identifier(Identifier::new("MONTH")),
                                                offset,
                                                date,
                                            ],
                                        )))
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "EOMONTH".to_string(),
                                        vec![date],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // Presto: LAST_DAY_OF_MONTH(CAST(CAST(date AS TIMESTAMP) AS DATE))
                                    // or with offset: LAST_DAY_OF_MONTH(DATE_ADD('MONTH', offset, CAST(CAST(date AS TIMESTAMP) AS DATE)))
                                    let cast_ts = Expression::Cast(Box::new(Cast {
                                        this: date_arg,
                                        to: DataType::Timestamp {
                                            timezone: false,
                                            precision: None,
                                        },
                                        trailing_comments: vec![],
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }));
                                    let date = cast_to_date(cast_ts);
                                    let date = if let Some(offset) = month_offset {
                                        Expression::Function(Box::new(Function::new(
                                            "DATE_ADD".to_string(),
                                            vec![Expression::string("MONTH"), offset, date],
                                        )))
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY_OF_MONTH".to_string(),
                                        vec![date],
                                    ))))
                                }
                                DialectType::PostgreSQL => {
                                    // PostgreSQL: CAST(DATE_TRUNC('MONTH', CAST(date AS DATE) [+ INTERVAL 'offset MONTH']) + INTERVAL '1 MONTH' - INTERVAL '1 DAY' AS DATE)
                                    let date = cast_to_date(date_arg);
                                    let date = if let Some(offset) = month_offset {
                                        let interval_str =
                                            format!("{} MONTH", expr_to_string_static(&offset));
                                        Expression::Add(Box::new(
                                            crate::expressions::BinaryOp::new(
                                                date,
                                                Expression::Interval(Box::new(
                                                    crate::expressions::Interval {
                                                        this: Some(Expression::string(
                                                            &interval_str,
                                                        )),
                                                        unit: None,
                                                    },
                                                )),
                                            ),
                                        ))
                                    } else {
                                        date
                                    };
                                    let truncated = Expression::Function(Box::new(Function::new(
                                        "DATE_TRUNC".to_string(),
                                        vec![Expression::string("MONTH"), date],
                                    )));
                                    let plus_month = Expression::Add(Box::new(
                                        crate::expressions::BinaryOp::new(
                                            truncated,
                                            Expression::Interval(Box::new(
                                                crate::expressions::Interval {
                                                    this: Some(Expression::string("1 MONTH")),
                                                    unit: None,
                                                },
                                            )),
                                        ),
                                    ));
                                    let minus_day = Expression::Sub(Box::new(
                                        crate::expressions::BinaryOp::new(
                                            plus_month,
                                            Expression::Interval(Box::new(
                                                crate::expressions::Interval {
                                                    this: Some(Expression::string("1 DAY")),
                                                    unit: None,
                                                },
                                            )),
                                        ),
                                    ));
                                    Ok(Expression::Cast(Box::new(Cast {
                                        this: minus_day,
                                        to: DataType::Date,
                                        trailing_comments: vec![],
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::DuckDB => {
                                    // DuckDB: LAST_DAY(CAST(date AS DATE) [+ INTERVAL (offset) MONTH])
                                    let date = cast_to_date(date_arg);
                                    let date = if let Some(offset) = month_offset {
                                        // Wrap negative numbers in parentheses for DuckDB INTERVAL
                                        let interval_val = if matches!(&offset, Expression::Neg(_))
                                        {
                                            Expression::Paren(Box::new(crate::expressions::Paren {
                                                this: offset,
                                                trailing_comments: Vec::new(),
                                            }))
                                        } else {
                                            offset
                                        };
                                        Expression::Add(Box::new(crate::expressions::BinaryOp::new(
                                            date,
                                            Expression::Interval(Box::new(crate::expressions::Interval {
                                                this: Some(interval_val),
                                                unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Month,
                                                    use_plural: false,
                                                }),
                                            })),
                                        )))
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY".to_string(),
                                        vec![date],
                                    ))))
                                }
                                DialectType::Snowflake | DialectType::Redshift => {
                                    // Snowflake/Redshift: LAST_DAY(TO_DATE(date) or CAST(date AS DATE))
                                    // With offset: LAST_DAY(DATEADD(MONTH, offset, TO_DATE(date)))
                                    let date = if matches!(target, DialectType::Snowflake) {
                                        Expression::Function(Box::new(Function::new(
                                            "TO_DATE".to_string(),
                                            vec![date_arg],
                                        )))
                                    } else {
                                        cast_to_date(date_arg)
                                    };
                                    let date = if let Some(offset) = month_offset {
                                        Expression::Function(Box::new(Function::new(
                                            "DATEADD".to_string(),
                                            vec![
                                                Expression::Identifier(Identifier::new("MONTH")),
                                                offset,
                                                date,
                                            ],
                                        )))
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY".to_string(),
                                        vec![date],
                                    ))))
                                }
                                DialectType::Spark | DialectType::Databricks => {
                                    // Spark: LAST_DAY(TO_DATE(date))
                                    // With offset: LAST_DAY(ADD_MONTHS(TO_DATE(date), offset))
                                    let date = Expression::Function(Box::new(Function::new(
                                        "TO_DATE".to_string(),
                                        vec![date_arg],
                                    )));
                                    let date = if let Some(offset) = month_offset {
                                        Expression::Function(Box::new(Function::new(
                                            "ADD_MONTHS".to_string(),
                                            vec![date, offset],
                                        )))
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY".to_string(),
                                        vec![date],
                                    ))))
                                }
                                DialectType::MySQL => {
                                    // MySQL: LAST_DAY(DATE(date)) - no offset
                                    // With offset: LAST_DAY(DATE_ADD(date, INTERVAL offset MONTH)) - no DATE() wrapper
                                    let date = if let Some(offset) = month_offset {
                                        let iu = crate::expressions::IntervalUnit::Month;
                                        Expression::DateAdd(Box::new(
                                            crate::expressions::DateAddFunc {
                                                this: date_arg,
                                                interval: offset,
                                                unit: iu,
                                            },
                                        ))
                                    } else {
                                        Expression::Function(Box::new(Function::new(
                                            "DATE".to_string(),
                                            vec![date_arg],
                                        )))
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY".to_string(),
                                        vec![date],
                                    ))))
                                }
                                DialectType::BigQuery => {
                                    // BigQuery: LAST_DAY(CAST(date AS DATE))
                                    // With offset: LAST_DAY(DATE_ADD(CAST(date AS DATE), INTERVAL offset MONTH))
                                    let date = cast_to_date(date_arg);
                                    let date = if let Some(offset) = month_offset {
                                        let interval = Expression::Interval(Box::new(
                                            crate::expressions::Interval {
                                                this: Some(offset),
                                                unit: Some(
                                                    crate::expressions::IntervalUnitSpec::Simple {
                                                        unit:
                                                            crate::expressions::IntervalUnit::Month,
                                                        use_plural: false,
                                                    },
                                                ),
                                            },
                                        ));
                                        Expression::Function(Box::new(Function::new(
                                            "DATE_ADD".to_string(),
                                            vec![date, interval],
                                        )))
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY".to_string(),
                                        vec![date],
                                    ))))
                                }
                                DialectType::ClickHouse => {
                                    // ClickHouse: LAST_DAY(CAST(date AS Nullable(DATE)))
                                    let date = Expression::Cast(Box::new(Cast {
                                        this: date_arg,
                                        to: DataType::Nullable {
                                            inner: Box::new(DataType::Date),
                                        },
                                        trailing_comments: vec![],
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }));
                                    let date = if let Some(offset) = month_offset {
                                        Expression::Function(Box::new(Function::new(
                                            "DATE_ADD".to_string(),
                                            vec![
                                                Expression::Identifier(Identifier::new("MONTH")),
                                                offset,
                                                date,
                                            ],
                                        )))
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY".to_string(),
                                        vec![date],
                                    ))))
                                }
                                DialectType::Hive => {
                                    // Hive: LAST_DAY(date)
                                    let date = if let Some(offset) = month_offset {
                                        Expression::Function(Box::new(Function::new(
                                            "ADD_MONTHS".to_string(),
                                            vec![date_arg, offset],
                                        )))
                                    } else {
                                        date_arg
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY".to_string(),
                                        vec![date],
                                    ))))
                                }
                                _ => {
                                    // Default: LAST_DAY(date)
                                    let date = if let Some(offset) = month_offset {
                                        let unit = Expression::Identifier(Identifier::new("MONTH"));
                                        Expression::Function(Box::new(Function::new(
                                            "DATEADD".to_string(),
                                            vec![unit, offset, date_arg],
                                        )))
                                    } else {
                                        date_arg
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY".to_string(),
                                        vec![date],
                                    ))))
                                }
                            }
                        }
                        // LAST_DAY(x) / LAST_DAY_OF_MONTH(x) -> target-specific
                        "LAST_DAY" | "LAST_DAY_OF_MONTH"
                            if !matches!(source, DialectType::BigQuery) && f.args.len() >= 1 =>
                        {
                            let first_arg = f.args.into_iter().next().unwrap();
                            match target {
                                DialectType::TSQL | DialectType::Fabric => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "EOMONTH".to_string(),
                                        vec![first_arg],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LAST_DAY_OF_MONTH".to_string(),
                                        vec![first_arg],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "LAST_DAY".to_string(),
                                    vec![first_arg],
                                )))),
                            }
                        }
                        // BigQuery PARSE_DATETIME(format, value) -> target-specific parsing calls.
                        "PARSE_DATETIME"
                            if matches!(source, DialectType::BigQuery) && f.args.len() == 2 =>
                        {
                            fn expand_bigquery_datetime_format(expr: Expression) -> Expression {
                                match expr {
                                    Expression::Literal(lit) => match lit.as_ref() {
                                        Literal::String(s) => Expression::string(
                                            s.replace("%F", "%Y-%m-%d").replace("%T", "%H:%M:%S"),
                                        ),
                                        _ => Expression::Literal(lit),
                                    },
                                    other => other,
                                }
                            }

                            let mut args = f.args;
                            let format = expand_bigquery_datetime_format(args.remove(0));
                            let value = args.remove(0);
                            match target {
                                DialectType::DuckDB => {
                                    let value_with_year = Expression::Concat(Box::new(
                                        crate::expressions::BinaryOp::new(
                                            Expression::string("1970 "),
                                            value,
                                        ),
                                    ));
                                    let format_with_year = Expression::Concat(Box::new(
                                        crate::expressions::BinaryOp::new(
                                            Expression::string("%Y "),
                                            format,
                                        ),
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "STRPTIME".to_string(),
                                        vec![value_with_year, format_with_year],
                                    ))))
                                }
                                DialectType::Snowflake => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "PARSE_DATETIME".to_string(),
                                        vec![value, format],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "PARSE_DATETIME".to_string(),
                                    vec![format, value],
                                )))),
                            }
                        }
                        // Presto/Trino ISO-8601 helpers become casts outside Presto-family targets.
                        "FROM_ISO8601_TIMESTAMP"
                            if matches!(
                                source,
                                DialectType::Presto | DialectType::Trino | DialectType::Athena
                            ) && f.args.len() == 1
                                && !matches!(
                                    target,
                                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                                ) =>
                        {
                            Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                                this: f.args.into_iter().next().unwrap(),
                                to: DataType::Timestamp {
                                    precision: None,
                                    timezone: matches!(
                                        target,
                                        DialectType::DuckDB | DialectType::Snowflake
                                    ),
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        "FROM_ISO8601_DATE"
                            if matches!(
                                source,
                                DialectType::Presto | DialectType::Trino | DialectType::Athena
                            ) && f.args.len() == 1
                                && !matches!(
                                    target,
                                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                                ) =>
                        {
                            Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                                this: f.args.into_iter().next().unwrap(),
                                to: DataType::Date,
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        // MAP(keys_array, vals_array) from Presto (2-arg form) -> target-specific
                        "MAP"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                                ) =>
                        {
                            let keys_arg = f.args[0].clone();
                            let vals_arg = f.args[1].clone();

                            // Helper: extract array elements from Array/ArrayFunc/Function("ARRAY") expressions
                            fn extract_array_elements(
                                expr: &Expression,
                            ) -> Option<&Vec<Expression>> {
                                match expr {
                                    Expression::Array(arr) => Some(&arr.expressions),
                                    Expression::ArrayFunc(arr) => Some(&arr.expressions),
                                    Expression::Function(f)
                                        if f.name.eq_ignore_ascii_case("ARRAY") =>
                                    {
                                        Some(&f.args)
                                    }
                                    _ => None,
                                }
                            }

                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    // Presto MAP(keys, vals) -> Spark MAP_FROM_ARRAYS(keys, vals)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "MAP_FROM_ARRAYS".to_string(),
                                        f.args,
                                    ))))
                                }
                                DialectType::Hive => {
                                    // Presto MAP(ARRAY[k1,k2], ARRAY[v1,v2]) -> Hive MAP(k1, v1, k2, v2)
                                    if let (Some(keys), Some(vals)) = (
                                        extract_array_elements(&keys_arg),
                                        extract_array_elements(&vals_arg),
                                    ) {
                                        if keys.len() == vals.len() {
                                            let mut interleaved = Vec::new();
                                            for (k, v) in keys.iter().zip(vals.iter()) {
                                                interleaved.push(k.clone());
                                                interleaved.push(v.clone());
                                            }
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "MAP".to_string(),
                                                interleaved,
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "MAP".to_string(),
                                                f.args,
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "MAP".to_string(),
                                            f.args,
                                        ))))
                                    }
                                }
                                DialectType::Snowflake => {
                                    // Presto MAP(ARRAY[k1,k2], ARRAY[v1,v2]) -> Snowflake OBJECT_CONSTRUCT(k1, v1, k2, v2)
                                    if let (Some(keys), Some(vals)) = (
                                        extract_array_elements(&keys_arg),
                                        extract_array_elements(&vals_arg),
                                    ) {
                                        if keys.len() == vals.len() {
                                            let mut interleaved = Vec::new();
                                            for (k, v) in keys.iter().zip(vals.iter()) {
                                                interleaved.push(k.clone());
                                                interleaved.push(v.clone());
                                            }
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "OBJECT_CONSTRUCT".to_string(),
                                                interleaved,
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "MAP".to_string(),
                                                f.args,
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "MAP".to_string(),
                                            f.args,
                                        ))))
                                    }
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // MAP() with 0 args from Spark -> MAP(ARRAY[], ARRAY[]) for Presto/Trino
                        "MAP"
                            if f.args.is_empty()
                                && matches!(
                                    source,
                                    DialectType::Hive
                                        | DialectType::Spark
                                        | DialectType::Databricks
                                )
                                && matches!(
                                    target,
                                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                                ) =>
                        {
                            let empty_keys =
                                Expression::Array(Box::new(crate::expressions::Array {
                                    expressions: vec![],
                                }));
                            let empty_vals =
                                Expression::Array(Box::new(crate::expressions::Array {
                                    expressions: vec![],
                                }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "MAP".to_string(),
                                vec![empty_keys, empty_vals],
                            ))))
                        }
                        // MAP(k1, v1, k2, v2, ...) from Hive/Spark -> target-specific
                        "MAP"
                            if f.args.len() >= 2
                                && f.args.len() % 2 == 0
                                && matches!(
                                    source,
                                    DialectType::Hive
                                        | DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::ClickHouse
                                        | DialectType::StarRocks
                                ) =>
                        {
                            let args = f.args;
                            match target {
                                DialectType::DuckDB => {
                                    // MAP([k1, k2], [v1, v2])
                                    let mut keys = Vec::new();
                                    let mut vals = Vec::new();
                                    for (i, arg) in args.into_iter().enumerate() {
                                        if i % 2 == 0 {
                                            keys.push(arg);
                                        } else {
                                            vals.push(arg);
                                        }
                                    }
                                    let keys_arr =
                                        Expression::Array(Box::new(crate::expressions::Array {
                                            expressions: keys,
                                        }));
                                    let vals_arr =
                                        Expression::Array(Box::new(crate::expressions::Array {
                                            expressions: vals,
                                        }));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "MAP".to_string(),
                                        vec![keys_arr, vals_arr],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino => {
                                    // MAP(ARRAY[k1, k2], ARRAY[v1, v2])
                                    let mut keys = Vec::new();
                                    let mut vals = Vec::new();
                                    for (i, arg) in args.into_iter().enumerate() {
                                        if i % 2 == 0 {
                                            keys.push(arg);
                                        } else {
                                            vals.push(arg);
                                        }
                                    }
                                    let keys_arr =
                                        Expression::Array(Box::new(crate::expressions::Array {
                                            expressions: keys,
                                        }));
                                    let vals_arr =
                                        Expression::Array(Box::new(crate::expressions::Array {
                                            expressions: vals,
                                        }));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "MAP".to_string(),
                                        vec![keys_arr, vals_arr],
                                    ))))
                                }
                                DialectType::Snowflake => Ok(Expression::Function(Box::new(
                                    Function::new("OBJECT_CONSTRUCT".to_string(), args),
                                ))),
                                DialectType::ClickHouse => Ok(Expression::Function(Box::new(
                                    Function::new("map".to_string(), args),
                                ))),
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "MAP".to_string(),
                                    args,
                                )))),
                            }
                        }
                        // COLLECT_LIST(x) -> ARRAY_AGG(x) for most targets
                        "COLLECT_LIST" if f.args.len() >= 1 => {
                            let name = match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "COLLECT_LIST",
                                DialectType::DuckDB
                                | DialectType::PostgreSQL
                                | DialectType::Redshift
                                | DialectType::Snowflake
                                | DialectType::BigQuery => "ARRAY_AGG",
                                DialectType::Presto | DialectType::Trino => "ARRAY_AGG",
                                _ => "ARRAY_AGG",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // COLLECT_SET(x) -> target-specific distinct array aggregation
                        "COLLECT_SET" if f.args.len() >= 1 => {
                            let name = match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "COLLECT_SET",
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    "SET_AGG"
                                }
                                DialectType::Snowflake => "ARRAY_UNIQUE_AGG",
                                _ => "ARRAY_AGG",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // ISNAN(x) / IS_NAN(x) - normalize
                        "ISNAN" | "IS_NAN" => {
                            let name = match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "ISNAN",
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    "IS_NAN"
                                }
                                DialectType::BigQuery
                                | DialectType::PostgreSQL
                                | DialectType::Redshift => "IS_NAN",
                                DialectType::ClickHouse => "IS_NAN",
                                _ => "ISNAN",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // SPLIT_PART(str, delim, index) -> target-specific
                        "SPLIT_PART" if f.args.len() == 3 => {
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    // Keep as SPLIT_PART (Spark 3.4+)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SPLIT_PART".to_string(),
                                        f.args,
                                    ))))
                                }
                                DialectType::DuckDB if matches!(source, DialectType::Snowflake) => {
                                    // Snowflake SPLIT_PART -> DuckDB with CASE wrapper:
                                    // - part_index 0 treated as 1
                                    // - empty delimiter: return whole string if index 1 or -1, else ''
                                    let mut args = f.args;
                                    let str_arg = args.remove(0);
                                    let delim_arg = args.remove(0);
                                    let idx_arg = args.remove(0);

                                    // (CASE WHEN idx = 0 THEN 1 ELSE idx END)
                                    let adjusted_idx = Expression::Paren(Box::new(Paren {
                                        this: Expression::Case(Box::new(Case {
                                            operand: None,
                                            whens: vec![(
                                                Expression::Eq(Box::new(BinaryOp {
                                                    left: idx_arg.clone(),
                                                    right: Expression::number(0),
                                                    left_comments: vec![],
                                                    operator_comments: vec![],
                                                    trailing_comments: vec![],
                                                    inferred_type: None,
                                                })),
                                                Expression::number(1),
                                            )],
                                            else_: Some(idx_arg.clone()),
                                            comments: vec![],
                                            inferred_type: None,
                                        })),
                                        trailing_comments: vec![],
                                    }));

                                    // SPLIT_PART(str, delim, adjusted_idx)
                                    let base_func = Expression::Function(Box::new(Function::new(
                                        "SPLIT_PART".to_string(),
                                        vec![
                                            str_arg.clone(),
                                            delim_arg.clone(),
                                            adjusted_idx.clone(),
                                        ],
                                    )));

                                    // (CASE WHEN adjusted_idx = 1 OR adjusted_idx = -1 THEN str ELSE '' END)
                                    let empty_delim_case = Expression::Paren(Box::new(Paren {
                                        this: Expression::Case(Box::new(Case {
                                            operand: None,
                                            whens: vec![(
                                                Expression::Or(Box::new(BinaryOp {
                                                    left: Expression::Eq(Box::new(BinaryOp {
                                                        left: adjusted_idx.clone(),
                                                        right: Expression::number(1),
                                                        left_comments: vec![],
                                                        operator_comments: vec![],
                                                        trailing_comments: vec![],
                                                        inferred_type: None,
                                                    })),
                                                    right: Expression::Eq(Box::new(BinaryOp {
                                                        left: adjusted_idx,
                                                        right: Expression::number(-1),
                                                        left_comments: vec![],
                                                        operator_comments: vec![],
                                                        trailing_comments: vec![],
                                                        inferred_type: None,
                                                    })),
                                                    left_comments: vec![],
                                                    operator_comments: vec![],
                                                    trailing_comments: vec![],
                                                    inferred_type: None,
                                                })),
                                                str_arg,
                                            )],
                                            else_: Some(Expression::string("")),
                                            comments: vec![],
                                            inferred_type: None,
                                        })),
                                        trailing_comments: vec![],
                                    }));

                                    // CASE WHEN delim = '' THEN (empty case) ELSE SPLIT_PART(...) END
                                    Ok(Expression::Case(Box::new(Case {
                                        operand: None,
                                        whens: vec![(
                                            Expression::Eq(Box::new(BinaryOp {
                                                left: delim_arg,
                                                right: Expression::string(""),
                                                left_comments: vec![],
                                                operator_comments: vec![],
                                                trailing_comments: vec![],
                                                inferred_type: None,
                                            })),
                                            empty_delim_case,
                                        )],
                                        else_: Some(base_func),
                                        comments: vec![],
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::DuckDB
                                | DialectType::PostgreSQL
                                | DialectType::Snowflake
                                | DialectType::Redshift
                                | DialectType::Trino
                                | DialectType::Presto => Ok(Expression::Function(Box::new(
                                    Function::new("SPLIT_PART".to_string(), f.args),
                                ))),
                                DialectType::Hive => {
                                    // SPLIT(str, delim)[index]
                                    // Complex conversion, just keep as-is for now
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SPLIT_PART".to_string(),
                                        f.args,
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "SPLIT_PART".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // JSON_EXTRACT(json, path) -> target-specific JSON extraction
                        "JSON_EXTRACT" | "JSON_EXTRACT_SCALAR" if f.args.len() == 2 => {
                            let is_scalar = name == "JSON_EXTRACT_SCALAR";
                            match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => {
                                    let mut args = f.args;
                                    // Spark/Hive don't support Presto's TRY(expr) wrapper form here.
                                    // Mirror sqlglot by unwrapping TRY(expr) to expr before GET_JSON_OBJECT.
                                    if let Some(Expression::Function(inner)) = args.first() {
                                        if inner.name.eq_ignore_ascii_case("TRY")
                                            && inner.args.len() == 1
                                        {
                                            let mut inner_args = inner.args.clone();
                                            args[0] = inner_args.remove(0);
                                        }
                                    }
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "GET_JSON_OBJECT".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::DuckDB | DialectType::SQLite => {
                                    // json -> path syntax
                                    let mut args = f.args;
                                    let json_expr = args.remove(0);
                                    let path = args.remove(0);
                                    Ok(Expression::JsonExtract(Box::new(
                                        crate::expressions::JsonExtractFunc {
                                            this: json_expr,
                                            path,
                                            returning: None,
                                            arrow_syntax: true,
                                            hash_arrow_syntax: false,
                                            wrapper_option: None,
                                            quotes_option: None,
                                            on_scalar_string: false,
                                            on_error: None,
                                        },
                                    )))
                                }
                                DialectType::TSQL => {
                                    let func_name = if is_scalar {
                                        "JSON_VALUE"
                                    } else {
                                        "JSON_QUERY"
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        f.args,
                                    ))))
                                }
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    let func_name = if is_scalar {
                                        "JSON_EXTRACT_PATH_TEXT"
                                    } else {
                                        "JSON_EXTRACT_PATH"
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        f.args,
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    name.to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // MySQL JSON_SEARCH(json_doc, mode, search[, escape_char[, path]]) -> DuckDB json_tree-based lookup
                        "JSON_SEARCH"
                            if matches!(target, DialectType::DuckDB)
                                && (3..=5).contains(&f.args.len()) =>
                        {
                            let args = &f.args;

                            // Only rewrite deterministic modes and NULL/no escape-char variant.
                            let mode = match &args[1] {
                                Expression::Literal(lit)
                                    if matches!(
                                        lit.as_ref(),
                                        crate::expressions::Literal::String(_)
                                    ) =>
                                {
                                    let crate::expressions::Literal::String(s) = lit.as_ref()
                                    else {
                                        unreachable!()
                                    };
                                    s.to_ascii_lowercase()
                                }
                                _ => return Ok(Expression::Function(f)),
                            };
                            if mode != "one" && mode != "all" {
                                return Ok(Expression::Function(f));
                            }
                            if args.len() >= 4 && !matches!(&args[3], Expression::Null(_)) {
                                return Ok(Expression::Function(f));
                            }

                            let json_doc_sql = match Generator::sql(&args[0]) {
                                Ok(sql) => sql,
                                Err(_) => return Ok(Expression::Function(f)),
                            };
                            let search_sql = match Generator::sql(&args[2]) {
                                Ok(sql) => sql,
                                Err(_) => return Ok(Expression::Function(f)),
                            };
                            let path_sql = if args.len() == 5 {
                                match Generator::sql(&args[4]) {
                                    Ok(sql) => sql,
                                    Err(_) => return Ok(Expression::Function(f)),
                                }
                            } else {
                                "'$'".to_string()
                            };

                            let rewrite_sql = if mode == "all" {
                                format!(
                                    "(SELECT TO_JSON(LIST(__jt.fullkey)) FROM json_tree({}, {}) AS __jt WHERE __jt.atom = TO_JSON({}))",
                                    json_doc_sql, path_sql, search_sql
                                )
                            } else {
                                format!(
                                    "(SELECT TO_JSON(__jt.fullkey) FROM json_tree({}, {}) AS __jt WHERE __jt.atom = TO_JSON({}) ORDER BY __jt.id LIMIT 1)",
                                    json_doc_sql, path_sql, search_sql
                                )
                            };

                            Ok(Expression::Raw(crate::expressions::Raw {
                                sql: rewrite_sql,
                            }))
                        }
                        // SingleStore JSON_EXTRACT_JSON(json, key1, key2, ...) -> JSON_EXTRACT(json, '$.key1.key2' or '$.key1[key2]')
                        // BSON_EXTRACT_BSON(json, key1, ...) -> JSONB_EXTRACT(json, '$.key1')
                        "JSON_EXTRACT_JSON" | "BSON_EXTRACT_BSON"
                            if f.args.len() >= 2 && matches!(source, DialectType::SingleStore) =>
                        {
                            let is_bson = name == "BSON_EXTRACT_BSON";
                            let mut args = f.args;
                            let json_expr = args.remove(0);

                            // Build JSONPath from remaining arguments
                            let mut path = String::from("$");
                            for arg in &args {
                                if let Expression::Literal(lit) = arg {
                                    if let crate::expressions::Literal::String(s) = lit.as_ref() {
                                        // Check if it's a numeric string (array index)
                                        if s.parse::<i64>().is_ok() {
                                            path.push('[');
                                            path.push_str(s);
                                            path.push(']');
                                        } else {
                                            path.push('.');
                                            path.push_str(s);
                                        }
                                    }
                                }
                            }

                            let target_func = if is_bson {
                                "JSONB_EXTRACT"
                            } else {
                                "JSON_EXTRACT"
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                target_func.to_string(),
                                vec![json_expr, Expression::string(&path)],
                            ))))
                        }
                        // ARRAY_SUM(lambda, array) from Doris -> ClickHouse arraySum
                        "ARRAY_SUM" if matches!(target, DialectType::ClickHouse) => {
                            Ok(Expression::Function(Box::new(Function {
                                name: "arraySum".to_string(),
                                args: f.args,
                                distinct: f.distinct,
                                trailing_comments: f.trailing_comments,
                                use_bracket_syntax: f.use_bracket_syntax,
                                no_parens: f.no_parens,
                                quoted: f.quoted,
                                span: None,
                                inferred_type: None,
                            })))
                        }
                        // TSQL JSON_QUERY/JSON_VALUE -> target-specific
                        // Note: For TSQL->TSQL, JsonQuery stays as Expression::JsonQuery (source transform not called)
                        // and is handled by JsonQueryValueConvert action. This handles the case where
                        // TSQL read transform converted JsonQuery to Function("JSON_QUERY") for cross-dialect.
                        "JSON_QUERY" | "JSON_VALUE"
                            if f.args.len() == 2
                                && matches!(source, DialectType::TSQL | DialectType::Fabric) =>
                        {
                            match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => Ok(Expression::Function(Box::new(
                                    Function::new("GET_JSON_OBJECT".to_string(), f.args),
                                ))),
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    name.to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // UNIX_TIMESTAMP(x) -> TO_UNIXTIME(x) for Presto
                        "UNIX_TIMESTAMP" if f.args.len() == 1 => {
                            let arg = f.args.into_iter().next().unwrap();
                            let is_hive_source = matches!(
                                source,
                                DialectType::Hive | DialectType::Spark | DialectType::Databricks
                            );
                            match target {
                                DialectType::DuckDB if is_hive_source => {
                                    // DuckDB: EPOCH(STRPTIME(x, '%Y-%m-%d %H:%M:%S'))
                                    let strptime = Expression::Function(Box::new(Function::new(
                                        "STRPTIME".to_string(),
                                        vec![arg, Expression::string("%Y-%m-%d %H:%M:%S")],
                                    )));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "EPOCH".to_string(),
                                        vec![strptime],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino if is_hive_source => {
                                    // Presto: TO_UNIXTIME(COALESCE(TRY(DATE_PARSE(CAST(x AS VARCHAR), '%Y-%m-%d %T')), PARSE_DATETIME(DATE_FORMAT(x, '%Y-%m-%d %T'), 'yyyy-MM-dd HH:mm:ss')))
                                    let cast_varchar =
                                        Expression::Cast(Box::new(crate::expressions::Cast {
                                            this: arg.clone(),
                                            to: DataType::VarChar {
                                                length: None,
                                                parenthesized_length: false,
                                            },
                                            trailing_comments: vec![],
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }));
                                    let date_parse = Expression::Function(Box::new(Function::new(
                                        "DATE_PARSE".to_string(),
                                        vec![cast_varchar, Expression::string("%Y-%m-%d %T")],
                                    )));
                                    let try_expr = Expression::Function(Box::new(Function::new(
                                        "TRY".to_string(),
                                        vec![date_parse],
                                    )));
                                    let date_format =
                                        Expression::Function(Box::new(Function::new(
                                            "DATE_FORMAT".to_string(),
                                            vec![arg, Expression::string("%Y-%m-%d %T")],
                                        )));
                                    let parse_datetime =
                                        Expression::Function(Box::new(Function::new(
                                            "PARSE_DATETIME".to_string(),
                                            vec![
                                                date_format,
                                                Expression::string("yyyy-MM-dd HH:mm:ss"),
                                            ],
                                        )));
                                    let coalesce = Expression::Function(Box::new(Function::new(
                                        "COALESCE".to_string(),
                                        vec![try_expr, parse_datetime],
                                    )));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_UNIXTIME".to_string(),
                                        vec![coalesce],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_UNIXTIME".to_string(),
                                        vec![arg],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "UNIX_TIMESTAMP".to_string(),
                                    vec![arg],
                                )))),
                            }
                        }
                        // TO_UNIX_TIMESTAMP(x) -> UNIX_TIMESTAMP(x) for Spark/Hive
                        "TO_UNIX_TIMESTAMP" if f.args.len() >= 1 => match target {
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "UNIX_TIMESTAMP".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "TO_UNIX_TIMESTAMP".to_string(),
                                f.args,
                            )))),
                        },
                        // CURDATE() -> CURRENT_DATE
                        "CURDATE" => Ok(Expression::CurrentDate(crate::expressions::CurrentDate)),
                        // CURTIME() -> CURRENT_TIME
                        "CURTIME" => Ok(Expression::CurrentTime(crate::expressions::CurrentTime {
                            precision: None,
                        })),
                        // ARRAY_SORT(x) or ARRAY_SORT(x, lambda) -> SORT_ARRAY(x) for Hive, LIST_SORT for DuckDB
                        "ARRAY_SORT" if f.args.len() >= 1 => {
                            match target {
                                DialectType::Hive => {
                                    let mut args = f.args;
                                    args.truncate(1); // Drop lambda comparator
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SORT_ARRAY".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::DuckDB if matches!(source, DialectType::Snowflake) => {
                                    // Snowflake ARRAY_SORT(arr[, asc_bool[, nulls_first_bool]]) -> DuckDB LIST_SORT(arr[, 'ASC'/'DESC'[, 'NULLS FIRST']])
                                    let mut args_iter = f.args.into_iter();
                                    let arr = args_iter.next().unwrap();
                                    let asc_arg = args_iter.next();
                                    let nulls_first_arg = args_iter.next();

                                    let is_asc_bool = asc_arg
                                        .as_ref()
                                        .map(|a| matches!(a, Expression::Boolean(_)))
                                        .unwrap_or(false);
                                    let is_nf_bool = nulls_first_arg
                                        .as_ref()
                                        .map(|a| matches!(a, Expression::Boolean(_)))
                                        .unwrap_or(false);

                                    // No boolean args: pass through as-is
                                    if !is_asc_bool && !is_nf_bool {
                                        let mut result_args = vec![arr];
                                        if let Some(asc) = asc_arg {
                                            result_args.push(asc);
                                            if let Some(nf) = nulls_first_arg {
                                                result_args.push(nf);
                                            }
                                        }
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "LIST_SORT".to_string(),
                                            result_args,
                                        ))))
                                    } else {
                                        // Has boolean args: convert to DuckDB LIST_SORT format
                                        let descending = matches!(&asc_arg, Some(Expression::Boolean(b)) if !b.value);

                                        // Snowflake defaults: nulls_first = TRUE for DESC, FALSE for ASC
                                        let nulls_are_first = match &nulls_first_arg {
                                            Some(Expression::Boolean(b)) => b.value,
                                            None if is_asc_bool => descending, // Snowflake default
                                            _ => false,
                                        };
                                        let nulls_first_sql = if nulls_are_first {
                                            Some(Expression::string("NULLS FIRST"))
                                        } else {
                                            None
                                        };

                                        if !is_asc_bool {
                                            // asc is non-boolean expression, nulls_first is boolean
                                            let mut result_args = vec![arr];
                                            if let Some(asc) = asc_arg {
                                                result_args.push(asc);
                                            }
                                            if let Some(nf) = nulls_first_sql {
                                                result_args.push(nf);
                                            }
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "LIST_SORT".to_string(),
                                                result_args,
                                            ))))
                                        } else {
                                            if !descending && !nulls_are_first {
                                                // ASC, NULLS LAST (default) -> LIST_SORT(arr)
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "LIST_SORT".to_string(),
                                                    vec![arr],
                                                ))))
                                            } else if descending && !nulls_are_first {
                                                // DESC, NULLS LAST -> ARRAY_REVERSE_SORT(arr)
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "ARRAY_REVERSE_SORT".to_string(),
                                                    vec![arr],
                                                ))))
                                            } else {
                                                // NULLS FIRST -> LIST_SORT(arr, 'ASC'/'DESC', 'NULLS FIRST')
                                                let order_str =
                                                    if descending { "DESC" } else { "ASC" };
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "LIST_SORT".to_string(),
                                                    vec![
                                                        arr,
                                                        Expression::string(order_str),
                                                        Expression::string("NULLS FIRST"),
                                                    ],
                                                ))))
                                            }
                                        }
                                    }
                                }
                                DialectType::DuckDB => {
                                    // Non-Snowflake source: ARRAY_SORT(x, lambda) -> ARRAY_SORT(x) (drop comparator)
                                    let mut args = f.args;
                                    args.truncate(1); // Drop lambda comparator for DuckDB
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "ARRAY_SORT".to_string(),
                                        args,
                                    ))))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // SORT_ARRAY(x) -> LIST_SORT(x) for DuckDB, ARRAY_SORT(x) for Presto/Trino, keep for Hive/Spark
                        "SORT_ARRAY" if f.args.len() == 1 => match target {
                            DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                                Ok(Expression::Function(f))
                            }
                            DialectType::DuckDB => Ok(Expression::Function(Box::new(
                                Function::new("LIST_SORT".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY_SORT".to_string(),
                                f.args,
                            )))),
                        },
                        // SORT_ARRAY(x, FALSE) -> ARRAY_REVERSE_SORT(x) for DuckDB, ARRAY_SORT(x, lambda) for Presto
                        "SORT_ARRAY" if f.args.len() == 2 => {
                            let is_desc = matches!(&f.args[1], Expression::Boolean(b) if !b.value);
                            if is_desc {
                                match target {
                                    DialectType::DuckDB => {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "ARRAY_REVERSE_SORT".to_string(),
                                            vec![f.args.into_iter().next().unwrap()],
                                        ))))
                                    }
                                    DialectType::Presto | DialectType::Trino => {
                                        let arr_arg = f.args.into_iter().next().unwrap();
                                        let a = Expression::Column(Box::new(
                                            crate::expressions::Column {
                                                name: crate::expressions::Identifier::new("a"),
                                                table: None,
                                                join_mark: false,
                                                trailing_comments: Vec::new(),
                                                span: None,
                                                inferred_type: None,
                                            },
                                        ));
                                        let b = Expression::Column(Box::new(
                                            crate::expressions::Column {
                                                name: crate::expressions::Identifier::new("b"),
                                                table: None,
                                                join_mark: false,
                                                trailing_comments: Vec::new(),
                                                span: None,
                                                inferred_type: None,
                                            },
                                        ));
                                        let case_expr =
                                            Expression::Case(Box::new(crate::expressions::Case {
                                                operand: None,
                                                whens: vec![
                                                    (
                                                        Expression::Lt(Box::new(BinaryOp::new(
                                                            a.clone(),
                                                            b.clone(),
                                                        ))),
                                                        Expression::Literal(Box::new(
                                                            Literal::Number("1".to_string()),
                                                        )),
                                                    ),
                                                    (
                                                        Expression::Gt(Box::new(BinaryOp::new(
                                                            a.clone(),
                                                            b.clone(),
                                                        ))),
                                                        Expression::Literal(Box::new(
                                                            Literal::Number("-1".to_string()),
                                                        )),
                                                    ),
                                                ],
                                                else_: Some(Expression::Literal(Box::new(
                                                    Literal::Number("0".to_string()),
                                                ))),
                                                comments: Vec::new(),
                                                inferred_type: None,
                                            }));
                                        let lambda = Expression::Lambda(Box::new(
                                            crate::expressions::LambdaExpr {
                                                parameters: vec![
                                                    crate::expressions::Identifier::new("a"),
                                                    crate::expressions::Identifier::new("b"),
                                                ],
                                                body: case_expr,
                                                colon: false,
                                                parameter_types: Vec::new(),
                                            },
                                        ));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "ARRAY_SORT".to_string(),
                                            vec![arr_arg, lambda],
                                        ))))
                                    }
                                    _ => Ok(Expression::Function(f)),
                                }
                            } else {
                                // SORT_ARRAY(x, TRUE) -> LIST_SORT(x) for DuckDB, ARRAY_SORT(x) for others
                                match target {
                                    DialectType::Hive => Ok(Expression::Function(f)),
                                    DialectType::DuckDB => {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "LIST_SORT".to_string(),
                                            vec![f.args.into_iter().next().unwrap()],
                                        ))))
                                    }
                                    _ => Ok(Expression::Function(Box::new(Function::new(
                                        "ARRAY_SORT".to_string(),
                                        vec![f.args.into_iter().next().unwrap()],
                                    )))),
                                }
                            }
                        }
                        // LEFT(x, n), RIGHT(x, n) -> SUBSTRING for targets without LEFT/RIGHT
                        "LEFT" if f.args.len() == 2 => {
                            match target {
                                DialectType::Hive
                                | DialectType::Presto
                                | DialectType::Trino
                                | DialectType::Athena => {
                                    let x = f.args[0].clone();
                                    let n = f.args[1].clone();
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SUBSTRING".to_string(),
                                        vec![x, Expression::number(1), n],
                                    ))))
                                }
                                DialectType::Spark | DialectType::Databricks
                                    if matches!(
                                        source,
                                        DialectType::TSQL | DialectType::Fabric
                                    ) =>
                                {
                                    // TSQL LEFT(x, n) -> LEFT(CAST(x AS STRING), n) for Spark
                                    let x = f.args[0].clone();
                                    let n = f.args[1].clone();
                                    let cast_x = Expression::Cast(Box::new(Cast {
                                        this: x,
                                        to: DataType::VarChar {
                                            length: None,
                                            parenthesized_length: false,
                                        },
                                        double_colon_syntax: false,
                                        trailing_comments: Vec::new(),
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LEFT".to_string(),
                                        vec![cast_x, n],
                                    ))))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        "RIGHT" if f.args.len() == 2 => {
                            match target {
                                DialectType::Hive
                                | DialectType::Presto
                                | DialectType::Trino
                                | DialectType::Athena => {
                                    let x = f.args[0].clone();
                                    let n = f.args[1].clone();
                                    // SUBSTRING(x, LENGTH(x) - (n - 1))
                                    let len_x = Expression::Function(Box::new(Function::new(
                                        "LENGTH".to_string(),
                                        vec![x.clone()],
                                    )));
                                    let n_minus_1 = Expression::Sub(Box::new(
                                        crate::expressions::BinaryOp::new(n, Expression::number(1)),
                                    ));
                                    let n_minus_1_paren =
                                        Expression::Paren(Box::new(crate::expressions::Paren {
                                            this: n_minus_1,
                                            trailing_comments: Vec::new(),
                                        }));
                                    let offset = Expression::Sub(Box::new(
                                        crate::expressions::BinaryOp::new(len_x, n_minus_1_paren),
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SUBSTRING".to_string(),
                                        vec![x, offset],
                                    ))))
                                }
                                DialectType::Spark | DialectType::Databricks
                                    if matches!(
                                        source,
                                        DialectType::TSQL | DialectType::Fabric
                                    ) =>
                                {
                                    // TSQL RIGHT(x, n) -> RIGHT(CAST(x AS STRING), n) for Spark
                                    let x = f.args[0].clone();
                                    let n = f.args[1].clone();
                                    let cast_x = Expression::Cast(Box::new(Cast {
                                        this: x,
                                        to: DataType::VarChar {
                                            length: None,
                                            parenthesized_length: false,
                                        },
                                        double_colon_syntax: false,
                                        trailing_comments: Vec::new(),
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "RIGHT".to_string(),
                                        vec![cast_x, n],
                                    ))))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // MAP_FROM_ARRAYS(keys, vals) -> target-specific map construction
                        "MAP_FROM_ARRAYS" if f.args.len() == 2 => match target {
                            DialectType::Snowflake => Ok(Expression::Function(Box::new(
                                Function::new("OBJECT_CONSTRUCT".to_string(), f.args),
                            ))),
                            DialectType::Spark | DialectType::Databricks => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "MAP_FROM_ARRAYS".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "MAP".to_string(),
                                f.args,
                            )))),
                        },
                        // LIKE(foo, 'pat') -> foo LIKE 'pat'; LIKE(foo, 'pat', '!') -> foo LIKE 'pat' ESCAPE '!'
                        // SQLite uses LIKE(pattern, string[, escape]) with args in reverse order
                        "LIKE" if f.args.len() >= 2 => {
                            let (this, pattern) = if matches!(source, DialectType::SQLite) {
                                // SQLite: LIKE(pattern, string) -> string LIKE pattern
                                (f.args[1].clone(), f.args[0].clone())
                            } else {
                                // Standard: LIKE(string, pattern) -> string LIKE pattern
                                (f.args[0].clone(), f.args[1].clone())
                            };
                            let escape = if f.args.len() >= 3 {
                                Some(f.args[2].clone())
                            } else {
                                None
                            };
                            Ok(Expression::Like(Box::new(crate::expressions::LikeOp {
                                left: this,
                                right: pattern,
                                escape,
                                quantifier: None,
                                inferred_type: None,
                            })))
                        }
                        // ILIKE(foo, 'pat') -> foo ILIKE 'pat'
                        "ILIKE" if f.args.len() >= 2 => {
                            let this = f.args[0].clone();
                            let pattern = f.args[1].clone();
                            let escape = if f.args.len() >= 3 {
                                Some(f.args[2].clone())
                            } else {
                                None
                            };
                            Ok(Expression::ILike(Box::new(crate::expressions::LikeOp {
                                left: this,
                                right: pattern,
                                escape,
                                quantifier: None,
                                inferred_type: None,
                            })))
                        }
                        // CHAR(n) -> CHR(n) for non-MySQL/non-TSQL targets
                        "CHAR" if f.args.len() == 1 => match target {
                            DialectType::MySQL | DialectType::SingleStore | DialectType::TSQL => {
                                Ok(Expression::Function(f))
                            }
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "CHR".to_string(),
                                f.args,
                            )))),
                        },
                        // CONCAT(a, b) -> a || b for PostgreSQL
                        "CONCAT"
                            if f.args.len() == 2
                                && matches!(target, DialectType::PostgreSQL)
                                && matches!(
                                    source,
                                    DialectType::ClickHouse | DialectType::MySQL
                                ) =>
                        {
                            let mut args = f.args;
                            let right = args.pop().unwrap();
                            let left = args.pop().unwrap();
                            Ok(Expression::DPipe(Box::new(crate::expressions::DPipe {
                                this: Box::new(left),
                                expression: Box::new(right),
                                safe: None,
                            })))
                        }
                        // ARRAY_TO_STRING(arr, delim) -> target-specific
                        "ARRAY_TO_STRING"
                            if f.args.len() == 2
                                && matches!(target, DialectType::DuckDB)
                                && matches!(source, DialectType::Snowflake) =>
                        {
                            let mut args = f.args;
                            let arr = args.remove(0);
                            let sep = args.remove(0);
                            // sep IS NULL
                            let sep_is_null = Expression::IsNull(Box::new(IsNull {
                                this: sep.clone(),
                                not: false,
                                postfix_form: false,
                            }));
                            // COALESCE(CAST(x AS TEXT), '')
                            let cast_x = Expression::Cast(Box::new(Cast {
                                this: Expression::Identifier(Identifier::new("x")),
                                to: DataType::Text,
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            let coalesce =
                                Expression::Coalesce(Box::new(crate::expressions::VarArgFunc {
                                    original_name: None,
                                    expressions: vec![
                                        cast_x,
                                        Expression::Literal(Box::new(Literal::String(
                                            String::new(),
                                        ))),
                                    ],
                                    inferred_type: None,
                                }));
                            let lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![Identifier::new("x")],
                                    body: coalesce,
                                    colon: false,
                                    parameter_types: Vec::new(),
                                }));
                            let list_transform = Expression::Function(Box::new(Function::new(
                                "LIST_TRANSFORM".to_string(),
                                vec![arr, lambda],
                            )));
                            let array_to_string = Expression::Function(Box::new(Function::new(
                                "ARRAY_TO_STRING".to_string(),
                                vec![list_transform, sep],
                            )));
                            Ok(Expression::Case(Box::new(Case {
                                operand: None,
                                whens: vec![(sep_is_null, Expression::Null(Null))],
                                else_: Some(array_to_string),
                                comments: Vec::new(),
                                inferred_type: None,
                            })))
                        }
                        "ARRAY_TO_STRING" if f.args.len() >= 2 => match target {
                            DialectType::Presto | DialectType::Trino => Ok(Expression::Function(
                                Box::new(Function::new("ARRAY_JOIN".to_string(), f.args)),
                            )),
                            DialectType::TSQL | DialectType::Fabric
                                if matches!(
                                    source,
                                    DialectType::PostgreSQL | DialectType::CockroachDB
                                ) =>
                            {
                                Ok(Expression::Function(f))
                            }
                            DialectType::TSQL => Ok(Expression::Function(Box::new(Function::new(
                                "STRING_AGG".to_string(),
                                f.args,
                            )))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // ARRAY_CONCAT / LIST_CONCAT -> target-specific
                        "ARRAY_CONCAT" | "LIST_CONCAT" if f.args.len() == 2 => match target {
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "CONCAT".to_string(),
                                    f.args,
                                ))))
                            }
                            DialectType::Snowflake => Ok(Expression::Function(Box::new(
                                Function::new("ARRAY_CAT".to_string(), f.args),
                            ))),
                            DialectType::Redshift => Ok(Expression::Function(Box::new(
                                Function::new("ARRAY_CONCAT".to_string(), f.args),
                            ))),
                            DialectType::PostgreSQL => Ok(Expression::Function(Box::new(
                                Function::new("ARRAY_CAT".to_string(), f.args),
                            ))),
                            DialectType::DuckDB => Ok(Expression::Function(Box::new(
                                Function::new("LIST_CONCAT".to_string(), f.args),
                            ))),
                            DialectType::Presto | DialectType::Trino => Ok(Expression::Function(
                                Box::new(Function::new("CONCAT".to_string(), f.args)),
                            )),
                            DialectType::BigQuery => Ok(Expression::Function(Box::new(
                                Function::new("ARRAY_CONCAT".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // ARRAY_CONTAINS(arr, x) / HAS(arr, x) / CONTAINS(arr, x) normalization
                        "HAS" if f.args.len() == 2 => match target {
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "ARRAY_CONTAINS".to_string(),
                                    f.args,
                                ))))
                            }
                            DialectType::Presto | DialectType::Trino => Ok(Expression::Function(
                                Box::new(Function::new("CONTAINS".to_string(), f.args)),
                            )),
                            _ => Ok(Expression::Function(f)),
                        },
                        // NVL(a, b, c, d) -> COALESCE(a, b, c, d) - NVL should keep all args
                        "NVL" if f.args.len() > 2 => Ok(Expression::Function(Box::new(
                            Function::new("COALESCE".to_string(), f.args),
                        ))),
                        // ISNULL(x) in MySQL -> (x IS NULL)
                        "ISNULL"
                            if f.args.len() == 1
                                && matches!(source, DialectType::MySQL)
                                && matches!(target, DialectType::MySQL) =>
                        {
                            let arg = f.args.into_iter().next().unwrap();
                            Ok(Expression::Paren(Box::new(crate::expressions::Paren {
                                this: Expression::IsNull(Box::new(crate::expressions::IsNull {
                                    this: arg,
                                    not: false,
                                    postfix_form: false,
                                })),
                                trailing_comments: Vec::new(),
                            })))
                        }
                        // MONTHNAME(x) -> DATE_FORMAT(x, '%M') for MySQL -> MySQL
                        "MONTHNAME"
                            if f.args.len() == 1 && matches!(target, DialectType::MySQL) =>
                        {
                            let arg = f.args.into_iter().next().unwrap();
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_FORMAT".to_string(),
                                vec![arg, Expression::string("%M")],
                            ))))
                        }
                        // ClickHouse splitByString('s', x) -> DuckDB STR_SPLIT(x, 's') / Hive SPLIT(x, CONCAT('\\Q', 's', '\\E'))
                        "SPLITBYSTRING" if f.args.len() == 2 => {
                            let sep = f.args[0].clone();
                            let str_arg = f.args[1].clone();
                            match target {
                                DialectType::DuckDB => Ok(Expression::Function(Box::new(
                                    Function::new("STR_SPLIT".to_string(), vec![str_arg, sep]),
                                ))),
                                DialectType::Doris => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SPLIT_BY_STRING".to_string(),
                                        vec![str_arg, sep],
                                    ))))
                                }
                                DialectType::Hive
                                | DialectType::Spark
                                | DialectType::Databricks => {
                                    // SPLIT(x, CONCAT('\\Q', sep, '\\E'))
                                    let escaped = Expression::Function(Box::new(Function::new(
                                        "CONCAT".to_string(),
                                        vec![
                                            Expression::string("\\Q"),
                                            sep,
                                            Expression::string("\\E"),
                                        ],
                                    )));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "SPLIT".to_string(),
                                        vec![str_arg, escaped],
                                    ))))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // ClickHouse splitByRegexp('pattern', x) -> DuckDB STR_SPLIT_REGEX(x, 'pattern')
                        "SPLITBYREGEXP" if f.args.len() == 2 => {
                            let sep = f.args[0].clone();
                            let str_arg = f.args[1].clone();
                            match target {
                                DialectType::DuckDB => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "STR_SPLIT_REGEX".to_string(),
                                        vec![str_arg, sep],
                                    ))))
                                }
                                DialectType::Hive
                                | DialectType::Spark
                                | DialectType::Databricks => Ok(Expression::Function(Box::new(
                                    Function::new("SPLIT".to_string(), vec![str_arg, sep]),
                                ))),
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // ClickHouse toMonday(x) -> DATE_TRUNC('WEEK', x) / DATE_TRUNC(x, 'WEEK') for Doris
                        "TOMONDAY" => {
                            if f.args.len() == 1 {
                                let arg = f.args.into_iter().next().unwrap();
                                match target {
                                    DialectType::Doris => {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_TRUNC".to_string(),
                                            vec![arg, Expression::string("WEEK")],
                                        ))))
                                    }
                                    _ => Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_TRUNC".to_string(),
                                        vec![Expression::string("WEEK"), arg],
                                    )))),
                                }
                            } else {
                                Ok(Expression::Function(f))
                            }
                        }
                        // COLLECT_LIST with FILTER(WHERE x IS NOT NULL) for targets that need it
                        "COLLECT_LIST" if f.args.len() == 1 => match target {
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                Ok(Expression::Function(f))
                            }
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY_AGG".to_string(),
                                f.args,
                            )))),
                        },
                        // TO_CHAR(x) with 1 arg -> CAST(x AS STRING) for Doris
                        "TO_CHAR" if f.args.len() == 1 && matches!(target, DialectType::Doris) => {
                            let arg = f.args.into_iter().next().unwrap();
                            Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                                this: arg,
                                to: DataType::Custom {
                                    name: "STRING".to_string(),
                                },
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        // DBMS_RANDOM.VALUE() -> RANDOM() for PostgreSQL
                        "DBMS_RANDOM.VALUE" if f.args.is_empty() => match target {
                            DialectType::PostgreSQL => Ok(Expression::Function(Box::new(
                                Function::new("RANDOM".to_string(), vec![]),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // ClickHouse formatDateTime -> target-specific
                        "FORMATDATETIME" if f.args.len() >= 2 => match target {
                            DialectType::MySQL => Ok(Expression::Function(Box::new(
                                Function::new("DATE_FORMAT".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // REPLICATE('x', n) -> REPEAT('x', n) for non-TSQL targets
                        "REPLICATE" if f.args.len() == 2 => match target {
                            DialectType::TSQL => Ok(Expression::Function(f)),
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "REPEAT".to_string(),
                                f.args,
                            )))),
                        },
                        // LEN(x) -> LENGTH(x) for non-TSQL targets
                        // No CAST needed when arg is already a string literal
                        "LEN" if f.args.len() == 1 => {
                            match target {
                                DialectType::TSQL => Ok(Expression::Function(f)),
                                DialectType::Spark | DialectType::Databricks => {
                                    let arg = f.args.into_iter().next().unwrap();
                                    // Don't wrap string literals with CAST - they're already strings
                                    let is_string = matches!(
                                        &arg,
                                        Expression::Literal(lit) if matches!(lit.as_ref(), crate::expressions::Literal::String(_))
                                    );
                                    let final_arg = if is_string {
                                        arg
                                    } else {
                                        Expression::Cast(Box::new(Cast {
                                            this: arg,
                                            to: DataType::VarChar {
                                                length: None,
                                                parenthesized_length: false,
                                            },
                                            double_colon_syntax: false,
                                            trailing_comments: Vec::new(),
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LENGTH".to_string(),
                                        vec![final_arg],
                                    ))))
                                }
                                _ => {
                                    let arg = f.args.into_iter().next().unwrap();
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LENGTH".to_string(),
                                        vec![arg],
                                    ))))
                                }
                            }
                        }
                        // COUNT_BIG(x) -> COUNT(x) for non-TSQL targets
                        "COUNT_BIG" if f.args.len() == 1 => match target {
                            DialectType::TSQL => Ok(Expression::Function(f)),
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "COUNT".to_string(),
                                f.args,
                            )))),
                        },
                        // DATEFROMPARTS(y, m, d) -> MAKE_DATE(y, m, d) for non-TSQL targets
                        "DATEFROMPARTS" if f.args.len() == 3 => match target {
                            DialectType::TSQL => Ok(Expression::Function(f)),
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "MAKE_DATE".to_string(),
                                f.args,
                            )))),
                        },
                        // REGEXP_LIKE(str, pattern) -> RegexpLike expression (target-specific output)
                        "REGEXP_LIKE" if f.args.len() >= 2 => {
                            let str_expr = f.args[0].clone();
                            let pattern = f.args[1].clone();
                            let flags = if f.args.len() >= 3 {
                                Some(f.args[2].clone())
                            } else {
                                None
                            };
                            match target {
                                DialectType::DuckDB => {
                                    let mut new_args = vec![str_expr, pattern];
                                    if let Some(fl) = flags {
                                        new_args.push(fl);
                                    }
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "REGEXP_MATCHES".to_string(),
                                        new_args,
                                    ))))
                                }
                                DialectType::TSQL | DialectType::Fabric
                                    if flags.is_none()
                                        && matches!(
                                            source,
                                            DialectType::PostgreSQL | DialectType::CockroachDB
                                        ) =>
                                {
                                    Ok(operators::build_tsql_regex_patindex_predicate(
                                        str_expr, pattern, false,
                                    ))
                                }
                                _ => Ok(Expression::RegexpLike(Box::new(
                                    crate::expressions::RegexpFunc {
                                        this: str_expr,
                                        pattern,
                                        flags,
                                    },
                                ))),
                            }
                        }
                        // ClickHouse arrayJoin -> UNNEST for PostgreSQL
                        "ARRAYJOIN" if f.args.len() == 1 => match target {
                            DialectType::PostgreSQL => Ok(Expression::Function(Box::new(
                                Function::new("UNNEST".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // DATETIMEFROMPARTS(y, m, d, h, mi, s, ms) -> MAKE_TIMESTAMP / TIMESTAMP_FROM_PARTS
                        "DATETIMEFROMPARTS" if f.args.len() == 7 => {
                            match target {
                                DialectType::TSQL => Ok(Expression::Function(f)),
                                DialectType::DuckDB => {
                                    // MAKE_TIMESTAMP(y, m, d, h, mi, s + (ms / 1000.0))
                                    let mut args = f.args;
                                    let ms = args.pop().unwrap();
                                    let s = args.pop().unwrap();
                                    // s + (ms / 1000.0)
                                    let ms_frac = Expression::Div(Box::new(BinaryOp::new(
                                        ms,
                                        Expression::Literal(Box::new(
                                            crate::expressions::Literal::Number(
                                                "1000.0".to_string(),
                                            ),
                                        )),
                                    )));
                                    let s_with_ms = Expression::Add(Box::new(BinaryOp::new(
                                        s,
                                        Expression::Paren(Box::new(Paren {
                                            this: ms_frac,
                                            trailing_comments: vec![],
                                        })),
                                    )));
                                    args.push(s_with_ms);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "MAKE_TIMESTAMP".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::Snowflake => {
                                    // TIMESTAMP_FROM_PARTS(y, m, d, h, mi, s, ms * 1000000)
                                    let mut args = f.args;
                                    let ms = args.pop().unwrap();
                                    // ms * 1000000
                                    let ns = Expression::Mul(Box::new(BinaryOp::new(
                                        ms,
                                        Expression::number(1000000),
                                    )));
                                    args.push(ns);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TIMESTAMP_FROM_PARTS".to_string(),
                                        args,
                                    ))))
                                }
                                _ => {
                                    // Default: keep function name for other targets
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATETIMEFROMPARTS".to_string(),
                                        f.args,
                                    ))))
                                }
                            }
                        }
                        // CONVERT(type, expr [, style]) -> CAST(expr AS type) for non-TSQL targets
                        // TRY_CONVERT(type, expr [, style]) -> TRY_CAST(expr AS type) for non-TSQL targets
                        "CONVERT" | "TRY_CONVERT" if f.args.len() >= 2 => {
                            let is_try = name == "TRY_CONVERT";
                            let type_expr = f.args[0].clone();
                            let value_expr = f.args[1].clone();
                            let style = if f.args.len() >= 3 {
                                Some(&f.args[2])
                            } else {
                                None
                            };

                            // For TSQL->TSQL, normalize types and preserve CONVERT/TRY_CONVERT
                            if matches!(target, DialectType::TSQL) {
                                let normalized_type = match &type_expr {
                                    Expression::DataType(dt) => {
                                        let new_dt = match dt {
                                            DataType::Int { .. } => DataType::Custom {
                                                name: "INTEGER".to_string(),
                                            },
                                            _ => dt.clone(),
                                        };
                                        Expression::DataType(new_dt)
                                    }
                                    Expression::Identifier(id) => {
                                        if id.name.eq_ignore_ascii_case("INT") {
                                            Expression::Identifier(
                                                crate::expressions::Identifier::new("INTEGER"),
                                            )
                                        } else {
                                            let upper = id.name.to_ascii_uppercase();
                                            Expression::Identifier(
                                                crate::expressions::Identifier::new(upper),
                                            )
                                        }
                                    }
                                    Expression::Column(col) => {
                                        if col.name.name.eq_ignore_ascii_case("INT") {
                                            Expression::Identifier(
                                                crate::expressions::Identifier::new("INTEGER"),
                                            )
                                        } else {
                                            let upper = col.name.name.to_ascii_uppercase();
                                            Expression::Identifier(
                                                crate::expressions::Identifier::new(upper),
                                            )
                                        }
                                    }
                                    _ => type_expr.clone(),
                                };
                                let func_name = if is_try { "TRY_CONVERT" } else { "CONVERT" };
                                let mut new_args = vec![normalized_type, value_expr];
                                if let Some(s) = style {
                                    new_args.push(s.clone());
                                }
                                return Ok(Expression::Function(Box::new(Function::new(
                                    func_name.to_string(),
                                    new_args,
                                ))));
                            }

                            // For other targets: CONVERT(type, expr) -> CAST(expr AS type)
                            fn expr_to_datatype(e: &Expression) -> Option<DataType> {
                                match e {
                                    Expression::DataType(dt) => {
                                        // Convert NVARCHAR/NCHAR Custom types to standard VarChar/Char
                                        match dt {
                                            DataType::Custom { name }
                                                if name.starts_with("NVARCHAR(")
                                                    || name.starts_with("NCHAR(") =>
                                            {
                                                // Extract the length from "NVARCHAR(200)" or "NCHAR(40)"
                                                let inner = &name
                                                    [name.find('(').unwrap() + 1..name.len() - 1];
                                                if inner.eq_ignore_ascii_case("MAX") {
                                                    Some(DataType::Text)
                                                } else if let Ok(len) = inner.parse::<u32>() {
                                                    if name.starts_with("NCHAR") {
                                                        Some(DataType::Char { length: Some(len) })
                                                    } else {
                                                        Some(DataType::VarChar {
                                                            length: Some(len),
                                                            parenthesized_length: false,
                                                        })
                                                    }
                                                } else {
                                                    Some(dt.clone())
                                                }
                                            }
                                            DataType::Custom { name } if name == "NVARCHAR" => {
                                                Some(DataType::VarChar {
                                                    length: None,
                                                    parenthesized_length: false,
                                                })
                                            }
                                            DataType::Custom { name } if name == "NCHAR" => {
                                                Some(DataType::Char { length: None })
                                            }
                                            DataType::Custom { name }
                                                if name == "NVARCHAR(MAX)"
                                                    || name == "VARCHAR(MAX)" =>
                                            {
                                                Some(DataType::Text)
                                            }
                                            _ => Some(dt.clone()),
                                        }
                                    }
                                    Expression::Identifier(id) => {
                                        let name = id.name.to_ascii_uppercase();
                                        match name.as_str() {
                                            "INT" | "INTEGER" => Some(DataType::Int {
                                                length: None,
                                                integer_spelling: false,
                                            }),
                                            "BIGINT" => Some(DataType::BigInt { length: None }),
                                            "SMALLINT" => Some(DataType::SmallInt { length: None }),
                                            "TINYINT" => Some(DataType::TinyInt { length: None }),
                                            "FLOAT" => Some(DataType::Float {
                                                precision: None,
                                                scale: None,
                                                real_spelling: false,
                                            }),
                                            "REAL" => Some(DataType::Float {
                                                precision: None,
                                                scale: None,
                                                real_spelling: true,
                                            }),
                                            "DATETIME" | "DATETIME2" => Some(DataType::Timestamp {
                                                timezone: false,
                                                precision: None,
                                            }),
                                            "DATE" => Some(DataType::Date),
                                            "BIT" => Some(DataType::Boolean),
                                            "TEXT" => Some(DataType::Text),
                                            "NUMERIC" => Some(DataType::Decimal {
                                                precision: None,
                                                scale: None,
                                            }),
                                            "MONEY" => Some(DataType::Decimal {
                                                precision: Some(15),
                                                scale: Some(4),
                                            }),
                                            "SMALLMONEY" => Some(DataType::Decimal {
                                                precision: Some(6),
                                                scale: Some(4),
                                            }),
                                            "VARCHAR" => Some(DataType::VarChar {
                                                length: None,
                                                parenthesized_length: false,
                                            }),
                                            "NVARCHAR" => Some(DataType::VarChar {
                                                length: None,
                                                parenthesized_length: false,
                                            }),
                                            "CHAR" => Some(DataType::Char { length: None }),
                                            "NCHAR" => Some(DataType::Char { length: None }),
                                            _ => Some(DataType::Custom { name }),
                                        }
                                    }
                                    Expression::Column(col) => {
                                        let name = col.name.name.to_ascii_uppercase();
                                        match name.as_str() {
                                            "INT" | "INTEGER" => Some(DataType::Int {
                                                length: None,
                                                integer_spelling: false,
                                            }),
                                            "BIGINT" => Some(DataType::BigInt { length: None }),
                                            "FLOAT" => Some(DataType::Float {
                                                precision: None,
                                                scale: None,
                                                real_spelling: false,
                                            }),
                                            "DATETIME" | "DATETIME2" => Some(DataType::Timestamp {
                                                timezone: false,
                                                precision: None,
                                            }),
                                            "DATE" => Some(DataType::Date),
                                            "NUMERIC" => Some(DataType::Decimal {
                                                precision: None,
                                                scale: None,
                                            }),
                                            "VARCHAR" => Some(DataType::VarChar {
                                                length: None,
                                                parenthesized_length: false,
                                            }),
                                            "NVARCHAR" => Some(DataType::VarChar {
                                                length: None,
                                                parenthesized_length: false,
                                            }),
                                            "CHAR" => Some(DataType::Char { length: None }),
                                            "NCHAR" => Some(DataType::Char { length: None }),
                                            _ => Some(DataType::Custom { name }),
                                        }
                                    }
                                    // NVARCHAR(200) parsed as Function("NVARCHAR", [200])
                                    Expression::Function(f) => {
                                        let fname = f.name.to_ascii_uppercase();
                                        match fname.as_str() {
                                            "VARCHAR" | "NVARCHAR" => {
                                                let len = f.args.first().and_then(|a| {
                                                    if let Expression::Literal(lit) = a {
                                                        if let crate::expressions::Literal::Number(
                                                            n,
                                                        ) = lit.as_ref()
                                                        {
                                                            n.parse::<u32>().ok()
                                                        } else {
                                                            None
                                                        }
                                                    } else if let Expression::Identifier(id) = a {
                                                        if id.name.eq_ignore_ascii_case("MAX") {
                                                            None
                                                        } else {
                                                            None
                                                        }
                                                    } else {
                                                        None
                                                    }
                                                });
                                                // Check for VARCHAR(MAX) -> TEXT
                                                let is_max = f.args.first().map_or(false, |a| {
                                                    matches!(a, Expression::Identifier(id) if id.name.eq_ignore_ascii_case("MAX"))
                                                    || matches!(a, Expression::Column(col) if col.name.name.eq_ignore_ascii_case("MAX"))
                                                });
                                                if is_max {
                                                    Some(DataType::Text)
                                                } else {
                                                    Some(DataType::VarChar {
                                                        length: len,
                                                        parenthesized_length: false,
                                                    })
                                                }
                                            }
                                            "NCHAR" | "CHAR" => {
                                                let len = f.args.first().and_then(|a| {
                                                    if let Expression::Literal(lit) = a {
                                                        if let crate::expressions::Literal::Number(
                                                            n,
                                                        ) = lit.as_ref()
                                                        {
                                                            n.parse::<u32>().ok()
                                                        } else {
                                                            None
                                                        }
                                                    } else {
                                                        None
                                                    }
                                                });
                                                Some(DataType::Char { length: len })
                                            }
                                            "NUMERIC" | "DECIMAL" => {
                                                let precision = f.args.first().and_then(|a| {
                                                    if let Expression::Literal(lit) = a {
                                                        if let crate::expressions::Literal::Number(
                                                            n,
                                                        ) = lit.as_ref()
                                                        {
                                                            n.parse::<u32>().ok()
                                                        } else {
                                                            None
                                                        }
                                                    } else {
                                                        None
                                                    }
                                                });
                                                let scale = f.args.get(1).and_then(|a| {
                                                    if let Expression::Literal(lit) = a {
                                                        if let crate::expressions::Literal::Number(
                                                            n,
                                                        ) = lit.as_ref()
                                                        {
                                                            n.parse::<u32>().ok()
                                                        } else {
                                                            None
                                                        }
                                                    } else {
                                                        None
                                                    }
                                                });
                                                Some(DataType::Decimal { precision, scale })
                                            }
                                            _ => None,
                                        }
                                    }
                                    _ => None,
                                }
                            }

                            if let Some(mut dt) = expr_to_datatype(&type_expr) {
                                // For TSQL source: VARCHAR/CHAR without length defaults to 30
                                let is_tsql_source =
                                    matches!(source, DialectType::TSQL | DialectType::Fabric);
                                if is_tsql_source {
                                    match &dt {
                                        DataType::VarChar { length: None, .. } => {
                                            dt = DataType::VarChar {
                                                length: Some(30),
                                                parenthesized_length: false,
                                            };
                                        }
                                        DataType::Char { length: None } => {
                                            dt = DataType::Char { length: Some(30) };
                                        }
                                        _ => {}
                                    }
                                }

                                // Determine if this is a string type
                                let is_string_type = matches!(
                                    dt,
                                    DataType::VarChar { .. }
                                        | DataType::Char { .. }
                                        | DataType::Text
                                ) || matches!(&dt, DataType::Custom { name } if name == "NVARCHAR" || name == "NCHAR"
                                        || name.starts_with("NVARCHAR(") || name.starts_with("NCHAR(")
                                        || name.starts_with("VARCHAR(") || name == "VARCHAR"
                                        || name == "STRING");

                                // Determine if this is a date/time type
                                let is_datetime_type = matches!(
                                    dt,
                                    DataType::Timestamp { .. } | DataType::Date
                                ) || matches!(&dt, DataType::Custom { name } if name == "DATETIME"
                                        || name == "DATETIME2" || name == "SMALLDATETIME");

                                // Check for date conversion with style
                                if style.is_some() {
                                    let style_num = style.and_then(|s| {
                                        if let Expression::Literal(lit) = s {
                                            if let crate::expressions::Literal::Number(n) =
                                                lit.as_ref()
                                            {
                                                n.parse::<u32>().ok()
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    });

                                    // TSQL CONVERT date styles (Java format)
                                    let format_str = style_num.and_then(|n| match n {
                                        101 => Some("MM/dd/yyyy"),
                                        102 => Some("yyyy.MM.dd"),
                                        103 => Some("dd/MM/yyyy"),
                                        104 => Some("dd.MM.yyyy"),
                                        105 => Some("dd-MM-yyyy"),
                                        108 => Some("HH:mm:ss"),
                                        110 => Some("MM-dd-yyyy"),
                                        112 => Some("yyyyMMdd"),
                                        120 | 20 => Some("yyyy-MM-dd HH:mm:ss"),
                                        121 | 21 => Some("yyyy-MM-dd HH:mm:ss.SSSSSS"),
                                        126 | 127 => Some("yyyy-MM-dd'T'HH:mm:ss.SSS"),
                                        _ => None,
                                    });

                                    // Non-string, non-datetime types with style: just CAST, ignore the style
                                    if !is_string_type && !is_datetime_type {
                                        let cast_expr = if is_try {
                                            Expression::TryCast(Box::new(
                                                crate::expressions::Cast {
                                                    this: value_expr,
                                                    to: dt,
                                                    trailing_comments: Vec::new(),
                                                    double_colon_syntax: false,
                                                    format: None,
                                                    default: None,
                                                    inferred_type: None,
                                                },
                                            ))
                                        } else {
                                            Expression::Cast(Box::new(crate::expressions::Cast {
                                                this: value_expr,
                                                to: dt,
                                                trailing_comments: Vec::new(),
                                                double_colon_syntax: false,
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }))
                                        };
                                        return Ok(cast_expr);
                                    }

                                    if let Some(java_fmt) = format_str {
                                        let c_fmt = java_fmt
                                            .replace("yyyy", "%Y")
                                            .replace("MM", "%m")
                                            .replace("dd", "%d")
                                            .replace("HH", "%H")
                                            .replace("mm", "%M")
                                            .replace("ss", "%S")
                                            .replace("SSSSSS", "%f")
                                            .replace("SSS", "%f")
                                            .replace("'T'", "T");

                                        // For datetime target types: style is the INPUT format for parsing strings -> dates
                                        if is_datetime_type {
                                            match target {
                                                DialectType::DuckDB => {
                                                    return Ok(Expression::Function(Box::new(
                                                        Function::new(
                                                            "STRPTIME".to_string(),
                                                            vec![
                                                                value_expr,
                                                                Expression::string(&c_fmt),
                                                            ],
                                                        ),
                                                    )));
                                                }
                                                DialectType::Spark | DialectType::Databricks => {
                                                    // CONVERT(DATETIME, x, style) -> TO_TIMESTAMP(x, fmt)
                                                    // CONVERT(DATE, x, style) -> TO_DATE(x, fmt)
                                                    let func_name = if matches!(dt, DataType::Date)
                                                    {
                                                        "TO_DATE"
                                                    } else {
                                                        "TO_TIMESTAMP"
                                                    };
                                                    return Ok(Expression::Function(Box::new(
                                                        Function::new(
                                                            func_name.to_string(),
                                                            vec![
                                                                value_expr,
                                                                Expression::string(java_fmt),
                                                            ],
                                                        ),
                                                    )));
                                                }
                                                DialectType::Hive => {
                                                    return Ok(Expression::Function(Box::new(
                                                        Function::new(
                                                            "TO_TIMESTAMP".to_string(),
                                                            vec![
                                                                value_expr,
                                                                Expression::string(java_fmt),
                                                            ],
                                                        ),
                                                    )));
                                                }
                                                _ => {
                                                    return Ok(Expression::Cast(Box::new(
                                                        crate::expressions::Cast {
                                                            this: value_expr,
                                                            to: dt,
                                                            trailing_comments: Vec::new(),
                                                            double_colon_syntax: false,
                                                            format: None,
                                                            default: None,
                                                            inferred_type: None,
                                                        },
                                                    )));
                                                }
                                            }
                                        }

                                        // For string target types: style is the OUTPUT format for dates -> strings
                                        match target {
                                            DialectType::DuckDB => {
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "STRPTIME".to_string(),
                                                    vec![value_expr, Expression::string(&c_fmt)],
                                                ))))
                                            }
                                            DialectType::Spark | DialectType::Databricks => {
                                                // For string target types with style: CAST(DATE_FORMAT(x, fmt) AS type)
                                                // Determine the target string type
                                                let string_dt = match &dt {
                                                    DataType::VarChar {
                                                        length: Some(l), ..
                                                    } => DataType::VarChar {
                                                        length: Some(*l),
                                                        parenthesized_length: false,
                                                    },
                                                    DataType::Text => DataType::Custom {
                                                        name: "STRING".to_string(),
                                                    },
                                                    _ => DataType::Custom {
                                                        name: "STRING".to_string(),
                                                    },
                                                };
                                                let date_format_expr =
                                                    Expression::Function(Box::new(Function::new(
                                                        "DATE_FORMAT".to_string(),
                                                        vec![
                                                            value_expr,
                                                            Expression::string(java_fmt),
                                                        ],
                                                    )));
                                                let cast_expr = if is_try {
                                                    Expression::TryCast(Box::new(
                                                        crate::expressions::Cast {
                                                            this: date_format_expr,
                                                            to: string_dt,
                                                            trailing_comments: Vec::new(),
                                                            double_colon_syntax: false,
                                                            format: None,
                                                            default: None,
                                                            inferred_type: None,
                                                        },
                                                    ))
                                                } else {
                                                    Expression::Cast(Box::new(
                                                        crate::expressions::Cast {
                                                            this: date_format_expr,
                                                            to: string_dt,
                                                            trailing_comments: Vec::new(),
                                                            double_colon_syntax: false,
                                                            format: None,
                                                            default: None,
                                                            inferred_type: None,
                                                        },
                                                    ))
                                                };
                                                Ok(cast_expr)
                                            }
                                            DialectType::MySQL | DialectType::SingleStore => {
                                                // For MySQL: CAST(DATE_FORMAT(x, mysql_fmt) AS CHAR(n))
                                                let mysql_fmt = java_fmt
                                                    .replace("yyyy", "%Y")
                                                    .replace("MM", "%m")
                                                    .replace("dd", "%d")
                                                    .replace("HH:mm:ss.SSSSSS", "%T")
                                                    .replace("HH:mm:ss", "%T")
                                                    .replace("HH", "%H")
                                                    .replace("mm", "%i")
                                                    .replace("ss", "%S");
                                                let date_format_expr =
                                                    Expression::Function(Box::new(Function::new(
                                                        "DATE_FORMAT".to_string(),
                                                        vec![
                                                            value_expr,
                                                            Expression::string(&mysql_fmt),
                                                        ],
                                                    )));
                                                // MySQL uses CHAR for string casts
                                                let mysql_dt = match &dt {
                                                    DataType::VarChar { length, .. } => {
                                                        DataType::Char { length: *length }
                                                    }
                                                    _ => dt,
                                                };
                                                Ok(Expression::Cast(Box::new(
                                                    crate::expressions::Cast {
                                                        this: date_format_expr,
                                                        to: mysql_dt,
                                                        trailing_comments: Vec::new(),
                                                        double_colon_syntax: false,
                                                        format: None,
                                                        default: None,
                                                        inferred_type: None,
                                                    },
                                                )))
                                            }
                                            DialectType::Hive => {
                                                let func_name = "TO_TIMESTAMP";
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    func_name.to_string(),
                                                    vec![value_expr, Expression::string(java_fmt)],
                                                ))))
                                            }
                                            _ => Ok(Expression::Cast(Box::new(
                                                crate::expressions::Cast {
                                                    this: value_expr,
                                                    to: dt,
                                                    trailing_comments: Vec::new(),
                                                    double_colon_syntax: false,
                                                    format: None,
                                                    default: None,
                                                    inferred_type: None,
                                                },
                                            ))),
                                        }
                                    } else {
                                        // Unknown style, just CAST
                                        let cast_expr = if is_try {
                                            Expression::TryCast(Box::new(
                                                crate::expressions::Cast {
                                                    this: value_expr,
                                                    to: dt,
                                                    trailing_comments: Vec::new(),
                                                    double_colon_syntax: false,
                                                    format: None,
                                                    default: None,
                                                    inferred_type: None,
                                                },
                                            ))
                                        } else {
                                            Expression::Cast(Box::new(crate::expressions::Cast {
                                                this: value_expr,
                                                to: dt,
                                                trailing_comments: Vec::new(),
                                                double_colon_syntax: false,
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }))
                                        };
                                        Ok(cast_expr)
                                    }
                                } else {
                                    // No style - simple CAST
                                    let final_dt = if matches!(
                                        target,
                                        DialectType::MySQL | DialectType::SingleStore
                                    ) {
                                        match &dt {
                                            DataType::Int { .. }
                                            | DataType::BigInt { .. }
                                            | DataType::SmallInt { .. }
                                            | DataType::TinyInt { .. } => DataType::Custom {
                                                name: "SIGNED".to_string(),
                                            },
                                            DataType::VarChar { length, .. } => {
                                                DataType::Char { length: *length }
                                            }
                                            _ => dt,
                                        }
                                    } else {
                                        dt
                                    };
                                    let cast_expr = if is_try {
                                        Expression::TryCast(Box::new(crate::expressions::Cast {
                                            this: value_expr,
                                            to: final_dt,
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    } else {
                                        Expression::Cast(Box::new(crate::expressions::Cast {
                                            this: value_expr,
                                            to: final_dt,
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    };
                                    Ok(cast_expr)
                                }
                            } else {
                                // Can't convert type expression - keep as CONVERT/TRY_CONVERT function
                                Ok(Expression::Function(f))
                            }
                        }
                        // STRFTIME(val, fmt) from DuckDB / STRFTIME(fmt, val) from SQLite -> target-specific
                        "STRFTIME" if f.args.len() == 2 => {
                            // SQLite uses STRFTIME(fmt, val); DuckDB uses STRFTIME(val, fmt)
                            let (val, fmt_expr) = if matches!(source, DialectType::SQLite) {
                                // SQLite: args[0] = format, args[1] = value
                                (f.args[1].clone(), &f.args[0])
                            } else {
                                // DuckDB and others: args[0] = value, args[1] = format
                                (f.args[0].clone(), &f.args[1])
                            };

                            // Helper to convert C-style format to Java-style
                            fn c_to_java_format(fmt: &str) -> String {
                                fmt.replace("%Y", "yyyy")
                                    .replace("%m", "MM")
                                    .replace("%d", "dd")
                                    .replace("%H", "HH")
                                    .replace("%M", "mm")
                                    .replace("%S", "ss")
                                    .replace("%f", "SSSSSS")
                                    .replace("%y", "yy")
                                    .replace("%-m", "M")
                                    .replace("%-d", "d")
                                    .replace("%-H", "H")
                                    .replace("%-I", "h")
                                    .replace("%I", "hh")
                                    .replace("%p", "a")
                                    .replace("%j", "DDD")
                                    .replace("%a", "EEE")
                                    .replace("%b", "MMM")
                                    .replace("%F", "yyyy-MM-dd")
                                    .replace("%T", "HH:mm:ss")
                            }

                            // Helper: recursively convert format strings within expressions (handles CONCAT)
                            fn convert_fmt_expr(
                                expr: &Expression,
                                converter: &dyn Fn(&str) -> String,
                            ) -> Expression {
                                match expr {
                                    Expression::Literal(lit)
                                        if matches!(
                                            lit.as_ref(),
                                            crate::expressions::Literal::String(_)
                                        ) =>
                                    {
                                        let crate::expressions::Literal::String(s) = lit.as_ref()
                                        else {
                                            unreachable!()
                                        };
                                        Expression::string(&converter(s))
                                    }
                                    Expression::Function(func)
                                        if func.name.eq_ignore_ascii_case("CONCAT") =>
                                    {
                                        let new_args: Vec<Expression> = func
                                            .args
                                            .iter()
                                            .map(|a| convert_fmt_expr(a, converter))
                                            .collect();
                                        Expression::Function(Box::new(Function::new(
                                            "CONCAT".to_string(),
                                            new_args,
                                        )))
                                    }
                                    other => other.clone(),
                                }
                            }

                            match target {
                                DialectType::DuckDB => {
                                    if matches!(source, DialectType::SQLite) {
                                        // SQLite STRFTIME(fmt, val) -> DuckDB STRFTIME(CAST(val AS TIMESTAMP), fmt)
                                        let cast_val = Expression::Cast(Box::new(Cast {
                                            this: val,
                                            to: crate::expressions::DataType::Timestamp {
                                                precision: None,
                                                timezone: false,
                                            },
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "STRFTIME".to_string(),
                                            vec![cast_val, fmt_expr.clone()],
                                        ))))
                                    } else {
                                        Ok(Expression::Function(f))
                                    }
                                }
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => {
                                    // STRFTIME(val, fmt) -> DATE_FORMAT(val, java_fmt)
                                    let converted_fmt =
                                        convert_fmt_expr(fmt_expr, &c_to_java_format);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_FORMAT".to_string(),
                                        vec![val, converted_fmt],
                                    ))))
                                }
                                DialectType::TSQL | DialectType::Fabric => {
                                    // STRFTIME(val, fmt) -> FORMAT(val, java_fmt)
                                    let converted_fmt =
                                        convert_fmt_expr(fmt_expr, &c_to_java_format);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "FORMAT".to_string(),
                                        vec![val, converted_fmt],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // STRFTIME(val, fmt) -> DATE_FORMAT(val, presto_fmt) (convert DuckDB format to Presto)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let presto_fmt = duckdb_to_presto_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_FORMAT".to_string(),
                                                vec![val, Expression::string(&presto_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_FORMAT".to_string(),
                                                vec![val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_FORMAT".to_string(),
                                            vec![val, fmt_expr.clone()],
                                        ))))
                                    }
                                }
                                DialectType::BigQuery => {
                                    // STRFTIME(val, fmt) -> FORMAT_DATE(bq_fmt, val) - note reversed arg order
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let bq_fmt = duckdb_to_bigquery_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "FORMAT_DATE".to_string(),
                                                vec![Expression::string(&bq_fmt), val],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "FORMAT_DATE".to_string(),
                                                vec![fmt_expr.clone(), val],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "FORMAT_DATE".to_string(),
                                            vec![fmt_expr.clone(), val],
                                        ))))
                                    }
                                }
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    // STRFTIME(val, fmt) -> TO_CHAR(val, pg_fmt)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let pg_fmt = s
                                                .replace("%Y", "YYYY")
                                                .replace("%m", "MM")
                                                .replace("%d", "DD")
                                                .replace("%H", "HH24")
                                                .replace("%M", "MI")
                                                .replace("%S", "SS")
                                                .replace("%y", "YY")
                                                .replace("%-m", "FMMM")
                                                .replace("%-d", "FMDD")
                                                .replace("%-H", "FMHH24")
                                                .replace("%-I", "FMHH12")
                                                .replace("%p", "AM")
                                                .replace("%F", "YYYY-MM-DD")
                                                .replace("%T", "HH24:MI:SS");
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "TO_CHAR".to_string(),
                                                vec![val, Expression::string(&pg_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "TO_CHAR".to_string(),
                                                vec![val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "TO_CHAR".to_string(),
                                            vec![val, fmt_expr.clone()],
                                        ))))
                                    }
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // STRPTIME(val, fmt) from DuckDB -> target-specific date parse function
                        "STRPTIME" if f.args.len() == 2 => {
                            let val = f.args[0].clone();
                            let fmt_expr = &f.args[1];

                            fn c_to_java_format_parse(fmt: &str) -> String {
                                fmt.replace("%Y", "yyyy")
                                    .replace("%m", "MM")
                                    .replace("%d", "dd")
                                    .replace("%H", "HH")
                                    .replace("%M", "mm")
                                    .replace("%S", "ss")
                                    .replace("%f", "SSSSSS")
                                    .replace("%y", "yy")
                                    .replace("%-m", "M")
                                    .replace("%-d", "d")
                                    .replace("%-H", "H")
                                    .replace("%-I", "h")
                                    .replace("%I", "hh")
                                    .replace("%p", "a")
                                    .replace("%F", "yyyy-MM-dd")
                                    .replace("%T", "HH:mm:ss")
                            }

                            match target {
                                DialectType::DuckDB => Ok(Expression::Function(f)),
                                DialectType::Spark | DialectType::Databricks => {
                                    // STRPTIME(val, fmt) -> TO_TIMESTAMP(val, java_fmt)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let java_fmt = c_to_java_format_parse(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "TO_TIMESTAMP".to_string(),
                                                vec![val, Expression::string(&java_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "TO_TIMESTAMP".to_string(),
                                                vec![val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "TO_TIMESTAMP".to_string(),
                                            vec![val, fmt_expr.clone()],
                                        ))))
                                    }
                                }
                                DialectType::Hive => {
                                    // STRPTIME(val, fmt) -> CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(val, java_fmt)) AS TIMESTAMP)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let java_fmt = c_to_java_format_parse(s);
                                            let unix_ts =
                                                Expression::Function(Box::new(Function::new(
                                                    "UNIX_TIMESTAMP".to_string(),
                                                    vec![val, Expression::string(&java_fmt)],
                                                )));
                                            let from_unix =
                                                Expression::Function(Box::new(Function::new(
                                                    "FROM_UNIXTIME".to_string(),
                                                    vec![unix_ts],
                                                )));
                                            Ok(Expression::Cast(Box::new(
                                                crate::expressions::Cast {
                                                    this: from_unix,
                                                    to: DataType::Timestamp {
                                                        timezone: false,
                                                        precision: None,
                                                    },
                                                    trailing_comments: Vec::new(),
                                                    double_colon_syntax: false,
                                                    format: None,
                                                    default: None,
                                                    inferred_type: None,
                                                },
                                            )))
                                        } else {
                                            Ok(Expression::Function(f))
                                        }
                                    } else {
                                        Ok(Expression::Function(f))
                                    }
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // STRPTIME(val, fmt) -> DATE_PARSE(val, presto_fmt) (convert DuckDB format to Presto)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let presto_fmt = duckdb_to_presto_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_PARSE".to_string(),
                                                vec![val, Expression::string(&presto_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_PARSE".to_string(),
                                                vec![val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_PARSE".to_string(),
                                            vec![val, fmt_expr.clone()],
                                        ))))
                                    }
                                }
                                DialectType::BigQuery => {
                                    // STRPTIME(val, fmt) -> PARSE_TIMESTAMP(bq_fmt, val) - note reversed arg order
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let bq_fmt = duckdb_to_bigquery_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "PARSE_TIMESTAMP".to_string(),
                                                vec![Expression::string(&bq_fmt), val],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "PARSE_TIMESTAMP".to_string(),
                                                vec![fmt_expr.clone(), val],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "PARSE_TIMESTAMP".to_string(),
                                            vec![fmt_expr.clone(), val],
                                        ))))
                                    }
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // DATE_FORMAT(val, fmt) from Presto source (C-style format) -> target-specific
                        "DATE_FORMAT"
                            if f.args.len() >= 2
                                && matches!(
                                    source,
                                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                                ) =>
                        {
                            let val = f.args[0].clone();
                            let fmt_expr = &f.args[1];

                            match target {
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // Presto -> Presto: normalize format (e.g., %H:%i:%S -> %T)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let normalized = normalize_presto_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_FORMAT".to_string(),
                                                vec![val, Expression::string(&normalized)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(f))
                                        }
                                    } else {
                                        Ok(Expression::Function(f))
                                    }
                                }
                                DialectType::Hive
                                | DialectType::Spark
                                | DialectType::Databricks => {
                                    // Convert Presto C-style to Java-style format
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let java_fmt = presto_to_java_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_FORMAT".to_string(),
                                                vec![val, Expression::string(&java_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(f))
                                        }
                                    } else {
                                        Ok(Expression::Function(f))
                                    }
                                }
                                DialectType::DuckDB => {
                                    // Convert to STRFTIME(val, duckdb_fmt)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let duckdb_fmt = presto_to_duckdb_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "STRFTIME".to_string(),
                                                vec![val, Expression::string(&duckdb_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "STRFTIME".to_string(),
                                                vec![val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "STRFTIME".to_string(),
                                            vec![val, fmt_expr.clone()],
                                        ))))
                                    }
                                }
                                DialectType::BigQuery => {
                                    // Convert to FORMAT_DATE(bq_fmt, val) - reversed args
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let bq_fmt = presto_to_bigquery_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "FORMAT_DATE".to_string(),
                                                vec![Expression::string(&bq_fmt), val],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "FORMAT_DATE".to_string(),
                                                vec![fmt_expr.clone(), val],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "FORMAT_DATE".to_string(),
                                            vec![fmt_expr.clone(), val],
                                        ))))
                                    }
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // DATE_PARSE(val, fmt) from Presto source -> target-specific parse function
                        "DATE_PARSE"
                            if f.args.len() >= 2
                                && matches!(
                                    source,
                                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                                ) =>
                        {
                            let val = f.args[0].clone();
                            let fmt_expr = &f.args[1];

                            match target {
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // Presto -> Presto: normalize format
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let normalized = normalize_presto_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_PARSE".to_string(),
                                                vec![val, Expression::string(&normalized)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(f))
                                        }
                                    } else {
                                        Ok(Expression::Function(f))
                                    }
                                }
                                DialectType::Hive => {
                                    // Presto -> Hive: if default format, just CAST(x AS TIMESTAMP)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            if is_default_presto_timestamp_format(s)
                                                || is_default_presto_date_format(s)
                                            {
                                                Ok(Expression::Cast(Box::new(
                                                    crate::expressions::Cast {
                                                        this: val,
                                                        to: DataType::Timestamp {
                                                            timezone: false,
                                                            precision: None,
                                                        },
                                                        trailing_comments: Vec::new(),
                                                        double_colon_syntax: false,
                                                        format: None,
                                                        default: None,
                                                        inferred_type: None,
                                                    },
                                                )))
                                            } else {
                                                let java_fmt = presto_to_java_format(s);
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "TO_TIMESTAMP".to_string(),
                                                    vec![val, Expression::string(&java_fmt)],
                                                ))))
                                            }
                                        } else {
                                            Ok(Expression::Function(f))
                                        }
                                    } else {
                                        Ok(Expression::Function(f))
                                    }
                                }
                                DialectType::Spark | DialectType::Databricks => {
                                    // Presto -> Spark: TO_TIMESTAMP(val, java_fmt)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let java_fmt = presto_to_java_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "TO_TIMESTAMP".to_string(),
                                                vec![val, Expression::string(&java_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(f))
                                        }
                                    } else {
                                        Ok(Expression::Function(f))
                                    }
                                }
                                DialectType::DuckDB => {
                                    // Presto -> DuckDB: STRPTIME(val, duckdb_fmt)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let duckdb_fmt = presto_to_duckdb_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "STRPTIME".to_string(),
                                                vec![val, Expression::string(&duckdb_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "STRPTIME".to_string(),
                                                vec![val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "STRPTIME".to_string(),
                                            vec![val, fmt_expr.clone()],
                                        ))))
                                    }
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // FROM_BASE64(x) / TO_BASE64(x) from Presto -> Hive-specific renames
                        "FROM_BASE64"
                            if f.args.len() == 1 && matches!(target, DialectType::Hive) =>
                        {
                            Ok(Expression::Function(Box::new(Function::new(
                                "UNBASE64".to_string(),
                                f.args,
                            ))))
                        }
                        "TO_BASE64" if f.args.len() == 1 && matches!(target, DialectType::Hive) => {
                            Ok(Expression::Function(Box::new(Function::new(
                                "BASE64".to_string(),
                                f.args,
                            ))))
                        }
                        // FROM_UNIXTIME(x) -> CAST(FROM_UNIXTIME(x) AS TIMESTAMP) for Spark
                        "FROM_UNIXTIME"
                            if f.args.len() == 1
                                && matches!(
                                    source,
                                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                                )
                                && matches!(
                                    target,
                                    DialectType::Spark | DialectType::Databricks
                                ) =>
                        {
                            // Wrap FROM_UNIXTIME(x) in CAST(... AS TIMESTAMP)
                            let from_unix = Expression::Function(Box::new(Function::new(
                                "FROM_UNIXTIME".to_string(),
                                f.args,
                            )));
                            Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                                this: from_unix,
                                to: DataType::Timestamp {
                                    timezone: false,
                                    precision: None,
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        // DATE_FORMAT(val, fmt) from Hive/Spark/MySQL -> target-specific format function
                        "DATE_FORMAT"
                            if f.args.len() >= 2
                                && !matches!(
                                    target,
                                    DialectType::Hive
                                        | DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::MySQL
                                        | DialectType::SingleStore
                                ) =>
                        {
                            let val = f.args[0].clone();
                            let fmt_expr = &f.args[1];
                            let is_hive_source = matches!(
                                source,
                                DialectType::Hive | DialectType::Spark | DialectType::Databricks
                            );

                            fn java_to_c_format(fmt: &str) -> String {
                                // Replace Java patterns with C strftime patterns.
                                // Uses multi-pass to handle patterns that conflict.
                                // First pass: replace multi-char patterns (longer first)
                                let result = fmt
                                    .replace("yyyy", "%Y")
                                    .replace("SSSSSS", "%f")
                                    .replace("EEEE", "%W")
                                    .replace("MM", "%m")
                                    .replace("dd", "%d")
                                    .replace("HH", "%H")
                                    .replace("mm", "%M")
                                    .replace("ss", "%S")
                                    .replace("yy", "%y");
                                // Second pass: handle single-char timezone patterns
                                // z -> %Z (timezone name), Z -> %z (timezone offset)
                                // Must be careful not to replace 'z'/'Z' inside already-replaced %Y, %M etc.
                                let mut out = String::new();
                                let chars: Vec<char> = result.chars().collect();
                                let mut i = 0;
                                while i < chars.len() {
                                    if chars[i] == '%' && i + 1 < chars.len() {
                                        // Already a format specifier, skip both chars
                                        out.push(chars[i]);
                                        out.push(chars[i + 1]);
                                        i += 2;
                                    } else if chars[i] == 'z' {
                                        out.push_str("%Z");
                                        i += 1;
                                    } else if chars[i] == 'Z' {
                                        out.push_str("%z");
                                        i += 1;
                                    } else {
                                        out.push(chars[i]);
                                        i += 1;
                                    }
                                }
                                out
                            }

                            fn java_to_presto_format(fmt: &str) -> String {
                                // Presto uses %T for HH:MM:SS
                                let c_fmt = java_to_c_format(fmt);
                                c_fmt.replace("%H:%M:%S", "%T")
                            }

                            fn java_to_bq_format(fmt: &str) -> String {
                                // BigQuery uses %F for yyyy-MM-dd and %T for HH:mm:ss
                                let c_fmt = java_to_c_format(fmt);
                                c_fmt.replace("%Y-%m-%d", "%F").replace("%H:%M:%S", "%T")
                            }

                            // For Hive source, CAST string literals to appropriate type
                            let cast_val = if is_hive_source {
                                match &val {
                                    Expression::Literal(lit)
                                        if matches!(
                                            lit.as_ref(),
                                            crate::expressions::Literal::String(_)
                                        ) =>
                                    {
                                        match target {
                                            DialectType::DuckDB
                                            | DialectType::Presto
                                            | DialectType::Trino
                                            | DialectType::Athena => {
                                                temporal::ensure_cast_timestamp(val.clone())
                                            }
                                            DialectType::BigQuery => {
                                                // BigQuery: CAST(val AS DATETIME)
                                                Expression::Cast(Box::new(
                                                    crate::expressions::Cast {
                                                        this: val.clone(),
                                                        to: DataType::Custom {
                                                            name: "DATETIME".to_string(),
                                                        },
                                                        trailing_comments: vec![],
                                                        double_colon_syntax: false,
                                                        format: None,
                                                        default: None,
                                                        inferred_type: None,
                                                    },
                                                ))
                                            }
                                            _ => val.clone(),
                                        }
                                    }
                                    // For CAST(x AS DATE) or DATE literal, Presto needs CAST(CAST(x AS DATE) AS TIMESTAMP)
                                    Expression::Cast(c)
                                        if matches!(c.to, DataType::Date)
                                            && matches!(
                                                target,
                                                DialectType::Presto
                                                    | DialectType::Trino
                                                    | DialectType::Athena
                                            ) =>
                                    {
                                        Expression::Cast(Box::new(crate::expressions::Cast {
                                            this: val.clone(),
                                            to: DataType::Timestamp {
                                                timezone: false,
                                                precision: None,
                                            },
                                            trailing_comments: vec![],
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    }
                                    Expression::Literal(lit)
                                        if matches!(
                                            lit.as_ref(),
                                            crate::expressions::Literal::Date(_)
                                        ) && matches!(
                                            target,
                                            DialectType::Presto
                                                | DialectType::Trino
                                                | DialectType::Athena
                                        ) =>
                                    {
                                        // DATE 'x' -> CAST(CAST('x' AS DATE) AS TIMESTAMP)
                                        let cast_date = temporal::date_literal_to_cast(val.clone());
                                        Expression::Cast(Box::new(crate::expressions::Cast {
                                            this: cast_date,
                                            to: DataType::Timestamp {
                                                timezone: false,
                                                precision: None,
                                            },
                                            trailing_comments: vec![],
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    }
                                    _ => val.clone(),
                                }
                            } else {
                                val.clone()
                            };

                            match target {
                                DialectType::DuckDB => {
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let c_fmt = if is_hive_source {
                                                java_to_c_format(s)
                                            } else {
                                                s.clone()
                                            };
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "STRFTIME".to_string(),
                                                vec![cast_val, Expression::string(&c_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "STRFTIME".to_string(),
                                                vec![cast_val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "STRFTIME".to_string(),
                                            vec![cast_val, fmt_expr.clone()],
                                        ))))
                                    }
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    if is_hive_source {
                                        if let Expression::Literal(lit) = fmt_expr {
                                            if let crate::expressions::Literal::String(s) =
                                                lit.as_ref()
                                            {
                                                let p_fmt = java_to_presto_format(s);
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "DATE_FORMAT".to_string(),
                                                    vec![cast_val, Expression::string(&p_fmt)],
                                                ))))
                                            } else {
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "DATE_FORMAT".to_string(),
                                                    vec![cast_val, fmt_expr.clone()],
                                                ))))
                                            }
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_FORMAT".to_string(),
                                                vec![cast_val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_FORMAT".to_string(),
                                            f.args,
                                        ))))
                                    }
                                }
                                DialectType::BigQuery => {
                                    // DATE_FORMAT(val, fmt) -> FORMAT_DATE(fmt, val)
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let bq_fmt = if is_hive_source {
                                                java_to_bq_format(s)
                                            } else {
                                                java_to_c_format(s)
                                            };
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "FORMAT_DATE".to_string(),
                                                vec![Expression::string(&bq_fmt), cast_val],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "FORMAT_DATE".to_string(),
                                                vec![fmt_expr.clone(), cast_val],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "FORMAT_DATE".to_string(),
                                            vec![fmt_expr.clone(), cast_val],
                                        ))))
                                    }
                                }
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    if let Expression::Literal(lit) = fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let pg_fmt = s
                                                .replace("yyyy", "YYYY")
                                                .replace("MM", "MM")
                                                .replace("dd", "DD")
                                                .replace("HH", "HH24")
                                                .replace("mm", "MI")
                                                .replace("ss", "SS")
                                                .replace("yy", "YY");
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "TO_CHAR".to_string(),
                                                vec![val, Expression::string(&pg_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "TO_CHAR".to_string(),
                                                vec![val, fmt_expr.clone()],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "TO_CHAR".to_string(),
                                            vec![val, fmt_expr.clone()],
                                        ))))
                                    }
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // DATEDIFF(unit, start, end) - 3-arg form
                        // SQLite uses DATEDIFF(date1, date2, unit_string) instead
                        "DATEDIFF" if f.args.len() == 3 => {
                            let mut args = f.args;
                            // SQLite source: args = (date1, date2, unit_string)
                            // Standard source: args = (unit, start, end)
                            let (_arg0, arg1, arg2, unit_str) =
                                if matches!(source, DialectType::SQLite) {
                                    let date1 = args.remove(0);
                                    let date2 = args.remove(0);
                                    let unit_expr = args.remove(0);
                                    let unit_s = temporal::get_unit_str_static(&unit_expr);

                                    // For SQLite target, generate JULIANDAY arithmetic directly
                                    if matches!(target, DialectType::SQLite) {
                                        let jd_first = Expression::Function(Box::new(
                                            Function::new("JULIANDAY".to_string(), vec![date1]),
                                        ));
                                        let jd_second = Expression::Function(Box::new(
                                            Function::new("JULIANDAY".to_string(), vec![date2]),
                                        ));
                                        let diff = Expression::Sub(Box::new(
                                            crate::expressions::BinaryOp::new(jd_first, jd_second),
                                        ));
                                        let paren_diff = Expression::Paren(Box::new(
                                            crate::expressions::Paren {
                                                this: diff,
                                                trailing_comments: Vec::new(),
                                            },
                                        ));
                                        let adjusted = match unit_s.as_str() {
                                            "HOUR" => Expression::Mul(Box::new(
                                                crate::expressions::BinaryOp::new(
                                                    paren_diff,
                                                    Expression::Literal(Box::new(Literal::Number(
                                                        "24.0".to_string(),
                                                    ))),
                                                ),
                                            )),
                                            "MINUTE" => Expression::Mul(Box::new(
                                                crate::expressions::BinaryOp::new(
                                                    paren_diff,
                                                    Expression::Literal(Box::new(Literal::Number(
                                                        "1440.0".to_string(),
                                                    ))),
                                                ),
                                            )),
                                            "SECOND" => Expression::Mul(Box::new(
                                                crate::expressions::BinaryOp::new(
                                                    paren_diff,
                                                    Expression::Literal(Box::new(Literal::Number(
                                                        "86400.0".to_string(),
                                                    ))),
                                                ),
                                            )),
                                            "MONTH" => Expression::Div(Box::new(
                                                crate::expressions::BinaryOp::new(
                                                    paren_diff,
                                                    Expression::Literal(Box::new(Literal::Number(
                                                        "30.0".to_string(),
                                                    ))),
                                                ),
                                            )),
                                            "YEAR" => Expression::Div(Box::new(
                                                crate::expressions::BinaryOp::new(
                                                    paren_diff,
                                                    Expression::Literal(Box::new(Literal::Number(
                                                        "365.0".to_string(),
                                                    ))),
                                                ),
                                            )),
                                            _ => paren_diff,
                                        };
                                        return Ok(Expression::Cast(Box::new(Cast {
                                            this: adjusted,
                                            to: DataType::Int {
                                                length: None,
                                                integer_spelling: true,
                                            },
                                            trailing_comments: vec![],
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        })));
                                    }

                                    // For other targets, remap to standard (unit, start, end) form
                                    let unit_ident =
                                        Expression::Identifier(Identifier::new(&unit_s));
                                    (unit_ident, date1, date2, unit_s)
                                } else {
                                    let arg0 = args.remove(0);
                                    let arg1 = args.remove(0);
                                    let arg2 = args.remove(0);
                                    let unit_s = temporal::get_unit_str_static(&arg0);
                                    (arg0, arg1, arg2, unit_s)
                                };

                            // For Hive/Spark source, string literal dates need to be cast
                            // Note: Databricks is excluded - it handles string args like standard SQL
                            let is_hive_spark =
                                matches!(source, DialectType::Hive | DialectType::Spark);

                            match target {
                                DialectType::Snowflake => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    // Use ensure_to_date_preserved to add TO_DATE with a marker
                                    // that prevents the Snowflake TO_DATE handler from converting it to CAST
                                    let d1 = if is_hive_spark {
                                        temporal::ensure_to_date_preserved(arg1)
                                    } else {
                                        arg1
                                    };
                                    let d2 = if is_hive_spark {
                                        temporal::ensure_to_date_preserved(arg2)
                                    } else {
                                        arg2
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, d1, d2],
                                    ))))
                                }
                                DialectType::Redshift => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    let d1 = if is_hive_spark {
                                        temporal::ensure_cast_date(arg1)
                                    } else {
                                        arg1
                                    };
                                    let d2 = if is_hive_spark {
                                        temporal::ensure_cast_date(arg2)
                                    } else {
                                        arg2
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, d1, d2],
                                    ))))
                                }
                                DialectType::TSQL => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    let is_redshift_tsql =
                                        matches!(source, DialectType::Redshift | DialectType::TSQL);
                                    if is_hive_spark {
                                        // For Hive/Spark source, CAST string args to DATE and emit DATE_DIFF directly
                                        let d1 = temporal::ensure_cast_date(arg1);
                                        let d2 = temporal::ensure_cast_date(arg2);
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_DIFF".to_string(),
                                            vec![Expression::string(&unit_str), d1, d2],
                                        ))))
                                    } else if matches!(source, DialectType::Snowflake) {
                                        // For Snowflake source: special handling per unit
                                        match unit_str.as_str() {
                                            "NANOSECOND" => {
                                                // DATEDIFF(NANOSECOND, start, end) -> EPOCH_NS(CAST(end AS TIMESTAMP_NS)) - EPOCH_NS(CAST(start AS TIMESTAMP_NS))
                                                fn cast_to_timestamp_ns(
                                                    expr: Expression,
                                                ) -> Expression
                                                {
                                                    Expression::Cast(Box::new(Cast {
                                                        this: expr,
                                                        to: DataType::Custom {
                                                            name: "TIMESTAMP_NS".to_string(),
                                                        },
                                                        trailing_comments: vec![],
                                                        double_colon_syntax: false,
                                                        format: None,
                                                        default: None,
                                                        inferred_type: None,
                                                    }))
                                                }
                                                let epoch_end =
                                                    Expression::Function(Box::new(Function::new(
                                                        "EPOCH_NS".to_string(),
                                                        vec![cast_to_timestamp_ns(arg2)],
                                                    )));
                                                let epoch_start =
                                                    Expression::Function(Box::new(Function::new(
                                                        "EPOCH_NS".to_string(),
                                                        vec![cast_to_timestamp_ns(arg1)],
                                                    )));
                                                Ok(Expression::Sub(Box::new(BinaryOp::new(
                                                    epoch_end,
                                                    epoch_start,
                                                ))))
                                            }
                                            "WEEK" => {
                                                // DATE_DIFF('WEEK', DATE_TRUNC('WEEK', CAST(x AS DATE)), DATE_TRUNC('WEEK', CAST(y AS DATE)))
                                                let d1 = temporal::force_cast_date(arg1);
                                                let d2 = temporal::force_cast_date(arg2);
                                                let dt1 =
                                                    Expression::Function(Box::new(Function::new(
                                                        "DATE_TRUNC".to_string(),
                                                        vec![Expression::string("WEEK"), d1],
                                                    )));
                                                let dt2 =
                                                    Expression::Function(Box::new(Function::new(
                                                        "DATE_TRUNC".to_string(),
                                                        vec![Expression::string("WEEK"), d2],
                                                    )));
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "DATE_DIFF".to_string(),
                                                    vec![Expression::string(&unit_str), dt1, dt2],
                                                ))))
                                            }
                                            _ => {
                                                // YEAR, MONTH, QUARTER, DAY, etc.: CAST to DATE
                                                let d1 = temporal::force_cast_date(arg1);
                                                let d2 = temporal::force_cast_date(arg2);
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "DATE_DIFF".to_string(),
                                                    vec![Expression::string(&unit_str), d1, d2],
                                                ))))
                                            }
                                        }
                                    } else if is_redshift_tsql {
                                        // For Redshift/TSQL source, CAST args to TIMESTAMP (always)
                                        let d1 = temporal::force_cast_timestamp(arg1);
                                        let d2 = temporal::force_cast_timestamp(arg2);
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_DIFF".to_string(),
                                            vec![Expression::string(&unit_str), d1, d2],
                                        ))))
                                    } else {
                                        // Keep as DATEDIFF so DuckDB's transform_datediff handles
                                        // DATE_TRUNC for WEEK, CAST for string literals, etc.
                                        let unit =
                                            Expression::Identifier(Identifier::new(&unit_str));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATEDIFF".to_string(),
                                            vec![unit, arg1, arg2],
                                        ))))
                                    }
                                }
                                DialectType::BigQuery => {
                                    let is_redshift_tsql = matches!(
                                        source,
                                        DialectType::Redshift
                                            | DialectType::TSQL
                                            | DialectType::Snowflake
                                    );
                                    let cast_d1 = if is_hive_spark {
                                        temporal::ensure_cast_date(arg1)
                                    } else if is_redshift_tsql {
                                        temporal::force_cast_datetime(arg1)
                                    } else {
                                        temporal::ensure_cast_datetime(arg1)
                                    };
                                    let cast_d2 = if is_hive_spark {
                                        temporal::ensure_cast_date(arg2)
                                    } else if is_redshift_tsql {
                                        temporal::force_cast_datetime(arg2)
                                    } else {
                                        temporal::ensure_cast_datetime(arg2)
                                    };
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![cast_d2, cast_d1, unit],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // For Hive/Spark source, string literals need double-cast: CAST(CAST(x AS TIMESTAMP) AS DATE)
                                    // For Redshift/TSQL source, args need CAST to TIMESTAMP (always)
                                    let is_redshift_tsql = matches!(
                                        source,
                                        DialectType::Redshift
                                            | DialectType::TSQL
                                            | DialectType::Snowflake
                                    );
                                    let d1 = if is_hive_spark {
                                        temporal::double_cast_timestamp_date(arg1)
                                    } else if is_redshift_tsql {
                                        temporal::force_cast_timestamp(arg1)
                                    } else {
                                        arg1
                                    };
                                    let d2 = if is_hive_spark {
                                        temporal::double_cast_timestamp_date(arg2)
                                    } else if is_redshift_tsql {
                                        temporal::force_cast_timestamp(arg2)
                                    } else {
                                        arg2
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![Expression::string(&unit_str), d1, d2],
                                    ))))
                                }
                                DialectType::Hive => match unit_str.as_str() {
                                    "MONTH" => Ok(Expression::Cast(Box::new(Cast {
                                        this: Expression::Function(Box::new(Function::new(
                                            "MONTHS_BETWEEN".to_string(),
                                            vec![arg2, arg1],
                                        ))),
                                        to: DataType::Int {
                                            length: None,
                                            integer_spelling: false,
                                        },
                                        trailing_comments: vec![],
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }))),
                                    "WEEK" => Ok(Expression::Cast(Box::new(Cast {
                                        this: Expression::Div(Box::new(
                                            crate::expressions::BinaryOp::new(
                                                Expression::Function(Box::new(Function::new(
                                                    "DATEDIFF".to_string(),
                                                    vec![arg2, arg1],
                                                ))),
                                                Expression::number(7),
                                            ),
                                        )),
                                        to: DataType::Int {
                                            length: None,
                                            integer_spelling: false,
                                        },
                                        trailing_comments: vec![],
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }))),
                                    _ => Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![arg2, arg1],
                                    )))),
                                },
                                DialectType::Spark | DialectType::Databricks => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                _ => {
                                    // For Hive/Spark source targeting PostgreSQL etc., cast string literals to DATE
                                    let d1 = if is_hive_spark {
                                        temporal::ensure_cast_date(arg1)
                                    } else {
                                        arg1
                                    };
                                    let d2 = if is_hive_spark {
                                        temporal::ensure_cast_date(arg2)
                                    } else {
                                        arg2
                                    };
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, d1, d2],
                                    ))))
                                }
                            }
                        }
                        // DATEDIFF(end, start) - 2-arg form from Hive/MySQL
                        "DATEDIFF" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let arg0 = args.remove(0);
                            let arg1 = args.remove(0);

                            // Helper: unwrap TO_DATE(x) -> x (extracts inner arg)
                            // Also recognizes TryCast/Cast to DATE that may have been produced by
                            // cross-dialect TO_DATE -> TRY_CAST conversion
                            let unwrap_to_date = |e: Expression| -> (Expression, bool) {
                                if let Expression::Function(ref f) = e {
                                    if f.name.eq_ignore_ascii_case("TO_DATE") && f.args.len() == 1 {
                                        return (f.args[0].clone(), true);
                                    }
                                }
                                // Also recognize TryCast(x, Date) as an already-converted TO_DATE
                                if let Expression::TryCast(ref c) = e {
                                    if matches!(c.to, DataType::Date) {
                                        return (e, true); // Already properly cast, return as-is
                                    }
                                }
                                (e, false)
                            };

                            match target {
                                DialectType::DuckDB => {
                                    // For Hive source, always CAST to DATE
                                    // If arg is TO_DATE(x) or TRY_CAST(x AS DATE), use it directly
                                    let cast_d0 = if matches!(
                                        source,
                                        DialectType::Hive
                                            | DialectType::Spark
                                            | DialectType::Databricks
                                    ) {
                                        let (inner, was_to_date) = unwrap_to_date(arg1);
                                        if was_to_date {
                                            // Already a date expression, use directly
                                            if matches!(&inner, Expression::TryCast(_)) {
                                                inner // Already TRY_CAST(x AS DATE)
                                            } else {
                                                temporal::try_cast_date(inner)
                                            }
                                        } else {
                                            temporal::force_cast_date(inner)
                                        }
                                    } else {
                                        temporal::ensure_cast_date(arg1)
                                    };
                                    let cast_d1 = if matches!(
                                        source,
                                        DialectType::Hive
                                            | DialectType::Spark
                                            | DialectType::Databricks
                                    ) {
                                        let (inner, was_to_date) = unwrap_to_date(arg0);
                                        if was_to_date {
                                            if matches!(&inner, Expression::TryCast(_)) {
                                                inner
                                            } else {
                                                temporal::try_cast_date(inner)
                                            }
                                        } else {
                                            temporal::force_cast_date(inner)
                                        }
                                    } else {
                                        temporal::ensure_cast_date(arg0)
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![Expression::string("DAY"), cast_d0, cast_d1],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // For Hive/Spark source, apply double_cast_timestamp_date
                                    // For other sources (MySQL etc.), just swap args without casting
                                    if matches!(
                                        source,
                                        DialectType::Hive
                                            | DialectType::Spark
                                            | DialectType::Databricks
                                    ) {
                                        let cast_fn = |e: Expression| -> Expression {
                                            let (inner, was_to_date) = unwrap_to_date(e);
                                            if was_to_date {
                                                let first_cast =
                                                    temporal::double_cast_timestamp_date(inner);
                                                temporal::double_cast_timestamp_date(first_cast)
                                            } else {
                                                temporal::double_cast_timestamp_date(inner)
                                            }
                                        };
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_DIFF".to_string(),
                                            vec![
                                                Expression::string("DAY"),
                                                cast_fn(arg1),
                                                cast_fn(arg0),
                                            ],
                                        ))))
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_DIFF".to_string(),
                                            vec![Expression::string("DAY"), arg1, arg0],
                                        ))))
                                    }
                                }
                                DialectType::Redshift => {
                                    let unit = Expression::Identifier(Identifier::new("DAY"));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, arg1, arg0],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "DATEDIFF".to_string(),
                                    vec![arg0, arg1],
                                )))),
                            }
                        }
                        // DATE_DIFF(unit, start, end) - 3-arg with string unit (ClickHouse/DuckDB style)
                        "DATE_DIFF" if f.args.len() == 3 => {
                            let mut args = f.args;
                            let arg0 = args.remove(0);
                            let arg1 = args.remove(0);
                            let arg2 = args.remove(0);
                            let unit_str = temporal::get_unit_str_static(&arg0);

                            match target {
                                DialectType::DuckDB => {
                                    // DuckDB: DATE_DIFF('UNIT', start, end)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![Expression::string(&unit_str), arg1, arg2],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![Expression::string(&unit_str), arg1, arg2],
                                    ))))
                                }
                                DialectType::ClickHouse => {
                                    // ClickHouse: DATE_DIFF(UNIT, start, end) - identifier unit
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                DialectType::Snowflake | DialectType::Redshift => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                _ => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                            }
                        }
                        // DATEADD(unit, val, date) - 3-arg form
                        "DATEADD" if f.args.len() == 3 => {
                            let mut args = f.args;
                            let arg0 = args.remove(0);
                            let arg1 = args.remove(0);
                            let arg2 = args.remove(0);
                            let unit_str = temporal::get_unit_str_static(&arg0);

                            // Normalize TSQL unit abbreviations to standard names
                            let unit_str = match unit_str.as_str() {
                                "YY" | "YYYY" => "YEAR".to_string(),
                                "QQ" | "Q" => "QUARTER".to_string(),
                                "MM" | "M" => "MONTH".to_string(),
                                "WK" | "WW" => "WEEK".to_string(),
                                "DD" | "D" | "DY" => "DAY".to_string(),
                                "HH" => "HOUR".to_string(),
                                "MI" | "N" => "MINUTE".to_string(),
                                "SS" | "S" => "SECOND".to_string(),
                                "MS" => "MILLISECOND".to_string(),
                                "MCS" | "US" => "MICROSECOND".to_string(),
                                _ => unit_str,
                            };
                            match target {
                                DialectType::Snowflake => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    // Cast string literal to TIMESTAMP, but not for Snowflake source
                                    // (Snowflake natively accepts string literals in DATEADD)
                                    let arg2 = if matches!(
                                        &arg2,
                                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_))
                                    ) && !matches!(source, DialectType::Snowflake)
                                    {
                                        Expression::Cast(Box::new(Cast {
                                            this: arg2,
                                            to: DataType::Timestamp {
                                                precision: None,
                                                timezone: false,
                                            },
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    } else {
                                        arg2
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                DialectType::TSQL => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    // Cast string literal to DATETIME2, but not when source is Spark/Databricks family
                                    let arg2 = if matches!(
                                        &arg2,
                                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_))
                                    ) && !matches!(
                                        source,
                                        DialectType::Spark
                                            | DialectType::Databricks
                                            | DialectType::Hive
                                    ) {
                                        Expression::Cast(Box::new(Cast {
                                            this: arg2,
                                            to: DataType::Custom {
                                                name: "DATETIME2".to_string(),
                                            },
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    } else {
                                        arg2
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                DialectType::Redshift => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                DialectType::Databricks => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    // Sources with native DATEADD (TSQL, Databricks, Snowflake) -> DATEADD
                                    // Other sources (Redshift TsOrDsAdd, etc.) -> DATE_ADD
                                    let func_name = if matches!(
                                        source,
                                        DialectType::TSQL
                                            | DialectType::Fabric
                                            | DialectType::Databricks
                                            | DialectType::Snowflake
                                    ) {
                                        "DATEADD"
                                    } else {
                                        "DATE_ADD"
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    // Special handling for NANOSECOND from Snowflake
                                    if unit_str == "NANOSECOND"
                                        && matches!(source, DialectType::Snowflake)
                                    {
                                        // DATEADD(NANOSECOND, offset, ts) -> MAKE_TIMESTAMP_NS(EPOCH_NS(CAST(ts AS TIMESTAMP_NS)) + offset)
                                        let cast_ts = Expression::Cast(Box::new(Cast {
                                            this: arg2,
                                            to: DataType::Custom {
                                                name: "TIMESTAMP_NS".to_string(),
                                            },
                                            trailing_comments: vec![],
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }));
                                        let epoch_ns = Expression::Function(Box::new(
                                            Function::new("EPOCH_NS".to_string(), vec![cast_ts]),
                                        ));
                                        let sum = Expression::Add(Box::new(BinaryOp::new(
                                            epoch_ns, arg1,
                                        )));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "MAKE_TIMESTAMP_NS".to_string(),
                                            vec![sum],
                                        ))))
                                    } else {
                                        // DuckDB: convert to date + INTERVAL syntax with CAST
                                        let iu = temporal::parse_interval_unit_static(&unit_str);
                                        let interval = Expression::Interval(Box::new(
                                            crate::expressions::Interval {
                                                this: Some(arg1),
                                                unit: Some(
                                                    crate::expressions::IntervalUnitSpec::Simple {
                                                        unit: iu,
                                                        use_plural: false,
                                                    },
                                                ),
                                            },
                                        ));
                                        // Cast string literal to TIMESTAMP
                                        let arg2 = if matches!(
                                            &arg2,
                                            Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_))
                                        ) {
                                            Expression::Cast(Box::new(Cast {
                                                this: arg2,
                                                to: DataType::Timestamp {
                                                    precision: None,
                                                    timezone: false,
                                                },
                                                trailing_comments: Vec::new(),
                                                double_colon_syntax: false,
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }))
                                        } else {
                                            arg2
                                        };
                                        Ok(Expression::Add(Box::new(
                                            crate::expressions::BinaryOp::new(arg2, interval),
                                        )))
                                    }
                                }
                                DialectType::Spark => {
                                    // For TSQL source: convert to ADD_MONTHS/DATE_ADD(date, val)
                                    // For other sources: keep 3-arg DATE_ADD(UNIT, val, date) form
                                    if matches!(source, DialectType::TSQL | DialectType::Fabric) {
                                        fn multiply_expr_spark(
                                            expr: Expression,
                                            factor: i64,
                                        ) -> Expression {
                                            if let Expression::Literal(lit) = &expr {
                                                if let crate::expressions::Literal::Number(n) =
                                                    lit.as_ref()
                                                {
                                                    if let Ok(val) = n.parse::<i64>() {
                                                        return Expression::Literal(Box::new(
                                                            crate::expressions::Literal::Number(
                                                                (val * factor).to_string(),
                                                            ),
                                                        ));
                                                    }
                                                }
                                            }
                                            Expression::Mul(Box::new(
                                                crate::expressions::BinaryOp::new(
                                                    expr,
                                                    Expression::Literal(Box::new(
                                                        crate::expressions::Literal::Number(
                                                            factor.to_string(),
                                                        ),
                                                    )),
                                                ),
                                            ))
                                        }
                                        let normalized_unit = match unit_str.as_str() {
                                            "YEAR" | "YY" | "YYYY" => "YEAR",
                                            "QUARTER" | "QQ" | "Q" => "QUARTER",
                                            "MONTH" | "MM" | "M" => "MONTH",
                                            "WEEK" | "WK" | "WW" => "WEEK",
                                            "DAY" | "DD" | "D" | "DY" => "DAY",
                                            _ => &unit_str,
                                        };
                                        match normalized_unit {
                                            "YEAR" => {
                                                let months = multiply_expr_spark(arg1, 12);
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "ADD_MONTHS".to_string(),
                                                    vec![arg2, months],
                                                ))))
                                            }
                                            "QUARTER" => {
                                                let months = multiply_expr_spark(arg1, 3);
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "ADD_MONTHS".to_string(),
                                                    vec![arg2, months],
                                                ))))
                                            }
                                            "MONTH" => {
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "ADD_MONTHS".to_string(),
                                                    vec![arg2, arg1],
                                                ))))
                                            }
                                            "WEEK" => {
                                                let days = multiply_expr_spark(arg1, 7);
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "DATE_ADD".to_string(),
                                                    vec![arg2, days],
                                                ))))
                                            }
                                            "DAY" => {
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "DATE_ADD".to_string(),
                                                    vec![arg2, arg1],
                                                ))))
                                            }
                                            _ => {
                                                let unit = Expression::Identifier(Identifier::new(
                                                    &unit_str,
                                                ));
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "DATE_ADD".to_string(),
                                                    vec![unit, arg1, arg2],
                                                ))))
                                            }
                                        }
                                    } else {
                                        // Non-TSQL source: keep 3-arg DATE_ADD(UNIT, val, date)
                                        let unit =
                                            Expression::Identifier(Identifier::new(&unit_str));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_ADD".to_string(),
                                            vec![unit, arg1, arg2],
                                        ))))
                                    }
                                }
                                DialectType::Hive => match unit_str.as_str() {
                                    "MONTH" => Ok(Expression::Function(Box::new(Function::new(
                                        "ADD_MONTHS".to_string(),
                                        vec![arg2, arg1],
                                    )))),
                                    _ => Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![arg2, arg1],
                                    )))),
                                },
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // Cast string literal date to TIMESTAMP
                                    let arg2 = if matches!(
                                        &arg2,
                                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_))
                                    ) {
                                        Expression::Cast(Box::new(Cast {
                                            this: arg2,
                                            to: DataType::Timestamp {
                                                precision: None,
                                                timezone: false,
                                            },
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    } else {
                                        arg2
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![Expression::string(&unit_str), arg1, arg2],
                                    ))))
                                }
                                DialectType::MySQL => {
                                    let iu = temporal::parse_interval_unit_static(&unit_str);
                                    Ok(Expression::DateAdd(Box::new(
                                        crate::expressions::DateAddFunc {
                                            this: arg2,
                                            interval: arg1,
                                            unit: iu,
                                        },
                                    )))
                                }
                                DialectType::PostgreSQL => {
                                    // Cast string literal date to TIMESTAMP
                                    let arg2 = if matches!(
                                        &arg2,
                                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_))
                                    ) {
                                        Expression::Cast(Box::new(Cast {
                                            this: arg2,
                                            to: DataType::Timestamp {
                                                precision: None,
                                                timezone: false,
                                            },
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    } else {
                                        arg2
                                    };
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::string(&format!(
                                                "{} {}",
                                                expr_to_string_static(&arg1),
                                                unit_str
                                            ))),
                                            unit: None,
                                        },
                                    ));
                                    Ok(Expression::Add(Box::new(
                                        crate::expressions::BinaryOp::new(arg2, interval),
                                    )))
                                }
                                DialectType::BigQuery => {
                                    let iu = temporal::parse_interval_unit_static(&unit_str);
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(arg1),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: iu,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    // Non-TSQL sources: CAST string literal to DATETIME
                                    let arg2 = if !matches!(
                                        source,
                                        DialectType::TSQL | DialectType::Fabric
                                    ) && matches!(
                                        &arg2,
                                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_))
                                    ) {
                                        Expression::Cast(Box::new(Cast {
                                            this: arg2,
                                            to: DataType::Custom {
                                                name: "DATETIME".to_string(),
                                            },
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        }))
                                    } else {
                                        arg2
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![arg2, interval],
                                    ))))
                                }
                                _ => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                            }
                        }
                        // DATE_ADD - 3-arg: either (unit, val, date) from Presto/ClickHouse
                        // or (date, val, 'UNIT') from Generic canonical form
                        "DATE_ADD" if f.args.len() == 3 => {
                            let mut args = f.args;
                            let arg0 = args.remove(0);
                            let arg1 = args.remove(0);
                            let arg2 = args.remove(0);
                            // Detect Generic canonical form: DATE_ADD(date, amount, 'UNIT')
                            // where arg2 is a string literal matching a unit name
                            let arg2_unit = match &arg2 {
                                Expression::Literal(lit)
                                    if matches!(lit.as_ref(), Literal::String(_)) =>
                                {
                                    let Literal::String(s) = lit.as_ref() else {
                                        unreachable!()
                                    };
                                    let u = s.to_ascii_uppercase();
                                    if matches!(
                                        u.as_str(),
                                        "DAY"
                                            | "MONTH"
                                            | "YEAR"
                                            | "HOUR"
                                            | "MINUTE"
                                            | "SECOND"
                                            | "WEEK"
                                            | "QUARTER"
                                            | "MILLISECOND"
                                            | "MICROSECOND"
                                    ) {
                                        Some(u)
                                    } else {
                                        None
                                    }
                                }
                                _ => None,
                            };
                            // Reorder: if arg2 is the unit, swap to (unit, val, date) form
                            let (unit_str, val, date) = if let Some(u) = arg2_unit {
                                (u, arg1, arg0)
                            } else {
                                (temporal::get_unit_str_static(&arg0), arg1, arg2)
                            };
                            // Alias for backward compat with the rest of the match
                            let arg1 = val;
                            let arg2 = date;

                            match target {
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![Expression::string(&unit_str), arg1, arg2],
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    let iu = temporal::parse_interval_unit_static(&unit_str);
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(arg1),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: iu,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Add(Box::new(
                                        crate::expressions::BinaryOp::new(arg2, interval),
                                    )))
                                }
                                DialectType::PostgreSQL
                                | DialectType::Materialize
                                | DialectType::RisingWave => {
                                    // PostgreSQL: x + INTERVAL '1 DAY'
                                    let amount_str = expr_to_string_static(&arg1);
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::string(&format!(
                                                "{} {}",
                                                amount_str, unit_str
                                            ))),
                                            unit: None,
                                        },
                                    ));
                                    Ok(Expression::Add(Box::new(
                                        crate::expressions::BinaryOp::new(arg2, interval),
                                    )))
                                }
                                DialectType::Snowflake
                                | DialectType::TSQL
                                | DialectType::Redshift => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                DialectType::BigQuery
                                | DialectType::MySQL
                                | DialectType::Doris
                                | DialectType::StarRocks
                                | DialectType::Drill => {
                                    // DATE_ADD(date, INTERVAL amount UNIT)
                                    let iu = temporal::parse_interval_unit_static(&unit_str);
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(arg1),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: iu,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![arg2, interval],
                                    ))))
                                }
                                DialectType::SQLite => {
                                    // SQLite: DATE(x, '1 DAY')
                                    // Build the string '1 DAY' from amount and unit
                                    let amount_str = match &arg1 {
                                        Expression::Literal(lit)
                                            if matches!(lit.as_ref(), Literal::Number(_)) =>
                                        {
                                            let Literal::Number(n) = lit.as_ref() else {
                                                unreachable!()
                                            };
                                            n.clone()
                                        }
                                        _ => "1".to_string(),
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE".to_string(),
                                        vec![
                                            arg2,
                                            Expression::string(format!(
                                                "{} {}",
                                                amount_str, unit_str
                                            )),
                                        ],
                                    ))))
                                }
                                DialectType::Dremio => {
                                    // Dremio: DATE_ADD(date, amount) - drops unit
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![arg2, arg1],
                                    ))))
                                }
                                DialectType::Spark => {
                                    // Spark: DATE_ADD(date, val) for DAY, or DATEADD(UNIT, val, date)
                                    if unit_str == "DAY" {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_ADD".to_string(),
                                            vec![arg2, arg1],
                                        ))))
                                    } else {
                                        let unit =
                                            Expression::Identifier(Identifier::new(&unit_str));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_ADD".to_string(),
                                            vec![unit, arg1, arg2],
                                        ))))
                                    }
                                }
                                DialectType::Databricks => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                                DialectType::Hive => {
                                    // Hive: DATE_ADD(date, val) for DAY
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![arg2, arg1],
                                    ))))
                                }
                                _ => {
                                    let unit = Expression::Identifier(Identifier::new(&unit_str));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![unit, arg1, arg2],
                                    ))))
                                }
                            }
                        }
                        // DATE_ADD(date, days) - 2-arg Hive/Spark/Generic form (add days)
                        "DATE_ADD"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::Hive
                                        | DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Generic
                                ) =>
                        {
                            let mut args = f.args;
                            let date = args.remove(0);
                            let days = args.remove(0);
                            match target {
                                DialectType::Hive | DialectType::Spark => {
                                    // Keep as DATE_ADD(date, days) for Hive/Spark
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![date, days],
                                    ))))
                                }
                                DialectType::Databricks => Ok(Expression::Function(Box::new(
                                    Function::new("DATE_ADD".to_string(), vec![date, days]),
                                ))),
                                DialectType::DuckDB => {
                                    // DuckDB: CAST(date AS DATE) + INTERVAL days DAY
                                    let cast_date = temporal::ensure_cast_date(date);
                                    // Wrap complex expressions (like Mul from DATE_SUB negation) in Paren
                                    let interval_val = if matches!(
                                        days,
                                        Expression::Mul(_)
                                            | Expression::Sub(_)
                                            | Expression::Add(_)
                                    ) {
                                        Expression::Paren(Box::new(crate::expressions::Paren {
                                            this: days,
                                            trailing_comments: vec![],
                                        }))
                                    } else {
                                        days
                                    };
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(interval_val),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Add(Box::new(
                                        crate::expressions::BinaryOp::new(cast_date, interval),
                                    )))
                                }
                                DialectType::Snowflake => {
                                    // For Hive source with string literal date, use CAST(CAST(date AS TIMESTAMP) AS DATE)
                                    let cast_date = if matches!(
                                        source,
                                        DialectType::Hive
                                            | DialectType::Spark
                                            | DialectType::Databricks
                                    ) {
                                        if matches!(
                                            date,
                                            Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(_))
                                        ) {
                                            temporal::double_cast_timestamp_date(date)
                                        } else {
                                            date
                                        }
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new("DAY")),
                                            days,
                                            cast_date,
                                        ],
                                    ))))
                                }
                                DialectType::Redshift => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new("DAY")),
                                            days,
                                            date,
                                        ],
                                    ))))
                                }
                                DialectType::TSQL | DialectType::Fabric => {
                                    // For Hive source with string literal date, use CAST(CAST(date AS DATETIME2) AS DATE)
                                    // But Databricks DATE_ADD doesn't need this wrapping for TSQL
                                    let cast_date = if matches!(
                                        source,
                                        DialectType::Hive
                                            | DialectType::Spark
                                            | DialectType::Databricks
                                    ) {
                                        if matches!(
                                            date,
                                            Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(_))
                                        ) {
                                            temporal::double_cast_datetime2_date(date)
                                        } else {
                                            date
                                        }
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new("DAY")),
                                            days,
                                            cast_date,
                                        ],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // For Hive source with string literal date, use CAST(CAST(date AS TIMESTAMP) AS DATE)
                                    let cast_date = if matches!(
                                        source,
                                        DialectType::Hive
                                            | DialectType::Spark
                                            | DialectType::Databricks
                                    ) {
                                        if matches!(
                                            date,
                                            Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(_))
                                        ) {
                                            temporal::double_cast_timestamp_date(date)
                                        } else {
                                            date
                                        }
                                    } else {
                                        date
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![Expression::string("DAY"), days, cast_date],
                                    ))))
                                }
                                DialectType::BigQuery => {
                                    // For Hive/Spark source, wrap date in CAST(CAST(date AS DATETIME) AS DATE)
                                    let cast_date = if matches!(
                                        source,
                                        DialectType::Hive
                                            | DialectType::Spark
                                            | DialectType::Databricks
                                    ) {
                                        temporal::double_cast_datetime_date(date)
                                    } else {
                                        date
                                    };
                                    // Wrap complex expressions in Paren for interval
                                    let interval_val = if matches!(
                                        days,
                                        Expression::Mul(_)
                                            | Expression::Sub(_)
                                            | Expression::Add(_)
                                    ) {
                                        Expression::Paren(Box::new(crate::expressions::Paren {
                                            this: days,
                                            trailing_comments: vec![],
                                        }))
                                    } else {
                                        days
                                    };
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(interval_val),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![cast_date, interval],
                                    ))))
                                }
                                DialectType::MySQL => {
                                    let iu = crate::expressions::IntervalUnit::Day;
                                    Ok(Expression::DateAdd(Box::new(
                                        crate::expressions::DateAddFunc {
                                            this: date,
                                            interval: days,
                                            unit: iu,
                                        },
                                    )))
                                }
                                DialectType::PostgreSQL => {
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::string(&format!(
                                                "{} DAY",
                                                expr_to_string_static(&days)
                                            ))),
                                            unit: None,
                                        },
                                    ));
                                    Ok(Expression::Add(Box::new(
                                        crate::expressions::BinaryOp::new(date, interval),
                                    )))
                                }
                                DialectType::Doris
                                | DialectType::StarRocks
                                | DialectType::Drill => {
                                    // DATE_ADD(date, INTERVAL days DAY)
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(days),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![date, interval],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_ADD".to_string(),
                                    vec![date, days],
                                )))),
                            }
                        }
                        // DATE_ADD(date, INTERVAL val UNIT) - MySQL 2-arg form with INTERVAL as 2nd arg
                        "DATE_ADD"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::MySQL | DialectType::SingleStore
                                )
                                && matches!(&f.args[1], Expression::Interval(_)) =>
                        {
                            let mut args = f.args;
                            let date = args.remove(0);
                            let interval_expr = args.remove(0);
                            let (val, unit) = Dialect::extract_interval_parts(&interval_expr)
                                .unwrap_or_else(|| {
                                    (interval_expr.clone(), crate::expressions::IntervalUnit::Day)
                                });
                            let unit_str = temporal::interval_unit_to_string(&unit);
                            let is_literal = matches!(&val,
                                Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(_) | Literal::String(_))
                            );

                            match target {
                                DialectType::MySQL | DialectType::SingleStore => {
                                    // Keep as DATE_ADD(date, INTERVAL val UNIT)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![date, interval_expr],
                                    ))))
                                }
                                DialectType::PostgreSQL => {
                                    if is_literal {
                                        // Literal: date + INTERVAL 'val UNIT'
                                        let interval = Expression::Interval(Box::new(
                                            crate::expressions::Interval {
                                                this: Some(Expression::Literal(Box::new(
                                                    Literal::String(format!(
                                                        "{} {}",
                                                        expr_to_string(&val),
                                                        unit_str
                                                    )),
                                                ))),
                                                unit: None,
                                            },
                                        ));
                                        Ok(Expression::Add(Box::new(
                                            crate::expressions::BinaryOp::new(date, interval),
                                        )))
                                    } else {
                                        // Non-literal (column ref): date + INTERVAL '1 UNIT' * val
                                        let interval_one = Expression::Interval(Box::new(
                                            crate::expressions::Interval {
                                                this: Some(Expression::Literal(Box::new(
                                                    Literal::String(format!("1 {}", unit_str)),
                                                ))),
                                                unit: None,
                                            },
                                        ));
                                        let mul = Expression::Mul(Box::new(
                                            crate::expressions::BinaryOp::new(interval_one, val),
                                        ));
                                        Ok(Expression::Add(Box::new(
                                            crate::expressions::BinaryOp::new(date, mul),
                                        )))
                                    }
                                }
                                _ => {
                                    // Default: keep as DATE_ADD(date, interval)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![date, interval_expr],
                                    ))))
                                }
                            }
                        }
                        // DATE_SUB(date, days) - 2-arg Hive/Spark form (subtract days)
                        "DATE_SUB"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::Hive
                                        | DialectType::Spark
                                        | DialectType::Databricks
                                ) =>
                        {
                            let mut args = f.args;
                            let date = args.remove(0);
                            let days = args.remove(0);
                            // Helper to create days * -1
                            let make_neg_days = |d: Expression| -> Expression {
                                Expression::Mul(Box::new(crate::expressions::BinaryOp::new(
                                    d,
                                    Expression::Literal(Box::new(Literal::Number(
                                        "-1".to_string(),
                                    ))),
                                )))
                            };
                            let is_string_literal = matches!(date, Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(_)));
                            match target {
                                DialectType::Hive
                                | DialectType::Spark
                                | DialectType::Databricks => {
                                    // Keep as DATE_SUB(date, days) for Hive/Spark
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_SUB".to_string(),
                                        vec![date, days],
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    let cast_date = temporal::ensure_cast_date(date);
                                    let neg = make_neg_days(days);
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::Paren(Box::new(
                                                crate::expressions::Paren {
                                                    this: neg,
                                                    trailing_comments: vec![],
                                                },
                                            ))),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Add(Box::new(
                                        crate::expressions::BinaryOp::new(cast_date, interval),
                                    )))
                                }
                                DialectType::Snowflake => {
                                    let cast_date = if is_string_literal {
                                        temporal::double_cast_timestamp_date(date)
                                    } else {
                                        date
                                    };
                                    let neg = make_neg_days(days);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new("DAY")),
                                            neg,
                                            cast_date,
                                        ],
                                    ))))
                                }
                                DialectType::Redshift => {
                                    let neg = make_neg_days(days);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new("DAY")),
                                            neg,
                                            date,
                                        ],
                                    ))))
                                }
                                DialectType::TSQL | DialectType::Fabric => {
                                    let cast_date = if is_string_literal {
                                        temporal::double_cast_datetime2_date(date)
                                    } else {
                                        date
                                    };
                                    let neg = make_neg_days(days);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new("DAY")),
                                            neg,
                                            cast_date,
                                        ],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    let cast_date = if is_string_literal {
                                        temporal::double_cast_timestamp_date(date)
                                    } else {
                                        date
                                    };
                                    let neg = make_neg_days(days);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![Expression::string("DAY"), neg, cast_date],
                                    ))))
                                }
                                DialectType::BigQuery => {
                                    let cast_date = if is_string_literal {
                                        temporal::double_cast_datetime_date(date)
                                    } else {
                                        date
                                    };
                                    let neg = make_neg_days(days);
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::Paren(Box::new(
                                                crate::expressions::Paren {
                                                    this: neg,
                                                    trailing_comments: vec![],
                                                },
                                            ))),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![cast_date, interval],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_SUB".to_string(),
                                    vec![date, days],
                                )))),
                            }
                        }
                        // ADD_MONTHS(date, val) -> target-specific
                        "ADD_MONTHS" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let date = args.remove(0);
                            let val = args.remove(0);
                            match target {
                                DialectType::TSQL => {
                                    let cast_date = temporal::ensure_cast_datetime2(date);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new("MONTH")),
                                            val,
                                            cast_date,
                                        ],
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(val),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Month,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Add(Box::new(
                                        crate::expressions::BinaryOp::new(date, interval),
                                    )))
                                }
                                DialectType::Snowflake => {
                                    // Keep ADD_MONTHS when source is Snowflake
                                    if matches!(source, DialectType::Snowflake) {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "ADD_MONTHS".to_string(),
                                            vec![date, val],
                                        ))))
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATEADD".to_string(),
                                            vec![
                                                Expression::Identifier(Identifier::new("MONTH")),
                                                val,
                                                date,
                                            ],
                                        ))))
                                    }
                                }
                                DialectType::Redshift => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEADD".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new("MONTH")),
                                            val,
                                            date,
                                        ],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![Expression::string("MONTH"), val, date],
                                    ))))
                                }
                                DialectType::BigQuery => {
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(val),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Month,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_ADD".to_string(),
                                        vec![date, interval],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "ADD_MONTHS".to_string(),
                                    vec![date, val],
                                )))),
                            }
                        }
                        // DATETRUNC(unit, date) - TSQL form -> DATE_TRUNC for other targets
                        "DATETRUNC" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let arg0 = args.remove(0);
                            let arg1 = args.remove(0);
                            let unit_str = temporal::get_unit_str_static(&arg0);
                            match target {
                                DialectType::TSQL | DialectType::Fabric => {
                                    // Keep as DATETRUNC for TSQL - the target handler will uppercase the unit
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATETRUNC".to_string(),
                                        vec![
                                            Expression::Identifier(Identifier::new(&unit_str)),
                                            arg1,
                                        ],
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    // DuckDB: DATE_TRUNC('UNIT', expr) with CAST for string literals
                                    let date = temporal::ensure_cast_timestamp(arg1);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_TRUNC".to_string(),
                                        vec![Expression::string(&unit_str), date],
                                    ))))
                                }
                                DialectType::ClickHouse => {
                                    // ClickHouse: dateTrunc('UNIT', expr)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "dateTrunc".to_string(),
                                        vec![Expression::string(&unit_str), arg1],
                                    ))))
                                }
                                _ => {
                                    // Standard: DATE_TRUNC('UNIT', expr)
                                    let unit = Expression::string(&unit_str);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_TRUNC".to_string(),
                                        vec![unit, arg1],
                                    ))))
                                }
                            }
                        }
                        // GETDATE() -> CURRENT_TIMESTAMP for non-TSQL targets
                        "GETDATE" if f.args.is_empty() => match target {
                            DialectType::TSQL => Ok(Expression::Function(f)),
                            DialectType::Redshift => Ok(Expression::Function(Box::new(
                                Function::new("GETDATE".to_string(), vec![]),
                            ))),
                            _ => Ok(Expression::CurrentTimestamp(
                                crate::expressions::CurrentTimestamp {
                                    precision: None,
                                    sysdate: false,
                                },
                            )),
                        },
                        // TO_HEX(x) / HEX(x) -> target-specific hex function
                        "TO_HEX" | "HEX" if f.args.len() == 1 => {
                            let name = match target {
                                DialectType::Presto | DialectType::Trino => "TO_HEX",
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "HEX",
                                DialectType::DuckDB
                                | DialectType::PostgreSQL
                                | DialectType::Redshift => "TO_HEX",
                                _ => &f.name,
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // FROM_HEX(x) / UNHEX(x) -> target-specific hex decode function
                        "FROM_HEX" | "UNHEX" if f.args.len() == 1 => {
                            match target {
                                DialectType::BigQuery => {
                                    // BigQuery: UNHEX(x) -> FROM_HEX(x)
                                    // Special case: UNHEX(MD5(x)) -> FROM_HEX(TO_HEX(MD5(x)))
                                    // because BigQuery MD5 returns BYTES, not hex string
                                    let arg = &f.args[0];
                                    let wrapped_arg = match arg {
                                        Expression::Function(inner_f)
                                            if inner_f.name.eq_ignore_ascii_case("MD5")
                                                || inner_f.name.eq_ignore_ascii_case("SHA1")
                                                || inner_f.name.eq_ignore_ascii_case("SHA256")
                                                || inner_f.name.eq_ignore_ascii_case("SHA512") =>
                                        {
                                            // Wrap hash function in TO_HEX for BigQuery
                                            Expression::Function(Box::new(Function::new(
                                                "TO_HEX".to_string(),
                                                vec![arg.clone()],
                                            )))
                                        }
                                        _ => f.args.into_iter().next().unwrap(),
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "FROM_HEX".to_string(),
                                        vec![wrapped_arg],
                                    ))))
                                }
                                _ => {
                                    let name = match target {
                                        DialectType::Presto | DialectType::Trino => "FROM_HEX",
                                        DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Hive => "UNHEX",
                                        _ => &f.name,
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        name.to_string(),
                                        f.args,
                                    ))))
                                }
                            }
                        }
                        // TO_UTF8(x) -> ENCODE(x, 'utf-8') for Spark
                        "TO_UTF8" if f.args.len() == 1 => match target {
                            DialectType::Spark | DialectType::Databricks => {
                                let mut args = f.args;
                                args.push(Expression::string("utf-8"));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "ENCODE".to_string(),
                                    args,
                                ))))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // FROM_UTF8(x) -> DECODE(x, 'utf-8') for Spark
                        "FROM_UTF8" if f.args.len() == 1 => match target {
                            DialectType::Spark | DialectType::Databricks => {
                                let mut args = f.args;
                                args.push(Expression::string("utf-8"));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DECODE".to_string(),
                                    args,
                                ))))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // STARTS_WITH(x, y) / STARTSWITH(x, y) -> target-specific
                        "STARTS_WITH" | "STARTSWITH" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::Spark | DialectType::Databricks => "STARTSWITH",
                                DialectType::Presto | DialectType::Trino => "STARTS_WITH",
                                DialectType::PostgreSQL | DialectType::Redshift => "STARTS_WITH",
                                _ => &f.name,
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // APPROX_COUNT_DISTINCT(x) <-> APPROX_DISTINCT(x)
                        "APPROX_COUNT_DISTINCT" if f.args.len() >= 1 => {
                            let name = match target {
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    "APPROX_DISTINCT"
                                }
                                _ => "APPROX_COUNT_DISTINCT",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // JSON_EXTRACT -> GET_JSON_OBJECT for Spark/Hive
                        "JSON_EXTRACT"
                            if f.args.len() == 2
                                && !matches!(source, DialectType::BigQuery)
                                && matches!(
                                    target,
                                    DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Hive
                                ) =>
                        {
                            Ok(Expression::Function(Box::new(Function::new(
                                "GET_JSON_OBJECT".to_string(),
                                f.args,
                            ))))
                        }
                        // JSON_EXTRACT(x, path) -> x -> path for SQLite (arrow syntax)
                        "JSON_EXTRACT"
                            if f.args.len() == 2 && matches!(target, DialectType::SQLite) =>
                        {
                            let mut args = f.args;
                            let path = args.remove(1);
                            let this = args.remove(0);
                            Ok(Expression::JsonExtract(Box::new(
                                crate::expressions::JsonExtractFunc {
                                    this,
                                    path,
                                    returning: None,
                                    arrow_syntax: true,
                                    hash_arrow_syntax: false,
                                    wrapper_option: None,
                                    quotes_option: None,
                                    on_scalar_string: false,
                                    on_error: None,
                                },
                            )))
                        }
                        // JSON_FORMAT(x) -> TO_JSON(x) for Spark, TO_JSON_STRING for BigQuery, CAST(TO_JSON(x) AS TEXT) for DuckDB
                        "JSON_FORMAT" if f.args.len() == 1 => {
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    // Presto JSON_FORMAT(JSON '...') needs Spark's string-unquoting flow:
                                    // REGEXP_EXTRACT(TO_JSON(FROM_JSON('[...]', SCHEMA_OF_JSON('[...]'))), '^.(.*).$', 1)
                                    if matches!(
                                        source,
                                        DialectType::Presto
                                            | DialectType::Trino
                                            | DialectType::Athena
                                    ) {
                                        if let Some(Expression::ParseJson(pj)) = f.args.first() {
                                            if let Expression::Literal(lit) = &pj.this {
                                                if let Literal::String(s) = lit.as_ref() {
                                                    let wrapped = Expression::Literal(Box::new(
                                                        Literal::String(format!("[{}]", s)),
                                                    ));
                                                    let schema_of_json = Expression::Function(
                                                        Box::new(Function::new(
                                                            "SCHEMA_OF_JSON".to_string(),
                                                            vec![wrapped.clone()],
                                                        )),
                                                    );
                                                    let from_json = Expression::Function(Box::new(
                                                        Function::new(
                                                            "FROM_JSON".to_string(),
                                                            vec![wrapped, schema_of_json],
                                                        ),
                                                    ));
                                                    let to_json = Expression::Function(Box::new(
                                                        Function::new(
                                                            "TO_JSON".to_string(),
                                                            vec![from_json],
                                                        ),
                                                    ));
                                                    return Ok(Expression::Function(Box::new(
                                                        Function::new(
                                                            "REGEXP_EXTRACT".to_string(),
                                                            vec![
                                                                to_json,
                                                                Expression::Literal(Box::new(
                                                                    Literal::String(
                                                                        "^.(.*).$".to_string(),
                                                                    ),
                                                                )),
                                                                Expression::Literal(Box::new(
                                                                    Literal::Number(
                                                                        "1".to_string(),
                                                                    ),
                                                                )),
                                                            ],
                                                        ),
                                                    )));
                                                }
                                            }
                                        }
                                    }

                                    // Strip inner CAST(... AS JSON) or TO_JSON() if present
                                    // The CastToJsonForSpark may have already converted CAST(x AS JSON) to TO_JSON(x)
                                    let mut args = f.args;
                                    if let Some(Expression::Cast(ref c)) = args.first() {
                                        if matches!(&c.to, DataType::Json | DataType::JsonB) {
                                            args = vec![c.this.clone()];
                                        }
                                    } else if let Some(Expression::Function(ref inner_f)) =
                                        args.first()
                                    {
                                        if inner_f.name.eq_ignore_ascii_case("TO_JSON")
                                            && inner_f.args.len() == 1
                                        {
                                            // Already TO_JSON(x) from CastToJsonForSpark, just use the inner arg
                                            args = inner_f.args.clone();
                                        }
                                    }
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_JSON".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::BigQuery => Ok(Expression::Function(Box::new(
                                    Function::new("TO_JSON_STRING".to_string(), f.args),
                                ))),
                                DialectType::DuckDB => {
                                    // CAST(TO_JSON(x) AS TEXT)
                                    let to_json = Expression::Function(Box::new(Function::new(
                                        "TO_JSON".to_string(),
                                        f.args,
                                    )));
                                    Ok(Expression::Cast(Box::new(Cast {
                                        this: to_json,
                                        to: DataType::Text,
                                        trailing_comments: Vec::new(),
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // SYSDATE -> CURRENT_TIMESTAMP for non-Oracle/Redshift/Snowflake targets
                        "SYSDATE" if f.args.is_empty() => {
                            match target {
                                DialectType::Oracle | DialectType::Redshift => {
                                    Ok(Expression::Function(f))
                                }
                                DialectType::Snowflake => {
                                    // Snowflake uses SYSDATE() with parens
                                    let mut f = *f;
                                    f.no_parens = false;
                                    Ok(Expression::Function(Box::new(f)))
                                }
                                DialectType::DuckDB => {
                                    // DuckDB: SYSDATE() -> CURRENT_TIMESTAMP AT TIME ZONE 'UTC'
                                    Ok(Expression::AtTimeZone(Box::new(
                                        crate::expressions::AtTimeZone {
                                            this: Expression::CurrentTimestamp(
                                                crate::expressions::CurrentTimestamp {
                                                    precision: None,
                                                    sysdate: false,
                                                },
                                            ),
                                            zone: Expression::Literal(Box::new(Literal::String(
                                                "UTC".to_string(),
                                            ))),
                                        },
                                    )))
                                }
                                _ => Ok(Expression::CurrentTimestamp(
                                    crate::expressions::CurrentTimestamp {
                                        precision: None,
                                        sysdate: true,
                                    },
                                )),
                            }
                        }
                        // LOGICAL_OR(x) -> BOOL_OR(x)
                        "LOGICAL_OR" if f.args.len() == 1 => {
                            let name = match target {
                                DialectType::Spark | DialectType::Databricks => "BOOL_OR",
                                _ => &f.name,
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // LOGICAL_AND(x) -> BOOL_AND(x)
                        "LOGICAL_AND" if f.args.len() == 1 => {
                            let name = match target {
                                DialectType::Spark | DialectType::Databricks => "BOOL_AND",
                                _ => &f.name,
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // MONTHS_ADD(d, n) -> ADD_MONTHS(d, n) for Oracle
                        "MONTHS_ADD" if f.args.len() == 2 => match target {
                            DialectType::Oracle => Ok(Expression::Function(Box::new(
                                Function::new("ADD_MONTHS".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // ARRAY_JOIN(arr, sep[, null_replacement]) -> target-specific
                        "ARRAY_JOIN" if f.args.len() >= 2 => {
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    // Keep as ARRAY_JOIN for Spark (it supports null_replacement)
                                    Ok(Expression::Function(f))
                                }
                                DialectType::Hive => {
                                    // ARRAY_JOIN(arr, sep[, null_rep]) -> CONCAT_WS(sep, arr) (drop null_replacement)
                                    let mut args = f.args;
                                    let arr = args.remove(0);
                                    let sep = args.remove(0);
                                    // Drop any remaining args (null_replacement)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "CONCAT_WS".to_string(),
                                        vec![sep, arr],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino => {
                                    Ok(Expression::Function(f))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // LOCATE(substr, str, pos) 3-arg -> target-specific
                        // For Presto/DuckDB: STRPOS doesn't support 3-arg, need complex expansion
                        "LOCATE"
                            if f.args.len() == 3
                                && matches!(
                                    target,
                                    DialectType::Presto
                                        | DialectType::Trino
                                        | DialectType::Athena
                                        | DialectType::DuckDB
                                ) =>
                        {
                            let mut args = f.args;
                            let substr = args.remove(0);
                            let string = args.remove(0);
                            let pos = args.remove(0);
                            // STRPOS(SUBSTRING(string, pos), substr)
                            let substring_call = Expression::Function(Box::new(Function::new(
                                "SUBSTRING".to_string(),
                                vec![string.clone(), pos.clone()],
                            )));
                            let strpos_call = Expression::Function(Box::new(Function::new(
                                "STRPOS".to_string(),
                                vec![substring_call, substr.clone()],
                            )));
                            // STRPOS(...) + pos - 1
                            let pos_adjusted =
                                Expression::Sub(Box::new(crate::expressions::BinaryOp::new(
                                    Expression::Add(Box::new(crate::expressions::BinaryOp::new(
                                        strpos_call.clone(),
                                        pos.clone(),
                                    ))),
                                    Expression::number(1),
                                )));
                            // STRPOS(...) = 0
                            let is_zero =
                                Expression::Eq(Box::new(crate::expressions::BinaryOp::new(
                                    strpos_call.clone(),
                                    Expression::number(0),
                                )));

                            match target {
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // IF(STRPOS(...) = 0, 0, STRPOS(...) + pos - 1)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "IF".to_string(),
                                        vec![is_zero, Expression::number(0), pos_adjusted],
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    // CASE WHEN STRPOS(...) = 0 THEN 0 ELSE STRPOS(...) + pos - 1 END
                                    Ok(Expression::Case(Box::new(crate::expressions::Case {
                                        operand: None,
                                        whens: vec![(is_zero, Expression::number(0))],
                                        else_: Some(pos_adjusted),
                                        comments: Vec::new(),
                                        inferred_type: None,
                                    })))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "LOCATE".to_string(),
                                    vec![substr, string, pos],
                                )))),
                            }
                        }
                        // STRPOS(haystack, needle, occurrence) 3-arg -> INSTR(haystack, needle, 1, occurrence)
                        "STRPOS"
                            if f.args.len() == 3
                                && matches!(
                                    target,
                                    DialectType::BigQuery
                                        | DialectType::Oracle
                                        | DialectType::Teradata
                                ) =>
                        {
                            let mut args = f.args;
                            let haystack = args.remove(0);
                            let needle = args.remove(0);
                            let occurrence = args.remove(0);
                            Ok(Expression::Function(Box::new(Function::new(
                                "INSTR".to_string(),
                                vec![haystack, needle, Expression::number(1), occurrence],
                            ))))
                        }
                        // SCHEMA_NAME(id) -> target-specific
                        "SCHEMA_NAME" if f.args.len() <= 1 => match target {
                            DialectType::MySQL | DialectType::SingleStore => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "SCHEMA".to_string(),
                                    vec![],
                                ))))
                            }
                            DialectType::PostgreSQL => Ok(Expression::CurrentSchema(Box::new(
                                crate::expressions::CurrentSchema { this: None },
                            ))),
                            DialectType::SQLite => Ok(Expression::string("main")),
                            _ => Ok(Expression::Function(f)),
                        },
                        // STRTOL(str, base) -> FROM_BASE(str, base) for Trino/Presto
                        "STRTOL" if f.args.len() == 2 => match target {
                            DialectType::Presto | DialectType::Trino => Ok(Expression::Function(
                                Box::new(Function::new("FROM_BASE".to_string(), f.args)),
                            )),
                            _ => Ok(Expression::Function(f)),
                        },
                        // EDITDIST3(a, b) -> LEVENSHTEIN(a, b) for Spark
                        "EDITDIST3" if f.args.len() == 2 => match target {
                            DialectType::Spark | DialectType::Databricks => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "LEVENSHTEIN".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // FORMAT(num, decimals) from MySQL -> DuckDB FORMAT('{:,.Xf}', num)
                        "FORMAT"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::MySQL | DialectType::SingleStore
                                )
                                && matches!(target, DialectType::DuckDB) =>
                        {
                            let mut args = f.args;
                            let num_expr = args.remove(0);
                            let decimals_expr = args.remove(0);
                            // Extract decimal count
                            let dec_count = match &decimals_expr {
                                Expression::Literal(lit)
                                    if matches!(lit.as_ref(), Literal::Number(_)) =>
                                {
                                    let Literal::Number(n) = lit.as_ref() else {
                                        unreachable!()
                                    };
                                    n.clone()
                                }
                                _ => "0".to_string(),
                            };
                            let fmt_str = format!("{{:,.{}f}}", dec_count);
                            Ok(Expression::Function(Box::new(Function::new(
                                "FORMAT".to_string(),
                                vec![Expression::string(&fmt_str), num_expr],
                            ))))
                        }
                        // FORMAT(x, fmt) from TSQL -> DATE_FORMAT for Spark, or expand short codes
                        "FORMAT"
                            if f.args.len() == 2
                                && matches!(source, DialectType::TSQL | DialectType::Fabric) =>
                        {
                            let val_expr = f.args[0].clone();
                            let fmt_expr = f.args[1].clone();
                            // Expand unambiguous .NET single-char date format shortcodes to full patterns.
                            // Only expand shortcodes that are NOT also valid numeric format specifiers.
                            // Ambiguous: d, D, f, F, g, G (used for both dates and numbers)
                            // Unambiguous date: m/M (Month day), t/T (Time), y/Y (Year month)
                            let (expanded_fmt, is_shortcode) = match &fmt_expr {
                                Expression::Literal(lit)
                                    if matches!(
                                        lit.as_ref(),
                                        crate::expressions::Literal::String(_)
                                    ) =>
                                {
                                    let crate::expressions::Literal::String(s) = lit.as_ref()
                                    else {
                                        unreachable!()
                                    };
                                    match s.as_str() {
                                        "m" | "M" => (Expression::string("MMMM d"), true),
                                        "t" => (Expression::string("h:mm tt"), true),
                                        "T" => (Expression::string("h:mm:ss tt"), true),
                                        "y" | "Y" => (Expression::string("MMMM yyyy"), true),
                                        _ => (fmt_expr.clone(), false),
                                    }
                                }
                                _ => (fmt_expr.clone(), false),
                            };
                            // Check if the format looks like a date format
                            let is_date_format = is_shortcode
                                || match &expanded_fmt {
                                    Expression::Literal(lit)
                                        if matches!(
                                            lit.as_ref(),
                                            crate::expressions::Literal::String(_)
                                        ) =>
                                    {
                                        let crate::expressions::Literal::String(s) = lit.as_ref()
                                        else {
                                            unreachable!()
                                        };
                                        // Date formats typically contain yyyy, MM, dd, MMMM, HH, etc.
                                        s.contains("yyyy")
                                            || s.contains("YYYY")
                                            || s.contains("MM")
                                            || s.contains("dd")
                                            || s.contains("MMMM")
                                            || s.contains("HH")
                                            || s.contains("hh")
                                            || s.contains("ss")
                                    }
                                    _ => false,
                                };
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    let func_name = if is_date_format {
                                        "DATE_FORMAT"
                                    } else {
                                        "FORMAT_NUMBER"
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        func_name.to_string(),
                                        vec![val_expr, expanded_fmt],
                                    ))))
                                }
                                _ => {
                                    // For TSQL and other targets, expand shortcodes but keep FORMAT
                                    if is_shortcode {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "FORMAT".to_string(),
                                            vec![val_expr, expanded_fmt],
                                        ))))
                                    } else {
                                        Ok(Expression::Function(f))
                                    }
                                }
                            }
                        }
                        // FORMAT('%s', x) from Trino/Presto -> target-specific
                        "FORMAT"
                            if f.args.len() >= 2
                                && matches!(
                                    source,
                                    DialectType::Trino | DialectType::Presto | DialectType::Athena
                                ) =>
                        {
                            let fmt_expr = f.args[0].clone();
                            let value_args: Vec<Expression> = f.args[1..].to_vec();
                            match target {
                                // DuckDB: replace %s with {} in format string
                                DialectType::DuckDB => {
                                    let new_fmt = match &fmt_expr {
                                        Expression::Literal(lit)
                                            if matches!(lit.as_ref(), Literal::String(_)) =>
                                        {
                                            let Literal::String(s) = lit.as_ref() else {
                                                unreachable!()
                                            };
                                            Expression::Literal(Box::new(Literal::String(
                                                s.replace("%s", "{}"),
                                            )))
                                        }
                                        _ => fmt_expr,
                                    };
                                    let mut args = vec![new_fmt];
                                    args.extend(value_args);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "FORMAT".to_string(),
                                        args,
                                    ))))
                                }
                                // Snowflake: FORMAT('%s', x) -> TO_CHAR(x) when just %s
                                DialectType::Snowflake => match &fmt_expr {
                                    Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(s) if s == "%s" && value_args.len() == 1) =>
                                    {
                                        let Literal::String(_) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "TO_CHAR".to_string(),
                                            value_args,
                                        ))))
                                    }
                                    _ => Ok(Expression::Function(f)),
                                },
                                // Default: keep FORMAT as-is
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // LIST_CONTAINS / LIST_HAS / ARRAY_CONTAINS -> target-specific
                        "LIST_CONTAINS" | "LIST_HAS" | "ARRAY_CONTAINS" if f.args.len() == 2 => {
                            // When coming from Snowflake source: ARRAY_CONTAINS(value, array)
                            // args[0]=value, args[1]=array. For DuckDB target, swap and add NULL-aware CASE.
                            if matches!(target, DialectType::DuckDB)
                                && matches!(source, DialectType::Snowflake)
                                && f.name.eq_ignore_ascii_case("ARRAY_CONTAINS")
                            {
                                let value = f.args[0].clone();
                                let array = f.args[1].clone();

                                // value IS NULL
                                let value_is_null =
                                    Expression::IsNull(Box::new(crate::expressions::IsNull {
                                        this: value.clone(),
                                        not: false,
                                        postfix_form: false,
                                    }));

                                // ARRAY_LENGTH(array)
                                let array_length = Expression::Function(Box::new(Function::new(
                                    "ARRAY_LENGTH".to_string(),
                                    vec![array.clone()],
                                )));
                                // LIST_COUNT(array)
                                let list_count = Expression::Function(Box::new(Function::new(
                                    "LIST_COUNT".to_string(),
                                    vec![array.clone()],
                                )));
                                // ARRAY_LENGTH(array) <> LIST_COUNT(array)
                                let neq = Expression::Neq(Box::new(crate::expressions::BinaryOp {
                                    left: array_length,
                                    right: list_count,
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                }));
                                // NULLIF(ARRAY_LENGTH(array) <> LIST_COUNT(array), FALSE)
                                let nullif =
                                    Expression::Nullif(Box::new(crate::expressions::Nullif {
                                        this: Box::new(neq),
                                        expression: Box::new(Expression::Boolean(
                                            crate::expressions::BooleanLiteral { value: false },
                                        )),
                                    }));

                                // ARRAY_CONTAINS(array, value) - DuckDB syntax: array first, value second
                                let array_contains = Expression::Function(Box::new(Function::new(
                                    "ARRAY_CONTAINS".to_string(),
                                    vec![array, value],
                                )));

                                // CASE WHEN value IS NULL THEN NULLIF(...) ELSE ARRAY_CONTAINS(array, value) END
                                return Ok(Expression::Case(Box::new(Case {
                                    operand: None,
                                    whens: vec![(value_is_null, nullif)],
                                    else_: Some(array_contains),
                                    comments: Vec::new(),
                                    inferred_type: None,
                                })));
                            }
                            match target {
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    // CASE WHEN needle IS NULL THEN NULL ELSE COALESCE(needle = ANY(arr), FALSE) END
                                    let arr = f.args[0].clone();
                                    let needle = f.args[1].clone();
                                    // Convert [] to ARRAY[] for PostgreSQL
                                    let pg_arr = match arr {
                                        Expression::Array(a) => Expression::ArrayFunc(Box::new(
                                            crate::expressions::ArrayConstructor {
                                                expressions: a.expressions,
                                                bracket_notation: false,
                                                use_list_keyword: false,
                                            },
                                        )),
                                        _ => arr,
                                    };
                                    // needle = ANY(arr) using the Any quantified expression
                                    let any_expr = Expression::Any(Box::new(
                                        crate::expressions::QuantifiedExpr {
                                            this: needle.clone(),
                                            subquery: pg_arr,
                                            op: Some(crate::expressions::QuantifiedOp::Eq),
                                        },
                                    ));
                                    let coalesce = Expression::Coalesce(Box::new(
                                        crate::expressions::VarArgFunc {
                                            expressions: vec![
                                                any_expr,
                                                Expression::Boolean(
                                                    crate::expressions::BooleanLiteral {
                                                        value: false,
                                                    },
                                                ),
                                            ],
                                            original_name: None,
                                            inferred_type: None,
                                        },
                                    ));
                                    let is_null_check =
                                        Expression::IsNull(Box::new(crate::expressions::IsNull {
                                            this: needle,
                                            not: false,
                                            postfix_form: false,
                                        }));
                                    Ok(Expression::Case(Box::new(Case {
                                        operand: None,
                                        whens: vec![(
                                            is_null_check,
                                            Expression::Null(crate::expressions::Null),
                                        )],
                                        else_: Some(coalesce),
                                        comments: Vec::new(),
                                        inferred_type: None,
                                    })))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "ARRAY_CONTAINS".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // LIST_HAS_ANY / ARRAY_HAS_ANY -> target-specific overlap operator
                        "LIST_HAS_ANY" | "ARRAY_HAS_ANY" if f.args.len() == 2 => {
                            match target {
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    // arr1 && arr2 with ARRAY[] syntax
                                    let mut args = f.args;
                                    let arr1 = args.remove(0);
                                    let arr2 = args.remove(0);
                                    let pg_arr1 = match arr1 {
                                        Expression::Array(a) => Expression::ArrayFunc(Box::new(
                                            crate::expressions::ArrayConstructor {
                                                expressions: a.expressions,
                                                bracket_notation: false,
                                                use_list_keyword: false,
                                            },
                                        )),
                                        _ => arr1,
                                    };
                                    let pg_arr2 = match arr2 {
                                        Expression::Array(a) => Expression::ArrayFunc(Box::new(
                                            crate::expressions::ArrayConstructor {
                                                expressions: a.expressions,
                                                bracket_notation: false,
                                                use_list_keyword: false,
                                            },
                                        )),
                                        _ => arr2,
                                    };
                                    Ok(Expression::ArrayOverlaps(Box::new(BinaryOp::new(
                                        pg_arr1, pg_arr2,
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    // DuckDB: arr1 && arr2 (native support)
                                    let mut args = f.args;
                                    let arr1 = args.remove(0);
                                    let arr2 = args.remove(0);
                                    Ok(Expression::ArrayOverlaps(Box::new(BinaryOp::new(
                                        arr1, arr2,
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "LIST_HAS_ANY".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // APPROX_QUANTILE(x, q) -> target-specific
                        "APPROX_QUANTILE" if f.args.len() == 2 => match target {
                            DialectType::Snowflake => Ok(Expression::Function(Box::new(
                                Function::new("APPROX_PERCENTILE".to_string(), f.args),
                            ))),
                            DialectType::DuckDB => Ok(Expression::Function(f)),
                            _ => Ok(Expression::Function(f)),
                        },
                        // MAKE_DATE(y, m, d) -> DATE(y, m, d) for BigQuery
                        "MAKE_DATE" if f.args.len() == 3 => match target {
                            DialectType::BigQuery => Ok(Expression::Function(Box::new(
                                Function::new("DATE".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // RANGE(start, end[, step]) -> target-specific
                        "RANGE" if f.args.len() >= 2 && !matches!(target, DialectType::DuckDB) => {
                            let start = f.args[0].clone();
                            let end = f.args[1].clone();
                            let step = f.args.get(2).cloned();
                            match target {
                                // Snowflake ARRAY_GENERATE_RANGE uses exclusive end (same as DuckDB RANGE),
                                // so just rename without adjusting the end argument.
                                DialectType::Snowflake => {
                                    let mut args = vec![start, end];
                                    if let Some(s) = step {
                                        args.push(s);
                                    }
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "ARRAY_GENERATE_RANGE".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::Spark | DialectType::Databricks => {
                                    // RANGE(start, end) -> SEQUENCE(start, end-1)
                                    // RANGE(start, end, step) -> SEQUENCE(start, end-step, step) when step constant
                                    // RANGE(start, start) -> ARRAY() (empty)
                                    // RANGE(start, end, 0) -> ARRAY() (empty)
                                    // When end is variable: IF((end - 1) <= start, ARRAY(), SEQUENCE(start, (end - 1)))

                                    // Check for constant args
                                    fn extract_i64(e: &Expression) -> Option<i64> {
                                        match e {
                                            Expression::Literal(lit)
                                                if matches!(lit.as_ref(), Literal::Number(_)) =>
                                            {
                                                let Literal::Number(n) = lit.as_ref() else {
                                                    unreachable!()
                                                };
                                                n.parse::<i64>().ok()
                                            }
                                            Expression::Neg(u) => {
                                                if let Expression::Literal(lit) = &u.this {
                                                    if let Literal::Number(n) = lit.as_ref() {
                                                        n.parse::<i64>().ok().map(|v| -v)
                                                    } else {
                                                        None
                                                    }
                                                } else {
                                                    None
                                                }
                                            }
                                            _ => None,
                                        }
                                    }
                                    let start_val = extract_i64(&start);
                                    let end_val = extract_i64(&end);
                                    let step_val = step.as_ref().and_then(|s| extract_i64(s));

                                    // Check for RANGE(x, x) or RANGE(x, y, 0) -> empty array
                                    if step_val == Some(0) {
                                        return Ok(Expression::Function(Box::new(Function::new(
                                            "ARRAY".to_string(),
                                            vec![],
                                        ))));
                                    }
                                    if let (Some(s), Some(e_val)) = (start_val, end_val) {
                                        if s == e_val {
                                            return Ok(Expression::Function(Box::new(
                                                Function::new("ARRAY".to_string(), vec![]),
                                            )));
                                        }
                                    }

                                    if let (Some(_s_val), Some(e_val)) = (start_val, end_val) {
                                        // All constants - compute new end = end - step (if step provided) or end - 1
                                        match step_val {
                                            Some(st) if st < 0 => {
                                                // Negative step: SEQUENCE(start, end - step, step)
                                                let new_end = e_val - st; // end - step (= end + |step|)
                                                let mut args =
                                                    vec![start, Expression::number(new_end)];
                                                if let Some(s) = step {
                                                    args.push(s);
                                                }
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "SEQUENCE".to_string(),
                                                    args,
                                                ))))
                                            }
                                            Some(st) => {
                                                let new_end = e_val - st;
                                                let mut args =
                                                    vec![start, Expression::number(new_end)];
                                                if let Some(s) = step {
                                                    args.push(s);
                                                }
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "SEQUENCE".to_string(),
                                                    args,
                                                ))))
                                            }
                                            None => {
                                                // No step: SEQUENCE(start, end - 1)
                                                let new_end = e_val - 1;
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "SEQUENCE".to_string(),
                                                    vec![start, Expression::number(new_end)],
                                                ))))
                                            }
                                        }
                                    } else {
                                        // Variable end: IF((end - 1) < start, ARRAY(), SEQUENCE(start, (end - 1)))
                                        let end_m1 = Expression::Sub(Box::new(BinaryOp::new(
                                            end.clone(),
                                            Expression::number(1),
                                        )));
                                        let cond = Expression::Lt(Box::new(BinaryOp::new(
                                            Expression::Paren(Box::new(Paren {
                                                this: end_m1.clone(),
                                                trailing_comments: Vec::new(),
                                            })),
                                            start.clone(),
                                        )));
                                        let empty = Expression::Function(Box::new(Function::new(
                                            "ARRAY".to_string(),
                                            vec![],
                                        )));
                                        let mut seq_args = vec![
                                            start,
                                            Expression::Paren(Box::new(Paren {
                                                this: end_m1,
                                                trailing_comments: Vec::new(),
                                            })),
                                        ];
                                        if let Some(s) = step {
                                            seq_args.push(s);
                                        }
                                        let seq = Expression::Function(Box::new(Function::new(
                                            "SEQUENCE".to_string(),
                                            seq_args,
                                        )));
                                        Ok(Expression::IfFunc(Box::new(
                                            crate::expressions::IfFunc {
                                                condition: cond,
                                                true_value: empty,
                                                false_value: Some(seq),
                                                original_name: None,
                                                inferred_type: None,
                                            },
                                        )))
                                    }
                                }
                                DialectType::SQLite => {
                                    // RANGE(start, end) -> GENERATE_SERIES(start, end)
                                    // The subquery wrapping is handled at the Alias level
                                    let mut args = vec![start, end];
                                    if let Some(s) = step {
                                        args.push(s);
                                    }
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "GENERATE_SERIES".to_string(),
                                        args,
                                    ))))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // ARRAY_REVERSE_SORT -> target-specific
                        // (handled above as well, but also need DuckDB self-normalization)
                        // MAP_FROM_ARRAYS(keys, values) -> target-specific map construction
                        "MAP_FROM_ARRAYS" if f.args.len() == 2 => match target {
                            DialectType::Snowflake => Ok(Expression::Function(Box::new(
                                Function::new("OBJECT_CONSTRUCT".to_string(), f.args),
                            ))),
                            DialectType::Spark | DialectType::Databricks => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "MAP_FROM_ARRAYS".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(Box::new(Function::new(
                                "MAP".to_string(),
                                f.args,
                            )))),
                        },
                        // VARIANCE(x) -> varSamp(x) for ClickHouse
                        "VARIANCE" if f.args.len() == 1 => match target {
                            DialectType::ClickHouse => Ok(Expression::Function(Box::new(
                                Function::new("varSamp".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // STDDEV(x) -> stddevSamp(x) for ClickHouse
                        "STDDEV" if f.args.len() == 1 => match target {
                            DialectType::ClickHouse => Ok(Expression::Function(Box::new(
                                Function::new("stddevSamp".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // ISINF(x) -> IS_INF(x) for BigQuery
                        "ISINF" if f.args.len() == 1 => match target {
                            DialectType::BigQuery => Ok(Expression::Function(Box::new(
                                Function::new("IS_INF".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // CONTAINS(arr, x) -> ARRAY_CONTAINS(arr, x) for Spark/Hive
                        "CONTAINS" if f.args.len() == 2 => match target {
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "ARRAY_CONTAINS".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // ARRAY_CONTAINS(arr, x) -> CONTAINS(arr, x) for Presto
                        "ARRAY_CONTAINS" if f.args.len() == 2 => match target {
                            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "CONTAINS".to_string(),
                                    f.args,
                                ))))
                            }
                            DialectType::DuckDB => Ok(Expression::Function(Box::new(
                                Function::new("ARRAY_CONTAINS".to_string(), f.args),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // TO_UNIXTIME(x) -> UNIX_TIMESTAMP(x) for Hive/Spark
                        "TO_UNIXTIME" if f.args.len() == 1 => match target {
                            DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "UNIX_TIMESTAMP".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // FROM_UNIXTIME(x) -> target-specific
                        "FROM_UNIXTIME" if f.args.len() == 1 => {
                            match target {
                                DialectType::Hive
                                | DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Presto
                                | DialectType::Trino => Ok(Expression::Function(f)),
                                DialectType::DuckDB => {
                                    // DuckDB: TO_TIMESTAMP(x)
                                    let arg = f.args.into_iter().next().unwrap();
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_TIMESTAMP".to_string(),
                                        vec![arg],
                                    ))))
                                }
                                DialectType::PostgreSQL => {
                                    // PG: TO_TIMESTAMP(col)
                                    let arg = f.args.into_iter().next().unwrap();
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_TIMESTAMP".to_string(),
                                        vec![arg],
                                    ))))
                                }
                                DialectType::Redshift => {
                                    // Redshift: (TIMESTAMP 'epoch' + col * INTERVAL '1 SECOND')
                                    let arg = f.args.into_iter().next().unwrap();
                                    let epoch_ts = Expression::Literal(Box::new(
                                        Literal::Timestamp("epoch".to_string()),
                                    ));
                                    let interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::string("1 SECOND")),
                                            unit: None,
                                        },
                                    ));
                                    let mul =
                                        Expression::Mul(Box::new(BinaryOp::new(arg, interval)));
                                    let add =
                                        Expression::Add(Box::new(BinaryOp::new(epoch_ts, mul)));
                                    Ok(Expression::Paren(Box::new(crate::expressions::Paren {
                                        this: add,
                                        trailing_comments: Vec::new(),
                                    })))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // FROM_UNIXTIME(x, fmt) with 2 args from Hive/Spark -> target-specific
                        "FROM_UNIXTIME"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::Hive
                                        | DialectType::Spark
                                        | DialectType::Databricks
                                ) =>
                        {
                            let mut args = f.args;
                            let unix_ts = args.remove(0);
                            let fmt_expr = args.remove(0);
                            match target {
                                DialectType::DuckDB => {
                                    // DuckDB: STRFTIME(TO_TIMESTAMP(x), c_fmt)
                                    let to_ts = Expression::Function(Box::new(Function::new(
                                        "TO_TIMESTAMP".to_string(),
                                        vec![unix_ts],
                                    )));
                                    if let Expression::Literal(lit) = &fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let c_fmt = temporal::hive_format_to_c_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "STRFTIME".to_string(),
                                                vec![to_ts, Expression::string(&c_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "STRFTIME".to_string(),
                                                vec![to_ts, fmt_expr],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "STRFTIME".to_string(),
                                            vec![to_ts, fmt_expr],
                                        ))))
                                    }
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // Presto: DATE_FORMAT(FROM_UNIXTIME(x), presto_fmt)
                                    let from_unix = Expression::Function(Box::new(Function::new(
                                        "FROM_UNIXTIME".to_string(),
                                        vec![unix_ts],
                                    )));
                                    if let Expression::Literal(lit) = &fmt_expr {
                                        if let crate::expressions::Literal::String(s) = lit.as_ref()
                                        {
                                            let p_fmt = temporal::hive_format_to_presto_format(s);
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_FORMAT".to_string(),
                                                vec![from_unix, Expression::string(&p_fmt)],
                                            ))))
                                        } else {
                                            Ok(Expression::Function(Box::new(Function::new(
                                                "DATE_FORMAT".to_string(),
                                                vec![from_unix, fmt_expr],
                                            ))))
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_FORMAT".to_string(),
                                            vec![from_unix, fmt_expr],
                                        ))))
                                    }
                                }
                                _ => {
                                    // Keep as FROM_UNIXTIME(x, fmt) for other targets
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "FROM_UNIXTIME".to_string(),
                                        vec![unix_ts, fmt_expr],
                                    ))))
                                }
                            }
                        }
                        // DATEPART(unit, expr) -> EXTRACT(unit FROM expr) for Spark
                        "DATEPART" | "DATE_PART" if f.args.len() == 2 => {
                            let unit_str = temporal::get_unit_str_static(&f.args[0]);
                            // Get the raw unit text preserving original case
                            let raw_unit = match &f.args[0] {
                                Expression::Identifier(id) => id.name.clone(),
                                Expression::Var(v) => v.this.clone(),
                                Expression::Literal(lit)
                                    if matches!(
                                        lit.as_ref(),
                                        crate::expressions::Literal::String(_)
                                    ) =>
                                {
                                    let crate::expressions::Literal::String(s) = lit.as_ref()
                                    else {
                                        unreachable!()
                                    };
                                    s.clone()
                                }
                                Expression::Cast(cast)
                                    if cast.format.is_none()
                                        && cast.default.is_none()
                                        && types::unit_cast_target_is_string(&cast.to)
                                        && matches!(
                                            &cast.this,
                                            Expression::Literal(lit)
                                                if matches!(
                                                    lit.as_ref(),
                                                    crate::expressions::Literal::String(_)
                                                )
                                        ) =>
                                {
                                    let Expression::Literal(lit) = &cast.this else {
                                        unreachable!()
                                    };
                                    let crate::expressions::Literal::String(s) = lit.as_ref()
                                    else {
                                        unreachable!()
                                    };
                                    s.clone()
                                }
                                Expression::Column(col) => col.name.name.clone(),
                                _ => unit_str.clone(),
                            };
                            match target {
                                DialectType::TSQL | DialectType::Fabric => {
                                    // Preserve original case of unit for TSQL
                                    let unit_name = match unit_str.as_str() {
                                        "YY" | "YYYY" => "YEAR".to_string(),
                                        "QQ" | "Q" => "QUARTER".to_string(),
                                        "MM" | "M" => "MONTH".to_string(),
                                        "WK" | "WW" => "WEEK".to_string(),
                                        "DD" | "D" | "DY" => "DAY".to_string(),
                                        "HH" => "HOUR".to_string(),
                                        "MI" | "N" => "MINUTE".to_string(),
                                        "SS" | "S" => "SECOND".to_string(),
                                        _ => raw_unit.clone(), // preserve original case
                                    };
                                    let mut args = f.args;
                                    args[0] = Expression::Identifier(Identifier::new(&unit_name));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEPART".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::Spark | DialectType::Databricks => {
                                    // DATEPART(unit, expr) -> EXTRACT(unit FROM expr)
                                    // Preserve original case for non-abbreviation units
                                    let unit = match unit_str.as_str() {
                                        "YY" | "YYYY" => "YEAR".to_string(),
                                        "QQ" | "Q" => "QUARTER".to_string(),
                                        "MM" | "M" => "MONTH".to_string(),
                                        "WK" | "WW" => "WEEK".to_string(),
                                        "DD" | "D" | "DY" => "DAY".to_string(),
                                        "HH" => "HOUR".to_string(),
                                        "MI" | "N" => "MINUTE".to_string(),
                                        "SS" | "S" => "SECOND".to_string(),
                                        _ => raw_unit, // preserve original case
                                    };
                                    Ok(Expression::Extract(Box::new(
                                        crate::expressions::ExtractFunc {
                                            this: f.args[1].clone(),
                                            field: crate::expressions::DateTimeField::Custom(unit),
                                        },
                                    )))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_PART".to_string(),
                                    f.args,
                                )))),
                            }
                        }
                        // DATENAME(mm, date) -> FORMAT(CAST(date AS DATETIME2), 'MMMM') for TSQL
                        // DATENAME(dw, date) -> FORMAT(CAST(date AS DATETIME2), 'dddd') for TSQL
                        // DATENAME(mm, date) -> DATE_FORMAT(CAST(date AS TIMESTAMP), 'MMMM') for Spark
                        // DATENAME(dw, date) -> DATE_FORMAT(CAST(date AS TIMESTAMP), 'EEEE') for Spark
                        "DATENAME" if f.args.len() == 2 => {
                            let unit_str = temporal::get_unit_str_static(&f.args[0]);
                            let date_expr = f.args[1].clone();
                            match unit_str.as_str() {
                                "MM" | "M" | "MONTH" => match target {
                                    DialectType::TSQL => {
                                        let cast_date =
                                            Expression::Cast(Box::new(crate::expressions::Cast {
                                                this: date_expr,
                                                to: DataType::Custom {
                                                    name: "DATETIME2".to_string(),
                                                },
                                                trailing_comments: Vec::new(),
                                                double_colon_syntax: false,
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "FORMAT".to_string(),
                                            vec![cast_date, Expression::string("MMMM")],
                                        ))))
                                    }
                                    DialectType::Spark | DialectType::Databricks => {
                                        let cast_date =
                                            Expression::Cast(Box::new(crate::expressions::Cast {
                                                this: date_expr,
                                                to: DataType::Timestamp {
                                                    timezone: false,
                                                    precision: None,
                                                },
                                                trailing_comments: Vec::new(),
                                                double_colon_syntax: false,
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_FORMAT".to_string(),
                                            vec![cast_date, Expression::string("MMMM")],
                                        ))))
                                    }
                                    _ => Ok(Expression::Function(f)),
                                },
                                "DW" | "WEEKDAY" => match target {
                                    DialectType::TSQL => {
                                        let cast_date =
                                            Expression::Cast(Box::new(crate::expressions::Cast {
                                                this: date_expr,
                                                to: DataType::Custom {
                                                    name: "DATETIME2".to_string(),
                                                },
                                                trailing_comments: Vec::new(),
                                                double_colon_syntax: false,
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "FORMAT".to_string(),
                                            vec![cast_date, Expression::string("dddd")],
                                        ))))
                                    }
                                    DialectType::Spark | DialectType::Databricks => {
                                        let cast_date =
                                            Expression::Cast(Box::new(crate::expressions::Cast {
                                                this: date_expr,
                                                to: DataType::Timestamp {
                                                    timezone: false,
                                                    precision: None,
                                                },
                                                trailing_comments: Vec::new(),
                                                double_colon_syntax: false,
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }));
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "DATE_FORMAT".to_string(),
                                            vec![cast_date, Expression::string("EEEE")],
                                        ))))
                                    }
                                    _ => Ok(Expression::Function(f)),
                                },
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // STRING_AGG(x, sep) without WITHIN GROUP -> target-specific
                        "STRING_AGG" if f.args.len() >= 2 => {
                            let x = f.args[0].clone();
                            let sep = f.args[1].clone();
                            match target {
                                DialectType::MySQL
                                | DialectType::SingleStore
                                | DialectType::Doris
                                | DialectType::StarRocks => Ok(Expression::GroupConcat(Box::new(
                                    crate::expressions::GroupConcatFunc {
                                        this: x,
                                        separator: Some(sep),
                                        order_by: None,
                                        distinct: false,
                                        filter: None,
                                        limit: None,
                                        inferred_type: None,
                                    },
                                ))),
                                DialectType::SQLite => Ok(Expression::GroupConcat(Box::new(
                                    crate::expressions::GroupConcatFunc {
                                        this: x,
                                        separator: Some(sep),
                                        order_by: None,
                                        distinct: false,
                                        filter: None,
                                        limit: None,
                                        inferred_type: None,
                                    },
                                ))),
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    Ok(Expression::StringAgg(Box::new(
                                        crate::expressions::StringAggFunc {
                                            this: x,
                                            separator: Some(sep),
                                            order_by: None,
                                            distinct: false,
                                            filter: None,
                                            limit: None,
                                            inferred_type: None,
                                        },
                                    )))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        "TRY_DIVIDE" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let x = args.remove(0);
                            let y = args.remove(0);
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TRY_DIVIDE".to_string(),
                                        vec![x, y],
                                    ))))
                                }
                                DialectType::Snowflake => {
                                    let y_ref = match &y {
                                        Expression::Column(_)
                                        | Expression::Literal(_)
                                        | Expression::Identifier(_) => y.clone(),
                                        _ => Expression::Paren(Box::new(Paren {
                                            this: y.clone(),
                                            trailing_comments: vec![],
                                        })),
                                    };
                                    let x_ref = match &x {
                                        Expression::Column(_)
                                        | Expression::Literal(_)
                                        | Expression::Identifier(_) => x.clone(),
                                        _ => Expression::Paren(Box::new(Paren {
                                            this: x.clone(),
                                            trailing_comments: vec![],
                                        })),
                                    };
                                    let condition = Expression::Neq(Box::new(
                                        crate::expressions::BinaryOp::new(
                                            y_ref.clone(),
                                            Expression::number(0),
                                        ),
                                    ));
                                    let div_expr = Expression::Div(Box::new(
                                        crate::expressions::BinaryOp::new(x_ref, y_ref),
                                    ));
                                    Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                                        condition,
                                        true_value: div_expr,
                                        false_value: Some(Expression::Null(Null)),
                                        original_name: Some("IFF".to_string()),
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::DuckDB => {
                                    let y_ref = match &y {
                                        Expression::Column(_)
                                        | Expression::Literal(_)
                                        | Expression::Identifier(_) => y.clone(),
                                        _ => Expression::Paren(Box::new(Paren {
                                            this: y.clone(),
                                            trailing_comments: vec![],
                                        })),
                                    };
                                    let x_ref = match &x {
                                        Expression::Column(_)
                                        | Expression::Literal(_)
                                        | Expression::Identifier(_) => x.clone(),
                                        _ => Expression::Paren(Box::new(Paren {
                                            this: x.clone(),
                                            trailing_comments: vec![],
                                        })),
                                    };
                                    let condition = Expression::Neq(Box::new(
                                        crate::expressions::BinaryOp::new(
                                            y_ref.clone(),
                                            Expression::number(0),
                                        ),
                                    ));
                                    let div_expr = Expression::Div(Box::new(
                                        crate::expressions::BinaryOp::new(x_ref, y_ref),
                                    ));
                                    Ok(Expression::Case(Box::new(Case {
                                        operand: None,
                                        whens: vec![(condition, div_expr)],
                                        else_: Some(Expression::Null(Null)),
                                        comments: Vec::new(),
                                        inferred_type: None,
                                    })))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "TRY_DIVIDE".to_string(),
                                    vec![x, y],
                                )))),
                            }
                        }
                        // JSON_ARRAYAGG -> JSON_AGG for PostgreSQL
                        "JSON_ARRAYAGG" => match target {
                            DialectType::PostgreSQL => {
                                Ok(Expression::Function(Box::new(Function {
                                    name: "JSON_AGG".to_string(),
                                    ..(*f)
                                })))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // SCHEMA_NAME(id) -> CURRENT_SCHEMA for PostgreSQL, 'main' for SQLite
                        "SCHEMA_NAME" => match target {
                            DialectType::PostgreSQL => Ok(Expression::CurrentSchema(Box::new(
                                crate::expressions::CurrentSchema { this: None },
                            ))),
                            DialectType::SQLite => Ok(Expression::string("main")),
                            _ => Ok(Expression::Function(f)),
                        },
                        // TO_TIMESTAMP(x, fmt) 2-arg from Spark/Hive: convert Java format to target format
                        "TO_TIMESTAMP"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Hive
                                )
                                && matches!(target, DialectType::DuckDB) =>
                        {
                            let mut args = f.args;
                            let val = args.remove(0);
                            let fmt_expr = args.remove(0);
                            if let Expression::Literal(ref lit) = fmt_expr {
                                if let Literal::String(ref s) = lit.as_ref() {
                                    // Convert Java/Spark format to C strptime format
                                    fn java_to_c_fmt(fmt: &str) -> String {
                                        let result = fmt
                                            .replace("yyyy", "%Y")
                                            .replace("SSSSSS", "%f")
                                            .replace("EEEE", "%W")
                                            .replace("MM", "%m")
                                            .replace("dd", "%d")
                                            .replace("HH", "%H")
                                            .replace("mm", "%M")
                                            .replace("ss", "%S")
                                            .replace("yy", "%y");
                                        let mut out = String::new();
                                        let chars: Vec<char> = result.chars().collect();
                                        let mut i = 0;
                                        while i < chars.len() {
                                            if chars[i] == '%' && i + 1 < chars.len() {
                                                out.push(chars[i]);
                                                out.push(chars[i + 1]);
                                                i += 2;
                                            } else if chars[i] == 'z' {
                                                out.push_str("%Z");
                                                i += 1;
                                            } else if chars[i] == 'Z' {
                                                out.push_str("%z");
                                                i += 1;
                                            } else {
                                                out.push(chars[i]);
                                                i += 1;
                                            }
                                        }
                                        out
                                    }
                                    let c_fmt = java_to_c_fmt(s);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "STRPTIME".to_string(),
                                        vec![val, Expression::string(&c_fmt)],
                                    ))))
                                } else {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "STRPTIME".to_string(),
                                        vec![val, fmt_expr],
                                    ))))
                                }
                            } else {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "STRPTIME".to_string(),
                                    vec![val, fmt_expr],
                                ))))
                            }
                        }
                        // TO_DATE(x) 1-arg from Doris: date conversion
                        "TO_DATE"
                            if f.args.len() == 1
                                && matches!(
                                    source,
                                    DialectType::Doris | DialectType::StarRocks
                                ) =>
                        {
                            let arg = f.args.into_iter().next().unwrap();
                            match target {
                                DialectType::Oracle | DialectType::DuckDB | DialectType::TSQL => {
                                    // CAST(x AS DATE)
                                    Ok(Expression::Cast(Box::new(Cast {
                                        this: arg,
                                        to: DataType::Date,
                                        double_colon_syntax: false,
                                        trailing_comments: vec![],
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::MySQL | DialectType::SingleStore => {
                                    // DATE(x)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE".to_string(),
                                        vec![arg],
                                    ))))
                                }
                                _ => {
                                    // Default: keep as TO_DATE(x) (Spark, PostgreSQL, etc.)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_DATE".to_string(),
                                        vec![arg],
                                    ))))
                                }
                            }
                        }
                        // TO_DATE(x) 1-arg from Spark/Hive: safe date conversion
                        "TO_DATE"
                            if f.args.len() == 1
                                && matches!(
                                    source,
                                    DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Hive
                                ) =>
                        {
                            let arg = f.args.into_iter().next().unwrap();
                            match target {
                                DialectType::DuckDB => {
                                    // Spark TO_DATE is safe -> TRY_CAST(x AS DATE)
                                    Ok(Expression::TryCast(Box::new(Cast {
                                        this: arg,
                                        to: DataType::Date,
                                        double_colon_syntax: false,
                                        trailing_comments: vec![],
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // CAST(CAST(x AS TIMESTAMP) AS DATE)
                                    Ok(temporal::double_cast_timestamp_date(arg))
                                }
                                DialectType::Snowflake => {
                                    // Spark's TO_DATE is safe -> TRY_TO_DATE(x, 'yyyy-mm-DD')
                                    // The default Spark format 'yyyy-MM-dd' maps to Snowflake 'yyyy-mm-DD'
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TRY_TO_DATE".to_string(),
                                        vec![arg, Expression::string("yyyy-mm-DD")],
                                    ))))
                                }
                                _ => {
                                    // Default: keep as TO_DATE(x)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_DATE".to_string(),
                                        vec![arg],
                                    ))))
                                }
                            }
                        }
                        // TO_DATE(x, fmt) 2-arg from Spark/Hive: format-based date conversion
                        "TO_DATE"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Hive
                                ) =>
                        {
                            let mut args = f.args;
                            let val = args.remove(0);
                            let fmt_expr = args.remove(0);
                            let is_default_format = matches!(&fmt_expr, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(s) if s == "yyyy-MM-dd"));

                            if is_default_format {
                                // Default format: same as 1-arg form
                                match target {
                                    DialectType::DuckDB => {
                                        Ok(Expression::TryCast(Box::new(Cast {
                                            this: val,
                                            to: DataType::Date,
                                            double_colon_syntax: false,
                                            trailing_comments: vec![],
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        })))
                                    }
                                    DialectType::Presto
                                    | DialectType::Trino
                                    | DialectType::Athena => {
                                        Ok(temporal::double_cast_timestamp_date(val))
                                    }
                                    DialectType::Snowflake => {
                                        // TRY_TO_DATE(x, format) with Snowflake format mapping
                                        let sf_fmt = "yyyy-MM-dd"
                                            .replace("yyyy", "yyyy")
                                            .replace("MM", "mm")
                                            .replace("dd", "DD");
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "TRY_TO_DATE".to_string(),
                                            vec![val, Expression::string(&sf_fmt)],
                                        ))))
                                    }
                                    _ => Ok(Expression::Function(Box::new(Function::new(
                                        "TO_DATE".to_string(),
                                        vec![val],
                                    )))),
                                }
                            } else {
                                // Non-default format: use format-based parsing
                                if let Expression::Literal(ref lit) = fmt_expr {
                                    if let Literal::String(ref s) = lit.as_ref() {
                                        match target {
                                            DialectType::DuckDB => {
                                                // CAST(CAST(TRY_STRPTIME(x, c_fmt) AS TIMESTAMP) AS DATE)
                                                fn java_to_c_fmt_todate(fmt: &str) -> String {
                                                    let result = fmt
                                                        .replace("yyyy", "%Y")
                                                        .replace("SSSSSS", "%f")
                                                        .replace("EEEE", "%W")
                                                        .replace("MM", "%m")
                                                        .replace("dd", "%d")
                                                        .replace("HH", "%H")
                                                        .replace("mm", "%M")
                                                        .replace("ss", "%S")
                                                        .replace("yy", "%y");
                                                    let mut out = String::new();
                                                    let chars: Vec<char> = result.chars().collect();
                                                    let mut i = 0;
                                                    while i < chars.len() {
                                                        if chars[i] == '%' && i + 1 < chars.len() {
                                                            out.push(chars[i]);
                                                            out.push(chars[i + 1]);
                                                            i += 2;
                                                        } else if chars[i] == 'z' {
                                                            out.push_str("%Z");
                                                            i += 1;
                                                        } else if chars[i] == 'Z' {
                                                            out.push_str("%z");
                                                            i += 1;
                                                        } else {
                                                            out.push(chars[i]);
                                                            i += 1;
                                                        }
                                                    }
                                                    out
                                                }
                                                let c_fmt = java_to_c_fmt_todate(s);
                                                // CAST(CAST(TRY_STRPTIME(x, fmt) AS TIMESTAMP) AS DATE)
                                                let try_strptime =
                                                    Expression::Function(Box::new(Function::new(
                                                        "TRY_STRPTIME".to_string(),
                                                        vec![val, Expression::string(&c_fmt)],
                                                    )));
                                                let cast_ts = Expression::Cast(Box::new(Cast {
                                                    this: try_strptime,
                                                    to: DataType::Timestamp {
                                                        precision: None,
                                                        timezone: false,
                                                    },
                                                    double_colon_syntax: false,
                                                    trailing_comments: vec![],
                                                    format: None,
                                                    default: None,
                                                    inferred_type: None,
                                                }));
                                                Ok(Expression::Cast(Box::new(Cast {
                                                    this: cast_ts,
                                                    to: DataType::Date,
                                                    double_colon_syntax: false,
                                                    trailing_comments: vec![],
                                                    format: None,
                                                    default: None,
                                                    inferred_type: None,
                                                })))
                                            }
                                            DialectType::Presto
                                            | DialectType::Trino
                                            | DialectType::Athena => {
                                                // CAST(DATE_PARSE(x, presto_fmt) AS DATE)
                                                let p_fmt = s
                                                    .replace("yyyy", "%Y")
                                                    .replace("SSSSSS", "%f")
                                                    .replace("MM", "%m")
                                                    .replace("dd", "%d")
                                                    .replace("HH", "%H")
                                                    .replace("mm", "%M")
                                                    .replace("ss", "%S")
                                                    .replace("yy", "%y");
                                                let date_parse =
                                                    Expression::Function(Box::new(Function::new(
                                                        "DATE_PARSE".to_string(),
                                                        vec![val, Expression::string(&p_fmt)],
                                                    )));
                                                Ok(Expression::Cast(Box::new(Cast {
                                                    this: date_parse,
                                                    to: DataType::Date,
                                                    double_colon_syntax: false,
                                                    trailing_comments: vec![],
                                                    format: None,
                                                    default: None,
                                                    inferred_type: None,
                                                })))
                                            }
                                            DialectType::Snowflake => {
                                                // TRY_TO_DATE(x, snowflake_fmt)
                                                Ok(Expression::Function(Box::new(Function::new(
                                                    "TRY_TO_DATE".to_string(),
                                                    vec![val, Expression::string(s)],
                                                ))))
                                            }
                                            _ => Ok(Expression::Function(Box::new(Function::new(
                                                "TO_DATE".to_string(),
                                                vec![val, fmt_expr],
                                            )))),
                                        }
                                    } else {
                                        Ok(Expression::Function(Box::new(Function::new(
                                            "TO_DATE".to_string(),
                                            vec![val, fmt_expr],
                                        ))))
                                    }
                                } else {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_DATE".to_string(),
                                        vec![val, fmt_expr],
                                    ))))
                                }
                            }
                        }
                        // TO_TIMESTAMP(x) 1-arg: epoch conversion
                        "TO_TIMESTAMP"
                            if f.args.len() == 1
                                && matches!(source, DialectType::DuckDB)
                                && matches!(
                                    target,
                                    DialectType::BigQuery
                                        | DialectType::Presto
                                        | DialectType::Trino
                                        | DialectType::Hive
                                        | DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Athena
                                ) =>
                        {
                            let arg = f.args.into_iter().next().unwrap();
                            let func_name = match target {
                                DialectType::BigQuery => "TIMESTAMP_SECONDS",
                                DialectType::Presto
                                | DialectType::Trino
                                | DialectType::Athena
                                | DialectType::Hive
                                | DialectType::Spark
                                | DialectType::Databricks => "FROM_UNIXTIME",
                                _ => "TO_TIMESTAMP",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                func_name.to_string(),
                                vec![arg],
                            ))))
                        }
                        // CONCAT(x) single-arg: -> CONCAT(COALESCE(x, '')) for Spark
                        "CONCAT" if f.args.len() == 1 => {
                            let arg = f.args.into_iter().next().unwrap();
                            match target {
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // CONCAT(a) -> CAST(a AS VARCHAR)
                                    Ok(Expression::Cast(Box::new(Cast {
                                        this: arg,
                                        to: DataType::VarChar {
                                            length: None,
                                            parenthesized_length: false,
                                        },
                                        trailing_comments: vec![],
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::TSQL => {
                                    // CONCAT(a) -> a
                                    Ok(arg)
                                }
                                DialectType::DuckDB => {
                                    // Keep CONCAT(a) for DuckDB (native support)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "CONCAT".to_string(),
                                        vec![arg],
                                    ))))
                                }
                                DialectType::Spark | DialectType::Databricks => {
                                    let coalesced = Expression::Coalesce(Box::new(
                                        crate::expressions::VarArgFunc {
                                            expressions: vec![arg, Expression::string("")],
                                            original_name: None,
                                            inferred_type: None,
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "CONCAT".to_string(),
                                        vec![coalesced],
                                    ))))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "CONCAT".to_string(),
                                    vec![arg],
                                )))),
                            }
                        }
                        // REGEXP_EXTRACT(a, p) 2-arg: BigQuery default group is 0 (no 3rd arg needed)
                        "REGEXP_EXTRACT"
                            if f.args.len() == 3 && matches!(target, DialectType::BigQuery) =>
                        {
                            // If group_index is 0, drop it
                            let drop_group = match &f.args[2] {
                                Expression::Literal(lit)
                                    if matches!(lit.as_ref(), Literal::Number(_)) =>
                                {
                                    let Literal::Number(n) = lit.as_ref() else {
                                        unreachable!()
                                    };
                                    n == "0"
                                }
                                _ => false,
                            };
                            if drop_group {
                                let mut args = f.args;
                                args.truncate(2);
                                Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_EXTRACT".to_string(),
                                    args,
                                ))))
                            } else {
                                Ok(Expression::Function(f))
                            }
                        }
                        // REGEXP_EXTRACT(a, pattern, group, flags) 4-arg -> REGEXP_SUBSTR for Snowflake
                        "REGEXP_EXTRACT"
                            if f.args.len() == 4 && matches!(target, DialectType::Snowflake) =>
                        {
                            // REGEXP_EXTRACT(a, 'pattern', 2, 'i') -> REGEXP_SUBSTR(a, 'pattern', 1, 1, 'i', 2)
                            let mut args = f.args;
                            let this = args.remove(0);
                            let pattern = args.remove(0);
                            let group = args.remove(0);
                            let flags = args.remove(0);
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_SUBSTR".to_string(),
                                vec![
                                    this,
                                    pattern,
                                    Expression::number(1),
                                    Expression::number(1),
                                    flags,
                                    group,
                                ],
                            ))))
                        }
                        // REGEXP_SUBSTR(a, pattern, position) 3-arg -> REGEXP_EXTRACT(SUBSTRING(a, pos), pattern)
                        "REGEXP_SUBSTR"
                            if f.args.len() == 3
                                && matches!(
                                    target,
                                    DialectType::DuckDB
                                        | DialectType::Presto
                                        | DialectType::Trino
                                        | DialectType::Spark
                                        | DialectType::Databricks
                                ) =>
                        {
                            let mut args = f.args;
                            let this = args.remove(0);
                            let pattern = args.remove(0);
                            let position = args.remove(0);
                            // Wrap subject in SUBSTRING(this, position) to apply the offset
                            let substring_expr = Expression::Function(Box::new(Function::new(
                                "SUBSTRING".to_string(),
                                vec![this, position],
                            )));
                            let target_name = match target {
                                DialectType::DuckDB => "REGEXP_EXTRACT",
                                _ => "REGEXP_EXTRACT",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                target_name.to_string(),
                                vec![substring_expr, pattern],
                            ))))
                        }
                        // TO_DAYS(x) -> (DATEDIFF(x, '0000-01-01') + 1) or target-specific
                        "TO_DAYS" if f.args.len() == 1 => {
                            let x = f.args.into_iter().next().unwrap();
                            let epoch = Expression::string("0000-01-01");
                            // Build the final target-specific expression directly
                            let datediff_expr = match target {
                                DialectType::MySQL | DialectType::SingleStore => {
                                    // MySQL: (DATEDIFF(x, '0000-01-01') + 1)
                                    Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![x, epoch],
                                    )))
                                }
                                DialectType::DuckDB => {
                                    // DuckDB: (DATE_DIFF('DAY', CAST('0000-01-01' AS DATE), CAST(x AS DATE)) + 1)
                                    let cast_epoch = Expression::Cast(Box::new(Cast {
                                        this: epoch,
                                        to: DataType::Date,
                                        trailing_comments: Vec::new(),
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }));
                                    let cast_x = Expression::Cast(Box::new(Cast {
                                        this: x,
                                        to: DataType::Date,
                                        trailing_comments: Vec::new(),
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }));
                                    Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![Expression::string("DAY"), cast_epoch, cast_x],
                                    )))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // Presto: (DATE_DIFF('DAY', CAST(CAST('0000-01-01' AS TIMESTAMP) AS DATE), CAST(CAST(x AS TIMESTAMP) AS DATE)) + 1)
                                    let cast_epoch = temporal::double_cast_timestamp_date(epoch);
                                    let cast_x = temporal::double_cast_timestamp_date(x);
                                    Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![Expression::string("DAY"), cast_epoch, cast_x],
                                    )))
                                }
                                _ => {
                                    // Default: (DATEDIFF(x, '0000-01-01') + 1)
                                    Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![x, epoch],
                                    )))
                                }
                            };
                            let add_one = Expression::Add(Box::new(BinaryOp::new(
                                datediff_expr,
                                Expression::number(1),
                            )));
                            Ok(Expression::Paren(Box::new(crate::expressions::Paren {
                                this: add_one,
                                trailing_comments: Vec::new(),
                            })))
                        }
                        // STR_TO_DATE(x, format) -> DATE_PARSE / STRPTIME / TO_DATE etc.
                        "STR_TO_DATE"
                            if f.args.len() == 2
                                && matches!(target, DialectType::Presto | DialectType::Trino) =>
                        {
                            let mut args = f.args;
                            let x = args.remove(0);
                            let format_expr = args.remove(0);
                            // Check if the format contains time components
                            let has_time = if let Expression::Literal(ref lit) = format_expr {
                                if let Literal::String(ref fmt) = lit.as_ref() {
                                    fmt.contains("%H")
                                        || fmt.contains("%T")
                                        || fmt.contains("%M")
                                        || fmt.contains("%S")
                                        || fmt.contains("%I")
                                        || fmt.contains("%p")
                                } else {
                                    false
                                }
                            } else {
                                false
                            };
                            let date_parse = Expression::Function(Box::new(Function::new(
                                "DATE_PARSE".to_string(),
                                vec![x, format_expr],
                            )));
                            if has_time {
                                // Has time components: just DATE_PARSE
                                Ok(date_parse)
                            } else {
                                // Date-only: CAST(DATE_PARSE(...) AS DATE)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: date_parse,
                                    to: DataType::Date,
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        "STR_TO_DATE"
                            if f.args.len() == 2
                                && matches!(
                                    target,
                                    DialectType::PostgreSQL | DialectType::Redshift
                                ) =>
                        {
                            let mut args = f.args;
                            let x = args.remove(0);
                            let fmt = args.remove(0);
                            let pg_fmt = match fmt {
                                Expression::Literal(lit)
                                    if matches!(lit.as_ref(), Literal::String(_)) =>
                                {
                                    let Literal::String(s) = lit.as_ref() else {
                                        unreachable!()
                                    };
                                    Expression::string(
                                        &s.replace("%Y", "YYYY")
                                            .replace("%m", "MM")
                                            .replace("%d", "DD")
                                            .replace("%H", "HH24")
                                            .replace("%M", "MI")
                                            .replace("%S", "SS"),
                                    )
                                }
                                other => other,
                            };
                            let to_date = Expression::Function(Box::new(Function::new(
                                "TO_DATE".to_string(),
                                vec![x, pg_fmt],
                            )));
                            Ok(Expression::Cast(Box::new(Cast {
                                this: to_date,
                                to: DataType::Timestamp {
                                    timezone: false,
                                    precision: None,
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        // RANGE(start, end) -> GENERATE_SERIES for SQLite
                        "RANGE"
                            if (f.args.len() == 1 || f.args.len() == 2)
                                && matches!(target, DialectType::SQLite) =>
                        {
                            if f.args.len() == 2 {
                                // RANGE(start, end) -> (SELECT value AS col_alias FROM GENERATE_SERIES(start, end))
                                // For SQLite, RANGE is exclusive on end, GENERATE_SERIES is inclusive
                                let mut args = f.args;
                                let start = args.remove(0);
                                let end = args.remove(0);
                                Ok(Expression::Function(Box::new(Function::new(
                                    "GENERATE_SERIES".to_string(),
                                    vec![start, end],
                                ))))
                            } else {
                                Ok(Expression::Function(f))
                            }
                        }
                        // UNIFORM(low, high[, seed]) -> UNIFORM(low, high, RANDOM([seed])) for Snowflake
                        // When source is Snowflake, keep as-is (args already in correct form)
                        "UNIFORM"
                            if matches!(target, DialectType::Snowflake)
                                && (f.args.len() == 2 || f.args.len() == 3) =>
                        {
                            if matches!(source, DialectType::Snowflake) {
                                // Snowflake -> Snowflake: keep as-is
                                Ok(Expression::Function(f))
                            } else {
                                let mut args = f.args;
                                let low = args.remove(0);
                                let high = args.remove(0);
                                let random = if !args.is_empty() {
                                    let seed = args.remove(0);
                                    Expression::Function(Box::new(Function::new(
                                        "RANDOM".to_string(),
                                        vec![seed],
                                    )))
                                } else {
                                    Expression::Function(Box::new(Function::new(
                                        "RANDOM".to_string(),
                                        vec![],
                                    )))
                                };
                                Ok(Expression::Function(Box::new(Function::new(
                                    "UNIFORM".to_string(),
                                    vec![low, high, random],
                                ))))
                            }
                        }
                        // TO_UTC_TIMESTAMP(ts, tz) -> target-specific UTC conversion
                        "TO_UTC_TIMESTAMP" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let ts_arg = args.remove(0);
                            let tz_arg = args.remove(0);
                            // Cast string literal to TIMESTAMP for all targets
                            let ts_cast = if matches!(&ts_arg, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                            {
                                Expression::Cast(Box::new(Cast {
                                    this: ts_arg,
                                    to: DataType::Timestamp {
                                        timezone: false,
                                        precision: None,
                                    },
                                    trailing_comments: vec![],
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))
                            } else {
                                ts_arg
                            };
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_UTC_TIMESTAMP".to_string(),
                                        vec![ts_cast, tz_arg],
                                    ))))
                                }
                                DialectType::Snowflake => {
                                    // CONVERT_TIMEZONE(tz, 'UTC', CAST(ts AS TIMESTAMP))
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "CONVERT_TIMEZONE".to_string(),
                                        vec![tz_arg, Expression::string("UTC"), ts_cast],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // WITH_TIMEZONE(CAST(ts AS TIMESTAMP), tz) AT TIME ZONE 'UTC'
                                    let wtz = Expression::Function(Box::new(Function::new(
                                        "WITH_TIMEZONE".to_string(),
                                        vec![ts_cast, tz_arg],
                                    )));
                                    Ok(Expression::AtTimeZone(Box::new(
                                        crate::expressions::AtTimeZone {
                                            this: wtz,
                                            zone: Expression::string("UTC"),
                                        },
                                    )))
                                }
                                DialectType::BigQuery => {
                                    // DATETIME(TIMESTAMP(CAST(ts AS DATETIME), tz), 'UTC')
                                    let cast_dt = Expression::Cast(Box::new(Cast {
                                        this: if let Expression::Cast(c) = ts_cast {
                                            c.this
                                        } else {
                                            ts_cast.clone()
                                        },
                                        to: DataType::Custom {
                                            name: "DATETIME".to_string(),
                                        },
                                        trailing_comments: vec![],
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }));
                                    let ts_func = Expression::Function(Box::new(Function::new(
                                        "TIMESTAMP".to_string(),
                                        vec![cast_dt, tz_arg],
                                    )));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATETIME".to_string(),
                                        vec![ts_func, Expression::string("UTC")],
                                    ))))
                                }
                                _ => {
                                    // DuckDB, PostgreSQL, Redshift: CAST(ts AS TIMESTAMP) AT TIME ZONE tz AT TIME ZONE 'UTC'
                                    let atz1 = Expression::AtTimeZone(Box::new(
                                        crate::expressions::AtTimeZone {
                                            this: ts_cast,
                                            zone: tz_arg,
                                        },
                                    ));
                                    Ok(Expression::AtTimeZone(Box::new(
                                        crate::expressions::AtTimeZone {
                                            this: atz1,
                                            zone: Expression::string("UTC"),
                                        },
                                    )))
                                }
                            }
                        }
                        // FROM_UTC_TIMESTAMP(ts, tz) -> target-specific UTC conversion
                        "FROM_UTC_TIMESTAMP" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let ts_arg = args.remove(0);
                            let tz_arg = args.remove(0);
                            // Cast string literal to TIMESTAMP
                            let ts_cast = if matches!(&ts_arg, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                            {
                                Expression::Cast(Box::new(Cast {
                                    this: ts_arg,
                                    to: DataType::Timestamp {
                                        timezone: false,
                                        precision: None,
                                    },
                                    trailing_comments: vec![],
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))
                            } else {
                                ts_arg
                            };
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "FROM_UTC_TIMESTAMP".to_string(),
                                        vec![ts_cast, tz_arg],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // AT_TIMEZONE(CAST(ts AS TIMESTAMP), tz)
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "AT_TIMEZONE".to_string(),
                                        vec![ts_cast, tz_arg],
                                    ))))
                                }
                                DialectType::Snowflake => {
                                    // CONVERT_TIMEZONE('UTC', tz, CAST(ts AS TIMESTAMP))
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "CONVERT_TIMEZONE".to_string(),
                                        vec![Expression::string("UTC"), tz_arg, ts_cast],
                                    ))))
                                }
                                _ => {
                                    // DuckDB, PostgreSQL, Redshift: CAST(ts AS TIMESTAMP) AT TIME ZONE tz
                                    Ok(Expression::AtTimeZone(Box::new(
                                        crate::expressions::AtTimeZone {
                                            this: ts_cast,
                                            zone: tz_arg,
                                        },
                                    )))
                                }
                            }
                        }
                        // MAP_FROM_ARRAYS(keys, values) -> target-specific map construction
                        "MAP_FROM_ARRAYS" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::Snowflake => "OBJECT_CONSTRUCT",
                                _ => "MAP",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // STR_TO_MAP(s, pair_delim, kv_delim) -> SPLIT_TO_MAP for Presto
                        "STR_TO_MAP" if f.args.len() >= 1 => match target {
                            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "SPLIT_TO_MAP".to_string(),
                                    f.args,
                                ))))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // TIME_TO_STR(x, fmt) -> Expression::TimeToStr for proper generation
                        "TIME_TO_STR" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            let fmt_expr = args.remove(0);
                            let format = if let Expression::Literal(lit) = fmt_expr {
                                if let Literal::String(s) = lit.as_ref() {
                                    s.clone()
                                } else {
                                    String::new()
                                }
                            } else {
                                "%Y-%m-%d %H:%M:%S".to_string()
                            };
                            Ok(Expression::TimeToStr(Box::new(
                                crate::expressions::TimeToStr {
                                    this: Box::new(this),
                                    format,
                                    culture: None,
                                    zone: None,
                                },
                            )))
                        }
                        // STR_TO_TIME(x, fmt) -> Expression::StrToTime for proper generation
                        "STR_TO_TIME" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            let fmt_expr = args.remove(0);
                            let format = if let Expression::Literal(lit) = fmt_expr {
                                if let Literal::String(s) = lit.as_ref() {
                                    s.clone()
                                } else {
                                    String::new()
                                }
                            } else {
                                "%Y-%m-%d %H:%M:%S".to_string()
                            };
                            Ok(Expression::StrToTime(Box::new(
                                crate::expressions::StrToTime {
                                    this: Box::new(this),
                                    format,
                                    zone: None,
                                    safe: None,
                                    target_type: None,
                                },
                            )))
                        }
                        // STR_TO_UNIX(x, fmt) -> Expression::StrToUnix for proper generation
                        "STR_TO_UNIX" if f.args.len() >= 1 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            let format = if !args.is_empty() {
                                if let Expression::Literal(lit) = args.remove(0) {
                                    if let Literal::String(s) = lit.as_ref() {
                                        Some(s.clone())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            Ok(Expression::StrToUnix(Box::new(
                                crate::expressions::StrToUnix {
                                    this: Some(Box::new(this)),
                                    format,
                                },
                            )))
                        }
                        // TIME_TO_UNIX(x) -> Expression::TimeToUnix for proper generation
                        "TIME_TO_UNIX" if f.args.len() == 1 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            Ok(Expression::TimeToUnix(Box::new(
                                crate::expressions::UnaryFunc {
                                    this,
                                    original_name: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                        // UNIX_TO_STR(x, fmt) -> Expression::UnixToStr for proper generation
                        "UNIX_TO_STR" if f.args.len() >= 1 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            let format = if !args.is_empty() {
                                if let Expression::Literal(lit) = args.remove(0) {
                                    if let Literal::String(s) = lit.as_ref() {
                                        Some(s.clone())
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            };
                            Ok(Expression::UnixToStr(Box::new(
                                crate::expressions::UnixToStr {
                                    this: Box::new(this),
                                    format,
                                },
                            )))
                        }
                        // UNIX_TO_TIME(x) -> Expression::UnixToTime for proper generation
                        "UNIX_TO_TIME" if f.args.len() == 1 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            Ok(Expression::UnixToTime(Box::new(
                                crate::expressions::UnixToTime {
                                    this: Box::new(this),
                                    scale: None,
                                    zone: None,
                                    hours: None,
                                    minutes: None,
                                    format: None,
                                    target_type: None,
                                },
                            )))
                        }
                        // TIME_STR_TO_DATE(x) -> Expression::TimeStrToDate for proper generation
                        "TIME_STR_TO_DATE" if f.args.len() == 1 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            Ok(Expression::TimeStrToDate(Box::new(
                                crate::expressions::UnaryFunc {
                                    this,
                                    original_name: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                        // TIME_STR_TO_TIME(x) -> Expression::TimeStrToTime for proper generation
                        "TIME_STR_TO_TIME" if f.args.len() == 1 => {
                            let mut args = f.args;
                            let this = args.remove(0);
                            Ok(Expression::TimeStrToTime(Box::new(
                                crate::expressions::TimeStrToTime {
                                    this: Box::new(this),
                                    zone: None,
                                },
                            )))
                        }
                        // MONTHS_BETWEEN(end, start) -> DuckDB complex expansion
                        "MONTHS_BETWEEN" if f.args.len() == 2 => {
                            match target {
                                DialectType::DuckDB => {
                                    let mut args = f.args;
                                    let end_date = args.remove(0);
                                    let start_date = args.remove(0);
                                    let cast_end = temporal::ensure_cast_date(end_date);
                                    let cast_start = temporal::ensure_cast_date(start_date);
                                    // DATE_DIFF('MONTH', start, end) + CASE WHEN DAY(end) = DAY(LAST_DAY(end)) AND DAY(start) = DAY(LAST_DAY(start)) THEN 0 ELSE (DAY(end) - DAY(start)) / 31.0 END
                                    let dd = Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![
                                            Expression::string("MONTH"),
                                            cast_start.clone(),
                                            cast_end.clone(),
                                        ],
                                    )));
                                    let day_end = Expression::Function(Box::new(Function::new(
                                        "DAY".to_string(),
                                        vec![cast_end.clone()],
                                    )));
                                    let day_start = Expression::Function(Box::new(Function::new(
                                        "DAY".to_string(),
                                        vec![cast_start.clone()],
                                    )));
                                    let last_day_end =
                                        Expression::Function(Box::new(Function::new(
                                            "LAST_DAY".to_string(),
                                            vec![cast_end.clone()],
                                        )));
                                    let last_day_start =
                                        Expression::Function(Box::new(Function::new(
                                            "LAST_DAY".to_string(),
                                            vec![cast_start.clone()],
                                        )));
                                    let day_last_end = Expression::Function(Box::new(
                                        Function::new("DAY".to_string(), vec![last_day_end]),
                                    ));
                                    let day_last_start = Expression::Function(Box::new(
                                        Function::new("DAY".to_string(), vec![last_day_start]),
                                    ));
                                    let cond1 = Expression::Eq(Box::new(BinaryOp::new(
                                        day_end.clone(),
                                        day_last_end,
                                    )));
                                    let cond2 = Expression::Eq(Box::new(BinaryOp::new(
                                        day_start.clone(),
                                        day_last_start,
                                    )));
                                    let both_cond =
                                        Expression::And(Box::new(BinaryOp::new(cond1, cond2)));
                                    let day_diff = Expression::Sub(Box::new(BinaryOp::new(
                                        day_end, day_start,
                                    )));
                                    let day_diff_paren =
                                        Expression::Paren(Box::new(crate::expressions::Paren {
                                            this: day_diff,
                                            trailing_comments: Vec::new(),
                                        }));
                                    let frac = Expression::Div(Box::new(BinaryOp::new(
                                        day_diff_paren,
                                        Expression::Literal(Box::new(Literal::Number(
                                            "31.0".to_string(),
                                        ))),
                                    )));
                                    let case_expr = Expression::Case(Box::new(Case {
                                        operand: None,
                                        whens: vec![(both_cond, Expression::number(0))],
                                        else_: Some(frac),
                                        comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    Ok(Expression::Add(Box::new(BinaryOp::new(dd, case_expr))))
                                }
                                DialectType::Snowflake | DialectType::Redshift => {
                                    let mut args = f.args;
                                    let end_date = args.remove(0);
                                    let start_date = args.remove(0);
                                    let unit = Expression::Identifier(Identifier::new("MONTH"));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATEDIFF".to_string(),
                                        vec![unit, start_date, end_date],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    let mut args = f.args;
                                    let end_date = args.remove(0);
                                    let start_date = args.remove(0);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_DIFF".to_string(),
                                        vec![Expression::string("MONTH"), start_date, end_date],
                                    ))))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // MONTHS_BETWEEN(end, start, roundOff) - 3-arg form (Spark-specific)
                        // Drop the roundOff arg for non-Spark targets, keep it for Spark
                        "MONTHS_BETWEEN" if f.args.len() == 3 => {
                            match target {
                                DialectType::Spark | DialectType::Databricks => {
                                    Ok(Expression::Function(f))
                                }
                                _ => {
                                    // Drop the 3rd arg and delegate to the 2-arg logic
                                    let mut args = f.args;
                                    let end_date = args.remove(0);
                                    let start_date = args.remove(0);
                                    // Re-create as 2-arg and process
                                    let f2 = Function::new(
                                        "MONTHS_BETWEEN".to_string(),
                                        vec![end_date, start_date],
                                    );
                                    let e2 = Expression::Function(Box::new(f2));
                                    normalize(e2, source, target, context.strict)
                                }
                            }
                        }
                        // TO_TIMESTAMP(x) with 1 arg -> CAST(x AS TIMESTAMP) for most targets
                        "TO_TIMESTAMP"
                            if f.args.len() == 1
                                && matches!(
                                    source,
                                    DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Hive
                                ) =>
                        {
                            let arg = f.args.into_iter().next().unwrap();
                            Ok(Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Timestamp {
                                    timezone: false,
                                    precision: None,
                                },
                                trailing_comments: vec![],
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        // STRING(x) -> CAST(x AS STRING) for Spark target
                        "STRING"
                            if f.args.len() == 1
                                && matches!(
                                    source,
                                    DialectType::Spark | DialectType::Databricks
                                ) =>
                        {
                            let arg = f.args.into_iter().next().unwrap();
                            let dt = match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => DataType::Custom {
                                    name: "STRING".to_string(),
                                },
                                _ => DataType::Text,
                            };
                            Ok(Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: dt,
                                trailing_comments: vec![],
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        // LOGICAL_OR(x) -> BOOL_OR(x) for Spark target
                        "LOGICAL_OR" if f.args.len() == 1 => {
                            let name = match target {
                                DialectType::Spark | DialectType::Databricks => "BOOL_OR",
                                _ => "LOGICAL_OR",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // SPLIT(x, pattern) from Spark -> STR_SPLIT_REGEX for DuckDB, REGEXP_SPLIT for Presto
                        "SPLIT"
                            if f.args.len() == 2
                                && matches!(
                                    source,
                                    DialectType::Spark
                                        | DialectType::Databricks
                                        | DialectType::Hive
                                ) =>
                        {
                            let name = match target {
                                DialectType::DuckDB => "STR_SPLIT_REGEX",
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    "REGEXP_SPLIT"
                                }
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => "SPLIT",
                                _ => "SPLIT",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // TRY_ELEMENT_AT -> ELEMENT_AT for Presto, array[idx] for DuckDB
                        "TRY_ELEMENT_AT" if f.args.len() == 2 => match target {
                            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "ELEMENT_AT".to_string(),
                                    f.args,
                                ))))
                            }
                            DialectType::DuckDB => {
                                let mut args = f.args;
                                let arr = args.remove(0);
                                let idx = args.remove(0);
                                Ok(Expression::Subscript(Box::new(
                                    crate::expressions::Subscript {
                                        this: arr,
                                        index: idx,
                                    },
                                )))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // ARRAY_FILTER(arr, lambda) -> FILTER for Hive/Spark/Presto, LIST_FILTER for DuckDB
                        "ARRAY_FILTER" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::DuckDB => "LIST_FILTER",
                                DialectType::StarRocks => "ARRAY_FILTER",
                                _ => "FILTER",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // FILTER(arr, lambda) -> ARRAY_FILTER for StarRocks, LIST_FILTER for DuckDB
                        "FILTER" if f.args.len() == 2 => {
                            let name = match target {
                                DialectType::DuckDB => "LIST_FILTER",
                                DialectType::StarRocks => "ARRAY_FILTER",
                                _ => "FILTER",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // REDUCE(arr, init, lambda1, lambda2) -> AGGREGATE for Spark
                        "REDUCE" if f.args.len() >= 3 => {
                            let name = match target {
                                DialectType::Spark | DialectType::Databricks => "AGGREGATE",
                                _ => "REDUCE",
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                name.to_string(),
                                f.args,
                            ))))
                        }
                        // CURRENT_SCHEMA() -> dialect-specific
                        "CURRENT_SCHEMA" => {
                            match target {
                                DialectType::PostgreSQL => {
                                    // PostgreSQL: CURRENT_SCHEMA (no parens)
                                    Ok(Expression::Function(Box::new(Function {
                                        name: "CURRENT_SCHEMA".to_string(),
                                        args: vec![],
                                        distinct: false,
                                        trailing_comments: vec![],
                                        use_bracket_syntax: false,
                                        no_parens: true,
                                        quoted: false,
                                        span: None,
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::MySQL
                                | DialectType::Doris
                                | DialectType::StarRocks => Ok(Expression::Function(Box::new(
                                    Function::new("SCHEMA".to_string(), vec![]),
                                ))),
                                DialectType::TSQL => Ok(Expression::Function(Box::new(
                                    Function::new("SCHEMA_NAME".to_string(), vec![]),
                                ))),
                                DialectType::SQLite => Ok(Expression::Literal(Box::new(
                                    Literal::String("main".to_string()),
                                ))),
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // LTRIM(str, chars) 2-arg -> TRIM(LEADING chars FROM str) for Spark/Hive/Databricks/ClickHouse
                        "LTRIM" if f.args.len() == 2 => match target {
                            DialectType::Spark
                            | DialectType::Hive
                            | DialectType::Databricks
                            | DialectType::ClickHouse => {
                                let mut args = f.args;
                                let str_expr = args.remove(0);
                                let chars = args.remove(0);
                                Ok(Expression::Trim(Box::new(crate::expressions::TrimFunc {
                                    this: str_expr,
                                    characters: Some(chars),
                                    position: crate::expressions::TrimPosition::Leading,
                                    sql_standard_syntax: true,
                                    position_explicit: true,
                                })))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // RTRIM(str, chars) 2-arg -> TRIM(TRAILING chars FROM str) for Spark/Hive/Databricks/ClickHouse
                        "RTRIM" if f.args.len() == 2 => match target {
                            DialectType::Spark
                            | DialectType::Hive
                            | DialectType::Databricks
                            | DialectType::ClickHouse => {
                                let mut args = f.args;
                                let str_expr = args.remove(0);
                                let chars = args.remove(0);
                                Ok(Expression::Trim(Box::new(crate::expressions::TrimFunc {
                                    this: str_expr,
                                    characters: Some(chars),
                                    position: crate::expressions::TrimPosition::Trailing,
                                    sql_standard_syntax: true,
                                    position_explicit: true,
                                })))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // ARRAY_REVERSE(x) -> arrayReverse(x) for ClickHouse
                        "ARRAY_REVERSE" if f.args.len() == 1 => match target {
                            DialectType::ClickHouse => {
                                let mut new_f = *f;
                                new_f.name = "arrayReverse".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // UUID() -> NEWID() for TSQL
                        "UUID" if f.args.is_empty() => match target {
                            DialectType::TSQL | DialectType::Fabric => Ok(Expression::Function(
                                Box::new(Function::new("NEWID".to_string(), vec![])),
                            )),
                            _ => Ok(Expression::Function(f)),
                        },
                        // FARM_FINGERPRINT(x) -> farmFingerprint64(x) for ClickHouse, FARMFINGERPRINT64(x) for Redshift
                        "FARM_FINGERPRINT" if f.args.len() == 1 => match target {
                            DialectType::ClickHouse => {
                                let mut new_f = *f;
                                new_f.name = "farmFingerprint64".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            DialectType::Redshift => {
                                let mut new_f = *f;
                                new_f.name = "FARMFINGERPRINT64".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // JSON_KEYS(x) -> JSON_OBJECT_KEYS(x) for Databricks/Spark, OBJECT_KEYS(x) for Snowflake
                        "JSON_KEYS" => match target {
                            DialectType::Databricks | DialectType::Spark => {
                                let mut new_f = *f;
                                new_f.name = "JSON_OBJECT_KEYS".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            DialectType::Snowflake => {
                                let mut new_f = *f;
                                new_f.name = "OBJECT_KEYS".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // WEEKOFYEAR(x) -> WEEKISO(x) for Snowflake
                        "WEEKOFYEAR" => match target {
                            DialectType::Snowflake => {
                                let mut new_f = *f;
                                new_f.name = "WEEKISO".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // FORMAT(fmt, args...) -> FORMAT_STRING(fmt, args...) for Databricks
                        "FORMAT" if f.args.len() >= 2 && matches!(source, DialectType::Generic) => {
                            match target {
                                DialectType::Databricks | DialectType::Spark => {
                                    let mut new_f = *f;
                                    new_f.name = "FORMAT_STRING".to_string();
                                    Ok(Expression::Function(Box::new(new_f)))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // CONCAT_WS from Generic is null-propagating in SQLGlot fixtures.
                        // Trino also requires non-separator arguments cast to VARCHAR.
                        "CONCAT_WS" if f.args.len() >= 2 => {
                            fn concat_ws_null_case(
                                args: Vec<Expression>,
                                else_expr: Expression,
                            ) -> Expression {
                                let mut null_checks = args.iter().cloned().map(|arg| {
                                    Expression::IsNull(Box::new(crate::expressions::IsNull {
                                        this: arg,
                                        not: false,
                                        postfix_form: false,
                                    }))
                                });
                                let first_null_check = null_checks
                                    .next()
                                    .expect("CONCAT_WS with >= 2 args must yield a null check");
                                let null_check =
                                    null_checks.fold(first_null_check, |left, right| {
                                        Expression::Or(Box::new(BinaryOp {
                                            left,
                                            right,
                                            left_comments: Vec::new(),
                                            operator_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }))
                                    });
                                Expression::Case(Box::new(Case {
                                    operand: None,
                                    whens: vec![(null_check, Expression::Null(Null))],
                                    else_: Some(else_expr),
                                    comments: vec![],
                                    inferred_type: None,
                                }))
                            }

                            match target {
                                DialectType::Trino if matches!(source, DialectType::Generic) => {
                                    let original_args = f.args.clone();
                                    let mut args = f.args;
                                    let sep = args.remove(0);
                                    let cast_args: Vec<Expression> = args
                                        .into_iter()
                                        .map(|a| {
                                            Expression::Cast(Box::new(Cast {
                                                this: a,
                                                to: DataType::VarChar {
                                                    length: None,
                                                    parenthesized_length: false,
                                                },
                                                double_colon_syntax: false,
                                                trailing_comments: Vec::new(),
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }))
                                        })
                                        .collect();
                                    let mut new_args = vec![sep];
                                    new_args.extend(cast_args);
                                    let else_expr = Expression::Function(Box::new(Function::new(
                                        "CONCAT_WS".to_string(),
                                        new_args,
                                    )));
                                    Ok(concat_ws_null_case(original_args, else_expr))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    let mut args = f.args;
                                    let sep = args.remove(0);
                                    let cast_args: Vec<Expression> = args
                                        .into_iter()
                                        .map(|a| {
                                            Expression::Cast(Box::new(Cast {
                                                this: a,
                                                to: DataType::VarChar {
                                                    length: None,
                                                    parenthesized_length: false,
                                                },
                                                double_colon_syntax: false,
                                                trailing_comments: Vec::new(),
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }))
                                        })
                                        .collect();
                                    let mut new_args = vec![sep];
                                    new_args.extend(cast_args);
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "CONCAT_WS".to_string(),
                                        new_args,
                                    ))))
                                }
                                DialectType::Spark | DialectType::Hive | DialectType::DuckDB
                                    if matches!(source, DialectType::Generic) =>
                                {
                                    let args = f.args;
                                    let else_expr = Expression::Function(Box::new(Function::new(
                                        "CONCAT_WS".to_string(),
                                        args.clone(),
                                    )));
                                    Ok(concat_ws_null_case(args, else_expr))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // ARRAY_SLICE(x, start, end) -> SLICE(x, start, end) for Presto/Trino/Databricks, arraySlice for ClickHouse
                        "ARRAY_SLICE" if f.args.len() >= 2 => match target {
                            DialectType::DuckDB
                                if f.args.len() == 3
                                    && matches!(source, DialectType::Snowflake) =>
                            {
                                // Snowflake ARRAY_SLICE (0-indexed, exclusive end)
                                // -> DuckDB ARRAY_SLICE (1-indexed, inclusive end)
                                let mut args = f.args;
                                let arr = args.remove(0);
                                let start = args.remove(0);
                                let end = args.remove(0);

                                // CASE WHEN start >= 0 THEN start + 1 ELSE start END
                                let adjusted_start = Expression::Case(Box::new(Case {
                                    operand: None,
                                    whens: vec![(
                                        Expression::Gte(Box::new(BinaryOp {
                                            left: start.clone(),
                                            right: Expression::number(0),
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        })),
                                        Expression::Add(Box::new(BinaryOp {
                                            left: start.clone(),
                                            right: Expression::number(1),
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        })),
                                    )],
                                    else_: Some(start),
                                    comments: vec![],
                                    inferred_type: None,
                                }));

                                // CASE WHEN end < 0 THEN end - 1 ELSE end END
                                let adjusted_end = Expression::Case(Box::new(Case {
                                    operand: None,
                                    whens: vec![(
                                        Expression::Lt(Box::new(BinaryOp {
                                            left: end.clone(),
                                            right: Expression::number(0),
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        })),
                                        Expression::Sub(Box::new(BinaryOp {
                                            left: end.clone(),
                                            right: Expression::number(1),
                                            left_comments: vec![],
                                            operator_comments: vec![],
                                            trailing_comments: vec![],
                                            inferred_type: None,
                                        })),
                                    )],
                                    else_: Some(end),
                                    comments: vec![],
                                    inferred_type: None,
                                }));

                                Ok(Expression::Function(Box::new(Function::new(
                                    "ARRAY_SLICE".to_string(),
                                    vec![arr, adjusted_start, adjusted_end],
                                ))))
                            }
                            DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Athena
                            | DialectType::Databricks
                            | DialectType::Spark => {
                                let mut new_f = *f;
                                new_f.name = "SLICE".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            DialectType::ClickHouse => {
                                let mut new_f = *f;
                                new_f.name = "arraySlice".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // ARRAY_PREPEND(arr, x) -> LIST_PREPEND(x, arr) for DuckDB (swap args)
                        "ARRAY_PREPEND" if f.args.len() == 2 => match target {
                            DialectType::DuckDB => {
                                let mut args = f.args;
                                let arr = args.remove(0);
                                let val = args.remove(0);
                                Ok(Expression::Function(Box::new(Function::new(
                                    "LIST_PREPEND".to_string(),
                                    vec![val, arr],
                                ))))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // ARRAY_REMOVE(arr, target) -> dialect-specific
                        "ARRAY_REMOVE" if f.args.len() == 2 => {
                            match target {
                                DialectType::DuckDB => {
                                    let mut args = f.args;
                                    let arr = args.remove(0);
                                    let target_val = args.remove(0);
                                    let u_id = crate::expressions::Identifier::new("_u");
                                    // LIST_FILTER(arr, _u -> _u <> target)
                                    let lambda = Expression::Lambda(Box::new(
                                        crate::expressions::LambdaExpr {
                                            parameters: vec![u_id.clone()],
                                            body: Expression::Neq(Box::new(BinaryOp {
                                                left: Expression::Identifier(u_id),
                                                right: target_val,
                                                left_comments: Vec::new(),
                                                operator_comments: Vec::new(),
                                                trailing_comments: Vec::new(),
                                                inferred_type: None,
                                            })),
                                            colon: false,
                                            parameter_types: Vec::new(),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "LIST_FILTER".to_string(),
                                        vec![arr, lambda],
                                    ))))
                                }
                                DialectType::ClickHouse => {
                                    let mut args = f.args;
                                    let arr = args.remove(0);
                                    let target_val = args.remove(0);
                                    let u_id = crate::expressions::Identifier::new("_u");
                                    // arrayFilter(_u -> _u <> target, arr)
                                    let lambda = Expression::Lambda(Box::new(
                                        crate::expressions::LambdaExpr {
                                            parameters: vec![u_id.clone()],
                                            body: Expression::Neq(Box::new(BinaryOp {
                                                left: Expression::Identifier(u_id),
                                                right: target_val,
                                                left_comments: Vec::new(),
                                                operator_comments: Vec::new(),
                                                trailing_comments: Vec::new(),
                                                inferred_type: None,
                                            })),
                                            colon: false,
                                            parameter_types: Vec::new(),
                                        },
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "arrayFilter".to_string(),
                                        vec![lambda, arr],
                                    ))))
                                }
                                DialectType::BigQuery => {
                                    // ARRAY(SELECT _u FROM UNNEST(the_array) AS _u WHERE _u <> target)
                                    let mut args = f.args;
                                    let arr = args.remove(0);
                                    let target_val = args.remove(0);
                                    let u_id = crate::expressions::Identifier::new("_u");
                                    let u_col =
                                        Expression::Column(Box::new(crate::expressions::Column {
                                            name: u_id.clone(),
                                            table: None,
                                            join_mark: false,
                                            trailing_comments: Vec::new(),
                                            span: None,
                                            inferred_type: None,
                                        }));
                                    // UNNEST(the_array) AS _u
                                    let unnest_expr = Expression::Unnest(Box::new(
                                        crate::expressions::UnnestFunc {
                                            this: arr,
                                            expressions: Vec::new(),
                                            with_ordinality: false,
                                            alias: None,
                                            offset_alias: None,
                                        },
                                    ));
                                    let aliased_unnest =
                                        Expression::Alias(Box::new(crate::expressions::Alias {
                                            this: unnest_expr,
                                            alias: u_id.clone(),
                                            column_aliases: Vec::new(),
                                            alias_explicit_as: false,
                                            alias_keyword: None,
                                            pre_alias_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }));
                                    // _u <> target
                                    let where_cond = Expression::Neq(Box::new(BinaryOp {
                                        left: u_col.clone(),
                                        right: target_val,
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    // SELECT _u FROM UNNEST(the_array) AS _u WHERE _u <> target
                                    let subquery = Expression::Select(Box::new(
                                        crate::expressions::Select::new()
                                            .column(u_col)
                                            .from(aliased_unnest)
                                            .where_(where_cond),
                                    ));
                                    // ARRAY(subquery) -- use ArrayFunc with subquery as single element
                                    Ok(Expression::ArrayFunc(Box::new(
                                        crate::expressions::ArrayConstructor {
                                            expressions: vec![subquery],
                                            bracket_notation: false,
                                            use_list_keyword: false,
                                        },
                                    )))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // PARSE_JSON(str) -> remove for SQLite/Doris (just use the string literal)
                        "PARSE_JSON" if f.args.len() == 1 => {
                            match target {
                                DialectType::SQLite
                                | DialectType::Doris
                                | DialectType::MySQL
                                | DialectType::StarRocks => {
                                    // Strip PARSE_JSON, return the inner argument
                                    Ok(f.args.into_iter().next().unwrap())
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // JSON_REMOVE(PARSE_JSON(str), path...) -> for SQLite strip PARSE_JSON
                        // This is handled by PARSE_JSON stripping above; JSON_REMOVE is passed through
                        "JSON_REMOVE" => Ok(Expression::Function(f)),
                        // JSON_SET(PARSE_JSON(str), path, PARSE_JSON(val)) -> for SQLite strip PARSE_JSON
                        // This is handled by PARSE_JSON stripping above; JSON_SET is passed through
                        "JSON_SET" => Ok(Expression::Function(f)),
                        // DECODE(x, search1, result1, ..., default) -> CASE WHEN
                        // Behavior per search value type:
                        //   NULL literal -> CASE WHEN x IS NULL THEN result
                        //   Literal (number, string, bool) -> CASE WHEN x = literal THEN result
                        //   Non-literal (column, expr) -> CASE WHEN x = search OR (x IS NULL AND search IS NULL) THEN result
                        "DECODE" if f.args.len() >= 3 => {
                            // Keep as DECODE for targets that support it natively
                            let keep_as_decode = matches!(
                                target,
                                DialectType::Oracle
                                    | DialectType::Snowflake
                                    | DialectType::Redshift
                                    | DialectType::Teradata
                                    | DialectType::Spark
                                    | DialectType::Databricks
                            );
                            if keep_as_decode {
                                return Ok(Expression::Function(f));
                            }

                            let mut args = f.args;
                            let this_expr = args.remove(0);
                            let mut pairs = Vec::new();
                            let mut default = None;
                            let mut i = 0;
                            while i + 1 < args.len() {
                                pairs.push((args[i].clone(), args[i + 1].clone()));
                                i += 2;
                            }
                            if i < args.len() {
                                default = Some(args[i].clone());
                            }
                            // Helper: check if expression is a literal value
                            fn is_literal(e: &Expression) -> bool {
                                matches!(
                                    e,
                                    Expression::Literal(_)
                                        | Expression::Boolean(_)
                                        | Expression::Neg(_)
                                )
                            }
                            let whens: Vec<(Expression, Expression)> = pairs
                                .into_iter()
                                .map(|(search, result)| {
                                    if matches!(&search, Expression::Null(_)) {
                                        // NULL search -> IS NULL
                                        let condition = Expression::Is(Box::new(BinaryOp {
                                            left: this_expr.clone(),
                                            right: Expression::Null(crate::expressions::Null),
                                            left_comments: Vec::new(),
                                            operator_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }));
                                        (condition, result)
                                    } else if is_literal(&search) {
                                        // Literal search -> simple equality
                                        let eq = Expression::Eq(Box::new(BinaryOp {
                                            left: this_expr.clone(),
                                            right: search,
                                            left_comments: Vec::new(),
                                            operator_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }));
                                        (eq, result)
                                    } else {
                                        // Non-literal (column ref, expression) -> null-safe comparison
                                        let needs_paren = matches!(
                                            &search,
                                            Expression::Eq(_)
                                                | Expression::Neq(_)
                                                | Expression::Gt(_)
                                                | Expression::Gte(_)
                                                | Expression::Lt(_)
                                                | Expression::Lte(_)
                                        );
                                        let search_for_eq = if needs_paren {
                                            Expression::Paren(Box::new(crate::expressions::Paren {
                                                this: search.clone(),
                                                trailing_comments: Vec::new(),
                                            }))
                                        } else {
                                            search.clone()
                                        };
                                        let eq = Expression::Eq(Box::new(BinaryOp {
                                            left: this_expr.clone(),
                                            right: search_for_eq,
                                            left_comments: Vec::new(),
                                            operator_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }));
                                        let search_for_null = if needs_paren {
                                            Expression::Paren(Box::new(crate::expressions::Paren {
                                                this: search.clone(),
                                                trailing_comments: Vec::new(),
                                            }))
                                        } else {
                                            search.clone()
                                        };
                                        let x_is_null = Expression::Is(Box::new(BinaryOp {
                                            left: this_expr.clone(),
                                            right: Expression::Null(crate::expressions::Null),
                                            left_comments: Vec::new(),
                                            operator_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }));
                                        let s_is_null = Expression::Is(Box::new(BinaryOp {
                                            left: search_for_null,
                                            right: Expression::Null(crate::expressions::Null),
                                            left_comments: Vec::new(),
                                            operator_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }));
                                        let both_null = Expression::And(Box::new(BinaryOp {
                                            left: x_is_null,
                                            right: s_is_null,
                                            left_comments: Vec::new(),
                                            operator_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }));
                                        let condition = Expression::Or(Box::new(BinaryOp {
                                            left: eq,
                                            right: Expression::Paren(Box::new(
                                                crate::expressions::Paren {
                                                    this: both_null,
                                                    trailing_comments: Vec::new(),
                                                },
                                            )),
                                            left_comments: Vec::new(),
                                            operator_comments: Vec::new(),
                                            trailing_comments: Vec::new(),
                                            inferred_type: None,
                                        }));
                                        (condition, result)
                                    }
                                })
                                .collect();
                            Ok(Expression::Case(Box::new(Case {
                                operand: None,
                                whens,
                                else_: default,
                                comments: Vec::new(),
                                inferred_type: None,
                            })))
                        }
                        // LEVENSHTEIN(a, b, ...) -> dialect-specific
                        "LEVENSHTEIN" => {
                            match target {
                                DialectType::BigQuery => {
                                    let mut new_f = *f;
                                    new_f.name = "EDIT_DISTANCE".to_string();
                                    Ok(Expression::Function(Box::new(new_f)))
                                }
                                DialectType::Drill => {
                                    let mut new_f = *f;
                                    new_f.name = "LEVENSHTEIN_DISTANCE".to_string();
                                    Ok(Expression::Function(Box::new(new_f)))
                                }
                                DialectType::PostgreSQL if f.args.len() == 6 => {
                                    // PostgreSQL: LEVENSHTEIN(src, tgt, ins, del, sub, max_d) -> LEVENSHTEIN_LESS_EQUAL
                                    // 2 args: basic, 5 args: with costs, 6 args: with costs + max_distance
                                    let mut new_f = *f;
                                    new_f.name = "LEVENSHTEIN_LESS_EQUAL".to_string();
                                    Ok(Expression::Function(Box::new(new_f)))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        // ARRAY_MAX(x) -> arrayMax(x) for ClickHouse, LIST_MAX(x) for DuckDB
                        "ARRAY_MAX" => {
                            let name = match target {
                                DialectType::ClickHouse => "arrayMax",
                                DialectType::DuckDB => "LIST_MAX",
                                _ => "ARRAY_MAX",
                            };
                            let mut new_f = *f;
                            new_f.name = name.to_string();
                            Ok(Expression::Function(Box::new(new_f)))
                        }
                        // ARRAY_MIN(x) -> arrayMin(x) for ClickHouse, LIST_MIN(x) for DuckDB
                        "ARRAY_MIN" => {
                            let name = match target {
                                DialectType::ClickHouse => "arrayMin",
                                DialectType::DuckDB => "LIST_MIN",
                                _ => "ARRAY_MIN",
                            };
                            let mut new_f = *f;
                            new_f.name = name.to_string();
                            Ok(Expression::Function(Box::new(new_f)))
                        }
                        // JAROWINKLER_SIMILARITY(a, b) -> jaroWinklerSimilarity(UPPER(a), UPPER(b)) for ClickHouse
                        // -> JARO_WINKLER_SIMILARITY(UPPER(a), UPPER(b)) for DuckDB
                        "JAROWINKLER_SIMILARITY" if f.args.len() == 2 => {
                            let mut args = f.args;
                            let b = args.pop().unwrap();
                            let a = args.pop().unwrap();
                            match target {
                                DialectType::ClickHouse => {
                                    let upper_a = Expression::Upper(Box::new(
                                        crate::expressions::UnaryFunc::new(a),
                                    ));
                                    let upper_b = Expression::Upper(Box::new(
                                        crate::expressions::UnaryFunc::new(b),
                                    ));
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "jaroWinklerSimilarity".to_string(),
                                        vec![upper_a, upper_b],
                                    ))))
                                }
                                DialectType::DuckDB => {
                                    let upper_a = Expression::Upper(Box::new(
                                        crate::expressions::UnaryFunc::new(a),
                                    ));
                                    let upper_b = Expression::Upper(Box::new(
                                        crate::expressions::UnaryFunc::new(b),
                                    ));
                                    let score = Expression::Function(Box::new(Function::new(
                                        "JARO_WINKLER_SIMILARITY".to_string(),
                                        vec![upper_a, upper_b],
                                    )));
                                    let scaled = Expression::Mul(Box::new(BinaryOp {
                                        left: score,
                                        right: Expression::number(100),
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    Ok(Expression::Cast(Box::new(Cast {
                                        this: scaled,
                                        to: DataType::Int {
                                            length: None,
                                            integer_spelling: false,
                                        },
                                        trailing_comments: Vec::new(),
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                _ => Ok(Expression::Function(Box::new(Function::new(
                                    "JAROWINKLER_SIMILARITY".to_string(),
                                    vec![a, b],
                                )))),
                            }
                        }
                        // CURRENT_SCHEMAS(x) -> CURRENT_SCHEMAS() for Snowflake (drop arg)
                        "CURRENT_SCHEMAS" => match target {
                            DialectType::Snowflake => Ok(Expression::Function(Box::new(
                                Function::new("CURRENT_SCHEMAS".to_string(), vec![]),
                            ))),
                            _ => Ok(Expression::Function(f)),
                        },
                        // TRUNC/TRUNCATE (numeric) -> dialect-specific
                        "TRUNC" | "TRUNCATE" if f.args.len() <= 2 => {
                            match target {
                                DialectType::TSQL | DialectType::Fabric => {
                                    // ROUND(x, decimals, 1) - the 1 flag means truncation
                                    let mut args = f.args;
                                    let this = if args.is_empty() {
                                        return Ok(Expression::Function(Box::new(Function::new(
                                            "TRUNC".to_string(),
                                            args,
                                        ))));
                                    } else {
                                        args.remove(0)
                                    };
                                    let decimals = if args.is_empty() {
                                        Expression::Literal(Box::new(Literal::Number(
                                            "0".to_string(),
                                        )))
                                    } else {
                                        args.remove(0)
                                    };
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "ROUND".to_string(),
                                        vec![
                                            this,
                                            decimals,
                                            Expression::Literal(Box::new(Literal::Number(
                                                "1".to_string(),
                                            ))),
                                        ],
                                    ))))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // TRUNCATE(x, decimals)
                                    let mut new_f = *f;
                                    new_f.name = "TRUNCATE".to_string();
                                    Ok(Expression::Function(Box::new(new_f)))
                                }
                                DialectType::MySQL
                                | DialectType::SingleStore
                                | DialectType::TiDB => {
                                    // TRUNCATE(x, decimals)
                                    let mut new_f = *f;
                                    new_f.name = "TRUNCATE".to_string();
                                    Ok(Expression::Function(Box::new(new_f)))
                                }
                                DialectType::DuckDB => {
                                    // DuckDB supports TRUNC(x, decimals) — preserve both args
                                    let mut args = f.args;
                                    // Snowflake fractions_supported: wrap non-INT decimals in CAST(... AS INT)
                                    if args.len() == 2 && matches!(source, DialectType::Snowflake) {
                                        let decimals = args.remove(1);
                                        let is_int = matches!(&decimals, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(_)))
                                            || matches!(&decimals, Expression::Cast(c) if matches!(c.to, DataType::Int { .. } | DataType::SmallInt { .. } | DataType::BigInt { .. } | DataType::TinyInt { .. }));
                                        let wrapped = if !is_int {
                                            Expression::Cast(Box::new(crate::expressions::Cast {
                                                this: decimals,
                                                to: DataType::Int {
                                                    length: None,
                                                    integer_spelling: false,
                                                },
                                                double_colon_syntax: false,
                                                trailing_comments: Vec::new(),
                                                format: None,
                                                default: None,
                                                inferred_type: None,
                                            }))
                                        } else {
                                            decimals
                                        };
                                        args.push(wrapped);
                                    }
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TRUNC".to_string(),
                                        args,
                                    ))))
                                }
                                DialectType::ClickHouse => {
                                    // trunc(x, decimals) - lowercase
                                    let mut new_f = *f;
                                    new_f.name = "trunc".to_string();
                                    Ok(Expression::Function(Box::new(new_f)))
                                }
                                DialectType::Spark | DialectType::Databricks => {
                                    // Spark: TRUNC is date-only; numeric TRUNC → CAST(x AS BIGINT)
                                    let this =
                                        f.args.into_iter().next().unwrap_or(Expression::Literal(
                                            Box::new(Literal::Number("0".to_string())),
                                        ));
                                    Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                                        this,
                                        to: crate::expressions::DataType::BigInt { length: None },
                                        double_colon_syntax: false,
                                        trailing_comments: Vec::new(),
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                _ => {
                                    // TRUNC(x, decimals) for PostgreSQL, Oracle, Snowflake, etc.
                                    let mut new_f = *f;
                                    new_f.name = "TRUNC".to_string();
                                    Ok(Expression::Function(Box::new(new_f)))
                                }
                            }
                        }
                        // CURRENT_VERSION() -> VERSION() for most dialects
                        "CURRENT_VERSION" => match target {
                            DialectType::Snowflake
                            | DialectType::Databricks
                            | DialectType::StarRocks => Ok(Expression::Function(f)),
                            DialectType::SQLite => {
                                let mut new_f = *f;
                                new_f.name = "SQLITE_VERSION".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            _ => {
                                let mut new_f = *f;
                                new_f.name = "VERSION".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                        },
                        // ARRAY_REVERSE(x) -> arrayReverse(x) for ClickHouse
                        "ARRAY_REVERSE" => match target {
                            DialectType::ClickHouse => {
                                let mut new_f = *f;
                                new_f.name = "arrayReverse".to_string();
                                Ok(Expression::Function(Box::new(new_f)))
                            }
                            _ => Ok(Expression::Function(f)),
                        },
                        // GENERATE_DATE_ARRAY(start, end[, step]) -> target-specific
                        "GENERATE_DATE_ARRAY" => {
                            let mut args = f.args;
                            if matches!(target, DialectType::BigQuery) {
                                // BigQuery keeps GENERATE_DATE_ARRAY; add default interval if not present
                                if args.len() == 2 {
                                    let default_interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::Literal(Box::new(
                                                Literal::String("1".to_string()),
                                            ))),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    args.push(default_interval);
                                }
                                Ok(Expression::Function(Box::new(Function::new(
                                    "GENERATE_DATE_ARRAY".to_string(),
                                    args,
                                ))))
                            } else if matches!(target, DialectType::DuckDB) {
                                // DuckDB: CAST(GENERATE_SERIES(start, end, step) AS DATE[])
                                let start = args.get(0).cloned();
                                let end = args.get(1).cloned();
                                let step = args.get(2).cloned().or_else(|| {
                                    Some(Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::Literal(Box::new(
                                                Literal::String("1".to_string()),
                                            ))),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    )))
                                });
                                let gen_series = Expression::GenerateSeries(Box::new(
                                    crate::expressions::GenerateSeries {
                                        start: start.map(Box::new),
                                        end: end.map(Box::new),
                                        step: step.map(Box::new),
                                        is_end_exclusive: None,
                                    },
                                ));
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: gen_series,
                                    to: DataType::Array {
                                        element_type: Box::new(DataType::Date),
                                        dimension: None,
                                    },
                                    trailing_comments: vec![],
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else if matches!(
                                target,
                                DialectType::Presto | DialectType::Trino | DialectType::Athena
                            ) {
                                // Presto/Trino: SEQUENCE(start, end, interval) with interval normalization
                                let start = args.get(0).cloned();
                                let end = args.get(1).cloned();
                                let step = args.get(2).cloned().or_else(|| {
                                    Some(Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::Literal(Box::new(
                                                Literal::String("1".to_string()),
                                            ))),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    )))
                                });
                                let gen_series = Expression::GenerateSeries(Box::new(
                                    crate::expressions::GenerateSeries {
                                        start: start.map(Box::new),
                                        end: end.map(Box::new),
                                        step: step.map(Box::new),
                                        is_end_exclusive: None,
                                    },
                                ));
                                Ok(gen_series)
                            } else if matches!(target, DialectType::Spark | DialectType::Databricks)
                            {
                                // Spark/Databricks: SEQUENCE(start, end, step) - keep step as-is
                                let start = args.get(0).cloned();
                                let end = args.get(1).cloned();
                                let step = args.get(2).cloned().or_else(|| {
                                    Some(Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::Literal(Box::new(
                                                Literal::String("1".to_string()),
                                            ))),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    )))
                                });
                                let gen_series = Expression::GenerateSeries(Box::new(
                                    crate::expressions::GenerateSeries {
                                        start: start.map(Box::new),
                                        end: end.map(Box::new),
                                        step: step.map(Box::new),
                                        is_end_exclusive: None,
                                    },
                                ));
                                Ok(gen_series)
                            } else if matches!(target, DialectType::Snowflake) {
                                // Snowflake: keep as GENERATE_DATE_ARRAY for later transform
                                if args.len() == 2 {
                                    let default_interval = Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::Literal(Box::new(
                                                Literal::String("1".to_string()),
                                            ))),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    ));
                                    args.push(default_interval);
                                }
                                Ok(Expression::Function(Box::new(Function::new(
                                    "GENERATE_DATE_ARRAY".to_string(),
                                    args,
                                ))))
                            } else if matches!(
                                target,
                                DialectType::MySQL
                                    | DialectType::TSQL
                                    | DialectType::Fabric
                                    | DialectType::Redshift
                            ) {
                                // MySQL/TSQL/Redshift: keep as GENERATE_DATE_ARRAY for the preprocess
                                // step (unnest_generate_date_array_using_recursive_cte) to convert to CTE
                                Ok(Expression::Function(Box::new(Function::new(
                                    "GENERATE_DATE_ARRAY".to_string(),
                                    args,
                                ))))
                            } else {
                                // PostgreSQL/others: convert to GenerateSeries
                                let start = args.get(0).cloned();
                                let end = args.get(1).cloned();
                                let step = args.get(2).cloned().or_else(|| {
                                    Some(Expression::Interval(Box::new(
                                        crate::expressions::Interval {
                                            this: Some(Expression::Literal(Box::new(
                                                Literal::String("1".to_string()),
                                            ))),
                                            unit: Some(
                                                crate::expressions::IntervalUnitSpec::Simple {
                                                    unit: crate::expressions::IntervalUnit::Day,
                                                    use_plural: false,
                                                },
                                            ),
                                        },
                                    )))
                                });
                                Ok(Expression::GenerateSeries(Box::new(
                                    crate::expressions::GenerateSeries {
                                        start: start.map(Box::new),
                                        end: end.map(Box::new),
                                        step: step.map(Box::new),
                                        is_end_exclusive: None,
                                    },
                                )))
                            }
                        }
                        // ARRAYS_OVERLAP(arr1, arr2) from Snowflake -> DuckDB:
                        // (arr1 && arr2) OR (ARRAY_LENGTH(arr1) <> LIST_COUNT(arr1) AND ARRAY_LENGTH(arr2) <> LIST_COUNT(arr2))
                        "ARRAYS_OVERLAP"
                            if f.args.len() == 2
                                && matches!(source, DialectType::Snowflake)
                                && matches!(target, DialectType::DuckDB) =>
                        {
                            let mut args = f.args;
                            let arr1 = args.remove(0);
                            let arr2 = args.remove(0);

                            // (arr1 && arr2)
                            let overlap = Expression::Paren(Box::new(Paren {
                                this: Expression::ArrayOverlaps(Box::new(BinaryOp {
                                    left: arr1.clone(),
                                    right: arr2.clone(),
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                })),
                                trailing_comments: vec![],
                            }));

                            // ARRAY_LENGTH(arr1) <> LIST_COUNT(arr1)
                            let arr1_has_null = Expression::Neq(Box::new(BinaryOp {
                                left: Expression::Function(Box::new(Function::new(
                                    "ARRAY_LENGTH".to_string(),
                                    vec![arr1.clone()],
                                ))),
                                right: Expression::Function(Box::new(Function::new(
                                    "LIST_COUNT".to_string(),
                                    vec![arr1],
                                ))),
                                left_comments: vec![],
                                operator_comments: vec![],
                                trailing_comments: vec![],
                                inferred_type: None,
                            }));

                            // ARRAY_LENGTH(arr2) <> LIST_COUNT(arr2)
                            let arr2_has_null = Expression::Neq(Box::new(BinaryOp {
                                left: Expression::Function(Box::new(Function::new(
                                    "ARRAY_LENGTH".to_string(),
                                    vec![arr2.clone()],
                                ))),
                                right: Expression::Function(Box::new(Function::new(
                                    "LIST_COUNT".to_string(),
                                    vec![arr2],
                                ))),
                                left_comments: vec![],
                                operator_comments: vec![],
                                trailing_comments: vec![],
                                inferred_type: None,
                            }));

                            // (ARRAY_LENGTH(arr1) <> LIST_COUNT(arr1) AND ARRAY_LENGTH(arr2) <> LIST_COUNT(arr2))
                            let null_check = Expression::Paren(Box::new(Paren {
                                this: Expression::And(Box::new(BinaryOp {
                                    left: arr1_has_null,
                                    right: arr2_has_null,
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                })),
                                trailing_comments: vec![],
                            }));

                            // (arr1 && arr2) OR (null_check)
                            Ok(Expression::Or(Box::new(BinaryOp {
                                left: overlap,
                                right: null_check,
                                left_comments: vec![],
                                operator_comments: vec![],
                                trailing_comments: vec![],
                                inferred_type: None,
                            })))
                        }
                        // ARRAY_INTERSECTION([1, 2], [2, 3]) from Snowflake -> DuckDB:
                        // Bag semantics using LIST_TRANSFORM/LIST_FILTER with GENERATE_SERIES
                        "ARRAY_INTERSECTION"
                            if f.args.len() == 2
                                && matches!(source, DialectType::Snowflake)
                                && matches!(target, DialectType::DuckDB) =>
                        {
                            let mut args = f.args;
                            let arr1 = args.remove(0);
                            let arr2 = args.remove(0);

                            // Build: arr1 IS NULL
                            let arr1_is_null = Expression::IsNull(Box::new(IsNull {
                                this: arr1.clone(),
                                not: false,
                                postfix_form: false,
                            }));
                            let arr2_is_null = Expression::IsNull(Box::new(IsNull {
                                this: arr2.clone(),
                                not: false,
                                postfix_form: false,
                            }));
                            let null_check = Expression::Or(Box::new(BinaryOp {
                                left: arr1_is_null,
                                right: arr2_is_null,
                                left_comments: vec![],
                                operator_comments: vec![],
                                trailing_comments: vec![],
                                inferred_type: None,
                            }));

                            // GENERATE_SERIES(1, LENGTH(arr1))
                            let gen_series = Expression::Function(Box::new(Function::new(
                                "GENERATE_SERIES".to_string(),
                                vec![
                                    Expression::number(1),
                                    Expression::Function(Box::new(Function::new(
                                        "LENGTH".to_string(),
                                        vec![arr1.clone()],
                                    ))),
                                ],
                            )));

                            // LIST_ZIP(arr1, GENERATE_SERIES(1, LENGTH(arr1)))
                            let list_zip = Expression::Function(Box::new(Function::new(
                                "LIST_ZIP".to_string(),
                                vec![arr1.clone(), gen_series],
                            )));

                            // pair[1] and pair[2]
                            let pair_col = Expression::column("pair");
                            let pair_1 =
                                Expression::Subscript(Box::new(crate::expressions::Subscript {
                                    this: pair_col.clone(),
                                    index: Expression::number(1),
                                }));
                            let pair_2 =
                                Expression::Subscript(Box::new(crate::expressions::Subscript {
                                    this: pair_col.clone(),
                                    index: Expression::number(2),
                                }));

                            // arr1[1:pair[2]]
                            let arr1_slice =
                                Expression::ArraySlice(Box::new(crate::expressions::ArraySlice {
                                    this: arr1.clone(),
                                    start: Some(Expression::number(1)),
                                    end: Some(pair_2),
                                }));

                            // e IS NOT DISTINCT FROM pair[1]
                            let e_col = Expression::column("e");
                            let is_not_distinct = Expression::NullSafeEq(Box::new(BinaryOp {
                                left: e_col.clone(),
                                right: pair_1.clone(),
                                left_comments: vec![],
                                operator_comments: vec![],
                                trailing_comments: vec![],
                                inferred_type: None,
                            }));

                            // e -> e IS NOT DISTINCT FROM pair[1]
                            let inner_lambda1 =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("e")],
                                    body: is_not_distinct,
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_FILTER(arr1[1:pair[2]], e -> e IS NOT DISTINCT FROM pair[1])
                            let inner_filter1 = Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![arr1_slice, inner_lambda1],
                            )));

                            // LENGTH(LIST_FILTER(arr1[1:pair[2]], ...))
                            let len1 = Expression::Function(Box::new(Function::new(
                                "LENGTH".to_string(),
                                vec![inner_filter1],
                            )));

                            // e -> e IS NOT DISTINCT FROM pair[1]
                            let inner_lambda2 =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("e")],
                                    body: Expression::NullSafeEq(Box::new(BinaryOp {
                                        left: e_col,
                                        right: pair_1.clone(),
                                        left_comments: vec![],
                                        operator_comments: vec![],
                                        trailing_comments: vec![],
                                        inferred_type: None,
                                    })),
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_FILTER(arr2, e -> e IS NOT DISTINCT FROM pair[1])
                            let inner_filter2 = Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![arr2.clone(), inner_lambda2],
                            )));

                            // LENGTH(LIST_FILTER(arr2, ...))
                            let len2 = Expression::Function(Box::new(Function::new(
                                "LENGTH".to_string(),
                                vec![inner_filter2],
                            )));

                            // LENGTH(...) <= LENGTH(...)
                            let cond = Expression::Paren(Box::new(Paren {
                                this: Expression::Lte(Box::new(BinaryOp {
                                    left: len1,
                                    right: len2,
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                })),
                                trailing_comments: vec![],
                            }));

                            // pair -> (condition)
                            let filter_lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("pair")],
                                    body: cond,
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_FILTER(LIST_ZIP(...), pair -> ...)
                            let outer_filter = Expression::Function(Box::new(Function::new(
                                "LIST_FILTER".to_string(),
                                vec![list_zip, filter_lambda],
                            )));

                            // pair -> pair[1]
                            let transform_lambda =
                                Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                                    parameters: vec![crate::expressions::Identifier::new("pair")],
                                    body: pair_1,
                                    colon: false,
                                    parameter_types: vec![],
                                }));

                            // LIST_TRANSFORM(LIST_FILTER(...), pair -> pair[1])
                            let list_transform = Expression::Function(Box::new(Function::new(
                                "LIST_TRANSFORM".to_string(),
                                vec![outer_filter, transform_lambda],
                            )));

                            // CASE WHEN arr1 IS NULL OR arr2 IS NULL THEN NULL
                            // ELSE LIST_TRANSFORM(LIST_FILTER(...), pair -> pair[1])
                            // END
                            Ok(Expression::Case(Box::new(Case {
                                operand: None,
                                whens: vec![(null_check, Expression::Null(Null))],
                                else_: Some(list_transform),
                                comments: vec![],
                                inferred_type: None,
                            })))
                        }
                        // ARRAY_CONSTRUCT(args) -> Expression::Array for all targets
                        "ARRAY_CONSTRUCT" => {
                            if matches!(target, DialectType::Snowflake) {
                                Ok(Expression::Function(f))
                            } else {
                                Ok(Expression::Array(Box::new(crate::expressions::Array {
                                    expressions: f.args,
                                })))
                            }
                        }
                        // ARRAY(args) function -> Expression::Array for DuckDB/Snowflake/Presto/Trino/Athena
                        "ARRAY"
                            if !f.args.iter().any(|a| {
                                matches!(a, Expression::Select(_) | Expression::Subquery(_))
                            }) =>
                        {
                            match target {
                                DialectType::DuckDB
                                | DialectType::Snowflake
                                | DialectType::Presto
                                | DialectType::Trino
                                | DialectType::Athena => {
                                    Ok(Expression::Array(Box::new(crate::expressions::Array {
                                        expressions: f.args,
                                    })))
                                }
                                _ => Ok(Expression::Function(f)),
                            }
                        }
                        _ => Ok(Expression::Function(f)),
                    }
                } else if let Expression::AggregateFunction(mut af) = e {
                    let name = af.name.to_ascii_uppercase();
                    match name.as_str() {
                        "ARBITRARY" if af.args.len() == 1 => {
                            let arg = af.args.into_iter().next().unwrap();
                            Ok(convert_arbitrary(arg, target))
                        }
                        "JSON_ARRAYAGG" => {
                            match target {
                                DialectType::PostgreSQL => {
                                    af.name = "JSON_AGG".to_string();
                                    // Add NULLS FIRST to ORDER BY items for PostgreSQL
                                    for ordered in af.order_by.iter_mut() {
                                        if ordered.nulls_first.is_none() {
                                            ordered.nulls_first = Some(true);
                                        }
                                    }
                                    Ok(Expression::AggregateFunction(af))
                                }
                                _ => Ok(Expression::AggregateFunction(af)),
                            }
                        }
                        _ => Ok(Expression::AggregateFunction(af)),
                    }
                } else if let Expression::JSONArrayAgg(ja) = e {
                    // JSONArrayAgg -> JSON_AGG for PostgreSQL, JSON_ARRAYAGG for others
                    match target {
                        DialectType::PostgreSQL => {
                            let mut order_by = Vec::new();
                            if let Some(order_expr) = ja.order {
                                if let Expression::OrderBy(ob) = *order_expr {
                                    for mut ordered in ob.expressions {
                                        if ordered.nulls_first.is_none() {
                                            ordered.nulls_first = Some(true);
                                        }
                                        order_by.push(ordered);
                                    }
                                }
                            }
                            Ok(Expression::AggregateFunction(Box::new(
                                crate::expressions::AggregateFunction {
                                    name: "JSON_AGG".to_string(),
                                    args: vec![*ja.this],
                                    distinct: false,
                                    filter: None,
                                    order_by,
                                    limit: None,
                                    ignore_nulls: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                        _ => Ok(Expression::JSONArrayAgg(ja)),
                    }
                } else if let Expression::JSONArray(ja) = e {
                    match target {
                        DialectType::Snowflake
                            if ja.null_handling.is_none()
                                && ja.return_type.is_none()
                                && ja.strict.is_none() =>
                        {
                            let array_construct = Expression::ArrayFunc(Box::new(
                                crate::expressions::ArrayConstructor {
                                    expressions: ja.expressions,
                                    bracket_notation: false,
                                    use_list_keyword: false,
                                },
                            ));
                            Ok(Expression::Function(Box::new(Function::new(
                                "TO_VARIANT".to_string(),
                                vec![array_construct],
                            ))))
                        }
                        _ => Ok(Expression::JSONArray(ja)),
                    }
                } else if let Expression::JsonArray(f) = e {
                    match target {
                        DialectType::Snowflake => {
                            let array_construct = Expression::ArrayFunc(Box::new(
                                crate::expressions::ArrayConstructor {
                                    expressions: f.expressions,
                                    bracket_notation: false,
                                    use_list_keyword: false,
                                },
                            ));
                            Ok(Expression::Function(Box::new(Function::new(
                                "TO_VARIANT".to_string(),
                                vec![array_construct],
                            ))))
                        }
                        _ => Ok(Expression::JsonArray(f)),
                    }
                } else if let Expression::CombinedParameterizedAgg(cpa) = e {
                    let function_name = match cpa.this.as_ref() {
                        Expression::Identifier(ident) => Some(ident.name.as_str()),
                        _ => None,
                    };
                    match function_name {
                        Some(name)
                            if name.eq_ignore_ascii_case("groupConcat")
                                && cpa.expressions.len() == 1 =>
                        {
                            match target {
                                DialectType::MySQL | DialectType::SingleStore => {
                                    let this = cpa.expressions[0].clone();
                                    let separator = cpa.params.first().cloned();
                                    Ok(Expression::GroupConcat(Box::new(
                                        crate::expressions::GroupConcatFunc {
                                            this,
                                            separator,
                                            order_by: None,
                                            distinct: false,
                                            filter: None,
                                            limit: None,
                                            inferred_type: None,
                                        },
                                    )))
                                }
                                DialectType::DuckDB => Ok(Expression::ListAgg(Box::new({
                                    let this = cpa.expressions[0].clone();
                                    let separator = cpa.params.first().cloned();
                                    crate::expressions::ListAggFunc {
                                        this,
                                        separator,
                                        on_overflow: None,
                                        order_by: None,
                                        distinct: false,
                                        filter: None,
                                        inferred_type: None,
                                    }
                                }))),
                                _ => Ok(Expression::CombinedParameterizedAgg(cpa)),
                            }
                        }
                        _ => Ok(Expression::CombinedParameterizedAgg(cpa)),
                    }
                } else if let Expression::ToNumber(tn) = e {
                    // TO_NUMBER(x) with no format/precision/scale -> CAST(x AS DOUBLE)
                    let arg = *tn.this;
                    Ok(Expression::Cast(Box::new(crate::expressions::Cast {
                        this: arg,
                        to: crate::expressions::DataType::Double {
                            precision: None,
                            scale: None,
                        },
                        double_colon_syntax: false,
                        trailing_comments: Vec::new(),
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                } else {
                    Ok(e)
                }
            }

            Action::NvlClearOriginal => {
                if let Expression::Nvl(mut f) = e {
                    f.original_name = None;
                    Ok(Expression::Nvl(f))
                } else {
                    Ok(e)
                }
            }
            Action::CurrentUserParens => {
                // CURRENT_USER -> CURRENT_USER() for Snowflake
                Ok(Expression::Function(Box::new(Function::new(
                    "CURRENT_USER".to_string(),
                    vec![],
                ))))
            }

            Action::EscapeStringNormalize => {
                if let Expression::Literal(ref lit) = e {
                    if let Literal::EscapeString(s) = lit.as_ref() {
                        // Strip prefix (e.g., "e:" or "E:") if present from tokenizer
                        let stripped = if s.starts_with("e:") || s.starts_with("E:") {
                            s[2..].to_string()
                        } else {
                            s.clone()
                        };
                        let normalized = stripped
                            .replace('\n', "\\n")
                            .replace('\r', "\\r")
                            .replace('\t', "\\t");
                        match target {
                            DialectType::BigQuery => {
                                // BigQuery: e'...' -> CAST(b'...' AS STRING)
                                // Use Raw for the b'...' part to avoid double-escaping
                                let raw_sql = format!("CAST(b'{}' AS STRING)", normalized);
                                Ok(Expression::Raw(crate::expressions::Raw { sql: raw_sql }))
                            }
                            _ => Ok(Expression::Literal(Box::new(Literal::EscapeString(
                                normalized,
                            )))),
                        }
                    } else {
                        Ok(e)
                    }
                } else {
                    Ok(e)
                }
            }

            Action::SnowflakeIntervalFormat => {
                // INTERVAL '2' HOUR -> INTERVAL '2 HOUR' for Snowflake
                if let Expression::Interval(mut iv) = e {
                    if let (Some(Expression::Literal(lit)), Some(ref unit_spec)) =
                        (&iv.this, &iv.unit)
                    {
                        if let Literal::String(ref val) = lit.as_ref() {
                            let unit_str = match unit_spec {
                                crate::expressions::IntervalUnitSpec::Simple { unit, .. } => {
                                    match unit {
                                        crate::expressions::IntervalUnit::Year => "YEAR",
                                        crate::expressions::IntervalUnit::Quarter => "QUARTER",
                                        crate::expressions::IntervalUnit::Month => "MONTH",
                                        crate::expressions::IntervalUnit::Week => "WEEK",
                                        crate::expressions::IntervalUnit::Day => "DAY",
                                        crate::expressions::IntervalUnit::Hour => "HOUR",
                                        crate::expressions::IntervalUnit::Minute => "MINUTE",
                                        crate::expressions::IntervalUnit::Second => "SECOND",
                                        crate::expressions::IntervalUnit::Millisecond => {
                                            "MILLISECOND"
                                        }
                                        crate::expressions::IntervalUnit::Microsecond => {
                                            "MICROSECOND"
                                        }
                                        crate::expressions::IntervalUnit::Nanosecond => {
                                            "NANOSECOND"
                                        }
                                    }
                                }
                                _ => "",
                            };
                            if !unit_str.is_empty() {
                                let combined = format!("{} {}", val, unit_str);
                                iv.this =
                                    Some(Expression::Literal(Box::new(Literal::String(combined))));
                                iv.unit = None;
                            }
                        }
                    }
                    Ok(Expression::Interval(iv))
                } else {
                    Ok(e)
                }
            }

            Action::MysqlNullsLastRewrite => {
                // DuckDB -> MySQL: Add CASE WHEN IS NULL THEN 1 ELSE 0 END to ORDER BY
                // to simulate NULLS LAST for ASC ordering
                if let Expression::WindowFunction(mut wf) = e {
                    let mut new_order_by = Vec::new();
                    for o in wf.over.order_by {
                        if !o.desc {
                            // ASC: DuckDB has NULLS LAST, MySQL has NULLS FIRST
                            // Add CASE WHEN expr IS NULL THEN 1 ELSE 0 END before expr
                            let case_expr = Expression::Case(Box::new(Case {
                                operand: None,
                                whens: vec![(
                                    Expression::IsNull(Box::new(crate::expressions::IsNull {
                                        this: o.this.clone(),
                                        not: false,
                                        postfix_form: false,
                                    })),
                                    Expression::Literal(Box::new(Literal::Number("1".to_string()))),
                                )],
                                else_: Some(Expression::Literal(Box::new(Literal::Number(
                                    "0".to_string(),
                                )))),
                                comments: Vec::new(),
                                inferred_type: None,
                            }));
                            new_order_by.push(crate::expressions::Ordered {
                                this: case_expr,
                                desc: false,
                                nulls_first: None,
                                explicit_asc: false,
                                with_fill: None,
                            });
                            let mut ordered = o;
                            ordered.nulls_first = None;
                            new_order_by.push(ordered);
                        } else {
                            // DESC: DuckDB has NULLS LAST, MySQL also has NULLS LAST (NULLs smallest in DESC)
                            // No change needed
                            let mut ordered = o;
                            ordered.nulls_first = None;
                            new_order_by.push(ordered);
                        }
                    }
                    wf.over.order_by = new_order_by;
                    Ok(Expression::WindowFunction(wf))
                } else {
                    Ok(e)
                }
            }

            Action::FilterToIff => {
                // FILTER(WHERE cond) -> rewrite aggregate: AGG(IFF(cond, val, NULL))
                if let Expression::Filter(f) = e {
                    let condition = *f.expression;
                    let agg = *f.this;
                    // Strip WHERE from condition
                    let cond = match condition {
                        Expression::Where(w) => w.this,
                        other => other,
                    };
                    // Extract the aggregate function and its argument
                    // We want AVG(IFF(condition, x, NULL))
                    match agg {
                        Expression::Function(mut func) => {
                            if !func.args.is_empty() {
                                let orig_arg = func.args[0].clone();
                                let iff_call = Expression::Function(Box::new(Function::new(
                                    "IFF".to_string(),
                                    vec![cond, orig_arg, Expression::Null(Null)],
                                )));
                                func.args[0] = iff_call;
                                Ok(Expression::Function(func))
                            } else {
                                Ok(Expression::Filter(Box::new(crate::expressions::Filter {
                                    this: Box::new(Expression::Function(func)),
                                    expression: Box::new(cond),
                                })))
                            }
                        }
                        Expression::Avg(mut avg) => {
                            let iff_call = Expression::Function(Box::new(Function::new(
                                "IFF".to_string(),
                                vec![cond, avg.this.clone(), Expression::Null(Null)],
                            )));
                            avg.this = iff_call;
                            Ok(Expression::Avg(avg))
                        }
                        Expression::Sum(mut s) => {
                            let iff_call = Expression::Function(Box::new(Function::new(
                                "IFF".to_string(),
                                vec![cond, s.this.clone(), Expression::Null(Null)],
                            )));
                            s.this = iff_call;
                            Ok(Expression::Sum(s))
                        }
                        Expression::Count(mut c) => {
                            if let Some(ref this_expr) = c.this {
                                let iff_call = Expression::Function(Box::new(Function::new(
                                    "IFF".to_string(),
                                    vec![cond, this_expr.clone(), Expression::Null(Null)],
                                )));
                                c.this = Some(iff_call);
                            }
                            Ok(Expression::Count(c))
                        }
                        other => {
                            // Fallback: keep as Filter
                            Ok(Expression::Filter(Box::new(crate::expressions::Filter {
                                this: Box::new(other),
                                expression: Box::new(cond),
                            })))
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::StrPositionExpand => {
                // StrPosition with position arg -> complex STRPOS expansion for Presto/DuckDB
                // For Presto: IF(STRPOS(SUBSTRING(str, pos), substr) = 0, 0, STRPOS(SUBSTRING(str, pos), substr) + pos - 1)
                // For DuckDB: CASE WHEN STRPOS(SUBSTRING(str, pos), substr) = 0 THEN 0 ELSE STRPOS(SUBSTRING(str, pos), substr) + pos - 1 END
                if let Expression::StrPosition(sp) = e {
                    let crate::expressions::StrPosition {
                        this,
                        substr,
                        position,
                        occurrence,
                    } = *sp;
                    let string = *this;
                    let substr_expr = match substr {
                        Some(s) => *s,
                        None => Expression::Null(Null),
                    };
                    let pos = match position {
                        Some(p) => *p,
                        None => Expression::number(1),
                    };

                    // SUBSTRING(string, pos)
                    let substring_call = Expression::Function(Box::new(Function::new(
                        "SUBSTRING".to_string(),
                        vec![string.clone(), pos.clone()],
                    )));
                    // STRPOS(SUBSTRING(string, pos), substr)
                    let strpos_call = Expression::Function(Box::new(Function::new(
                        "STRPOS".to_string(),
                        vec![substring_call, substr_expr.clone()],
                    )));
                    // STRPOS(...) + pos - 1
                    let pos_adjusted =
                        Expression::Sub(Box::new(crate::expressions::BinaryOp::new(
                            Expression::Add(Box::new(crate::expressions::BinaryOp::new(
                                strpos_call.clone(),
                                pos.clone(),
                            ))),
                            Expression::number(1),
                        )));
                    // STRPOS(...) = 0
                    let is_zero = Expression::Eq(Box::new(crate::expressions::BinaryOp::new(
                        strpos_call.clone(),
                        Expression::number(0),
                    )));

                    match target {
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // IF(STRPOS(SUBSTRING(str, pos), substr) = 0, 0, STRPOS(SUBSTRING(str, pos), substr) + pos - 1)
                            Ok(Expression::Function(Box::new(Function::new(
                                "IF".to_string(),
                                vec![is_zero, Expression::number(0), pos_adjusted],
                            ))))
                        }
                        DialectType::DuckDB => {
                            // CASE WHEN STRPOS(SUBSTRING(str, pos), substr) = 0 THEN 0 ELSE STRPOS(SUBSTRING(str, pos), substr) + pos - 1 END
                            Ok(Expression::Case(Box::new(Case {
                                operand: None,
                                whens: vec![(is_zero, Expression::number(0))],
                                else_: Some(pos_adjusted),
                                comments: Vec::new(),
                                inferred_type: None,
                            })))
                        }
                        _ => {
                            // Reconstruct StrPosition
                            Ok(Expression::StrPosition(Box::new(
                                crate::expressions::StrPosition {
                                    this: Box::new(string),
                                    substr: Some(Box::new(substr_expr)),
                                    position: Some(Box::new(pos)),
                                    occurrence,
                                },
                            )))
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::CurrentUserSparkParens => {
                // CURRENT_USER -> CURRENT_USER() for Spark
                if let Expression::CurrentUser(_) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "CURRENT_USER".to_string(),
                        vec![],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ConcatCoalesceWrap => {
                // CONCAT(a, b) function -> CONCAT(COALESCE(CAST(a AS VARCHAR), ''), ...) for Presto
                // CONCAT(a, b) function -> CONCAT(COALESCE(a, ''), ...) for ClickHouse
                if let Expression::Function(f) = e {
                    if f.name.eq_ignore_ascii_case("CONCAT") {
                        let new_args: Vec<Expression> = f
                            .args
                            .into_iter()
                            .map(|arg| {
                                let cast_arg = if matches!(
                                    target,
                                    DialectType::Presto | DialectType::Trino | DialectType::Athena
                                ) {
                                    Expression::Cast(Box::new(Cast {
                                        this: arg,
                                        to: DataType::VarChar {
                                            length: None,
                                            parenthesized_length: false,
                                        },
                                        trailing_comments: Vec::new(),
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }))
                                } else {
                                    arg
                                };
                                Expression::Function(Box::new(Function::new(
                                    "COALESCE".to_string(),
                                    vec![cast_arg, Expression::string("")],
                                )))
                            })
                            .collect();
                        Ok(Expression::Function(Box::new(Function::new(
                            "CONCAT".to_string(),
                            new_args,
                        ))))
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::PostgresSingleValueConcatToTsql => {
                let mut function = if let Expression::Function(f) = e {
                    *f
                } else {
                    unreachable!("action only triggered for Function expressions")
                };

                if function.name.eq_ignore_ascii_case("CONCAT") {
                    function.args.push(Expression::string(""));
                    Ok(Expression::Function(Box::new(function)))
                } else {
                    let separator = function.args.remove(0);
                    let value = function.args.remove(0);
                    function.name = "CONCAT".to_string();
                    function.args = vec![value, Expression::string("")];
                    let concat = Expression::Function(Box::new(function));

                    if matches!(separator, Expression::Literal(_)) {
                        Ok(concat)
                    } else {
                        Ok(Expression::Case(Box::new(Case {
                            operand: None,
                            whens: vec![(
                                Expression::IsNull(Box::new(IsNull {
                                    this: separator,
                                    not: false,
                                    postfix_form: false,
                                })),
                                Expression::Null(Null),
                            )],
                            else_: Some(concat),
                            comments: Vec::new(),
                            inferred_type: None,
                        })))
                    }
                }
            }

            Action::CbrtToPower => match e {
                Expression::Cbrt(f) => Ok(operators::build_tsql_cbrt_power(f.this)),
                Expression::Function(f) if f.args.len() == 1 => {
                    let mut args = f.args;
                    Ok(operators::build_tsql_cbrt_power(args.remove(0)))
                }
                _ => Ok(e),
            },

            Action::MinMaxToLeastGreatest => {
                // Multi-arg MIN(a,b,c) -> LEAST(a,b,c), MAX(a,b,c) -> GREATEST(a,b,c)
                if let Expression::Function(f) = e {
                    let new_name = if f.name.eq_ignore_ascii_case("MIN") {
                        "LEAST"
                    } else if f.name.eq_ignore_ascii_case("MAX") {
                        "GREATEST"
                    } else {
                        return Ok(Expression::Function(f));
                    };
                    Ok(Expression::Function(Box::new(Function::new(
                        new_name.to_string(),
                        f.args,
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::Nvl2Expand => {
                // NVL2(a, b[, c]) -> CASE WHEN NOT a IS NULL THEN b [ELSE c] END
                // But keep as NVL2 for dialects that support it natively
                let nvl2_native = matches!(
                    target,
                    DialectType::Oracle
                        | DialectType::Snowflake
                        | DialectType::Redshift
                        | DialectType::Teradata
                        | DialectType::Spark
                        | DialectType::Databricks
                );
                let (a, b, c) = if let Expression::Nvl2(nvl2) = e {
                    if nvl2_native {
                        return Ok(Expression::Nvl2(nvl2));
                    }
                    (nvl2.this, nvl2.true_value, Some(nvl2.false_value))
                } else if let Expression::Function(f) = e {
                    if nvl2_native {
                        return Ok(Expression::Function(Box::new(Function::new(
                            "NVL2".to_string(),
                            f.args,
                        ))));
                    }
                    if f.args.len() < 2 {
                        return Ok(Expression::Function(f));
                    }
                    let mut args = f.args;
                    let a = args.remove(0);
                    let b = args.remove(0);
                    let c = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        Option::None
                    };
                    (a, b, c)
                } else {
                    return Ok(e);
                };
                // Build: NOT (a IS NULL)
                let is_null = Expression::IsNull(Box::new(IsNull {
                    this: a,
                    not: false,
                    postfix_form: false,
                }));
                let not_null = Expression::Not(Box::new(crate::expressions::UnaryOp {
                    this: is_null,
                    inferred_type: None,
                }));
                Ok(Expression::Case(Box::new(Case {
                    operand: Option::None,
                    whens: vec![(not_null, b)],
                    else_: c,
                    comments: Vec::new(),
                    inferred_type: None,
                })))
            }

            Action::IfnullToCoalesce => {
                // IFNULL(a, b) -> COALESCE(a, b): clear original_name to output COALESCE
                if let Expression::Coalesce(mut cf) = e {
                    cf.original_name = Option::None;
                    Ok(Expression::Coalesce(cf))
                } else if let Expression::Function(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "COALESCE".to_string(),
                        f.args,
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::IsAsciiConvert => {
                // IS_ASCII(x) -> dialect-specific ASCII check
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    match target {
                        DialectType::MySQL | DialectType::SingleStore | DialectType::TiDB => {
                            // REGEXP_LIKE(x, '^[[:ascii:]]*$')
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_LIKE".to_string(),
                                vec![
                                    arg,
                                    Expression::Literal(Box::new(Literal::String(
                                        "^[[:ascii:]]*$".to_string(),
                                    ))),
                                ],
                            ))))
                        }
                        DialectType::PostgreSQL
                        | DialectType::Redshift
                        | DialectType::Materialize
                        | DialectType::RisingWave => {
                            // (x ~ '^[[:ascii:]]*$')
                            Ok(Expression::Paren(Box::new(Paren {
                                this: Expression::RegexpLike(Box::new(
                                    crate::expressions::RegexpFunc {
                                        this: arg,
                                        pattern: Expression::Literal(Box::new(Literal::String(
                                            "^[[:ascii:]]*$".to_string(),
                                        ))),
                                        flags: Option::None,
                                    },
                                )),
                                trailing_comments: Vec::new(),
                            })))
                        }
                        DialectType::SQLite => {
                            // (NOT x GLOB CAST(x'2a5b5e012d7f5d2a' AS TEXT))
                            let hex_lit = Expression::Literal(Box::new(Literal::HexString(
                                "2a5b5e012d7f5d2a".to_string(),
                            )));
                            let cast_expr = Expression::Cast(Box::new(Cast {
                                this: hex_lit,
                                to: DataType::Text,
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: Option::None,
                                default: Option::None,
                                inferred_type: None,
                            }));
                            let glob = Expression::Glob(Box::new(BinaryOp {
                                left: arg,
                                right: cast_expr,
                                left_comments: Vec::new(),
                                operator_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                                inferred_type: None,
                            }));
                            Ok(Expression::Paren(Box::new(Paren {
                                this: Expression::Not(Box::new(crate::expressions::UnaryOp {
                                    this: glob,
                                    inferred_type: None,
                                })),
                                trailing_comments: Vec::new(),
                            })))
                        }
                        DialectType::TSQL | DialectType::Fabric => {
                            // (PATINDEX(CONVERT(VARCHAR(MAX), 0x255b5e002d7f5d25) COLLATE Latin1_General_BIN, x) = 0)
                            let hex_lit = Expression::Literal(Box::new(Literal::HexNumber(
                                "255b5e002d7f5d25".to_string(),
                            )));
                            let convert_expr =
                                Expression::Convert(Box::new(crate::expressions::ConvertFunc {
                                    this: hex_lit,
                                    to: DataType::Text, // Text generates as VARCHAR(MAX) for TSQL
                                    style: None,
                                }));
                            let collated = Expression::Collation(Box::new(
                                crate::expressions::CollationExpr {
                                    this: convert_expr,
                                    collation: "Latin1_General_BIN".to_string(),
                                    quoted: false,
                                    double_quoted: false,
                                },
                            ));
                            let patindex = Expression::Function(Box::new(Function::new(
                                "PATINDEX".to_string(),
                                vec![collated, arg],
                            )));
                            let zero =
                                Expression::Literal(Box::new(Literal::Number("0".to_string())));
                            let eq_zero = Expression::Eq(Box::new(BinaryOp {
                                left: patindex,
                                right: zero,
                                left_comments: Vec::new(),
                                operator_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                                inferred_type: None,
                            }));
                            Ok(Expression::Paren(Box::new(Paren {
                                this: eq_zero,
                                trailing_comments: Vec::new(),
                            })))
                        }
                        DialectType::Oracle => {
                            // NVL(REGEXP_LIKE(x, '^[' || CHR(1) || '-' || CHR(127) || ']*$'), TRUE)
                            // Build the pattern: '^[' || CHR(1) || '-' || CHR(127) || ']*$'
                            let s1 =
                                Expression::Literal(Box::new(Literal::String("^[".to_string())));
                            let chr1 = Expression::Function(Box::new(Function::new(
                                "CHR".to_string(),
                                vec![Expression::Literal(Box::new(Literal::Number(
                                    "1".to_string(),
                                )))],
                            )));
                            let dash =
                                Expression::Literal(Box::new(Literal::String("-".to_string())));
                            let chr127 = Expression::Function(Box::new(Function::new(
                                "CHR".to_string(),
                                vec![Expression::Literal(Box::new(Literal::Number(
                                    "127".to_string(),
                                )))],
                            )));
                            let s2 =
                                Expression::Literal(Box::new(Literal::String("]*$".to_string())));
                            // Build: '^[' || CHR(1) || '-' || CHR(127) || ']*$'
                            let concat1 = Expression::DPipe(Box::new(crate::expressions::DPipe {
                                this: Box::new(s1),
                                expression: Box::new(chr1),
                                safe: None,
                            }));
                            let concat2 = Expression::DPipe(Box::new(crate::expressions::DPipe {
                                this: Box::new(concat1),
                                expression: Box::new(dash),
                                safe: None,
                            }));
                            let concat3 = Expression::DPipe(Box::new(crate::expressions::DPipe {
                                this: Box::new(concat2),
                                expression: Box::new(chr127),
                                safe: None,
                            }));
                            let concat4 = Expression::DPipe(Box::new(crate::expressions::DPipe {
                                this: Box::new(concat3),
                                expression: Box::new(s2),
                                safe: None,
                            }));
                            let regexp_like = Expression::Function(Box::new(Function::new(
                                "REGEXP_LIKE".to_string(),
                                vec![arg, concat4],
                            )));
                            // Use Column("TRUE") to output literal TRUE keyword (not boolean 1/0)
                            let true_expr =
                                Expression::Column(Box::new(crate::expressions::Column {
                                    name: Identifier {
                                        name: "TRUE".to_string(),
                                        quoted: false,
                                        trailing_comments: Vec::new(),
                                        span: None,
                                    },
                                    table: None,
                                    join_mark: false,
                                    trailing_comments: Vec::new(),
                                    span: None,
                                    inferred_type: None,
                                }));
                            let nvl = Expression::Function(Box::new(Function::new(
                                "NVL".to_string(),
                                vec![regexp_like, true_expr],
                            )));
                            Ok(nvl)
                        }
                        _ => Ok(Expression::Function(Box::new(Function::new(
                            "IS_ASCII".to_string(),
                            vec![arg],
                        )))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::StrPositionConvert => {
                // STR_POSITION(haystack, needle[, position[, occurrence]]) -> dialect-specific
                if let Expression::Function(f) = e {
                    if f.args.len() < 2 {
                        return Ok(Expression::Function(f));
                    }
                    let mut args = f.args;

                    let haystack = args.remove(0);
                    let needle = args.remove(0);
                    let position = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        Option::None
                    };
                    let occurrence = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        Option::None
                    };

                    // Helper to build: STRPOS/INSTR(SUBSTRING(haystack, pos), needle) expansion
                    // Returns: CASE/IF WHEN func(SUBSTRING(haystack, pos), needle[, occ]) = 0 THEN 0 ELSE ... + pos - 1 END
                    fn build_position_expansion(
                        haystack: Expression,
                        needle: Expression,
                        pos: Expression,
                        occurrence: Option<Expression>,
                        inner_func: &str,
                        wrapper: &str, // "CASE", "IF", "IIF"
                    ) -> Expression {
                        let substr = Expression::Function(Box::new(Function::new(
                            "SUBSTRING".to_string(),
                            vec![haystack, pos.clone()],
                        )));
                        let mut inner_args = vec![substr, needle];
                        if let Some(occ) = occurrence {
                            inner_args.push(occ);
                        }
                        let inner_call = Expression::Function(Box::new(Function::new(
                            inner_func.to_string(),
                            inner_args,
                        )));
                        let zero = Expression::Literal(Box::new(Literal::Number("0".to_string())));
                        let one = Expression::Literal(Box::new(Literal::Number("1".to_string())));
                        let eq_zero = Expression::Eq(Box::new(BinaryOp {
                            left: inner_call.clone(),
                            right: zero.clone(),
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                            inferred_type: None,
                        }));
                        let add_pos = Expression::Add(Box::new(BinaryOp {
                            left: inner_call,
                            right: pos,
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                            inferred_type: None,
                        }));
                        let sub_one = Expression::Sub(Box::new(BinaryOp {
                            left: add_pos,
                            right: one,
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                            inferred_type: None,
                        }));

                        match wrapper {
                            "CASE" => Expression::Case(Box::new(Case {
                                operand: Option::None,
                                whens: vec![(eq_zero, zero)],
                                else_: Some(sub_one),
                                comments: Vec::new(),
                                inferred_type: None,
                            })),
                            "IIF" => Expression::Function(Box::new(Function::new(
                                "IIF".to_string(),
                                vec![eq_zero, zero, sub_one],
                            ))),
                            _ => Expression::Function(Box::new(Function::new(
                                "IF".to_string(),
                                vec![eq_zero, zero, sub_one],
                            ))),
                        }
                    }

                    match target {
                        // STRPOS group: Athena, DuckDB, Presto, Trino, Drill
                        DialectType::Athena
                        | DialectType::DuckDB
                        | DialectType::Presto
                        | DialectType::Trino
                        | DialectType::Drill => {
                            if let Some(pos) = position {
                                let wrapper = if matches!(target, DialectType::DuckDB) {
                                    "CASE"
                                } else {
                                    "IF"
                                };
                                let result = build_position_expansion(
                                    haystack, needle, pos, occurrence, "STRPOS", wrapper,
                                );
                                if matches!(target, DialectType::Drill) {
                                    // Drill uses backtick-quoted `IF`
                                    if let Expression::Function(mut f) = result {
                                        f.name = "`IF`".to_string();
                                        Ok(Expression::Function(f))
                                    } else {
                                        Ok(result)
                                    }
                                } else {
                                    Ok(result)
                                }
                            } else {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "STRPOS".to_string(),
                                    vec![haystack, needle],
                                ))))
                            }
                        }
                        // SQLite: IIF wrapper
                        DialectType::SQLite => {
                            if let Some(pos) = position {
                                Ok(build_position_expansion(
                                    haystack, needle, pos, occurrence, "INSTR", "IIF",
                                ))
                            } else {
                                Ok(Expression::Function(Box::new(Function::new(
                                    "INSTR".to_string(),
                                    vec![haystack, needle],
                                ))))
                            }
                        }
                        // INSTR group: Teradata, BigQuery, Oracle
                        DialectType::Teradata | DialectType::BigQuery | DialectType::Oracle => {
                            let mut a = vec![haystack, needle];
                            if let Some(pos) = position {
                                a.push(pos);
                            }
                            if let Some(occ) = occurrence {
                                a.push(occ);
                            }
                            Ok(Expression::Function(Box::new(Function::new(
                                "INSTR".to_string(),
                                a,
                            ))))
                        }
                        // CHARINDEX group: Snowflake, TSQL
                        DialectType::Snowflake | DialectType::TSQL | DialectType::Fabric => {
                            let mut a = vec![needle, haystack];
                            if let Some(pos) = position {
                                a.push(pos);
                            }
                            Ok(Expression::Function(Box::new(Function::new(
                                "CHARINDEX".to_string(),
                                a,
                            ))))
                        }
                        // POSITION(needle IN haystack): PostgreSQL, Materialize, RisingWave, Redshift
                        DialectType::PostgreSQL
                        | DialectType::Materialize
                        | DialectType::RisingWave
                        | DialectType::Redshift => {
                            if let Some(pos) = position {
                                // Build: CASE WHEN POSITION(needle IN SUBSTRING(haystack FROM pos)) = 0 THEN 0
                                //   ELSE POSITION(...) + pos - 1 END
                                let substr = Expression::Substring(Box::new(
                                    crate::expressions::SubstringFunc {
                                        this: haystack,
                                        start: pos.clone(),
                                        length: Option::None,
                                        from_for_syntax: true,
                                    },
                                ));
                                let pos_in = Expression::StrPosition(Box::new(
                                    crate::expressions::StrPosition {
                                        this: Box::new(substr),
                                        substr: Some(Box::new(needle)),
                                        position: Option::None,
                                        occurrence: Option::None,
                                    },
                                ));
                                let zero =
                                    Expression::Literal(Box::new(Literal::Number("0".to_string())));
                                let one =
                                    Expression::Literal(Box::new(Literal::Number("1".to_string())));
                                let eq_zero = Expression::Eq(Box::new(BinaryOp {
                                    left: pos_in.clone(),
                                    right: zero.clone(),
                                    left_comments: Vec::new(),
                                    operator_comments: Vec::new(),
                                    trailing_comments: Vec::new(),
                                    inferred_type: None,
                                }));
                                let add_pos = Expression::Add(Box::new(BinaryOp {
                                    left: pos_in,
                                    right: pos,
                                    left_comments: Vec::new(),
                                    operator_comments: Vec::new(),
                                    trailing_comments: Vec::new(),
                                    inferred_type: None,
                                }));
                                let sub_one = Expression::Sub(Box::new(BinaryOp {
                                    left: add_pos,
                                    right: one,
                                    left_comments: Vec::new(),
                                    operator_comments: Vec::new(),
                                    trailing_comments: Vec::new(),
                                    inferred_type: None,
                                }));
                                Ok(Expression::Case(Box::new(Case {
                                    operand: Option::None,
                                    whens: vec![(eq_zero, zero)],
                                    else_: Some(sub_one),
                                    comments: Vec::new(),
                                    inferred_type: None,
                                })))
                            } else {
                                Ok(Expression::StrPosition(Box::new(
                                    crate::expressions::StrPosition {
                                        this: Box::new(haystack),
                                        substr: Some(Box::new(needle)),
                                        position: Option::None,
                                        occurrence: Option::None,
                                    },
                                )))
                            }
                        }
                        // LOCATE group: MySQL, Hive, Spark, Databricks, Doris
                        DialectType::MySQL
                        | DialectType::SingleStore
                        | DialectType::TiDB
                        | DialectType::Hive
                        | DialectType::Spark
                        | DialectType::Databricks
                        | DialectType::Doris
                        | DialectType::StarRocks => {
                            let mut a = vec![needle, haystack];
                            if let Some(pos) = position {
                                a.push(pos);
                            }
                            Ok(Expression::Function(Box::new(Function::new(
                                "LOCATE".to_string(),
                                a,
                            ))))
                        }
                        // ClickHouse: POSITION(haystack, needle[, position])
                        DialectType::ClickHouse => {
                            let mut a = vec![haystack, needle];
                            if let Some(pos) = position {
                                a.push(pos);
                            }
                            Ok(Expression::Function(Box::new(Function::new(
                                "POSITION".to_string(),
                                a,
                            ))))
                        }
                        _ => {
                            let mut a = vec![haystack, needle];
                            if let Some(pos) = position {
                                a.push(pos);
                            }
                            if let Some(occ) = occurrence {
                                a.push(occ);
                            }
                            Ok(Expression::Function(Box::new(Function::new(
                                "STR_POSITION".to_string(),
                                a,
                            ))))
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::DecodeSimplify => {
                // DECODE(x, search1, result1, ..., default) -> CASE WHEN ... THEN result1 ... [ELSE default] END
                // For literal search values: CASE WHEN x = search THEN result
                // For NULL search: CASE WHEN x IS NULL THEN result
                // For non-literal (column, expr): CASE WHEN x = search OR (x IS NULL AND search IS NULL) THEN result
                fn is_decode_literal(e: &Expression) -> bool {
                    matches!(
                        e,
                        Expression::Literal(_) | Expression::Boolean(_) | Expression::Neg(_)
                    )
                }

                let build_decode_case =
                    |this_expr: Expression,
                     pairs: Vec<(Expression, Expression)>,
                     default: Option<Expression>| {
                        let whens: Vec<(Expression, Expression)> = pairs
                            .into_iter()
                            .map(|(search, result)| {
                                if matches!(&search, Expression::Null(_)) {
                                    // NULL search -> IS NULL
                                    let condition = Expression::Is(Box::new(BinaryOp {
                                        left: this_expr.clone(),
                                        right: Expression::Null(crate::expressions::Null),
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    (condition, result)
                                } else if is_decode_literal(&search)
                                    || is_decode_literal(&this_expr)
                                {
                                    // At least one side is a literal -> simple equality (no NULL check needed)
                                    let eq = Expression::Eq(Box::new(BinaryOp {
                                        left: this_expr.clone(),
                                        right: search,
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    (eq, result)
                                } else {
                                    // Non-literal -> null-safe comparison
                                    let needs_paren = matches!(
                                        &search,
                                        Expression::Eq(_)
                                            | Expression::Neq(_)
                                            | Expression::Gt(_)
                                            | Expression::Gte(_)
                                            | Expression::Lt(_)
                                            | Expression::Lte(_)
                                    );
                                    let search_ref = if needs_paren {
                                        Expression::Paren(Box::new(crate::expressions::Paren {
                                            this: search.clone(),
                                            trailing_comments: Vec::new(),
                                        }))
                                    } else {
                                        search.clone()
                                    };
                                    // Build: x = search OR (x IS NULL AND search IS NULL)
                                    let eq = Expression::Eq(Box::new(BinaryOp {
                                        left: this_expr.clone(),
                                        right: search_ref,
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    let search_in_null = if needs_paren {
                                        Expression::Paren(Box::new(crate::expressions::Paren {
                                            this: search.clone(),
                                            trailing_comments: Vec::new(),
                                        }))
                                    } else {
                                        search.clone()
                                    };
                                    let x_is_null = Expression::Is(Box::new(BinaryOp {
                                        left: this_expr.clone(),
                                        right: Expression::Null(crate::expressions::Null),
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    let search_is_null = Expression::Is(Box::new(BinaryOp {
                                        left: search_in_null,
                                        right: Expression::Null(crate::expressions::Null),
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    let both_null = Expression::And(Box::new(BinaryOp {
                                        left: x_is_null,
                                        right: search_is_null,
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    let condition = Expression::Or(Box::new(BinaryOp {
                                        left: eq,
                                        right: Expression::Paren(Box::new(
                                            crate::expressions::Paren {
                                                this: both_null,
                                                trailing_comments: Vec::new(),
                                            },
                                        )),
                                        left_comments: Vec::new(),
                                        operator_comments: Vec::new(),
                                        trailing_comments: Vec::new(),
                                        inferred_type: None,
                                    }));
                                    (condition, result)
                                }
                            })
                            .collect();
                        Expression::Case(Box::new(Case {
                            operand: None,
                            whens,
                            else_: default,
                            comments: Vec::new(),
                            inferred_type: None,
                        }))
                    };

                if let Expression::Decode(decode) = e {
                    Ok(build_decode_case(
                        decode.this,
                        decode.search_results,
                        decode.default,
                    ))
                } else if let Expression::DecodeCase(dc) = e {
                    // DecodeCase has flat expressions: [x, s1, r1, s2, r2, ..., default?]
                    let mut exprs = dc.expressions;
                    if exprs.len() < 3 {
                        return Ok(Expression::DecodeCase(Box::new(
                            crate::expressions::DecodeCase { expressions: exprs },
                        )));
                    }
                    let this_expr = exprs.remove(0);
                    let mut pairs = Vec::new();
                    let mut default = None;
                    let mut i = 0;
                    while i + 1 < exprs.len() {
                        pairs.push((exprs[i].clone(), exprs[i + 1].clone()));
                        i += 2;
                    }
                    if i < exprs.len() {
                        // Odd remaining element is the default
                        default = Some(exprs[i].clone());
                    }
                    Ok(build_decode_case(this_expr, pairs, default))
                } else {
                    Ok(e)
                }
            }
        }
    })()?;

    Ok(RewriteOutcome::Rewritten(expression))
}

pub(super) fn normalize_bigquery_function(
    e: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<Expression> {
    use crate::expressions::{BinaryOp, Cast, DataType, Function, Identifier, Literal, Paren};

    let f = if let Expression::Function(f) = e {
        *f
    } else {
        return Ok(e);
    };
    let name = f.name.to_ascii_uppercase();
    let mut args = f.args;

    /// Helper to extract unit string from an identifier, column, or literal expression
    fn get_unit_str(expr: &Expression) -> String {
        match expr {
            Expression::Identifier(id) => id.name.to_ascii_uppercase(),
            Expression::Var(v) => v.this.to_ascii_uppercase(),
            Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
                let Literal::String(s) = lit.as_ref() else {
                    unreachable!()
                };
                s.to_ascii_uppercase()
            }
            Expression::Column(col) => col.name.name.to_ascii_uppercase(),
            // Handle WEEK(MONDAY), WEEK(SUNDAY) etc. which are parsed as Function("WEEK", [Column("MONDAY")])
            Expression::Function(f) => {
                let base = f.name.to_ascii_uppercase();
                if !f.args.is_empty() {
                    // e.g., WEEK(MONDAY) -> "WEEK(MONDAY)"
                    let inner = get_unit_str(&f.args[0]);
                    format!("{}({})", base, inner)
                } else {
                    base
                }
            }
            _ => "DAY".to_string(),
        }
    }

    /// Parse unit string to IntervalUnit
    fn parse_interval_unit(s: &str) -> crate::expressions::IntervalUnit {
        match s {
            "YEAR" => crate::expressions::IntervalUnit::Year,
            "QUARTER" => crate::expressions::IntervalUnit::Quarter,
            "MONTH" => crate::expressions::IntervalUnit::Month,
            "WEEK" | "ISOWEEK" => crate::expressions::IntervalUnit::Week,
            "DAY" => crate::expressions::IntervalUnit::Day,
            "HOUR" => crate::expressions::IntervalUnit::Hour,
            "MINUTE" => crate::expressions::IntervalUnit::Minute,
            "SECOND" => crate::expressions::IntervalUnit::Second,
            "MILLISECOND" => crate::expressions::IntervalUnit::Millisecond,
            "MICROSECOND" => crate::expressions::IntervalUnit::Microsecond,
            _ if s.starts_with("WEEK(") => crate::expressions::IntervalUnit::Week,
            _ => crate::expressions::IntervalUnit::Day,
        }
    }

    match name.as_str() {
        // TIMESTAMP_DIFF(date1, date2, unit) -> TIMESTAMPDIFF(unit, date2, date1)
        // (BigQuery: result = date1 - date2, Standard: result = end - start)
        "TIMESTAMP_DIFF" | "DATETIME_DIFF" | "TIME_DIFF" if args.len() == 3 => {
            let date1 = args.remove(0);
            let date2 = args.remove(0);
            let unit_expr = args.remove(0);
            let unit_str = get_unit_str(&unit_expr);

            if matches!(target, DialectType::BigQuery) {
                // BigQuery -> BigQuery: just uppercase the unit
                let unit = Expression::Identifier(Identifier::new(unit_str.clone()));
                return Ok(Expression::Function(Box::new(Function::new(
                    f.name,
                    vec![date1, date2, unit],
                ))));
            }

            // For Snowflake: use TimestampDiff expression so it generates TIMESTAMPDIFF
            // (Function("TIMESTAMPDIFF") would be converted to DATEDIFF by Snowflake's function normalization)
            if matches!(target, DialectType::Snowflake) {
                return Ok(Expression::TimestampDiff(Box::new(
                    crate::expressions::TimestampDiff {
                        this: Box::new(date2),
                        expression: Box::new(date1),
                        unit: Some(unit_str),
                    },
                )));
            }

            // For DuckDB: DATE_DIFF('UNIT', start, end) with proper CAST
            if matches!(target, DialectType::DuckDB) {
                let (cast_d1, cast_d2) = if name == "TIME_DIFF" {
                    // CAST to TIME
                    let cast_fn = |e: Expression| -> Expression {
                        match e {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                Expression::Cast(Box::new(Cast {
                                    this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                                    to: DataType::Custom {
                                        name: "TIME".to_string(),
                                    },
                                    trailing_comments: vec![],
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))
                            }
                            other => other,
                        }
                    };
                    (cast_fn(date1), cast_fn(date2))
                } else if name == "DATETIME_DIFF" {
                    // CAST to TIMESTAMP
                    (
                        temporal::ensure_cast_timestamp(date1),
                        temporal::ensure_cast_timestamp(date2),
                    )
                } else {
                    // TIMESTAMP_DIFF: CAST to TIMESTAMPTZ
                    (
                        temporal::ensure_cast_timestamptz(date1),
                        temporal::ensure_cast_timestamptz(date2),
                    )
                };
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::Literal(Box::new(Literal::String(unit_str))),
                        cast_d2,
                        cast_d1,
                    ],
                ))));
            }

            // Convert to standard TIMESTAMPDIFF(unit, start, end)
            let unit = Expression::Identifier(Identifier::new(unit_str));
            Ok(Expression::Function(Box::new(Function::new(
                "TIMESTAMPDIFF".to_string(),
                vec![unit, date2, date1],
            ))))
        }

        // DATEDIFF(unit, start, end) -> target-specific form
        // Used by: Redshift, Snowflake, TSQL, Databricks, Spark
        "DATEDIFF" if args.len() == 3 => {
            let arg0 = args.remove(0);
            let arg1 = args.remove(0);
            let arg2 = args.remove(0);
            let unit_str = get_unit_str(&arg0);

            // Redshift DATEDIFF(unit, start, end) order: result = end - start
            // Snowflake DATEDIFF(unit, start, end) order: result = end - start
            // TSQL DATEDIFF(unit, start, end) order: result = end - start

            if matches!(target, DialectType::Snowflake) {
                // Snowflake: DATEDIFF(UNIT, start, end) - uppercase unit
                let unit = Expression::Identifier(Identifier::new(unit_str));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    vec![unit, arg1, arg2],
                ))));
            }

            if matches!(target, DialectType::DuckDB) {
                // DuckDB: DATE_DIFF('UNIT', start, end) with CAST
                let cast_d1 = temporal::ensure_cast_timestamp(arg1);
                let cast_d2 = temporal::ensure_cast_timestamp(arg2);
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::Literal(Box::new(Literal::String(unit_str))),
                        cast_d1,
                        cast_d2,
                    ],
                ))));
            }

            if matches!(target, DialectType::BigQuery) {
                // BigQuery: DATE_DIFF(end_date, start_date, UNIT) - reversed args, CAST to DATETIME
                let cast_d1 = temporal::ensure_cast_datetime(arg1);
                let cast_d2 = temporal::ensure_cast_datetime(arg2);
                let unit = Expression::Identifier(Identifier::new(unit_str));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![cast_d2, cast_d1, unit],
                ))));
            }

            if matches!(target, DialectType::Spark | DialectType::Databricks) {
                // Spark/Databricks: DATEDIFF(UNIT, start, end) - uppercase unit
                let unit = Expression::Identifier(Identifier::new(unit_str));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    vec![unit, arg1, arg2],
                ))));
            }

            if matches!(target, DialectType::Hive) {
                // Hive: DATEDIFF(end, start) for DAY only, use MONTHS_BETWEEN for MONTH
                match unit_str.as_str() {
                    "MONTH" => {
                        return Ok(Expression::Function(Box::new(Function::new(
                            "CAST".to_string(),
                            vec![Expression::Function(Box::new(Function::new(
                                "MONTHS_BETWEEN".to_string(),
                                vec![arg2, arg1],
                            )))],
                        ))));
                    }
                    "WEEK" => {
                        return Ok(Expression::Cast(Box::new(Cast {
                            this: Expression::Div(Box::new(crate::expressions::BinaryOp::new(
                                Expression::Function(Box::new(Function::new(
                                    "DATEDIFF".to_string(),
                                    vec![arg2, arg1],
                                ))),
                                Expression::Literal(Box::new(Literal::Number("7".to_string()))),
                            ))),
                            to: DataType::Int {
                                length: None,
                                integer_spelling: false,
                            },
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        })));
                    }
                    _ => {
                        // Default: DATEDIFF(end, start) for DAY
                        return Ok(Expression::Function(Box::new(Function::new(
                            "DATEDIFF".to_string(),
                            vec![arg2, arg1],
                        ))));
                    }
                }
            }

            if matches!(
                target,
                DialectType::Presto | DialectType::Trino | DialectType::Athena
            ) {
                // Presto/Trino: DATE_DIFF('UNIT', start, end)
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::Literal(Box::new(Literal::String(unit_str))),
                        arg1,
                        arg2,
                    ],
                ))));
            }

            if matches!(target, DialectType::TSQL) {
                // TSQL: DATEDIFF(UNIT, start, CAST(end AS DATETIME2))
                let cast_d2 = temporal::ensure_cast_datetime2(arg2);
                let unit = Expression::Identifier(Identifier::new(unit_str));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    vec![unit, arg1, cast_d2],
                ))));
            }

            if matches!(target, DialectType::PostgreSQL) {
                // PostgreSQL doesn't have DATEDIFF - use date subtraction or EXTRACT
                // For now, use DATEDIFF (passthrough) with uppercased unit
                let unit = Expression::Identifier(Identifier::new(unit_str));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    vec![unit, arg1, arg2],
                ))));
            }

            // Default: DATEDIFF(UNIT, start, end) with uppercase unit
            let unit = Expression::Identifier(Identifier::new(unit_str));
            Ok(Expression::Function(Box::new(Function::new(
                "DATEDIFF".to_string(),
                vec![unit, arg1, arg2],
            ))))
        }

        // DATE_DIFF(date1, date2, unit) -> standard form
        "DATE_DIFF" if args.len() == 3 => {
            let date1 = args.remove(0);
            let date2 = args.remove(0);
            let unit_expr = args.remove(0);
            let unit_str = get_unit_str(&unit_expr);

            if matches!(target, DialectType::BigQuery) {
                // BigQuery -> BigQuery: just uppercase the unit, normalize WEEK(SUNDAY) -> WEEK
                let norm_unit = if unit_str == "WEEK(SUNDAY)" {
                    "WEEK".to_string()
                } else {
                    unit_str
                };
                let norm_d1 = temporal::date_literal_to_cast(date1);
                let norm_d2 = temporal::date_literal_to_cast(date2);
                let unit = Expression::Identifier(Identifier::new(norm_unit));
                return Ok(Expression::Function(Box::new(Function::new(
                    f.name,
                    vec![norm_d1, norm_d2, unit],
                ))));
            }

            if matches!(target, DialectType::MySQL) {
                // MySQL DATEDIFF only takes 2 args (date1, date2), returns day difference
                let norm_d1 = temporal::date_literal_to_cast(date1);
                let norm_d2 = temporal::date_literal_to_cast(date2);
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEDIFF".to_string(),
                    vec![norm_d1, norm_d2],
                ))));
            }

            if matches!(target, DialectType::StarRocks) {
                // StarRocks: DATE_DIFF('UNIT', date1, date2) - unit as string, args NOT swapped
                let norm_d1 = temporal::date_literal_to_cast(date1);
                let norm_d2 = temporal::date_literal_to_cast(date2);
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::Literal(Box::new(Literal::String(unit_str))),
                        norm_d1,
                        norm_d2,
                    ],
                ))));
            }

            if matches!(target, DialectType::DuckDB) {
                // DuckDB: DATE_DIFF('UNIT', date2, date1) with proper CAST for dates
                let norm_d1 = temporal::ensure_cast_date(date1);
                let norm_d2 = temporal::ensure_cast_date(date2);

                // Handle WEEK variants: WEEK(MONDAY)/WEEK(SUNDAY)/ISOWEEK/WEEK
                let is_week_variant =
                    unit_str == "WEEK" || unit_str.starts_with("WEEK(") || unit_str == "ISOWEEK";
                if is_week_variant {
                    // For DuckDB, WEEK-based diffs use DATE_TRUNC approach
                    // WEEK(MONDAY) / ISOWEEK: DATE_DIFF('WEEK', DATE_TRUNC('WEEK', d2), DATE_TRUNC('WEEK', d1))
                    // WEEK / WEEK(SUNDAY): DATE_DIFF('WEEK', DATE_TRUNC('WEEK', d2 + INTERVAL '1' DAY), DATE_TRUNC('WEEK', d1 + INTERVAL '1' DAY))
                    // WEEK(SATURDAY): DATE_DIFF('WEEK', DATE_TRUNC('WEEK', d2 + INTERVAL '-5' DAY), DATE_TRUNC('WEEK', d1 + INTERVAL '-5' DAY))
                    let day_offset = if unit_str == "WEEK(MONDAY)" || unit_str == "ISOWEEK" {
                        None // ISO weeks start on Monday, aligned with DATE_TRUNC('WEEK')
                    } else if unit_str == "WEEK" || unit_str == "WEEK(SUNDAY)" {
                        Some("1") // Shift Sunday to Monday alignment
                    } else if unit_str == "WEEK(SATURDAY)" {
                        Some("-5")
                    } else if unit_str == "WEEK(TUESDAY)" {
                        Some("-1")
                    } else if unit_str == "WEEK(WEDNESDAY)" {
                        Some("-2")
                    } else if unit_str == "WEEK(THURSDAY)" {
                        Some("-3")
                    } else if unit_str == "WEEK(FRIDAY)" {
                        Some("-4")
                    } else {
                        Some("1") // default to Sunday
                    };

                    let make_trunc = |date: Expression, offset: Option<&str>| -> Expression {
                        let shifted = if let Some(off) = offset {
                            let interval =
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(Expression::Literal(Box::new(Literal::String(
                                        off.to_string(),
                                    )))),
                                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                        unit: crate::expressions::IntervalUnit::Day,
                                        use_plural: false,
                                    }),
                                }));
                            Expression::Add(Box::new(crate::expressions::BinaryOp::new(
                                date, interval,
                            )))
                        } else {
                            date
                        };
                        Expression::Function(Box::new(Function::new(
                            "DATE_TRUNC".to_string(),
                            vec![
                                Expression::Literal(Box::new(Literal::String("WEEK".to_string()))),
                                shifted,
                            ],
                        )))
                    };

                    let trunc_d2 = make_trunc(norm_d2, day_offset);
                    let trunc_d1 = make_trunc(norm_d1, day_offset);
                    return Ok(Expression::Function(Box::new(Function::new(
                        "DATE_DIFF".to_string(),
                        vec![
                            Expression::Literal(Box::new(Literal::String("WEEK".to_string()))),
                            trunc_d2,
                            trunc_d1,
                        ],
                    ))));
                }

                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_DIFF".to_string(),
                    vec![
                        Expression::Literal(Box::new(Literal::String(unit_str))),
                        norm_d2,
                        norm_d1,
                    ],
                ))));
            }

            // Default: DATEDIFF(unit, date2, date1)
            let unit = Expression::Identifier(Identifier::new(unit_str));
            Ok(Expression::Function(Box::new(Function::new(
                "DATEDIFF".to_string(),
                vec![unit, date2, date1],
            ))))
        }

        // TIMESTAMP_ADD(ts, INTERVAL n UNIT) -> target-specific
        "TIMESTAMP_ADD" | "DATETIME_ADD" | "TIME_ADD" if args.len() == 2 => {
            let ts = args.remove(0);
            let interval_expr = args.remove(0);
            let (val, unit) = Dialect::extract_interval_parts(&interval_expr)
                .unwrap_or_else(|| (interval_expr.clone(), crate::expressions::IntervalUnit::Day));

            match target {
                DialectType::Snowflake => {
                    // TIMESTAMPADD(UNIT, val, CAST(ts AS TIMESTAMPTZ))
                    // Use TimestampAdd expression so Snowflake generates TIMESTAMPADD
                    // (Function("TIMESTAMPADD") would be converted to DATEADD by Snowflake's function normalization)
                    let unit_str = temporal::interval_unit_to_string(&unit);
                    let cast_ts = temporal::maybe_cast_ts_to_tz(ts, &name);
                    Ok(Expression::TimestampAdd(Box::new(
                        crate::expressions::TimestampAdd {
                            this: Box::new(val),
                            expression: Box::new(cast_ts),
                            unit: Some(unit_str.to_string()),
                        },
                    )))
                }
                DialectType::Spark | DialectType::Databricks => {
                    if name == "DATETIME_ADD" && matches!(target, DialectType::Spark) {
                        // Spark DATETIME_ADD: ts + INTERVAL val UNIT
                        let interval =
                            Expression::Interval(Box::new(crate::expressions::Interval {
                                this: Some(val),
                                unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                    unit,
                                    use_plural: false,
                                }),
                            }));
                        Ok(Expression::Add(Box::new(
                            crate::expressions::BinaryOp::new(ts, interval),
                        )))
                    } else if name == "DATETIME_ADD" && matches!(target, DialectType::Databricks) {
                        // Databricks DATETIME_ADD: TIMESTAMPADD(UNIT, val, ts)
                        let unit_str = temporal::interval_unit_to_string(&unit);
                        Ok(Expression::Function(Box::new(Function::new(
                            "TIMESTAMPADD".to_string(),
                            vec![Expression::Identifier(Identifier::new(unit_str)), val, ts],
                        ))))
                    } else {
                        // Presto-style: DATE_ADD('unit', val, CAST(ts AS TIMESTAMP))
                        let unit_str = temporal::interval_unit_to_string(&unit);
                        let cast_ts =
                            if name.starts_with("TIMESTAMP") || name.starts_with("DATETIME") {
                                temporal::maybe_cast_ts(ts)
                            } else {
                                ts
                            };
                        Ok(Expression::Function(Box::new(Function::new(
                            "DATE_ADD".to_string(),
                            vec![
                                Expression::Identifier(Identifier::new(unit_str)),
                                val,
                                cast_ts,
                            ],
                        ))))
                    }
                }
                DialectType::MySQL => {
                    // DATE_ADD(TIMESTAMP(ts), INTERVAL val UNIT) for MySQL
                    let mysql_ts = if name.starts_with("TIMESTAMP") {
                        // Check if already wrapped in TIMESTAMP() function (from cross-dialect normalization)
                        match &ts {
                            Expression::Function(ref inner_f)
                                if inner_f.name.eq_ignore_ascii_case("TIMESTAMP") =>
                            {
                                // Already wrapped, keep as-is
                                ts
                            }
                            _ => {
                                // Unwrap typed literals: TIMESTAMP '...' -> '...' for TIMESTAMP() wrapper
                                let unwrapped = match ts {
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::Timestamp(_)) =>
                                    {
                                        let Literal::Timestamp(s) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        Expression::Literal(Box::new(Literal::String(s.clone())))
                                    }
                                    other => other,
                                };
                                Expression::Function(Box::new(Function::new(
                                    "TIMESTAMP".to_string(),
                                    vec![unwrapped],
                                )))
                            }
                        }
                    } else {
                        ts
                    };
                    Ok(Expression::DateAdd(Box::new(
                        crate::expressions::DateAddFunc {
                            this: mysql_ts,
                            interval: val,
                            unit,
                        },
                    )))
                }
                _ => {
                    // DuckDB and others use DateAdd expression (DuckDB converts to + INTERVAL)
                    let cast_ts = if matches!(target, DialectType::DuckDB) {
                        if name == "DATETIME_ADD" {
                            temporal::ensure_cast_timestamp(ts)
                        } else if name.starts_with("TIMESTAMP") {
                            temporal::maybe_cast_ts_to_tz(ts, &name)
                        } else {
                            ts
                        }
                    } else {
                        ts
                    };
                    Ok(Expression::DateAdd(Box::new(
                        crate::expressions::DateAddFunc {
                            this: cast_ts,
                            interval: val,
                            unit,
                        },
                    )))
                }
            }
        }

        // TIMESTAMP_SUB(ts, INTERVAL n UNIT) -> target-specific
        "TIMESTAMP_SUB" | "DATETIME_SUB" | "TIME_SUB" if args.len() == 2 => {
            let ts = args.remove(0);
            let interval_expr = args.remove(0);
            let (val, unit) = Dialect::extract_interval_parts(&interval_expr)
                .unwrap_or_else(|| (interval_expr.clone(), crate::expressions::IntervalUnit::Day));

            match target {
                DialectType::Snowflake => {
                    // TIMESTAMPADD(UNIT, val * -1, CAST(ts AS TIMESTAMPTZ))
                    let unit_str = temporal::interval_unit_to_string(&unit);
                    let cast_ts = temporal::maybe_cast_ts_to_tz(ts, &name);
                    let neg_val = Expression::Mul(Box::new(crate::expressions::BinaryOp::new(
                        val,
                        Expression::Neg(Box::new(crate::expressions::UnaryOp {
                            this: Expression::number(1),
                            inferred_type: None,
                        })),
                    )));
                    Ok(Expression::TimestampAdd(Box::new(
                        crate::expressions::TimestampAdd {
                            this: Box::new(neg_val),
                            expression: Box::new(cast_ts),
                            unit: Some(unit_str.to_string()),
                        },
                    )))
                }
                DialectType::Spark | DialectType::Databricks => {
                    if (name == "DATETIME_SUB" && matches!(target, DialectType::Spark))
                        || (name == "TIMESTAMP_SUB" && matches!(target, DialectType::Spark))
                    {
                        // Spark: ts - INTERVAL val UNIT
                        let cast_ts = if name.starts_with("TIMESTAMP") {
                            temporal::maybe_cast_ts(ts)
                        } else {
                            ts
                        };
                        let interval =
                            Expression::Interval(Box::new(crate::expressions::Interval {
                                this: Some(val),
                                unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                    unit,
                                    use_plural: false,
                                }),
                            }));
                        Ok(Expression::Sub(Box::new(
                            crate::expressions::BinaryOp::new(cast_ts, interval),
                        )))
                    } else {
                        // Databricks: TIMESTAMPADD(UNIT, val * -1, ts)
                        let unit_str = temporal::interval_unit_to_string(&unit);
                        let neg_val = Expression::Mul(Box::new(crate::expressions::BinaryOp::new(
                            val,
                            Expression::Neg(Box::new(crate::expressions::UnaryOp {
                                this: Expression::number(1),
                                inferred_type: None,
                            })),
                        )));
                        Ok(Expression::Function(Box::new(Function::new(
                            "TIMESTAMPADD".to_string(),
                            vec![
                                Expression::Identifier(Identifier::new(unit_str)),
                                neg_val,
                                ts,
                            ],
                        ))))
                    }
                }
                DialectType::MySQL => {
                    let mysql_ts = if name.starts_with("TIMESTAMP") {
                        // Check if already wrapped in TIMESTAMP() function (from cross-dialect normalization)
                        match &ts {
                            Expression::Function(ref inner_f)
                                if inner_f.name.eq_ignore_ascii_case("TIMESTAMP") =>
                            {
                                // Already wrapped, keep as-is
                                ts
                            }
                            _ => {
                                let unwrapped = match ts {
                                    Expression::Literal(lit)
                                        if matches!(lit.as_ref(), Literal::Timestamp(_)) =>
                                    {
                                        let Literal::Timestamp(s) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        Expression::Literal(Box::new(Literal::String(s.clone())))
                                    }
                                    other => other,
                                };
                                Expression::Function(Box::new(Function::new(
                                    "TIMESTAMP".to_string(),
                                    vec![unwrapped],
                                )))
                            }
                        }
                    } else {
                        ts
                    };
                    Ok(Expression::DateSub(Box::new(
                        crate::expressions::DateAddFunc {
                            this: mysql_ts,
                            interval: val,
                            unit,
                        },
                    )))
                }
                _ => {
                    let cast_ts = if matches!(target, DialectType::DuckDB) {
                        if name == "DATETIME_SUB" {
                            temporal::ensure_cast_timestamp(ts)
                        } else if name.starts_with("TIMESTAMP") {
                            temporal::maybe_cast_ts_to_tz(ts, &name)
                        } else {
                            ts
                        }
                    } else {
                        ts
                    };
                    Ok(Expression::DateSub(Box::new(
                        crate::expressions::DateAddFunc {
                            this: cast_ts,
                            interval: val,
                            unit,
                        },
                    )))
                }
            }
        }

        // DATE_SUB(date, INTERVAL n UNIT) -> target-specific
        "DATE_SUB" if args.len() == 2 => {
            let date = args.remove(0);
            let interval_expr = args.remove(0);
            let (val, unit) = Dialect::extract_interval_parts(&interval_expr)
                .unwrap_or_else(|| (interval_expr.clone(), crate::expressions::IntervalUnit::Day));

            match target {
                DialectType::Databricks | DialectType::Spark => {
                    // Databricks/Spark: DATE_ADD(date, -val)
                    // Use DateAdd expression with negative val so it generates correctly
                    // The generator will output DATE_ADD(date, INTERVAL -val DAY)
                    // Then Databricks transform converts 2-arg DATE_ADD(date, interval) to DATEADD(DAY, interval, date)
                    // Instead, we directly output as a simple negated DateSub
                    Ok(Expression::DateSub(Box::new(
                        crate::expressions::DateAddFunc {
                            this: date,
                            interval: val,
                            unit,
                        },
                    )))
                }
                DialectType::DuckDB => {
                    // DuckDB: CAST(date AS DATE) - INTERVAL 'val' UNIT
                    let cast_date = temporal::ensure_cast_date(date);
                    let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(val),
                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                            unit,
                            use_plural: false,
                        }),
                    }));
                    Ok(Expression::Sub(Box::new(
                        crate::expressions::BinaryOp::new(cast_date, interval),
                    )))
                }
                DialectType::Snowflake => {
                    // Snowflake: Let Snowflake's own DateSub -> DATEADD(UNIT, val * -1, date) handler work
                    // Just ensure the date is cast properly
                    let cast_date = temporal::ensure_cast_date(date);
                    Ok(Expression::DateSub(Box::new(
                        crate::expressions::DateAddFunc {
                            this: cast_date,
                            interval: val,
                            unit,
                        },
                    )))
                }
                DialectType::PostgreSQL => {
                    // PostgreSQL: date - INTERVAL 'val UNIT'
                    let unit_str = temporal::interval_unit_to_string(&unit);
                    let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(Expression::Literal(Box::new(Literal::String(format!(
                            "{} {}",
                            expr_to_string(&val),
                            unit_str
                        ))))),
                        unit: None,
                    }));
                    Ok(Expression::Sub(Box::new(
                        crate::expressions::BinaryOp::new(date, interval),
                    )))
                }
                _ => Ok(Expression::DateSub(Box::new(
                    crate::expressions::DateAddFunc {
                        this: date,
                        interval: val,
                        unit,
                    },
                ))),
            }
        }

        // DATEADD(unit, val, date) -> target-specific form
        // Used by: Redshift, Snowflake, TSQL, ClickHouse
        "DATEADD" if args.len() == 3 => {
            let arg0 = args.remove(0);
            let arg1 = args.remove(0);
            let arg2 = args.remove(0);
            let unit_str = get_unit_str(&arg0);

            if matches!(target, DialectType::Snowflake | DialectType::TSQL) {
                // Keep DATEADD(UNIT, val, date) with uppercased unit
                let unit = Expression::Identifier(Identifier::new(unit_str));
                // Only CAST to DATETIME2 for TSQL target when source is NOT Spark/Databricks family
                let date = if matches!(target, DialectType::TSQL)
                    && !matches!(
                        source,
                        DialectType::Spark | DialectType::Databricks | DialectType::Hive
                    ) {
                    temporal::ensure_cast_datetime2(arg2)
                } else {
                    arg2
                };
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![unit, arg1, date],
                ))));
            }

            if matches!(target, DialectType::DuckDB) {
                // DuckDB: date + INTERVAL 'val' UNIT
                let iu = parse_interval_unit(&unit_str);
                let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                    this: Some(arg1),
                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                        unit: iu,
                        use_plural: false,
                    }),
                }));
                let cast_date = temporal::ensure_cast_timestamp(arg2);
                return Ok(Expression::Add(Box::new(
                    crate::expressions::BinaryOp::new(cast_date, interval),
                )));
            }

            if matches!(target, DialectType::BigQuery) {
                // BigQuery: DATE_ADD(date, INTERVAL val UNIT) or TIMESTAMP_ADD(ts, INTERVAL val UNIT)
                let iu = parse_interval_unit(&unit_str);
                let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                    this: Some(arg1),
                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                        unit: iu,
                        use_plural: false,
                    }),
                }));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![arg2, interval],
                ))));
            }

            if matches!(target, DialectType::Databricks) {
                // Databricks: keep DATEADD(UNIT, val, date) format
                let unit = Expression::Identifier(Identifier::new(unit_str));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![unit, arg1, arg2],
                ))));
            }

            if matches!(target, DialectType::Spark) {
                // Spark: convert month-based units to ADD_MONTHS, rest to DATE_ADD
                fn multiply_expr_dateadd(expr: Expression, factor: i64) -> Expression {
                    if let Expression::Literal(lit) = &expr {
                        if let crate::expressions::Literal::Number(n) = lit.as_ref() {
                            if let Ok(val) = n.parse::<i64>() {
                                return Expression::Literal(Box::new(
                                    crate::expressions::Literal::Number((val * factor).to_string()),
                                ));
                            }
                        }
                    }
                    Expression::Mul(Box::new(crate::expressions::BinaryOp::new(
                        expr,
                        Expression::Literal(Box::new(crate::expressions::Literal::Number(
                            factor.to_string(),
                        ))),
                    )))
                }
                match unit_str.as_str() {
                    "YEAR" => {
                        let months = multiply_expr_dateadd(arg1, 12);
                        return Ok(Expression::Function(Box::new(Function::new(
                            "ADD_MONTHS".to_string(),
                            vec![arg2, months],
                        ))));
                    }
                    "QUARTER" => {
                        let months = multiply_expr_dateadd(arg1, 3);
                        return Ok(Expression::Function(Box::new(Function::new(
                            "ADD_MONTHS".to_string(),
                            vec![arg2, months],
                        ))));
                    }
                    "MONTH" => {
                        return Ok(Expression::Function(Box::new(Function::new(
                            "ADD_MONTHS".to_string(),
                            vec![arg2, arg1],
                        ))));
                    }
                    "WEEK" => {
                        let days = multiply_expr_dateadd(arg1, 7);
                        return Ok(Expression::Function(Box::new(Function::new(
                            "DATE_ADD".to_string(),
                            vec![arg2, days],
                        ))));
                    }
                    "DAY" => {
                        return Ok(Expression::Function(Box::new(Function::new(
                            "DATE_ADD".to_string(),
                            vec![arg2, arg1],
                        ))));
                    }
                    _ => {
                        let unit = Expression::Identifier(Identifier::new(unit_str));
                        return Ok(Expression::Function(Box::new(Function::new(
                            "DATE_ADD".to_string(),
                            vec![unit, arg1, arg2],
                        ))));
                    }
                }
            }

            if matches!(target, DialectType::Hive) {
                // Hive: DATE_ADD(date, val) for DAY, or date + INTERVAL for others
                match unit_str.as_str() {
                    "DAY" => {
                        return Ok(Expression::Function(Box::new(Function::new(
                            "DATE_ADD".to_string(),
                            vec![arg2, arg1],
                        ))));
                    }
                    "MONTH" => {
                        return Ok(Expression::Function(Box::new(Function::new(
                            "ADD_MONTHS".to_string(),
                            vec![arg2, arg1],
                        ))));
                    }
                    _ => {
                        let iu = parse_interval_unit(&unit_str);
                        let interval =
                            Expression::Interval(Box::new(crate::expressions::Interval {
                                this: Some(arg1),
                                unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                    unit: iu,
                                    use_plural: false,
                                }),
                            }));
                        return Ok(Expression::Add(Box::new(
                            crate::expressions::BinaryOp::new(arg2, interval),
                        )));
                    }
                }
            }

            if matches!(target, DialectType::PostgreSQL) {
                // PostgreSQL: date + INTERVAL 'val UNIT'
                let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                    this: Some(Expression::Literal(Box::new(Literal::String(format!(
                        "{} {}",
                        expr_to_string(&arg1),
                        unit_str
                    ))))),
                    unit: None,
                }));
                return Ok(Expression::Add(Box::new(
                    crate::expressions::BinaryOp::new(arg2, interval),
                )));
            }

            if matches!(
                target,
                DialectType::Presto | DialectType::Trino | DialectType::Athena
            ) {
                // Presto/Trino: DATE_ADD('UNIT', val, date)
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![
                        Expression::Literal(Box::new(Literal::String(unit_str))),
                        arg1,
                        arg2,
                    ],
                ))));
            }

            if matches!(target, DialectType::ClickHouse) {
                // ClickHouse: DATE_ADD(UNIT, val, date)
                let unit = Expression::Identifier(Identifier::new(unit_str));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![unit, arg1, arg2],
                ))));
            }

            // Default: keep DATEADD with uppercased unit
            let unit = Expression::Identifier(Identifier::new(unit_str));
            Ok(Expression::Function(Box::new(Function::new(
                "DATEADD".to_string(),
                vec![unit, arg1, arg2],
            ))))
        }

        // DATE_ADD(unit, val, date) - 3 arg form from ClickHouse/Presto
        "DATE_ADD" if args.len() == 3 => {
            let arg0 = args.remove(0);
            let arg1 = args.remove(0);
            let arg2 = args.remove(0);
            let unit_str = get_unit_str(&arg0);

            if matches!(
                target,
                DialectType::Presto | DialectType::Trino | DialectType::Athena
            ) {
                // Presto/Trino: DATE_ADD('UNIT', val, date)
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![
                        Expression::Literal(Box::new(Literal::String(unit_str))),
                        arg1,
                        arg2,
                    ],
                ))));
            }

            if matches!(
                target,
                DialectType::Snowflake | DialectType::TSQL | DialectType::Redshift
            ) {
                // DATEADD(UNIT, val, date)
                let unit = Expression::Identifier(Identifier::new(unit_str));
                let date = if matches!(target, DialectType::TSQL) {
                    temporal::ensure_cast_datetime2(arg2)
                } else {
                    arg2
                };
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![unit, arg1, date],
                ))));
            }

            if matches!(target, DialectType::DuckDB) {
                // DuckDB: date + INTERVAL val UNIT
                let iu = parse_interval_unit(&unit_str);
                let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                    this: Some(arg1),
                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                        unit: iu,
                        use_plural: false,
                    }),
                }));
                return Ok(Expression::Add(Box::new(
                    crate::expressions::BinaryOp::new(arg2, interval),
                )));
            }

            if matches!(target, DialectType::Spark | DialectType::Databricks) {
                // Spark: DATE_ADD(UNIT, val, date) with uppercased unit
                let unit = Expression::Identifier(Identifier::new(unit_str));
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![unit, arg1, arg2],
                ))));
            }

            // Default: DATE_ADD(UNIT, val, date)
            let unit = Expression::Identifier(Identifier::new(unit_str));
            Ok(Expression::Function(Box::new(Function::new(
                "DATE_ADD".to_string(),
                vec![unit, arg1, arg2],
            ))))
        }

        // DATE_ADD(date, INTERVAL val UNIT) - 2 arg BigQuery form
        "DATE_ADD" if args.len() == 2 => {
            let date = args.remove(0);
            let interval_expr = args.remove(0);
            let (val, unit) = Dialect::extract_interval_parts(&interval_expr)
                .unwrap_or_else(|| (interval_expr.clone(), crate::expressions::IntervalUnit::Day));
            let unit_str = temporal::interval_unit_to_string(&unit);

            match target {
                DialectType::DuckDB => {
                    // DuckDB: CAST(date AS DATE) + INTERVAL 'val' UNIT
                    let cast_date = temporal::ensure_cast_date(date);
                    let quoted_val = temporal::quote_interval_val(&val);
                    let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(quoted_val),
                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                            unit,
                            use_plural: false,
                        }),
                    }));
                    Ok(Expression::Add(Box::new(
                        crate::expressions::BinaryOp::new(cast_date, interval),
                    )))
                }
                DialectType::PostgreSQL => {
                    // PostgreSQL: date + INTERVAL 'val UNIT'
                    let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(Expression::Literal(Box::new(Literal::String(format!(
                            "{} {}",
                            expr_to_string(&val),
                            unit_str
                        ))))),
                        unit: None,
                    }));
                    Ok(Expression::Add(Box::new(
                        crate::expressions::BinaryOp::new(date, interval),
                    )))
                }
                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                    // Presto: DATE_ADD('UNIT', CAST('val' AS BIGINT), date)
                    let val_str = expr_to_string(&val);
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_ADD".to_string(),
                        vec![
                            Expression::Literal(Box::new(Literal::String(unit_str.to_string()))),
                            Expression::Cast(Box::new(Cast {
                                this: Expression::Literal(Box::new(Literal::String(val_str))),
                                to: DataType::BigInt { length: None },
                                trailing_comments: vec![],
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })),
                            date,
                        ],
                    ))))
                }
                DialectType::Spark | DialectType::Hive => {
                    // Spark/Hive: DATE_ADD(date, val) for DAY
                    match unit_str {
                        "DAY" => Ok(Expression::Function(Box::new(Function::new(
                            "DATE_ADD".to_string(),
                            vec![date, val],
                        )))),
                        "MONTH" => Ok(Expression::Function(Box::new(Function::new(
                            "ADD_MONTHS".to_string(),
                            vec![date, val],
                        )))),
                        _ => {
                            let iu = parse_interval_unit(&unit_str);
                            let interval =
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(val),
                                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                        unit: iu,
                                        use_plural: false,
                                    }),
                                }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_ADD".to_string(),
                                vec![date, interval],
                            ))))
                        }
                    }
                }
                DialectType::Snowflake => {
                    // Snowflake: DATEADD(UNIT, 'val', CAST(date AS DATE))
                    let cast_date = temporal::ensure_cast_date(date);
                    let val_str = expr_to_string(&val);
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATEADD".to_string(),
                        vec![
                            Expression::Identifier(Identifier::new(unit_str)),
                            Expression::Literal(Box::new(Literal::String(val_str))),
                            cast_date,
                        ],
                    ))))
                }
                DialectType::TSQL | DialectType::Fabric => {
                    let cast_date = temporal::ensure_cast_datetime2(date);
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATEADD".to_string(),
                        vec![
                            Expression::Identifier(Identifier::new(unit_str)),
                            val,
                            cast_date,
                        ],
                    ))))
                }
                DialectType::Redshift => Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![Expression::Identifier(Identifier::new(unit_str)), val, date],
                )))),
                DialectType::MySQL => {
                    // MySQL: DATE_ADD(date, INTERVAL 'val' UNIT)
                    let quoted_val = temporal::quote_interval_val(&val);
                    let iu = parse_interval_unit(&unit_str);
                    let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(quoted_val),
                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                            unit: iu,
                            use_plural: false,
                        }),
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_ADD".to_string(),
                        vec![date, interval],
                    ))))
                }
                DialectType::BigQuery => {
                    // BigQuery: DATE_ADD(date, INTERVAL 'val' UNIT)
                    let quoted_val = temporal::quote_interval_val(&val);
                    let iu = parse_interval_unit(&unit_str);
                    let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(quoted_val),
                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                            unit: iu,
                            use_plural: false,
                        }),
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_ADD".to_string(),
                        vec![date, interval],
                    ))))
                }
                DialectType::Databricks => Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![Expression::Identifier(Identifier::new(unit_str)), val, date],
                )))),
                _ => {
                    // Default: keep as DATE_ADD with decomposed interval
                    Ok(Expression::DateAdd(Box::new(
                        crate::expressions::DateAddFunc {
                            this: date,
                            interval: val,
                            unit,
                        },
                    )))
                }
            }
        }

        // ADD_MONTHS(date, val) -> target-specific form
        "ADD_MONTHS" if args.len() == 2 => {
            let date = args.remove(0);
            let val = args.remove(0);

            if matches!(target, DialectType::TSQL) {
                // TSQL: DATEADD(MONTH, val, CAST(date AS DATETIME2))
                let cast_date = temporal::ensure_cast_datetime2(date);
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![
                        Expression::Identifier(Identifier::new("MONTH")),
                        val,
                        cast_date,
                    ],
                ))));
            }

            if matches!(target, DialectType::DuckDB) {
                // DuckDB: date + INTERVAL val MONTH
                let interval = Expression::Interval(Box::new(crate::expressions::Interval {
                    this: Some(val),
                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                        unit: crate::expressions::IntervalUnit::Month,
                        use_plural: false,
                    }),
                }));
                return Ok(Expression::Add(Box::new(
                    crate::expressions::BinaryOp::new(date, interval),
                )));
            }

            if matches!(target, DialectType::Snowflake) {
                // Snowflake: keep ADD_MONTHS when source is also Snowflake, else DATEADD
                if matches!(source, DialectType::Snowflake) {
                    return Ok(Expression::Function(Box::new(Function::new(
                        "ADD_MONTHS".to_string(),
                        vec![date, val],
                    ))));
                }
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATEADD".to_string(),
                    vec![Expression::Identifier(Identifier::new("MONTH")), val, date],
                ))));
            }

            if matches!(target, DialectType::Spark | DialectType::Databricks) {
                // Spark: ADD_MONTHS(date, val) - keep as is
                return Ok(Expression::Function(Box::new(Function::new(
                    "ADD_MONTHS".to_string(),
                    vec![date, val],
                ))));
            }

            if matches!(target, DialectType::Hive) {
                return Ok(Expression::Function(Box::new(Function::new(
                    "ADD_MONTHS".to_string(),
                    vec![date, val],
                ))));
            }

            if matches!(
                target,
                DialectType::Presto | DialectType::Trino | DialectType::Athena
            ) {
                // Presto: DATE_ADD('MONTH', val, date)
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATE_ADD".to_string(),
                    vec![
                        Expression::Literal(Box::new(Literal::String("MONTH".to_string()))),
                        val,
                        date,
                    ],
                ))));
            }

            // Default: keep ADD_MONTHS
            Ok(Expression::Function(Box::new(Function::new(
                "ADD_MONTHS".to_string(),
                vec![date, val],
            ))))
        }

        // SAFE_DIVIDE(x, y) -> target-specific form directly
        "SAFE_DIVIDE" if args.len() == 2 => {
            let x = args.remove(0);
            let y = args.remove(0);
            // Wrap x and y in parens if they're complex expressions
            let y_ref = match &y {
                Expression::Column(_) | Expression::Literal(_) | Expression::Identifier(_) => {
                    y.clone()
                }
                _ => Expression::Paren(Box::new(Paren {
                    this: y.clone(),
                    trailing_comments: vec![],
                })),
            };
            let x_ref = match &x {
                Expression::Column(_) | Expression::Literal(_) | Expression::Identifier(_) => {
                    x.clone()
                }
                _ => Expression::Paren(Box::new(Paren {
                    this: x.clone(),
                    trailing_comments: vec![],
                })),
            };
            let condition = Expression::Neq(Box::new(crate::expressions::BinaryOp::new(
                y_ref.clone(),
                Expression::number(0),
            )));
            let div_expr = Expression::Div(Box::new(crate::expressions::BinaryOp::new(
                x_ref.clone(),
                y_ref.clone(),
            )));

            match target {
                DialectType::Spark | DialectType::Databricks => Ok(Expression::Function(Box::new(
                    Function::new("TRY_DIVIDE".to_string(), vec![x, y]),
                ))),
                DialectType::DuckDB | DialectType::PostgreSQL => {
                    // CASE WHEN y <> 0 THEN x / y ELSE NULL END
                    let result_div = if matches!(target, DialectType::PostgreSQL) {
                        let cast_x = Expression::Cast(Box::new(Cast {
                            this: x_ref,
                            to: DataType::Custom {
                                name: "DOUBLE PRECISION".to_string(),
                            },
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        }));
                        Expression::Div(Box::new(crate::expressions::BinaryOp::new(cast_x, y_ref)))
                    } else {
                        div_expr
                    };
                    Ok(Expression::Case(Box::new(crate::expressions::Case {
                        operand: None,
                        whens: vec![(condition, result_div)],
                        else_: Some(Expression::Null(crate::expressions::Null)),
                        comments: Vec::new(),
                        inferred_type: None,
                    })))
                }
                DialectType::Snowflake => {
                    // IFF(y <> 0, x / y, NULL)
                    Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                        condition,
                        true_value: div_expr,
                        false_value: Some(Expression::Null(crate::expressions::Null)),
                        original_name: Some("IFF".to_string()),
                        inferred_type: None,
                    })))
                }
                DialectType::Presto | DialectType::Trino => {
                    // IF(y <> 0, CAST(x AS DOUBLE) / y, NULL)
                    let cast_x = Expression::Cast(Box::new(Cast {
                        this: x_ref,
                        to: DataType::Double {
                            precision: None,
                            scale: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let cast_div =
                        Expression::Div(Box::new(crate::expressions::BinaryOp::new(cast_x, y_ref)));
                    Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                        condition,
                        true_value: cast_div,
                        false_value: Some(Expression::Null(crate::expressions::Null)),
                        original_name: None,
                        inferred_type: None,
                    })))
                }
                _ => {
                    // IF(y <> 0, x / y, NULL)
                    Ok(Expression::IfFunc(Box::new(crate::expressions::IfFunc {
                        condition,
                        true_value: div_expr,
                        false_value: Some(Expression::Null(crate::expressions::Null)),
                        original_name: None,
                        inferred_type: None,
                    })))
                }
            }
        }

        // GENERATE_UUID() -> UUID() with CAST to string
        "GENERATE_UUID" => {
            let uuid_expr = Expression::Uuid(Box::new(crate::expressions::Uuid {
                this: None,
                name: None,
                is_string: None,
            }));
            // Most targets need CAST(UUID() AS TEXT/VARCHAR/STRING)
            let cast_type = match target {
                DialectType::DuckDB => Some(DataType::Text),
                DialectType::Presto | DialectType::Trino => Some(DataType::VarChar {
                    length: None,
                    parenthesized_length: false,
                }),
                DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                    Some(DataType::String { length: None })
                }
                _ => None,
            };
            if let Some(dt) = cast_type {
                Ok(Expression::Cast(Box::new(Cast {
                    this: uuid_expr,
                    to: dt,
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })))
            } else {
                Ok(uuid_expr)
            }
        }

        // COUNTIF(x) -> CountIf expression
        "COUNTIF" if args.len() == 1 => {
            let arg = args.remove(0);
            Ok(Expression::CountIf(Box::new(crate::expressions::AggFunc {
                this: arg,
                distinct: false,
                filter: None,
                order_by: vec![],
                name: None,
                ignore_nulls: None,
                having_max: None,
                limit: None,
                inferred_type: None,
            })))
        }

        // EDIT_DISTANCE(col1, col2, ...) -> Levenshtein expression
        "EDIT_DISTANCE" => {
            // Strip named arguments (max_distance => N) and pass as positional
            let mut positional_args: Vec<Expression> = vec![];
            for arg in args {
                match arg {
                    Expression::NamedArgument(na) => {
                        positional_args.push(na.value);
                    }
                    other => positional_args.push(other),
                }
            }
            if positional_args.len() >= 2 {
                let col1 = positional_args.remove(0);
                let col2 = positional_args.remove(0);
                let levenshtein = crate::expressions::BinaryFunc {
                    this: col1,
                    expression: col2,
                    original_name: None,
                    inferred_type: None,
                };
                // Pass extra args through a function wrapper with all args
                if !positional_args.is_empty() {
                    let max_dist = positional_args.remove(0);
                    // DuckDB: CASE WHEN LEVENSHTEIN(a, b) IS NULL OR max IS NULL THEN NULL ELSE LEAST(LEVENSHTEIN(a, b), max) END
                    if matches!(target, DialectType::DuckDB) {
                        let lev = Expression::Function(Box::new(Function::new(
                            "LEVENSHTEIN".to_string(),
                            vec![levenshtein.this, levenshtein.expression],
                        )));
                        let lev_is_null =
                            Expression::IsNull(Box::new(crate::expressions::IsNull {
                                this: lev.clone(),
                                not: false,
                                postfix_form: false,
                            }));
                        let max_is_null =
                            Expression::IsNull(Box::new(crate::expressions::IsNull {
                                this: max_dist.clone(),
                                not: false,
                                postfix_form: false,
                            }));
                        let null_check = Expression::Or(Box::new(crate::expressions::BinaryOp {
                            left: lev_is_null,
                            right: max_is_null,
                            left_comments: Vec::new(),
                            operator_comments: Vec::new(),
                            trailing_comments: Vec::new(),
                            inferred_type: None,
                        }));
                        let least = Expression::Least(Box::new(crate::expressions::VarArgFunc {
                            expressions: vec![lev, max_dist],
                            original_name: None,
                            inferred_type: None,
                        }));
                        return Ok(Expression::Case(Box::new(crate::expressions::Case {
                            operand: None,
                            whens: vec![(null_check, Expression::Null(crate::expressions::Null))],
                            else_: Some(least),
                            comments: Vec::new(),
                            inferred_type: None,
                        })));
                    }
                    let mut all_args = vec![levenshtein.this, levenshtein.expression, max_dist];
                    all_args.extend(positional_args);
                    // PostgreSQL: use LEVENSHTEIN_LESS_EQUAL when max_distance is provided
                    let func_name = if matches!(target, DialectType::PostgreSQL) {
                        "LEVENSHTEIN_LESS_EQUAL"
                    } else {
                        "LEVENSHTEIN"
                    };
                    return Ok(Expression::Function(Box::new(Function::new(
                        func_name.to_string(),
                        all_args,
                    ))));
                }
                Ok(Expression::Levenshtein(Box::new(levenshtein)))
            } else {
                Ok(Expression::Function(Box::new(Function::new(
                    "EDIT_DISTANCE".to_string(),
                    positional_args,
                ))))
            }
        }

        // TIMESTAMP_SECONDS(x) -> UnixToTime with scale 0
        "TIMESTAMP_SECONDS" if args.len() == 1 => {
            let arg = args.remove(0);
            Ok(Expression::UnixToTime(Box::new(
                crate::expressions::UnixToTime {
                    this: Box::new(arg),
                    scale: Some(0),
                    zone: None,
                    hours: None,
                    minutes: None,
                    format: None,
                    target_type: None,
                },
            )))
        }

        // TIMESTAMP_MILLIS(x) -> UnixToTime with scale 3
        "TIMESTAMP_MILLIS" if args.len() == 1 => {
            let arg = args.remove(0);
            Ok(Expression::UnixToTime(Box::new(
                crate::expressions::UnixToTime {
                    this: Box::new(arg),
                    scale: Some(3),
                    zone: None,
                    hours: None,
                    minutes: None,
                    format: None,
                    target_type: None,
                },
            )))
        }

        // TIMESTAMP_MICROS(x) -> UnixToTime with scale 6
        "TIMESTAMP_MICROS" if args.len() == 1 => {
            let arg = args.remove(0);
            Ok(Expression::UnixToTime(Box::new(
                crate::expressions::UnixToTime {
                    this: Box::new(arg),
                    scale: Some(6),
                    zone: None,
                    hours: None,
                    minutes: None,
                    format: None,
                    target_type: None,
                },
            )))
        }

        // DIV(x, y) -> IntDiv expression
        "DIV" if args.len() == 2 => {
            let x = args.remove(0);
            let y = args.remove(0);
            Ok(Expression::IntDiv(Box::new(
                crate::expressions::BinaryFunc {
                    this: x,
                    expression: y,
                    original_name: None,
                    inferred_type: None,
                },
            )))
        }

        // TO_HEX(x) -> target-specific form
        "TO_HEX" if args.len() == 1 => {
            let arg = args.remove(0);
            // Check if inner function already returns hex string in certain targets
            let inner_returns_hex = matches!(&arg, Expression::Function(f) if matches!(f.name.as_str(), "MD5" | "SHA1" | "SHA256" | "SHA512"));
            if matches!(target, DialectType::BigQuery) {
                // BQ->BQ: keep as TO_HEX
                Ok(Expression::Function(Box::new(Function::new(
                    "TO_HEX".to_string(),
                    vec![arg],
                ))))
            } else if matches!(target, DialectType::DuckDB) && inner_returns_hex {
                // DuckDB: MD5/SHA already return hex strings, so TO_HEX is redundant
                Ok(arg)
            } else if matches!(target, DialectType::Snowflake) && inner_returns_hex {
                // Snowflake: TO_HEX(SHA1(x)) -> TO_CHAR(SHA1_BINARY(x))
                // TO_HEX(MD5(x)) -> TO_CHAR(MD5_BINARY(x))
                // TO_HEX(SHA256(x)) -> TO_CHAR(SHA2_BINARY(x, 256))
                // TO_HEX(SHA512(x)) -> TO_CHAR(SHA2_BINARY(x, 512))
                if let Expression::Function(ref inner_f) = arg {
                    let inner_args = inner_f.args.clone();
                    let binary_func = match inner_f.name.to_ascii_uppercase().as_str() {
                        "SHA1" => Expression::Function(Box::new(Function::new(
                            "SHA1_BINARY".to_string(),
                            inner_args,
                        ))),
                        "MD5" => Expression::Function(Box::new(Function::new(
                            "MD5_BINARY".to_string(),
                            inner_args,
                        ))),
                        "SHA256" => {
                            let mut a = inner_args;
                            a.push(Expression::number(256));
                            Expression::Function(Box::new(Function::new(
                                "SHA2_BINARY".to_string(),
                                a,
                            )))
                        }
                        "SHA512" => {
                            let mut a = inner_args;
                            a.push(Expression::number(512));
                            Expression::Function(Box::new(Function::new(
                                "SHA2_BINARY".to_string(),
                                a,
                            )))
                        }
                        _ => arg.clone(),
                    };
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_CHAR".to_string(),
                        vec![binary_func],
                    ))))
                } else {
                    let inner =
                        Expression::Function(Box::new(Function::new("HEX".to_string(), vec![arg])));
                    Ok(Expression::Lower(Box::new(
                        crate::expressions::UnaryFunc::new(inner),
                    )))
                }
            } else if matches!(target, DialectType::Presto | DialectType::Trino) {
                let inner =
                    Expression::Function(Box::new(Function::new("TO_HEX".to_string(), vec![arg])));
                Ok(Expression::Lower(Box::new(
                    crate::expressions::UnaryFunc::new(inner),
                )))
            } else {
                let inner =
                    Expression::Function(Box::new(Function::new("HEX".to_string(), vec![arg])));
                Ok(Expression::Lower(Box::new(
                    crate::expressions::UnaryFunc::new(inner),
                )))
            }
        }

        // LAST_DAY(date, unit) -> strip unit for most targets, or transform for PostgreSQL
        "LAST_DAY" if args.len() == 2 => {
            let date = args.remove(0);
            let _unit = args.remove(0); // Strip the unit (MONTH is default)
            Ok(Expression::Function(Box::new(Function::new(
                "LAST_DAY".to_string(),
                vec![date],
            ))))
        }

        // GENERATE_ARRAY(start, end, step?) -> GenerateSeries expression
        "GENERATE_ARRAY" => {
            let start = args.get(0).cloned();
            let end = args.get(1).cloned();
            let step = args.get(2).cloned();
            Ok(Expression::GenerateSeries(Box::new(
                crate::expressions::GenerateSeries {
                    start: start.map(Box::new),
                    end: end.map(Box::new),
                    step: step.map(Box::new),
                    is_end_exclusive: None,
                },
            )))
        }

        // GENERATE_TIMESTAMP_ARRAY(start, end, step) -> GenerateSeries expression
        "GENERATE_TIMESTAMP_ARRAY" => {
            let start = args.get(0).cloned();
            let end = args.get(1).cloned();
            let step = args.get(2).cloned();

            if matches!(target, DialectType::DuckDB) {
                // DuckDB: GENERATE_SERIES(CAST(start AS TIMESTAMP), CAST(end AS TIMESTAMP), step)
                // Only cast string literals - leave columns/expressions as-is
                let maybe_cast_ts = |expr: Expression| -> Expression {
                    if matches!(&expr, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                    {
                        Expression::Cast(Box::new(Cast {
                            this: expr,
                            to: DataType::Timestamp {
                                precision: None,
                                timezone: false,
                            },
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        }))
                    } else {
                        expr
                    }
                };
                let cast_start = start.map(maybe_cast_ts);
                let cast_end = end.map(maybe_cast_ts);
                Ok(Expression::GenerateSeries(Box::new(
                    crate::expressions::GenerateSeries {
                        start: cast_start.map(Box::new),
                        end: cast_end.map(Box::new),
                        step: step.map(Box::new),
                        is_end_exclusive: None,
                    },
                )))
            } else {
                Ok(Expression::GenerateSeries(Box::new(
                    crate::expressions::GenerateSeries {
                        start: start.map(Box::new),
                        end: end.map(Box::new),
                        step: step.map(Box::new),
                        is_end_exclusive: None,
                    },
                )))
            }
        }

        // TO_JSON(x) -> target-specific (from Spark/Hive)
        "TO_JSON" => {
            match target {
                DialectType::Presto | DialectType::Trino => {
                    // JSON_FORMAT(CAST(x AS JSON))
                    let arg = args
                        .into_iter()
                        .next()
                        .unwrap_or(Expression::Null(crate::expressions::Null));
                    let cast_json = Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Custom {
                            name: "JSON".to_string(),
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "JSON_FORMAT".to_string(),
                        vec![cast_json],
                    ))))
                }
                DialectType::BigQuery => Ok(Expression::Function(Box::new(Function::new(
                    "TO_JSON_STRING".to_string(),
                    args,
                )))),
                DialectType::DuckDB => {
                    // CAST(TO_JSON(x) AS TEXT)
                    let arg = args
                        .into_iter()
                        .next()
                        .unwrap_or(Expression::Null(crate::expressions::Null));
                    let to_json = Expression::Function(Box::new(Function::new(
                        "TO_JSON".to_string(),
                        vec![arg],
                    )));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: to_json,
                        to: DataType::Text,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "TO_JSON".to_string(),
                    args,
                )))),
            }
        }

        // TO_JSON_STRING(x) -> target-specific
        "TO_JSON_STRING" => {
            match target {
                DialectType::Spark | DialectType::Databricks | DialectType::Hive => Ok(
                    Expression::Function(Box::new(Function::new("TO_JSON".to_string(), args))),
                ),
                DialectType::Presto | DialectType::Trino => {
                    // JSON_FORMAT(CAST(x AS JSON))
                    let arg = args
                        .into_iter()
                        .next()
                        .unwrap_or(Expression::Null(crate::expressions::Null));
                    let cast_json = Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Custom {
                            name: "JSON".to_string(),
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "JSON_FORMAT".to_string(),
                        vec![cast_json],
                    ))))
                }
                DialectType::DuckDB => {
                    // CAST(TO_JSON(x) AS TEXT)
                    let arg = args
                        .into_iter()
                        .next()
                        .unwrap_or(Expression::Null(crate::expressions::Null));
                    let to_json = Expression::Function(Box::new(Function::new(
                        "TO_JSON".to_string(),
                        vec![arg],
                    )));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: to_json,
                        to: DataType::Text,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
                DialectType::Snowflake => {
                    // TO_JSON(x)
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_JSON".to_string(),
                        args,
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "TO_JSON_STRING".to_string(),
                    args,
                )))),
            }
        }

        // SAFE_ADD(x, y) -> SafeAdd expression
        "SAFE_ADD" if args.len() == 2 => {
            let x = args.remove(0);
            let y = args.remove(0);
            Ok(Expression::SafeAdd(Box::new(crate::expressions::SafeAdd {
                this: Box::new(x),
                expression: Box::new(y),
            })))
        }

        // SAFE_SUBTRACT(x, y) -> SafeSubtract expression
        "SAFE_SUBTRACT" if args.len() == 2 => {
            let x = args.remove(0);
            let y = args.remove(0);
            Ok(Expression::SafeSubtract(Box::new(
                crate::expressions::SafeSubtract {
                    this: Box::new(x),
                    expression: Box::new(y),
                },
            )))
        }

        // SAFE_MULTIPLY(x, y) -> SafeMultiply expression
        "SAFE_MULTIPLY" if args.len() == 2 => {
            let x = args.remove(0);
            let y = args.remove(0);
            Ok(Expression::SafeMultiply(Box::new(
                crate::expressions::SafeMultiply {
                    this: Box::new(x),
                    expression: Box::new(y),
                },
            )))
        }

        // REGEXP_CONTAINS(str, pattern) -> RegexpLike expression
        "REGEXP_CONTAINS" if args.len() == 2 => {
            let str_expr = args.remove(0);
            let pattern = args.remove(0);
            Ok(Expression::RegexpLike(Box::new(
                crate::expressions::RegexpFunc {
                    this: str_expr,
                    pattern,
                    flags: None,
                },
            )))
        }

        // CONTAINS_SUBSTR(a, b) -> CONTAINS(LOWER(a), LOWER(b))
        "CONTAINS_SUBSTR" if args.len() == 2 => {
            let a = args.remove(0);
            let b = args.remove(0);
            let lower_a = Expression::Lower(Box::new(crate::expressions::UnaryFunc::new(a)));
            let lower_b = Expression::Lower(Box::new(crate::expressions::UnaryFunc::new(b)));
            Ok(Expression::Function(Box::new(Function::new(
                "CONTAINS".to_string(),
                vec![lower_a, lower_b],
            ))))
        }

        // INT64(x) -> CAST(x AS BIGINT)
        "INT64" if args.len() == 1 => {
            let arg = args.remove(0);
            Ok(Expression::Cast(Box::new(Cast {
                this: arg,
                to: DataType::BigInt { length: None },
                trailing_comments: vec![],
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            })))
        }

        // INSTR(str, substr) -> target-specific
        "INSTR" if args.len() >= 2 => {
            let str_expr = args.remove(0);
            let substr = args.remove(0);
            if matches!(target, DialectType::Snowflake) {
                // CHARINDEX(substr, str)
                Ok(Expression::Function(Box::new(Function::new(
                    "CHARINDEX".to_string(),
                    vec![substr, str_expr],
                ))))
            } else if matches!(target, DialectType::BigQuery) {
                // Keep as INSTR
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![str_expr, substr],
                ))))
            } else {
                // Default: keep as INSTR
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    vec![str_expr, substr],
                ))))
            }
        }

        // BigQuery DATE_TRUNC(expr, unit) -> DATE_TRUNC('unit', expr) for standard SQL
        "DATE_TRUNC" if args.len() == 2 => {
            let expr = args.remove(0);
            let unit_expr = args.remove(0);
            let unit_str = get_unit_str(&unit_expr);

            match target {
                DialectType::DuckDB
                | DialectType::Snowflake
                | DialectType::PostgreSQL
                | DialectType::Presto
                | DialectType::Trino
                | DialectType::Databricks
                | DialectType::Spark
                | DialectType::Redshift
                | DialectType::ClickHouse
                | DialectType::TSQL => {
                    // Standard: DATE_TRUNC('UNIT', expr)
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_TRUNC".to_string(),
                        vec![
                            Expression::Literal(Box::new(Literal::String(unit_str))),
                            expr,
                        ],
                    ))))
                }
                _ => {
                    // Keep BigQuery arg order: DATE_TRUNC(expr, unit)
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_TRUNC".to_string(),
                        vec![expr, unit_expr],
                    ))))
                }
            }
        }

        // TIMESTAMP_TRUNC / DATETIME_TRUNC -> target-specific
        "TIMESTAMP_TRUNC" | "DATETIME_TRUNC" if args.len() >= 2 => {
            // TIMESTAMP_TRUNC(ts, unit) or TIMESTAMP_TRUNC(ts, unit, timezone)
            let ts = args.remove(0);
            let unit_expr = args.remove(0);
            let tz = if !args.is_empty() {
                Some(args.remove(0))
            } else {
                None
            };
            let unit_str = get_unit_str(&unit_expr);

            match target {
                DialectType::DuckDB => {
                    // DuckDB: DATE_TRUNC('UNIT', CAST(ts AS TIMESTAMPTZ))
                    // With timezone: DATE_TRUNC('UNIT', ts AT TIME ZONE 'tz') AT TIME ZONE 'tz' (for DAY granularity)
                    // Without timezone for MINUTE+ granularity: just DATE_TRUNC
                    let is_coarse = matches!(
                        unit_str.as_str(),
                        "DAY" | "WEEK" | "MONTH" | "QUARTER" | "YEAR"
                    );
                    // For DATETIME_TRUNC, cast string args to TIMESTAMP
                    let cast_ts = if name == "DATETIME_TRUNC" {
                        match ts {
                            Expression::Literal(ref lit)
                                if matches!(lit.as_ref(), Literal::String(ref _s)) =>
                            {
                                Expression::Cast(Box::new(Cast {
                                    this: ts,
                                    to: DataType::Timestamp {
                                        precision: None,
                                        timezone: false,
                                    },
                                    trailing_comments: vec![],
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))
                            }
                            _ => temporal::maybe_cast_ts_to_tz(ts, &name),
                        }
                    } else {
                        temporal::maybe_cast_ts_to_tz(ts, &name)
                    };

                    if let Some(tz_arg) = tz {
                        if is_coarse {
                            // DATE_TRUNC('UNIT', ts AT TIME ZONE 'tz') AT TIME ZONE 'tz'
                            let at_tz =
                                Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone {
                                    this: cast_ts,
                                    zone: tz_arg.clone(),
                                }));
                            let date_trunc = Expression::Function(Box::new(Function::new(
                                "DATE_TRUNC".to_string(),
                                vec![
                                    Expression::Literal(Box::new(Literal::String(unit_str))),
                                    at_tz,
                                ],
                            )));
                            Ok(Expression::AtTimeZone(Box::new(
                                crate::expressions::AtTimeZone {
                                    this: date_trunc,
                                    zone: tz_arg,
                                },
                            )))
                        } else {
                            // For MINUTE/HOUR: no AT TIME ZONE wrapper, just DATE_TRUNC('UNIT', ts)
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_TRUNC".to_string(),
                                vec![
                                    Expression::Literal(Box::new(Literal::String(unit_str))),
                                    cast_ts,
                                ],
                            ))))
                        }
                    } else {
                        // No timezone: DATE_TRUNC('UNIT', CAST(ts AS TIMESTAMPTZ))
                        Ok(Expression::Function(Box::new(Function::new(
                            "DATE_TRUNC".to_string(),
                            vec![
                                Expression::Literal(Box::new(Literal::String(unit_str))),
                                cast_ts,
                            ],
                        ))))
                    }
                }
                DialectType::Databricks | DialectType::Spark => {
                    // Databricks/Spark: DATE_TRUNC('UNIT', ts)
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_TRUNC".to_string(),
                        vec![Expression::Literal(Box::new(Literal::String(unit_str))), ts],
                    ))))
                }
                _ => {
                    // Default: keep as TIMESTAMP_TRUNC('UNIT', ts, [tz])
                    let unit = Expression::Literal(Box::new(Literal::String(unit_str)));
                    let mut date_trunc_args = vec![unit, ts];
                    if let Some(tz_arg) = tz {
                        date_trunc_args.push(tz_arg);
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "TIMESTAMP_TRUNC".to_string(),
                        date_trunc_args,
                    ))))
                }
            }
        }

        // TIME(h, m, s) -> target-specific, TIME('string') -> CAST('string' AS TIME)
        "TIME" => {
            if args.len() == 3 {
                // TIME(h, m, s) constructor
                match target {
                    DialectType::TSQL => {
                        // TIMEFROMPARTS(h, m, s, 0, 0)
                        args.push(Expression::number(0));
                        args.push(Expression::number(0));
                        Ok(Expression::Function(Box::new(Function::new(
                            "TIMEFROMPARTS".to_string(),
                            args,
                        ))))
                    }
                    DialectType::MySQL => Ok(Expression::Function(Box::new(Function::new(
                        "MAKETIME".to_string(),
                        args,
                    )))),
                    DialectType::PostgreSQL => Ok(Expression::Function(Box::new(Function::new(
                        "MAKE_TIME".to_string(),
                        args,
                    )))),
                    _ => Ok(Expression::Function(Box::new(Function::new(
                        "TIME".to_string(),
                        args,
                    )))),
                }
            } else if args.len() == 1 {
                let arg = args.remove(0);
                if matches!(target, DialectType::Spark) {
                    // Spark: CAST(x AS TIMESTAMP) (yes, TIMESTAMP not TIME)
                    Ok(Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Timestamp {
                            timezone: false,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                } else {
                    // Most targets: CAST(x AS TIME)
                    Ok(Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Time {
                            precision: None,
                            timezone: false,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
            } else if args.len() == 2 {
                // TIME(expr, timezone) -> CAST(CAST(expr AS TIMESTAMPTZ) AT TIME ZONE tz AS TIME)
                let expr = args.remove(0);
                let tz = args.remove(0);
                let cast_tstz = Expression::Cast(Box::new(Cast {
                    this: expr,
                    to: DataType::Timestamp {
                        timezone: true,
                        precision: None,
                    },
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                }));
                let at_tz = Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone {
                    this: cast_tstz,
                    zone: tz,
                }));
                Ok(Expression::Cast(Box::new(Cast {
                    this: at_tz,
                    to: DataType::Time {
                        precision: None,
                        timezone: false,
                    },
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })))
            } else {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIME".to_string(),
                    args,
                ))))
            }
        }

        // DATETIME('string') -> CAST('string' AS TIMESTAMP)
        // DATETIME('date', TIME 'time') -> CAST(CAST('date' AS DATE) + CAST('time' AS TIME) AS TIMESTAMP)
        // DATETIME('string', 'timezone') -> CAST(CAST('string' AS TIMESTAMPTZ) AT TIME ZONE tz AS TIMESTAMP)
        // DATETIME(y, m, d, h, min, s) -> target-specific
        "DATETIME" => {
            // For BigQuery target: keep DATETIME function but convert TIME literal to CAST
            if matches!(target, DialectType::BigQuery) {
                if args.len() == 2 {
                    let has_time_literal = matches!(&args[1], Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Time(_)));
                    if has_time_literal {
                        let first = args.remove(0);
                        let second = args.remove(0);
                        let time_as_cast = match second {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::Time(_)) =>
                            {
                                let Literal::Time(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                Expression::Cast(Box::new(Cast {
                                    this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                                    to: DataType::Time {
                                        precision: None,
                                        timezone: false,
                                    },
                                    trailing_comments: vec![],
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))
                            }
                            other => other,
                        };
                        return Ok(Expression::Function(Box::new(Function::new(
                            "DATETIME".to_string(),
                            vec![first, time_as_cast],
                        ))));
                    }
                }
                return Ok(Expression::Function(Box::new(Function::new(
                    "DATETIME".to_string(),
                    args,
                ))));
            }

            if args.len() == 1 {
                let arg = args.remove(0);
                Ok(Expression::Cast(Box::new(Cast {
                    this: arg,
                    to: DataType::Timestamp {
                        timezone: false,
                        precision: None,
                    },
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })))
            } else if args.len() == 2 {
                let first = args.remove(0);
                let second = args.remove(0);
                // Check if second arg is a TIME literal
                let is_time_literal = matches!(&second, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Time(_)));
                if is_time_literal {
                    // DATETIME('date', TIME 'time') -> CAST(CAST(date AS DATE) + CAST('time' AS TIME) AS TIMESTAMP)
                    let cast_date = Expression::Cast(Box::new(Cast {
                        this: first,
                        to: DataType::Date,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    // Convert TIME 'x' literal to string 'x' so CAST produces CAST('x' AS TIME) not CAST(TIME 'x' AS TIME)
                    let time_as_string = match second {
                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Time(_)) => {
                            let Literal::Time(s) = lit.as_ref() else {
                                unreachable!()
                            };
                            Expression::Literal(Box::new(Literal::String(s.clone())))
                        }
                        other => other,
                    };
                    let cast_time = Expression::Cast(Box::new(Cast {
                        this: time_as_string,
                        to: DataType::Time {
                            precision: None,
                            timezone: false,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let add_expr = Expression::Add(Box::new(BinaryOp::new(cast_date, cast_time)));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: add_expr,
                        to: DataType::Timestamp {
                            timezone: false,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                } else {
                    // DATETIME('string', 'timezone')
                    let cast_tstz = Expression::Cast(Box::new(Cast {
                        this: first,
                        to: DataType::Timestamp {
                            timezone: true,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let at_tz = Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone {
                        this: cast_tstz,
                        zone: second,
                    }));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: at_tz,
                        to: DataType::Timestamp {
                            timezone: false,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
            } else if args.len() >= 3 {
                // DATETIME(y, m, d, h, min, s) -> TIMESTAMP_FROM_PARTS for Snowflake
                // For other targets, use MAKE_TIMESTAMP or similar
                if matches!(target, DialectType::Snowflake) {
                    Ok(Expression::Function(Box::new(Function::new(
                        "TIMESTAMP_FROM_PARTS".to_string(),
                        args,
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATETIME".to_string(),
                        args,
                    ))))
                }
            } else {
                Ok(Expression::Function(Box::new(Function::new(
                    "DATETIME".to_string(),
                    args,
                ))))
            }
        }

        // TIMESTAMP(x) -> CAST(x AS TIMESTAMP WITH TIME ZONE) for Presto
        // TIMESTAMP(x, tz) -> CAST(x AS TIMESTAMP) AT TIME ZONE tz for DuckDB
        "TIMESTAMP" => {
            if args.len() == 1 {
                let arg = args.remove(0);
                Ok(Expression::Cast(Box::new(Cast {
                    this: arg,
                    to: DataType::Timestamp {
                        timezone: true,
                        precision: None,
                    },
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })))
            } else if args.len() == 2 {
                let arg = args.remove(0);
                let tz = args.remove(0);
                let cast_ts = Expression::Cast(Box::new(Cast {
                    this: arg,
                    to: DataType::Timestamp {
                        timezone: false,
                        precision: None,
                    },
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                }));
                if matches!(target, DialectType::Snowflake) {
                    // CONVERT_TIMEZONE('tz', CAST(x AS TIMESTAMP))
                    Ok(Expression::Function(Box::new(Function::new(
                        "CONVERT_TIMEZONE".to_string(),
                        vec![tz, cast_ts],
                    ))))
                } else {
                    Ok(Expression::AtTimeZone(Box::new(
                        crate::expressions::AtTimeZone {
                            this: cast_ts,
                            zone: tz,
                        },
                    )))
                }
            } else {
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP".to_string(),
                    args,
                ))))
            }
        }

        // STRING(x) -> CAST(x AS VARCHAR/TEXT)
        // STRING(x, tz) -> CAST(CAST(x AS TIMESTAMP) AT TIME ZONE 'UTC' AT TIME ZONE tz AS VARCHAR/TEXT)
        "STRING" => {
            if args.len() == 1 {
                let arg = args.remove(0);
                let cast_type = match target {
                    DialectType::DuckDB => DataType::Text,
                    _ => DataType::VarChar {
                        length: None,
                        parenthesized_length: false,
                    },
                };
                Ok(Expression::Cast(Box::new(Cast {
                    this: arg,
                    to: cast_type,
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })))
            } else if args.len() == 2 {
                let arg = args.remove(0);
                let tz = args.remove(0);
                let cast_type = match target {
                    DialectType::DuckDB => DataType::Text,
                    _ => DataType::VarChar {
                        length: None,
                        parenthesized_length: false,
                    },
                };
                if matches!(target, DialectType::Snowflake) {
                    // STRING(x, tz) -> CAST(CONVERT_TIMEZONE('UTC', tz, x) AS VARCHAR)
                    let convert_tz = Expression::Function(Box::new(Function::new(
                        "CONVERT_TIMEZONE".to_string(),
                        vec![
                            Expression::Literal(Box::new(Literal::String("UTC".to_string()))),
                            tz,
                            arg,
                        ],
                    )));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: convert_tz,
                        to: cast_type,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                } else {
                    // STRING(x, tz) -> CAST(CAST(x AS TIMESTAMP) AT TIME ZONE 'UTC' AT TIME ZONE tz AS TEXT/VARCHAR)
                    let cast_ts = Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Timestamp {
                            timezone: false,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let at_utc = Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone {
                        this: cast_ts,
                        zone: Expression::Literal(Box::new(Literal::String("UTC".to_string()))),
                    }));
                    let at_tz = Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone {
                        this: at_utc,
                        zone: tz,
                    }));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: at_tz,
                        to: cast_type,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
            } else {
                Ok(Expression::Function(Box::new(Function::new(
                    "STRING".to_string(),
                    args,
                ))))
            }
        }

        // UNIX_SECONDS, UNIX_MILLIS, UNIX_MICROS as functions (not expressions)
        "UNIX_SECONDS" if args.len() == 1 => {
            let ts = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // CAST(EPOCH(CAST(ts AS TIMESTAMPTZ)) AS BIGINT)
                    let cast_ts = temporal::ensure_cast_timestamptz(ts);
                    let epoch = Expression::Function(Box::new(Function::new(
                        "EPOCH".to_string(),
                        vec![cast_ts],
                    )));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: epoch,
                        to: DataType::BigInt { length: None },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
                DialectType::Snowflake => {
                    // TIMESTAMPDIFF(SECONDS, CAST('1970-01-01 00:00:00+00' AS TIMESTAMPTZ), ts)
                    let epoch = Expression::Cast(Box::new(Cast {
                        this: Expression::Literal(Box::new(Literal::String(
                            "1970-01-01 00:00:00+00".to_string(),
                        ))),
                        to: DataType::Timestamp {
                            timezone: true,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::TimestampDiff(Box::new(
                        crate::expressions::TimestampDiff {
                            this: Box::new(epoch),
                            expression: Box::new(ts),
                            unit: Some("SECONDS".to_string()),
                        },
                    )))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "UNIX_SECONDS".to_string(),
                    vec![ts],
                )))),
            }
        }

        "UNIX_MILLIS" if args.len() == 1 => {
            let ts = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // EPOCH_MS(CAST(ts AS TIMESTAMPTZ))
                    let cast_ts = temporal::ensure_cast_timestamptz(ts);
                    Ok(Expression::Function(Box::new(Function::new(
                        "EPOCH_MS".to_string(),
                        vec![cast_ts],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "UNIX_MILLIS".to_string(),
                    vec![ts],
                )))),
            }
        }

        "UNIX_MICROS" if args.len() == 1 => {
            let ts = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // EPOCH_US(CAST(ts AS TIMESTAMPTZ))
                    let cast_ts = temporal::ensure_cast_timestamptz(ts);
                    Ok(Expression::Function(Box::new(Function::new(
                        "EPOCH_US".to_string(),
                        vec![cast_ts],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "UNIX_MICROS".to_string(),
                    vec![ts],
                )))),
            }
        }

        // ARRAY_CONCAT / LIST_CONCAT -> target-specific
        "ARRAY_CONCAT" | "LIST_CONCAT" => {
            match target {
                DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                    // CONCAT(arr1, arr2, ...)
                    Ok(Expression::Function(Box::new(Function::new(
                        "CONCAT".to_string(),
                        args,
                    ))))
                }
                DialectType::Presto | DialectType::Trino => {
                    // CONCAT(arr1, arr2, ...)
                    Ok(Expression::Function(Box::new(Function::new(
                        "CONCAT".to_string(),
                        args,
                    ))))
                }
                DialectType::Snowflake => {
                    // ARRAY_CAT(arr1, ARRAY_CAT(arr2, arr3))
                    if args.len() == 1 {
                        // ARRAY_CAT requires 2 args, add empty array as []
                        let empty_arr =
                            Expression::ArrayFunc(Box::new(crate::expressions::ArrayConstructor {
                                expressions: vec![],
                                bracket_notation: true,
                                use_list_keyword: false,
                            }));
                        let mut new_args = args;
                        new_args.push(empty_arr);
                        Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY_CAT".to_string(),
                            new_args,
                        ))))
                    } else if args.is_empty() {
                        Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY_CAT".to_string(),
                            args,
                        ))))
                    } else {
                        let mut it = args.into_iter().rev();
                        let mut result = it.next().unwrap();
                        for arr in it {
                            result = Expression::Function(Box::new(Function::new(
                                "ARRAY_CAT".to_string(),
                                vec![arr, result],
                            )));
                        }
                        Ok(result)
                    }
                }
                DialectType::PostgreSQL => {
                    // ARRAY_CAT(arr1, ARRAY_CAT(arr2, arr3))
                    if args.len() <= 1 {
                        Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY_CAT".to_string(),
                            args,
                        ))))
                    } else {
                        let mut it = args.into_iter().rev();
                        let mut result = it.next().unwrap();
                        for arr in it {
                            result = Expression::Function(Box::new(Function::new(
                                "ARRAY_CAT".to_string(),
                                vec![arr, result],
                            )));
                        }
                        Ok(result)
                    }
                }
                DialectType::Redshift => {
                    // ARRAY_CONCAT(arr1, ARRAY_CONCAT(arr2, arr3))
                    if args.len() <= 2 {
                        Ok(Expression::Function(Box::new(Function::new(
                            "ARRAY_CONCAT".to_string(),
                            args,
                        ))))
                    } else {
                        let mut it = args.into_iter().rev();
                        let mut result = it.next().unwrap();
                        for arr in it {
                            result = Expression::Function(Box::new(Function::new(
                                "ARRAY_CONCAT".to_string(),
                                vec![arr, result],
                            )));
                        }
                        Ok(result)
                    }
                }
                DialectType::DuckDB => {
                    // LIST_CONCAT supports multiple args natively in DuckDB
                    Ok(Expression::Function(Box::new(Function::new(
                        "LIST_CONCAT".to_string(),
                        args,
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_CONCAT".to_string(),
                    args,
                )))),
            }
        }

        // ARRAY_CONCAT_AGG -> Snowflake: ARRAY_FLATTEN(ARRAY_AGG(x))
        "ARRAY_CONCAT_AGG" if args.len() == 1 => {
            let arg = args.remove(0);
            match target {
                DialectType::Snowflake => {
                    let array_agg = Expression::ArrayAgg(Box::new(crate::expressions::AggFunc {
                        this: arg,
                        distinct: false,
                        filter: None,
                        order_by: vec![],
                        name: None,
                        ignore_nulls: None,
                        having_max: None,
                        limit: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_FLATTEN".to_string(),
                        vec![array_agg],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_CONCAT_AGG".to_string(),
                    vec![arg],
                )))),
            }
        }

        // MD5/SHA1/SHA256/SHA512 -> target-specific hash functions
        "MD5" if args.len() == 1 => {
            let arg = args.remove(0);
            match target {
                DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                    // UNHEX(MD5(x))
                    let md5 =
                        Expression::Function(Box::new(Function::new("MD5".to_string(), vec![arg])));
                    Ok(Expression::Function(Box::new(Function::new(
                        "UNHEX".to_string(),
                        vec![md5],
                    ))))
                }
                DialectType::Snowflake => {
                    // MD5_BINARY(x)
                    Ok(Expression::Function(Box::new(Function::new(
                        "MD5_BINARY".to_string(),
                        vec![arg],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "MD5".to_string(),
                    vec![arg],
                )))),
            }
        }

        "SHA1" if args.len() == 1 => {
            let arg = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // UNHEX(SHA1(x))
                    let sha1 = Expression::Function(Box::new(Function::new(
                        "SHA1".to_string(),
                        vec![arg],
                    )));
                    Ok(Expression::Function(Box::new(Function::new(
                        "UNHEX".to_string(),
                        vec![sha1],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "SHA1".to_string(),
                    vec![arg],
                )))),
            }
        }

        "SHA256" if args.len() == 1 => {
            let arg = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // UNHEX(SHA256(x))
                    let sha = Expression::Function(Box::new(Function::new(
                        "SHA256".to_string(),
                        vec![arg],
                    )));
                    Ok(Expression::Function(Box::new(Function::new(
                        "UNHEX".to_string(),
                        vec![sha],
                    ))))
                }
                DialectType::Snowflake => {
                    // SHA2_BINARY(x, 256)
                    Ok(Expression::Function(Box::new(Function::new(
                        "SHA2_BINARY".to_string(),
                        vec![arg, Expression::number(256)],
                    ))))
                }
                DialectType::Redshift | DialectType::Spark => {
                    // SHA2(x, 256)
                    Ok(Expression::Function(Box::new(Function::new(
                        "SHA2".to_string(),
                        vec![arg, Expression::number(256)],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "SHA256".to_string(),
                    vec![arg],
                )))),
            }
        }

        "SHA512" if args.len() == 1 => {
            let arg = args.remove(0);
            match target {
                DialectType::Snowflake => {
                    // SHA2_BINARY(x, 512)
                    Ok(Expression::Function(Box::new(Function::new(
                        "SHA2_BINARY".to_string(),
                        vec![arg, Expression::number(512)],
                    ))))
                }
                DialectType::Redshift | DialectType::Spark => {
                    // SHA2(x, 512)
                    Ok(Expression::Function(Box::new(Function::new(
                        "SHA2".to_string(),
                        vec![arg, Expression::number(512)],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "SHA512".to_string(),
                    vec![arg],
                )))),
            }
        }

        // REGEXP_EXTRACT_ALL(str, pattern) -> add default group arg
        "REGEXP_EXTRACT_ALL" if args.len() == 2 => {
            let str_expr = args.remove(0);
            let pattern = args.remove(0);

            // Check if pattern contains capturing groups (parentheses)
            let has_groups = match &pattern {
                Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
                    let Literal::String(s) = lit.as_ref() else {
                        unreachable!()
                    };
                    s.contains('(') && s.contains(')')
                }
                _ => false,
            };

            match target {
                DialectType::DuckDB => {
                    let group = if has_groups {
                        Expression::number(1)
                    } else {
                        Expression::number(0)
                    };
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_EXTRACT_ALL".to_string(),
                        vec![str_expr, pattern, group],
                    ))))
                }
                DialectType::Spark | DialectType::Databricks => {
                    // Spark's default group_index is 1 (same as BigQuery), so omit for capturing groups
                    if has_groups {
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT_ALL".to_string(),
                            vec![str_expr, pattern],
                        ))))
                    } else {
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT_ALL".to_string(),
                            vec![str_expr, pattern, Expression::number(0)],
                        ))))
                    }
                }
                DialectType::Presto | DialectType::Trino => {
                    if has_groups {
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT_ALL".to_string(),
                            vec![str_expr, pattern, Expression::number(1)],
                        ))))
                    } else {
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT_ALL".to_string(),
                            vec![str_expr, pattern],
                        ))))
                    }
                }
                DialectType::Snowflake => {
                    if has_groups {
                        // REGEXP_EXTRACT_ALL(str, pattern, 1, 1, 'c', 1)
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT_ALL".to_string(),
                            vec![
                                str_expr,
                                pattern,
                                Expression::number(1),
                                Expression::number(1),
                                Expression::Literal(Box::new(Literal::String("c".to_string()))),
                                Expression::number(1),
                            ],
                        ))))
                    } else {
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT_ALL".to_string(),
                            vec![str_expr, pattern],
                        ))))
                    }
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "REGEXP_EXTRACT_ALL".to_string(),
                    vec![str_expr, pattern],
                )))),
            }
        }

        // MOD(x, y) -> x % y for dialects that prefer or require the infix operator.
        "MOD" if args.len() == 2 => {
            match target {
                DialectType::PostgreSQL
                | DialectType::DuckDB
                | DialectType::Presto
                | DialectType::Trino
                | DialectType::Athena
                | DialectType::Snowflake
                | DialectType::TSQL
                | DialectType::Fabric => {
                    let x = args.remove(0);
                    let y = args.remove(0);
                    // Wrap complex expressions in parens to preserve precedence
                    let needs_paren = |e: &Expression| {
                        matches!(
                            e,
                            Expression::Add(_)
                                | Expression::Sub(_)
                                | Expression::Mul(_)
                                | Expression::Div(_)
                                | Expression::Mod(_)
                                | Expression::ModFunc(_)
                        )
                    };
                    let x = if needs_paren(&x) {
                        Expression::Paren(Box::new(crate::expressions::Paren {
                            this: x,
                            trailing_comments: vec![],
                        }))
                    } else {
                        x
                    };
                    let y = if needs_paren(&y) {
                        Expression::Paren(Box::new(crate::expressions::Paren {
                            this: y,
                            trailing_comments: vec![],
                        }))
                    } else {
                        y
                    };
                    Ok(Expression::Mod(Box::new(
                        crate::expressions::BinaryOp::new(x, y),
                    )))
                }
                DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                    // Hive/Spark: a % b
                    let x = args.remove(0);
                    let y = args.remove(0);
                    let needs_paren = |e: &Expression| {
                        matches!(
                            e,
                            Expression::Add(_)
                                | Expression::Sub(_)
                                | Expression::Mul(_)
                                | Expression::Div(_)
                                | Expression::Mod(_)
                                | Expression::ModFunc(_)
                        )
                    };
                    let x = if needs_paren(&x) {
                        Expression::Paren(Box::new(crate::expressions::Paren {
                            this: x,
                            trailing_comments: vec![],
                        }))
                    } else {
                        x
                    };
                    let y = if needs_paren(&y) {
                        Expression::Paren(Box::new(crate::expressions::Paren {
                            this: y,
                            trailing_comments: vec![],
                        }))
                    } else {
                        y
                    };
                    Ok(Expression::Mod(Box::new(
                        crate::expressions::BinaryOp::new(x, y),
                    )))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "MOD".to_string(),
                    args,
                )))),
            }
        }

        // ARRAY_FILTER(arr, lambda) -> FILTER for Hive/Spark/Presto, ARRAY_FILTER for StarRocks
        "ARRAY_FILTER" if args.len() == 2 => {
            let name = match target {
                DialectType::DuckDB => "LIST_FILTER",
                DialectType::StarRocks => "ARRAY_FILTER",
                _ => "FILTER",
            };
            Ok(Expression::Function(Box::new(Function::new(
                name.to_string(),
                args,
            ))))
        }
        // FILTER(arr, lambda) -> ARRAY_FILTER for StarRocks, LIST_FILTER for DuckDB
        "FILTER" if args.len() == 2 => {
            let name = match target {
                DialectType::DuckDB => "LIST_FILTER",
                DialectType::StarRocks => "ARRAY_FILTER",
                _ => "FILTER",
            };
            Ok(Expression::Function(Box::new(Function::new(
                name.to_string(),
                args,
            ))))
        }
        // REDUCE(arr, init, lambda1, lambda2) -> AGGREGATE for Spark
        "REDUCE" if args.len() >= 3 => {
            let name = match target {
                DialectType::Spark | DialectType::Databricks => "AGGREGATE",
                _ => "REDUCE",
            };
            Ok(Expression::Function(Box::new(Function::new(
                name.to_string(),
                args,
            ))))
        }
        // ARRAY_REVERSE(x) -> arrayReverse for ClickHouse (handled by generator)
        "ARRAY_REVERSE" if args.len() == 1 => Ok(Expression::Function(Box::new(Function::new(
            "ARRAY_REVERSE".to_string(),
            args,
        )))),

        // CONCAT(a, b, ...) -> a || b || ... for DuckDB with 3+ args
        "CONCAT" if args.len() > 2 => match target {
            DialectType::DuckDB => {
                let mut it = args.into_iter();
                let mut result = it.next().unwrap();
                for arg in it {
                    result = Expression::DPipe(Box::new(crate::expressions::DPipe {
                        this: Box::new(result),
                        expression: Box::new(arg),
                        safe: None,
                    }));
                }
                Ok(result)
            }
            _ => Ok(Expression::Function(Box::new(Function::new(
                "CONCAT".to_string(),
                args,
            )))),
        },

        // GENERATE_DATE_ARRAY(start, end[, step]) -> target-specific
        "GENERATE_DATE_ARRAY" => {
            if matches!(target, DialectType::BigQuery) {
                // BQ->BQ: add default interval if not present
                if args.len() == 2 {
                    let start = args.remove(0);
                    let end = args.remove(0);
                    let default_interval =
                        Expression::Interval(Box::new(crate::expressions::Interval {
                            this: Some(Expression::Literal(Box::new(Literal::String(
                                "1".to_string(),
                            )))),
                            unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                unit: crate::expressions::IntervalUnit::Day,
                                use_plural: false,
                            }),
                        }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "GENERATE_DATE_ARRAY".to_string(),
                        vec![start, end, default_interval],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "GENERATE_DATE_ARRAY".to_string(),
                        args,
                    ))))
                }
            } else if matches!(target, DialectType::DuckDB) {
                // DuckDB: CAST(GENERATE_SERIES(CAST(start AS DATE), CAST(end AS DATE), step) AS DATE[])
                let start = args.get(0).cloned();
                let end = args.get(1).cloned();
                let step = args.get(2).cloned().or_else(|| {
                    Some(Expression::Interval(Box::new(
                        crate::expressions::Interval {
                            this: Some(Expression::Literal(Box::new(Literal::String(
                                "1".to_string(),
                            )))),
                            unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                unit: crate::expressions::IntervalUnit::Day,
                                use_plural: false,
                            }),
                        },
                    )))
                });

                // Wrap start/end in CAST(... AS DATE) only for string literals
                let maybe_cast_date = |expr: Expression| -> Expression {
                    if matches!(&expr, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                    {
                        Expression::Cast(Box::new(Cast {
                            this: expr,
                            to: DataType::Date,
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        }))
                    } else {
                        expr
                    }
                };
                let cast_start = start.map(maybe_cast_date);
                let cast_end = end.map(maybe_cast_date);

                let gen_series =
                    Expression::GenerateSeries(Box::new(crate::expressions::GenerateSeries {
                        start: cast_start.map(Box::new),
                        end: cast_end.map(Box::new),
                        step: step.map(Box::new),
                        is_end_exclusive: None,
                    }));

                // Wrap in CAST(... AS DATE[])
                Ok(Expression::Cast(Box::new(Cast {
                    this: gen_series,
                    to: DataType::Array {
                        element_type: Box::new(DataType::Date),
                        dimension: None,
                    },
                    trailing_comments: vec![],
                    double_colon_syntax: false,
                    format: None,
                    default: None,
                    inferred_type: None,
                })))
            } else if matches!(target, DialectType::Snowflake) {
                // Snowflake: keep as GENERATE_DATE_ARRAY function for later transform
                // (transform_generate_date_array_snowflake will convert to ARRAY_GENERATE_RANGE + DATEADD)
                if args.len() == 2 {
                    let start = args.remove(0);
                    let end = args.remove(0);
                    let default_interval =
                        Expression::Interval(Box::new(crate::expressions::Interval {
                            this: Some(Expression::Literal(Box::new(Literal::String(
                                "1".to_string(),
                            )))),
                            unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                unit: crate::expressions::IntervalUnit::Day,
                                use_plural: false,
                            }),
                        }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "GENERATE_DATE_ARRAY".to_string(),
                        vec![start, end, default_interval],
                    ))))
                } else {
                    Ok(Expression::Function(Box::new(Function::new(
                        "GENERATE_DATE_ARRAY".to_string(),
                        args,
                    ))))
                }
            } else {
                // Convert to GenerateSeries for other targets
                let start = args.get(0).cloned();
                let end = args.get(1).cloned();
                let step = args.get(2).cloned().or_else(|| {
                    Some(Expression::Interval(Box::new(
                        crate::expressions::Interval {
                            this: Some(Expression::Literal(Box::new(Literal::String(
                                "1".to_string(),
                            )))),
                            unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                unit: crate::expressions::IntervalUnit::Day,
                                use_plural: false,
                            }),
                        },
                    )))
                });
                Ok(Expression::GenerateSeries(Box::new(
                    crate::expressions::GenerateSeries {
                        start: start.map(Box::new),
                        end: end.map(Box::new),
                        step: step.map(Box::new),
                        is_end_exclusive: None,
                    },
                )))
            }
        }

        // PARSE_DATE(format, str) -> target-specific
        "PARSE_DATE" if args.len() == 2 => {
            let format = args.remove(0);
            let str_expr = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // CAST(STRPTIME(str, duck_format) AS DATE)
                    let duck_format = temporal::bq_format_to_duckdb(&format);
                    let strptime = Expression::Function(Box::new(Function::new(
                        "STRPTIME".to_string(),
                        vec![str_expr, duck_format],
                    )));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: strptime,
                        to: DataType::Date,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
                DialectType::Snowflake => {
                    // _POLYGLOT_DATE(str, snowflake_format)
                    // Use marker so Snowflake target transform keeps it as DATE() instead of TO_DATE()
                    let sf_format = temporal::bq_format_to_snowflake(&format);
                    Ok(Expression::Function(Box::new(Function::new(
                        "_POLYGLOT_DATE".to_string(),
                        vec![str_expr, sf_format],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "PARSE_DATE".to_string(),
                    vec![format, str_expr],
                )))),
            }
        }

        // PARSE_DATETIME(format, str) -> target-specific
        "PARSE_DATETIME" if args.len() == 2 => {
            let format = args.remove(0);
            let str_expr = args.remove(0);
            let c_format = temporal::bq_format_to_duckdb(&format);
            match target {
                DialectType::DuckDB => {
                    // DuckDB STRPTIME needs a date-bearing format for DATETIME parsing.
                    let str_with_year = Expression::Concat(Box::new(BinaryOp::new(
                        Expression::string("1970 "),
                        str_expr,
                    )));
                    let format_with_year = Expression::Concat(Box::new(BinaryOp::new(
                        Expression::string("%Y "),
                        c_format,
                    )));
                    Ok(Expression::Function(Box::new(Function::new(
                        "STRPTIME".to_string(),
                        vec![str_with_year, format_with_year],
                    ))))
                }
                DialectType::Snowflake => {
                    // SQLGlot emits PARSE_DATETIME(value, format) with expanded C-style format tokens.
                    Ok(Expression::Function(Box::new(Function::new(
                        "PARSE_DATETIME".to_string(),
                        vec![str_expr, c_format],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "PARSE_DATETIME".to_string(),
                    vec![format, str_expr],
                )))),
            }
        }

        // PARSE_TIMESTAMP(format, str) -> target-specific
        "PARSE_TIMESTAMP" if args.len() >= 2 => {
            let format = args.remove(0);
            let str_expr = args.remove(0);
            let tz = if !args.is_empty() {
                Some(args.remove(0))
            } else {
                None
            };
            match target {
                DialectType::DuckDB => {
                    let duck_format = temporal::bq_format_to_duckdb(&format);
                    let strptime = Expression::Function(Box::new(Function::new(
                        "STRPTIME".to_string(),
                        vec![str_expr, duck_format],
                    )));
                    Ok(strptime)
                }
                _ => {
                    let mut result_args = vec![format, str_expr];
                    if let Some(tz_arg) = tz {
                        result_args.push(tz_arg);
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "PARSE_TIMESTAMP".to_string(),
                        result_args,
                    ))))
                }
            }
        }

        // FORMAT_DATE(format, date) -> target-specific
        "FORMAT_DATE" if args.len() == 2 => {
            let format = args.remove(0);
            let date_expr = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // STRFTIME(CAST(date AS DATE), format)
                    let cast_date = Expression::Cast(Box::new(Cast {
                        this: date_expr,
                        to: DataType::Date,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "STRFTIME".to_string(),
                        vec![cast_date, format],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "FORMAT_DATE".to_string(),
                    vec![format, date_expr],
                )))),
            }
        }

        // FORMAT_DATETIME(format, datetime) -> target-specific
        "FORMAT_DATETIME" if args.len() == 2 => {
            let format = args.remove(0);
            let dt_expr = args.remove(0);

            if matches!(target, DialectType::BigQuery) {
                // BQ->BQ: normalize %H:%M:%S to %T, %x to %D
                let norm_format = temporal::bq_format_normalize_bq(&format);
                // Also strip DATETIME keyword from typed literals
                let norm_dt = match dt_expr {
                    Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Timestamp(_)) => {
                        let Literal::Timestamp(s) = lit.as_ref() else {
                            unreachable!()
                        };
                        Expression::Cast(Box::new(Cast {
                            this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                            to: DataType::Custom {
                                name: "DATETIME".to_string(),
                            },
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        }))
                    }
                    other => other,
                };
                return Ok(Expression::Function(Box::new(Function::new(
                    "FORMAT_DATETIME".to_string(),
                    vec![norm_format, norm_dt],
                ))));
            }

            match target {
                DialectType::DuckDB => {
                    // STRFTIME(CAST(dt AS TIMESTAMP), duckdb_format)
                    let cast_dt = temporal::ensure_cast_timestamp(dt_expr);
                    let duck_format = temporal::bq_format_to_duckdb(&format);
                    Ok(Expression::Function(Box::new(Function::new(
                        "STRFTIME".to_string(),
                        vec![cast_dt, duck_format],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "FORMAT_DATETIME".to_string(),
                    vec![format, dt_expr],
                )))),
            }
        }

        // FORMAT_TIMESTAMP(format, ts) -> target-specific
        "FORMAT_TIMESTAMP" if args.len() == 2 => {
            let format = args.remove(0);
            let ts_expr = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // STRFTIME(CAST(CAST(ts AS TIMESTAMPTZ) AS TIMESTAMP), format)
                    let cast_tstz = temporal::ensure_cast_timestamptz(ts_expr);
                    let cast_ts = Expression::Cast(Box::new(Cast {
                        this: cast_tstz,
                        to: DataType::Timestamp {
                            timezone: false,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "STRFTIME".to_string(),
                        vec![cast_ts, format],
                    ))))
                }
                DialectType::Snowflake => {
                    // TO_CHAR(CAST(CAST(ts AS TIMESTAMPTZ) AS TIMESTAMP), snowflake_format)
                    let cast_tstz = temporal::ensure_cast_timestamptz(ts_expr);
                    let cast_ts = Expression::Cast(Box::new(Cast {
                        this: cast_tstz,
                        to: DataType::Timestamp {
                            timezone: false,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let sf_format = temporal::bq_format_to_snowflake(&format);
                    Ok(Expression::Function(Box::new(Function::new(
                        "TO_CHAR".to_string(),
                        vec![cast_ts, sf_format],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "FORMAT_TIMESTAMP".to_string(),
                    vec![format, ts_expr],
                )))),
            }
        }

        // UNIX_DATE(date) -> DATE_DIFF('DAY', '1970-01-01', date) for DuckDB
        "UNIX_DATE" if args.len() == 1 => {
            let date = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    let epoch = Expression::Cast(Box::new(Cast {
                        this: Expression::Literal(Box::new(Literal::String(
                            "1970-01-01".to_string(),
                        ))),
                        to: DataType::Date,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    // DATE_DIFF('DAY', epoch, date) but date might be DATE '...' literal
                    // Need to convert DATE literal to CAST
                    let norm_date = temporal::date_literal_to_cast(date);
                    Ok(Expression::Function(Box::new(Function::new(
                        "DATE_DIFF".to_string(),
                        vec![
                            Expression::Literal(Box::new(Literal::String("DAY".to_string()))),
                            epoch,
                            norm_date,
                        ],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "UNIX_DATE".to_string(),
                    vec![date],
                )))),
            }
        }

        // UNIX_SECONDS(ts) -> target-specific
        "UNIX_SECONDS" if args.len() == 1 => {
            let ts = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // CAST(EPOCH(CAST(ts AS TIMESTAMPTZ)) AS BIGINT)
                    let norm_ts = temporal::ts_literal_to_cast_tz(ts);
                    let epoch = Expression::Function(Box::new(Function::new(
                        "EPOCH".to_string(),
                        vec![norm_ts],
                    )));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: epoch,
                        to: DataType::BigInt { length: None },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
                DialectType::Snowflake => {
                    // TIMESTAMPDIFF(SECONDS, CAST('1970-01-01 00:00:00+00' AS TIMESTAMPTZ), ts)
                    let epoch = Expression::Cast(Box::new(Cast {
                        this: Expression::Literal(Box::new(Literal::String(
                            "1970-01-01 00:00:00+00".to_string(),
                        ))),
                        to: DataType::Timestamp {
                            timezone: true,
                            precision: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "TIMESTAMPDIFF".to_string(),
                        vec![
                            Expression::Identifier(Identifier::new("SECONDS".to_string())),
                            epoch,
                            ts,
                        ],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "UNIX_SECONDS".to_string(),
                    vec![ts],
                )))),
            }
        }

        // UNIX_MILLIS(ts) -> target-specific
        "UNIX_MILLIS" if args.len() == 1 => {
            let ts = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    let norm_ts = temporal::ts_literal_to_cast_tz(ts);
                    Ok(Expression::Function(Box::new(Function::new(
                        "EPOCH_MS".to_string(),
                        vec![norm_ts],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "UNIX_MILLIS".to_string(),
                    vec![ts],
                )))),
            }
        }

        // UNIX_MICROS(ts) -> target-specific
        "UNIX_MICROS" if args.len() == 1 => {
            let ts = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    let norm_ts = temporal::ts_literal_to_cast_tz(ts);
                    Ok(Expression::Function(Box::new(Function::new(
                        "EPOCH_US".to_string(),
                        vec![norm_ts],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "UNIX_MICROS".to_string(),
                    vec![ts],
                )))),
            }
        }

        // INSTR(str, substr) -> target-specific
        "INSTR" => {
            if matches!(target, DialectType::BigQuery) {
                // BQ->BQ: keep as INSTR
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    args,
                ))))
            } else if matches!(target, DialectType::Snowflake) && args.len() == 2 {
                // Snowflake: CHARINDEX(substr, str) - swap args
                let str_expr = args.remove(0);
                let substr = args.remove(0);
                Ok(Expression::Function(Box::new(Function::new(
                    "CHARINDEX".to_string(),
                    vec![substr, str_expr],
                ))))
            } else {
                // Keep as INSTR for other targets
                Ok(Expression::Function(Box::new(Function::new(
                    "INSTR".to_string(),
                    args,
                ))))
            }
        }

        // CURRENT_TIMESTAMP / CURRENT_DATE handling - parens normalization and timezone
        "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_DATETIME" | "CURRENT_TIME" => {
            if matches!(target, DialectType::BigQuery) {
                // BQ->BQ: always output with parens (function form), keep any timezone arg
                Ok(Expression::Function(Box::new(Function::new(name, args))))
            } else if name == "CURRENT_DATE" && args.len() == 1 {
                // CURRENT_DATE('UTC') - has timezone arg
                let tz_arg = args.remove(0);
                match target {
                    DialectType::DuckDB => {
                        // CAST(CURRENT_TIMESTAMP AT TIME ZONE 'UTC' AS DATE)
                        let ct =
                            Expression::CurrentTimestamp(crate::expressions::CurrentTimestamp {
                                precision: None,
                                sysdate: false,
                            });
                        let at_tz =
                            Expression::AtTimeZone(Box::new(crate::expressions::AtTimeZone {
                                this: ct,
                                zone: tz_arg,
                            }));
                        Ok(Expression::Cast(Box::new(Cast {
                            this: at_tz,
                            to: DataType::Date,
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        })))
                    }
                    DialectType::Snowflake => {
                        // CAST(CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP()) AS DATE)
                        let ct = Expression::Function(Box::new(Function::new(
                            "CURRENT_TIMESTAMP".to_string(),
                            vec![],
                        )));
                        let convert = Expression::Function(Box::new(Function::new(
                            "CONVERT_TIMEZONE".to_string(),
                            vec![tz_arg, ct],
                        )));
                        Ok(Expression::Cast(Box::new(Cast {
                            this: convert,
                            to: DataType::Date,
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        })))
                    }
                    _ => {
                        // PostgreSQL, MySQL, etc.: CURRENT_DATE AT TIME ZONE 'UTC'
                        let cd = Expression::CurrentDate(crate::expressions::CurrentDate);
                        Ok(Expression::AtTimeZone(Box::new(
                            crate::expressions::AtTimeZone {
                                this: cd,
                                zone: tz_arg,
                            },
                        )))
                    }
                }
            } else if (name == "CURRENT_TIMESTAMP"
                || name == "CURRENT_TIME"
                || name == "CURRENT_DATE")
                && args.is_empty()
                && matches!(
                    target,
                    DialectType::PostgreSQL
                        | DialectType::DuckDB
                        | DialectType::Presto
                        | DialectType::Trino
                )
            {
                // These targets want no-parens CURRENT_TIMESTAMP / CURRENT_DATE / CURRENT_TIME
                if name == "CURRENT_TIMESTAMP" {
                    Ok(Expression::CurrentTimestamp(
                        crate::expressions::CurrentTimestamp {
                            precision: None,
                            sysdate: false,
                        },
                    ))
                } else if name == "CURRENT_DATE" {
                    Ok(Expression::CurrentDate(crate::expressions::CurrentDate))
                } else {
                    // CURRENT_TIME
                    Ok(Expression::CurrentTime(crate::expressions::CurrentTime {
                        precision: None,
                    }))
                }
            } else {
                // All other targets: keep as function (with parens)
                Ok(Expression::Function(Box::new(Function::new(name, args))))
            }
        }

        // JSON_QUERY(json, path) -> target-specific
        "JSON_QUERY" if args.len() == 2 => {
            match target {
                DialectType::DuckDB | DialectType::SQLite => {
                    // json -> path syntax
                    let json_expr = args.remove(0);
                    let path = args.remove(0);
                    Ok(Expression::JsonExtract(Box::new(
                        crate::expressions::JsonExtractFunc {
                            this: json_expr,
                            path,
                            returning: None,
                            arrow_syntax: true,
                            hash_arrow_syntax: false,
                            wrapper_option: None,
                            quotes_option: None,
                            on_scalar_string: false,
                            on_error: None,
                        },
                    )))
                }
                DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                    Ok(Expression::Function(Box::new(Function::new(
                        "GET_JSON_OBJECT".to_string(),
                        args,
                    ))))
                }
                DialectType::PostgreSQL | DialectType::Redshift => Ok(Expression::Function(
                    Box::new(Function::new("JSON_EXTRACT_PATH".to_string(), args)),
                )),
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "JSON_QUERY".to_string(),
                    args,
                )))),
            }
        }

        // JSON_VALUE_ARRAY(json, path) -> target-specific
        "JSON_VALUE_ARRAY" if args.len() == 2 => {
            match target {
                DialectType::DuckDB => {
                    // CAST(json -> path AS TEXT[])
                    let json_expr = args.remove(0);
                    let path = args.remove(0);
                    let arrow =
                        Expression::JsonExtract(Box::new(crate::expressions::JsonExtractFunc {
                            this: json_expr,
                            path,
                            returning: None,
                            arrow_syntax: true,
                            hash_arrow_syntax: false,
                            wrapper_option: None,
                            quotes_option: None,
                            on_scalar_string: false,
                            on_error: None,
                        }));
                    Ok(Expression::Cast(Box::new(Cast {
                        this: arrow,
                        to: DataType::Array {
                            element_type: Box::new(DataType::Text),
                            dimension: None,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                }
                DialectType::Snowflake => {
                    let json_expr = args.remove(0);
                    let path_expr = args.remove(0);
                    // Convert JSON path from $.path to just path
                    let sf_path = if let Expression::Literal(ref lit) = path_expr {
                        if let Literal::String(ref s) = lit.as_ref() {
                            let trimmed = s.trim_start_matches('$').trim_start_matches('.');
                            Expression::Literal(Box::new(Literal::String(trimmed.to_string())))
                        } else {
                            path_expr.clone()
                        }
                    } else {
                        path_expr
                    };
                    let parse_json = Expression::Function(Box::new(Function::new(
                        "PARSE_JSON".to_string(),
                        vec![json_expr],
                    )));
                    let get_path = Expression::Function(Box::new(Function::new(
                        "GET_PATH".to_string(),
                        vec![parse_json, sf_path],
                    )));
                    // TRANSFORM(get_path, x -> CAST(x AS VARCHAR))
                    let cast_expr = Expression::Cast(Box::new(Cast {
                        this: Expression::Identifier(Identifier::new("x")),
                        to: DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let lambda = Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                        parameters: vec![Identifier::new("x")],
                        body: cast_expr,
                        colon: false,
                        parameter_types: vec![],
                    }));
                    Ok(Expression::Function(Box::new(Function::new(
                        "TRANSFORM".to_string(),
                        vec![get_path, lambda],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "JSON_VALUE_ARRAY".to_string(),
                    args,
                )))),
            }
        }

        // BigQuery REGEXP_EXTRACT(val, regex[, position[, occurrence]]) -> target dialects
        // BigQuery's 3rd arg is "position" (starting char index), 4th is "occurrence" (which match to return)
        // This is different from Hive/Spark where 3rd arg is "group_index"
        "REGEXP_EXTRACT" if matches!(source, DialectType::BigQuery) => {
            match target {
                DialectType::DuckDB
                | DialectType::Presto
                | DialectType::Trino
                | DialectType::Athena => {
                    if args.len() == 2 {
                        // REGEXP_EXTRACT(val, regex) -> REGEXP_EXTRACT(val, regex, 1)
                        args.push(Expression::number(1));
                        Ok(Expression::Function(Box::new(Function::new(
                            "REGEXP_EXTRACT".to_string(),
                            args,
                        ))))
                    } else if args.len() == 3 {
                        let val = args.remove(0);
                        let regex = args.remove(0);
                        let position = args.remove(0);
                        let is_pos_1 = matches!(&position, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));
                        if is_pos_1 {
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_EXTRACT".to_string(),
                                vec![val, regex, Expression::number(1)],
                            ))))
                        } else {
                            let substring_expr = Expression::Function(Box::new(Function::new(
                                "SUBSTRING".to_string(),
                                vec![val, position],
                            )));
                            let nullif_expr = Expression::Function(Box::new(Function::new(
                                "NULLIF".to_string(),
                                vec![
                                    substring_expr,
                                    Expression::Literal(Box::new(Literal::String(String::new()))),
                                ],
                            )));
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_EXTRACT".to_string(),
                                vec![nullif_expr, regex, Expression::number(1)],
                            ))))
                        }
                    } else if args.len() == 4 {
                        let val = args.remove(0);
                        let regex = args.remove(0);
                        let position = args.remove(0);
                        let occurrence = args.remove(0);
                        let is_pos_1 = matches!(&position, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));
                        let is_occ_1 = matches!(&occurrence, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n == "1"));
                        if is_pos_1 && is_occ_1 {
                            Ok(Expression::Function(Box::new(Function::new(
                                "REGEXP_EXTRACT".to_string(),
                                vec![val, regex, Expression::number(1)],
                            ))))
                        } else {
                            let subject = if is_pos_1 {
                                val
                            } else {
                                let substring_expr = Expression::Function(Box::new(Function::new(
                                    "SUBSTRING".to_string(),
                                    vec![val, position],
                                )));
                                Expression::Function(Box::new(Function::new(
                                    "NULLIF".to_string(),
                                    vec![
                                        substring_expr,
                                        Expression::Literal(Box::new(Literal::String(
                                            String::new(),
                                        ))),
                                    ],
                                )))
                            };
                            let extract_all = Expression::Function(Box::new(Function::new(
                                "REGEXP_EXTRACT_ALL".to_string(),
                                vec![subject, regex, Expression::number(1)],
                            )));
                            Ok(Expression::Function(Box::new(Function::new(
                                "ARRAY_EXTRACT".to_string(),
                                vec![extract_all, occurrence],
                            ))))
                        }
                    } else {
                        Ok(Expression::Function(Box::new(Function {
                            name: f.name,
                            args,
                            distinct: f.distinct,
                            trailing_comments: f.trailing_comments,
                            use_bracket_syntax: f.use_bracket_syntax,
                            no_parens: f.no_parens,
                            quoted: f.quoted,
                            span: None,
                            inferred_type: None,
                        })))
                    }
                }
                DialectType::Snowflake => {
                    // BigQuery REGEXP_EXTRACT -> Snowflake REGEXP_SUBSTR
                    Ok(Expression::Function(Box::new(Function::new(
                        "REGEXP_SUBSTR".to_string(),
                        args,
                    ))))
                }
                _ => {
                    // For other targets (Hive/Spark/BigQuery): pass through as-is
                    // BigQuery's default group behavior matches Hive/Spark for 2-arg case
                    Ok(Expression::Function(Box::new(Function {
                        name: f.name,
                        args,
                        distinct: f.distinct,
                        trailing_comments: f.trailing_comments,
                        use_bracket_syntax: f.use_bracket_syntax,
                        no_parens: f.no_parens,
                        quoted: f.quoted,
                        span: None,
                        inferred_type: None,
                    })))
                }
            }
        }

        // BigQuery STRUCT(args) -> target-specific struct expression
        "STRUCT" => {
            // Convert Function args to Struct fields
            let mut fields: Vec<(Option<String>, Expression)> = Vec::new();
            for (i, arg) in args.into_iter().enumerate() {
                match arg {
                    Expression::Alias(a) => {
                        // Named field: expr AS name
                        fields.push((Some(a.alias.name.clone()), a.this));
                    }
                    other => {
                        // Unnamed field: for Spark/Hive, keep as None
                        // For Snowflake, auto-name as _N
                        // For DuckDB, use column name for column refs, _N for others
                        if matches!(target, DialectType::Snowflake) {
                            fields.push((Some(format!("_{}", i)), other));
                        } else if matches!(target, DialectType::DuckDB) {
                            let auto_name = match &other {
                                Expression::Column(col) => col.name.name.clone(),
                                _ => format!("_{}", i),
                            };
                            fields.push((Some(auto_name), other));
                        } else {
                            fields.push((None, other));
                        }
                    }
                }
            }

            match target {
                DialectType::Snowflake => {
                    // OBJECT_CONSTRUCT('name', value, ...)
                    let mut oc_args = Vec::new();
                    for (name, val) in &fields {
                        if let Some(n) = name {
                            oc_args.push(Expression::Literal(Box::new(Literal::String(n.clone()))));
                            oc_args.push(val.clone());
                        } else {
                            oc_args.push(val.clone());
                        }
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "OBJECT_CONSTRUCT".to_string(),
                        oc_args,
                    ))))
                }
                DialectType::DuckDB => {
                    // {'name': value, ...}
                    Ok(Expression::Struct(Box::new(crate::expressions::Struct {
                        fields,
                    })))
                }
                DialectType::Hive => {
                    // STRUCT(val1, val2, ...) - strip aliases
                    let hive_fields: Vec<(Option<String>, Expression)> =
                        fields.into_iter().map(|(_, v)| (None, v)).collect();
                    Ok(Expression::Struct(Box::new(crate::expressions::Struct {
                        fields: hive_fields,
                    })))
                }
                DialectType::Spark | DialectType::Databricks => {
                    // Use Expression::Struct to bypass Spark target transform auto-naming
                    Ok(Expression::Struct(Box::new(crate::expressions::Struct {
                        fields,
                    })))
                }
                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                    // Check if all fields are named AND all have inferable types - if so, wrap in CAST(ROW(...) AS ROW(name TYPE, ...))
                    let all_named =
                        !fields.is_empty() && fields.iter().all(|(name, _)| name.is_some());
                    let all_types_inferable = all_named
                        && fields
                            .iter()
                            .all(|(_, val)| types::can_infer_presto_type(val));
                    let row_args: Vec<Expression> = fields.iter().map(|(_, v)| v.clone()).collect();
                    let row_expr =
                        Expression::Function(Box::new(Function::new("ROW".to_string(), row_args)));
                    if all_named && all_types_inferable {
                        // Build ROW type with inferred types
                        let mut row_type_fields = Vec::new();
                        for (name, val) in &fields {
                            if let Some(n) = name {
                                let type_str = types::infer_sql_type_for_presto(val);
                                row_type_fields.push(crate::expressions::StructField::new(
                                    n.clone(),
                                    crate::expressions::DataType::Custom { name: type_str },
                                ));
                            }
                        }
                        let row_type = crate::expressions::DataType::Struct {
                            fields: row_type_fields,
                            nested: true,
                        };
                        Ok(Expression::Cast(Box::new(Cast {
                            this: row_expr,
                            to: row_type,
                            trailing_comments: Vec::new(),
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        })))
                    } else {
                        Ok(row_expr)
                    }
                }
                _ => {
                    // Default: keep as STRUCT function with original args
                    let mut new_args = Vec::new();
                    for (name, val) in fields {
                        if let Some(n) = name {
                            new_args.push(Expression::Alias(Box::new(
                                crate::expressions::Alias::new(val, Identifier::new(n)),
                            )));
                        } else {
                            new_args.push(val);
                        }
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "STRUCT".to_string(),
                        new_args,
                    ))))
                }
            }
        }

        // ROUND(x, n, 'ROUND_HALF_EVEN') -> ROUND_EVEN(x, n) for DuckDB
        "ROUND" if args.len() == 3 => {
            let x = args.remove(0);
            let n = args.remove(0);
            let mode = args.remove(0);
            // Check if mode is 'ROUND_HALF_EVEN'
            let is_half_even = matches!(&mode, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(s) if s.eq_ignore_ascii_case("ROUND_HALF_EVEN")));
            if is_half_even && matches!(target, DialectType::DuckDB) {
                Ok(Expression::Function(Box::new(Function::new(
                    "ROUND_EVEN".to_string(),
                    vec![x, n],
                ))))
            } else {
                // Pass through with all args
                Ok(Expression::Function(Box::new(Function::new(
                    "ROUND".to_string(),
                    vec![x, n, mode],
                ))))
            }
        }

        // MAKE_INTERVAL(year, month, named_args...) -> INTERVAL string for Snowflake/DuckDB
        "MAKE_INTERVAL" => {
            // MAKE_INTERVAL(1, 2, minute => 5, day => 3)
            // The positional args are: year, month
            // Named args are: day =>, minute =>, etc.
            // For Snowflake: INTERVAL '1 year, 2 month, 5 minute, 3 day'
            // For DuckDB: INTERVAL '1 year 2 month 5 minute 3 day'
            // For BigQuery->BigQuery: reorder named args (day before minute)
            if matches!(target, DialectType::Snowflake | DialectType::DuckDB) {
                let mut parts: Vec<(String, String)> = Vec::new();
                let mut pos_idx = 0;
                let pos_units = ["year", "month"];
                for arg in &args {
                    if let Expression::NamedArgument(na) = arg {
                        // Named arg like minute => 5
                        let unit = na.name.name.clone();
                        if let Expression::Literal(lit) = &na.value {
                            if let Literal::Number(n) = lit.as_ref() {
                                parts.push((unit, n.clone()));
                            }
                        }
                    } else if pos_idx < pos_units.len() {
                        if let Expression::Literal(lit) = arg {
                            if let Literal::Number(n) = lit.as_ref() {
                                parts.push((pos_units[pos_idx].to_string(), n.clone()));
                            }
                        }
                        pos_idx += 1;
                    }
                }
                // Don't sort - preserve original argument order
                let separator = if matches!(target, DialectType::Snowflake) {
                    ", "
                } else {
                    " "
                };
                let interval_str = parts
                    .iter()
                    .map(|(u, v)| format!("{} {}", v, u))
                    .collect::<Vec<_>>()
                    .join(separator);
                Ok(Expression::Interval(Box::new(
                    crate::expressions::Interval {
                        this: Some(Expression::Literal(Box::new(Literal::String(interval_str)))),
                        unit: None,
                    },
                )))
            } else if matches!(target, DialectType::BigQuery) {
                // BigQuery->BigQuery: reorder named args (day, minute, etc.)
                let mut positional = Vec::new();
                let mut named: Vec<(String, Expression, crate::expressions::NamedArgSeparator)> =
                    Vec::new();
                let _pos_units = ["year", "month"];
                let mut _pos_idx = 0;
                for arg in args {
                    if let Expression::NamedArgument(na) = arg {
                        named.push((na.name.name.clone(), na.value, na.separator));
                    } else {
                        positional.push(arg);
                        _pos_idx += 1;
                    }
                }
                // Sort named args by: day, hour, minute, second
                let unit_order = |u: &str| -> usize {
                    match u.to_ascii_lowercase().as_str() {
                        "day" => 0,
                        "hour" => 1,
                        "minute" => 2,
                        "second" => 3,
                        _ => 4,
                    }
                };
                named.sort_by_key(|(u, _, _)| unit_order(u));
                let mut result_args = positional;
                for (name, value, sep) in named {
                    result_args.push(Expression::NamedArgument(Box::new(
                        crate::expressions::NamedArgument {
                            name: Identifier::new(&name),
                            value,
                            separator: sep,
                        },
                    )));
                }
                Ok(Expression::Function(Box::new(Function::new(
                    "MAKE_INTERVAL".to_string(),
                    result_args,
                ))))
            } else {
                Ok(Expression::Function(Box::new(Function::new(
                    "MAKE_INTERVAL".to_string(),
                    args,
                ))))
            }
        }

        // ARRAY_TO_STRING(array, sep, null_text) -> ARRAY_TO_STRING(LIST_TRANSFORM(array, x -> COALESCE(x, null_text)), sep) for DuckDB
        "ARRAY_TO_STRING" if args.len() == 3 => {
            let arr = args.remove(0);
            let sep = args.remove(0);
            let null_text = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // LIST_TRANSFORM(array, x -> COALESCE(x, null_text))
                    let _lambda_param =
                        Expression::Identifier(crate::expressions::Identifier::new("x"));
                    let coalesce = Expression::Coalesce(Box::new(crate::expressions::VarArgFunc {
                        original_name: None,
                        expressions: vec![
                            Expression::Identifier(crate::expressions::Identifier::new("x")),
                            null_text,
                        ],
                        inferred_type: None,
                    }));
                    let lambda = Expression::Lambda(Box::new(crate::expressions::LambdaExpr {
                        parameters: vec![crate::expressions::Identifier::new("x")],
                        body: coalesce,
                        colon: false,
                        parameter_types: vec![],
                    }));
                    let list_transform = Expression::Function(Box::new(Function::new(
                        "LIST_TRANSFORM".to_string(),
                        vec![arr, lambda],
                    )));
                    Ok(Expression::Function(Box::new(Function::new(
                        "ARRAY_TO_STRING".to_string(),
                        vec![list_transform, sep],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_TO_STRING".to_string(),
                    vec![arr, sep, null_text],
                )))),
            }
        }

        // LENGTH(x) -> CASE TYPEOF(x) ... for DuckDB
        "LENGTH" if args.len() == 1 => {
            let arg = args.remove(0);
            match target {
                DialectType::DuckDB => {
                    // CASE TYPEOF(foo) WHEN 'BLOB' THEN OCTET_LENGTH(CAST(foo AS BLOB)) ELSE LENGTH(CAST(foo AS TEXT)) END
                    let typeof_func = Expression::Function(Box::new(Function::new(
                        "TYPEOF".to_string(),
                        vec![arg.clone()],
                    )));
                    let blob_cast = Expression::Cast(Box::new(Cast {
                        this: arg.clone(),
                        to: DataType::VarBinary { length: None },
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let octet_length = Expression::Function(Box::new(Function::new(
                        "OCTET_LENGTH".to_string(),
                        vec![blob_cast],
                    )));
                    let text_cast = Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Text,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let length_text = Expression::Function(Box::new(Function::new(
                        "LENGTH".to_string(),
                        vec![text_cast],
                    )));
                    Ok(Expression::Case(Box::new(crate::expressions::Case {
                        operand: Some(typeof_func),
                        whens: vec![(
                            Expression::Literal(Box::new(Literal::String("BLOB".to_string()))),
                            octet_length,
                        )],
                        else_: Some(length_text),
                        comments: Vec::new(),
                        inferred_type: None,
                    })))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "LENGTH".to_string(),
                    vec![arg],
                )))),
            }
        }

        // PERCENTILE_CONT(x, fraction RESPECT NULLS) -> QUANTILE_CONT(x, fraction) for DuckDB
        "PERCENTILE_CONT" if args.len() >= 2 && matches!(source, DialectType::BigQuery) => {
            // BigQuery PERCENTILE_CONT(x, fraction [RESPECT|IGNORE NULLS]) OVER ()
            // The args should be [x, fraction] with the null handling stripped
            // For DuckDB: QUANTILE_CONT(x, fraction)
            // For Spark: PERCENTILE_CONT(x, fraction) RESPECT NULLS (handled at window level)
            match target {
                DialectType::DuckDB => {
                    // Strip down to just 2 args, rename to QUANTILE_CONT
                    let x = args[0].clone();
                    let frac = args[1].clone();
                    Ok(Expression::Function(Box::new(Function::new(
                        "QUANTILE_CONT".to_string(),
                        vec![x, frac],
                    ))))
                }
                _ => Ok(Expression::Function(Box::new(Function::new(
                    "PERCENTILE_CONT".to_string(),
                    args,
                )))),
            }
        }

        // All others: pass through
        _ => Ok(Expression::Function(Box::new(Function {
            name: f.name,
            args,
            distinct: f.distinct,
            trailing_comments: f.trailing_comments,
            use_bracket_syntax: f.use_bracket_syntax,
            no_parens: f.no_parens,
            quoted: f.quoted,
            span: None,
            inferred_type: None,
        }))),
    }
}

pub(super) fn expr_to_string_static(expr: &Expression) -> String {
    use crate::expressions::Literal;
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(_)) => {
            let Literal::Number(s) = lit.as_ref() else {
                unreachable!()
            };
            s.clone()
        }
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
            let Literal::String(s) = lit.as_ref() else {
                unreachable!()
            };
            s.clone()
        }
        Expression::Identifier(id) => id.name.clone(),
        Expression::Neg(f) => format!("-{}", expr_to_string_static(&f.this)),
        _ => "1".to_string(),
    }
}

pub(super) fn expr_to_string(expr: &Expression) -> String {
    use crate::expressions::Literal;
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(_)) => {
            let Literal::Number(s) = lit.as_ref() else {
                unreachable!()
            };
            s.clone()
        }
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
            let Literal::String(s) = lit.as_ref() else {
                unreachable!()
            };
            s.clone()
        }
        Expression::Neg(f) => format!("-{}", expr_to_string(&f.this)),
        Expression::Identifier(id) => id.name.clone(),
        _ => "1".to_string(),
    }
}
