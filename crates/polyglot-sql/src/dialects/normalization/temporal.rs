use super::postgres_interval::{
    self, DecomposeOutcome, ParsedInterval, MICROS_PER_DAY, MICROS_PER_HOUR, MICROS_PER_MINUTE,
    MICROS_PER_SECOND,
};
use super::{scalar, types, NormalizationContext, RewriteOutcome};
use crate::dialects::{Dialect, DialectType};
use crate::error::Error;
use crate::error::Result;
use crate::expressions::*;

#[derive(Debug)]
pub(super) enum Action {
    PostgresDatePartForTsql,
    ConvertTimezoneToExpr,
    EpochConvert,
    EpochMsConvert,
    DatePartUnquote,
    AtTimeZoneConvert,
    DayOfWeekConvert,
    TruncToDateTrunc,
    MonthsBetweenConvert,
    AddMonthsConvert,
    DateBinConvert,
    TsOrDsToDateConvert,
    TsOrDsToDateStrConvert,
    DateStrToDateConvert,
    TimeStrToDateConvert,
    TimeStrToTimeConvert,
    DateToDateStrConvert,
    DateToDiConvert,
    DiToDateConvert,
    TsOrDiToDiConvert,
    UnixToStrConvert,
    UnixToTimeConvert,
    UnixToTimeStrConvert,
    TimeToUnixConvert,
    TimeToStrConvert,
    StrToUnixConvert,
    DateTruncSwapArgs,
    TimestampTruncConvert,
    StrToDateConvert,
    TsOrDsAddConvert,
    DateFromUnixDateConvert,
    TimeStrToUnixConvert,
    TimeToTimeStrConvert,
    WeekOfYearToWeekIso,
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
            Action::PostgresDatePartForTsql => rewrite_postgres_date_part_for_tsql(e),
            Action::ConvertTimezoneToExpr => {
                // Convert Function("CONVERT_TIMEZONE", args) to Expression::ConvertTimezone
                // This prevents Redshift's transform_expr from expanding 2-arg to 3-arg with 'UTC'
                if let Expression::Function(f) = e {
                    if f.args.len() == 2 {
                        let mut args = f.args;
                        let target_tz = args.remove(0);
                        let timestamp = args.remove(0);
                        Ok(Expression::ConvertTimezone(Box::new(ConvertTimezone {
                            source_tz: None,
                            target_tz: Some(Box::new(target_tz)),
                            timestamp: Some(Box::new(timestamp)),
                            options: vec![],
                        })))
                    } else if f.args.len() == 3 {
                        let mut args = f.args;
                        let source_tz = args.remove(0);
                        let target_tz = args.remove(0);
                        let timestamp = args.remove(0);
                        Ok(Expression::ConvertTimezone(Box::new(ConvertTimezone {
                            source_tz: Some(Box::new(source_tz)),
                            target_tz: Some(Box::new(target_tz)),
                            timestamp: Some(Box::new(timestamp)),
                            options: vec![],
                        })))
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::EpochConvert => {
                if let Expression::Epoch(f) = e {
                    let arg = f.this;
                    let name = match target {
                        DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                            "UNIX_TIMESTAMP"
                        }
                        DialectType::Presto | DialectType::Trino => "TO_UNIXTIME",
                        DialectType::BigQuery => "TIME_TO_UNIX",
                        _ => "EPOCH",
                    };
                    Ok(Expression::Function(Box::new(Function::new(
                        name.to_string(),
                        vec![arg],
                    ))))
                } else {
                    Ok(e)
                }
            }
            Action::EpochMsConvert => {
                use crate::expressions::{BinaryOp, Cast};
                if let Expression::EpochMs(f) = e {
                    let arg = f.this;
                    match target {
                        DialectType::Spark | DialectType::Databricks => Ok(Expression::Function(
                            Box::new(Function::new("TIMESTAMP_MILLIS".to_string(), vec![arg])),
                        )),
                        DialectType::BigQuery => Ok(Expression::Function(Box::new(Function::new(
                            "TIMESTAMP_MILLIS".to_string(),
                            vec![arg],
                        )))),
                        DialectType::Presto | DialectType::Trino => {
                            // FROM_UNIXTIME(CAST(x AS DOUBLE) / POW(10, 3))
                            let cast_arg = Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Double {
                                    precision: None,
                                    scale: None,
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            let div = Expression::Div(Box::new(BinaryOp::new(
                                cast_arg,
                                Expression::Function(Box::new(Function::new(
                                    "POW".to_string(),
                                    vec![Expression::number(10), Expression::number(3)],
                                ))),
                            )));
                            Ok(Expression::Function(Box::new(Function::new(
                                "FROM_UNIXTIME".to_string(),
                                vec![div],
                            ))))
                        }
                        DialectType::MySQL => {
                            // FROM_UNIXTIME(x / POWER(10, 3))
                            let div = Expression::Div(Box::new(BinaryOp::new(
                                arg,
                                Expression::Function(Box::new(Function::new(
                                    "POWER".to_string(),
                                    vec![Expression::number(10), Expression::number(3)],
                                ))),
                            )));
                            Ok(Expression::Function(Box::new(Function::new(
                                "FROM_UNIXTIME".to_string(),
                                vec![div],
                            ))))
                        }
                        DialectType::PostgreSQL | DialectType::Redshift => {
                            // TO_TIMESTAMP(CAST(x AS DOUBLE PRECISION) / POWER(10, 3))
                            let cast_arg = Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Custom {
                                    name: "DOUBLE PRECISION".to_string(),
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            let div = Expression::Div(Box::new(BinaryOp::new(
                                cast_arg,
                                Expression::Function(Box::new(Function::new(
                                    "POWER".to_string(),
                                    vec![Expression::number(10), Expression::number(3)],
                                ))),
                            )));
                            Ok(Expression::Function(Box::new(Function::new(
                                "TO_TIMESTAMP".to_string(),
                                vec![div],
                            ))))
                        }
                        DialectType::ClickHouse => {
                            // fromUnixTimestamp64Milli(CAST(x AS Nullable(Int64)))
                            let cast_arg = Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Nullable {
                                    inner: Box::new(DataType::BigInt { length: None }),
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "fromUnixTimestamp64Milli".to_string(),
                                vec![cast_arg],
                            ))))
                        }
                        _ => Ok(Expression::Function(Box::new(Function::new(
                            "EPOCH_MS".to_string(),
                            vec![arg],
                        )))),
                    }
                } else {
                    Ok(e)
                }
            }
            Action::DatePartUnquote => {
                // DATE_PART('month', x) -> DATE_PART(month, x) for Snowflake target
                // Convert the quoted string first arg to a bare Column/Identifier
                if let Expression::Function(mut f) = e {
                    if let Some(Expression::Literal(lit)) = f.args.first() {
                        if let crate::expressions::Literal::String(s) = lit.as_ref() {
                            let bare_name = s.to_ascii_lowercase();
                            f.args[0] = Expression::Column(Box::new(crate::expressions::Column {
                                name: Identifier::new(bare_name),
                                table: None,
                                join_mark: false,
                                trailing_comments: Vec::new(),
                                span: None,
                                inferred_type: None,
                            }));
                        }
                    }
                    Ok(Expression::Function(f))
                } else {
                    Ok(e)
                }
            }
            Action::AtTimeZoneConvert => {
                // AT TIME ZONE -> target-specific conversion
                if let Expression::AtTimeZone(atz) = e {
                    match target {
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            Ok(Expression::Function(Box::new(Function::new(
                                "AT_TIMEZONE".to_string(),
                                vec![atz.this, atz.zone],
                            ))))
                        }
                        DialectType::Spark | DialectType::Databricks => {
                            Ok(Expression::Function(Box::new(Function::new(
                                "FROM_UTC_TIMESTAMP".to_string(),
                                vec![atz.this, atz.zone],
                            ))))
                        }
                        DialectType::Snowflake => {
                            // CONVERT_TIMEZONE('zone', expr)
                            Ok(Expression::Function(Box::new(Function::new(
                                "CONVERT_TIMEZONE".to_string(),
                                vec![atz.zone, atz.this],
                            ))))
                        }
                        DialectType::BigQuery => {
                            // TIMESTAMP(DATETIME(expr, 'zone'))
                            let datetime_call = Expression::Function(Box::new(Function::new(
                                "DATETIME".to_string(),
                                vec![atz.this, atz.zone],
                            )));
                            Ok(Expression::Function(Box::new(Function::new(
                                "TIMESTAMP".to_string(),
                                vec![datetime_call],
                            ))))
                        }
                        _ => Ok(Expression::Function(Box::new(Function::new(
                            "AT_TIMEZONE".to_string(),
                            vec![atz.this, atz.zone],
                        )))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::DayOfWeekConvert => {
                // DAY_OF_WEEK -> ISODOW for DuckDB, ((DAYOFWEEK(x) % 7) + 1) for Spark
                if let Expression::DayOfWeek(f) = e {
                    match target {
                        DialectType::DuckDB => Ok(Expression::Function(Box::new(Function::new(
                            "ISODOW".to_string(),
                            vec![f.this],
                        )))),
                        DialectType::Spark | DialectType::Databricks => {
                            // ((DAYOFWEEK(x) % 7) + 1)
                            let dayofweek = Expression::Function(Box::new(Function::new(
                                "DAYOFWEEK".to_string(),
                                vec![f.this],
                            )));
                            let modulo = Expression::Mod(Box::new(BinaryOp {
                                left: dayofweek,
                                right: Expression::number(7),
                                left_comments: Vec::new(),
                                operator_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                                inferred_type: None,
                            }));
                            let paren_mod = Expression::Paren(Box::new(Paren {
                                this: modulo,
                                trailing_comments: Vec::new(),
                            }));
                            let add_one = Expression::Add(Box::new(BinaryOp {
                                left: paren_mod,
                                right: Expression::number(1),
                                left_comments: Vec::new(),
                                operator_comments: Vec::new(),
                                trailing_comments: Vec::new(),
                                inferred_type: None,
                            }));
                            Ok(Expression::Paren(Box::new(Paren {
                                this: add_one,
                                trailing_comments: Vec::new(),
                            })))
                        }
                        _ => Ok(Expression::DayOfWeek(f)),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TruncToDateTrunc => {
                // TRUNC(timestamp, 'MONTH') -> DATE_TRUNC('MONTH', timestamp)
                if let Expression::Function(f) = e {
                    if f.args.len() == 2 {
                        let timestamp = f.args[0].clone();
                        let unit_expr = f.args[1].clone();

                        if matches!(target, DialectType::ClickHouse) {
                            // For ClickHouse, produce Expression::DateTrunc which the generator
                            // outputs as DATE_TRUNC(...) without going through the ClickHouse
                            // target transform that would convert it to dateTrunc
                            let unit_str = get_unit_str_static(&unit_expr);
                            let dt_field = match unit_str.as_str() {
                                "YEAR" => DateTimeField::Year,
                                "MONTH" => DateTimeField::Month,
                                "DAY" => DateTimeField::Day,
                                "HOUR" => DateTimeField::Hour,
                                "MINUTE" => DateTimeField::Minute,
                                "SECOND" => DateTimeField::Second,
                                "WEEK" => DateTimeField::Week,
                                "QUARTER" => DateTimeField::Quarter,
                                _ => DateTimeField::Custom(unit_str),
                            };
                            Ok(Expression::DateTrunc(Box::new(
                                crate::expressions::DateTruncFunc {
                                    this: timestamp,
                                    unit: dt_field,
                                },
                            )))
                        } else {
                            let new_args = vec![unit_expr, timestamp];
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_TRUNC".to_string(),
                                new_args,
                            ))))
                        }
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::MonthsBetweenConvert => {
                if let Expression::MonthsBetween(mb) = e {
                    let crate::expressions::BinaryFunc {
                        this: end_date,
                        expression: start_date,
                        ..
                    } = *mb;
                    match target {
                        DialectType::DuckDB => {
                            let cast_end = ensure_cast_date(end_date);
                            let cast_start = ensure_cast_date(start_date);
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
                            let last_day_end = Expression::Function(Box::new(Function::new(
                                "LAST_DAY".to_string(),
                                vec![cast_end.clone()],
                            )));
                            let last_day_start = Expression::Function(Box::new(Function::new(
                                "LAST_DAY".to_string(),
                                vec![cast_start.clone()],
                            )));
                            let day_last_end = Expression::Function(Box::new(Function::new(
                                "DAY".to_string(),
                                vec![last_day_end],
                            )));
                            let day_last_start = Expression::Function(Box::new(Function::new(
                                "DAY".to_string(),
                                vec![last_day_start],
                            )));
                            let cond1 = Expression::Eq(Box::new(BinaryOp::new(
                                day_end.clone(),
                                day_last_end,
                            )));
                            let cond2 = Expression::Eq(Box::new(BinaryOp::new(
                                day_start.clone(),
                                day_last_start,
                            )));
                            let both_cond = Expression::And(Box::new(BinaryOp::new(cond1, cond2)));
                            let day_diff =
                                Expression::Sub(Box::new(BinaryOp::new(day_end, day_start)));
                            let day_diff_paren =
                                Expression::Paren(Box::new(crate::expressions::Paren {
                                    this: day_diff,
                                    trailing_comments: Vec::new(),
                                }));
                            let frac = Expression::Div(Box::new(BinaryOp::new(
                                day_diff_paren,
                                Expression::Literal(Box::new(Literal::Number("31.0".to_string()))),
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
                            let unit = Expression::Identifier(Identifier::new("MONTH"));
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATEDIFF".to_string(),
                                vec![unit, start_date, end_date],
                            ))))
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_DIFF".to_string(),
                                vec![Expression::string("MONTH"), start_date, end_date],
                            ))))
                        }
                        _ => Ok(Expression::MonthsBetween(Box::new(
                            crate::expressions::BinaryFunc {
                                this: end_date,
                                expression: start_date,
                                original_name: None,
                                inferred_type: None,
                            },
                        ))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::AddMonthsConvert => {
                if let Expression::AddMonths(am) = e {
                    let date = am.this;
                    let val = am.expression;
                    match target {
                        DialectType::TSQL | DialectType::Fabric => {
                            let cast_date = ensure_cast_datetime2(date);
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATEADD".to_string(),
                                vec![
                                    Expression::Identifier(Identifier::new("MONTH")),
                                    val,
                                    cast_date,
                                ],
                            ))))
                        }
                        DialectType::DuckDB if matches!(source, DialectType::Snowflake) => {
                            // DuckDB ADD_MONTHS from Snowflake: CASE WHEN LAST_DAY(date) = date THEN LAST_DAY(date + interval) ELSE date + interval END
                            // Optionally wrapped in CAST(... AS type) if the input had a specific type

                            // Determine the cast type from the date expression
                            let (cast_date, return_type) = match &date {
                                Expression::Literal(lit)
                                    if matches!(lit.as_ref(), Literal::String(_)) =>
                                {
                                    // String literal: CAST(str AS TIMESTAMP), no outer CAST
                                    (
                                        Expression::Cast(Box::new(Cast {
                                            this: date.clone(),
                                            to: DataType::Timestamp {
                                                precision: None,
                                                timezone: false,
                                            },
                                            trailing_comments: Vec::new(),
                                            double_colon_syntax: false,
                                            format: None,
                                            default: None,
                                            inferred_type: None,
                                        })),
                                        None,
                                    )
                                }
                                Expression::Cast(c) => {
                                    // Already cast (e.g., '2023-01-31'::DATE) - keep the cast, wrap result in CAST(... AS type)
                                    (date.clone(), Some(c.to.clone()))
                                }
                                _ => {
                                    // Expression or NULL::TYPE - keep as-is, check for cast type
                                    if let Expression::Cast(c) = &date {
                                        (date.clone(), Some(c.to.clone()))
                                    } else {
                                        (date.clone(), None)
                                    }
                                }
                            };

                            // Build the interval expression
                            // For non-integer values (float, decimal, cast), use TO_MONTHS(CAST(ROUND(val) AS INT))
                            // For integer values, use INTERVAL val MONTH
                            let is_non_integer_val = match &val {
                                Expression::Literal(lit)
                                    if matches!(lit.as_ref(), Literal::Number(_)) =>
                                {
                                    let Literal::Number(n) = lit.as_ref() else {
                                        unreachable!()
                                    };
                                    n.contains('.')
                                }
                                Expression::Cast(_) => true, // e.g., 3.2::DECIMAL(10,2)
                                Expression::Neg(n) => {
                                    if let Expression::Literal(lit) = &n.this {
                                        if let Literal::Number(s) = lit.as_ref() {
                                            s.contains('.')
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    }
                                }
                                _ => false,
                            };

                            let add_interval = if is_non_integer_val {
                                // TO_MONTHS(CAST(ROUND(val) AS INT))
                                let round_val = Expression::Function(Box::new(Function::new(
                                    "ROUND".to_string(),
                                    vec![val.clone()],
                                )));
                                let cast_int = Expression::Cast(Box::new(Cast {
                                    this: round_val,
                                    to: DataType::Int {
                                        length: None,
                                        integer_spelling: false,
                                    },
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                Expression::Function(Box::new(Function::new(
                                    "TO_MONTHS".to_string(),
                                    vec![cast_int],
                                )))
                            } else {
                                // INTERVAL val MONTH
                                // For negative numbers, wrap in parens
                                let interval_val = match &val {
                                    Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n.starts_with('-')) =>
                                    {
                                        let Literal::Number(_) = lit.as_ref() else {
                                            unreachable!()
                                        };
                                        Expression::Paren(Box::new(Paren {
                                            this: val.clone(),
                                            trailing_comments: Vec::new(),
                                        }))
                                    }
                                    Expression::Neg(_) => Expression::Paren(Box::new(Paren {
                                        this: val.clone(),
                                        trailing_comments: Vec::new(),
                                    })),
                                    Expression::Null(_) => Expression::Paren(Box::new(Paren {
                                        this: val.clone(),
                                        trailing_comments: Vec::new(),
                                    })),
                                    _ => val.clone(),
                                };
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(interval_val),
                                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                        unit: crate::expressions::IntervalUnit::Month,
                                        use_plural: false,
                                    }),
                                }))
                            };

                            // Build: date + interval
                            let date_plus_interval = Expression::Add(Box::new(BinaryOp::new(
                                cast_date.clone(),
                                add_interval.clone(),
                            )));

                            // Build LAST_DAY(date)
                            let last_day_date = Expression::Function(Box::new(Function::new(
                                "LAST_DAY".to_string(),
                                vec![cast_date.clone()],
                            )));

                            // Build LAST_DAY(date + interval)
                            let last_day_date_plus = Expression::Function(Box::new(Function::new(
                                "LAST_DAY".to_string(),
                                vec![date_plus_interval.clone()],
                            )));

                            // Build: CASE WHEN LAST_DAY(date) = date THEN LAST_DAY(date + interval) ELSE date + interval END
                            let case_expr = Expression::Case(Box::new(Case {
                                operand: None,
                                whens: vec![(
                                    Expression::Eq(Box::new(BinaryOp::new(
                                        last_day_date,
                                        cast_date.clone(),
                                    ))),
                                    last_day_date_plus,
                                )],
                                else_: Some(date_plus_interval),
                                comments: Vec::new(),
                                inferred_type: None,
                            }));

                            // Wrap in CAST(... AS type) if needed
                            if let Some(dt) = return_type {
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: case_expr,
                                    to: dt,
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                Ok(case_expr)
                            }
                        }
                        DialectType::DuckDB => {
                            // Non-Snowflake source: simple date + INTERVAL
                            let cast_date = if matches!(&date, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                            {
                                Expression::Cast(Box::new(Cast {
                                    this: date,
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
                                date
                            };
                            let interval =
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(val),
                                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                        unit: crate::expressions::IntervalUnit::Month,
                                        use_plural: false,
                                    }),
                                }));
                            Ok(Expression::Add(Box::new(BinaryOp::new(
                                cast_date, interval,
                            ))))
                        }
                        DialectType::Snowflake => {
                            // Keep ADD_MONTHS when source is also Snowflake
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
                        DialectType::Redshift => Ok(Expression::Function(Box::new(Function::new(
                            "DATEADD".to_string(),
                            vec![Expression::Identifier(Identifier::new("MONTH")), val, date],
                        )))),
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            let cast_date = if matches!(&date, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                            {
                                Expression::Cast(Box::new(Cast {
                                    this: date,
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
                                date
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_ADD".to_string(),
                                vec![Expression::string("MONTH"), val, cast_date],
                            ))))
                        }
                        DialectType::BigQuery => {
                            let interval =
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(val),
                                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                        unit: crate::expressions::IntervalUnit::Month,
                                        use_plural: false,
                                    }),
                                }));
                            let cast_date = if matches!(&date, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                            {
                                Expression::Cast(Box::new(Cast {
                                    this: date,
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
                                date
                            };
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_ADD".to_string(),
                                vec![cast_date, interval],
                            ))))
                        }
                        DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                            Ok(Expression::Function(Box::new(Function::new(
                                "ADD_MONTHS".to_string(),
                                vec![date, val],
                            ))))
                        }
                        _ => {
                            // Default: keep as AddMonths expression
                            Ok(Expression::AddMonths(Box::new(
                                crate::expressions::BinaryFunc {
                                    this: date,
                                    expression: val,
                                    original_name: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::DateBinConvert => {
                // DATE_BIN('interval', ts, origin) -> TIME_BUCKET('interval', ts, origin) for DuckDB
                if let Expression::Function(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "TIME_BUCKET".to_string(),
                        f.args,
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::TsOrDsToDateConvert => {
                // TS_OR_DS_TO_DATE(x[, fmt]) -> dialect-specific date conversion
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let this = args.remove(0);
                    let fmt = if !args.is_empty() {
                        match &args[0] {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                Some(s.clone())
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    Ok(Expression::TsOrDsToDate(Box::new(
                        crate::expressions::TsOrDsToDate {
                            this: Box::new(this),
                            format: fmt,
                            safe: None,
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::TsOrDsToDateStrConvert => {
                // TS_OR_DS_TO_DATE_STR(x) -> SUBSTRING(CAST(x AS type), 1, 10)
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    let str_type = match target {
                        DialectType::DuckDB
                        | DialectType::PostgreSQL
                        | DialectType::Materialize => DataType::Text,
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                            DataType::Custom {
                                name: "STRING".to_string(),
                            }
                        }
                        DialectType::Presto
                        | DialectType::Trino
                        | DialectType::Athena
                        | DialectType::Drill => DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                        DialectType::MySQL | DialectType::Doris | DialectType::StarRocks => {
                            DataType::Custom {
                                name: "STRING".to_string(),
                            }
                        }
                        _ => DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                    };
                    let cast_expr = Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: str_type,
                        double_colon_syntax: false,
                        trailing_comments: Vec::new(),
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    Ok(Expression::Substring(Box::new(
                        crate::expressions::SubstringFunc {
                            this: cast_expr,
                            start: Expression::number(1),
                            length: Some(Expression::number(10)),
                            from_for_syntax: false,
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::DateStrToDateConvert => {
                // DATE_STR_TO_DATE(x) -> dialect-specific
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    match target {
                        DialectType::SQLite => {
                            // SQLite: just the bare expression (dates are strings)
                            Ok(arg)
                        }
                        _ => Ok(Expression::Cast(Box::new(Cast {
                            this: arg,
                            to: DataType::Date,
                            double_colon_syntax: false,
                            trailing_comments: Vec::new(),
                            format: None,
                            default: None,
                            inferred_type: None,
                        }))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TimeStrToDateConvert => {
                // TIME_STR_TO_DATE(x) -> dialect-specific
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    match target {
                        DialectType::Hive
                        | DialectType::Doris
                        | DialectType::StarRocks
                        | DialectType::Snowflake => Ok(Expression::Function(Box::new(
                            Function::new("TO_DATE".to_string(), vec![arg]),
                        ))),
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // Presto: CAST(x AS TIMESTAMP)
                            Ok(Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Timestamp {
                                    timezone: false,
                                    precision: None,
                                },
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        _ => {
                            // Default: CAST(x AS DATE)
                            Ok(Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Date,
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TimeStrToTimeConvert => {
                // TIME_STR_TO_TIME(x[, zone]) -> dialect-specific CAST to timestamp type
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let this = args.remove(0);
                    let zone = if !args.is_empty() {
                        match &args[0] {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                Some(s.clone())
                            }
                            _ => None,
                        }
                    } else {
                        None
                    };
                    let has_zone = zone.is_some();

                    match target {
                        DialectType::SQLite => {
                            // SQLite: just the bare expression
                            Ok(this)
                        }
                        DialectType::MySQL => {
                            if has_zone {
                                // MySQL with zone: TIMESTAMP(x)
                                Ok(Expression::Function(Box::new(Function::new(
                                    "TIMESTAMP".to_string(),
                                    vec![this],
                                ))))
                            } else {
                                // MySQL: CAST(x AS DATETIME) or with precision
                                // Use DataType::Custom to avoid MySQL's transform_cast converting
                                // CAST(x AS TIMESTAMP) -> TIMESTAMP(x)
                                let precision = if let Expression::Literal(ref lit) = this {
                                    if let Literal::String(ref s) = lit.as_ref() {
                                        if let Some(dot_pos) = s.rfind('.') {
                                            let frac = &s[dot_pos + 1..];
                                            let digit_count = frac
                                                .chars()
                                                .take_while(|c| c.is_ascii_digit())
                                                .count();
                                            if digit_count > 0 {
                                                Some(digit_count)
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };
                                let type_name = match precision {
                                    Some(p) => format!("DATETIME({})", p),
                                    None => "DATETIME".to_string(),
                                };
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Custom { name: type_name },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        DialectType::ClickHouse => {
                            if has_zone {
                                // ClickHouse with zone: CAST(x AS DateTime64(6, 'zone'))
                                // We need to strip the timezone offset from the literal if present
                                let clean_this = if let Expression::Literal(ref lit) = this {
                                    if let Literal::String(ref s) = lit.as_ref() {
                                        // Strip timezone offset like "-08:00" or "+00:00"
                                        let re_offset = s.rfind(|c: char| c == '+' || c == '-');
                                        if let Some(offset_pos) = re_offset {
                                            if offset_pos > 10 {
                                                // After the date part
                                                let trimmed = s[..offset_pos].to_string();
                                                Expression::Literal(Box::new(Literal::String(
                                                    trimmed,
                                                )))
                                            } else {
                                                this.clone()
                                            }
                                        } else {
                                            this.clone()
                                        }
                                    } else {
                                        this.clone()
                                    }
                                } else {
                                    this.clone()
                                };
                                let zone_str = zone.unwrap();
                                // Build: CAST(x AS DateTime64(6, 'zone'))
                                let type_name = format!("DateTime64(6, '{}')", zone_str);
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: clean_this,
                                    to: DataType::Custom { name: type_name },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Custom {
                                        name: "DateTime64(6)".to_string(),
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        DialectType::BigQuery => {
                            if has_zone {
                                // BigQuery with zone: CAST(x AS TIMESTAMP)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: false,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                // BigQuery: CAST(x AS DATETIME) - Timestamp{tz:false} renders as DATETIME for BigQuery
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Custom {
                                        name: "DATETIME".to_string(),
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        DialectType::Doris => {
                            // Doris: CAST(x AS DATETIME)
                            Ok(Expression::Cast(Box::new(Cast {
                                this,
                                to: DataType::Custom {
                                    name: "DATETIME".to_string(),
                                },
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        DialectType::TSQL | DialectType::Fabric => {
                            if has_zone {
                                // TSQL with zone: CAST(x AS DATETIMEOFFSET) AT TIME ZONE 'UTC'
                                let cast_expr = Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Custom {
                                        name: "DATETIMEOFFSET".to_string(),
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                Ok(Expression::AtTimeZone(Box::new(
                                    crate::expressions::AtTimeZone {
                                        this: cast_expr,
                                        zone: Expression::Literal(Box::new(Literal::String(
                                            "UTC".to_string(),
                                        ))),
                                    },
                                )))
                            } else {
                                // TSQL: CAST(x AS DATETIME2)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Custom {
                                        name: "DATETIME2".to_string(),
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        DialectType::DuckDB => {
                            if has_zone {
                                // DuckDB with zone: CAST(x AS TIMESTAMPTZ)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: true,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                // DuckDB: CAST(x AS TIMESTAMP)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: false,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        DialectType::PostgreSQL
                        | DialectType::Materialize
                        | DialectType::RisingWave => {
                            if has_zone {
                                // PostgreSQL with zone: CAST(x AS TIMESTAMPTZ)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: true,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                // PostgreSQL: CAST(x AS TIMESTAMP)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: false,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        DialectType::Snowflake => {
                            if has_zone {
                                // Snowflake with zone: CAST(x AS TIMESTAMPTZ)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: true,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                // Snowflake: CAST(x AS TIMESTAMP)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: false,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            if has_zone {
                                // Presto/Trino with zone: CAST(x AS TIMESTAMP WITH TIME ZONE)
                                // Check for precision from sub-second digits
                                let precision = if let Expression::Literal(ref lit) = this {
                                    if let Literal::String(ref s) = lit.as_ref() {
                                        if let Some(dot_pos) = s.rfind('.') {
                                            let frac = &s[dot_pos + 1..];
                                            let digit_count = frac
                                                .chars()
                                                .take_while(|c| c.is_ascii_digit())
                                                .count();
                                            if digit_count > 0
                                                && matches!(target, DialectType::Trino)
                                            {
                                                Some(digit_count as u32)
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };
                                let dt = if let Some(prec) = precision {
                                    DataType::Timestamp {
                                        timezone: true,
                                        precision: Some(prec),
                                    }
                                } else {
                                    DataType::Timestamp {
                                        timezone: true,
                                        precision: None,
                                    }
                                };
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: dt,
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                // Check for sub-second precision for Trino
                                let precision = if let Expression::Literal(ref lit) = this {
                                    if let Literal::String(ref s) = lit.as_ref() {
                                        if let Some(dot_pos) = s.rfind('.') {
                                            let frac = &s[dot_pos + 1..];
                                            let digit_count = frac
                                                .chars()
                                                .take_while(|c| c.is_ascii_digit())
                                                .count();
                                            if digit_count > 0
                                                && matches!(target, DialectType::Trino)
                                            {
                                                Some(digit_count as u32)
                                            } else {
                                                None
                                            }
                                        } else {
                                            None
                                        }
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                };
                                let dt = DataType::Timestamp {
                                    timezone: false,
                                    precision,
                                };
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: dt,
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        DialectType::Redshift => {
                            if has_zone {
                                // Redshift with zone: CAST(x AS TIMESTAMP WITH TIME ZONE)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: true,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                // Redshift: CAST(x AS TIMESTAMP)
                                Ok(Expression::Cast(Box::new(Cast {
                                    this,
                                    to: DataType::Timestamp {
                                        timezone: false,
                                        precision: None,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        }
                        _ => {
                            // Default: CAST(x AS TIMESTAMP)
                            Ok(Expression::Cast(Box::new(Cast {
                                this,
                                to: DataType::Timestamp {
                                    timezone: false,
                                    precision: None,
                                },
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::DateToDateStrConvert => {
                // DATE_TO_DATE_STR(x) -> CAST(x AS text_type) per dialect
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    let str_type = match target {
                        DialectType::DuckDB => DataType::Text,
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                            DataType::Custom {
                                name: "STRING".to_string(),
                            }
                        }
                        DialectType::Presto
                        | DialectType::Trino
                        | DialectType::Athena
                        | DialectType::Drill => DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                        _ => DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                    };
                    Ok(Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: str_type,
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

            Action::DateToDiConvert => {
                // DATE_TO_DI(x) -> CAST(format_func(x, fmt) AS INT)
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    let inner = match target {
                        DialectType::DuckDB => {
                            // STRFTIME(x, '%Y%m%d')
                            Expression::Function(Box::new(Function::new(
                                "STRFTIME".to_string(),
                                vec![arg, Expression::string("%Y%m%d")],
                            )))
                        }
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                            // DATE_FORMAT(x, 'yyyyMMdd')
                            Expression::Function(Box::new(Function::new(
                                "DATE_FORMAT".to_string(),
                                vec![arg, Expression::string("yyyyMMdd")],
                            )))
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // DATE_FORMAT(x, '%Y%m%d')
                            Expression::Function(Box::new(Function::new(
                                "DATE_FORMAT".to_string(),
                                vec![arg, Expression::string("%Y%m%d")],
                            )))
                        }
                        DialectType::Drill => {
                            // TO_DATE(x, 'yyyyMMdd')
                            Expression::Function(Box::new(Function::new(
                                "TO_DATE".to_string(),
                                vec![arg, Expression::string("yyyyMMdd")],
                            )))
                        }
                        _ => {
                            // Default: STRFTIME(x, '%Y%m%d')
                            Expression::Function(Box::new(Function::new(
                                "STRFTIME".to_string(),
                                vec![arg, Expression::string("%Y%m%d")],
                            )))
                        }
                    };
                    // Use INT (not INTEGER) for Presto/Trino
                    let int_type = match target {
                        DialectType::Presto
                        | DialectType::Trino
                        | DialectType::Athena
                        | DialectType::TSQL
                        | DialectType::Fabric
                        | DialectType::SQLite
                        | DialectType::Redshift => DataType::Custom {
                            name: "INT".to_string(),
                        },
                        _ => DataType::Int {
                            length: None,
                            integer_spelling: false,
                        },
                    };
                    Ok(Expression::Cast(Box::new(Cast {
                        this: inner,
                        to: int_type,
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

            Action::DiToDateConvert => {
                // DI_TO_DATE(x) -> dialect-specific integer-to-date conversion
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    match target {
                        DialectType::DuckDB => {
                            // CAST(STRPTIME(CAST(x AS TEXT), '%Y%m%d') AS DATE)
                            let cast_text = Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Text,
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            let strptime = Expression::Function(Box::new(Function::new(
                                "STRPTIME".to_string(),
                                vec![cast_text, Expression::string("%Y%m%d")],
                            )));
                            Ok(Expression::Cast(Box::new(Cast {
                                this: strptime,
                                to: DataType::Date,
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                            // TO_DATE(CAST(x AS STRING), 'yyyyMMdd')
                            let cast_str = Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Custom {
                                    name: "STRING".to_string(),
                                },
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "TO_DATE".to_string(),
                                vec![cast_str, Expression::string("yyyyMMdd")],
                            ))))
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // CAST(DATE_PARSE(CAST(x AS VARCHAR), '%Y%m%d') AS DATE)
                            let cast_varchar = Expression::Cast(Box::new(Cast {
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
                            }));
                            let date_parse = Expression::Function(Box::new(Function::new(
                                "DATE_PARSE".to_string(),
                                vec![cast_varchar, Expression::string("%Y%m%d")],
                            )));
                            Ok(Expression::Cast(Box::new(Cast {
                                this: date_parse,
                                to: DataType::Date,
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        DialectType::Drill => {
                            // TO_DATE(CAST(x AS VARCHAR), 'yyyyMMdd')
                            let cast_varchar = Expression::Cast(Box::new(Cast {
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
                            }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "TO_DATE".to_string(),
                                vec![cast_varchar, Expression::string("yyyyMMdd")],
                            ))))
                        }
                        _ => Ok(Expression::Function(Box::new(Function::new(
                            "DI_TO_DATE".to_string(),
                            vec![arg],
                        )))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TsOrDiToDiConvert => {
                // TS_OR_DI_TO_DI(x) -> CAST(SUBSTR(REPLACE(CAST(x AS type), '-', ''), 1, 8) AS INT)
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    let str_type = match target {
                        DialectType::DuckDB => DataType::Text,
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                            DataType::Custom {
                                name: "STRING".to_string(),
                            }
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            DataType::VarChar {
                                length: None,
                                parenthesized_length: false,
                            }
                        }
                        _ => DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                    };
                    let cast_str = Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: str_type,
                        double_colon_syntax: false,
                        trailing_comments: Vec::new(),
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let replace_expr = Expression::Function(Box::new(Function::new(
                        "REPLACE".to_string(),
                        vec![cast_str, Expression::string("-"), Expression::string("")],
                    )));
                    let substr_name = match target {
                        DialectType::DuckDB
                        | DialectType::Hive
                        | DialectType::Spark
                        | DialectType::Databricks => "SUBSTR",
                        _ => "SUBSTR",
                    };
                    let substr = Expression::Function(Box::new(Function::new(
                        substr_name.to_string(),
                        vec![replace_expr, Expression::number(1), Expression::number(8)],
                    )));
                    // Use INT (not INTEGER) for Presto/Trino etc.
                    let int_type = match target {
                        DialectType::Presto
                        | DialectType::Trino
                        | DialectType::Athena
                        | DialectType::TSQL
                        | DialectType::Fabric
                        | DialectType::SQLite
                        | DialectType::Redshift => DataType::Custom {
                            name: "INT".to_string(),
                        },
                        _ => DataType::Int {
                            length: None,
                            integer_spelling: false,
                        },
                    };
                    Ok(Expression::Cast(Box::new(Cast {
                        this: substr,
                        to: int_type,
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

            Action::UnixToStrConvert => {
                // UNIX_TO_STR(x, fmt) -> convert to Expression::UnixToStr for generator
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let this = args.remove(0);
                    let fmt_expr = if !args.is_empty() {
                        Some(args.remove(0))
                    } else {
                        None
                    };

                    // Check if format is a string literal
                    let fmt_str = fmt_expr.as_ref().and_then(|f| {
                        if let Expression::Literal(lit) = f {
                            if let Literal::String(s) = lit.as_ref() {
                                Some(s.clone())
                            } else {
                                None
                            }
                        } else {
                            None
                        }
                    });

                    if let Some(fmt_string) = fmt_str {
                        // String literal format -> use UnixToStr expression (generator handles it)
                        Ok(Expression::UnixToStr(Box::new(
                            crate::expressions::UnixToStr {
                                this: Box::new(this),
                                format: Some(fmt_string),
                            },
                        )))
                    } else if let Some(fmt_e) = fmt_expr {
                        // Non-literal format (e.g., identifier `y`) -> build target expression directly
                        match target {
                            DialectType::DuckDB => {
                                // STRFTIME(TO_TIMESTAMP(x), y)
                                let to_ts = Expression::Function(Box::new(Function::new(
                                    "TO_TIMESTAMP".to_string(),
                                    vec![this],
                                )));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "STRFTIME".to_string(),
                                    vec![to_ts, fmt_e],
                                ))))
                            }
                            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                // DATE_FORMAT(FROM_UNIXTIME(x), y)
                                let from_unix = Expression::Function(Box::new(Function::new(
                                    "FROM_UNIXTIME".to_string(),
                                    vec![this],
                                )));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_FORMAT".to_string(),
                                    vec![from_unix, fmt_e],
                                ))))
                            }
                            DialectType::Hive
                            | DialectType::Spark
                            | DialectType::Databricks
                            | DialectType::Doris
                            | DialectType::StarRocks => {
                                // FROM_UNIXTIME(x, y)
                                Ok(Expression::Function(Box::new(Function::new(
                                    "FROM_UNIXTIME".to_string(),
                                    vec![this, fmt_e],
                                ))))
                            }
                            _ => {
                                // Default: keep as UNIX_TO_STR(x, y)
                                Ok(Expression::Function(Box::new(Function::new(
                                    "UNIX_TO_STR".to_string(),
                                    vec![this, fmt_e],
                                ))))
                            }
                        }
                    } else {
                        Ok(Expression::UnixToStr(Box::new(
                            crate::expressions::UnixToStr {
                                this: Box::new(this),
                                format: None,
                            },
                        )))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::UnixToTimeConvert => {
                // UNIX_TO_TIME(x) -> convert to Expression::UnixToTime for generator
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    Ok(Expression::UnixToTime(Box::new(
                        crate::expressions::UnixToTime {
                            this: Box::new(arg),
                            scale: None,
                            zone: None,
                            hours: None,
                            minutes: None,
                            format: None,
                            target_type: None,
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::UnixToTimeStrConvert => {
                // UNIX_TO_TIME_STR(x) -> dialect-specific
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    match target {
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                            // FROM_UNIXTIME(x)
                            Ok(Expression::Function(Box::new(Function::new(
                                "FROM_UNIXTIME".to_string(),
                                vec![arg],
                            ))))
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // CAST(FROM_UNIXTIME(x) AS VARCHAR)
                            let from_unix = Expression::Function(Box::new(Function::new(
                                "FROM_UNIXTIME".to_string(),
                                vec![arg],
                            )));
                            Ok(Expression::Cast(Box::new(Cast {
                                this: from_unix,
                                to: DataType::VarChar {
                                    length: None,
                                    parenthesized_length: false,
                                },
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        DialectType::DuckDB => {
                            // CAST(TO_TIMESTAMP(x) AS TEXT)
                            let to_ts = Expression::Function(Box::new(Function::new(
                                "TO_TIMESTAMP".to_string(),
                                vec![arg],
                            )));
                            Ok(Expression::Cast(Box::new(Cast {
                                this: to_ts,
                                to: DataType::Text,
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            })))
                        }
                        _ => Ok(Expression::Function(Box::new(Function::new(
                            "UNIX_TO_TIME_STR".to_string(),
                            vec![arg],
                        )))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TimeToUnixConvert => {
                // TIME_TO_UNIX(x) -> convert to Expression::TimeToUnix for generator
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    Ok(Expression::TimeToUnix(Box::new(
                        crate::expressions::UnaryFunc {
                            this: arg,
                            original_name: None,
                            inferred_type: None,
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::TimeToStrConvert => {
                // TIME_TO_STR(x, fmt) -> convert to Expression::TimeToStr for generator
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let this = args.remove(0);
                    let fmt = match args.remove(0) {
                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
                            let Literal::String(s) = lit.as_ref() else {
                                unreachable!()
                            };
                            s.clone()
                        }
                        other => {
                            return Ok(Expression::Function(Box::new(Function::new(
                                "TIME_TO_STR".to_string(),
                                vec![this, other],
                            ))));
                        }
                    };
                    Ok(Expression::TimeToStr(Box::new(
                        crate::expressions::TimeToStr {
                            this: Box::new(this),
                            format: fmt,
                            culture: None,
                            zone: None,
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::StrToUnixConvert => {
                // STR_TO_UNIX(x, fmt) -> convert to Expression::StrToUnix for generator
                if let Expression::Function(f) = e {
                    let mut args = f.args;
                    let this = args.remove(0);
                    let fmt = match args.remove(0) {
                        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
                            let Literal::String(s) = lit.as_ref() else {
                                unreachable!()
                            };
                            s.clone()
                        }
                        other => {
                            return Ok(Expression::Function(Box::new(Function::new(
                                "STR_TO_UNIX".to_string(),
                                vec![this, other],
                            ))));
                        }
                    };
                    Ok(Expression::StrToUnix(Box::new(
                        crate::expressions::StrToUnix {
                            this: Some(Box::new(this)),
                            format: Some(fmt),
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::DateTruncSwapArgs => {
                // DATE_TRUNC('unit', x) from Generic -> target-specific
                if let Expression::Function(f) = e {
                    if f.args.len() == 2 {
                        let unit_arg = f.args[0].clone();
                        let expr_arg = f.args[1].clone();
                        // Extract unit string from the first arg
                        let unit_str = match &unit_arg {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                s.to_ascii_uppercase()
                            }
                            _ => return Ok(Expression::Function(f)),
                        };
                        match target {
                            DialectType::BigQuery => {
                                // BigQuery: DATE_TRUNC(x, UNIT) - unquoted unit
                                let unit_ident =
                                    Expression::Column(Box::new(crate::expressions::Column {
                                        name: crate::expressions::Identifier::new(unit_str),
                                        table: None,
                                        join_mark: false,
                                        trailing_comments: Vec::new(),
                                        span: None,
                                        inferred_type: None,
                                    }));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_string(),
                                    vec![expr_arg, unit_ident],
                                ))))
                            }
                            DialectType::Doris => {
                                // Doris: DATE_TRUNC(x, 'UNIT')
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_string(),
                                    vec![expr_arg, Expression::string(&unit_str)],
                                ))))
                            }
                            DialectType::StarRocks => {
                                // StarRocks: DATE_TRUNC('UNIT', x) - keep standard order
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_string(),
                                    vec![Expression::string(&unit_str), expr_arg],
                                ))))
                            }
                            DialectType::Spark | DialectType::Databricks => {
                                // Spark: TRUNC(x, 'UNIT')
                                Ok(Expression::Function(Box::new(Function::new(
                                    "TRUNC".to_string(),
                                    vec![expr_arg, Expression::string(&unit_str)],
                                ))))
                            }
                            DialectType::MySQL => {
                                // MySQL: complex expansion based on unit
                                date_trunc_to_mysql(&unit_str, &expr_arg)
                            }
                            _ => Ok(Expression::Function(f)),
                        }
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TimestampTruncConvert => {
                // TIMESTAMP_TRUNC(x, UNIT[, tz]) from Generic -> target-specific
                if let Expression::Function(f) = e {
                    if f.args.len() >= 2 {
                        let expr_arg = f.args[0].clone();
                        let unit_arg = f.args[1].clone();
                        let tz_arg = if f.args.len() >= 3 {
                            Some(f.args[2].clone())
                        } else {
                            None
                        };
                        // Extract unit string
                        let unit_str = match &unit_arg {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                s.to_ascii_uppercase()
                            }
                            Expression::Column(c) => c.name.name.to_ascii_uppercase(),
                            _ => {
                                return Ok(Expression::Function(f));
                            }
                        };
                        match target {
                            DialectType::Spark | DialectType::Databricks => {
                                // Spark: DATE_TRUNC('UNIT', x)
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_string(),
                                    vec![Expression::string(&unit_str), expr_arg],
                                ))))
                            }
                            DialectType::Doris | DialectType::StarRocks => {
                                // Doris: DATE_TRUNC(x, 'UNIT')
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_string(),
                                    vec![expr_arg, Expression::string(&unit_str)],
                                ))))
                            }
                            DialectType::BigQuery => {
                                // BigQuery: TIMESTAMP_TRUNC(x, UNIT) - keep but with unquoted unit
                                let unit_ident =
                                    Expression::Column(Box::new(crate::expressions::Column {
                                        name: crate::expressions::Identifier::new(unit_str),
                                        table: None,
                                        join_mark: false,
                                        trailing_comments: Vec::new(),
                                        span: None,
                                        inferred_type: None,
                                    }));
                                let mut args = vec![expr_arg, unit_ident];
                                if let Some(tz) = tz_arg {
                                    args.push(tz);
                                }
                                Ok(Expression::Function(Box::new(Function::new(
                                    "TIMESTAMP_TRUNC".to_string(),
                                    args,
                                ))))
                            }
                            DialectType::DuckDB => {
                                // DuckDB with timezone: DATE_TRUNC('UNIT', x AT TIME ZONE 'tz') AT TIME ZONE 'tz'
                                if let Some(tz) = tz_arg {
                                    let tz_str = match &tz {
                                        Expression::Literal(lit)
                                            if matches!(lit.as_ref(), Literal::String(_)) =>
                                        {
                                            let Literal::String(s) = lit.as_ref() else {
                                                unreachable!()
                                            };
                                            s.clone()
                                        }
                                        _ => "UTC".to_string(),
                                    };
                                    // x AT TIME ZONE 'tz'
                                    let at_tz = Expression::AtTimeZone(Box::new(
                                        crate::expressions::AtTimeZone {
                                            this: expr_arg,
                                            zone: Expression::string(&tz_str),
                                        },
                                    ));
                                    // DATE_TRUNC('UNIT', x AT TIME ZONE 'tz')
                                    let trunc = Expression::Function(Box::new(Function::new(
                                        "DATE_TRUNC".to_string(),
                                        vec![Expression::string(&unit_str), at_tz],
                                    )));
                                    // DATE_TRUNC(...) AT TIME ZONE 'tz'
                                    Ok(Expression::AtTimeZone(Box::new(
                                        crate::expressions::AtTimeZone {
                                            this: trunc,
                                            zone: Expression::string(&tz_str),
                                        },
                                    )))
                                } else {
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "DATE_TRUNC".to_string(),
                                        vec![Expression::string(&unit_str), expr_arg],
                                    ))))
                                }
                            }
                            DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Athena
                            | DialectType::Snowflake => {
                                // Presto/Snowflake: DATE_TRUNC('UNIT', x) - drop timezone
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_string(),
                                    vec![Expression::string(&unit_str), expr_arg],
                                ))))
                            }
                            _ => {
                                // For most dialects: DATE_TRUNC('UNIT', x) + tz handling
                                let mut args = vec![Expression::string(&unit_str), expr_arg];
                                if let Some(tz) = tz_arg {
                                    args.push(tz);
                                }
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_string(),
                                    args,
                                ))))
                            }
                        }
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::StrToDateConvert => {
                // STR_TO_DATE(x, fmt) from Generic -> dialect-specific date parsing
                if let Expression::Function(f) = e {
                    if f.args.len() == 2 {
                        let mut args = f.args;
                        let this = args.remove(0);
                        let fmt_expr = args.remove(0);
                        let fmt_str = match &fmt_expr {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                Some(s.clone())
                            }
                            _ => None,
                        };
                        let default_date = "%Y-%m-%d";
                        let default_time = "%Y-%m-%d %H:%M:%S";
                        let is_default = fmt_str
                            .as_ref()
                            .map_or(false, |f| f == default_date || f == default_time);

                        if is_default {
                            // Default format: handle per-dialect
                            match target {
                                DialectType::MySQL
                                | DialectType::Doris
                                | DialectType::StarRocks => {
                                    // Keep STR_TO_DATE(x, fmt) as-is
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "STR_TO_DATE".to_string(),
                                        vec![this, fmt_expr],
                                    ))))
                                }
                                DialectType::Hive => {
                                    // Hive: CAST(x AS DATE)
                                    Ok(Expression::Cast(Box::new(Cast {
                                        this,
                                        to: DataType::Date,
                                        double_colon_syntax: false,
                                        trailing_comments: Vec::new(),
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                    // Presto: CAST(DATE_PARSE(x, '%Y-%m-%d') AS DATE)
                                    let date_parse = Expression::Function(Box::new(Function::new(
                                        "DATE_PARSE".to_string(),
                                        vec![this, fmt_expr],
                                    )));
                                    Ok(Expression::Cast(Box::new(Cast {
                                        this: date_parse,
                                        to: DataType::Date,
                                        double_colon_syntax: false,
                                        trailing_comments: Vec::new(),
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                _ => {
                                    // Others: TsOrDsToDate (delegates to generator)
                                    Ok(Expression::TsOrDsToDate(Box::new(
                                        crate::expressions::TsOrDsToDate {
                                            this: Box::new(this),
                                            format: None,
                                            safe: None,
                                        },
                                    )))
                                }
                            }
                        } else if let Some(fmt) = fmt_str {
                            match target {
                                DialectType::Doris
                                | DialectType::StarRocks
                                | DialectType::MySQL => {
                                    // Keep STR_TO_DATE but with normalized format (%H:%M:%S -> %T, %-d -> %e)
                                    let mut normalized = fmt.clone();
                                    normalized = normalized.replace("%-d", "%e");
                                    normalized = normalized.replace("%-m", "%c");
                                    normalized = normalized.replace("%H:%M:%S", "%T");
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "STR_TO_DATE".to_string(),
                                        vec![this, Expression::string(&normalized)],
                                    ))))
                                }
                                DialectType::Hive => {
                                    // Hive: CAST(FROM_UNIXTIME(UNIX_TIMESTAMP(x, java_fmt)) AS DATE)
                                    let java_fmt =
                                        crate::generator::Generator::strftime_to_java_format_static(
                                            &fmt,
                                        );
                                    let unix_ts = Expression::Function(Box::new(Function::new(
                                        "UNIX_TIMESTAMP".to_string(),
                                        vec![this, Expression::string(&java_fmt)],
                                    )));
                                    let from_unix = Expression::Function(Box::new(Function::new(
                                        "FROM_UNIXTIME".to_string(),
                                        vec![unix_ts],
                                    )));
                                    Ok(Expression::Cast(Box::new(Cast {
                                        this: from_unix,
                                        to: DataType::Date,
                                        double_colon_syntax: false,
                                        trailing_comments: Vec::new(),
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    })))
                                }
                                DialectType::Spark | DialectType::Databricks => {
                                    // Spark: TO_DATE(x, java_fmt)
                                    let java_fmt =
                                        crate::generator::Generator::strftime_to_java_format_static(
                                            &fmt,
                                        );
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_DATE".to_string(),
                                        vec![this, Expression::string(&java_fmt)],
                                    ))))
                                }
                                DialectType::Drill => {
                                    // Drill: TO_DATE(x, java_fmt) with T quoted as 'T' in Java format
                                    // The generator's string literal escaping will double the quotes: 'T' -> ''T''
                                    let java_fmt =
                                        crate::generator::Generator::strftime_to_java_format_static(
                                            &fmt,
                                        );
                                    let java_fmt = java_fmt.replace('T', "'T'");
                                    Ok(Expression::Function(Box::new(Function::new(
                                        "TO_DATE".to_string(),
                                        vec![this, Expression::string(&java_fmt)],
                                    ))))
                                }
                                _ => {
                                    // For other dialects: use TsOrDsToDate which delegates to generator
                                    Ok(Expression::TsOrDsToDate(Box::new(
                                        crate::expressions::TsOrDsToDate {
                                            this: Box::new(this),
                                            format: Some(fmt),
                                            safe: None,
                                        },
                                    )))
                                }
                            }
                        } else {
                            // Non-string format - keep as-is
                            let mut new_args = Vec::new();
                            new_args.push(this);
                            new_args.push(fmt_expr);
                            Ok(Expression::Function(Box::new(Function::new(
                                "STR_TO_DATE".to_string(),
                                new_args,
                            ))))
                        }
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TsOrDsAddConvert => {
                // TS_OR_DS_ADD(x, n, 'UNIT') from Generic -> dialect-specific DATE_ADD
                if let Expression::Function(f) = e {
                    if f.args.len() == 3 {
                        let mut args = f.args;
                        let x = args.remove(0);
                        let n = args.remove(0);
                        let unit_expr = args.remove(0);
                        let unit_str = match &unit_expr {
                            Expression::Literal(lit)
                                if matches!(lit.as_ref(), Literal::String(_)) =>
                            {
                                let Literal::String(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                s.to_ascii_uppercase()
                            }
                            _ => "DAY".to_string(),
                        };

                        match target {
                            DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                                // DATE_ADD(x, n) - only supports DAY unit
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_ADD".to_string(),
                                    vec![x, n],
                                ))))
                            }
                            DialectType::MySQL => {
                                // DATE_ADD(x, INTERVAL n UNIT)
                                let iu = match unit_str.as_str() {
                                    "YEAR" => crate::expressions::IntervalUnit::Year,
                                    "QUARTER" => crate::expressions::IntervalUnit::Quarter,
                                    "MONTH" => crate::expressions::IntervalUnit::Month,
                                    "WEEK" => crate::expressions::IntervalUnit::Week,
                                    "HOUR" => crate::expressions::IntervalUnit::Hour,
                                    "MINUTE" => crate::expressions::IntervalUnit::Minute,
                                    "SECOND" => crate::expressions::IntervalUnit::Second,
                                    _ => crate::expressions::IntervalUnit::Day,
                                };
                                let interval =
                                    Expression::Interval(Box::new(crate::expressions::Interval {
                                        this: Some(n),
                                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                            unit: iu,
                                            use_plural: false,
                                        }),
                                    }));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_ADD".to_string(),
                                    vec![x, interval],
                                ))))
                            }
                            DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                                // DATE_ADD('UNIT', n, CAST(CAST(x AS TIMESTAMP) AS DATE))
                                let cast_ts = Expression::Cast(Box::new(Cast {
                                    this: x,
                                    to: DataType::Timestamp {
                                        precision: None,
                                        timezone: false,
                                    },
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                let cast_date = Expression::Cast(Box::new(Cast {
                                    this: cast_ts,
                                    to: DataType::Date,
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_ADD".to_string(),
                                    vec![Expression::string(&unit_str), n, cast_date],
                                ))))
                            }
                            DialectType::DuckDB => {
                                // CAST(x AS DATE) + INTERVAL n UNIT
                                let cast_date = Expression::Cast(Box::new(Cast {
                                    this: x,
                                    to: DataType::Date,
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                let iu = match unit_str.as_str() {
                                    "YEAR" => crate::expressions::IntervalUnit::Year,
                                    "QUARTER" => crate::expressions::IntervalUnit::Quarter,
                                    "MONTH" => crate::expressions::IntervalUnit::Month,
                                    "WEEK" => crate::expressions::IntervalUnit::Week,
                                    "HOUR" => crate::expressions::IntervalUnit::Hour,
                                    "MINUTE" => crate::expressions::IntervalUnit::Minute,
                                    "SECOND" => crate::expressions::IntervalUnit::Second,
                                    _ => crate::expressions::IntervalUnit::Day,
                                };
                                let interval =
                                    Expression::Interval(Box::new(crate::expressions::Interval {
                                        this: Some(n),
                                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                            unit: iu,
                                            use_plural: false,
                                        }),
                                    }));
                                Ok(Expression::Add(Box::new(crate::expressions::BinaryOp {
                                    left: cast_date,
                                    right: interval,
                                    left_comments: Vec::new(),
                                    operator_comments: Vec::new(),
                                    trailing_comments: Vec::new(),
                                    inferred_type: None,
                                })))
                            }
                            DialectType::Drill => {
                                // DATE_ADD(CAST(x AS DATE), INTERVAL n UNIT)
                                let cast_date = Expression::Cast(Box::new(Cast {
                                    this: x,
                                    to: DataType::Date,
                                    double_colon_syntax: false,
                                    trailing_comments: Vec::new(),
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                let iu = match unit_str.as_str() {
                                    "YEAR" => crate::expressions::IntervalUnit::Year,
                                    "QUARTER" => crate::expressions::IntervalUnit::Quarter,
                                    "MONTH" => crate::expressions::IntervalUnit::Month,
                                    "WEEK" => crate::expressions::IntervalUnit::Week,
                                    "HOUR" => crate::expressions::IntervalUnit::Hour,
                                    "MINUTE" => crate::expressions::IntervalUnit::Minute,
                                    "SECOND" => crate::expressions::IntervalUnit::Second,
                                    _ => crate::expressions::IntervalUnit::Day,
                                };
                                let interval =
                                    Expression::Interval(Box::new(crate::expressions::Interval {
                                        this: Some(n),
                                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                            unit: iu,
                                            use_plural: false,
                                        }),
                                    }));
                                Ok(Expression::Function(Box::new(Function::new(
                                    "DATE_ADD".to_string(),
                                    vec![cast_date, interval],
                                ))))
                            }
                            _ => {
                                // Default: keep as TS_OR_DS_ADD
                                Ok(Expression::Function(Box::new(Function::new(
                                    "TS_OR_DS_ADD".to_string(),
                                    vec![x, n, unit_expr],
                                ))))
                            }
                        }
                    } else {
                        Ok(Expression::Function(f))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::DateFromUnixDateConvert => {
                // DATE_FROM_UNIX_DATE(n) -> DATEADD(DAY, n, CAST('1970-01-01' AS DATE))
                if let Expression::Function(f) = e {
                    // Keep as-is for dialects that support DATE_FROM_UNIX_DATE natively
                    if matches!(
                        target,
                        DialectType::Spark | DialectType::Databricks | DialectType::BigQuery
                    ) {
                        return Ok(Expression::Function(Box::new(Function::new(
                            "DATE_FROM_UNIX_DATE".to_string(),
                            f.args,
                        ))));
                    }
                    let n = f.args.into_iter().next().unwrap();
                    let epoch_date = Expression::Cast(Box::new(Cast {
                        this: Expression::string("1970-01-01"),
                        to: DataType::Date,
                        double_colon_syntax: false,
                        trailing_comments: Vec::new(),
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    match target {
                        DialectType::DuckDB => {
                            // CAST('1970-01-01' AS DATE) + INTERVAL n DAY
                            let interval =
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(n),
                                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                        unit: crate::expressions::IntervalUnit::Day,
                                        use_plural: false,
                                    }),
                                }));
                            Ok(Expression::Add(Box::new(
                                crate::expressions::BinaryOp::new(epoch_date, interval),
                            )))
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // DATE_ADD('DAY', n, CAST('1970-01-01' AS DATE))
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_ADD".to_string(),
                                vec![Expression::string("DAY"), n, epoch_date],
                            ))))
                        }
                        DialectType::Snowflake | DialectType::Redshift | DialectType::TSQL => {
                            // DATEADD(DAY, n, CAST('1970-01-01' AS DATE))
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATEADD".to_string(),
                                vec![
                                    Expression::Identifier(Identifier::new("DAY")),
                                    n,
                                    epoch_date,
                                ],
                            ))))
                        }
                        DialectType::BigQuery => {
                            // DATE_ADD(CAST('1970-01-01' AS DATE), INTERVAL n DAY)
                            let interval =
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(n),
                                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                        unit: crate::expressions::IntervalUnit::Day,
                                        use_plural: false,
                                    }),
                                }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_ADD".to_string(),
                                vec![epoch_date, interval],
                            ))))
                        }
                        DialectType::MySQL
                        | DialectType::Doris
                        | DialectType::StarRocks
                        | DialectType::Drill => {
                            // DATE_ADD(CAST('1970-01-01' AS DATE), INTERVAL n DAY)
                            let interval =
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(n),
                                    unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                                        unit: crate::expressions::IntervalUnit::Day,
                                        use_plural: false,
                                    }),
                                }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_ADD".to_string(),
                                vec![epoch_date, interval],
                            ))))
                        }
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                            // DATE_ADD(CAST('1970-01-01' AS DATE), n)
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_ADD".to_string(),
                                vec![epoch_date, n],
                            ))))
                        }
                        DialectType::PostgreSQL
                        | DialectType::Materialize
                        | DialectType::RisingWave => {
                            // CAST('1970-01-01' AS DATE) + INTERVAL 'n DAY'
                            let n_str = match &n {
                                Expression::Literal(lit)
                                    if matches!(lit.as_ref(), Literal::Number(_)) =>
                                {
                                    let Literal::Number(s) = lit.as_ref() else {
                                        unreachable!()
                                    };
                                    s.clone()
                                }
                                _ => scalar::expr_to_string_static(&n),
                            };
                            let interval =
                                Expression::Interval(Box::new(crate::expressions::Interval {
                                    this: Some(Expression::string(&format!("{} DAY", n_str))),
                                    unit: None,
                                }));
                            Ok(Expression::Add(Box::new(
                                crate::expressions::BinaryOp::new(epoch_date, interval),
                            )))
                        }
                        _ => {
                            // Default: keep as-is
                            Ok(Expression::Function(Box::new(Function::new(
                                "DATE_FROM_UNIX_DATE".to_string(),
                                vec![n],
                            ))))
                        }
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TimeStrToUnixConvert => {
                // TIME_STR_TO_UNIX(x) -> dialect-specific
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    match target {
                        DialectType::DuckDB => {
                            // EPOCH(CAST(x AS TIMESTAMP))
                            let cast_ts = Expression::Cast(Box::new(Cast {
                                this: arg,
                                to: DataType::Timestamp {
                                    timezone: false,
                                    precision: None,
                                },
                                double_colon_syntax: false,
                                trailing_comments: Vec::new(),
                                format: None,
                                default: None,
                                inferred_type: None,
                            }));
                            Ok(Expression::Function(Box::new(Function::new(
                                "EPOCH".to_string(),
                                vec![cast_ts],
                            ))))
                        }
                        DialectType::Hive
                        | DialectType::Doris
                        | DialectType::StarRocks
                        | DialectType::MySQL => {
                            // UNIX_TIMESTAMP(x)
                            Ok(Expression::Function(Box::new(Function::new(
                                "UNIX_TIMESTAMP".to_string(),
                                vec![arg],
                            ))))
                        }
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            // TO_UNIXTIME(DATE_PARSE(x, '%Y-%m-%d %T'))
                            let date_parse = Expression::Function(Box::new(Function::new(
                                "DATE_PARSE".to_string(),
                                vec![arg, Expression::string("%Y-%m-%d %T")],
                            )));
                            Ok(Expression::Function(Box::new(Function::new(
                                "TO_UNIXTIME".to_string(),
                                vec![date_parse],
                            ))))
                        }
                        _ => Ok(Expression::Function(Box::new(Function::new(
                            "TIME_STR_TO_UNIX".to_string(),
                            vec![arg],
                        )))),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::TimeToTimeStrConvert => {
                // TIME_TO_TIME_STR(x) -> CAST(x AS str_type) per dialect
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    let str_type = match target {
                        DialectType::DuckDB => DataType::Text,
                        DialectType::Hive
                        | DialectType::Spark
                        | DialectType::Databricks
                        | DialectType::Doris
                        | DialectType::StarRocks => DataType::Custom {
                            name: "STRING".to_string(),
                        },
                        DialectType::Redshift => DataType::Custom {
                            name: "VARCHAR(MAX)".to_string(),
                        },
                        _ => DataType::VarChar {
                            length: None,
                            parenthesized_length: false,
                        },
                    };
                    Ok(Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: str_type,
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

            Action::WeekOfYearToWeekIso => {
                // WEEKOFYEAR(x) -> WEEKISO(x) for Snowflake (cross-dialect normalization)
                if let Expression::WeekOfYear(uf) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "WEEKISO".to_string(),
                        vec![uf.this],
                    ))))
                } else {
                    Ok(e)
                }
            }
        }
    })()?;

    Ok(RewriteOutcome::Rewritten(expression))
}

#[derive(Debug, Clone, Copy)]
enum PostgresTimeDatePart {
    Second,
    Millisecond,
    Microsecond,
    Epoch,
}

pub(super) fn is_postgres_time_date_part(expression: &Expression) -> bool {
    postgres_time_date_part(expression).is_some()
}

pub(super) fn is_postgres_date_part_function(expression: &Expression) -> bool {
    matches!(
        expression,
        Expression::Function(function)
            if function.args.len() == 2
                && matches!(
                    function.name.to_ascii_uppercase().as_str(),
                    "DATE_PART" | "DATEPART"
                )
    )
}

fn postgres_time_date_part(expression: &Expression) -> Option<(PostgresTimeDatePart, Expression)> {
    match expression {
        Expression::Extract(extract) if is_time_value(&extract.this) => {
            let field = match &extract.field {
                DateTimeField::Second => PostgresTimeDatePart::Second,
                DateTimeField::Millisecond => PostgresTimeDatePart::Millisecond,
                DateTimeField::Microsecond => PostgresTimeDatePart::Microsecond,
                DateTimeField::Epoch => PostgresTimeDatePart::Epoch,
                _ => return None,
            };
            Some((field, extract.this.clone()))
        }
        Expression::Function(function)
            if function.args.len() == 2
                && matches!(
                    function.name.to_ascii_uppercase().as_str(),
                    "DATE_PART" | "DATEPART"
                )
                && is_time_value(&function.args[1]) =>
        {
            let field = postgres_time_date_part_name(&function.args[0])?;
            Some((field, function.args[1].clone()))
        }
        _ => None,
    }
}

fn postgres_time_date_part_name(expression: &Expression) -> Option<PostgresTimeDatePart> {
    let name = match expression {
        Expression::Literal(literal) => match literal.as_ref() {
            Literal::String(name) => name.as_str(),
            _ => return None,
        },
        Expression::Identifier(identifier) => identifier.name.as_str(),
        Expression::Column(column) if column.table.is_none() => column.name.name.as_str(),
        Expression::Cast(cast)
            if matches!(
                cast.to,
                DataType::Text | DataType::TextWithLength { .. } | DataType::VarChar { .. }
            ) =>
        {
            return postgres_time_date_part_name(&cast.this);
        }
        _ => return None,
    };

    match name.trim().to_ascii_uppercase().as_str() {
        "SECOND" | "SECONDS" | "SEC" | "SECS" | "S" | "SS" => Some(PostgresTimeDatePart::Second),
        "MILLISECOND" | "MILLISECONDS" | "MILLISEC" | "MILLISECS" | "MS" | "MSEC" | "MSECS" => {
            Some(PostgresTimeDatePart::Millisecond)
        }
        "MICROSECOND" | "MICROSECONDS" | "MICROSEC" | "MICROSECS" | "US" | "USEC" | "USECS"
        | "MCS" => Some(PostgresTimeDatePart::Microsecond),
        "EPOCH" | "EPOCH_SECOND" | "EPOCH_SECONDS" => Some(PostgresTimeDatePart::Epoch),
        _ => None,
    }
}

fn is_time_value(expression: &Expression) -> bool {
    let is_time_type = |data_type: &DataType| {
        matches!(data_type, DataType::Time { .. })
            || matches!(
                data_type,
                DataType::Custom { name }
                    if name.eq_ignore_ascii_case("TIME") || name.to_ascii_uppercase().starts_with("TIME(")
            )
    };

    match expression {
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            is_time_type(&cast.to)
        }
        Expression::Literal(literal) => matches!(literal.as_ref(), Literal::Time(_)),
        Expression::Paren(paren) => is_time_value(&paren.this),
        Expression::Alias(alias) => is_time_value(&alias.this),
        Expression::CurrentTime(_) | Expression::Localtime(_) => true,
        other => other.inferred_type().is_some_and(is_time_type),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TemporalOperand {
    Time,
    Date,
    Timestamp,
    Unknown,
}

pub(super) fn rewrite_tsql_interval_arithmetic(
    expression: &Expression,
    context: &NormalizationContext,
) -> Result<Option<Expression>> {
    let (base, interval, subtract) = match expression {
        Expression::Add(operation) if is_interval_value(&operation.right) => {
            (&operation.left, &operation.right, false)
        }
        Expression::Sub(operation) if is_interval_value(&operation.right) => {
            (&operation.left, &operation.right, true)
        }
        _ => {
            return Ok(Dialect::rewrite_tsql_interval_arithmetic_legacy(
                expression,
                context.source,
            ));
        }
    };

    if !Dialect::is_postgres_family_source(context.source) {
        return Ok(Dialect::rewrite_tsql_interval_arithmetic_legacy(
            expression,
            context.source,
        ));
    }

    match postgres_interval::decompose(interval) {
        DecomposeOutcome::NotLiteral => Ok(Dialect::rewrite_tsql_interval_arithmetic_legacy(
            expression,
            context.source,
        )),
        DecomposeOutcome::Invalid => unsupported_or_preserve(
            expression,
            context,
            "PostgreSQL interval literal cannot be represented safely",
        ),
        DecomposeOutcome::Parsed(parsed) => {
            if let Some(simple) = parsed.simple {
                if i32::try_from(simple.amount).is_ok()
                    && legacy_simple_interval_is_safe(base, simple.unit)
                {
                    return Ok(Dialect::rewrite_tsql_interval_arithmetic_legacy(
                        expression,
                        context.source,
                    ));
                }
            }
            rewrite_decomposed_interval(expression, base, parsed, subtract, context)
        }
    }
}

fn rewrite_decomposed_interval(
    original: &Expression,
    base: &Expression,
    mut parsed: ParsedInterval,
    subtract: bool,
    context: &NormalizationContext,
) -> Result<Option<Expression>> {
    if subtract {
        let Some(months) = parsed.value.months.checked_neg() else {
            return unsupported_or_preserve(
                original,
                context,
                "PostgreSQL interval exceeds the DATEADD integer range",
            );
        };
        let Some(days) = parsed.value.days.checked_neg() else {
            return unsupported_or_preserve(
                original,
                context,
                "PostgreSQL interval exceeds the DATEADD integer range",
            );
        };
        let Some(micros) = parsed.value.micros.checked_neg() else {
            return unsupported_or_preserve(
                original,
                context,
                "PostgreSQL interval exceeds the DATEADD integer range",
            );
        };
        parsed.value.months = months;
        parsed.value.days = days;
        parsed.value.micros = micros;
    }

    let operand = temporal_operand(base);
    if operand == TemporalOperand::Unknown
        && parsed.has_date_fields
        && parsed.has_time_fields
        && context.strict
    {
        return Err(Error::unsupported(
            "compound PostgreSQL interval arithmetic with an unresolved operand type",
            context.target.to_string(),
        ));
    }

    let (base, components) = match operand {
        TemporalOperand::Time => {
            let micros = parsed.value.micros % MICROS_PER_DAY;
            (base.clone(), time_components(micros))
        }
        TemporalOperand::Date => {
            let base = if parsed.has_time_fields {
                timestamp_cast(base.clone())
            } else {
                base.clone()
            };
            (base, timestamp_components(parsed))
        }
        TemporalOperand::Timestamp | TemporalOperand::Unknown => {
            (base.clone(), timestamp_components(parsed))
        }
    };

    let Some(rewritten) = build_dateadd_chain(base, &components) else {
        return unsupported_or_preserve(
            original,
            context,
            "PostgreSQL interval exceeds the DATEADD integer range",
        );
    };
    Ok(Some(rewritten))
}

fn legacy_simple_interval_is_safe(base: &Expression, unit: IntervalUnit) -> bool {
    let is_date_unit = matches!(
        unit,
        IntervalUnit::Year
            | IntervalUnit::Quarter
            | IntervalUnit::Month
            | IntervalUnit::Week
            | IntervalUnit::Day
    );
    match temporal_operand(base) {
        TemporalOperand::Time => !is_date_unit,
        TemporalOperand::Date => is_date_unit,
        TemporalOperand::Timestamp | TemporalOperand::Unknown => true,
    }
}

fn unsupported_or_preserve(
    original: &Expression,
    context: &NormalizationContext,
    feature: &str,
) -> Result<Option<Expression>> {
    if context.strict {
        Err(Error::unsupported(feature, context.target.to_string()))
    } else {
        Ok(Some(original.clone()))
    }
}

fn is_interval_value(expression: &Expression) -> bool {
    match expression {
        Expression::Interval(_) => true,
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            matches!(cast.to, DataType::Interval { .. })
                || matches!(&cast.to, DataType::Custom { name } if name.eq_ignore_ascii_case("INTERVAL"))
        }
        _ => false,
    }
}

fn temporal_operand(expression: &Expression) -> TemporalOperand {
    if is_time_value(expression) {
        return TemporalOperand::Time;
    }
    match expression {
        Expression::Literal(literal) => match literal.as_ref() {
            Literal::Date(_) => TemporalOperand::Date,
            Literal::Timestamp(_) => TemporalOperand::Timestamp,
            _ => TemporalOperand::Unknown,
        },
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            temporal_data_type(&cast.to)
        }
        Expression::Paren(paren) => temporal_operand(&paren.this),
        Expression::Alias(alias) => temporal_operand(&alias.this),
        Expression::CurrentDate(_)
        | Expression::Date(_)
        | Expression::MakeDate(_)
        | Expression::ToDate(_)
        | Expression::DateStrToDate(_) => TemporalOperand::Date,
        Expression::CurrentTimestamp(_)
        | Expression::CurrentTimestampLTZ(_)
        | Expression::Localtimestamp(_)
        | Expression::CurrentDatetime(_) => TemporalOperand::Timestamp,
        Expression::Function(function)
            if matches!(
                function.name.to_ascii_uppercase().as_str(),
                "GETDATE" | "SYSDATETIME" | "CURRENT_TIMESTAMP" | "NOW"
            ) =>
        {
            TemporalOperand::Timestamp
        }
        other => other
            .inferred_type()
            .map(temporal_data_type)
            .unwrap_or(TemporalOperand::Unknown),
    }
}

fn temporal_data_type(data_type: &DataType) -> TemporalOperand {
    match data_type {
        DataType::Date => TemporalOperand::Date,
        DataType::Time { .. } => TemporalOperand::Time,
        DataType::Timestamp { .. } => TemporalOperand::Timestamp,
        DataType::Custom { name } => {
            let name = name.trim().to_ascii_uppercase();
            if name == "DATE" {
                TemporalOperand::Date
            } else if name == "TIME" || name.starts_with("TIME(") {
                TemporalOperand::Time
            } else if name.starts_with("TIMESTAMP")
                || name.starts_with("DATETIME")
                || name.starts_with("SMALLDATETIME")
            {
                TemporalOperand::Timestamp
            } else {
                TemporalOperand::Unknown
            }
        }
        _ => TemporalOperand::Unknown,
    }
}

fn timestamp_cast(expression: Expression) -> Expression {
    Expression::Cast(Box::new(Cast {
        this: expression,
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
}

fn timestamp_components(parsed: ParsedInterval) -> Vec<(&'static str, i128)> {
    let mut micros = parsed.value.micros;
    let hours = micros / MICROS_PER_HOUR;
    micros %= MICROS_PER_HOUR;
    let minutes = micros / MICROS_PER_MINUTE;
    micros %= MICROS_PER_MINUTE;
    let seconds = micros / MICROS_PER_SECOND;
    micros %= MICROS_PER_SECOND;
    vec![
        ("MONTH", parsed.value.months),
        ("DAY", parsed.value.days),
        ("HOUR", hours),
        ("MINUTE", minutes),
        ("SECOND", seconds),
        ("MICROSECOND", micros),
    ]
}

fn time_components(mut micros: i128) -> Vec<(&'static str, i128)> {
    let hours = micros / MICROS_PER_HOUR;
    micros %= MICROS_PER_HOUR;
    let minutes = micros / MICROS_PER_MINUTE;
    micros %= MICROS_PER_MINUTE;
    let seconds = micros / MICROS_PER_SECOND;
    micros %= MICROS_PER_SECOND;
    vec![
        ("HOUR", hours),
        ("MINUTE", minutes),
        ("SECOND", seconds),
        ("MICROSECOND", micros),
    ]
}

fn build_dateadd_chain(
    mut expression: Expression,
    components: &[(&'static str, i128)],
) -> Option<Expression> {
    for (unit, amount) in components {
        if *amount == 0 {
            continue;
        }
        let amount = i32::try_from(*amount).ok()?;
        expression = Expression::Function(Box::new(Function::new(
            "DATEADD",
            vec![
                Expression::Identifier(Identifier::new(*unit)),
                Expression::Literal(Box::new(Literal::Number(amount.to_string()))),
                expression,
            ],
        )));
    }
    Some(expression)
}

fn rewrite_postgres_date_part_for_tsql(expression: Expression) -> Result<Expression> {
    let returns_double_precision = is_postgres_date_part_function(&expression);
    let Some((field, value)) = postgres_time_date_part(&expression) else {
        return Ok(if returns_double_precision {
            cast_postgres_date_part_to_double(expression)
        } else {
            expression
        });
    };

    let date_part = |name: &str, value: Expression| {
        Expression::Function(Box::new(Function::new(
            "DATEPART",
            vec![Expression::Identifier(Identifier::new(name)), value],
        )))
    };
    let decimal = |value: &str| Expression::Literal(Box::new(Literal::Number(value.to_string())));
    let fractional_microseconds = date_part("MICROSECOND", value.clone());
    let fractional_seconds = Expression::Div(Box::new(BinaryOp::new(
        fractional_microseconds.clone(),
        decimal("1000000.0"),
    )));
    let whole_seconds = date_part("SECOND", value.clone());

    let rewritten = match field {
        PostgresTimeDatePart::Second => {
            Expression::Add(Box::new(BinaryOp::new(whole_seconds, fractional_seconds)))
        }
        PostgresTimeDatePart::Millisecond => {
            let whole_milliseconds = Expression::Mul(Box::new(BinaryOp::new(
                whole_seconds,
                Expression::number(1000),
            )));
            let fractional_milliseconds = Expression::Div(Box::new(BinaryOp::new(
                fractional_microseconds,
                decimal("1000.0"),
            )));
            Expression::Add(Box::new(BinaryOp::new(
                whole_milliseconds,
                fractional_milliseconds,
            )))
        }
        PostgresTimeDatePart::Microsecond => {
            let whole_microseconds = Expression::Mul(Box::new(BinaryOp::new(
                whole_seconds,
                Expression::number(1_000_000),
            )));
            Expression::Add(Box::new(BinaryOp::new(
                whole_microseconds,
                fractional_microseconds,
            )))
        }
        PostgresTimeDatePart::Epoch => {
            let midnight = Expression::Cast(Box::new(Cast {
                this: Expression::string("00:00:00"),
                to: DataType::Time {
                    precision: Some(6),
                    timezone: false,
                },
                trailing_comments: Vec::new(),
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }));
            let elapsed_seconds = Expression::Function(Box::new(Function::new(
                "DATEDIFF",
                vec![
                    Expression::Identifier(Identifier::new("SECOND")),
                    midnight,
                    value,
                ],
            )));
            Expression::Add(Box::new(BinaryOp::new(elapsed_seconds, fractional_seconds)))
        }
    };

    Ok(if returns_double_precision {
        cast_postgres_date_part_to_double(rewritten)
    } else {
        rewritten
    })
}

fn cast_postgres_date_part_to_double(expression: Expression) -> Expression {
    Expression::Cast(Box::new(Cast {
        this: expression,
        to: DataType::Double {
            precision: None,
            scale: None,
        },
        trailing_comments: Vec::new(),
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }))
}

pub(super) fn date_trunc_to_mysql(unit: &str, expr: &Expression) -> Result<Expression> {
    use crate::expressions::Function;
    match unit {
        "DAY" => {
            // DATE(x)
            Ok(Expression::Function(Box::new(Function::new(
                "DATE".to_string(),
                vec![expr.clone()],
            ))))
        }
        "WEEK" => {
            // STR_TO_DATE(CONCAT(YEAR(x), ' ', WEEK(x, 1), ' 1'), '%Y %u %w')
            let year_x = Expression::Function(Box::new(Function::new(
                "YEAR".to_string(),
                vec![expr.clone()],
            )));
            let week_x = Expression::Function(Box::new(Function::new(
                "WEEK".to_string(),
                vec![expr.clone(), Expression::number(1)],
            )));
            let concat_args = vec![
                year_x,
                Expression::string(" "),
                week_x,
                Expression::string(" 1"),
            ];
            let concat =
                Expression::Function(Box::new(Function::new("CONCAT".to_string(), concat_args)));
            Ok(Expression::Function(Box::new(Function::new(
                "STR_TO_DATE".to_string(),
                vec![concat, Expression::string("%Y %u %w")],
            ))))
        }
        "MONTH" => {
            // STR_TO_DATE(CONCAT(YEAR(x), ' ', MONTH(x), ' 1'), '%Y %c %e')
            let year_x = Expression::Function(Box::new(Function::new(
                "YEAR".to_string(),
                vec![expr.clone()],
            )));
            let month_x = Expression::Function(Box::new(Function::new(
                "MONTH".to_string(),
                vec![expr.clone()],
            )));
            let concat_args = vec![
                year_x,
                Expression::string(" "),
                month_x,
                Expression::string(" 1"),
            ];
            let concat =
                Expression::Function(Box::new(Function::new("CONCAT".to_string(), concat_args)));
            Ok(Expression::Function(Box::new(Function::new(
                "STR_TO_DATE".to_string(),
                vec![concat, Expression::string("%Y %c %e")],
            ))))
        }
        "QUARTER" => {
            // STR_TO_DATE(CONCAT(YEAR(x), ' ', QUARTER(x) * 3 - 2, ' 1'), '%Y %c %e')
            let year_x = Expression::Function(Box::new(Function::new(
                "YEAR".to_string(),
                vec![expr.clone()],
            )));
            let quarter_x = Expression::Function(Box::new(Function::new(
                "QUARTER".to_string(),
                vec![expr.clone()],
            )));
            // QUARTER(x) * 3 - 2
            let mul = Expression::Mul(Box::new(crate::expressions::BinaryOp {
                left: quarter_x,
                right: Expression::number(3),
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
                inferred_type: None,
            }));
            let sub = Expression::Sub(Box::new(crate::expressions::BinaryOp {
                left: mul,
                right: Expression::number(2),
                left_comments: Vec::new(),
                operator_comments: Vec::new(),
                trailing_comments: Vec::new(),
                inferred_type: None,
            }));
            let concat_args = vec![
                year_x,
                Expression::string(" "),
                sub,
                Expression::string(" 1"),
            ];
            let concat =
                Expression::Function(Box::new(Function::new("CONCAT".to_string(), concat_args)));
            Ok(Expression::Function(Box::new(Function::new(
                "STR_TO_DATE".to_string(),
                vec![concat, Expression::string("%Y %c %e")],
            ))))
        }
        "YEAR" => {
            // STR_TO_DATE(CONCAT(YEAR(x), ' 1 1'), '%Y %c %e')
            let year_x = Expression::Function(Box::new(Function::new(
                "YEAR".to_string(),
                vec![expr.clone()],
            )));
            let concat_args = vec![year_x, Expression::string(" 1 1")];
            let concat =
                Expression::Function(Box::new(Function::new("CONCAT".to_string(), concat_args)));
            Ok(Expression::Function(Box::new(Function::new(
                "STR_TO_DATE".to_string(),
                vec![concat, Expression::string("%Y %c %e")],
            ))))
        }
        _ => {
            // Unsupported unit -> keep as DATE_TRUNC
            Ok(Expression::Function(Box::new(Function::new(
                "DATE_TRUNC".to_string(),
                vec![Expression::string(unit), expr.clone()],
            ))))
        }
    }
}

pub(super) fn normalize_interval_string(expr: Expression, target: DialectType) -> Expression {
    if let Expression::Literal(ref lit) = expr {
        if let crate::expressions::Literal::String(ref s) = lit.as_ref() {
            // Try to parse patterns like '1day', '1 day', '2 days', '  2   days  '
            let trimmed = s.trim();

            // Find where digits end and unit text begins
            let digit_end = trimmed
                .find(|c: char| !c.is_ascii_digit())
                .unwrap_or(trimmed.len());
            if digit_end == 0 || digit_end == trimmed.len() {
                return expr;
            }
            let num = &trimmed[..digit_end];
            let unit_text = trimmed[digit_end..].trim().to_ascii_uppercase();
            if unit_text.is_empty() {
                return expr;
            }

            let known_units = [
                "DAY", "DAYS", "HOUR", "HOURS", "MINUTE", "MINUTES", "SECOND", "SECONDS", "WEEK",
                "WEEKS", "MONTH", "MONTHS", "YEAR", "YEARS",
            ];
            if !known_units.contains(&unit_text.as_str()) {
                return expr;
            }

            let unit_str = unit_text.clone();
            // Singularize
            let unit_singular = if unit_str.ends_with('S') && unit_str.len() > 3 {
                &unit_str[..unit_str.len() - 1]
            } else {
                &unit_str
            };
            let unit = unit_singular;

            match target {
                DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                    // INTERVAL '2' DAY
                    let iu = match unit {
                        "DAY" => crate::expressions::IntervalUnit::Day,
                        "HOUR" => crate::expressions::IntervalUnit::Hour,
                        "MINUTE" => crate::expressions::IntervalUnit::Minute,
                        "SECOND" => crate::expressions::IntervalUnit::Second,
                        "WEEK" => crate::expressions::IntervalUnit::Week,
                        "MONTH" => crate::expressions::IntervalUnit::Month,
                        "YEAR" => crate::expressions::IntervalUnit::Year,
                        _ => return expr,
                    };
                    return Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(Expression::string(num)),
                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                            unit: iu,
                            use_plural: false,
                        }),
                    }));
                }
                DialectType::PostgreSQL | DialectType::Redshift | DialectType::DuckDB => {
                    // INTERVAL '2 DAYS'
                    let plural = if num != "1" && !unit_str.ends_with('S') {
                        format!("{} {}S", num, unit)
                    } else if unit_str.ends_with('S') {
                        format!("{} {}", num, unit_str)
                    } else {
                        format!("{} {}", num, unit)
                    };
                    return Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(Expression::string(&plural)),
                        unit: None,
                    }));
                }
                _ => {
                    // Spark/Databricks/Hive: INTERVAL '1' DAY
                    let iu = match unit {
                        "DAY" => crate::expressions::IntervalUnit::Day,
                        "HOUR" => crate::expressions::IntervalUnit::Hour,
                        "MINUTE" => crate::expressions::IntervalUnit::Minute,
                        "SECOND" => crate::expressions::IntervalUnit::Second,
                        "WEEK" => crate::expressions::IntervalUnit::Week,
                        "MONTH" => crate::expressions::IntervalUnit::Month,
                        "YEAR" => crate::expressions::IntervalUnit::Year,
                        _ => return expr,
                    };
                    return Expression::Interval(Box::new(crate::expressions::Interval {
                        this: Some(Expression::string(num)),
                        unit: Some(crate::expressions::IntervalUnitSpec::Simple {
                            unit: iu,
                            use_plural: false,
                        }),
                    }));
                }
            }
        }
    }
    // If it's already an INTERVAL expression, pass through
    expr
}

pub(in crate::dialects) fn interval_unit_to_string(
    unit: &crate::expressions::IntervalUnit,
) -> &'static str {
    match unit {
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
    }
}

pub(super) fn get_unit_str_static(expr: &Expression) -> String {
    use crate::expressions::Literal;
    match expr {
        Expression::Identifier(id) => id.name.to_ascii_uppercase(),
        Expression::Var(v) => v.this.to_ascii_uppercase(),
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
            let Literal::String(s) = lit.as_ref() else {
                unreachable!()
            };
            s.to_ascii_uppercase()
        }
        Expression::Cast(cast)
            if cast.format.is_none()
                && cast.default.is_none()
                && types::unit_cast_target_is_string(&cast.to)
                && matches!(
                    &cast.this,
                    Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_))
                ) =>
        {
            let Expression::Literal(lit) = &cast.this else {
                unreachable!()
            };
            let Literal::String(s) = lit.as_ref() else {
                unreachable!()
            };
            s.to_ascii_uppercase()
        }
        Expression::Column(col) => col.name.name.to_ascii_uppercase(),
        Expression::Function(f) => {
            let base = f.name.to_ascii_uppercase();
            if !f.args.is_empty() {
                let inner = get_unit_str_static(&f.args[0]);
                format!("{}({})", base, inner)
            } else {
                base
            }
        }
        _ => "DAY".to_string(),
    }
}

pub(super) fn parse_interval_unit_static(s: &str) -> crate::expressions::IntervalUnit {
    match s {
        "YEAR" | "YY" | "YYYY" => crate::expressions::IntervalUnit::Year,
        "QUARTER" | "QQ" | "Q" => crate::expressions::IntervalUnit::Quarter,
        "MONTH" | "MONTHS" | "MON" | "MONS" | "MM" | "M" => crate::expressions::IntervalUnit::Month,
        "WEEK" | "WK" | "WW" | "ISOWEEK" => crate::expressions::IntervalUnit::Week,
        "DAY" | "DD" | "D" | "DY" => crate::expressions::IntervalUnit::Day,
        "HOUR" | "HH" => crate::expressions::IntervalUnit::Hour,
        "MINUTE" | "MI" | "N" => crate::expressions::IntervalUnit::Minute,
        "SECOND" | "SS" | "S" => crate::expressions::IntervalUnit::Second,
        "MILLISECOND" | "MS" => crate::expressions::IntervalUnit::Millisecond,
        "MICROSECOND" | "MCS" | "US" => crate::expressions::IntervalUnit::Microsecond,
        _ if s.starts_with("WEEK(") => crate::expressions::IntervalUnit::Week,
        _ => crate::expressions::IntervalUnit::Day,
    }
}

pub(super) fn quote_interval_val(expr: &Expression) -> Expression {
    use crate::expressions::Literal;
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(_)) => {
            let Literal::Number(n) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Literal(Box::new(Literal::String(n.clone())))
        }
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => expr.clone(),
        Expression::Neg(inner) => {
            if let Expression::Literal(lit) = &inner.this {
                if let Literal::Number(n) = lit.as_ref() {
                    Expression::Literal(Box::new(Literal::String(format!("-{}", n))))
                } else {
                    inner.this.clone()
                }
            } else {
                expr.clone()
            }
        }
        _ => expr.clone(),
    }
}

pub(super) fn timestamp_string_has_timezone(ts: &str) -> bool {
    let trimmed = ts.trim();
    // Check for numeric timezone offsets: +N, -N, +NN:NN, -NN:NN at end
    if let Some(last_space) = trimmed.rfind(' ') {
        let suffix = &trimmed[last_space + 1..];
        if (suffix.starts_with('+') || suffix.starts_with('-')) && suffix.len() > 1 {
            let rest = &suffix[1..];
            if rest.chars().all(|c| c.is_ascii_digit() || c == ':') {
                return true;
            }
        }
    }
    // Check for named timezone abbreviations
    let ts_lower = trimmed.to_ascii_lowercase();
    let tz_abbrevs = [" utc", " gmt", " cet", " est", " pst", " cst", " mst"];
    for abbrev in &tz_abbrevs {
        if ts_lower.ends_with(abbrev) {
            return true;
        }
    }
    false
}

pub(super) fn maybe_cast_ts_to_tz(expr: Expression, func_name: &str) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Timestamp(_)) => {
            let Literal::Timestamp(s) = lit.as_ref() else {
                unreachable!()
            };
            let tz = func_name.starts_with("TIMESTAMP");
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                to: if tz {
                    DataType::Timestamp {
                        timezone: true,
                        precision: None,
                    }
                } else {
                    DataType::Timestamp {
                        timezone: false,
                        precision: None,
                    }
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
}

pub(super) fn maybe_cast_ts(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Timestamp(_)) => {
            let Literal::Timestamp(s) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
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
        other => other,
    }
}

pub(super) fn date_literal_to_cast(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Date(_)) => {
            let Literal::Date(s) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                to: DataType::Date,
                trailing_comments: vec![],
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))
        }
        other => other,
    }
}

pub(super) fn ensure_cast_date(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Date(_)) => {
            let Literal::Date(s) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                to: DataType::Date,
                trailing_comments: vec![],
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))
        }
        Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(ref _s)) => {
            // String literal that should be a date -> CAST('s' AS DATE)
            Expression::Cast(Box::new(Cast {
                this: expr,
                to: DataType::Date,
                trailing_comments: vec![],
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))
        }
        // Already a CAST or other expression -> leave as-is
        other => other,
    }
}

pub(super) fn force_cast_date(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType};
    // If it's already a CAST to DATE, don't double-wrap
    if let Expression::Cast(ref c) = expr {
        if matches!(c.to, DataType::Date) {
            return expr;
        }
    }
    Expression::Cast(Box::new(Cast {
        this: expr,
        to: DataType::Date,
        trailing_comments: vec![],
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }))
}

pub(super) fn ensure_to_date_preserved(expr: Expression) -> Expression {
    use crate::expressions::{Function, Literal};
    if matches!(expr, Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(_))) {
        Expression::Function(Box::new(Function::new(
            Dialect::PRESERVED_TO_DATE.to_string(),
            vec![expr],
        )))
    } else {
        expr
    }
}

pub(super) fn try_cast_date(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType};
    Expression::TryCast(Box::new(Cast {
        this: expr,
        to: DataType::Date,
        trailing_comments: vec![],
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }))
}

pub(super) fn double_cast_timestamp_date(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType};
    let inner = Expression::Cast(Box::new(Cast {
        this: expr,
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
    Expression::Cast(Box::new(Cast {
        this: inner,
        to: DataType::Date,
        trailing_comments: vec![],
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }))
}

pub(super) fn double_cast_datetime_date(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType};
    let inner = Expression::Cast(Box::new(Cast {
        this: expr,
        to: DataType::Custom {
            name: "DATETIME".to_string(),
        },
        trailing_comments: vec![],
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }));
    Expression::Cast(Box::new(Cast {
        this: inner,
        to: DataType::Date,
        trailing_comments: vec![],
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }))
}

pub(super) fn double_cast_datetime2_date(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType};
    let inner = Expression::Cast(Box::new(Cast {
        this: expr,
        to: DataType::Custom {
            name: "DATETIME2".to_string(),
        },
        trailing_comments: vec![],
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }));
    Expression::Cast(Box::new(Cast {
        this: inner,
        to: DataType::Date,
        trailing_comments: vec![],
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }))
}

pub(super) fn hive_format_to_c_format(fmt: &str) -> String {
    let mut result = String::new();
    let chars: Vec<char> = fmt.chars().collect();
    let mut i = 0;
    while i < chars.len() {
        match chars[i] {
            'y' => {
                let mut count = 0;
                while i < chars.len() && chars[i] == 'y' {
                    count += 1;
                    i += 1;
                }
                if count >= 4 {
                    result.push_str("%Y");
                } else if count == 2 {
                    result.push_str("%y");
                } else {
                    result.push_str("%Y");
                }
            }
            'M' => {
                let mut count = 0;
                while i < chars.len() && chars[i] == 'M' {
                    count += 1;
                    i += 1;
                }
                if count >= 3 {
                    result.push_str("%b");
                } else if count == 2 {
                    result.push_str("%m");
                } else {
                    result.push_str("%m");
                }
            }
            'd' => {
                let mut _count = 0;
                while i < chars.len() && chars[i] == 'd' {
                    _count += 1;
                    i += 1;
                }
                result.push_str("%d");
            }
            'H' => {
                let mut _count = 0;
                while i < chars.len() && chars[i] == 'H' {
                    _count += 1;
                    i += 1;
                }
                result.push_str("%H");
            }
            'h' => {
                let mut _count = 0;
                while i < chars.len() && chars[i] == 'h' {
                    _count += 1;
                    i += 1;
                }
                result.push_str("%I");
            }
            'm' => {
                let mut _count = 0;
                while i < chars.len() && chars[i] == 'm' {
                    _count += 1;
                    i += 1;
                }
                result.push_str("%M");
            }
            's' => {
                let mut _count = 0;
                while i < chars.len() && chars[i] == 's' {
                    _count += 1;
                    i += 1;
                }
                result.push_str("%S");
            }
            'S' => {
                // Fractional seconds - skip
                while i < chars.len() && chars[i] == 'S' {
                    i += 1;
                }
                result.push_str("%f");
            }
            'a' => {
                // AM/PM
                while i < chars.len() && chars[i] == 'a' {
                    i += 1;
                }
                result.push_str("%p");
            }
            'E' => {
                let mut count = 0;
                while i < chars.len() && chars[i] == 'E' {
                    count += 1;
                    i += 1;
                }
                if count >= 4 {
                    result.push_str("%A");
                } else {
                    result.push_str("%a");
                }
            }
            '\'' => {
                // Quoted literal text - pass through the quotes and content
                result.push('\'');
                i += 1;
                while i < chars.len() && chars[i] != '\'' {
                    result.push(chars[i]);
                    i += 1;
                }
                if i < chars.len() {
                    result.push('\'');
                    i += 1;
                }
            }
            c => {
                result.push(c);
                i += 1;
            }
        }
    }
    result
}

pub(super) fn hive_format_to_presto_format(fmt: &str) -> String {
    let c_fmt = hive_format_to_c_format(fmt);
    // Presto uses %T for HH:MM:SS
    c_fmt.replace("%H:%M:%S", "%T")
}

pub(super) fn ensure_cast_timestamp(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Timestamp(_)) => {
            let Literal::Timestamp(s) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
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
        Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(ref _s)) => {
            Expression::Cast(Box::new(Cast {
                this: expr,
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
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Datetime(_)) => {
            let Literal::Datetime(s) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
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
        other => other,
    }
}

pub(super) fn force_cast_timestamp(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType};
    // Don't double-wrap if already a CAST to TIMESTAMP
    if let Expression::Cast(ref c) = expr {
        if matches!(c.to, DataType::Timestamp { .. }) {
            return expr;
        }
    }
    Expression::Cast(Box::new(Cast {
        this: expr,
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

pub(super) fn ensure_cast_timestamptz(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Timestamp(_)) => {
            let Literal::Timestamp(s) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                to: DataType::Timestamp {
                    timezone: true,
                    precision: None,
                },
                trailing_comments: vec![],
                double_colon_syntax: false,
                format: None,
                default: None,
                inferred_type: None,
            }))
        }
        Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(ref _s)) => {
            Expression::Cast(Box::new(Cast {
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
            }))
        }
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Datetime(_)) => {
            let Literal::Datetime(s) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                to: DataType::Timestamp {
                    timezone: true,
                    precision: None,
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
}

pub(super) fn ensure_cast_datetime(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(ref _s)) => {
            Expression::Cast(Box::new(Cast {
                this: expr,
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
    }
}

pub(super) fn force_cast_datetime(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType};
    if let Expression::Cast(ref c) = expr {
        if let DataType::Custom { ref name } = c.to {
            if name.eq_ignore_ascii_case("DATETIME") {
                return expr;
            }
        }
    }
    Expression::Cast(Box::new(Cast {
        this: expr,
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

pub(super) fn ensure_cast_datetime2(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::String(ref _s)) => {
            Expression::Cast(Box::new(Cast {
                this: expr,
                to: DataType::Custom {
                    name: "DATETIME2".to_string(),
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
}

pub(super) fn ts_literal_to_cast_tz(expr: Expression) -> Expression {
    use crate::expressions::{Cast, DataType, Literal};
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Timestamp(_)) => {
            let Literal::Timestamp(s) = lit.as_ref() else {
                unreachable!()
            };
            Expression::Cast(Box::new(Cast {
                this: Expression::Literal(Box::new(Literal::String(s.clone()))),
                to: DataType::Timestamp {
                    timezone: true,
                    precision: None,
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
}

pub(super) fn bq_format_to_snowflake(format_expr: &Expression) -> Expression {
    use crate::expressions::Literal;
    if let Expression::Literal(lit) = format_expr {
        if let Literal::String(s) = lit.as_ref() {
            let sf = s
                .replace("%Y", "yyyy")
                .replace("%m", "mm")
                .replace("%d", "DD")
                .replace("%H", "HH24")
                .replace("%M", "MI")
                .replace("%S", "SS")
                .replace("%b", "mon")
                .replace("%B", "Month")
                .replace("%e", "FMDD");
            Expression::Literal(Box::new(Literal::String(sf)))
        } else {
            format_expr.clone()
        }
    } else {
        format_expr.clone()
    }
}

pub(super) fn bq_format_to_duckdb(format_expr: &Expression) -> Expression {
    use crate::expressions::Literal;
    if let Expression::Literal(lit) = format_expr {
        if let Literal::String(s) = lit.as_ref() {
            let duck = s
                .replace("%T", "%H:%M:%S")
                .replace("%F", "%Y-%m-%d")
                .replace("%D", "%m/%d/%y")
                .replace("%x", "%m/%d/%y")
                .replace("%c", "%a %b %-d %H:%M:%S %Y")
                .replace("%e", "%-d")
                .replace("%E6S", "%S.%f");
            Expression::Literal(Box::new(Literal::String(duck)))
        } else {
            format_expr.clone()
        }
    } else {
        format_expr.clone()
    }
}

pub(super) fn bq_cast_format_to_strftime(format_expr: &Expression) -> Expression {
    use crate::expressions::Literal;
    if let Expression::Literal(lit) = format_expr {
        if let Literal::String(s) = lit.as_ref() {
            // Replace format elements from longest to shortest to avoid partial matches
            let result = s
                .replace("YYYYMMDD", "%Y%m%d")
                .replace("YYYY", "%Y")
                .replace("YY", "%y")
                .replace("MONTH", "%B")
                .replace("MON", "%b")
                .replace("MM", "%m")
                .replace("DD", "%d")
                .replace("HH24", "%H")
                .replace("HH12", "%I")
                .replace("HH", "%I")
                .replace("MI", "%M")
                .replace("SSTZH", "%S%z")
                .replace("SS", "%S")
                .replace("TZH", "%z");
            Expression::Literal(Box::new(Literal::String(result)))
        } else {
            format_expr.clone()
        }
    } else {
        format_expr.clone()
    }
}

pub(super) fn bq_format_normalize_bq(format_expr: &Expression) -> Expression {
    use crate::expressions::Literal;
    if let Expression::Literal(lit) = format_expr {
        if let Literal::String(s) = lit.as_ref() {
            let norm = s.replace("%H:%M:%S", "%T").replace("%x", "%D");
            Expression::Literal(Box::new(Literal::String(norm)))
        } else {
            format_expr.clone()
        }
    } else {
        format_expr.clone()
    }
}
