use super::{temporal, NormalizationContext, RewriteOutcome};
use crate::dialects::DialectType;
use crate::error::Result;
use crate::expressions::*;

#[derive(Debug)]
pub(super) enum Action {
    PostgresUnknownNullCast,
    PostgresFloatToIntegerCast,
    CastTimestampToDatetime,
    DateTruncWrapCast,
    ToDateToCast,
    BigQueryCastType,
    BigQueryCastFormat,
    TSQLTypeNormalize,
    HiveCastToTryCast,
    CastTimestampStripTz,
    BitAggFloatCast,
    StrftimeCastTimestamp,
    SnowflakeFloatProtect,
    DecimalDefaultPrecision,
    SparkDateFuncCast,
    MysqlCastCharToText,
    SparkCastVarcharToString,
    OracleVarchar2ToVarchar,
    CastTimestamptzToFunc,
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
            Action::PostgresUnknownNullCast => {
                let c = if let Expression::Cast(c) = e {
                    *c
                } else {
                    unreachable!("action only triggered for Cast expressions")
                };
                Ok(c.this)
            }
            Action::PostgresFloatToIntegerCast => {
                let c = if let Expression::Cast(c) = e {
                    *c
                } else {
                    unreachable!("action only triggered for Cast expressions")
                };
                Ok(rewrite_postgres_float_to_integer_cast(c))
            }
            Action::CastTimestampToDatetime => {
                let c = if let Expression::Cast(c) = e {
                    *c
                } else {
                    unreachable!("action only triggered for Cast expressions")
                };
                Ok(Expression::Cast(Box::new(Cast {
                    to: DataType::Custom {
                        name: "DATETIME".to_string(),
                    },
                    ..c
                })))
            }

            Action::DateTruncWrapCast => {
                // Handle both Expression::DateTrunc/TimestampTrunc and
                // Expression::Function("DATE_TRUNC", [unit, expr])
                match e {
                    Expression::DateTrunc(d) | Expression::TimestampTrunc(d) => {
                        let input_type = match &d.this {
                            Expression::Cast(c) => Some(c.to.clone()),
                            _ => None,
                        };
                        if let Some(cast_type) = input_type {
                            let is_time = matches!(cast_type, DataType::Time { .. });
                            if is_time {
                                let date_expr = Expression::Cast(Box::new(Cast {
                                    this: Expression::Literal(Box::new(
                                        crate::expressions::Literal::String(
                                            "1970-01-01".to_string(),
                                        ),
                                    )),
                                    to: DataType::Date,
                                    double_colon_syntax: false,
                                    trailing_comments: vec![],
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                let add_expr =
                                    Expression::Add(Box::new(BinaryOp::new(date_expr, d.this)));
                                let inner = Expression::DateTrunc(Box::new(DateTruncFunc {
                                    this: add_expr,
                                    unit: d.unit,
                                }));
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: inner,
                                    to: cast_type,
                                    double_colon_syntax: false,
                                    trailing_comments: vec![],
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                let inner = Expression::DateTrunc(Box::new(*d));
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: inner,
                                    to: cast_type,
                                    double_colon_syntax: false,
                                    trailing_comments: vec![],
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        } else {
                            Ok(Expression::DateTrunc(d))
                        }
                    }
                    Expression::Function(f) if f.args.len() == 2 => {
                        // Function-based DATE_TRUNC(unit, expr)
                        let input_type = match &f.args[1] {
                            Expression::Cast(c) => Some(c.to.clone()),
                            _ => None,
                        };
                        if let Some(cast_type) = input_type {
                            let is_time = matches!(cast_type, DataType::Time { .. });
                            if is_time {
                                let date_expr = Expression::Cast(Box::new(Cast {
                                    this: Expression::Literal(Box::new(
                                        crate::expressions::Literal::String(
                                            "1970-01-01".to_string(),
                                        ),
                                    )),
                                    to: DataType::Date,
                                    double_colon_syntax: false,
                                    trailing_comments: vec![],
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }));
                                let mut args = f.args;
                                let unit_arg = args.remove(0);
                                let time_expr = args.remove(0);
                                let add_expr =
                                    Expression::Add(Box::new(BinaryOp::new(date_expr, time_expr)));
                                let inner = Expression::Function(Box::new(Function::new(
                                    "DATE_TRUNC".to_string(),
                                    vec![unit_arg, add_expr],
                                )));
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: inner,
                                    to: cast_type,
                                    double_colon_syntax: false,
                                    trailing_comments: vec![],
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            } else {
                                // Wrap the function in CAST
                                Ok(Expression::Cast(Box::new(Cast {
                                    this: Expression::Function(f),
                                    to: cast_type,
                                    double_colon_syntax: false,
                                    trailing_comments: vec![],
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                })))
                            }
                        } else {
                            Ok(Expression::Function(f))
                        }
                    }
                    other => Ok(other),
                }
            }

            Action::ToDateToCast => {
                // Convert TO_DATE(x) -> CAST(x AS DATE) for DuckDB
                if let Expression::Function(f) = e {
                    let arg = f.args.into_iter().next().unwrap();
                    Ok(Expression::Cast(Box::new(Cast {
                        this: arg,
                        to: DataType::Date,
                        double_colon_syntax: false,
                        trailing_comments: vec![],
                        format: None,
                        default: None,
                        inferred_type: None,
                    })))
                } else {
                    Ok(e)
                }
            }
            Action::BigQueryCastType => {
                // Convert BigQuery types to standard SQL types
                if let Expression::DataType(dt) = e {
                    match dt {
                        DataType::Custom { ref name } if name.eq_ignore_ascii_case("INT64") => {
                            Ok(Expression::DataType(DataType::BigInt { length: None }))
                        }
                        DataType::Custom { ref name } if name.eq_ignore_ascii_case("FLOAT64") => {
                            Ok(Expression::DataType(DataType::Double {
                                precision: None,
                                scale: None,
                            }))
                        }
                        DataType::Custom { ref name } if name.eq_ignore_ascii_case("BOOL") => {
                            Ok(Expression::DataType(DataType::Boolean))
                        }
                        DataType::Custom { ref name } if name.eq_ignore_ascii_case("BYTES") => {
                            Ok(Expression::DataType(DataType::VarBinary { length: None }))
                        }
                        DataType::Custom { ref name } if name.eq_ignore_ascii_case("NUMERIC") => {
                            // For DuckDB target, use Custom("DECIMAL") to avoid DuckDB's
                            // default precision (18, 3) being added to bare DECIMAL
                            if matches!(target, DialectType::DuckDB) {
                                Ok(Expression::DataType(DataType::Custom {
                                    name: "DECIMAL".to_string(),
                                }))
                            } else {
                                Ok(Expression::DataType(DataType::Decimal {
                                    precision: None,
                                    scale: None,
                                }))
                            }
                        }
                        DataType::Custom { ref name } if name.eq_ignore_ascii_case("STRING") => {
                            Ok(Expression::DataType(DataType::String { length: None }))
                        }
                        DataType::Custom { ref name } if name.eq_ignore_ascii_case("DATETIME") => {
                            Ok(Expression::DataType(DataType::Timestamp {
                                precision: None,
                                timezone: false,
                            }))
                        }
                        _ => Ok(Expression::DataType(dt)),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::BigQueryCastFormat => {
                // CAST(x AS DATE FORMAT 'fmt') -> PARSE_DATE('%m/%d/%Y', x) for BigQuery
                // CAST(x AS TIMESTAMP FORMAT 'fmt') -> PARSE_TIMESTAMP(...) for BigQuery
                // SAFE_CAST(x AS DATE FORMAT 'fmt') -> CAST(TRY_STRPTIME(x, ...) AS DATE) for DuckDB
                let (this, to, format_expr, is_safe) = match e {
                    Expression::Cast(ref c) if c.format.is_some() => (
                        c.this.clone(),
                        c.to.clone(),
                        c.format.as_ref().unwrap().as_ref().clone(),
                        false,
                    ),
                    Expression::SafeCast(ref c) if c.format.is_some() => (
                        c.this.clone(),
                        c.to.clone(),
                        c.format.as_ref().unwrap().as_ref().clone(),
                        true,
                    ),
                    _ => return Ok(e),
                };
                // For CAST(x AS STRING FORMAT ...) when target is BigQuery, keep as-is
                if matches!(target, DialectType::BigQuery) {
                    match &to {
                        DataType::String { .. } | DataType::VarChar { .. } | DataType::Text => {
                            // CAST(x AS STRING FORMAT 'fmt') stays as CAST expression for BigQuery
                            return Ok(e);
                        }
                        _ => {}
                    }
                }
                // Extract timezone from format if AT TIME ZONE is present
                let (actual_format_expr, timezone) = match &format_expr {
                    Expression::AtTimeZone(ref atz) => (atz.this.clone(), Some(atz.zone.clone())),
                    _ => (format_expr.clone(), None),
                };
                let strftime_fmt = temporal::bq_cast_format_to_strftime(&actual_format_expr);
                match target {
                    DialectType::BigQuery => {
                        // CAST(x AS DATE FORMAT 'fmt') -> PARSE_DATE(strftime_fmt, x)
                        // CAST(x AS TIMESTAMP FORMAT 'fmt' AT TIME ZONE 'tz') -> PARSE_TIMESTAMP(strftime_fmt, x, tz)
                        let func_name = match &to {
                            DataType::Date => "PARSE_DATE",
                            DataType::Timestamp { .. } => "PARSE_TIMESTAMP",
                            DataType::Time { .. } => "PARSE_TIMESTAMP",
                            _ => "PARSE_TIMESTAMP",
                        };
                        let mut func_args = vec![strftime_fmt, this];
                        if let Some(tz) = timezone {
                            func_args.push(tz);
                        }
                        Ok(Expression::Function(Box::new(Function::new(
                            func_name.to_string(),
                            func_args,
                        ))))
                    }
                    DialectType::DuckDB => {
                        // SAFE_CAST(x AS DATE FORMAT 'fmt') -> CAST(TRY_STRPTIME(x, fmt) AS DATE)
                        // CAST(x AS DATE FORMAT 'fmt') -> CAST(STRPTIME(x, fmt) AS DATE)
                        let duck_fmt = temporal::bq_format_to_duckdb(&strftime_fmt);
                        let parse_fn_name = if is_safe { "TRY_STRPTIME" } else { "STRPTIME" };
                        let parse_call = Expression::Function(Box::new(Function::new(
                            parse_fn_name.to_string(),
                            vec![this, duck_fmt],
                        )));
                        Ok(Expression::Cast(Box::new(Cast {
                            this: parse_call,
                            to,
                            trailing_comments: vec![],
                            double_colon_syntax: false,
                            format: None,
                            default: None,
                            inferred_type: None,
                        })))
                    }
                    _ => Ok(e),
                }
            }

            Action::TSQLTypeNormalize => {
                if let Expression::DataType(dt) = e {
                    let new_dt = match &dt {
                        DataType::Custom { name } if name.eq_ignore_ascii_case("MONEY") => {
                            DataType::Decimal {
                                precision: Some(15),
                                scale: Some(4),
                            }
                        }
                        DataType::Custom { name } if name.eq_ignore_ascii_case("SMALLMONEY") => {
                            DataType::Decimal {
                                precision: Some(6),
                                scale: Some(4),
                            }
                        }
                        DataType::Custom { name } if name.eq_ignore_ascii_case("DATETIME2") => {
                            DataType::Timestamp {
                                timezone: false,
                                precision: None,
                            }
                        }
                        DataType::Custom { name } if name.eq_ignore_ascii_case("REAL") => {
                            DataType::Float {
                                precision: None,
                                scale: None,
                                real_spelling: false,
                            }
                        }
                        DataType::Float {
                            real_spelling: true,
                            ..
                        } => DataType::Float {
                            precision: None,
                            scale: None,
                            real_spelling: false,
                        },
                        DataType::Custom { name } if name.eq_ignore_ascii_case("IMAGE") => {
                            DataType::Custom {
                                name: "BLOB".to_string(),
                            }
                        }
                        DataType::Custom { name } if name.eq_ignore_ascii_case("BIT") => {
                            DataType::Boolean
                        }
                        DataType::Custom { name } if name.eq_ignore_ascii_case("ROWVERSION") => {
                            DataType::Custom {
                                name: "BINARY".to_string(),
                            }
                        }
                        DataType::Custom { name }
                            if name.eq_ignore_ascii_case("UNIQUEIDENTIFIER") =>
                        {
                            match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => DataType::Custom {
                                    name: "STRING".to_string(),
                                },
                                _ => DataType::VarChar {
                                    length: Some(36),
                                    parenthesized_length: true,
                                },
                            }
                        }
                        DataType::Custom { name }
                            if name.eq_ignore_ascii_case("DATETIMEOFFSET") =>
                        {
                            match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => DataType::Timestamp {
                                    timezone: false,
                                    precision: None,
                                },
                                _ => DataType::Timestamp {
                                    timezone: true,
                                    precision: None,
                                },
                            }
                        }
                        DataType::Custom { ref name }
                            if name.len() >= 10
                                && name[..10].eq_ignore_ascii_case("DATETIME2(") =>
                        {
                            // DATETIME2(n) -> TIMESTAMP
                            DataType::Timestamp {
                                timezone: false,
                                precision: None,
                            }
                        }
                        DataType::Custom { ref name }
                            if name.len() >= 5 && name[..5].eq_ignore_ascii_case("TIME(") =>
                        {
                            // TIME(n) -> TIMESTAMP for Spark, keep as TIME for others
                            match target {
                                DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive => DataType::Timestamp {
                                    timezone: false,
                                    precision: None,
                                },
                                _ => return Ok(Expression::DataType(dt)),
                            }
                        }
                        DataType::Custom { ref name }
                            if name.len() >= 7 && name[..7].eq_ignore_ascii_case("NUMERIC") =>
                        {
                            // Parse NUMERIC(p,s) back to Decimal(p,s)
                            let upper = name.to_ascii_uppercase();
                            if let Some(inner) = upper
                                .strip_prefix("NUMERIC(")
                                .and_then(|s| s.strip_suffix(')'))
                            {
                                let parts: Vec<&str> = inner.split(',').collect();
                                let precision =
                                    parts.first().and_then(|s| s.trim().parse::<u32>().ok());
                                let scale = parts.get(1).and_then(|s| s.trim().parse::<u32>().ok());
                                DataType::Decimal { precision, scale }
                            } else if upper == "NUMERIC" {
                                DataType::Decimal {
                                    precision: None,
                                    scale: None,
                                }
                            } else {
                                return Ok(Expression::DataType(dt));
                            }
                        }
                        DataType::Float {
                            precision: Some(p), ..
                        } => {
                            // For Hive/Spark: FLOAT(1-32) -> FLOAT, FLOAT(33+) -> DOUBLE (IEEE 754 boundary)
                            // For other targets: FLOAT(1-24) -> FLOAT, FLOAT(25+) -> DOUBLE (TSQL boundary)
                            let boundary = match target {
                                DialectType::Hive
                                | DialectType::Spark
                                | DialectType::Databricks => 32,
                                _ => 24,
                            };
                            if *p <= boundary {
                                DataType::Float {
                                    precision: None,
                                    scale: None,
                                    real_spelling: false,
                                }
                            } else {
                                DataType::Double {
                                    precision: None,
                                    scale: None,
                                }
                            }
                        }
                        DataType::TinyInt { .. } => match target {
                            DialectType::DuckDB => DataType::Custom {
                                name: "UTINYINT".to_string(),
                            },
                            DialectType::Hive | DialectType::Spark | DialectType::Databricks => {
                                DataType::SmallInt { length: None }
                            }
                            _ => return Ok(Expression::DataType(dt)),
                        },
                        // INTEGER -> INT for Spark/Databricks
                        DataType::Int {
                            length,
                            integer_spelling: true,
                        } => DataType::Int {
                            length: *length,
                            integer_spelling: false,
                        },
                        _ => return Ok(Expression::DataType(dt)),
                    };
                    Ok(Expression::DataType(new_dt))
                } else {
                    Ok(e)
                }
            }
            Action::HiveCastToTryCast => {
                // Convert Hive/Spark CAST to TRY_CAST for targets that support it
                if let Expression::Cast(mut c) = e {
                    // For Spark/Hive -> DuckDB: TIMESTAMP -> TIMESTAMPTZ
                    // (Spark's TIMESTAMP is always timezone-aware)
                    if matches!(target, DialectType::DuckDB)
                        && matches!(source, DialectType::Spark | DialectType::Databricks)
                        && matches!(
                            c.to,
                            DataType::Timestamp {
                                timezone: false,
                                ..
                            }
                        )
                    {
                        c.to = DataType::Custom {
                            name: "TIMESTAMPTZ".to_string(),
                        };
                    }
                    // For Spark source -> Databricks: VARCHAR/CHAR -> STRING
                    // Spark parses VARCHAR(n)/CHAR(n) as TEXT, normalize to STRING
                    if matches!(target, DialectType::Databricks | DialectType::Spark)
                        && matches!(
                            source,
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive
                        )
                        && has_varchar_char_type(&c.to)
                    {
                        c.to = normalize_varchar_to_string(c.to);
                    }
                    Ok(Expression::TryCast(c))
                } else {
                    Ok(e)
                }
            }
            Action::CastTimestampStripTz => {
                // CAST(x AS TIMESTAMP(n) WITH TIME ZONE) -> CAST(x AS TIMESTAMP) for Hive/Spark/BigQuery
                let c = if let Expression::Cast(c) = e {
                    *c
                } else {
                    unreachable!("action only triggered for Cast expressions")
                };
                Ok(Expression::Cast(Box::new(Cast {
                    to: DataType::Timestamp {
                        precision: None,
                        timezone: false,
                    },
                    ..c
                })))
            }

            Action::BitAggFloatCast => {
                // BIT_OR/BIT_AND/BIT_XOR with float/decimal cast arg -> wrap with ROUND+INT cast for DuckDB
                // For FLOAT/DOUBLE/REAL: CAST(ROUND(CAST(val AS type)) AS INT)
                // For DECIMAL: CAST(CAST(val AS DECIMAL(p,s)) AS INT)
                let int_type = DataType::Int {
                    length: None,
                    integer_spelling: false,
                };
                let wrap_agg = |agg_this: Expression, int_dt: DataType| -> Expression {
                    if let Expression::Cast(c) = agg_this {
                        match &c.to {
                            DataType::Float { .. }
                            | DataType::Double { .. }
                            | DataType::Custom { .. } => {
                                // FLOAT/DOUBLE/REAL: CAST(ROUND(CAST(val AS type)) AS INT)
                                // Change FLOAT to REAL (Float with real_spelling=true) for DuckDB generator
                                let inner_type = match &c.to {
                                    DataType::Float {
                                        precision, scale, ..
                                    } => DataType::Float {
                                        precision: *precision,
                                        scale: *scale,
                                        real_spelling: true,
                                    },
                                    other => other.clone(),
                                };
                                let inner_cast =
                                    Expression::Cast(Box::new(crate::expressions::Cast {
                                        this: c.this.clone(),
                                        to: inner_type,
                                        trailing_comments: Vec::new(),
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }));
                                let rounded = Expression::Function(Box::new(Function::new(
                                    "ROUND".to_string(),
                                    vec![inner_cast],
                                )));
                                Expression::Cast(Box::new(crate::expressions::Cast {
                                    this: rounded,
                                    to: int_dt,
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))
                            }
                            DataType::Decimal { .. } => {
                                // DECIMAL: CAST(CAST(val AS DECIMAL(p,s)) AS INT)
                                Expression::Cast(Box::new(crate::expressions::Cast {
                                    this: Expression::Cast(c),
                                    to: int_dt,
                                    trailing_comments: Vec::new(),
                                    double_colon_syntax: false,
                                    format: None,
                                    default: None,
                                    inferred_type: None,
                                }))
                            }
                            _ => Expression::Cast(c),
                        }
                    } else {
                        agg_this
                    }
                };
                match e {
                    Expression::BitwiseOrAgg(mut f) => {
                        f.this = wrap_agg(f.this, int_type);
                        Ok(Expression::BitwiseOrAgg(f))
                    }
                    Expression::BitwiseAndAgg(mut f) => {
                        let int_type = DataType::Int {
                            length: None,
                            integer_spelling: false,
                        };
                        f.this = wrap_agg(f.this, int_type);
                        Ok(Expression::BitwiseAndAgg(f))
                    }
                    Expression::BitwiseXorAgg(mut f) => {
                        let int_type = DataType::Int {
                            length: None,
                            integer_spelling: false,
                        };
                        f.this = wrap_agg(f.this, int_type);
                        Ok(Expression::BitwiseXorAgg(f))
                    }
                    _ => Ok(e),
                }
            }

            Action::StrftimeCastTimestamp => {
                // CAST(x AS TIMESTAMP) -> CAST(x AS TIMESTAMP_NTZ) for Spark
                if let Expression::Cast(mut c) = e {
                    if matches!(
                        c.to,
                        DataType::Timestamp {
                            timezone: false,
                            ..
                        }
                    ) {
                        c.to = DataType::Custom {
                            name: "TIMESTAMP_NTZ".to_string(),
                        };
                    }
                    Ok(Expression::Cast(c))
                } else {
                    Ok(e)
                }
            }

            Action::SnowflakeFloatProtect => {
                // Convert DataType::Float to DataType::Custom("FLOAT") to prevent
                // Snowflake's target transform from converting it to DOUBLE.
                // Non-Snowflake sources should keep their FLOAT spelling.
                if let Expression::DataType(DataType::Float { .. }) = e {
                    Ok(Expression::DataType(DataType::Custom {
                        name: "FLOAT".to_string(),
                    }))
                } else {
                    Ok(e)
                }
            }

            Action::DecimalDefaultPrecision => {
                // DECIMAL without precision -> DECIMAL(18, 3) for Snowflake
                if let Expression::Cast(mut c) = e {
                    if matches!(
                        c.to,
                        DataType::Decimal {
                            precision: None,
                            ..
                        }
                    ) {
                        c.to = DataType::Decimal {
                            precision: Some(18),
                            scale: Some(3),
                        };
                    }
                    Ok(Expression::Cast(c))
                } else {
                    Ok(e)
                }
            }

            Action::SparkDateFuncCast => {
                // MONTH/YEAR/DAY('string') from Spark -> wrap arg in CAST to DATE
                let cast_arg = |arg: Expression| -> Expression {
                    match target {
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            temporal::double_cast_timestamp_date(arg)
                        }
                        _ => {
                            // DuckDB, PostgreSQL, etc: CAST(arg AS DATE)
                            temporal::ensure_cast_date(arg)
                        }
                    }
                };
                match e {
                    Expression::Month(f) => Ok(Expression::Month(Box::new(
                        crate::expressions::UnaryFunc::new(cast_arg(f.this)),
                    ))),
                    Expression::Year(f) => Ok(Expression::Year(Box::new(
                        crate::expressions::UnaryFunc::new(cast_arg(f.this)),
                    ))),
                    Expression::Day(f) => Ok(Expression::Day(Box::new(
                        crate::expressions::UnaryFunc::new(cast_arg(f.this)),
                    ))),
                    other => Ok(other),
                }
            }

            Action::MysqlCastCharToText => {
                // MySQL CAST(x AS CHAR) was originally TEXT -> convert to target text type
                if let Expression::Cast(mut c) = e {
                    c.to = DataType::Text;
                    Ok(Expression::Cast(c))
                } else {
                    Ok(e)
                }
            }

            Action::SparkCastVarcharToString => {
                // Spark parses VARCHAR(n)/CHAR(n) as TEXT -> normalize to STRING
                match e {
                    Expression::Cast(mut c) => {
                        c.to = normalize_varchar_to_string(c.to);
                        Ok(Expression::Cast(c))
                    }
                    Expression::TryCast(mut c) => {
                        c.to = normalize_varchar_to_string(c.to);
                        Ok(Expression::TryCast(c))
                    }
                    _ => Ok(e),
                }
            }

            Action::OracleVarchar2ToVarchar => {
                // Oracle VARCHAR2(N CHAR/BYTE) / NVARCHAR2(N) -> VarChar(N) for non-Oracle targets
                if let Expression::DataType(DataType::Custom { ref name }) = e {
                    // Extract length from VARCHAR2(N ...) or NVARCHAR2(N ...)
                    let starts_varchar2 =
                        name.len() >= 9 && name[..9].eq_ignore_ascii_case("VARCHAR2(");
                    let starts_nvarchar2 =
                        name.len() >= 10 && name[..10].eq_ignore_ascii_case("NVARCHAR2(");
                    let inner = if starts_varchar2 || starts_nvarchar2 {
                        let start = if starts_nvarchar2 { 10 } else { 9 }; // skip "NVARCHAR2(" or "VARCHAR2("
                        let end = name.len() - 1; // skip trailing ")"
                        Some(&name[start..end])
                    } else {
                        Option::None
                    };
                    if let Some(inner_str) = inner {
                        // Parse the number part, ignoring BYTE/CHAR qualifier
                        let num_str = inner_str.split_whitespace().next().unwrap_or("");
                        if let Ok(n) = num_str.parse::<u32>() {
                            Ok(Expression::DataType(DataType::VarChar {
                                length: Some(n),
                                parenthesized_length: false,
                            }))
                        } else {
                            Ok(e)
                        }
                    } else {
                        // Plain VARCHAR2 / NVARCHAR2 without parens
                        Ok(Expression::DataType(DataType::VarChar {
                            length: Option::None,
                            parenthesized_length: false,
                        }))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::CastTimestamptzToFunc => {
                // CAST(x AS TIMESTAMPTZ) -> TIMESTAMP(x) function for MySQL/StarRocks
                let c = if let Expression::Cast(c) = e {
                    *c
                } else {
                    unreachable!("action only triggered for Cast expressions")
                };
                Ok(Expression::Function(Box::new(Function::new(
                    "TIMESTAMP".to_string(),
                    vec![c.this],
                ))))
            }
        }
    })()?;

    Ok(RewriteOutcome::Rewritten(expression))
}

pub(super) fn is_postgres_unknown_type(data_type: &DataType) -> bool {
    matches!(data_type, DataType::Unknown)
        || matches!(data_type, DataType::Custom { name } if name.eq_ignore_ascii_case("UNKNOWN"))
}

pub(super) fn is_postgres_unknown_null_cast(cast: &Cast) -> bool {
    matches!(cast.this, Expression::Null(_)) && is_postgres_unknown_type(&cast.to)
}

fn without_parens(mut expression: &Expression) -> &Expression {
    while let Expression::Paren(paren) = expression {
        expression = &paren.this;
    }
    expression
}

fn without_parens_mut(mut expression: &mut Expression) -> &mut Expression {
    loop {
        match expression {
            Expression::Paren(paren) => expression = &mut paren.this,
            expression => return expression,
        }
    }
}

pub(super) fn is_postgres_float_to_integer_cast(cast: &Cast) -> bool {
    let is_float_type = |data_type: &DataType| {
        matches!(data_type, DataType::Float { .. } | DataType::Double { .. })
            || matches!(
                data_type,
                DataType::Custom { name }
                    if matches!(
                        name.to_ascii_uppercase().as_str(),
                        "REAL" | "FLOAT4" | "FLOAT8" | "DOUBLE PRECISION"
                    )
            )
    };
    let integer_target = matches!(
        cast.to,
        DataType::SmallInt { .. } | DataType::Int { .. } | DataType::BigInt { .. }
    );
    let float_input = match without_parens(&cast.this) {
        Expression::Cast(inner) | Expression::TryCast(inner) | Expression::SafeCast(inner) => {
            is_float_type(&inner.to)
        }
        expression => expression.inferred_type().is_some_and(is_float_type),
    };

    integer_target && float_input
}

pub(super) fn rewrite_postgres_float_to_integer_cast(mut cast: Cast) -> Expression {
    if is_postgres_float_to_integer_cast(&cast) {
        if let Expression::Cast(inner) | Expression::TryCast(inner) | Expression::SafeCast(inner) =
            without_parens_mut(&mut cast.this)
        {
            if let DataType::Custom { name } = &inner.to {
                inner.to = match name.to_ascii_uppercase().as_str() {
                    "REAL" | "FLOAT4" => DataType::Float {
                        precision: None,
                        scale: None,
                        real_spelling: true,
                    },
                    "FLOAT8" | "DOUBLE PRECISION" => DataType::Double {
                        precision: None,
                        scale: None,
                    },
                    _ => inner.to.clone(),
                };
            }
        }
        cast.this = Expression::Round(Box::new(RoundFunc {
            this: cast.this,
            decimals: Some(Expression::number(0)),
        }));
    }
    Expression::Cast(Box::new(cast))
}

pub(super) fn has_varchar_char_type(dt: &crate::expressions::DataType) -> bool {
    use crate::expressions::DataType;
    match dt {
        DataType::VarChar { .. } | DataType::Char { .. } => true,
        DataType::Struct { fields, .. } => {
            fields.iter().any(|f| has_varchar_char_type(&f.data_type))
        }
        _ => false,
    }
}

pub(super) fn normalize_varchar_to_string(
    dt: crate::expressions::DataType,
) -> crate::expressions::DataType {
    use crate::expressions::DataType;
    match dt {
        DataType::VarChar { .. } | DataType::Char { .. } => DataType::Custom {
            name: "STRING".to_string(),
        },
        DataType::Struct { fields, nested } => {
            let fields = fields
                .into_iter()
                .map(|mut f| {
                    f.data_type = normalize_varchar_to_string(f.data_type);
                    f
                })
                .collect();
            DataType::Struct { fields, nested }
        }
        other => other,
    }
}

pub(super) fn data_type_to_spark_string(dt: &crate::expressions::DataType) -> String {
    use crate::expressions::DataType;
    match dt {
        DataType::Int { .. } => "INT".to_string(),
        DataType::BigInt { .. } => "BIGINT".to_string(),
        DataType::SmallInt { .. } => "SMALLINT".to_string(),
        DataType::TinyInt { .. } => "TINYINT".to_string(),
        DataType::Float { .. } => "FLOAT".to_string(),
        DataType::Double { .. } => "DOUBLE".to_string(),
        DataType::Decimal {
            precision: Some(p),
            scale: Some(s),
        } => format!("DECIMAL({}, {})", p, s),
        DataType::Decimal {
            precision: Some(p), ..
        } => format!("DECIMAL({})", p),
        DataType::Decimal { .. } => "DECIMAL".to_string(),
        DataType::VarChar { .. } | DataType::Text | DataType::String { .. } => "STRING".to_string(),
        DataType::Char { .. } => "STRING".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Timestamp { .. } => "TIMESTAMP".to_string(),
        DataType::Json | DataType::JsonB => "STRING".to_string(),
        DataType::Binary { .. } => "BINARY".to_string(),
        DataType::Array { element_type, .. } => {
            format!("ARRAY<{}>", data_type_to_spark_string(element_type))
        }
        DataType::Map {
            key_type,
            value_type,
        } => format!(
            "MAP<{}, {}>",
            data_type_to_spark_string(key_type),
            data_type_to_spark_string(value_type)
        ),
        DataType::Struct { fields, .. } => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| {
                    if f.name.is_empty() {
                        data_type_to_spark_string(&f.data_type)
                    } else {
                        format!("{}: {}", f.name, data_type_to_spark_string(&f.data_type))
                    }
                })
                .collect();
            format!("STRUCT<{}>", field_strs.join(", "))
        }
        DataType::Custom { name } => name.clone(),
        _ => format!("{:?}", dt),
    }
}

pub(super) fn can_infer_presto_type(expr: &Expression) -> bool {
    match expr {
        Expression::Literal(_) => true,
        Expression::Boolean(_) => true,
        Expression::Array(_) | Expression::ArrayFunc(_) => true,
        Expression::Struct(_) | Expression::StructFunc(_) => true,
        Expression::Function(f) => {
            f.name.eq_ignore_ascii_case("STRUCT")
                || f.name.eq_ignore_ascii_case("ROW")
                || f.name.eq_ignore_ascii_case("CURRENT_DATE")
                || f.name.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
                || f.name.eq_ignore_ascii_case("NOW")
        }
        Expression::Cast(_) => true,
        Expression::Neg(inner) => can_infer_presto_type(&inner.this),
        _ => false,
    }
}

pub(super) fn infer_sql_type_for_presto(expr: &Expression) -> String {
    use crate::expressions::Literal;
    match expr {
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)) => {
            "VARCHAR".to_string()
        }
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(_)) => {
            let Literal::Number(n) = lit.as_ref() else {
                unreachable!()
            };
            if n.contains('.') {
                "DOUBLE".to_string()
            } else {
                "INTEGER".to_string()
            }
        }
        Expression::Boolean(_) => "BOOLEAN".to_string(),
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Date(_)) => "DATE".to_string(),
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Timestamp(_)) => {
            "TIMESTAMP".to_string()
        }
        Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Datetime(_)) => {
            "TIMESTAMP".to_string()
        }
        Expression::Array(_) | Expression::ArrayFunc(_) => "ARRAY(VARCHAR)".to_string(),
        Expression::Struct(_) | Expression::StructFunc(_) => "ROW".to_string(),
        Expression::Function(f) => {
            if f.name.eq_ignore_ascii_case("STRUCT") || f.name.eq_ignore_ascii_case("ROW") {
                "ROW".to_string()
            } else if f.name.eq_ignore_ascii_case("CURRENT_DATE") {
                "DATE".to_string()
            } else if f.name.eq_ignore_ascii_case("CURRENT_TIMESTAMP")
                || f.name.eq_ignore_ascii_case("NOW")
            {
                "TIMESTAMP".to_string()
            } else {
                "VARCHAR".to_string()
            }
        }
        Expression::Cast(c) => {
            // If already cast, use the target type
            data_type_to_presto_string(&c.to)
        }
        _ => "VARCHAR".to_string(),
    }
}

pub(super) fn data_type_to_presto_string(dt: &crate::expressions::DataType) -> String {
    use crate::expressions::DataType;
    match dt {
        DataType::VarChar { .. } | DataType::Text | DataType::String { .. } => {
            "VARCHAR".to_string()
        }
        DataType::Int { .. }
        | DataType::BigInt { .. }
        | DataType::SmallInt { .. }
        | DataType::TinyInt { .. } => "INTEGER".to_string(),
        DataType::Float { .. } | DataType::Double { .. } => "DOUBLE".to_string(),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Timestamp { .. } => "TIMESTAMP".to_string(),
        DataType::Struct { fields, .. } => {
            let field_strs: Vec<String> = fields
                .iter()
                .map(|f| format!("{} {}", f.name, data_type_to_presto_string(&f.data_type)))
                .collect();
            format!("ROW({})", field_strs.join(", "))
        }
        DataType::Array { element_type, .. } => {
            format!("ARRAY({})", data_type_to_presto_string(element_type))
        }
        DataType::Custom { name } => {
            // Pass through custom type names (e.g., "INTEGER", "VARCHAR" from earlier inference)
            name.clone()
        }
        _ => "VARCHAR".to_string(),
    }
}

pub(super) fn unit_cast_target_is_string(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Char { .. }
            | DataType::VarChar { .. }
            | DataType::String { .. }
            | DataType::Text
            | DataType::TextWithLength { .. }
    ) || matches!(
        data_type,
        DataType::Custom { name, .. }
            if name.eq_ignore_ascii_case("VARCHAR")
                || name.eq_ignore_ascii_case("NVARCHAR")
                || name.eq_ignore_ascii_case("VARCHAR(MAX)")
                || name.eq_ignore_ascii_case("NVARCHAR(MAX)")
    )
}
