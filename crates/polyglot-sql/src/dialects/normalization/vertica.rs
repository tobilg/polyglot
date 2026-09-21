//! Rewrites for Vertica-specific source semantics.
//!
//! Vertica shares most of its surface syntax with PostgreSQL, but a few functions
//! carry Vertica-only meaning that must be lowered before a foreign target sees them.

use super::*;
use crate::expressions::{
    AggFunc, AtTimeZone, DateAddFunc, GroupConcatFunc, Interval, IntervalUnit, IntervalUnitSpec,
    StringAggFunc, VarArgFunc,
};

/// Rewrite a single node parsed as Vertica for a non-Vertica target.
pub(super) fn normalize_from_vertica(e: Expression, target: DialectType) -> Result<Expression> {
    match e {
        Expression::Function(f) if f.args.is_empty() && !f.quoted => {
            match f.name.to_ascii_uppercase().as_str() {
                // GETDATE() and SYSDATE are the statement-start local timestamp, not the
                // transaction-start CURRENT_TIMESTAMP.
                "GETDATE" | "SYSDATE" => {
                    Ok(statement_timestamp(target, false).unwrap_or(Expression::Function(f)))
                }
                "GETUTCDATE" => {
                    Ok(statement_timestamp(target, true).unwrap_or(Expression::Function(f)))
                }
                _ => Ok(Expression::Function(f)),
            }
        }
        Expression::CurrentTimestamp(ts) if ts.sysdate => {
            Ok(statement_timestamp(target, false).unwrap_or(Expression::CurrentTimestamp(ts)))
        }
        Expression::Function(f) if !f.quoted => {
            match (f.name.to_ascii_uppercase().as_str(), f.args.len()) {
                // ZEROIFNULL(x) -> COALESCE(x, 0), except where it is native
                ("ZEROIFNULL", 1) if !matches!(target, DialectType::Snowflake) => {
                    let mut args = f.args;
                    args.push(Expression::number(0));
                    Ok(Expression::Coalesce(Box::new(VarArgFunc {
                        original_name: None,
                        expressions: args,
                        inferred_type: None,
                    })))
                }
                // TIMESTAMPADD(unit, n, ts) -> the portable date-add node
                ("TIMESTAMPADD", 3) => match timestamp_unit(&f.args[0]) {
                    Some(unit) => {
                        let mut args = f.args;
                        let this = args.pop().unwrap();
                        let interval = args.pop().unwrap();
                        if is_postgres_family(target) {
                            return Ok(postgres_interval_add(this, interval, unit));
                        }
                        Ok(Expression::DateAdd(Box::new(DateAddFunc {
                            this,
                            interval,
                            unit,
                        })))
                    }
                    None => Ok(Expression::Function(f)),
                },
                // APPROXIMATE_COUNT_DISTINCT(x) -> the portable approximate-distinct node
                ("APPROXIMATE_COUNT_DISTINCT", 1) => {
                    let this = f.args.into_iter().next().unwrap();
                    Ok(Expression::ApproxDistinct(Box::new(AggFunc {
                        this,
                        distinct: false,
                        filter: None,
                        order_by: Vec::new(),
                        name: None,
                        ignore_nulls: None,
                        having_max: None,
                        limit: None,
                        inferred_type: None,
                    })))
                }
                _ => Ok(Expression::Function(f)),
            }
        }
        Expression::ListAgg(f) => Ok(lower_listagg(*f, target)),
        // LISTAGG(...) WITHIN GROUP (ORDER BY ...): fold the ordering into the aggregate
        // when the target's native form carries it inline.
        Expression::WithinGroup(wg) if matches!(wg.this, Expression::ListAgg(_)) => {
            let crate::expressions::WithinGroup { this, order_by } = *wg;
            let Expression::ListAgg(mut f) = this else {
                unreachable!()
            };
            match lower_listagg(*f.clone(), target) {
                Expression::ListAgg(lowered) => Ok(Expression::WithinGroup(Box::new(
                    crate::expressions::WithinGroup {
                        this: Expression::ListAgg(lowered),
                        order_by,
                    },
                ))),
                _ => {
                    f.order_by = Some(order_by);
                    Ok(lower_listagg(*f, target))
                }
            }
        }
        other => Ok(other),
    }
}

/// Vertica statement-start timestamps, for targets that can express them.
fn statement_timestamp(target: DialectType, utc: bool) -> Option<Expression> {
    let timestamp = |this: Expression| {
        Expression::Cast(Box::new(Cast {
            this,
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
    };
    let at_utc = |this: Expression| {
        Expression::AtTimeZone(Box::new(AtTimeZone {
            this,
            zone: Expression::string("UTC"),
        }))
    };
    match target {
        // PostgreSQL has a true statement-start clock
        DialectType::PostgreSQL => {
            let now = Expression::Function(Box::new(Function::new(
                "STATEMENT_TIMESTAMP".to_string(),
                vec![],
            )));
            Some(timestamp(if utc { at_utc(now) } else { now }))
        }
        // These targets have the same statement-start functions natively
        DialectType::TSQL | DialectType::Fabric => None,
        DialectType::Redshift if !utc => None,
        _ if utc => Some(timestamp(at_utc(Expression::CurrentTimestamp(
            crate::expressions::CurrentTimestamp {
                precision: None,
                sysdate: false,
            },
        )))),
        _ => None,
    }
}

/// LISTAGG defaults to a ',' separator in Vertica; spell it out and pick the
/// target's native string-aggregation form.
fn lower_listagg(mut f: crate::expressions::ListAggFunc, target: DialectType) -> Expression {
    if f.separator.is_none() {
        f.separator = Some(Expression::string(","));
    }
    match target {
        DialectType::PostgreSQL
        | DialectType::Materialize
        | DialectType::RisingWave
        | DialectType::CockroachDB
        | DialectType::TSQL
        | DialectType::Fabric
        | DialectType::BigQuery => Expression::StringAgg(Box::new(StringAggFunc {
            this: f.this,
            separator: f.separator,
            order_by: f.order_by,
            distinct: f.distinct,
            filter: f.filter,
            limit: None,
            inferred_type: None,
        })),
        DialectType::MySQL
        | DialectType::TiDB
        | DialectType::SingleStore
        | DialectType::Doris
        | DialectType::StarRocks
        | DialectType::SQLite => Expression::GroupConcat(Box::new(GroupConcatFunc {
            this: f.this,
            separator: f.separator,
            order_by: f.order_by,
            distinct: f.distinct,
            filter: f.filter,
            limit: None,
            inferred_type: None,
        })),
        _ => Expression::ListAgg(Box::new(f)),
    }
}

/// Vertica datetime units accepted by TIMESTAMPADD, including the ODBC SQL_TSI_ forms.
fn timestamp_unit(unit: &Expression) -> Option<IntervalUnit> {
    let name = temporal::get_unit_str_static(unit);
    Some(match name.trim_start_matches("SQL_TSI_") {
        "YEAR" | "YEARS" | "YY" | "YYYY" => IntervalUnit::Year,
        "QUARTER" | "QUARTERS" | "QQ" | "Q" => IntervalUnit::Quarter,
        "MONTH" | "MONTHS" | "MM" | "M" => IntervalUnit::Month,
        "WEEK" | "WEEKS" | "WK" | "WW" => IntervalUnit::Week,
        "DAY" | "DAYS" | "DD" | "D" | "DAYOFYEAR" | "DY" | "Y" => IntervalUnit::Day,
        "HOUR" | "HOURS" | "HH" => IntervalUnit::Hour,
        "MINUTE" | "MINUTES" | "MI" | "N" => IntervalUnit::Minute,
        "SECOND" | "SECONDS" | "SS" | "S" => IntervalUnit::Second,
        "MILLISECOND" | "MILLISECONDS" | "MS" => IntervalUnit::Millisecond,
        "MICROSECOND" | "MICROSECONDS" | "US" => IntervalUnit::Microsecond,
        _ => return None,
    })
}

fn is_postgres_family(target: DialectType) -> bool {
    matches!(
        target,
        DialectType::PostgreSQL
            | DialectType::Materialize
            | DialectType::RisingWave
            | DialectType::CockroachDB
    )
}

/// `ts + INTERVAL 'n UNIT'` for literal amounts, `ts + INTERVAL '1 UNIT' * n` otherwise.
fn postgres_interval_add(ts: Expression, amount: Expression, unit: IntervalUnit) -> Expression {
    let interval = |value: String| {
        Expression::Interval(Box::new(Interval {
            this: Some(Expression::string(&value)),
            unit: Some(IntervalUnitSpec::Simple {
                unit,
                use_plural: false,
            }),
        }))
    };
    let offset = match amount {
        Expression::Literal(ref lit) if matches!(lit.as_ref(), Literal::Number(_)) => {
            let Literal::Number(n) = lit.as_ref() else {
                unreachable!()
            };
            interval(n.clone())
        }
        other => Expression::Mul(Box::new(BinaryOp::new(interval("1".to_string()), other))),
    };
    Expression::Add(Box::new(BinaryOp::new(ts, offset)))
}
