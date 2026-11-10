//! Generic `DATE_TRUNC(unit, x)` written for Spark and Databricks. Spark's
//! `TRUNC(date, fmt)` only takes YEAR, QUARTER, MONTH and WEEK (and their
//! abbreviations) and returns NULL for any other unit, so `TRUNC(x, 'DAY')`
//! is NULL on Spark 4.0.1. Those units keep `TRUNC`; every other unit is
//! written as `DATE_TRUNC(unit, x)`, which Spark takes for every unit.
use polyglot_sql::DialectType;

fn transpile(sql: &str, source: DialectType, target: DialectType) -> String {
    polyglot_sql::transpile(sql, source, target)
        .unwrap()
        .remove(0)
}

#[test]
fn units_spark_trunc_lacks_become_date_trunc() {
    for target in [DialectType::Spark, DialectType::Databricks] {
        for unit in ["day", "DAY", "hour", "minute", "second"] {
            let upper = unit.to_ascii_uppercase();
            assert_eq!(
                transpile(
                    &format!("SELECT DATE_TRUNC('{unit}', ts) FROM t"),
                    DialectType::Generic,
                    target
                ),
                format!("SELECT DATE_TRUNC('{upper}', ts) FROM t"),
                "{target:?} {unit}"
            );
        }
    }
}

#[test]
fn units_spark_trunc_takes_stay_trunc() {
    for target in [DialectType::Spark, DialectType::Databricks] {
        for unit in ["year", "quarter", "month", "week", "MM", "yyyy"] {
            let upper = unit.to_ascii_uppercase();
            assert_eq!(
                transpile(
                    &format!("SELECT DATE_TRUNC('{unit}', ts) FROM t"),
                    DialectType::Generic,
                    target
                ),
                format!("SELECT TRUNC(ts, '{upper}') FROM t"),
                "{target:?} {unit}"
            );
        }
    }
}

#[test]
fn spark_date_trunc_round_trips() {
    for target in [DialectType::Spark, DialectType::Databricks] {
        assert_eq!(
            transpile("SELECT DATE_TRUNC('HOUR', ts) FROM t", target, target),
            "SELECT DATE_TRUNC('HOUR', ts) FROM t"
        );
    }
}
