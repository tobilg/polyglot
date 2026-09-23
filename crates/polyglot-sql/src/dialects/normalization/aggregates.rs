use super::{
    scalar::{expression_numeric_kind, NumericKind},
    NormalizationContext, RewriteOutcome,
};
use crate::dialects::DialectType;
use crate::error::Result;
use crate::expressions::*;

#[derive(Debug)]
pub(super) enum Action {
    ArrayAggCollectList,
    ArrayAggWithinGroupFilter,
    ArrayAggFilter,
    BigQueryAnyValueHaving,
    BigQueryApproxQuantiles,
    StringAggConvert,
    GroupConcatConvert,
    MaxByMinByConvert,
    ArrayAggToCollectList,
    ArrayAggToGroupConcat,
    ArrayAggNullFilter,
    ArrayAggIgnoreNullsDuckDB,
    BigQueryPercentileContToDuckDB,
    CountDistinctMultiArg,
    VarianceToClickHouse,
    StddevToClickHouse,
    ApproxQuantileConvert,
    BitAggSnowflakeRename,
    AnyValueIgnoreNulls,
    AggFilterToIff,
    ApproxCountDistinctToApproxDistinct,
    CollectListToArrayAgg,
    CollectSetConvert,
    PercentileConvert,
    DuckDBQuantileConvert,
    CorrIsnanWrap,
    FirstToAnyValue,
    PercentileContConvert,
    ClickHouseUniqToApproxCountDistinct,
    ClickHouseAnyToAnyValue,
    SnowflakeMedianToClickHouse,
}

pub(super) fn rewrite(
    action: Action,
    expression: Expression,
    context: &NormalizationContext,
) -> Result<RewriteOutcome> {
    let target = context.target;
    let e = expression;
    let expression = (|| -> Result<Expression> {
        match action {
            Action::SnowflakeMedianToClickHouse => {
                let mut agg = if let Expression::Median(agg) = e {
                    *agg
                } else {
                    unreachable!("action only triggered for Median expressions")
                };

                let has_unsupported_modifiers = agg.distinct
                    || !agg.order_by.is_empty()
                    || agg.having_max.is_some()
                    || agg.limit.is_some()
                    || agg.ignore_nulls == Some(false);
                if has_unsupported_modifiers {
                    if context.strict {
                        return Err(crate::error::Error::unsupported(
                            "Snowflake MEDIAN modifiers cannot be preserved for ClickHouse",
                            target.to_string(),
                        ));
                    }
                    return Ok(Expression::Median(Box::new(agg)));
                }

                let mut input = agg.this;
                input = match expression_numeric_kind(&input) {
                    NumericKind::Float => input,
                    NumericKind::Integer => cast_integer_median_input(input),
                    NumericKind::Decimal => widen_zero_scale_median_input(input),
                    NumericKind::Unknown if context.strict => {
                        return Err(crate::error::Error::unsupported(
                            "Snowflake MEDIAN with an unresolved input type cannot be preserved",
                            target.to_string(),
                        ));
                    }
                    NumericKind::Unknown => input,
                };

                // ClickHouse aggregate functions ignore NULL inputs, so a SQL FILTER
                // can be preserved by feeding NULL to rows that do not match.
                if let Some(condition) = agg.filter.take() {
                    input = Expression::IfFunc(Box::new(IfFunc {
                        condition,
                        true_value: input,
                        false_value: Some(Expression::Null(Null)),
                        original_name: None,
                        inferred_type: None,
                    }));
                }

                Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                    source_dialect: None,

                    name: "medianExactWeightedInterpolatedOrNull".to_string(),
                    args: vec![input, Expression::number(1)],
                    distinct: false,
                    filter: None,
                    order_by: Vec::new(),
                    limit: None,
                    ignore_nulls: None,
                    inferred_type: agg.inferred_type,
                })))
            }
            Action::ArrayAggCollectList => {
                let agg = if let Expression::ArrayAgg(a) = e {
                    *a
                } else {
                    unreachable!("action only triggered for ArrayAgg expressions")
                };
                Ok(Expression::ArrayAgg(Box::new(AggFunc {
                    name: Some("COLLECT_LIST".to_string()),
                    ..agg
                })))
            }

            Action::ArrayAggWithinGroupFilter => {
                let wg = if let Expression::WithinGroup(w) = e {
                    *w
                } else {
                    unreachable!("action only triggered for WithinGroup expressions")
                };
                if let Expression::ArrayAgg(inner_agg) = wg.this {
                    let col = inner_agg.this.clone();
                    let filter = Expression::IsNull(Box::new(IsNull {
                        this: col,
                        not: true,
                        postfix_form: false,
                    }));
                    // For DuckDB, add explicit NULLS FIRST for DESC ordering
                    let order_by = if matches!(target, DialectType::DuckDB) {
                        wg.order_by
                            .into_iter()
                            .map(|mut o| {
                                if o.desc && o.nulls_first.is_none() {
                                    o.nulls_first = Some(true);
                                }
                                o
                            })
                            .collect()
                    } else {
                        wg.order_by
                    };
                    Ok(Expression::ArrayAgg(Box::new(AggFunc {
                        this: inner_agg.this,
                        distinct: inner_agg.distinct,
                        filter: Some(filter),
                        order_by,
                        name: inner_agg.name,
                        ignore_nulls: inner_agg.ignore_nulls,
                        having_max: inner_agg.having_max,
                        limit: inner_agg.limit,
                        inferred_type: None,
                    })))
                } else {
                    Ok(Expression::WithinGroup(Box::new(wg)))
                }
            }

            Action::ArrayAggFilter => {
                let agg = if let Expression::ArrayAgg(a) = e {
                    *a
                } else {
                    unreachable!("action only triggered for ArrayAgg expressions")
                };
                let col = agg.this.clone();
                let filter = Expression::IsNull(Box::new(IsNull {
                    this: col,
                    not: true,
                    postfix_form: false,
                }));
                Ok(Expression::ArrayAgg(Box::new(AggFunc {
                    filter: Some(filter),
                    ..agg
                })))
            }

            Action::BigQueryAnyValueHaving => {
                // ANY_VALUE(x HAVING MAX y) -> ARG_MAX_NULL(x, y)
                // ANY_VALUE(x HAVING MIN y) -> ARG_MIN_NULL(x, y)
                if let Expression::AnyValue(agg) = e {
                    if let Some((having_expr, is_max)) = agg.having_max {
                        let func_name = if is_max {
                            "ARG_MAX_NULL"
                        } else {
                            "ARG_MIN_NULL"
                        };
                        Ok(Expression::AggregateFunction(Box::new(AggregateFunction {
                            source_dialect: None,

                            name: func_name.to_string(),
                            args: vec![agg.this, *having_expr],
                            distinct: agg.distinct,
                            filter: agg.filter,
                            order_by: agg.order_by,
                            limit: agg.limit,
                            ignore_nulls: agg.ignore_nulls,
                            inferred_type: agg.inferred_type,
                        })))
                    } else {
                        Ok(Expression::AnyValue(agg))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::BigQueryApproxQuantiles => {
                // APPROX_QUANTILES(x, n) -> APPROX_QUANTILE(x, [0, 1/n, 2/n, ..., 1])
                // APPROX_QUANTILES(DISTINCT x, n) -> APPROX_QUANTILE(DISTINCT x, [0, 1/n, ..., 1])
                if let Expression::AggregateFunction(agg) = e {
                    if agg.args.len() >= 2 {
                        let x_expr = agg.args[0].clone();
                        let n_expr = &agg.args[1];

                        // Extract the numeric value from n_expr
                        let n = match n_expr {
                            Expression::Literal(lit)
                                if matches!(
                                    lit.as_ref(),
                                    crate::expressions::Literal::Number(_)
                                ) =>
                            {
                                let crate::expressions::Literal::Number(s) = lit.as_ref() else {
                                    unreachable!()
                                };
                                s.parse::<usize>().unwrap_or(2)
                            }
                            _ => 2,
                        };

                        // Generate quantile array: [0, 1/n, 2/n, ..., 1]
                        let mut quantiles = Vec::new();
                        for i in 0..=n {
                            let q = i as f64 / n as f64;
                            // Format nicely: 0 -> 0, 0.25 -> 0.25, 1 -> 1
                            if q == 0.0 {
                                quantiles.push(Expression::number(0));
                            } else if q == 1.0 {
                                quantiles.push(Expression::number(1));
                            } else {
                                quantiles.push(Expression::Literal(Box::new(
                                    crate::expressions::Literal::Number(format!("{}", q)),
                                )));
                            }
                        }

                        let array_expr = Expression::Array(Box::new(crate::expressions::Array {
                            expressions: quantiles,
                            inferred_type: None,
                        }));

                        let mut new_agg = *agg;
                        new_agg.name = "APPROX_QUANTILE".to_string();
                        new_agg.args = vec![x_expr, array_expr];
                        Ok(Expression::AggregateFunction(Box::new(new_agg)))
                    } else {
                        Ok(Expression::AggregateFunction(agg))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::StringAggConvert => {
                match e {
                    Expression::WithinGroup(wg) => {
                        // STRING_AGG(x, sep) WITHIN GROUP (ORDER BY z) -> target-specific
                        // Extract args and distinct flag from either Function, AggregateFunction, or StringAgg
                        let (x_opt, sep_opt, distinct) = match wg.this {
                            Expression::AggregateFunction(ref af)
                                if af.name.eq_ignore_ascii_case("STRING_AGG")
                                    && af.args.len() >= 2 =>
                            {
                                (
                                    Some(af.args[0].clone()),
                                    Some(af.args[1].clone()),
                                    af.distinct,
                                )
                            }
                            Expression::Function(ref f)
                                if f.name.eq_ignore_ascii_case("STRING_AGG")
                                    && f.args.len() >= 2 =>
                            {
                                (Some(f.args[0].clone()), Some(f.args[1].clone()), false)
                            }
                            Expression::StringAgg(ref sa) => {
                                (Some(sa.this.clone()), sa.separator.clone(), sa.distinct)
                            }
                            _ => (None, None, false),
                        };
                        if let (Some(x), Some(sep)) = (x_opt, sep_opt) {
                            let order_by = wg.order_by;

                            match target {
                                DialectType::TSQL | DialectType::Fabric => {
                                    // Keep as WithinGroup(StringAgg) for TSQL
                                    Ok(Expression::WithinGroup(Box::new(
                                        crate::expressions::WithinGroup {
                                            this: Expression::StringAgg(Box::new(
                                                crate::expressions::StringAggFunc {
                                                    this: x,
                                                    separator: Some(sep),
                                                    order_by: None, // order_by goes in WithinGroup, not StringAgg
                                                    distinct,
                                                    filter: None,
                                                    limit: None,
                                                    inferred_type: None,
                                                },
                                            )),
                                            order_by,
                                        },
                                    )))
                                }
                                DialectType::MySQL
                                | DialectType::SingleStore
                                | DialectType::Doris
                                | DialectType::StarRocks => {
                                    // GROUP_CONCAT(x ORDER BY z SEPARATOR sep)
                                    Ok(Expression::GroupConcat(Box::new(
                                        crate::expressions::GroupConcatFunc {
                                            this: x,
                                            separator: Some(sep),
                                            order_by: Some(order_by),
                                            distinct,
                                            filter: None,
                                            limit: None,
                                            inferred_type: None,
                                        },
                                    )))
                                }
                                DialectType::SQLite => {
                                    // GROUP_CONCAT(x, sep) - no ORDER BY support
                                    Ok(Expression::GroupConcat(Box::new(
                                        crate::expressions::GroupConcatFunc {
                                            this: x,
                                            separator: Some(sep),
                                            order_by: None,
                                            distinct,
                                            filter: None,
                                            limit: None,
                                            inferred_type: None,
                                        },
                                    )))
                                }
                                DialectType::PostgreSQL | DialectType::Redshift => {
                                    // STRING_AGG(x, sep ORDER BY z)
                                    Ok(Expression::StringAgg(Box::new(
                                        crate::expressions::StringAggFunc {
                                            this: x,
                                            separator: Some(sep),
                                            order_by: Some(order_by),
                                            distinct,
                                            filter: None,
                                            limit: None,
                                            inferred_type: None,
                                        },
                                    )))
                                }
                                _ => {
                                    // Default: keep as STRING_AGG(x, sep) with ORDER BY inside
                                    Ok(Expression::StringAgg(Box::new(
                                        crate::expressions::StringAggFunc {
                                            this: x,
                                            separator: Some(sep),
                                            order_by: Some(order_by),
                                            distinct,
                                            filter: None,
                                            limit: None,
                                            inferred_type: None,
                                        },
                                    )))
                                }
                            }
                        } else {
                            Ok(Expression::WithinGroup(wg))
                        }
                    }
                    Expression::StringAgg(sa) => {
                        match target {
                            DialectType::MySQL
                            | DialectType::SingleStore
                            | DialectType::Doris
                            | DialectType::StarRocks => {
                                // STRING_AGG(x, sep) -> GROUP_CONCAT(x SEPARATOR sep)
                                Ok(Expression::GroupConcat(Box::new(
                                    crate::expressions::GroupConcatFunc {
                                        this: sa.this,
                                        separator: sa.separator,
                                        order_by: sa.order_by,
                                        distinct: sa.distinct,
                                        filter: sa.filter,
                                        limit: None,
                                        inferred_type: None,
                                    },
                                )))
                            }
                            DialectType::SQLite => {
                                // STRING_AGG(x, sep) -> GROUP_CONCAT(x, sep)
                                Ok(Expression::GroupConcat(Box::new(
                                    crate::expressions::GroupConcatFunc {
                                        this: sa.this,
                                        separator: sa.separator,
                                        order_by: None, // SQLite doesn't support ORDER BY in GROUP_CONCAT
                                        distinct: sa.distinct,
                                        filter: sa.filter,
                                        limit: None,
                                        inferred_type: None,
                                    },
                                )))
                            }
                            DialectType::Spark | DialectType::Databricks => {
                                // STRING_AGG(x, sep) -> LISTAGG(x, sep)
                                Ok(Expression::ListAgg(Box::new(
                                    crate::expressions::ListAggFunc {
                                        this: sa.this,
                                        separator: sa.separator,
                                        on_overflow: None,
                                        order_by: sa.order_by,
                                        distinct: sa.distinct,
                                        filter: None,
                                        inferred_type: None,
                                    },
                                )))
                            }
                            _ => Ok(Expression::StringAgg(sa)),
                        }
                    }
                    _ => Ok(e),
                }
            }
            Action::GroupConcatConvert => {
                // Helper to expand CONCAT(a, b, c) -> a || b || c (for PostgreSQL/SQLite)
                // or CONCAT(a, b, c) -> a + b + c (for TSQL)
                fn expand_concat_to_dpipe(expr: Expression) -> Expression {
                    if let Expression::Function(ref f) = expr {
                        if f.name.eq_ignore_ascii_case("CONCAT") && f.args.len() > 1 {
                            let mut result = f.args[0].clone();
                            for arg in &f.args[1..] {
                                result = Expression::Concat(Box::new(BinaryOp {
                                    left: result,
                                    right: arg.clone(),
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                }));
                            }
                            return result;
                        }
                    }
                    expr
                }
                fn expand_concat_to_plus(expr: Expression) -> Expression {
                    if let Expression::Function(ref f) = expr {
                        if f.name.eq_ignore_ascii_case("CONCAT") && f.args.len() > 1 {
                            let mut result = f.args[0].clone();
                            for arg in &f.args[1..] {
                                result = Expression::Add(Box::new(BinaryOp {
                                    left: result,
                                    right: arg.clone(),
                                    left_comments: vec![],
                                    operator_comments: vec![],
                                    trailing_comments: vec![],
                                    inferred_type: None,
                                }));
                            }
                            return result;
                        }
                    }
                    expr
                }
                // Helper to wrap each arg in CAST(arg AS VARCHAR) for Presto/Trino CONCAT
                fn wrap_concat_args_in_varchar_cast(expr: Expression) -> Expression {
                    if let Expression::Function(ref f) = expr {
                        if f.name.eq_ignore_ascii_case("CONCAT") && f.args.len() > 1 {
                            let new_args: Vec<Expression> = f
                                .args
                                .iter()
                                .map(|arg| {
                                    Expression::Cast(Box::new(crate::expressions::Cast {
                                        this: arg.clone(),
                                        to: crate::expressions::DataType::VarChar {
                                            length: None,
                                            parenthesized_length: false,
                                        },
                                        trailing_comments: Vec::new(),
                                        double_colon_syntax: false,
                                        format: None,
                                        default: None,
                                        inferred_type: None,
                                    }))
                                })
                                .collect();
                            return Expression::Function(Box::new(
                                crate::expressions::Function::new("CONCAT".to_string(), new_args),
                            ));
                        }
                    }
                    expr
                }
                if let Expression::GroupConcat(gc) = e {
                    match target {
                        DialectType::Presto => {
                            // GROUP_CONCAT(x [, sep]) -> ARRAY_JOIN(ARRAY_AGG(x), sep)
                            let sep = gc.separator.unwrap_or(Expression::string(","));
                            // For multi-arg CONCAT, wrap each arg in CAST(... AS VARCHAR)
                            let this = wrap_concat_args_in_varchar_cast(gc.this);
                            let array_agg =
                                Expression::ArrayAgg(Box::new(crate::expressions::AggFunc {
                                    this,
                                    distinct: gc.distinct,
                                    filter: gc.filter,
                                    order_by: gc.order_by.unwrap_or_default(),
                                    name: None,
                                    ignore_nulls: None,
                                    having_max: None,
                                    limit: None,
                                    inferred_type: None,
                                }));
                            Ok(Expression::ArrayJoin(Box::new(
                                crate::expressions::ArrayJoinFunc {
                                    this: array_agg,
                                    separator: sep,
                                    null_replacement: None,
                                },
                            )))
                        }
                        DialectType::Trino => {
                            // GROUP_CONCAT(x [, sep]) -> LISTAGG(x, sep)
                            let sep = gc.separator.unwrap_or(Expression::string(","));
                            // For multi-arg CONCAT, wrap each arg in CAST(... AS VARCHAR)
                            let this = wrap_concat_args_in_varchar_cast(gc.this);
                            Ok(Expression::ListAgg(Box::new(
                                crate::expressions::ListAggFunc {
                                    this,
                                    separator: Some(sep),
                                    on_overflow: None,
                                    order_by: gc.order_by,
                                    distinct: gc.distinct,
                                    filter: gc.filter,
                                    inferred_type: None,
                                },
                            )))
                        }
                        DialectType::PostgreSQL
                        | DialectType::Redshift
                        | DialectType::Snowflake
                        | DialectType::DuckDB
                        | DialectType::Hive
                        | DialectType::ClickHouse => {
                            // GROUP_CONCAT(x [, sep]) -> STRING_AGG(x, sep)
                            let sep = gc.separator.unwrap_or(Expression::string(","));
                            // Expand CONCAT(a,b,c) -> a || b || c for || dialects
                            let this = expand_concat_to_dpipe(gc.this);
                            // For PostgreSQL, add NULLS LAST for DESC / NULLS FIRST for ASC
                            let order_by = if target == DialectType::PostgreSQL {
                                gc.order_by.map(|ords| {
                                    ords.into_iter()
                                        .map(|mut o| {
                                            if o.nulls_first.is_none() {
                                                if o.desc {
                                                    o.nulls_first = Some(false);
                                                // NULLS LAST
                                                } else {
                                                    o.nulls_first = Some(true);
                                                    // NULLS FIRST
                                                }
                                            }
                                            o
                                        })
                                        .collect()
                                })
                            } else {
                                gc.order_by
                            };
                            Ok(Expression::StringAgg(Box::new(
                                crate::expressions::StringAggFunc {
                                    this,
                                    separator: Some(sep),
                                    order_by,
                                    distinct: gc.distinct,
                                    filter: gc.filter,
                                    limit: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                        DialectType::TSQL => {
                            // GROUP_CONCAT(x [, sep]) -> STRING_AGG(x, sep) WITHIN GROUP (ORDER BY ...)
                            // TSQL doesn't support DISTINCT in STRING_AGG
                            let sep = gc.separator.unwrap_or(Expression::string(","));
                            // Expand CONCAT(a,b,c) -> a + b + c for TSQL
                            let this = expand_concat_to_plus(gc.this);
                            Ok(Expression::StringAgg(Box::new(
                                crate::expressions::StringAggFunc {
                                    this,
                                    separator: Some(sep),
                                    order_by: gc.order_by,
                                    distinct: false, // TSQL doesn't support DISTINCT in STRING_AGG
                                    filter: gc.filter,
                                    limit: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                        DialectType::SQLite => {
                            // GROUP_CONCAT stays as GROUP_CONCAT but ORDER BY is removed
                            // SQLite GROUP_CONCAT doesn't support ORDER BY
                            // Expand CONCAT(a,b,c) -> a || b || c
                            let this = expand_concat_to_dpipe(gc.this);
                            Ok(Expression::GroupConcat(Box::new(
                                crate::expressions::GroupConcatFunc {
                                    this,
                                    separator: gc.separator,
                                    order_by: None, // SQLite doesn't support ORDER BY in GROUP_CONCAT
                                    distinct: gc.distinct,
                                    filter: gc.filter,
                                    limit: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                        DialectType::Spark | DialectType::Databricks => {
                            // GROUP_CONCAT(x [, sep]) -> LISTAGG(x, sep)
                            let sep = gc.separator.unwrap_or(Expression::string(","));
                            Ok(Expression::ListAgg(Box::new(
                                crate::expressions::ListAggFunc {
                                    this: gc.this,
                                    separator: Some(sep),
                                    on_overflow: None,
                                    order_by: gc.order_by,
                                    distinct: gc.distinct,
                                    filter: None,
                                    inferred_type: None,
                                },
                            )))
                        }
                        DialectType::MySQL | DialectType::SingleStore | DialectType::StarRocks => {
                            // MySQL GROUP_CONCAT should have explicit SEPARATOR (default ',')
                            if gc.separator.is_none() {
                                let mut gc = gc;
                                gc.separator = Some(Expression::string(","));
                                Ok(Expression::GroupConcat(gc))
                            } else {
                                Ok(Expression::GroupConcat(gc))
                            }
                        }
                        _ => Ok(Expression::GroupConcat(gc)),
                    }
                } else {
                    Ok(e)
                }
            }
            Action::MaxByMinByConvert => {
                // MAX_BY -> argMax for ClickHouse, drop 3rd arg for Spark
                // MIN_BY -> argMin for ClickHouse, ARG_MIN for DuckDB, drop 3rd arg for Spark/ClickHouse
                // Handle both Expression::Function and Expression::AggregateFunction
                let (is_max, args) = match &e {
                    Expression::Function(f) => {
                        (f.name.eq_ignore_ascii_case("MAX_BY"), f.args.clone())
                    }
                    Expression::AggregateFunction(af) => {
                        (af.name.eq_ignore_ascii_case("MAX_BY"), af.args.clone())
                    }
                    _ => return Ok(e),
                };
                match target {
                    DialectType::ClickHouse => {
                        let name = if is_max { "argMax" } else { "argMin" };
                        let mut args = args;
                        args.truncate(2);
                        Ok(Expression::Function(Box::new(Function::new(
                            name.to_string(),
                            args,
                        ))))
                    }
                    DialectType::DuckDB => {
                        let preserve_null_value = args.len() == 2
                            && matches!(
                                context.source,
                                DialectType::Athena | DialectType::Presto | DialectType::Trino
                            );
                        // Trino/Presto retain a NULL value when its key wins.
                        // DuckDB's ordinary ARG_MAX/ARG_MIN skip such rows.
                        let name = match (is_max, preserve_null_value) {
                            (true, true) => "ARG_MAX_NULL",
                            (false, true) => "ARG_MIN_NULL",
                            (true, false) => "ARG_MAX",
                            (false, false) => "ARG_MIN",
                        };
                        // Renaming must retain FILTER and other aggregate modifiers.
                        match e {
                            Expression::AggregateFunction(mut af) => {
                                af.name = name.to_string();
                                Ok(Expression::AggregateFunction(af))
                            }
                            Expression::Function(mut f) => {
                                f.name = name.to_string();
                                Ok(Expression::Function(f))
                            }
                            _ => Ok(e),
                        }
                    }
                    DialectType::Spark | DialectType::Databricks => {
                        let mut args = args;
                        args.truncate(2);
                        let name = if is_max { "MAX_BY" } else { "MIN_BY" };
                        Ok(Expression::Function(Box::new(Function::new(
                            name.to_string(),
                            args,
                        ))))
                    }
                    _ => Ok(e),
                }
            }

            Action::ArrayAggToCollectList => {
                // ARRAY_AGG(x ORDER BY ...) -> COLLECT_LIST(x) for Hive/Spark
                match e {
                    Expression::AggregateFunction(mut af) => {
                        let preserves_order = af.limit.is_some();
                        let args = if af.args.is_empty() {
                            vec![]
                        } else {
                            vec![af.args[0].clone()]
                        };
                        af.name = "COLLECT_LIST".to_string();
                        af.args = args;
                        if !preserves_order {
                            af.order_by = Vec::new();
                        }
                        Ok(Expression::AggregateFunction(af))
                    }
                    Expression::ArrayAgg(agg) => {
                        let preserves_order = agg.limit.is_some();
                        Ok(Expression::AggregateFunction(Box::new(
                            crate::expressions::AggregateFunction {
                                source_dialect: None,

                                name: "COLLECT_LIST".to_string(),
                                args: vec![agg.this.clone()],
                                distinct: agg.distinct,
                                filter: agg.filter.clone(),
                                order_by: if preserves_order {
                                    agg.order_by.clone()
                                } else {
                                    Vec::new()
                                },
                                limit: agg.limit.clone(),
                                ignore_nulls: agg.ignore_nulls,
                                inferred_type: None,
                            },
                        )))
                    }
                    _ => Ok(e),
                }
            }

            Action::ArrayAggToGroupConcat => {
                let agg = if let Expression::ArrayAgg(a) = e {
                    *a
                } else {
                    unreachable!("action only triggered for ArrayAgg expressions")
                };
                Ok(Expression::ArrayAgg(Box::new(AggFunc {
                    name: Some("GROUP_CONCAT".to_string()),
                    ..agg
                })))
            }

            Action::ArrayAggNullFilter => {
                // ARRAY_AGG(x) FILTER(WHERE cond) -> ARRAY_AGG(x) FILTER(WHERE cond AND NOT x IS NULL)
                // For source dialects that exclude NULLs (Spark/Hive) targeting DuckDB which includes them
                let agg = if let Expression::ArrayAgg(a) = e {
                    *a
                } else {
                    unreachable!("action only triggered for ArrayAgg expressions")
                };
                let col = agg.this.clone();
                let not_null = Expression::IsNull(Box::new(IsNull {
                    this: col,
                    not: true,
                    postfix_form: true, // Use "NOT x IS NULL" form (prefix NOT)
                }));
                let new_filter = if let Some(existing_filter) = agg.filter {
                    // AND the NOT IS NULL with existing filter
                    Expression::And(Box::new(crate::expressions::BinaryOp::new(
                        existing_filter,
                        not_null,
                    )))
                } else {
                    not_null
                };
                Ok(Expression::ArrayAgg(Box::new(AggFunc {
                    filter: Some(new_filter),
                    ..agg
                })))
            }

            Action::ArrayAggIgnoreNullsDuckDB => {
                // ARRAY_AGG(x IGNORE NULLS ...) -> ARRAY_AGG(x ...) FILTER(WHERE x IS NOT NULL)
                // DuckDB also needs NULLS FIRST on the first inline ordering expression.
                let mut agg = if let Expression::ArrayAgg(a) = e {
                    *a
                } else {
                    unreachable!("action only triggered for ArrayAgg expressions")
                };
                agg.ignore_nulls = None;
                agg.filter = Some(Expression::IsNull(Box::new(IsNull {
                    this: agg.this.clone(),
                    not: true,
                    postfix_form: false,
                })));
                if !agg.order_by.is_empty() {
                    agg.order_by[0].nulls_first = Some(true);
                }
                Ok(Expression::ArrayAgg(Box::new(agg)))
            }

            Action::BigQueryPercentileContToDuckDB => {
                // PERCENTILE_CONT(x, frac [RESPECT NULLS]) -> QUANTILE_CONT(x, frac) for DuckDB
                if let Expression::AggregateFunction(mut af) = e {
                    af.name = "QUANTILE_CONT".to_string();
                    af.ignore_nulls = None; // Strip RESPECT/IGNORE NULLS
                                            // Keep only first 2 args
                    if af.args.len() > 2 {
                        af.args.truncate(2);
                    }
                    Ok(Expression::AggregateFunction(af))
                } else {
                    Ok(e)
                }
            }

            Action::CountDistinctMultiArg => {
                // COUNT(DISTINCT a, b) -> COUNT(DISTINCT CASE WHEN a IS NULL THEN NULL WHEN b IS NULL THEN NULL ELSE (a, b) END)
                if let Expression::Count(c) = e {
                    if let Some(Expression::Tuple(t)) = c.this {
                        let args = t.expressions;
                        // Build CASE expression:
                        // WHEN a IS NULL THEN NULL WHEN b IS NULL THEN NULL ELSE (a, b) END
                        let mut whens = Vec::new();
                        for arg in &args {
                            whens.push((
                                Expression::IsNull(Box::new(IsNull {
                                    this: arg.clone(),
                                    not: false,
                                    postfix_form: false,
                                })),
                                Expression::Null(crate::expressions::Null),
                            ));
                        }
                        // Build the tuple for ELSE
                        let tuple_expr = Expression::Tuple(Box::new(crate::expressions::Tuple {
                            expressions: args,
                        }));
                        let case_expr = Expression::Case(Box::new(crate::expressions::Case {
                            operand: None,
                            whens,
                            else_: Some(tuple_expr),
                            comments: Vec::new(),
                            inferred_type: None,
                        }));
                        Ok(Expression::Count(Box::new(crate::expressions::CountFunc {
                            this: Some(case_expr),
                            star: false,
                            distinct: true,
                            filter: c.filter,
                            ignore_nulls: c.ignore_nulls,
                            original_name: c.original_name,
                            inferred_type: None,
                        })))
                    } else {
                        Ok(Expression::Count(c))
                    }
                } else {
                    Ok(e)
                }
            }

            Action::VarianceToClickHouse => {
                if let Expression::Variance(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "varSamp".to_string(),
                        vec![f.this],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::StddevToClickHouse => {
                if let Expression::Stddev(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "stddevSamp".to_string(),
                        vec![f.this],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ApproxQuantileConvert => {
                if let Expression::ApproxQuantile(aq) = e {
                    let mut args = vec![*aq.this];
                    if let Some(q) = aq.quantile {
                        args.push(*q);
                    }
                    Ok(Expression::Function(Box::new(Function::new(
                        "APPROX_PERCENTILE".to_string(),
                        args,
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::BitAggSnowflakeRename => {
                // BIT_OR -> BITORAGG, BIT_AND -> BITANDAGG, BIT_XOR -> BITXORAGG for Snowflake
                match e {
                    Expression::BitwiseOrAgg(f) => Ok(Expression::Function(Box::new(
                        Function::new("BITORAGG".to_string(), vec![f.this]),
                    ))),
                    Expression::BitwiseAndAgg(f) => Ok(Expression::Function(Box::new(
                        Function::new("BITANDAGG".to_string(), vec![f.this]),
                    ))),
                    Expression::BitwiseXorAgg(f) => Ok(Expression::Function(Box::new(
                        Function::new("BITXORAGG".to_string(), vec![f.this]),
                    ))),
                    _ => Ok(e),
                }
            }

            Action::AnyValueIgnoreNulls => {
                // ANY_VALUE(x) -> ANY_VALUE(x) IGNORE NULLS for Spark
                if let Expression::AnyValue(mut av) = e {
                    if av.ignore_nulls.is_none() {
                        av.ignore_nulls = Some(true);
                    }
                    Ok(Expression::AnyValue(av))
                } else {
                    Ok(e)
                }
            }

            Action::AggFilterToIff => {
                // AggFunc.filter -> IFF wrapping: AVG(x) FILTER(WHERE cond) -> AVG(IFF(cond, x, NULL))
                let prefix_not_null = |condition: Expression| match condition {
                    Expression::IsNull(mut is_null) if is_null.not => {
                        is_null.postfix_form = true;
                        Expression::IsNull(is_null)
                    }
                    other => other,
                };
                // Helper macro to handle the common AggFunc case
                macro_rules! handle_agg_filter_to_iff {
                    ($variant:ident, $agg:expr) => {{
                        let mut agg = $agg;
                        if let Some(filter_cond) = agg.filter.take() {
                            let filter_cond = prefix_not_null(filter_cond);
                            let iff_call = Expression::Function(Box::new(Function::new(
                                "IFF".to_string(),
                                vec![filter_cond, agg.this.clone(), Expression::Null(Null)],
                            )));
                            agg.this = iff_call;
                        }
                        Ok(Expression::$variant(agg))
                    }};
                }

                match e {
                    Expression::Avg(agg) => handle_agg_filter_to_iff!(Avg, agg),
                    Expression::Sum(agg) => handle_agg_filter_to_iff!(Sum, agg),
                    Expression::Min(agg) => handle_agg_filter_to_iff!(Min, agg),
                    Expression::Max(agg) => handle_agg_filter_to_iff!(Max, agg),
                    Expression::ArrayAgg(mut agg) => {
                        if let Some(filter_cond) = agg.filter.take() {
                            let filter_cond = prefix_not_null(filter_cond);
                            agg.this = Expression::Function(Box::new(Function::new(
                                "IFF".to_string(),
                                vec![filter_cond, agg.this, Expression::Null(Null)],
                            )));
                        }
                        if agg.order_by.is_empty() {
                            Ok(Expression::ArrayAgg(agg))
                        } else {
                            let order_by = std::mem::take(&mut agg.order_by);
                            Ok(Expression::WithinGroup(Box::new(WithinGroup {
                                this: Expression::ArrayAgg(agg),
                                order_by,
                            })))
                        }
                    }
                    Expression::CountIf(agg) => handle_agg_filter_to_iff!(CountIf, agg),
                    Expression::Stddev(agg) => handle_agg_filter_to_iff!(Stddev, agg),
                    Expression::StddevPop(agg) => handle_agg_filter_to_iff!(StddevPop, agg),
                    Expression::StddevSamp(agg) => handle_agg_filter_to_iff!(StddevSamp, agg),
                    Expression::Variance(agg) => handle_agg_filter_to_iff!(Variance, agg),
                    Expression::VarPop(agg) => handle_agg_filter_to_iff!(VarPop, agg),
                    Expression::VarSamp(agg) => handle_agg_filter_to_iff!(VarSamp, agg),
                    Expression::Median(agg) => handle_agg_filter_to_iff!(Median, agg),
                    Expression::Mode(agg) => handle_agg_filter_to_iff!(Mode, agg),
                    Expression::First(agg) => handle_agg_filter_to_iff!(First, agg),
                    Expression::Last(agg) => handle_agg_filter_to_iff!(Last, agg),
                    Expression::AnyValue(agg) => handle_agg_filter_to_iff!(AnyValue, agg),
                    Expression::ApproxDistinct(agg) => {
                        handle_agg_filter_to_iff!(ApproxDistinct, agg)
                    }
                    Expression::Count(mut c) => {
                        if let Some(filter_cond) = c.filter.take() {
                            let filter_cond = prefix_not_null(filter_cond);
                            let is_star = c.star || matches!(c.this, Some(Expression::Star(_)));
                            if is_star {
                                return Ok(Expression::CountIf(Box::new(AggFunc {
                                    this: filter_cond,
                                    distinct: false,
                                    filter: None,
                                    order_by: Vec::new(),
                                    name: None,
                                    ignore_nulls: None,
                                    having_max: None,
                                    limit: None,
                                    inferred_type: None,
                                })));
                            } else if let Some(ref this_expr) = c.this {
                                let iff_call = Expression::Function(Box::new(Function::new(
                                    "IFF".to_string(),
                                    vec![filter_cond, this_expr.clone(), Expression::Null(Null)],
                                )));
                                c.this = Some(iff_call);
                            }
                        }
                        Ok(Expression::Count(c))
                    }
                    Expression::AggregateFunction(mut af) => {
                        if let Some(filter_cond) = af.filter.take() {
                            let filter_cond = prefix_not_null(filter_cond);
                            if let Some(first_arg) = af.args.first_mut() {
                                *first_arg = Expression::Function(Box::new(Function::new(
                                    "IFF".to_string(),
                                    vec![filter_cond, first_arg.clone(), Expression::Null(Null)],
                                )));
                            }
                        }
                        Ok(Expression::AggregateFunction(af))
                    }
                    other => Ok(other),
                }
            }

            Action::ApproxCountDistinctToApproxDistinct => {
                // APPROX_COUNT_DISTINCT(x) -> APPROX_DISTINCT(x)
                if let Expression::ApproxCountDistinct(f) = e {
                    Ok(Expression::ApproxDistinct(f))
                } else {
                    Ok(e)
                }
            }

            Action::CollectListToArrayAgg => {
                // COLLECT_LIST(x) -> ARRAY_AGG(x) FILTER(WHERE x IS NOT NULL)
                if let Expression::AggregateFunction(f) = e {
                    let filter_expr = if !f.args.is_empty() {
                        let arg = f.args[0].clone();
                        Some(Expression::IsNull(Box::new(crate::expressions::IsNull {
                            this: arg,
                            not: true,
                            postfix_form: false,
                        })))
                    } else {
                        None
                    };
                    let agg = crate::expressions::AggFunc {
                        this: if f.args.is_empty() {
                            Expression::Null(crate::expressions::Null)
                        } else {
                            f.args[0].clone()
                        },
                        distinct: f.distinct,
                        order_by: f.order_by.clone(),
                        filter: filter_expr,
                        ignore_nulls: None,
                        name: None,
                        having_max: None,
                        limit: None,
                        inferred_type: None,
                    };
                    Ok(Expression::ArrayAgg(Box::new(agg)))
                } else {
                    Ok(e)
                }
            }

            Action::CollectSetConvert => {
                // COLLECT_SET(x) -> target-specific
                if let Expression::AggregateFunction(f) = e {
                    match target {
                        DialectType::Presto => Ok(Expression::AggregateFunction(Box::new(
                            crate::expressions::AggregateFunction {
                                source_dialect: None,

                                name: "SET_AGG".to_string(),
                                args: f.args,
                                distinct: false,
                                order_by: f.order_by,
                                filter: f.filter,
                                limit: f.limit,
                                ignore_nulls: f.ignore_nulls,
                                inferred_type: None,
                            },
                        ))),
                        DialectType::Snowflake => Ok(Expression::AggregateFunction(Box::new(
                            crate::expressions::AggregateFunction {
                                source_dialect: None,

                                name: "ARRAY_UNIQUE_AGG".to_string(),
                                args: f.args,
                                distinct: false,
                                order_by: f.order_by,
                                filter: f.filter,
                                limit: f.limit,
                                ignore_nulls: f.ignore_nulls,
                                inferred_type: None,
                            },
                        ))),
                        DialectType::Trino | DialectType::DuckDB => {
                            let agg = crate::expressions::AggFunc {
                                this: if f.args.is_empty() {
                                    Expression::Null(crate::expressions::Null)
                                } else {
                                    f.args[0].clone()
                                },
                                distinct: true,
                                order_by: Vec::new(),
                                filter: None,
                                ignore_nulls: None,
                                name: None,
                                having_max: None,
                                limit: None,
                                inferred_type: None,
                            };
                            Ok(Expression::ArrayAgg(Box::new(agg)))
                        }
                        _ => Ok(Expression::AggregateFunction(f)),
                    }
                } else {
                    Ok(e)
                }
            }

            Action::PercentileConvert => {
                // PERCENTILE(x, 0.5) -> QUANTILE(x, 0.5) / APPROX_PERCENTILE(x, 0.5)
                if let Expression::AggregateFunction(f) = e {
                    let name = match target {
                        DialectType::DuckDB => "QUANTILE",
                        DialectType::Presto | DialectType::Trino => "APPROX_PERCENTILE",
                        _ => "PERCENTILE",
                    };
                    Ok(Expression::AggregateFunction(Box::new(
                        crate::expressions::AggregateFunction {
                            source_dialect: None,

                            name: name.to_string(),
                            args: f.args,
                            distinct: f.distinct,
                            order_by: f.order_by,
                            filter: f.filter,
                            limit: f.limit,
                            ignore_nulls: f.ignore_nulls,
                            inferred_type: None,
                        },
                    )))
                } else {
                    Ok(e)
                }
            }

            Action::DuckDBQuantileConvert => {
                let mut aggregate = if let Expression::AggregateFunction(aggregate) = e {
                    *aggregate
                } else {
                    unreachable!("action only triggered for AggregateFunction expressions")
                };
                let name = aggregate.name.to_ascii_uppercase();

                match name.as_str() {
                    "QUANTILE" => {
                        aggregate.name = match target {
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive => {
                                "PERCENTILE"
                            }
                            DialectType::Presto | DialectType::Trino => "APPROX_PERCENTILE",
                            DialectType::BigQuery => "PERCENTILE_CONT",
                            _ => "QUANTILE",
                        }
                        .to_string();
                        Ok(Expression::AggregateFunction(Box::new(aggregate)))
                    }
                    "APPROX_QUANTILE" => {
                        if matches!(target, DialectType::Snowflake) {
                            aggregate.name = "APPROX_PERCENTILE".to_string();
                        }
                        Ok(Expression::AggregateFunction(Box::new(aggregate)))
                    }
                    "QUANTILE_CONT" | "QUANTILE_DISC"
                        if aggregate.args.len() == 2
                            && matches!(
                                target,
                                DialectType::PostgreSQL
                                    | DialectType::Redshift
                                    | DialectType::Snowflake
                            ) =>
                    {
                        // DISTINCT, an inner ORDER BY, LIMIT, and explicit null
                        // handling cannot be represented by these targets' ordered-set
                        // aggregate syntax. Keep the original aggregate intact rather
                        // than silently dropping any of those modifiers.
                        if aggregate.distinct
                            || !aggregate.order_by.is_empty()
                            || aggregate.limit.is_some()
                            || aggregate.ignore_nulls.is_some()
                        {
                            return Ok(Expression::AggregateFunction(Box::new(aggregate)));
                        }

                        let mut args = aggregate.args;
                        let column = args.remove(0);
                        let percentile = args.remove(0);
                        let percentile = PercentileFunc {
                            this: column.clone(),
                            percentile,
                            order_by: Some(vec![Ordered {
                                this: column,
                                desc: false,
                                nulls_first: None,
                                explicit_asc: false,
                                with_fill: None,
                            }]),
                            filter: aggregate.filter,
                        };

                        if name == "QUANTILE_CONT" {
                            Ok(Expression::PercentileCont(Box::new(percentile)))
                        } else {
                            Ok(Expression::PercentileDisc(Box::new(percentile)))
                        }
                    }
                    _ => Ok(Expression::AggregateFunction(Box::new(aggregate))),
                }
            }

            Action::CorrIsnanWrap => {
                // CORR(a, b) -> CASE WHEN ISNAN(CORR(a, b)) THEN NULL ELSE CORR(a, b) END
                // The CORR expression could be AggregateFunction, WindowFunction, or Filter-wrapped
                let corr_clone = e.clone();
                let isnan = Expression::Function(Box::new(Function::new(
                    "ISNAN".to_string(),
                    vec![corr_clone.clone()],
                )));
                let case_expr = Expression::Case(Box::new(Case {
                    operand: None,
                    whens: vec![(isnan, Expression::Null(crate::expressions::Null))],
                    else_: Some(corr_clone),
                    comments: Vec::new(),
                    inferred_type: None,
                }));
                Ok(case_expr)
            }

            Action::FirstToAnyValue => {
                // FIRST(col) IGNORE NULLS -> ANY_VALUE(col) for DuckDB
                if let Expression::First(mut agg) = e {
                    agg.ignore_nulls = None;
                    agg.name = Some("ANY_VALUE".to_string());
                    Ok(Expression::AnyValue(agg))
                } else {
                    Ok(e)
                }
            }

            Action::PercentileContConvert => {
                // PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY col) ->
                // Presto/Trino: APPROX_PERCENTILE(col, p)
                // Spark/Databricks: PERCENTILE_APPROX(col, p)
                if let Expression::WithinGroup(wg) = e {
                    // Extract percentile value and order by column
                    let (percentile, _is_disc) =
                        match &wg.this {
                            Expression::Function(f) => {
                                let is_disc = f.name.eq_ignore_ascii_case("PERCENTILE_DISC");
                                let pct = f.args.first().cloned().unwrap_or(Expression::Literal(
                                    Box::new(Literal::Number("0.5".to_string())),
                                ));
                                (pct, is_disc)
                            }
                            Expression::AggregateFunction(af) => {
                                let is_disc = af.name.eq_ignore_ascii_case("PERCENTILE_DISC");
                                let pct = af.args.first().cloned().unwrap_or(Expression::Literal(
                                    Box::new(Literal::Number("0.5".to_string())),
                                ));
                                (pct, is_disc)
                            }
                            Expression::PercentileCont(pc) => (pc.percentile.clone(), false),
                            _ => return Ok(Expression::WithinGroup(wg)),
                        };
                    let col =
                        wg.order_by
                            .first()
                            .map(|o| o.this.clone())
                            .unwrap_or(Expression::Literal(Box::new(Literal::Number(
                                "1".to_string(),
                            ))));

                    let func_name = match target {
                        DialectType::Presto | DialectType::Trino | DialectType::Athena => {
                            "APPROX_PERCENTILE"
                        }
                        _ => "PERCENTILE_APPROX", // Spark, Databricks
                    };
                    Ok(Expression::Function(Box::new(Function::new(
                        func_name.to_string(),
                        vec![col, percentile],
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ClickHouseUniqToApproxCountDistinct => {
                // ClickHouse uniq(x) -> APPROX_COUNT_DISTINCT(x) for non-ClickHouse targets
                if let Expression::Function(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "APPROX_COUNT_DISTINCT".to_string(),
                        f.args,
                    ))))
                } else {
                    Ok(e)
                }
            }

            Action::ClickHouseAnyToAnyValue => {
                // ClickHouse any(x) -> ANY_VALUE(x) for non-ClickHouse targets
                if let Expression::Function(f) = e {
                    Ok(Expression::Function(Box::new(Function::new(
                        "ANY_VALUE".to_string(),
                        f.args,
                    ))))
                } else {
                    Ok(e)
                }
            }
        }
    })()?;

    Ok(RewriteOutcome::Rewritten(expression))
}

fn cast_integer_median_input(expression: Expression) -> Expression {
    Expression::Cast(Box::new(Cast {
        this: expression,
        // Snowflake fixed-point numbers have at most 38 digits. One fractional
        // digit is required so an even-sized integer median can retain `.5`.
        to: DataType::Decimal {
            precision: Some(39),
            scale: Some(1),
        },
        trailing_comments: Vec::new(),
        double_colon_syntax: false,
        format: None,
        default: None,
        inferred_type: None,
    }))
}

fn widen_zero_scale_median_input(expression: Expression) -> Expression {
    let decimal_shape = match &expression {
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            match &cast.to {
                DataType::Decimal { precision, scale } => Some((*precision, *scale)),
                _ => None,
            }
        }
        other => match other.inferred_type() {
            Some(DataType::Decimal { precision, scale }) => Some((*precision, *scale)),
            _ => None,
        },
    };

    match decimal_shape {
        Some((precision, scale)) if scale.unwrap_or(0) == 0 => Expression::Cast(Box::new(Cast {
            this: expression,
            to: DataType::Decimal {
                precision: Some(precision.unwrap_or(38).saturating_add(1).min(76)),
                scale: Some(1),
            },
            trailing_comments: Vec::new(),
            double_colon_syntax: false,
            format: None,
            default: None,
            inferred_type: None,
        })),
        _ => expression,
    }
}
