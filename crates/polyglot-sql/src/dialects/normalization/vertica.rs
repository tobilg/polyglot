//! Rewrites for Vertica-specific source semantics.
//!
//! Vertica shares most of its surface syntax with PostgreSQL, but a few functions
//! carry Vertica-only meaning that must be lowered before a foreign target sees them.

use super::*;
use crate::expressions::{
    AggFunc, AtTimeZone, DateAddFunc, GroupConcatFunc, Interval, IntervalUnit, IntervalUnitSpec,
    StringAggFunc, VarArgFunc,
};

/// Validate before source transforms erase parameters or failure semantics.
/// These are correctness errors, independently of the diagnostic verbosity.
pub(in crate::dialects) fn validate_conversion(
    expression: &Expression,
    source: DialectType,
    target: DialectType,
) -> Result<()> {
    if source == DialectType::Vertica || target == DialectType::Vertica {
        for node in expression.dfs() {
            if let Expression::Select(select) = node {
                for lock in &select.locks {
                    if target == DialectType::Vertica
                        && (lock.update.is_none() || lock.key.is_some() || lock.wait.is_some())
                    {
                        return Err(crate::error::Error::unsupported(
                            "Vertica supports only FOR UPDATE [OF tables]",
                            target.to_string(),
                        ));
                    }
                    if source != target {
                        return Err(crate::error::Error::unsupported(
                            "Vertica locking semantics",
                            target.to_string(),
                        ));
                    }
                }
            }
        }
    }
    if source == target || (source != DialectType::Vertica && target != DialectType::Vertica) {
        return Ok(());
    }
    for node in expression.dfs() {
        let feature = match node {
            Expression::Select(select)
                if source == DialectType::Vertica
                    && select
                        .vertica
                        .as_ref()
                        .is_some_and(|v| v.timeseries.is_some() || v.match_clause.is_some()) =>
            {
                Some("Vertica event-series query")
            }
            Expression::CreateTable(table)
                if source == DialectType::Vertica
                    && table.columns.iter().any(|column| column.encoding.is_some()) =>
            {
                Some("Vertica column encoding")
            }
            Expression::Subscript(_) | Expression::ArraySlice(_)
                if target == DialectType::Vertica =>
            {
                Some("array index and bound semantics when targeting Vertica")
            }
            Expression::Vertica(v)
                if source == DialectType::Vertica
                    && !matches!(
                        v.as_ref(),
                        crate::expressions::VerticaExpression::Binary { .. }
                    ) =>
            {
                Some("native Vertica expression")
            }
            Expression::Function(f)
                if source == DialectType::Vertica
                    && !f.quoted
                    && matches!(
                        f.name.to_ascii_uppercase().as_str(),
                        "TIME_SLICE"
                            | "ROW"
                            | "MATCH_COLUMNS"
                            | "CONDITIONAL_TRUE_EVENT"
                            | "CONDITIONAL_CHANGE_EVENT"
                            | "APPROXIMATE_PERCENTILE"
                            | "REGEXP_SUBSTR"
                    ) =>
            {
                Some("Vertica function semantics without a verified target mapping")
            }
            Expression::Raw(_) | Expression::Command(_) if source == DialectType::Vertica => {
                Some("unstructured Vertica statement")
            }
            Expression::Select(select)
                if source == DialectType::Vertica && select.hint.is_some() =>
            {
                Some("Vertica optimizer hint")
            }
            Expression::ListAgg(_) => Some("Vertica LISTAGG byte limit and overflow behavior"),
            Expression::StringAgg(_) | Expression::GroupConcat(_)
                if target == DialectType::Vertica =>
            {
                Some("string aggregation without Vertica's byte limit and overflow behavior")
            }
            Expression::TryCast(_) | Expression::SafeCast(_) if target == DialectType::Vertica => {
                Some("safe casts: Vertica ::! does not suppress constant cast failures")
            }
            _ => None,
        };
        if let Some(feature) = feature {
            return Err(crate::error::Error::unsupported(
                feature,
                target.to_string(),
            ));
        }
    }
    Ok(())
}

fn known_local_datetime(expression: &Expression) -> bool {
    match expression {
        Expression::Cast(c) => matches!(
            c.to,
            DataType::Date
                | DataType::Timestamp {
                    timezone: false,
                    ..
                }
        ),
        Expression::Literal(l) => matches!(l.as_ref(), Literal::Date(_) | Literal::Timestamp(_)),
        _ => matches!(
            expression.inferred_type(),
            Some(
                DataType::Date
                    | DataType::Timestamp {
                        timezone: false,
                        ..
                    }
            )
        ),
    }
}

/// Resolve defaults while the enclosing ordering context is still available.
pub(in crate::dialects) fn prepare_conversion(
    expr: Expression,
    source: DialectType,
    target: DialectType,
) -> Result<Expression> {
    if source != DialectType::Vertica || source == target {
        return Ok(expr);
    }
    fn analytic(ordered: &mut crate::expressions::Ordered, target: DialectType) -> Result<()> {
        if ordered.nulls_auto {
            return Err(crate::error::Error::unsupported(
                "Vertica NULLS AUTO",
                target.to_string(),
            ));
        }
        ordered.nulls_first.get_or_insert(ordered.desc);
        Ok(())
    }
    transform_recursive(expr, &|mut node| {
        match &mut node {
            Expression::WindowFunction(window) => {
                for order in &mut window.over.order_by {
                    analytic(order, target)?;
                }
            }
            Expression::WithinGroup(group) => {
                for order in &mut group.order_by {
                    analytic(order, target)?;
                }
            }
            Expression::Select(select) => {
                if let Some(windows) = &mut select.windows {
                    for window in windows {
                        for order in &mut window.spec.order_by {
                            analytic(order, target)?;
                        }
                    }
                }
                if let Some(over) = select.vertica.as_mut().and_then(|v| v.limit_over.as_mut()) {
                    for order in &mut over.order_by {
                        analytic(order, target)?;
                    }
                }
                if let Some(order) = &mut select.order_by {
                    for ordered in &mut order.expressions {
                        if ordered.nulls_auto {
                            return Err(crate::error::Error::unsupported(
                                "Vertica top-level NULLS AUTO",
                                target.to_string(),
                            ));
                        }
                        if ordered.nulls_first.is_some() {
                            continue;
                        }
                        let mut key = &ordered.this;
                        if let Expression::Literal(lit) = key {
                            if let Literal::Number(number) = lit.as_ref() {
                                if let Ok(index) = number.parse::<usize>() {
                                    if let Some(expression) =
                                        index.checked_sub(1).and_then(|i| select.expressions.get(i))
                                    {
                                        key = expression;
                                    }
                                }
                            }
                        }
                        if let Expression::Column(column) = key {
                            if let Some(Expression::Alias(alias)) = select.expressions.iter().find(|e| matches!(e, Expression::Alias(a) if a.alias.name.eq_ignore_ascii_case(&column.name.name))) { key = &alias.this; }
                        }
                        if let Expression::Alias(alias) = key {
                            key = &alias.this;
                        }
                        let data_type = match key {
                            Expression::Cast(c) => Some(&c.to),
                            _ => key.inferred_type(),
                        };
                        let low = match data_type {
                            Some(
                                DataType::Int { .. }
                                | DataType::BigInt { .. }
                                | DataType::SmallInt { .. }
                                | DataType::TinyInt { .. }
                                | DataType::Date
                                | DataType::Time { .. }
                                | DataType::Timestamp { .. },
                            ) => true,
                            Some(
                                DataType::Float { .. }
                                | DataType::Double { .. }
                                | DataType::Boolean
                                | DataType::Char { .. }
                                | DataType::VarChar { .. }
                                | DataType::Text
                                | DataType::String { .. }
                                | DataType::Array { .. },
                            ) => false,
                            _ if matches!(key, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n.parse::<i64>().is_ok())) => {
                                true
                            }
                            _ if matches!(key, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_) | Literal::Number(_)))
                                || matches!(key, Expression::Boolean(_) | Expression::Array(_)) =>
                            {
                                false
                            }
                            _ => {
                                return Err(crate::error::Error::unsupported(
                                    "Vertica ORDER BY requires a known sort-key type",
                                    target.to_string(),
                                ))
                            }
                        };
                        ordered.nulls_first = Some(low != ordered.desc);
                    }
                }
            }
            _ => {}
        }
        Ok(node)
    })
}

/// Rewrite a single node parsed as Vertica for a non-Vertica target.
pub(super) fn normalize_from_vertica(e: Expression, target: DialectType) -> Result<Expression> {
    match e {
        Expression::Select(select)
            if select
                .vertica
                .as_ref()
                .is_some_and(|v| v.limit_over.is_some()) =>
        {
            lower_partitioned_limit(*select, target)
        }
        Expression::Subscript(sub) => {
            let mut this = sub.this;
            let mut indices = Vec::new();
            if let Expression::Vertica(node) = this {
                match *node {
                    crate::expressions::VerticaExpression::ArrayAccess {
                        this: array,
                        indices: inner,
                    } => {
                        this = array;
                        indices = inner;
                    }
                    other => this = Expression::Vertica(Box::new(other)),
                }
            }
            indices.push(sub.index);
            Ok(Expression::Vertica(Box::new(
                crate::expressions::VerticaExpression::ArrayAccess { this, indices },
            )))
        }
        Expression::ArraySlice(slice) => Ok(Expression::Vertica(Box::new(
            crate::expressions::VerticaExpression::ArraySlice {
                this: slice.this,
                start: slice.start,
                end: slice.end,
            },
        ))),
        Expression::DateDiff(diff) => {
            let unit = diff.unit.ok_or_else(|| {
                crate::error::Error::unsupported(
                    "Vertica DATEDIFF dynamic unit",
                    target.to_string(),
                )
            })?;
            if unit == IntervalUnit::Week {
                return Err(crate::error::Error::unsupported(
                    "Vertica DATEDIFF week cutoff requires engine verification",
                    target.to_string(),
                ));
            }
            if !known_local_datetime(&diff.this) || !known_local_datetime(&diff.expression) {
                return Err(crate::error::Error::unsupported(
                    "Vertica DATEDIFF requires known DATE or TIMESTAMP WITHOUT TIME ZONE operands",
                    target.to_string(),
                ));
            }
            Ok(Expression::Vertica(Box::new(
                crate::expressions::VerticaExpression::BoundaryDateDiff {
                    start: diff.expression,
                    end: diff.this,
                    unit,
                },
            )))
        }
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
                ("DATEDIFF" | "TIMESTAMPDIFF", 3) => {
                    let unit = timestamp_unit(&f.args[0]).ok_or_else(|| {
                        crate::error::Error::unsupported(
                            "Vertica DATEDIFF dynamic unit",
                            target.to_string(),
                        )
                    })?;
                    normalize_from_vertica(
                        Expression::DateDiff(Box::new(crate::expressions::DateDiffFunc {
                            this: f.args[2].clone(),
                            expression: f.args[1].clone(),
                            unit: Some(unit),
                        })),
                        target,
                    )
                }
                // ZEROIFNULL(x) -> COALESCE(x, 0), except where it is native
                ("NULLIFZERO", 1) => Ok(Expression::NullIf(Box::new(
                    crate::expressions::BinaryFunc {
                        original_name: None,
                        this: f.args[0].clone(),
                        expression: Expression::number(0),
                        inferred_type: None,
                    },
                ))),
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
        "MICROSECOND" | "MICROSECONDS" | "US" | "MCS" => IntervalUnit::Microsecond,
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

fn lower_partitioned_limit(
    mut base: crate::expressions::Select,
    target: DialectType,
) -> Result<Expression> {
    use crate::expressions::{Alias, From, Select, Subquery, Where, WindowFunction};
    let unsupported = || {
        crate::error::Error::unsupported("Vertica partitioned LIMIT requires named outputs, resolvable ordering and no locking or event-series clauses", target.to_string())
    };
    if !matches!(target, DialectType::PostgreSQL | DialectType::DuckDB)
        || !base.locks.is_empty()
        || base.into.is_some()
        || base.offset.is_some()
    {
        return Err(unsupported());
    }
    let extension = base.vertica.take().ok_or_else(unsupported)?;
    if extension.timeseries.is_some() || extension.match_clause.is_some() {
        return Err(unsupported());
    }
    let mut over = extension.limit_over.ok_or_else(unsupported)?;
    if over.partition_by.is_empty()
        || over.order_by.is_empty()
        || over.frame.is_some()
        || over.window_name.is_some()
    {
        return Err(unsupported());
    }
    let limit = base.limit.take().ok_or_else(unsupported)?;
    if !matches!(&limit.this, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(n) if n.parse::<u64>().is_ok_and(|n| n > 0)))
    {
        return Err(unsupported());
    }
    let mut outer_order = base.order_by.take();
    let outer_offset = base.offset.take();
    let outer_with = base.with.take();
    let outer_comments = std::mem::take(&mut base.leading_comments);
    let original = base.expressions.clone();
    let serialized = serde_json::to_string(&Expression::Select(Box::new(base.clone())))
        .map_err(|_| unsupported())?;
    let mut used = std::collections::HashSet::new();
    let mut fresh = |stem: &str| {
        let mut name = stem.to_string();
        let mut n = 0;
        while serialized.contains(&name) || used.contains(&name) {
            n += 1;
            name = format!("{stem}_{n}");
        }
        used.insert(name.clone());
        name
    };
    let source_alias = fresh("_vertica_source");
    let ranked_alias = fresh("_vertica_ranked");
    let rank_name = fresh("_vertica_row_number");
    let mut output_names = Vec::new();
    let mut internal_names = Vec::new();
    let mut visible_names = std::collections::HashSet::new();
    base.expressions.clear();
    for (i, expression) in original.iter().enumerate() {
        let (value, name) = match expression {
            Expression::Alias(alias) if alias.column_aliases.is_empty() => {
                (alias.this.clone(), alias.alias.clone())
            }
            Expression::Column(column) => (expression.clone(), column.name.clone()),
            _ => return Err(unsupported()),
        };
        if !visible_names.insert(name.name.to_ascii_lowercase()) {
            return Err(unsupported());
        }
        let internal = fresh(&format!("_vertica_output_{i}"));
        base.expressions.push(value.alias(&internal));
        output_names.push(name);
        internal_names.push(internal);
    }
    let visible_count = internal_names.len();
    let mut resolve = |key: &Expression, table: &str| -> Result<Expression> {
        let projected = match key {
            Expression::Literal(lit) => match lit.as_ref() {
                Literal::Number(n) => n
                    .parse::<usize>()
                    .ok()
                    .and_then(|i| i.checked_sub(1))
                    .filter(|&i| i < visible_count),
                _ => None,
            },
            Expression::Column(column) if column.table.is_none() => output_names
                .iter()
                .position(|name| name.name.eq_ignore_ascii_case(&column.name.name)),
            _ => None,
        }
        .or_else(|| {
            original.iter().position(|expression| {
                let value = if let Expression::Alias(alias) = expression {
                    &alias.this
                } else {
                    expression
                };
                value == key
            })
        });
        let name = if let Some(i) = projected {
            internal_names[i].clone()
        } else {
            if base.distinct || base.group_by.is_some() || base.having.is_some() {
                return Err(unsupported());
            }
            let name = fresh("_vertica_hidden");
            base.expressions.push(key.clone().alias(&name));
            internal_names.push(name.clone());
            name
        };
        Ok(Expression::qualified_column(table, name))
    };
    for key in &mut over.partition_by {
        *key = resolve(key, &source_alias)?;
    }
    for key in &mut over.order_by {
        key.this = resolve(&key.this, &source_alias)?;
        key.nulls_first.get_or_insert(key.desc);
    }
    if let Some(order) = &mut outer_order {
        for key in &mut order.expressions {
            key.this = resolve(&key.this, &ranked_alias)?;
        }
    }
    let subquery = |select: Select, name: String| {
        Expression::Subquery(Box::new(Subquery {
            this: Expression::Select(Box::new(select)),
            alias: Some(Identifier::new(name)),
            column_aliases: Vec::new(),
            alias_explicit_as: true,
            alias_keyword: None,
            order_by: None,
            limit: None,
            offset: None,
            distribute_by: None,
            sort_by: None,
            cluster_by: None,
            lateral: false,
            modifiers_inside: true,
            trailing_comments: Vec::new(),
            inferred_type: None,
        }))
    };
    let mut ranked = Select::new();
    ranked.expressions = internal_names
        .iter()
        .map(|name| Expression::qualified_column(&source_alias, name))
        .collect();
    ranked.expressions.push(
        Expression::WindowFunction(Box::new(WindowFunction {
            this: Expression::RowNumber(crate::expressions::RowNumber),
            over,
            keep: None,
            inferred_type: None,
        }))
        .alias(&rank_name),
    );
    ranked.from = Some(From {
        expressions: vec![subquery(base, source_alias)],
    });
    let mut result = Select::new();
    result.expressions = output_names
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            Expression::Alias(Box::new(Alias::new(
                Expression::qualified_column(&ranked_alias, &internal_names[i]),
                name,
            )))
        })
        .collect();
    result.where_clause = Some(Where {
        this: Expression::Lte(Box::new(BinaryOp::new(
            Expression::qualified_column(&ranked_alias, rank_name),
            limit.this,
        ))),
    });
    result.from = Some(From {
        expressions: vec![subquery(ranked, ranked_alias)],
    });
    result.order_by = outer_order;
    result.offset = outer_offset;
    result.with = outer_with;
    result.leading_comments = outer_comments;
    Ok(Expression::Select(Box::new(result)))
}
