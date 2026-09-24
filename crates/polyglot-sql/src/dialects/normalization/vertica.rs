//! Rewrites for Vertica-specific source semantics.
//!
//! Vertica shares most of its surface syntax with PostgreSQL, but a few functions
//! carry Vertica-only meaning that must be lowered before a foreign target sees them.

use super::*;
use crate::ast_transforms::{output_identifier, query_projections, AstNames};
use crate::expressions::{
    AggFunc, AtTimeZone, DateAddFunc, Interval, IntervalUnit, IntervalUnitSpec, VarArgFunc,
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
        if let Expression::Cast(cast) = node {
            if target == DialectType::Vertica {
                crate::generator::Generator::validate_vertica_cast_type(&cast.to)?;
            }
        }
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
                            | crate::expressions::VerticaExpression::ArrayAccess { .. }
                            | crate::expressions::VerticaExpression::ArraySlice { .. }
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
            Expression::Function(f)
                if source == DialectType::Vertica
                    && target == DialectType::DuckDB
                    && !f.quoted
                    && f.args.is_empty()
                    && matches!(
                        f.name.to_ascii_uppercase().as_str(),
                        "GETDATE" | "GETUTCDATE" | "SYSDATE"
                    ) =>
            {
                Some("Vertica statement-start timestamps have no verified DuckDB equivalent")
            }
            Expression::CurrentTimestamp(ts)
                if source == DialectType::Vertica
                    && target == DialectType::DuckDB
                    && ts.sysdate =>
            {
                Some("Vertica statement-start timestamps have no verified DuckDB equivalent")
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
    // BigQuery's COUNTIF returns zero for an empty input/frame. Preserve that
    // before the source transform converts it to a generic function call.
    if source == DialectType::BigQuery && target == DialectType::Vertica {
        return transform_recursive(expr, &|node| match node {
            Expression::CountIf(mut count) => {
                if !count.order_by.is_empty()
                    || count.limit.is_some()
                    || count.having_max.is_some()
                    || count.ignore_nulls.is_some()
                {
                    return Err(crate::error::Error::unsupported(
                        "COUNTIF modifiers",
                        "vertica",
                    ));
                }
                let mut predicate = count.this;
                if let Some(filter) = count.filter.take() {
                    predicate = Expression::And(Box::new(BinaryOp::new(predicate, filter)));
                }
                count.this = Expression::Case(Box::new(crate::expressions::Case {
                    operand: None,
                    whens: vec![(predicate, Expression::number(1))],
                    else_: None,
                    comments: Vec::new(),
                    inferred_type: None,
                }));
                Ok(Expression::Count(Box::new(crate::expressions::CountFunc {
                    this: Some(count.this),
                    star: false,
                    distinct: count.distinct,
                    filter: None,
                    ignore_nulls: None,
                    original_name: None,
                    inferred_type: count.inferred_type,
                })))
            }
            other => Ok(other),
        });
    }
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
                    prepare_select_order(order, &select.expressions, target)?;
                }
            }
            Expression::Union(set) => {
                prepare_set_order(&mut set.order_by, &set.left, &set.right, target)?
            }
            Expression::Intersect(set) => {
                prepare_set_order(&mut set.order_by, &set.left, &set.right, target)?
            }
            Expression::Except(set) => {
                prepare_set_order(&mut set.order_by, &set.left, &set.right, target)?
            }
            Expression::Subquery(sub) => {
                if let Some(order) = &mut sub.order_by {
                    prepare_query_order(order, &[&sub.this], target)?;
                }
            }
            _ => {}
        }
        Ok(node)
    })
}

fn unknown_order(target: DialectType) -> crate::error::Error {
    crate::error::Error::unsupported(
        "Vertica ORDER BY requires a known, unambiguous sort-key type",
        target.to_string(),
    )
}

fn projection_index(key: &Expression, projections: &[Expression]) -> Option<usize> {
    match key {
        Expression::Literal(lit) => match lit.as_ref() {
            Literal::Number(n) => n
                .parse::<usize>()
                .ok()
                .and_then(|n| n.checked_sub(1))
                .filter(|&i| i < projections.len()),
            _ => None,
        },
        Expression::Column(c) if c.table.is_none() => {
            let mut matches = projections.iter().enumerate().filter(|(_, p)| {
                output_identifier(p).is_some_and(|n| n.name.eq_ignore_ascii_case(&c.name.name))
            });
            let first = matches.next()?.0;
            matches.next().is_none().then_some(first)
        }
        _ => None,
    }
}

fn nulls_low(mut key: &Expression) -> Option<bool> {
    while let Expression::Alias(a) = key {
        key = &a.this;
    }
    if let Expression::Paren(p) = key {
        return nulls_low(&p.this);
    }
    let data_type = match key {
        Expression::Cast(c) => Some(&c.to),
        _ => key.inferred_type(),
    };
    match data_type {
        Some(
            DataType::Int { .. }
            | DataType::BigInt { .. }
            | DataType::SmallInt { .. }
            | DataType::TinyInt { .. }
            | DataType::Date
            | DataType::Time { .. }
            | DataType::Timestamp { .. },
        ) => Some(true),
        Some(
            DataType::Float { .. }
            | DataType::Double { .. }
            | DataType::Boolean
            | DataType::Char { .. }
            | DataType::VarChar { .. }
            | DataType::Text
            | DataType::String { .. }
            | DataType::Array { .. },
        ) => Some(false),
        _ => match key {
            Expression::Literal(l) => match l.as_ref() {
                Literal::Number(n) => Some(n.parse::<i64>().is_ok()),
                Literal::String(_) => Some(false),
                Literal::Date(_) | Literal::Timestamp(_) => Some(true),
                _ => None,
            },
            Expression::Boolean(_) | Expression::Array(_) => Some(false),
            _ => None,
        },
    }
}

fn prepare_select_order(
    order: &mut crate::expressions::OrderBy,
    projections: &[Expression],
    target: DialectType,
) -> Result<()> {
    for ordered in &mut order.expressions {
        if ordered.nulls_auto {
            return Err(unknown_order(target));
        }
        if ordered.nulls_first.is_some() {
            continue;
        }
        let key =
            projection_index(&ordered.this, projections).map_or(&ordered.this, |i| &projections[i]);
        let low = nulls_low(key).ok_or_else(|| unknown_order(target))?;
        ordered.nulls_first = Some(low != ordered.desc);
    }
    Ok(())
}

fn query_nulls_low(query: &Expression, index: usize) -> Option<bool> {
    let mut pending = vec![query];
    let mut low = None;
    while let Some(query) = pending.pop() {
        let (left, right) = match query {
            Expression::Select(s) => {
                let current = nulls_low(s.expressions.get(index)?)?;
                if low.is_some_and(|low| low != current) {
                    return None;
                }
                low = Some(current);
                continue;
            }
            Expression::Subquery(s) => {
                pending.push(&s.this);
                continue;
            }
            Expression::Paren(p) => {
                pending.push(&p.this);
                continue;
            }
            Expression::Union(s) if !s.by_name && !s.corresponding => (&s.left, &s.right),
            Expression::Intersect(s) if !s.by_name && !s.corresponding => (&s.left, &s.right),
            Expression::Except(s) if !s.by_name && !s.corresponding => (&s.left, &s.right),
            _ => return None,
        };
        pending.extend([left, right]);
    }
    low
}

fn prepare_query_order(
    order: &mut crate::expressions::OrderBy,
    queries: &[&Expression],
    target: DialectType,
) -> Result<()> {
    let projections = query_projections(queries[0]).ok_or_else(|| unknown_order(target))?;
    for ordered in &mut order.expressions {
        if ordered.nulls_auto {
            return Err(unknown_order(target));
        }
        if ordered.nulls_first.is_some() {
            continue;
        }
        let index =
            projection_index(&ordered.this, projections).ok_or_else(|| unknown_order(target))?;
        let low = query_nulls_low(queries[0], index).ok_or_else(|| unknown_order(target))?;
        if queries
            .iter()
            .skip(1)
            .any(|q| query_nulls_low(q, index) != Some(low))
        {
            return Err(unknown_order(target));
        }
        ordered.nulls_first = Some(low != ordered.desc);
    }
    Ok(())
}

fn prepare_set_order(
    order: &mut Option<crate::expressions::OrderBy>,
    left: &Expression,
    right: &Expression,
    target: DialectType,
) -> Result<()> {
    if let Some(order) = order {
        prepare_query_order(order, &[left, right], target)?;
    }
    Ok(())
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
    use crate::expressions::{Alias, From, Select, Where, WindowFunction};
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
    let mut names = AstNames::default();
    // Collect identifiers directly, without cloning/serializing the complete AST.
    let base_node = Expression::Select(Box::new(base));
    names.collect(&base_node);
    for key in &over.partition_by {
        names.collect(key);
    }
    for key in &over.order_by {
        names.collect(&key.this);
    }
    if let Some(order) = &outer_order {
        for key in &order.expressions {
            names.collect(&key.this);
        }
    }
    let Expression::Select(base_box) = base_node else {
        unreachable!()
    };
    let mut base = *base_box;
    let source_alias = names.fresh("_vertica_source");
    let ranked_alias = names.fresh("_vertica_ranked");
    let rank_name = names.fresh("_vertica_row_number");
    let mut output_names = Vec::new();
    let mut internal_names = Vec::new();
    let mut visible_names = std::collections::HashSet::new();
    // Preserve the base query's aliases: GROUP BY, WHERE, HAVING and later
    // projections may still refer to them in the original scope.
    for expression in &original {
        let name = output_identifier(expression)
            .ok_or_else(unsupported)?
            .clone();
        if !visible_names.insert(name.name.to_ascii_lowercase()) {
            return Err(unsupported());
        }
        output_names.push(name.clone());
        internal_names.push(name);
    }
    let references_alias = |expression: &Expression| {
        expression.dfs().any(|node| {
            let Expression::Column(c) = node else {
                return false;
            };
            c.table.is_none()
                && original.iter().any(|p| {
                    matches!(p, Expression::Alias(a) if a.alias.name.eq_ignore_ascii_case(&c.name.name))
                })
        })
    };
    // PostgreSQL does not accept SELECT aliases in predicates. Without schema
    // information alias/input-name collisions cannot be resolved safely.
    if target == DialectType::PostgreSQL
        && (base
            .where_clause
            .as_ref()
            .is_some_and(|w| references_alias(&w.this))
            || base
                .having
                .as_ref()
                .is_some_and(|h| references_alias(&h.this)))
    {
        return Err(unsupported());
    }
    let visible_count = internal_names.len();
    let mut resolve = |key: &Expression, table: &str, ordinal: bool| -> Result<Expression> {
        if !ordinal
            && matches!(
                key,
                Expression::Literal(_) | Expression::Boolean(_) | Expression::Null(_)
            )
        {
            return Ok(key.clone());
        }
        let projected = match key {
            Expression::Literal(lit) if ordinal => match lit.as_ref() {
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
            if target == DialectType::PostgreSQL && references_alias(key) {
                return Err(unsupported());
            }
            let name = names.fresh("_vertica_hidden");
            base.expressions.push(key.clone().alias(&name));
            let name = Identifier::new(name);
            internal_names.push(name.clone());
            name
        };
        Ok(crate::ast_mutation::qualified_column(table, &name))
    };
    for key in &mut over.partition_by {
        *key = resolve(key, &source_alias, false)?;
    }
    for key in &mut over.order_by {
        key.this = resolve(&key.this, &source_alias, false)?;
        key.nulls_first.get_or_insert(key.desc);
    }
    if let Some(order) = &mut outer_order {
        for key in &mut order.expressions {
            key.this = resolve(&key.this, &ranked_alias, true)?;
        }
    }
    let subquery = |select: Select, name: String| {
        crate::ast_mutation::derived_table(
            Expression::Select(Box::new(select)),
            Some(Identifier::new(name)),
        )
    };
    let mut ranked = Select::new();
    ranked.expressions = internal_names
        .iter()
        .map(|name| crate::ast_mutation::qualified_column(&source_alias, name))
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
                crate::ast_mutation::qualified_column(&ranked_alias, &internal_names[i]),
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
