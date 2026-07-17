//! Cross-dialect semantic normalization.
//!
//! This module owns rewrites that need both the source and target dialect. The
//! public transpilation pipeline remains in `dialects::Dialect`; this is a private
//! implementation detail so phase ordering cannot leak into the public API.

use super::*;

mod aggregates;
mod collections;
mod json;
mod operators;
mod postgres_interval;
mod scalar;
mod statements;
pub(in crate::dialects) mod temporal;
mod types;

#[derive(Debug, Clone, Copy)]
struct NormalizationContext {
    source: DialectType,
    target: DialectType,
    strict: bool,
}

pub(super) fn rewrite_postgres_float_to_integer_cast(cast: Cast) -> Expression {
    types::rewrite_postgres_float_to_integer_cast(cast)
}

pub(super) fn is_postgres_unknown_type(data_type: &DataType) -> bool {
    types::is_postgres_unknown_type(data_type)
}

pub(super) fn unsupported_tsql_json_constructor_return_type(
    expression: &Expression,
) -> Option<String> {
    json::unsupported_tsql_json_constructor_return_type(expression)
}

enum RewriteOutcome {
    // No domain action was selected. The ordered fallback may still have
    // normalized the expression before returning it.
    NoMatch(Expression),
    Rewritten(Expression),
}

impl RewriteOutcome {
    fn into_expression(self) -> Expression {
        match self {
            Self::NoMatch(expression) | Self::Rewritten(expression) => expression,
        }
    }
}

/// Apply cross-dialect semantic normalizations that depend on knowing both source and target.
/// This handles cases where the same syntax has different semantics across dialects.
pub(super) fn normalize(
    expr: Expression,
    source: DialectType,
    target: DialectType,
    strict: bool,
) -> Result<Expression> {
    use crate::expressions::{Case, Cast, DataType, Function, Identifier, Literal};

    let context = NormalizationContext {
        source,
        target,
        strict,
    };

    // This is intentionally a small routing enum. Each domain owns its concrete
    // actions, while this dispatcher preserves the original global precedence.
    #[derive(Debug)]
    enum Action {
        None,
        Statements(statements::Action),
        Types(types::Action),
        Temporal(temporal::Action),
        Collections(collections::Action),
        Json(json::Action),
        Aggregates(aggregates::Action),
        Operators(operators::Action),
        Scalar(scalar::Action),
    }

    let expr = statements::normalize_root(expr, &context);

    transform_recursive(expr, &|e| {
        if matches!(source, DialectType::PostgreSQL | DialectType::Redshift)
            && matches!(target, DialectType::TSQL | DialectType::Fabric)
        {
            if let Expression::Round(mut f) = e {
                if f.decimals.is_none() {
                    f.decimals = Some(Expression::number(0));
                }
                return Ok(Expression::Round(f));
            }

            if let Expression::Function(f) = &e {
                if f.name.eq_ignore_ascii_case("ROUND") && f.args.len() == 1 {
                    let mut f = f.clone();
                    f.args.push(Expression::number(0));
                    return Ok(Expression::Function(f));
                }
            }

            if let Expression::Log(f) = e {
                if f.base.is_none() {
                    return Ok(Expression::Function(Box::new(Function::new(
                        "LOG10".to_string(),
                        vec![f.this],
                    ))));
                }
                return Ok(Expression::Log(f));
            }
        }

        // BigQuery CAST(ARRAY[STRUCT(...)] AS STRUCT_TYPE[]) -> DuckDB: convert unnamed Structs to ROW()
        // This converts auto-named struct literals {'_0': x, '_1': y} inside typed arrays to ROW(x, y)
        if matches!(source, DialectType::BigQuery) && matches!(target, DialectType::DuckDB) {
            if let Expression::Cast(ref c) = e {
                // Check if this is a CAST of an array to a struct array type
                let is_struct_array_cast =
                    matches!(&c.to, crate::expressions::DataType::Array { .. });
                if is_struct_array_cast {
                    let has_auto_named_structs = match &c.this {
                        Expression::Array(arr) => arr.expressions.iter().any(|elem| {
                            if let Expression::Struct(s) = elem {
                                s.fields.iter().all(|(name, _)| {
                                    name.as_ref().map_or(true, |n| {
                                        n.starts_with('_') && n[1..].parse::<usize>().is_ok()
                                    })
                                })
                            } else {
                                false
                            }
                        }),
                        Expression::ArrayFunc(arr) => arr.expressions.iter().any(|elem| {
                            if let Expression::Struct(s) = elem {
                                s.fields.iter().all(|(name, _)| {
                                    name.as_ref().map_or(true, |n| {
                                        n.starts_with('_') && n[1..].parse::<usize>().is_ok()
                                    })
                                })
                            } else {
                                false
                            }
                        }),
                        _ => false,
                    };
                    if has_auto_named_structs {
                        let convert_struct_to_row = |elem: Expression| -> Expression {
                            if let Expression::Struct(s) = elem {
                                let row_args: Vec<Expression> =
                                    s.fields.into_iter().map(|(_, v)| v).collect();
                                Expression::Function(Box::new(Function::new(
                                    "ROW".to_string(),
                                    row_args,
                                )))
                            } else {
                                elem
                            }
                        };
                        let mut c_clone = c.as_ref().clone();
                        match &mut c_clone.this {
                            Expression::Array(arr) => {
                                arr.expressions = arr
                                    .expressions
                                    .drain(..)
                                    .map(convert_struct_to_row)
                                    .collect();
                            }
                            Expression::ArrayFunc(arr) => {
                                arr.expressions = arr
                                    .expressions
                                    .drain(..)
                                    .map(convert_struct_to_row)
                                    .collect();
                            }
                            _ => {}
                        }
                        return Ok(Expression::Cast(Box::new(c_clone)));
                    }
                }
            }
        }

        // BigQuery SELECT AS STRUCT -> DuckDB struct literal {'key': value, ...}
        if matches!(source, DialectType::BigQuery) && matches!(target, DialectType::DuckDB) {
            if let Expression::Select(ref sel) = e {
                if sel.kind.as_deref() == Some("STRUCT") {
                    let mut fields = Vec::new();
                    for expr in &sel.expressions {
                        match expr {
                            Expression::Alias(a) => {
                                fields.push((Some(a.alias.name.clone()), a.this.clone()));
                            }
                            Expression::Column(c) => {
                                fields.push((Some(c.name.name.clone()), expr.clone()));
                            }
                            _ => {
                                fields.push((None, expr.clone()));
                            }
                        }
                    }
                    let struct_lit =
                        Expression::Struct(Box::new(crate::expressions::Struct { fields }));
                    let mut new_select = sel.as_ref().clone();
                    new_select.kind = None;
                    new_select.expressions = vec![struct_lit];
                    return Ok(Expression::Select(Box::new(new_select)));
                }
            }
        }

        // Convert @variable -> ${variable} for Spark/Hive/Databricks
        if matches!(source, DialectType::TSQL | DialectType::Fabric)
            && matches!(
                target,
                DialectType::Spark | DialectType::Databricks | DialectType::Hive
            )
        {
            if let Expression::Parameter(ref p) = e {
                if p.style == crate::expressions::ParameterStyle::At {
                    if let Some(ref name) = p.name {
                        return Ok(Expression::Parameter(Box::new(
                            crate::expressions::Parameter {
                                name: Some(name.clone()),
                                index: p.index,
                                style: crate::expressions::ParameterStyle::DollarBrace,
                                quoted: p.quoted,
                                string_quoted: p.string_quoted,
                                expression: None,
                            },
                        )));
                    }
                }
            }
            // Also handle Column("@x") -> Parameter("x", DollarBrace) for TSQL vars
            if let Expression::Column(ref col) = e {
                if col.name.name.starts_with('@') && col.table.is_none() {
                    let var_name = col.name.name.trim_start_matches('@').to_string();
                    return Ok(Expression::Parameter(Box::new(
                        crate::expressions::Parameter {
                            name: Some(var_name),
                            index: None,
                            style: crate::expressions::ParameterStyle::DollarBrace,
                            quoted: false,
                            string_quoted: false,
                            expression: None,
                        },
                    )));
                }
            }
        }

        // Convert @variable -> variable in SET statements for Spark/Databricks
        if matches!(source, DialectType::TSQL | DialectType::Fabric)
            && matches!(target, DialectType::Spark | DialectType::Databricks)
        {
            if let Expression::SetStatement(ref s) = e {
                let mut new_items = s.items.clone();
                let mut changed = false;
                for item in &mut new_items {
                    // Strip @ from the SET name (Parameter style)
                    if let Expression::Parameter(ref p) = item.name {
                        if p.style == crate::expressions::ParameterStyle::At {
                            if let Some(ref name) = p.name {
                                item.name = Expression::Identifier(Identifier::new(name));
                                changed = true;
                            }
                        }
                    }
                    // Strip @ from the SET name (Identifier style - SET parser)
                    if let Expression::Identifier(ref id) = item.name {
                        if id.name.starts_with('@') {
                            let var_name = id.name.trim_start_matches('@').to_string();
                            item.name = Expression::Identifier(Identifier::new(&var_name));
                            changed = true;
                        }
                    }
                    // Strip @ from the SET name (Column style - alternative parsing)
                    if let Expression::Column(ref col) = item.name {
                        if col.name.name.starts_with('@') && col.table.is_none() {
                            let var_name = col.name.name.trim_start_matches('@').to_string();
                            item.name = Expression::Identifier(Identifier::new(&var_name));
                            changed = true;
                        }
                    }
                }
                if changed {
                    let mut new_set = (**s).clone();
                    new_set.items = new_items;
                    return Ok(Expression::SetStatement(Box::new(new_set)));
                }
            }
        }

        // Strip NOLOCK hint for non-TSQL targets
        if matches!(source, DialectType::TSQL | DialectType::Fabric)
            && !matches!(target, DialectType::TSQL | DialectType::Fabric)
        {
            if let Expression::Table(ref tr) = e {
                if !tr.hints.is_empty() {
                    let mut new_tr = tr.clone();
                    new_tr.hints.clear();
                    return Ok(Expression::Table(new_tr));
                }
            }
        }

        // Snowflake: TRUE IS TRUE -> TRUE, FALSE IS FALSE -> FALSE
        // Snowflake simplifies IS TRUE/IS FALSE on boolean literals
        if matches!(target, DialectType::Snowflake) {
            if let Expression::IsTrue(ref itf) = e {
                if let Expression::Boolean(ref b) = itf.this {
                    if !itf.not {
                        return Ok(Expression::Boolean(crate::expressions::BooleanLiteral {
                            value: b.value,
                        }));
                    } else {
                        return Ok(Expression::Boolean(crate::expressions::BooleanLiteral {
                            value: !b.value,
                        }));
                    }
                }
            }
            if let Expression::IsFalse(ref itf) = e {
                if let Expression::Boolean(ref b) = itf.this {
                    if !itf.not {
                        return Ok(Expression::Boolean(crate::expressions::BooleanLiteral {
                            value: !b.value,
                        }));
                    } else {
                        return Ok(Expression::Boolean(crate::expressions::BooleanLiteral {
                            value: b.value,
                        }));
                    }
                }
            }
        }

        // BigQuery: split dotted backtick identifiers in table names
        // e.g., `a.b.c` -> "a"."b"."c" when source is BigQuery and target is not BigQuery
        if matches!(source, DialectType::BigQuery) && !matches!(target, DialectType::BigQuery) {
            if let Expression::CreateTable(ref ct) = e {
                let mut changed = false;
                let mut new_ct = ct.clone();
                // Split the table name
                if ct.name.schema.is_none() && ct.name.name.name.contains('.') {
                    let parts: Vec<&str> = ct.name.name.name.split('.').collect();
                    // Use quoted identifiers when the original was quoted (backtick in BigQuery)
                    let was_quoted = ct.name.name.quoted;
                    let mk_id = |s: &str| {
                        if was_quoted {
                            Identifier::quoted(s)
                        } else {
                            Identifier::new(s)
                        }
                    };
                    if parts.len() == 3 {
                        new_ct.name.catalog = Some(mk_id(parts[0]));
                        new_ct.name.schema = Some(mk_id(parts[1]));
                        new_ct.name.name = mk_id(parts[2]);
                        changed = true;
                    } else if parts.len() == 2 {
                        new_ct.name.schema = Some(mk_id(parts[0]));
                        new_ct.name.name = mk_id(parts[1]);
                        changed = true;
                    }
                }
                // Split the clone source name
                if let Some(ref clone_src) = ct.clone_source {
                    if clone_src.schema.is_none() && clone_src.name.name.contains('.') {
                        let parts: Vec<&str> = clone_src.name.name.split('.').collect();
                        let was_quoted = clone_src.name.quoted;
                        let mk_id = |s: &str| {
                            if was_quoted {
                                Identifier::quoted(s)
                            } else {
                                Identifier::new(s)
                            }
                        };
                        let mut new_src = clone_src.clone();
                        if parts.len() == 3 {
                            new_src.catalog = Some(mk_id(parts[0]));
                            new_src.schema = Some(mk_id(parts[1]));
                            new_src.name = mk_id(parts[2]);
                            new_ct.clone_source = Some(new_src);
                            changed = true;
                        } else if parts.len() == 2 {
                            new_src.schema = Some(mk_id(parts[0]));
                            new_src.name = mk_id(parts[1]);
                            new_ct.clone_source = Some(new_src);
                            changed = true;
                        }
                    }
                }
                if changed {
                    return Ok(Expression::CreateTable(new_ct));
                }
            }
        }

        // BigQuery array subscript: a[1], b[OFFSET(1)], c[ORDINAL(1)], d[SAFE_OFFSET(1)], e[SAFE_ORDINAL(1)]
        // -> DuckDB/Presto: convert 0-based to 1-based, handle SAFE_* -> ELEMENT_AT for Presto
        if matches!(source, DialectType::BigQuery)
            && matches!(
                target,
                DialectType::DuckDB
                    | DialectType::Presto
                    | DialectType::Trino
                    | DialectType::Athena
            )
        {
            if let Expression::Subscript(ref sub) = e {
                let (new_index, is_safe) = match &sub.index {
                    // a[1] -> a[1+1] = a[2] (plain index is 0-based in BQ)
                    Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(_)) => {
                        let Literal::Number(n) = lit.as_ref() else {
                            unreachable!()
                        };
                        if let Ok(val) = n.parse::<i64>() {
                            (
                                Some(Expression::Literal(Box::new(Literal::Number(
                                    (val + 1).to_string(),
                                )))),
                                false,
                            )
                        } else {
                            (None, false)
                        }
                    }
                    // OFFSET(n) -> n+1 (0-based)
                    Expression::Function(ref f)
                        if f.name.eq_ignore_ascii_case("OFFSET") && f.args.len() == 1 =>
                    {
                        if let Expression::Literal(lit) = &f.args[0] {
                            if let Literal::Number(n) = lit.as_ref() {
                                if let Ok(val) = n.parse::<i64>() {
                                    (
                                        Some(Expression::Literal(Box::new(Literal::Number(
                                            (val + 1).to_string(),
                                        )))),
                                        false,
                                    )
                                } else {
                                    (
                                        Some(Expression::Add(Box::new(
                                            crate::expressions::BinaryOp::new(
                                                f.args[0].clone(),
                                                Expression::number(1),
                                            ),
                                        ))),
                                        false,
                                    )
                                }
                            } else {
                                (None, false)
                            }
                        } else {
                            (
                                Some(Expression::Add(Box::new(
                                    crate::expressions::BinaryOp::new(
                                        f.args[0].clone(),
                                        Expression::number(1),
                                    ),
                                ))),
                                false,
                            )
                        }
                    }
                    // ORDINAL(n) -> n (already 1-based)
                    Expression::Function(ref f)
                        if f.name.eq_ignore_ascii_case("ORDINAL") && f.args.len() == 1 =>
                    {
                        (Some(f.args[0].clone()), false)
                    }
                    // SAFE_OFFSET(n) -> n+1 (0-based, safe)
                    Expression::Function(ref f)
                        if f.name.eq_ignore_ascii_case("SAFE_OFFSET") && f.args.len() == 1 =>
                    {
                        if let Expression::Literal(lit) = &f.args[0] {
                            if let Literal::Number(n) = lit.as_ref() {
                                if let Ok(val) = n.parse::<i64>() {
                                    (
                                        Some(Expression::Literal(Box::new(Literal::Number(
                                            (val + 1).to_string(),
                                        )))),
                                        true,
                                    )
                                } else {
                                    (
                                        Some(Expression::Add(Box::new(
                                            crate::expressions::BinaryOp::new(
                                                f.args[0].clone(),
                                                Expression::number(1),
                                            ),
                                        ))),
                                        true,
                                    )
                                }
                            } else {
                                (None, false)
                            }
                        } else {
                            (
                                Some(Expression::Add(Box::new(
                                    crate::expressions::BinaryOp::new(
                                        f.args[0].clone(),
                                        Expression::number(1),
                                    ),
                                ))),
                                true,
                            )
                        }
                    }
                    // SAFE_ORDINAL(n) -> n (already 1-based, safe)
                    Expression::Function(ref f)
                        if f.name.eq_ignore_ascii_case("SAFE_ORDINAL") && f.args.len() == 1 =>
                    {
                        (Some(f.args[0].clone()), true)
                    }
                    _ => (None, false),
                };
                if let Some(idx) = new_index {
                    if is_safe
                        && matches!(
                            target,
                            DialectType::Presto | DialectType::Trino | DialectType::Athena
                        )
                    {
                        // Presto: SAFE_OFFSET/SAFE_ORDINAL -> ELEMENT_AT(arr, idx)
                        return Ok(Expression::Function(Box::new(Function::new(
                            "ELEMENT_AT".to_string(),
                            vec![sub.this.clone(), idx],
                        ))));
                    } else {
                        // DuckDB or non-safe: just use subscript with converted index
                        return Ok(Expression::Subscript(Box::new(
                            crate::expressions::Subscript {
                                this: sub.this.clone(),
                                index: idx,
                            },
                        )));
                    }
                }
            }
        }

        // BigQuery LENGTH(x) -> DuckDB CASE TYPEOF(x) WHEN 'BLOB' THEN OCTET_LENGTH(...) ELSE LENGTH(...) END
        if matches!(source, DialectType::BigQuery) && matches!(target, DialectType::DuckDB) {
            if let Expression::Length(ref uf) = e {
                let arg = uf.this.clone();
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
                let length_text = Expression::Length(Box::new(crate::expressions::UnaryFunc {
                    this: text_cast,
                    original_name: None,
                    inferred_type: None,
                }));
                return Ok(Expression::Case(Box::new(Case {
                    operand: Some(typeof_func),
                    whens: vec![(
                        Expression::Literal(Box::new(Literal::String("BLOB".to_string()))),
                        octet_length,
                    )],
                    else_: Some(length_text),
                    comments: Vec::new(),
                    inferred_type: None,
                })));
            }
        }

        // BigQuery UNNEST alias handling (only for non-BigQuery sources):
        // UNNEST(...) AS x -> UNNEST(...) (drop unused table alias)
        // UNNEST(...) AS x(y) -> UNNEST(...) AS y (use column alias as main alias)
        if matches!(target, DialectType::BigQuery) && !matches!(source, DialectType::BigQuery) {
            if let Expression::Alias(ref a) = e {
                if matches!(&a.this, Expression::Unnest(_)) {
                    if a.column_aliases.is_empty() {
                        // Drop the entire alias, return just the UNNEST expression
                        return Ok(a.this.clone());
                    } else {
                        // Use first column alias as the main alias
                        let mut new_alias = a.as_ref().clone();
                        new_alias.alias = a.column_aliases[0].clone();
                        new_alias.column_aliases.clear();
                        return Ok(Expression::Alias(Box::new(new_alias)));
                    }
                }
            }
        }

        // BigQuery IN UNNEST(expr) -> IN (SELECT UNNEST/EXPLODE(expr)) for non-BigQuery targets
        if matches!(source, DialectType::BigQuery) && !matches!(target, DialectType::BigQuery) {
            if let Expression::In(ref in_expr) = e {
                if let Some(ref unnest_inner) = in_expr.unnest {
                    // Build the function call for the target dialect
                    let func_expr = if matches!(
                        target,
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks
                    ) {
                        // Use EXPLODE for Hive/Spark
                        Expression::Function(Box::new(Function::new(
                            "EXPLODE".to_string(),
                            vec![*unnest_inner.clone()],
                        )))
                    } else {
                        // Use UNNEST for Presto/Trino/DuckDB/etc.
                        Expression::Unnest(Box::new(crate::expressions::UnnestFunc {
                            this: *unnest_inner.clone(),
                            expressions: Vec::new(),
                            with_ordinality: false,
                            alias: None,
                            offset_alias: None,
                        }))
                    };

                    // Wrap in SELECT
                    let mut inner_select = crate::expressions::Select::new();
                    inner_select.expressions = vec![func_expr];

                    let subquery_expr = Expression::Select(Box::new(inner_select));

                    return Ok(Expression::In(Box::new(crate::expressions::In {
                        this: in_expr.this.clone(),
                        expressions: Vec::new(),
                        query: Some(subquery_expr),
                        not: in_expr.not,
                        global: in_expr.global,
                        unnest: None,
                        is_field: false,
                    })));
                }
            }
        }

        // SQLite: GENERATE_SERIES AS t(i) -> (SELECT value AS i FROM GENERATE_SERIES(...)) AS t
        // This handles the subquery wrapping for RANGE -> GENERATE_SERIES in FROM context
        if matches!(target, DialectType::SQLite) && matches!(source, DialectType::DuckDB) {
            if let Expression::Alias(ref a) = e {
                if let Expression::Function(ref f) = a.this {
                    if f.name.eq_ignore_ascii_case("GENERATE_SERIES")
                        && !a.column_aliases.is_empty()
                    {
                        // Build: (SELECT value AS col_alias FROM GENERATE_SERIES(start, end)) AS table_alias
                        let col_alias = a.column_aliases[0].clone();
                        let mut inner_select = crate::expressions::Select::new();
                        inner_select.expressions =
                            vec![Expression::Alias(Box::new(crate::expressions::Alias::new(
                                Expression::Identifier(Identifier::new("value".to_string())),
                                col_alias,
                            )))];
                        inner_select.from = Some(crate::expressions::From {
                            expressions: vec![a.this.clone()],
                        });
                        let subquery =
                            Expression::Subquery(Box::new(crate::expressions::Subquery {
                                this: Expression::Select(Box::new(inner_select)),
                                alias: Some(a.alias.clone()),
                                column_aliases: Vec::new(),
                                alias_explicit_as: false,
                                alias_keyword: None,
                                order_by: None,
                                limit: None,
                                offset: None,
                                lateral: false,
                                modifiers_inside: false,
                                trailing_comments: Vec::new(),
                                distribute_by: None,
                                sort_by: None,
                                cluster_by: None,
                                inferred_type: None,
                            }));
                        return Ok(subquery);
                    }
                }
            }
        }

        // BigQuery implicit UNNEST: comma-join on array path -> CROSS JOIN UNNEST
        // e.g., SELECT results FROM Coordinates, Coordinates.position AS results
        //     -> SELECT results FROM Coordinates CROSS JOIN UNNEST(Coordinates.position) AS results
        if matches!(source, DialectType::BigQuery) {
            if let Expression::Select(ref s) = e {
                if let Some(ref from) = s.from {
                    if from.expressions.len() >= 2 {
                        // Collect table names from first expression
                        let first_tables: Vec<String> = from
                            .expressions
                            .iter()
                            .take(1)
                            .filter_map(|expr| {
                                if let Expression::Table(t) = expr {
                                    Some(t.name.name.to_ascii_lowercase())
                                } else {
                                    None
                                }
                            })
                            .collect();

                        // Check if any subsequent FROM expressions are schema-qualified with a matching table name
                        // or have a dotted name matching a table
                        let mut needs_rewrite = false;
                        for expr in from.expressions.iter().skip(1) {
                            if let Expression::Table(t) = expr {
                                if let Some(ref schema) = t.schema {
                                    if first_tables.contains(&schema.name.to_ascii_lowercase()) {
                                        needs_rewrite = true;
                                        break;
                                    }
                                }
                                // Also check dotted names in quoted identifiers (e.g., `Coordinates.position`)
                                if t.schema.is_none() && t.name.name.contains('.') {
                                    let parts: Vec<&str> = t.name.name.split('.').collect();
                                    if parts.len() >= 2
                                        && first_tables.contains(&parts[0].to_ascii_lowercase())
                                    {
                                        needs_rewrite = true;
                                        break;
                                    }
                                }
                            }
                        }

                        if needs_rewrite {
                            let mut new_select = s.clone();
                            let mut new_from_exprs = vec![from.expressions[0].clone()];
                            let mut new_joins = s.joins.clone();

                            for expr in from.expressions.iter().skip(1) {
                                if let Expression::Table(ref t) = expr {
                                    if let Some(ref schema) = t.schema {
                                        if first_tables.contains(&schema.name.to_ascii_lowercase())
                                        {
                                            // This is an array path reference, convert to CROSS JOIN UNNEST
                                            let col_expr = Expression::Column(Box::new(
                                                crate::expressions::Column {
                                                    name: t.name.clone(),
                                                    table: Some(schema.clone()),
                                                    join_mark: false,
                                                    trailing_comments: vec![],
                                                    span: None,
                                                    inferred_type: None,
                                                },
                                            ));
                                            let unnest_expr = Expression::Unnest(Box::new(
                                                crate::expressions::UnnestFunc {
                                                    this: col_expr,
                                                    expressions: Vec::new(),
                                                    with_ordinality: false,
                                                    alias: None,
                                                    offset_alias: None,
                                                },
                                            ));
                                            let join_this = if let Some(ref alias) = t.alias {
                                                if matches!(
                                                    target,
                                                    DialectType::Presto
                                                        | DialectType::Trino
                                                        | DialectType::Athena
                                                ) {
                                                    // Presto: UNNEST(x) AS _t0(results)
                                                    Expression::Alias(Box::new(
                                                        crate::expressions::Alias {
                                                            this: unnest_expr,
                                                            alias: Identifier::new("_t0"),
                                                            column_aliases: vec![alias.clone()],
                                                            alias_explicit_as: false,
                                                            alias_keyword: None,
                                                            pre_alias_comments: vec![],
                                                            trailing_comments: vec![],
                                                            inferred_type: None,
                                                        },
                                                    ))
                                                } else {
                                                    // BigQuery: UNNEST(x) AS results
                                                    Expression::Alias(Box::new(
                                                        crate::expressions::Alias {
                                                            this: unnest_expr,
                                                            alias: alias.clone(),
                                                            column_aliases: vec![],
                                                            alias_explicit_as: false,
                                                            alias_keyword: None,
                                                            pre_alias_comments: vec![],
                                                            trailing_comments: vec![],
                                                            inferred_type: None,
                                                        },
                                                    ))
                                                }
                                            } else {
                                                unnest_expr
                                            };
                                            new_joins.push(crate::expressions::Join {
                                                kind: crate::expressions::JoinKind::Cross,
                                                this: join_this,
                                                on: None,
                                                using: Vec::new(),
                                                use_inner_keyword: false,
                                                use_outer_keyword: false,
                                                deferred_condition: false,
                                                join_hint: None,
                                                match_condition: None,
                                                pivots: Vec::new(),
                                                comments: Vec::new(),
                                                nesting_group: 0,
                                                directed: false,
                                            });
                                        } else {
                                            new_from_exprs.push(expr.clone());
                                        }
                                    } else if t.schema.is_none() && t.name.name.contains('.') {
                                        // Dotted name in quoted identifier: `Coordinates.position`
                                        let parts: Vec<&str> = t.name.name.split('.').collect();
                                        if parts.len() >= 2
                                            && first_tables.contains(&parts[0].to_ascii_lowercase())
                                        {
                                            let join_this =
                                                if matches!(target, DialectType::BigQuery) {
                                                    // BigQuery: keep as single quoted identifier, just convert comma -> CROSS JOIN
                                                    Expression::Table(t.clone())
                                                } else {
                                                    // Other targets: split into "schema"."name"
                                                    let mut new_t = t.clone();
                                                    new_t.schema =
                                                        Some(Identifier::quoted(parts[0]));
                                                    new_t.name = Identifier::quoted(parts[1]);
                                                    Expression::Table(new_t)
                                                };
                                            new_joins.push(crate::expressions::Join {
                                                kind: crate::expressions::JoinKind::Cross,
                                                this: join_this,
                                                on: None,
                                                using: Vec::new(),
                                                use_inner_keyword: false,
                                                use_outer_keyword: false,
                                                deferred_condition: false,
                                                join_hint: None,
                                                match_condition: None,
                                                pivots: Vec::new(),
                                                comments: Vec::new(),
                                                nesting_group: 0,
                                                directed: false,
                                            });
                                        } else {
                                            new_from_exprs.push(expr.clone());
                                        }
                                    } else {
                                        new_from_exprs.push(expr.clone());
                                    }
                                } else {
                                    new_from_exprs.push(expr.clone());
                                }
                            }

                            new_select.from = Some(crate::expressions::From {
                                expressions: new_from_exprs,
                                ..from.clone()
                            });
                            new_select.joins = new_joins;
                            return Ok(Expression::Select(new_select));
                        }
                    }
                }
            }
        }

        // CROSS JOIN UNNEST -> LATERAL VIEW EXPLODE for Hive/Spark
        if matches!(
            target,
            DialectType::Hive | DialectType::Spark | DialectType::Databricks
        ) {
            if let Expression::Select(ref s) = e {
                // Check if any joins are CROSS JOIN with UNNEST/EXPLODE
                let is_unnest_or_explode_expr = |expr: &Expression| -> bool {
                    matches!(expr, Expression::Unnest(_))
                        || matches!(expr, Expression::Function(f) if f.name.eq_ignore_ascii_case("EXPLODE"))
                };
                let has_unnest_join = s.joins.iter().any(|j| {
                    j.kind == crate::expressions::JoinKind::Cross && (
                        matches!(&j.this, Expression::Alias(a) if is_unnest_or_explode_expr(&a.this))
                        || is_unnest_or_explode_expr(&j.this)
                    )
                });
                if has_unnest_join {
                    let mut select = s.clone();
                    let mut new_joins = Vec::new();
                    for join in select.joins.drain(..) {
                        if join.kind == crate::expressions::JoinKind::Cross {
                            // Extract the UNNEST/EXPLODE from the join
                            let (func_expr, table_alias, col_aliases) = match &join.this {
                                Expression::Alias(a) => {
                                    let ta = if a.alias.is_empty() {
                                        None
                                    } else {
                                        Some(a.alias.clone())
                                    };
                                    let cas = a.column_aliases.clone();
                                    match &a.this {
                                        Expression::Unnest(u) => {
                                            // Multi-arg UNNEST(y, z) -> INLINE(ARRAYS_ZIP(y, z))
                                            if !u.expressions.is_empty() {
                                                let mut all_args = vec![u.this.clone()];
                                                all_args.extend(u.expressions.clone());
                                                let arrays_zip = Expression::Function(Box::new(
                                                    crate::expressions::Function::new(
                                                        "ARRAYS_ZIP".to_string(),
                                                        all_args,
                                                    ),
                                                ));
                                                let inline = Expression::Function(Box::new(
                                                    crate::expressions::Function::new(
                                                        "INLINE".to_string(),
                                                        vec![arrays_zip],
                                                    ),
                                                ));
                                                (Some(inline), ta, a.column_aliases.clone())
                                            } else {
                                                // Convert UNNEST(x) to EXPLODE(x) or POSEXPLODE(x)
                                                let func_name = if u.with_ordinality {
                                                    "POSEXPLODE"
                                                } else {
                                                    "EXPLODE"
                                                };
                                                let explode = Expression::Function(Box::new(
                                                    crate::expressions::Function::new(
                                                        func_name.to_string(),
                                                        vec![u.this.clone()],
                                                    ),
                                                ));
                                                // For POSEXPLODE, add 'pos' to column aliases
                                                let cas = if u.with_ordinality {
                                                    let mut pos_aliases =
                                                        vec![Identifier::new("pos".to_string())];
                                                    pos_aliases.extend(a.column_aliases.clone());
                                                    pos_aliases
                                                } else {
                                                    a.column_aliases.clone()
                                                };
                                                (Some(explode), ta, cas)
                                            }
                                        }
                                        Expression::Function(f)
                                            if f.name.eq_ignore_ascii_case("EXPLODE") =>
                                        {
                                            (Some(Expression::Function(f.clone())), ta, cas)
                                        }
                                        _ => (None, None, Vec::new()),
                                    }
                                }
                                Expression::Unnest(u) => {
                                    let func_name = if u.with_ordinality {
                                        "POSEXPLODE"
                                    } else {
                                        "EXPLODE"
                                    };
                                    let explode = Expression::Function(Box::new(
                                        crate::expressions::Function::new(
                                            func_name.to_string(),
                                            vec![u.this.clone()],
                                        ),
                                    ));
                                    let ta = u.alias.clone();
                                    let col_aliases = if u.with_ordinality {
                                        vec![Identifier::new("pos".to_string())]
                                    } else {
                                        Vec::new()
                                    };
                                    (Some(explode), ta, col_aliases)
                                }
                                _ => (None, None, Vec::new()),
                            };
                            if let Some(func) = func_expr {
                                select.lateral_views.push(crate::expressions::LateralView {
                                    this: func,
                                    table_alias,
                                    column_aliases: col_aliases,
                                    outer: false,
                                });
                            } else {
                                new_joins.push(join);
                            }
                        } else {
                            new_joins.push(join);
                        }
                    }
                    select.joins = new_joins;
                    return Ok(Expression::Select(select));
                }
            }
        }

        // UNNEST expansion: DuckDB SELECT UNNEST(arr) in SELECT list -> expanded query
        // for BigQuery, Presto/Trino, Snowflake
        if matches!(source, DialectType::DuckDB | DialectType::PostgreSQL)
            && matches!(
                target,
                DialectType::BigQuery
                    | DialectType::Presto
                    | DialectType::Trino
                    | DialectType::Snowflake
            )
        {
            if let Expression::Select(ref s) = e {
                // Check if any SELECT expressions contain UNNEST
                // Note: UNNEST can appear as Expression::Unnest OR Expression::Function("UNNEST")
                let has_unnest_in_select = s.expressions.iter().any(|expr| {
                    fn contains_unnest(e: &Expression) -> bool {
                        match e {
                            Expression::Unnest(_) => true,
                            Expression::Function(f) if f.name.eq_ignore_ascii_case("UNNEST") => {
                                true
                            }
                            Expression::Alias(a) => contains_unnest(&a.this),
                            Expression::Add(op)
                            | Expression::Sub(op)
                            | Expression::Mul(op)
                            | Expression::Div(op) => {
                                contains_unnest(&op.left) || contains_unnest(&op.right)
                            }
                            _ => false,
                        }
                    }
                    contains_unnest(expr)
                });

                if has_unnest_in_select {
                    let rewritten = collections::rewrite_unnest_expansion(s, target);
                    if let Some(new_select) = rewritten {
                        return Ok(Expression::Select(Box::new(new_select)));
                    }
                }
            }
        }

        // BigQuery -> PostgreSQL: convert escape sequences in string literals to actual characters
        // BigQuery '\n' -> PostgreSQL literal newline in string
        if matches!(source, DialectType::BigQuery) && matches!(target, DialectType::PostgreSQL) {
            if let Expression::Literal(ref lit) = e {
                if let Literal::String(ref s) = lit.as_ref() {
                    if s.contains("\\n")
                        || s.contains("\\t")
                        || s.contains("\\r")
                        || s.contains("\\\\")
                    {
                        let converted = s
                            .replace("\\n", "\n")
                            .replace("\\t", "\t")
                            .replace("\\r", "\r")
                            .replace("\\\\", "\\");
                        return Ok(Expression::Literal(Box::new(Literal::String(converted))));
                    }
                }
            }
        }

        // Cross-dialect: convert Literal::Timestamp to target-specific CAST form
        // when source != target (identity tests keep the Literal::Timestamp for native handling)
        if source != target {
            if let Expression::Literal(ref lit) = e {
                if let Literal::Timestamp(ref s) = lit.as_ref() {
                    let s = s.clone();
                    // MySQL: TIMESTAMP handling depends on source dialect
                    // BigQuery TIMESTAMP is timezone-aware -> TIMESTAMP() function in MySQL
                    // Other sources' TIMESTAMP is non-timezone -> CAST('x' AS DATETIME) in MySQL
                    if matches!(target, DialectType::MySQL) {
                        if matches!(source, DialectType::BigQuery) {
                            // BigQuery TIMESTAMP is timezone-aware -> MySQL TIMESTAMP() function
                            return Ok(Expression::Function(Box::new(Function::new(
                                "TIMESTAMP".to_string(),
                                vec![Expression::Literal(Box::new(Literal::String(s)))],
                            ))));
                        } else {
                            // Non-timezone TIMESTAMP -> CAST('x' AS DATETIME) in MySQL
                            return Ok(Expression::Cast(Box::new(Cast {
                                this: Expression::Literal(Box::new(Literal::String(s))),
                                to: DataType::Custom {
                                    name: "DATETIME".to_string(),
                                },
                                trailing_comments: Vec::new(),
                                double_colon_syntax: false,
                                format: None,
                                default: None,
                                inferred_type: None,
                            })));
                        }
                    }
                    let dt = match target {
                        DialectType::BigQuery | DialectType::StarRocks => DataType::Custom {
                            name: "DATETIME".to_string(),
                        },
                        DialectType::Snowflake => {
                            // BigQuery TIMESTAMP is timezone-aware -> use TIMESTAMPTZ for Snowflake
                            if matches!(source, DialectType::BigQuery) {
                                DataType::Custom {
                                    name: "TIMESTAMPTZ".to_string(),
                                }
                            } else if matches!(
                                source,
                                DialectType::PostgreSQL
                                    | DialectType::Redshift
                                    | DialectType::Snowflake
                            ) {
                                DataType::Timestamp {
                                    precision: None,
                                    timezone: false,
                                }
                            } else {
                                DataType::Custom {
                                    name: "TIMESTAMPNTZ".to_string(),
                                }
                            }
                        }
                        DialectType::Spark | DialectType::Databricks => {
                            // BigQuery TIMESTAMP is timezone-aware -> use plain TIMESTAMP for Spark/Databricks
                            if matches!(source, DialectType::BigQuery) {
                                DataType::Timestamp {
                                    precision: None,
                                    timezone: false,
                                }
                            } else {
                                DataType::Custom {
                                    name: "TIMESTAMP_NTZ".to_string(),
                                }
                            }
                        }
                        DialectType::ClickHouse => DataType::Nullable {
                            inner: Box::new(DataType::Custom {
                                name: "DateTime".to_string(),
                            }),
                        },
                        DialectType::TSQL | DialectType::Fabric => DataType::Custom {
                            name: "DATETIME2".to_string(),
                        },
                        DialectType::DuckDB => {
                            // DuckDB: use TIMESTAMPTZ when source is BigQuery (BQ TIMESTAMP is always UTC/tz-aware)
                            // or when the timestamp string explicitly has timezone info
                            if matches!(source, DialectType::BigQuery)
                                || temporal::timestamp_string_has_timezone(&s)
                            {
                                DataType::Custom {
                                    name: "TIMESTAMPTZ".to_string(),
                                }
                            } else {
                                DataType::Timestamp {
                                    precision: None,
                                    timezone: false,
                                }
                            }
                        }
                        _ => DataType::Timestamp {
                            precision: None,
                            timezone: false,
                        },
                    };
                    return Ok(Expression::Cast(Box::new(Cast {
                        this: Expression::Literal(Box::new(Literal::String(s))),
                        to: dt,
                        trailing_comments: vec![],
                        double_colon_syntax: false,
                        format: None,
                        default: None,
                        inferred_type: None,
                    })));
                }
            }
        }

        // PostgreSQL DELETE requires explicit AS for table aliases
        if matches!(target, DialectType::PostgreSQL | DialectType::Redshift) {
            if let Expression::Delete(ref del) = e {
                if del.alias.is_some() && !del.alias_explicit_as {
                    let mut new_del = del.clone();
                    new_del.alias_explicit_as = true;
                    return Ok(Expression::Delete(new_del));
                }
            }
        }

        // UNION/INTERSECT/EXCEPT DISTINCT handling:
        // Some dialects require explicit DISTINCT (BigQuery, ClickHouse),
        // while others don't support it (Presto, Spark, DuckDB, etc.)
        {
            let needs_distinct = matches!(target, DialectType::BigQuery | DialectType::ClickHouse);
            let drop_distinct = matches!(
                target,
                DialectType::Presto
                    | DialectType::Trino
                    | DialectType::Athena
                    | DialectType::Spark
                    | DialectType::Databricks
                    | DialectType::DuckDB
                    | DialectType::Hive
                    | DialectType::MySQL
                    | DialectType::PostgreSQL
                    | DialectType::SQLite
                    | DialectType::TSQL
                    | DialectType::Redshift
                    | DialectType::Snowflake
                    | DialectType::Oracle
                    | DialectType::Teradata
                    | DialectType::Drill
                    | DialectType::Doris
                    | DialectType::StarRocks
            );
            match &e {
                Expression::Union(u) if !u.all && needs_distinct && !u.distinct => {
                    let mut new_u = (**u).clone();
                    new_u.distinct = true;
                    return Ok(Expression::Union(Box::new(new_u)));
                }
                Expression::Intersect(i) if !i.all && needs_distinct && !i.distinct => {
                    let mut new_i = (**i).clone();
                    new_i.distinct = true;
                    return Ok(Expression::Intersect(Box::new(new_i)));
                }
                Expression::Except(ex) if !ex.all && needs_distinct && !ex.distinct => {
                    let mut new_ex = (**ex).clone();
                    new_ex.distinct = true;
                    return Ok(Expression::Except(Box::new(new_ex)));
                }
                Expression::Union(u) if u.distinct && drop_distinct => {
                    let mut new_u = (**u).clone();
                    new_u.distinct = false;
                    return Ok(Expression::Union(Box::new(new_u)));
                }
                Expression::Intersect(i) if i.distinct && drop_distinct => {
                    let mut new_i = (**i).clone();
                    new_i.distinct = false;
                    return Ok(Expression::Intersect(Box::new(new_i)));
                }
                Expression::Except(ex) if ex.distinct && drop_distinct => {
                    let mut new_ex = (**ex).clone();
                    new_ex.distinct = false;
                    return Ok(Expression::Except(Box::new(new_ex)));
                }
                _ => {}
            }
        }

        // ClickHouse: MAP('a', '1') -> map('a', '1') (lowercase function name)
        if matches!(target, DialectType::ClickHouse) {
            if let Expression::Function(ref f) = e {
                if f.name.eq_ignore_ascii_case("MAP") && !f.args.is_empty() {
                    let mut new_f = f.as_ref().clone();
                    new_f.name = "map".to_string();
                    return Ok(Expression::Function(Box::new(new_f)));
                }
            }
        }

        // ClickHouse: INTERSECT ALL -> INTERSECT (ClickHouse doesn't support ALL on INTERSECT)
        if matches!(target, DialectType::ClickHouse) {
            if let Expression::Intersect(ref i) = e {
                if i.all {
                    let mut new_i = (**i).clone();
                    new_i.all = false;
                    return Ok(Expression::Intersect(Box::new(new_i)));
                }
            }
        }

        // Integer division: a / b -> CAST(a AS DOUBLE) / b for dialects that need it
        // Only from Generic source, to prevent double-wrapping
        if matches!(source, DialectType::Generic) {
            if let Expression::Div(ref op) = e {
                let cast_type = match target {
                    DialectType::TSQL | DialectType::Fabric => Some(DataType::Float {
                        precision: None,
                        scale: None,
                        real_spelling: false,
                    }),
                    DialectType::Drill
                    | DialectType::Trino
                    | DialectType::Athena
                    | DialectType::Presto => Some(DataType::Double {
                        precision: None,
                        scale: None,
                    }),
                    DialectType::PostgreSQL
                    | DialectType::Redshift
                    | DialectType::Materialize
                    | DialectType::Teradata
                    | DialectType::RisingWave => Some(DataType::Double {
                        precision: None,
                        scale: None,
                    }),
                    _ => None,
                };
                if let Some(dt) = cast_type {
                    let cast_left = Expression::Cast(Box::new(Cast {
                        this: op.left.clone(),
                        to: dt,
                        double_colon_syntax: false,
                        trailing_comments: Vec::new(),
                        format: None,
                        default: None,
                        inferred_type: None,
                    }));
                    let new_op = crate::expressions::BinaryOp {
                        left: cast_left,
                        right: op.right.clone(),
                        left_comments: op.left_comments.clone(),
                        operator_comments: op.operator_comments.clone(),
                        trailing_comments: op.trailing_comments.clone(),
                        inferred_type: None,
                    };
                    return Ok(Expression::Div(Box::new(new_op)));
                }
            }
        }

        // CREATE DATABASE -> CREATE SCHEMA for DuckDB target
        if matches!(target, DialectType::DuckDB) {
            if let Expression::CreateDatabase(db) = e {
                let mut schema = crate::expressions::CreateSchema::new(db.name.name.clone());
                schema.if_not_exists = db.if_not_exists;
                return Ok(Expression::CreateSchema(Box::new(schema)));
            }
            if let Expression::DropDatabase(db) = e {
                let mut schema = crate::expressions::DropSchema::new(db.name.name.clone());
                schema.if_exists = db.if_exists;
                return Ok(Expression::DropSchema(Box::new(schema)));
            }
        }

        // Strip ClickHouse Nullable(...) wrapper for non-ClickHouse targets
        if matches!(source, DialectType::ClickHouse) && !matches!(target, DialectType::ClickHouse) {
            if let Expression::Cast(ref c) = e {
                if let DataType::Custom { ref name } = c.to {
                    if name.len() >= 9
                        && name[..9].eq_ignore_ascii_case("NULLABLE(")
                        && name.ends_with(")")
                    {
                        let inner = &name[9..name.len() - 1]; // strip "Nullable(" and ")"
                        let inner_upper = inner.to_ascii_uppercase();
                        let new_dt = match inner_upper.as_str() {
                            "DATETIME" | "DATETIME64" => DataType::Timestamp {
                                precision: None,
                                timezone: false,
                            },
                            "DATE" => DataType::Date,
                            "INT64" | "BIGINT" => DataType::BigInt { length: None },
                            "INT32" | "INT" | "INTEGER" => DataType::Int {
                                length: None,
                                integer_spelling: false,
                            },
                            "FLOAT64" | "DOUBLE" => DataType::Double {
                                precision: None,
                                scale: None,
                            },
                            "STRING" => DataType::Text,
                            _ => DataType::Custom {
                                name: inner.to_string(),
                            },
                        };
                        let mut new_cast = c.clone();
                        new_cast.to = new_dt;
                        return Ok(Expression::Cast(new_cast));
                    }
                }
            }
        }

        // ARRAY_CONCAT_AGG -> Snowflake: ARRAY_FLATTEN(ARRAY_AGG(...))
        if matches!(target, DialectType::Snowflake) {
            if let Expression::ArrayConcatAgg(ref agg) = e {
                let mut agg_clone = agg.as_ref().clone();
                agg_clone.name = None; // Clear name so generator uses default "ARRAY_AGG"
                let array_agg = Expression::ArrayAgg(Box::new(agg_clone));
                let flatten = Expression::Function(Box::new(Function::new(
                    "ARRAY_FLATTEN".to_string(),
                    vec![array_agg],
                )));
                return Ok(flatten);
            }
        }

        // ARRAY_CONCAT_AGG -> others: keep as function for cross-dialect
        if !matches!(target, DialectType::BigQuery | DialectType::Snowflake) {
            if let Expression::ArrayConcatAgg(agg) = e {
                let arg = agg.this;
                return Ok(Expression::Function(Box::new(Function::new(
                    "ARRAY_CONCAT_AGG".to_string(),
                    vec![arg],
                ))));
            }
        }

        // Determine what action to take by inspecting e immutably
        let action = {
            let source_propagates_nulls =
                matches!(source, DialectType::Snowflake | DialectType::BigQuery);
            let target_ignores_nulls =
                matches!(target, DialectType::DuckDB | DialectType::PostgreSQL);

            match &e {
                Expression::Subquery(subquery)
                    if matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && subquery.alias.is_some()
                        && subquery.column_aliases.is_empty()
                        && matches!(&subquery.this, Expression::Values(values) if !values.expressions.is_empty()) =>
                {
                    Action::Statements(
                        statements::Action::EnsureValuesDerivedTableColumnAliases,
                    )
                }
                Expression::Extract(_)
                    if matches!(source, DialectType::PostgreSQL)
                        && matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && temporal::is_postgres_time_date_part(&e) =>
                {
                    Action::Temporal(temporal::Action::PostgresDatePartForTsql)
                }
                Expression::Function(f) => {
                    let name = f.name.to_ascii_uppercase();
                    // DuckDB json(x) is a synonym for CAST(x AS JSON) — parses a string.
                    // Map to JSON_PARSE(x) for Trino/Presto/Athena to preserve semantics.
                    if matches!(source, DialectType::PostgreSQL)
                        && matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && temporal::is_postgres_date_part_function(&e)
                    {
                        Action::Temporal(temporal::Action::PostgresDatePartForTsql)
                    } else if name == "JSON"
                        && f.args.len() == 1
                        && matches!(source, DialectType::DuckDB)
                        && matches!(
                            target,
                            DialectType::Presto | DialectType::Trino | DialectType::Athena
                        )
                    {
                        Action::Json(json::Action::DuckDBJsonFuncToJsonParse)
                    // DuckDB json_valid(x) has no direct Trino equivalent; emit the
                    // SQL:2016 `x IS JSON` predicate which has matching semantics.
                    } else if name == "JSON_VALID"
                        && f.args.len() == 1
                        && matches!(source, DialectType::DuckDB)
                        && matches!(
                            target,
                            DialectType::Presto | DialectType::Trino | DialectType::Athena
                        )
                    {
                        Action::Json(json::Action::DuckDBJsonValidToIsJson)
                    // DATE_PART: strip quotes from first arg when target is Snowflake (source != Snowflake)
                    } else if (name == "DATE_PART" || name == "DATEPART")
                        && f.args.len() == 2
                        && matches!(target, DialectType::Snowflake)
                        && !matches!(source, DialectType::Snowflake)
                        && matches!(
                            &f.args[0],
                            Expression::Literal(lit) if matches!(lit.as_ref(), crate::expressions::Literal::String(_))
                        )
                    {
                        Action::Temporal(temporal::Action::DatePartUnquote)
                    } else if source_propagates_nulls
                        && target_ignores_nulls
                        && (name == "GREATEST" || name == "LEAST")
                        && f.args.len() >= 2
                    {
                        Action::Scalar(scalar::Action::GreatestLeastNull)
                    } else if matches!(source, DialectType::Snowflake)
                        && name == "ARRAY_GENERATE_RANGE"
                        && f.args.len() >= 2
                    {
                        Action::Collections(collections::Action::ArrayGenerateRange)
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                        && name == "DATE_TRUNC"
                        && f.args.len() == 2
                    {
                        // Determine if DuckDB DATE_TRUNC needs CAST wrapping to preserve input type.
                        // Logic based on Python sqlglot's input_type_preserved flag:
                        // - DATE + non-date-unit (HOUR, MINUTE, etc.) -> wrap
                        // - TIMESTAMP + date-unit (YEAR, QUARTER, MONTH, WEEK, DAY) -> wrap
                        // - TIMESTAMPTZ/TIMESTAMPLTZ/TIME -> always wrap
                        let unit_str = match &f.args[0] {
                            Expression::Literal(lit) if matches!(lit.as_ref(), crate::expressions::Literal::String(_)) => {
                                let crate::expressions::Literal::String(s) = lit.as_ref() else { unreachable!() };
                                Some(s.to_ascii_uppercase())
                            }
                            _ => None,
                        };
                        let is_date_unit = unit_str.as_ref().map_or(false, |u| {
                            matches!(u.as_str(), "YEAR" | "QUARTER" | "MONTH" | "WEEK" | "DAY")
                        });
                        match &f.args[1] {
                            Expression::Cast(c) => match &c.to {
                                DataType::Time { .. } => Action::Types(types::Action::DateTruncWrapCast),
                                DataType::Custom { name }
                                    if name.eq_ignore_ascii_case("TIMESTAMPTZ")
                                        || name.eq_ignore_ascii_case("TIMESTAMPLTZ") =>
                                {
                                    Action::Types(types::Action::DateTruncWrapCast)
                                }
                                DataType::Timestamp { timezone: true, .. } => {
                                    Action::Types(types::Action::DateTruncWrapCast)
                                }
                                DataType::Date if !is_date_unit => Action::Types(types::Action::DateTruncWrapCast),
                                DataType::Timestamp {
                                    timezone: false, ..
                                } if is_date_unit => Action::Types(types::Action::DateTruncWrapCast),
                                _ => Action::None,
                            },
                            _ => Action::None,
                        }
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                        && name == "TO_DATE"
                        && f.args.len() == 1
                        && !matches!(
                            &f.args[0],
                            Expression::Literal(lit) if matches!(lit.as_ref(), crate::expressions::Literal::String(_))
                        )
                    {
                        Action::Types(types::Action::ToDateToCast)
                    } else if !matches!(source, DialectType::Redshift)
                        && matches!(target, DialectType::Redshift)
                        && name == "CONVERT_TIMEZONE"
                        && (f.args.len() == 2 || f.args.len() == 3)
                    {
                        // Convert Function("CONVERT_TIMEZONE") to Expression::ConvertTimezone
                        // so Redshift's transform_expr won't expand 2-arg to 3-arg with 'UTC'.
                        // The Redshift parser adds 'UTC' as default source_tz, but when
                        // transpiling from other dialects, we should preserve the original form.
                        Action::Temporal(temporal::Action::ConvertTimezoneToExpr)
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                        && name == "REGEXP_REPLACE"
                        && f.args.len() == 4
                        && !matches!(
                            &f.args[3],
                            Expression::Literal(lit) if matches!(lit.as_ref(), crate::expressions::Literal::String(_))
                        )
                    {
                        // Snowflake REGEXP_REPLACE with position arg -> DuckDB needs 'g' flag
                        Action::Operators(operators::Action::RegexpReplaceSnowflakeToDuckDB)
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                        && name == "REGEXP_REPLACE"
                        && f.args.len() == 5
                    {
                        // Snowflake REGEXP_REPLACE(s, p, r, pos, occ) -> DuckDB
                        Action::Operators(operators::Action::RegexpReplacePositionSnowflakeToDuckDB)
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                        && name == "REGEXP_SUBSTR"
                    {
                        // Snowflake REGEXP_SUBSTR -> DuckDB REGEXP_EXTRACT variants
                        Action::Operators(operators::Action::RegexpSubstrSnowflakeToDuckDB)
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::Snowflake)
                        && (name == "REGEXP_SUBSTR" || name == "REGEXP_SUBSTR_ALL")
                        && f.args.len() == 6
                    {
                        // Snowflake identity: strip trailing group=0
                        Action::Operators(operators::Action::RegexpSubstrSnowflakeIdentity)
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                        && name == "REGEXP_SUBSTR_ALL"
                    {
                        // Snowflake REGEXP_SUBSTR_ALL -> DuckDB REGEXP_EXTRACT_ALL variants
                        Action::Operators(operators::Action::RegexpSubstrAllSnowflakeToDuckDB)
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                        && name == "REGEXP_COUNT"
                    {
                        // Snowflake REGEXP_COUNT -> DuckDB LENGTH(REGEXP_EXTRACT_ALL(...))
                        Action::Operators(operators::Action::RegexpCountSnowflakeToDuckDB)
                    } else if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                        && name == "REGEXP_INSTR"
                    {
                        // Snowflake REGEXP_INSTR -> DuckDB complex CASE expression
                        Action::Operators(operators::Action::RegexpInstrSnowflakeToDuckDB)
                    } else if matches!(source, DialectType::BigQuery)
                        && matches!(target, DialectType::Snowflake)
                        && name == "REGEXP_EXTRACT_ALL"
                    {
                        // BigQuery REGEXP_EXTRACT_ALL -> Snowflake REGEXP_SUBSTR_ALL
                        Action::Operators(operators::Action::RegexpExtractAllToSnowflake)
                    } else if name == "_BQ_TO_HEX" {
                        // Internal marker from TO_HEX conversion - bare (no LOWER/UPPER wrapper)
                        Action::Scalar(scalar::Action::BigQueryToHexBare)
                    } else if matches!(source, DialectType::BigQuery)
                        && !matches!(target, DialectType::BigQuery)
                    {
                        // BigQuery-specific functions that need to be converted to standard forms
                        match name.as_str() {
                            "TIMESTAMP_DIFF" | "DATETIME_DIFF" | "TIME_DIFF"
                            | "DATE_DIFF"
                            | "TIMESTAMP_ADD" | "TIMESTAMP_SUB"
                            | "DATETIME_ADD" | "DATETIME_SUB"
                            | "TIME_ADD" | "TIME_SUB"
                            | "DATE_ADD" | "DATE_SUB"
                            | "SAFE_DIVIDE"
                            | "GENERATE_UUID"
                            | "COUNTIF"
                            | "EDIT_DISTANCE"
                            | "TIMESTAMP_SECONDS" | "TIMESTAMP_MILLIS" | "TIMESTAMP_MICROS"
                            | "TIMESTAMP_TRUNC" | "DATETIME_TRUNC" | "DATE_TRUNC"
                            | "TO_HEX"
                            | "TO_JSON_STRING"
                            | "GENERATE_ARRAY" | "GENERATE_TIMESTAMP_ARRAY"
                            | "DIV"
                            | "UNIX_DATE" | "UNIX_SECONDS" | "UNIX_MILLIS" | "UNIX_MICROS"
                            | "LAST_DAY"
                            | "TIME" | "DATETIME" | "TIMESTAMP" | "STRING"
                            | "REGEXP_CONTAINS"
                            | "CONTAINS_SUBSTR"
                            | "SAFE_ADD" | "SAFE_SUBTRACT" | "SAFE_MULTIPLY"
                            | "SAFE_CAST"
                            | "GENERATE_DATE_ARRAY"
                            | "PARSE_DATE" | "PARSE_DATETIME" | "PARSE_TIMESTAMP"
                            | "FORMAT_DATE" | "FORMAT_DATETIME" | "FORMAT_TIMESTAMP"
                            | "ARRAY_CONCAT"
                            | "JSON_QUERY" | "JSON_VALUE_ARRAY"
                            | "INSTR"
                            | "MD5" | "SHA1" | "SHA256" | "SHA512"
                            | "GENERATE_UUID()" // just in case
                            | "REGEXP_EXTRACT_ALL"
                            | "REGEXP_EXTRACT"
                            | "INT64"
                            | "ARRAY_CONCAT_AGG"
                            | "DATE_DIFF(" // just in case
                            | "TO_HEX_MD5" // internal
                            | "MOD"
                            | "CONCAT"
                            | "CURRENT_TIMESTAMP" | "CURRENT_DATE" | "CURRENT_DATETIME" | "CURRENT_TIME"
                            | "STRUCT"
                            | "ROUND"
                            | "MAKE_INTERVAL"
                            | "ARRAY_TO_STRING"
                            | "PERCENTILE_CONT"
                                => Action::Scalar(scalar::Action::BigQueryFunctionNormalize),
                            "ARRAY" if matches!(target, DialectType::Snowflake)
                                && f.args.len() == 1
                                && matches!(&f.args[0], Expression::Select(s) if s.kind.as_deref() == Some("STRUCT"))
                                => Action::Collections(collections::Action::BigQueryArraySelectAsStructToSnowflake),
                            _ => Action::None,
                        }
                    } else if matches!(source, DialectType::BigQuery)
                        && matches!(target, DialectType::BigQuery)
                    {
                        // BigQuery -> BigQuery normalizations
                        match name.as_str() {
                            "TIMESTAMP_DIFF"
                            | "DATETIME_DIFF"
                            | "TIME_DIFF"
                            | "DATE_DIFF"
                            | "DATE_ADD"
                            | "TO_HEX"
                            | "CURRENT_TIMESTAMP"
                            | "CURRENT_DATE"
                            | "CURRENT_TIME"
                            | "CURRENT_DATETIME"
                            | "GENERATE_DATE_ARRAY"
                            | "INSTR"
                            | "FORMAT_DATETIME"
                            | "DATETIME"
                            | "MAKE_INTERVAL" => Action::Scalar(scalar::Action::BigQueryFunctionNormalize),
                            _ => Action::None,
                        }
                    } else {
                        // Generic function normalization for non-BigQuery sources
                        match name.as_str() {
                            "CONCAT" if f.args.len() == 1
                                && matches!(source, DialectType::PostgreSQL)
                                && matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                            {
                                Action::Scalar(scalar::Action::PostgresSingleValueConcatToTsql)
                            }
                            "CONCAT_WS" if f.args.len() == 2
                                && matches!(source, DialectType::PostgreSQL)
                                && matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                            {
                                Action::Scalar(scalar::Action::PostgresSingleValueConcatToTsql)
                            }
                            "ARBITRARY" | "AGGREGATE"
                            | "REGEXP_MATCHES" | "REGEXP_FULL_MATCH"
                            | "STRUCT_EXTRACT"
                            | "LIST_FILTER" | "LIST_TRANSFORM" | "LIST_SORT" | "LIST_REVERSE_SORT"
                            | "STRING_TO_ARRAY" | "STR_SPLIT" | "STR_SPLIT_REGEX" | "SPLIT_TO_ARRAY"
                            | "SUBSTRINGINDEX"
                            | "ARRAY_LENGTH" | "SIZE" | "CARDINALITY"
                            | "UNICODE"
                            | "XOR"
                            | "ARRAY_REVERSE_SORT"
                            | "ENCODE" | "DECODE"
                            | "QUANTILE"
                            | "EPOCH" | "EPOCH_MS"
                            | "HASHBYTES"
                            | "JSON_EXTRACT_PATH" | "JSON_EXTRACT_PATH_TEXT"
                            | "APPROX_DISTINCT"
                            | "DATE_PARSE" | "FORMAT_DATETIME"
                            | "REGEXP_EXTRACT" | "REGEXP_SUBSTR" | "TO_DAYS"
                            | "RLIKE"
                            | "DATEDIFF" | "DATE_DIFF" | "MONTHS_BETWEEN"
                            | "ADD_MONTHS" | "DATEADD" | "DATE_ADD" | "DATE_SUB" | "DATETRUNC"
                            | "LAST_DAY" | "LAST_DAY_OF_MONTH" | "EOMONTH"
                            | "ARRAY_CONSTRUCT" | "ARRAY_CAT" | "ARRAY_COMPACT"
                            | "ARRAY_FILTER" | "FILTER" | "REDUCE" | "ARRAY_REVERSE"
                            | "MAP" | "MAP_FROM_ENTRIES"
                            | "COLLECT_LIST" | "COLLECT_SET"
                            | "ISNAN" | "IS_NAN"
                            | "TO_UTC_TIMESTAMP" | "FROM_UTC_TIMESTAMP"
                            | "FORMAT_NUMBER"
                            | "TOMONDAY" | "TOSTARTOFWEEK" | "TOSTARTOFMONTH" | "TOSTARTOFYEAR"
                            | "ELEMENT_AT"
                            | "EXPLODE" | "EXPLODE_OUTER" | "POSEXPLODE"
                            | "SPLIT_PART"
                            // GENERATE_SERIES: handled separately below
                            | "JSON_EXTRACT" | "JSON_EXTRACT_SCALAR"
                            | "JSON_QUERY" | "JSON_VALUE"
                            | "JSON_SEARCH"
                            | "JSON_EXTRACT_JSON" | "BSON_EXTRACT_BSON"
                            | "TO_UNIX_TIMESTAMP" | "UNIX_TIMESTAMP"
                            | "CURDATE" | "CURTIME"
                            | "ARRAY_TO_STRING"
                            | "ARRAY_SORT" | "SORT_ARRAY"
                            | "LEFT" | "RIGHT"
                            | "MAP_FROM_ARRAYS"
                            | "LIKE" | "ILIKE"
                            | "ARRAY_CONCAT" | "LIST_CONCAT"
                            | "QUANTILE_CONT" | "QUANTILE_DISC"
                            | "PERCENTILE_CONT" | "PERCENTILE_DISC"
                            | "PERCENTILE_APPROX" | "APPROX_PERCENTILE"
                            | "LOCATE" | "STRPOS" | "INSTR"
                            | "CHAR"
                            // CONCAT: handled separately for COALESCE wrapping
                            | "ARRAY_JOIN"
                            | "ARRAY_CONTAINS" | "HAS" | "CONTAINS"
                            | "ISNULL"
                            | "MONTHNAME"
                            | "TO_TIMESTAMP"
                            | "TO_DATE"
                            | "TO_JSON"
                            | "REGEXP_SPLIT"
                            | "SPLIT"
                            | "FORMATDATETIME"
                            | "ARRAYJOIN"
                            | "SPLITBYSTRING" | "SPLITBYREGEXP"
                            | "NVL"
                            | "TO_CHAR"
                            | "DBMS_RANDOM.VALUE"
                            | "REGEXP_LIKE"
                            | "REPLICATE"
                            | "LEN"
                            | "COUNT_BIG"
                            | "DATEFROMPARTS"
                            | "DATETIMEFROMPARTS"
                            | "CONVERT" | "TRY_CONVERT"
                            | "STRFTIME" | "STRPTIME"
                            | "DATE_FORMAT" | "FORMAT_DATE"
                            | "PARSE_TIMESTAMP" | "PARSE_DATETIME" | "PARSE_DATE"
                            | "FROM_ISO8601_TIMESTAMP" | "FROM_ISO8601_DATE"
                            | "FROM_BASE64" | "TO_BASE64"
                            | "GETDATE"
                            | "TO_HEX" | "FROM_HEX" | "UNHEX" | "HEX"
                            | "TO_UTF8" | "FROM_UTF8"
                            | "STARTS_WITH" | "STARTSWITH"
                            | "APPROX_COUNT_DISTINCT"
                            | "JSON_FORMAT"
                            | "SYSDATE"
                            | "LOGICAL_OR" | "LOGICAL_AND"
                            | "MONTHS_ADD"
                            | "SCHEMA_NAME"
                            | "STRTOL"
                            | "EDITDIST3"
                            | "FORMAT"
                            | "LIST_CONTAINS" | "LIST_HAS"
                            | "VARIANCE" | "STDDEV"
                            | "ISINF"
                            | "TO_UNIXTIME"
                            | "FROM_UNIXTIME"
                            | "DATEPART" | "DATE_PART"
                            | "DATENAME"
                            | "STRING_AGG"
                            | "JSON_ARRAYAGG"
                            | "APPROX_QUANTILE"
                            | "MAKE_DATE"
                            | "LIST_HAS_ANY" | "ARRAY_HAS_ANY"
                            | "RANGE"
                            | "TRY_ELEMENT_AT"
                            | "STR_TO_MAP"
                            | "STRING"
                            | "STR_TO_TIME"
                            | "CURRENT_SCHEMA"
                            | "LTRIM" | "RTRIM"
                            | "UUID"
                            | "FARM_FINGERPRINT"
                            | "JSON_KEYS"
                            | "WEEKOFYEAR"
                            | "CONCAT_WS"
                            | "TRY_DIVIDE"
                            | "ARRAY_SLICE"
                            | "ARRAY_PREPEND"
                            | "ARRAY_REMOVE"
                            | "GENERATE_DATE_ARRAY"
                            | "PARSE_JSON"
                            | "JSON_REMOVE"
                            | "JSON_SET"
                            | "LEVENSHTEIN"
                            | "CURRENT_VERSION"
                            | "ARRAY_MAX"
                            | "ARRAY_MIN"
                            | "JAROWINKLER_SIMILARITY"
                            | "CURRENT_SCHEMAS"
                            | "TO_VARIANT"
                            | "JSON_GROUP_ARRAY" | "JSON_GROUP_OBJECT"
                            | "ARRAYS_OVERLAP" | "ARRAY_INTERSECTION"
                                => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                            // Canonical date functions -> dialect-specific
                            "TS_OR_DS_TO_DATE" => Action::Temporal(temporal::Action::TsOrDsToDateConvert),
                            "TS_OR_DS_TO_DATE_STR" if f.args.len() == 1 => Action::Temporal(temporal::Action::TsOrDsToDateStrConvert),
                            "DATE_STR_TO_DATE" if f.args.len() == 1 => Action::Temporal(temporal::Action::DateStrToDateConvert),
                            "TIME_STR_TO_DATE" if f.args.len() == 1 => Action::Temporal(temporal::Action::TimeStrToDateConvert),
                            "TIME_STR_TO_TIME" if f.args.len() <= 2 => Action::Temporal(temporal::Action::TimeStrToTimeConvert),
                            "TIME_STR_TO_UNIX" if f.args.len() == 1 => Action::Temporal(temporal::Action::TimeStrToUnixConvert),
                            "TIME_TO_TIME_STR" if f.args.len() == 1 => Action::Temporal(temporal::Action::TimeToTimeStrConvert),
                            "DATE_TO_DATE_STR" if f.args.len() == 1 => Action::Temporal(temporal::Action::DateToDateStrConvert),
                            "DATE_TO_DI" if f.args.len() == 1 => Action::Temporal(temporal::Action::DateToDiConvert),
                            "DI_TO_DATE" if f.args.len() == 1 => Action::Temporal(temporal::Action::DiToDateConvert),
                            "TS_OR_DI_TO_DI" if f.args.len() == 1 => Action::Temporal(temporal::Action::TsOrDiToDiConvert),
                            "UNIX_TO_STR" if f.args.len() == 2 => Action::Temporal(temporal::Action::UnixToStrConvert),
                            "UNIX_TO_TIME" if f.args.len() == 1 => Action::Temporal(temporal::Action::UnixToTimeConvert),
                            "UNIX_TO_TIME_STR" if f.args.len() == 1 => Action::Temporal(temporal::Action::UnixToTimeStrConvert),
                            "TIME_TO_UNIX" if f.args.len() == 1 => Action::Temporal(temporal::Action::TimeToUnixConvert),
                            "TIME_TO_STR" if f.args.len() == 2 => Action::Temporal(temporal::Action::TimeToStrConvert),
                            "STR_TO_UNIX" if f.args.len() == 2 => Action::Temporal(temporal::Action::StrToUnixConvert),
                            // STR_TO_DATE(x, fmt) -> dialect-specific
                            "STR_TO_DATE" if f.args.len() == 2
                                && matches!(source, DialectType::Generic) => Action::Temporal(temporal::Action::StrToDateConvert),
                            "STR_TO_DATE" => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                            // TS_OR_DS_ADD(x, n, 'UNIT') from Generic -> dialect-specific DATE_ADD
                            "TS_OR_DS_ADD" if f.args.len() == 3
                                && matches!(source, DialectType::Generic) => Action::Temporal(temporal::Action::TsOrDsAddConvert),
                            // DATE_FROM_UNIX_DATE(n) -> DATEADD(DAY, n, '1970-01-01')
                            "DATE_FROM_UNIX_DATE" if f.args.len() == 1 => Action::Temporal(temporal::Action::DateFromUnixDateConvert),
                            // NVL2(a, b, c) -> CASE WHEN NOT a IS NULL THEN b [ELSE c] END
                            "NVL2" if (f.args.len() == 2 || f.args.len() == 3) => Action::Scalar(scalar::Action::Nvl2Expand),
                            // IFNULL(a, b) -> COALESCE(a, b) when coming from Generic source
                            "IFNULL" if f.args.len() == 2 => Action::Scalar(scalar::Action::IfnullToCoalesce),
                            // IS_ASCII(x) -> dialect-specific
                            "IS_ASCII" if f.args.len() == 1 => Action::Scalar(scalar::Action::IsAsciiConvert),
                            // STR_POSITION(haystack, needle[, pos[, occ]]) -> dialect-specific
                            "STR_POSITION" => Action::Scalar(scalar::Action::StrPositionConvert),
                            // ARRAY_SUM -> dialect-specific
                            "ARRAY_SUM" => Action::Collections(collections::Action::ArraySumConvert),
                            // ARRAY_SIZE -> dialect-specific (Drill only)
                            "ARRAY_SIZE" if matches!(target, DialectType::Drill) => Action::Collections(collections::Action::ArraySizeConvert),
                            // ARRAY_ANY -> dialect-specific
                            "ARRAY_ANY" if f.args.len() == 2 => Action::Collections(collections::Action::ArrayAnyConvert),
                            // Functions needing specific cross-dialect transforms
                            "MAX_BY" | "MIN_BY" if matches!(target, DialectType::ClickHouse | DialectType::Spark | DialectType::Databricks | DialectType::DuckDB) => Action::Aggregates(aggregates::Action::MaxByMinByConvert),
                            "STRUCT" if matches!(source, DialectType::Spark | DialectType::Databricks)
                                && !matches!(target, DialectType::Spark | DialectType::Databricks | DialectType::Hive) => Action::Collections(collections::Action::SparkStructConvert),
                            "ARRAY" if matches!(source, DialectType::BigQuery)
                                && matches!(target, DialectType::Snowflake)
                                && f.args.len() == 1
                                && matches!(&f.args[0], Expression::Select(s) if s.kind.as_deref() == Some("STRUCT")) => Action::Collections(collections::Action::BigQueryArraySelectAsStructToSnowflake),
                            "ARRAY" if matches!(target, DialectType::Presto | DialectType::Trino | DialectType::Athena | DialectType::BigQuery | DialectType::DuckDB | DialectType::Snowflake | DialectType::ClickHouse | DialectType::StarRocks) => Action::Collections(collections::Action::ArraySyntaxConvert),
                            "TRUNC" if f.args.len() == 2 && matches!(&f.args[1], Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_))) && matches!(target, DialectType::Presto | DialectType::Trino | DialectType::ClickHouse) => Action::Temporal(temporal::Action::TruncToDateTrunc),
                            "TRUNC" | "TRUNCATE" if f.args.len() <= 2 && !f.args.get(1).map_or(false, |a| matches!(a, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))) => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                            // DATE_TRUNC('unit', x) from Generic source -> arg swap for BigQuery/Doris/Spark/MySQL
                            "DATE_TRUNC" if f.args.len() == 2
                                && matches!(source, DialectType::Generic)
                                && matches!(target, DialectType::BigQuery | DialectType::Doris | DialectType::StarRocks
                                    | DialectType::Spark | DialectType::Databricks | DialectType::MySQL) => Action::Temporal(temporal::Action::DateTruncSwapArgs),
                            // TIMESTAMP_TRUNC(x, UNIT) from Generic source -> convert to per-dialect
                            "TIMESTAMP_TRUNC" if f.args.len() >= 2
                                && matches!(source, DialectType::Generic) => Action::Temporal(temporal::Action::TimestampTruncConvert),
                            "UNIFORM" if matches!(target, DialectType::Snowflake) => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                            // GENERATE_SERIES -> SEQUENCE/UNNEST/EXPLODE for target dialects
                            "GENERATE_SERIES" if matches!(source, DialectType::PostgreSQL | DialectType::Redshift)
                                && !matches!(target, DialectType::PostgreSQL | DialectType::Redshift | DialectType::TSQL | DialectType::Fabric) => Action::Collections(collections::Action::GenerateSeriesConvert),
                            // GENERATE_SERIES with interval normalization for PG target
                            "GENERATE_SERIES" if f.args.len() >= 3
                                && matches!(source, DialectType::PostgreSQL | DialectType::Redshift)
                                && matches!(target, DialectType::PostgreSQL | DialectType::Redshift) => Action::Collections(collections::Action::GenerateSeriesConvert),
                            "GENERATE_SERIES" => Action::None, // passthrough for other cases
                            // CONCAT(a, b) -> COALESCE wrapping for Presto/ClickHouse from PostgreSQL
                            "CONCAT" if matches!(source, DialectType::PostgreSQL | DialectType::Redshift)
                                && matches!(target, DialectType::Presto | DialectType::Trino | DialectType::ClickHouse) => Action::Scalar(scalar::Action::ConcatCoalesceWrap),
                            "CONCAT" => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                            // CBRT(x) -> POWER(CAST(x AS FLOAT), 1.0 / 3.0)
                            "CBRT" if f.args.len() == 1
                                && Dialect::is_postgres_family_source(source)
                                && matches!(target, DialectType::TSQL | DialectType::Fabric) => Action::Scalar(scalar::Action::CbrtToPower),
                            "JSON_BUILD_OBJECT" | "JSONB_BUILD_OBJECT"
                                if Dialect::is_postgres_family_source(source)
                                    && matches!(target, DialectType::TSQL | DialectType::Fabric)
                                    && f.args.len() % 2 == 0 =>
                            {
                                Action::Json(json::Action::PostgresJsonBuildObjectToJsonObject)
                            }
                            "JSON_AGG" | "JSONB_AGG"
                                if Dialect::is_postgres_family_source(source)
                                    && matches!(target, DialectType::TSQL | DialectType::Fabric)
                                    && !f.quoted
                                    && f.args.len() == 1 =>
                            {
                                Action::Json(json::Action::PostgresJsonAggToJsonArrayAgg)
                            }
                            // DIV(a, b) -> target-specific integer division
                            "DIV" if f.args.len() == 2
                                && Dialect::is_postgres_family_source(source)
                                && matches!(
                                    target,
                                    DialectType::DuckDB
                                        | DialectType::BigQuery
                                        | DialectType::SQLite
                                        | DialectType::TSQL
                                        | DialectType::Fabric
                                ) => Action::Operators(operators::Action::DivFuncConvert),
                            // JSON_OBJECT_AGG/JSONB_OBJECT_AGG -> JSON_GROUP_OBJECT for DuckDB
                            "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG" if f.args.len() == 2
                                && matches!(target, DialectType::DuckDB) => Action::Json(json::Action::JsonObjectAggConvert),
                            // JSONB_EXISTS -> JSON_EXISTS for DuckDB
                            "JSONB_EXISTS" if f.args.len() == 2
                                && matches!(target, DialectType::DuckDB) => Action::Json(json::Action::JsonbExistsConvert),
                            // DATE_BIN -> TIME_BUCKET for DuckDB
                            "DATE_BIN" if matches!(target, DialectType::DuckDB) => Action::Temporal(temporal::Action::DateBinConvert),
                            // Multi-arg MIN(a,b,c) -> LEAST, MAX(a,b,c) -> GREATEST
                            "MIN" | "MAX" if f.args.len() > 1 && !matches!(target, DialectType::SQLite) => Action::Scalar(scalar::Action::MinMaxToLeastGreatest),
                            // ClickHouse uniq -> APPROX_COUNT_DISTINCT for other dialects
                            "UNIQ" if matches!(source, DialectType::ClickHouse) && !matches!(target, DialectType::ClickHouse) => Action::Aggregates(aggregates::Action::ClickHouseUniqToApproxCountDistinct),
                            // ClickHouse any -> ANY_VALUE for other dialects
                            "ANY" if f.args.len() == 1 && matches!(source, DialectType::ClickHouse) && !matches!(target, DialectType::ClickHouse) => Action::Aggregates(aggregates::Action::ClickHouseAnyToAnyValue),
                            _ => Action::None,
                        }
                    }
                }
                Expression::AggregateFunction(af) => {
                    let name = af.name.to_ascii_uppercase();
                    match name.as_str() {
                        "ARBITRARY" | "AGGREGATE" => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                        "JSON_AGG" | "JSONB_AGG"
                            if Dialect::is_postgres_family_source(source)
                                && matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                        {
                            Action::Json(json::Action::PostgresJsonAggToJsonArrayAgg)
                        }
                        "JSON_ARRAYAGG" => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                        // JSON_OBJECT_AGG/JSONB_OBJECT_AGG -> JSON_GROUP_OBJECT for DuckDB
                        "JSON_OBJECT_AGG" | "JSONB_OBJECT_AGG"
                            if matches!(target, DialectType::DuckDB) =>
                        {
                            Action::Json(json::Action::JsonObjectAggConvert)
                        }
                        "ARRAY_AGG"
                            if matches!(
                                target,
                                DialectType::Hive
                                    | DialectType::Spark
                                    | DialectType::Databricks
                            ) =>
                        {
                            Action::Aggregates(aggregates::Action::ArrayAggToCollectList)
                        }
                        "MAX_BY" | "MIN_BY"
                            if matches!(
                                target,
                                DialectType::ClickHouse
                                    | DialectType::Spark
                                    | DialectType::Databricks
                                    | DialectType::DuckDB
                            ) =>
                        {
                            Action::Aggregates(aggregates::Action::MaxByMinByConvert)
                        }
                        "COLLECT_LIST"
                            if matches!(
                                target,
                                DialectType::Presto | DialectType::Trino | DialectType::DuckDB
                            ) =>
                        {
                            Action::Aggregates(aggregates::Action::CollectListToArrayAgg)
                        }
                        "COLLECT_SET"
                            if matches!(
                                target,
                                DialectType::Presto
                                    | DialectType::Trino
                                    | DialectType::Snowflake
                                    | DialectType::DuckDB
                            ) =>
                        {
                            Action::Aggregates(aggregates::Action::CollectSetConvert)
                        }
                        "PERCENTILE"
                            if matches!(
                                target,
                                DialectType::DuckDB | DialectType::Presto | DialectType::Trino
                            ) =>
                        {
                            Action::Aggregates(aggregates::Action::PercentileConvert)
                        }
                        // CORR -> CASE WHEN ISNAN(CORR(a,b)) THEN NULL ELSE CORR(a,b) END for DuckDB
                        "CORR"
                            if matches!(target, DialectType::DuckDB)
                                && matches!(source, DialectType::Snowflake) =>
                        {
                            Action::Aggregates(aggregates::Action::CorrIsnanWrap)
                        }
                        // BigQuery APPROX_QUANTILES(x, n) -> APPROX_QUANTILE(x, [quantiles]) for DuckDB
                        "APPROX_QUANTILES"
                            if matches!(source, DialectType::BigQuery)
                                && matches!(target, DialectType::DuckDB) =>
                        {
                            Action::Aggregates(aggregates::Action::BigQueryApproxQuantiles)
                        }
                        // BigQuery PERCENTILE_CONT(x, frac RESPECT NULLS) -> QUANTILE_CONT(x, frac) for DuckDB
                        "PERCENTILE_CONT"
                            if matches!(source, DialectType::BigQuery)
                                && matches!(target, DialectType::DuckDB)
                                && af.args.len() >= 2 =>
                        {
                            Action::Aggregates(aggregates::Action::BigQueryPercentileContToDuckDB)
                        }
                        _ => Action::None,
                    }
                }
                ref expression
                    if source != target
                        && matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && json::has_json_constructor_return_type(expression) =>
                {
                    Action::Json(json::Action::TsqlJsonConstructorReturnType)
                }
                Expression::JSONArrayAgg(_) => match target {
                    DialectType::PostgreSQL => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                    _ => Action::None,
                },
                Expression::ToNumber(tn) => {
                    // TO_NUMBER(x) with 1 arg -> CAST(x AS DOUBLE) for most targets
                    if tn.format.is_none() && tn.precision.is_none() && tn.scale.is_none() {
                        match target {
                            DialectType::Oracle
                            | DialectType::Snowflake
                            | DialectType::Teradata => Action::None,
                            _ => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                        }
                    } else {
                        Action::None
                    }
                }
                Expression::Nvl2(_) => {
                    // NVL2(a, b, c) -> CASE WHEN NOT a IS NULL THEN b ELSE c END for most dialects
                    // Keep as NVL2 for dialects that support it natively
                    match target {
                        DialectType::Oracle
                        | DialectType::Snowflake
                        | DialectType::Teradata
                        | DialectType::Spark
                        | DialectType::Databricks
                        | DialectType::Redshift => Action::None,
                        _ => Action::Scalar(scalar::Action::Nvl2Expand),
                    }
                }
                Expression::Decode(_) | Expression::DecodeCase(_) => {
                    // DECODE(a, b, c[, d, e[, ...]]) -> CASE WHEN with null-safe comparisons
                    // Keep as DECODE for Oracle/Snowflake
                    match target {
                        DialectType::Oracle | DialectType::Snowflake => Action::None,
                        _ => Action::Scalar(scalar::Action::DecodeSimplify),
                    }
                }
                Expression::Coalesce(ref cf) => {
                    // IFNULL(a, b) -> COALESCE(a, b): clear original_name for cross-dialect
                    // BigQuery keeps IFNULL natively when source is also BigQuery
                    if cf.original_name.as_deref() == Some("IFNULL")
                        && !(matches!(source, DialectType::BigQuery)
                            && matches!(target, DialectType::BigQuery))
                    {
                        Action::Scalar(scalar::Action::IfnullToCoalesce)
                    } else {
                        Action::None
                    }
                }
                Expression::IfFunc(if_func) => {
                    if matches!(source, DialectType::Snowflake)
                        && matches!(
                            target,
                            DialectType::Presto | DialectType::Trino | DialectType::SQLite
                        )
                        && matches!(if_func.false_value, Some(Expression::Div(_)))
                    {
                        Action::Operators(operators::Action::Div0TypedDivision)
                    } else {
                        Action::None
                    }
                }
                Expression::ToJson(_) => match target {
                    DialectType::Presto | DialectType::Trino => Action::Json(json::Action::ToJsonConvert),
                    DialectType::BigQuery => Action::Json(json::Action::ToJsonConvert),
                    DialectType::DuckDB => Action::Json(json::Action::ToJsonConvert),
                    _ => Action::None,
                },
                Expression::ArrayAgg(ref agg) => {
                    if matches!(target, DialectType::MySQL | DialectType::SingleStore) {
                        Action::Aggregates(aggregates::Action::ArrayAggToGroupConcat)
                    } else if matches!(
                        target,
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks
                    ) {
                        // Any source -> Hive/Spark: convert ARRAY_AGG to COLLECT_LIST
                        Action::Aggregates(aggregates::Action::ArrayAggToCollectList)
                    } else if matches!(
                        source,
                        DialectType::Spark | DialectType::Databricks | DialectType::Hive
                    ) && matches!(target, DialectType::DuckDB)
                        && agg.filter.is_some()
                    {
                        // Spark/Hive ARRAY_AGG excludes NULLs, DuckDB includes them
                        // Need to add NOT x IS NULL to existing filter
                        Action::Aggregates(aggregates::Action::ArrayAggNullFilter)
                    } else if matches!(target, DialectType::DuckDB)
                        && agg.ignore_nulls == Some(true)
                        && !agg.order_by.is_empty()
                    {
                        // BigQuery ARRAY_AGG(x IGNORE NULLS ORDER BY ...) -> DuckDB ARRAY_AGG(x ORDER BY a NULLS FIRST, ...)
                        Action::Aggregates(aggregates::Action::ArrayAggIgnoreNullsDuckDB)
                    } else if !matches!(source, DialectType::Snowflake) {
                        Action::None
                    } else if matches!(target, DialectType::Spark | DialectType::Databricks) {
                        let is_array_agg = agg.name.as_deref().map_or(false, |n| n.eq_ignore_ascii_case("ARRAY_AGG"))
                            || agg.name.is_none();
                        if is_array_agg {
                            Action::Aggregates(aggregates::Action::ArrayAggCollectList)
                        } else {
                            Action::None
                        }
                    } else if matches!(
                        target,
                        DialectType::DuckDB | DialectType::Presto | DialectType::Trino
                    ) && agg.filter.is_none()
                    {
                        Action::Aggregates(aggregates::Action::ArrayAggFilter)
                    } else {
                        Action::None
                    }
                }
                Expression::WithinGroup(wg) => {
                    if matches!(source, DialectType::Snowflake)
                        && matches!(
                            target,
                            DialectType::DuckDB | DialectType::Presto | DialectType::Trino
                        )
                        && matches!(wg.this, Expression::ArrayAgg(_))
                    {
                        Action::Aggregates(aggregates::Action::ArrayAggWithinGroupFilter)
                    } else if matches!(&wg.this, Expression::AggregateFunction(af) if af.name.eq_ignore_ascii_case("STRING_AGG"))
                        || matches!(&wg.this, Expression::Function(f) if f.name.eq_ignore_ascii_case("STRING_AGG"))
                        || matches!(&wg.this, Expression::StringAgg(_))
                    {
                        Action::Aggregates(aggregates::Action::StringAggConvert)
                    } else if matches!(
                        target,
                        DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Athena
                            | DialectType::Spark
                            | DialectType::Databricks
                    ) && (matches!(&wg.this, Expression::Function(f) if f.name.eq_ignore_ascii_case("PERCENTILE_CONT") || f.name.eq_ignore_ascii_case("PERCENTILE_DISC"))
                        || matches!(&wg.this, Expression::AggregateFunction(af) if af.name.eq_ignore_ascii_case("PERCENTILE_CONT") || af.name.eq_ignore_ascii_case("PERCENTILE_DISC"))
                        || matches!(&wg.this, Expression::PercentileCont(_)))
                    {
                        Action::Aggregates(aggregates::Action::PercentileContConvert)
                    } else {
                        Action::None
                    }
                }
                // For BigQuery: CAST(x AS TIMESTAMP) -> CAST(x AS DATETIME)
                // because BigQuery's TIMESTAMP is really TIMESTAMPTZ, and
                // DATETIME is the timezone-unaware type
                Expression::Cast(ref c) => {
                    if matches!(source, DialectType::PostgreSQL)
                        && matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && types::is_postgres_unknown_null_cast(c)
                    {
                        Action::Types(types::Action::PostgresUnknownNullCast)
                    } else if c.format.is_some()
                        && (matches!(source, DialectType::BigQuery)
                            || matches!(source, DialectType::Teradata))
                    {
                        Action::Types(types::Action::BigQueryCastFormat)
                    } else if matches!(source, DialectType::PostgreSQL)
                        && matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && types::is_postgres_float_to_integer_cast(c)
                    {
                        Action::Types(types::Action::PostgresFloatToIntegerCast)
                    } else if matches!(target, DialectType::BigQuery)
                        && !matches!(source, DialectType::BigQuery)
                        && matches!(
                            c.to,
                            DataType::Timestamp {
                                timezone: false,
                                ..
                            }
                        )
                    {
                        Action::Types(types::Action::CastTimestampToDatetime)
                    } else if matches!(target, DialectType::MySQL | DialectType::StarRocks)
                        && !matches!(source, DialectType::MySQL | DialectType::StarRocks)
                        && matches!(
                            c.to,
                            DataType::Timestamp {
                                timezone: false,
                                ..
                            }
                        )
                    {
                        // Generic/other -> MySQL/StarRocks: CAST(x AS TIMESTAMP) -> CAST(x AS DATETIME)
                        // but MySQL-native CAST(x AS TIMESTAMP) stays as TIMESTAMP(x) via transform_cast
                        Action::Types(types::Action::CastTimestampToDatetime)
                    } else if matches!(
                        source,
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks
                    ) && matches!(
                        target,
                        DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Athena
                            | DialectType::DuckDB
                            | DialectType::Snowflake
                            | DialectType::BigQuery
                            | DialectType::Databricks
                            | DialectType::TSQL
                    ) {
                        Action::Types(types::Action::HiveCastToTryCast)
                    } else if matches!(c.to, DataType::Timestamp { timezone: true, .. })
                        && matches!(target, DialectType::MySQL | DialectType::StarRocks)
                    {
                        // CAST(x AS TIMESTAMPTZ) -> TIMESTAMP(x) function for MySQL/StarRocks
                        Action::Types(types::Action::CastTimestamptzToFunc)
                    } else if matches!(c.to, DataType::Timestamp { timezone: true, .. })
                        && matches!(
                            target,
                            DialectType::Hive
                                | DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::BigQuery
                        )
                    {
                        // CAST(x AS TIMESTAMP WITH TIME ZONE) -> CAST(x AS TIMESTAMP) for Hive/Spark/BigQuery
                        Action::Types(types::Action::CastTimestampStripTz)
                    } else if matches!(&c.to, DataType::Json)
                        && matches!(source, DialectType::DuckDB)
                        && matches!(target, DialectType::Snowflake)
                    {
                        Action::Json(json::Action::DuckDBCastJsonToVariant)
                    } else if matches!(&c.to, DataType::Json)
                        && matches!(&c.this, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                        && matches!(
                            target,
                            DialectType::Presto
                                | DialectType::Trino
                                | DialectType::Athena
                                | DialectType::Snowflake
                        )
                    {
                        // CAST('x' AS JSON) -> JSON_PARSE('x') for Presto, PARSE_JSON for Snowflake
                        // Only when the input is a string literal (JSON 'value' syntax)
                        Action::Json(json::Action::JsonLiteralToJsonParse)
                    } else if matches!(&c.to, DataType::Json)
                        && matches!(source, DialectType::DuckDB)
                        && matches!(
                            target,
                            DialectType::Presto | DialectType::Trino | DialectType::Athena
                        )
                    {
                        // DuckDB's CAST(x AS JSON) parses the string value into a JSON value.
                        // Trino/Presto/Athena's CAST(x AS JSON) instead wraps the value as a
                        // JSON string (no parsing) — different semantics. Use JSON_PARSE(x)
                        // in the target to preserve DuckDB's parse semantics.
                        Action::Json(json::Action::JsonLiteralToJsonParse)
                    } else if matches!(&c.to, DataType::Json | DataType::JsonB)
                        && matches!(target, DialectType::Spark | DialectType::Databricks)
                    {
                        // CAST(x AS JSON) -> TO_JSON(x) for Spark
                        Action::Json(json::Action::CastToJsonForSpark)
                    } else if (matches!(
                        &c.to,
                        DataType::Array { .. } | DataType::Map { .. } | DataType::Struct { .. }
                    )) && matches!(
                        target,
                        DialectType::Spark | DialectType::Databricks
                    ) && (matches!(&c.this, Expression::ParseJson(_))
                        || matches!(
                            &c.this,
                            Expression::Function(f)
                                if f.name.eq_ignore_ascii_case("JSON_EXTRACT")
                                    || f.name.eq_ignore_ascii_case("JSON_EXTRACT_SCALAR")
                                    || f.name.eq_ignore_ascii_case("GET_JSON_OBJECT")
                        ))
                    {
                        // CAST(JSON_PARSE(...) AS ARRAY/MAP) or CAST(JSON_EXTRACT/GET_JSON_OBJECT(...) AS ARRAY/MAP)
                        // -> FROM_JSON(..., type_string) for Spark
                        Action::Json(json::Action::CastJsonToFromJson)
                    } else if matches!(target, DialectType::Spark | DialectType::Databricks)
                        && matches!(
                            c.to,
                            DataType::Timestamp {
                                timezone: false,
                                ..
                            }
                        )
                        && matches!(source, DialectType::DuckDB)
                    {
                        Action::Types(types::Action::StrftimeCastTimestamp)
                    } else if matches!(source, DialectType::DuckDB)
                        && matches!(
                            c.to,
                            DataType::Decimal {
                                precision: None,
                                ..
                            }
                        )
                    {
                        Action::Types(types::Action::DecimalDefaultPrecision)
                    } else if matches!(source, DialectType::MySQL | DialectType::SingleStore)
                        && matches!(c.to, DataType::Char { length: None })
                        && !matches!(target, DialectType::MySQL | DialectType::SingleStore)
                    {
                        // MySQL CAST(x AS CHAR) was originally TEXT - convert to target text type
                        Action::Types(types::Action::MysqlCastCharToText)
                    } else if matches!(
                        source,
                        DialectType::Spark | DialectType::Databricks | DialectType::Hive
                    ) && matches!(
                        target,
                        DialectType::Spark | DialectType::Databricks | DialectType::Hive
                    ) && types::has_varchar_char_type(&c.to)
                    {
                        // Spark parses VARCHAR(n)/CHAR(n) as TEXT, so normalize back to STRING
                        Action::Types(types::Action::SparkCastVarcharToString)
                    } else {
                        Action::None
                    }
                }
                Expression::SafeCast(ref c) => {
                    if c.format.is_some()
                        && matches!(source, DialectType::BigQuery)
                        && !matches!(target, DialectType::BigQuery)
                    {
                        Action::Types(types::Action::BigQueryCastFormat)
                    } else {
                        Action::None
                    }
                }
                Expression::TryCast(ref c) => {
                    if matches!(&c.to, DataType::Json)
                        && matches!(source, DialectType::DuckDB)
                        && matches!(
                            target,
                            DialectType::Presto | DialectType::Trino | DialectType::Athena
                        )
                    {
                        // DuckDB's TRY_CAST(x AS JSON) tries to parse x as JSON, returning
                        // NULL on parse failure. Trino/Presto/Athena's TRY_CAST(x AS JSON)
                        // wraps the value as a JSON string (no parse). Emit TRY(JSON_PARSE(x))
                        // to preserve DuckDB's parse-or-null semantics.
                        Action::Json(json::Action::DuckDBTryCastJsonToTryJsonParse)
                    } else {
                        Action::None
                    }
                }
                Expression::JSONArray(ref ja)
                    if matches!(target, DialectType::Snowflake)
                        && ja.null_handling.is_none()
                        && ja.return_type.is_none()
                        && ja.strict.is_none() =>
                {
                    Action::Scalar(scalar::Action::GenericFunctionNormalize)
                }
                Expression::JsonArray(_) if matches!(target, DialectType::Snowflake) => {
                    Action::Scalar(scalar::Action::GenericFunctionNormalize)
                }
                // For DuckDB: DATE_TRUNC should preserve the input type
                Expression::DateTrunc(_) | Expression::TimestampTrunc(_) => {
                    if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB)
                    {
                        Action::Types(types::Action::DateTruncWrapCast)
                    } else {
                        Action::None
                    }
                }
                // For DuckDB: SET a = 1 -> SET VARIABLE a = 1
                Expression::SetStatement(s) => {
                    if matches!(target, DialectType::DuckDB)
                        && !matches!(source, DialectType::TSQL | DialectType::Fabric)
                        && s.items.iter().any(|item| item.kind.is_none())
                    {
                        Action::Statements(statements::Action::SetToVariable)
                    } else {
                        Action::None
                    }
                }
                // Cross-dialect NULL ordering normalization.
                // When nulls_first is not specified, fill in the source dialect's implied
                // default so the target generator can correctly add/strip NULLS FIRST/LAST.
                Expression::Ordered(o) => {
                    // MySQL doesn't support NULLS FIRST/LAST - strip or rewrite
                    if matches!(target, DialectType::MySQL) && o.nulls_first.is_some() {
                        Action::Operators(operators::Action::MysqlNullsOrdering)
                    } else {
                        // Skip targets that don't support NULLS FIRST/LAST syntax unless
                        // the generator can preserve semantics with a CASE sort key.
                        let target_rewrites_nulls =
                            matches!(target, DialectType::TSQL | DialectType::Fabric);
                        let target_supports_nulls = !matches!(
                            target,
                            DialectType::MySQL
                                | DialectType::TSQL
                                | DialectType::Fabric
                                | DialectType::StarRocks
                                | DialectType::Doris
                        );
                        if o.nulls_first.is_none()
                            && source != target
                            && (target_supports_nulls || target_rewrites_nulls)
                        {
                            Action::Operators(operators::Action::NullsOrdering)
                        } else {
                            Action::None
                        }
                    }
                }
                // BigQuery data types: convert INT64, BYTES, NUMERIC etc. to standard types
                Expression::DataType(dt) => {
                    if matches!(source, DialectType::BigQuery)
                        && !matches!(target, DialectType::BigQuery)
                    {
                        match dt {
                            DataType::Custom { ref name }
                                if name.eq_ignore_ascii_case("INT64")
                                    || name.eq_ignore_ascii_case("FLOAT64")
                                    || name.eq_ignore_ascii_case("BOOL")
                                    || name.eq_ignore_ascii_case("BYTES")
                                    || name.eq_ignore_ascii_case("NUMERIC")
                                    || name.eq_ignore_ascii_case("STRING")
                                    || name.eq_ignore_ascii_case("DATETIME") =>
                            {
                                Action::Types(types::Action::BigQueryCastType)
                            }
                            _ => Action::None,
                        }
                    } else if matches!(source, DialectType::TSQL) {
                        // For TSQL source -> any target (including TSQL itself for REAL)
                        match dt {
                            // REAL -> FLOAT even for TSQL->TSQL
                            DataType::Custom { ref name }
                                if name.eq_ignore_ascii_case("REAL") =>
                            {
                                Action::Types(types::Action::TSQLTypeNormalize)
                            }
                            DataType::Float {
                                real_spelling: true,
                                ..
                            } => Action::Types(types::Action::TSQLTypeNormalize),
                            // Other TSQL type normalizations only for non-TSQL targets
                            DataType::Custom { ref name }
                                if !matches!(target, DialectType::TSQL)
                                    && (name.eq_ignore_ascii_case("MONEY")
                                        || name.eq_ignore_ascii_case("SMALLMONEY")
                                        || name.eq_ignore_ascii_case("DATETIME2")
                                        || name.eq_ignore_ascii_case("IMAGE")
                                        || name.eq_ignore_ascii_case("BIT")
                                        || name.eq_ignore_ascii_case("ROWVERSION")
                                        || name.eq_ignore_ascii_case("UNIQUEIDENTIFIER")
                                        || name.eq_ignore_ascii_case("DATETIMEOFFSET")
                                        || (name.len() >= 7 && name[..7].eq_ignore_ascii_case("NUMERIC"))
                                        || (name.len() >= 10 && name[..10].eq_ignore_ascii_case("DATETIME2("))
                                        || (name.len() >= 5 && name[..5].eq_ignore_ascii_case("TIME("))) =>
                            {
                                Action::Types(types::Action::TSQLTypeNormalize)
                            }
                            DataType::Float {
                                precision: Some(_), ..
                            } if !matches!(target, DialectType::TSQL) => {
                                Action::Types(types::Action::TSQLTypeNormalize)
                            }
                            DataType::TinyInt { .. }
                                if !matches!(target, DialectType::TSQL) =>
                            {
                                Action::Types(types::Action::TSQLTypeNormalize)
                            }
                            // INTEGER -> INT for Databricks/Spark targets
                            DataType::Int {
                                integer_spelling: true,
                                ..
                            } if matches!(
                                target,
                                DialectType::Databricks | DialectType::Spark
                            ) =>
                            {
                                Action::Types(types::Action::TSQLTypeNormalize)
                            }
                            _ => Action::None,
                        }
                    } else if (matches!(source, DialectType::Oracle)
                        || matches!(source, DialectType::Generic))
                        && !matches!(target, DialectType::Oracle)
                    {
                        match dt {
                            DataType::Custom { ref name }
                                if (name.len() >= 9 && name[..9].eq_ignore_ascii_case("VARCHAR2("))
                                    || (name.len() >= 10 && name[..10].eq_ignore_ascii_case("NVARCHAR2("))
                                    || name.eq_ignore_ascii_case("VARCHAR2")
                                    || name.eq_ignore_ascii_case("NVARCHAR2") =>
                            {
                                Action::Types(types::Action::OracleVarchar2ToVarchar)
                            }
                            _ => Action::None,
                        }
                    } else if matches!(target, DialectType::Snowflake)
                        && !matches!(source, DialectType::Snowflake)
                    {
                        // When target is Snowflake but source is NOT Snowflake,
                        // protect FLOAT from being converted to DOUBLE by Snowflake's transform.
                        // Snowflake treats FLOAT=DOUBLE internally, but non-Snowflake sources
                        // should keep their FLOAT spelling.
                        match dt {
                            DataType::Float { .. } => Action::Types(types::Action::SnowflakeFloatProtect),
                            _ => Action::None,
                        }
                    } else {
                        Action::None
                    }
                }
                // LOWER patterns from BigQuery TO_HEX conversions:
                // - LOWER(LOWER(HEX(x))) from non-BQ targets: flatten
                // - LOWER(Function("TO_HEX")) for BQ->BQ: strip LOWER
                Expression::Lower(uf) => {
                    if matches!(source, DialectType::BigQuery) {
                        match &uf.this {
                            Expression::Lower(_) => Action::Scalar(scalar::Action::BigQueryToHexLower),
                            Expression::Function(f)
                                if f.name == "TO_HEX"
                                    && matches!(target, DialectType::BigQuery) =>
                            {
                                // BQ->BQ: LOWER(TO_HEX(x)) -> TO_HEX(x)
                                Action::Scalar(scalar::Action::BigQueryToHexLower)
                            }
                            _ => Action::None,
                        }
                    } else {
                        Action::None
                    }
                }
                // UPPER patterns from BigQuery TO_HEX conversions:
                // - UPPER(LOWER(HEX(x))) from non-BQ targets: extract inner
                // - UPPER(Function("TO_HEX")) for BQ->BQ: keep as UPPER(TO_HEX(x))
                Expression::Upper(uf) => {
                    if matches!(source, DialectType::BigQuery) {
                        match &uf.this {
                            Expression::Lower(_) => Action::Scalar(scalar::Action::BigQueryToHexUpper),
                            _ => Action::None,
                        }
                    } else {
                        Action::None
                    }
                }
                // BigQuery LAST_DAY(date, unit) -> strip unit for non-BigQuery targets
                // Snowflake supports LAST_DAY with unit, so keep it there
                Expression::LastDay(ld) => {
                    if matches!(source, DialectType::BigQuery)
                        && !matches!(target, DialectType::BigQuery | DialectType::Snowflake)
                        && ld.unit.is_some()
                    {
                        Action::Scalar(scalar::Action::BigQueryLastDayStripUnit)
                    } else {
                        Action::None
                    }
                }
                // BigQuery SafeDivide expressions (already parsed as SafeDivide)
                Expression::SafeDivide(_) => {
                    if matches!(source, DialectType::BigQuery)
                        && !matches!(target, DialectType::BigQuery)
                    {
                        Action::Operators(operators::Action::BigQuerySafeDivide)
                    } else {
                        Action::None
                    }
                }
                // BigQuery ANY_VALUE(x HAVING MAX/MIN y) -> ARG_MAX_NULL/ARG_MIN_NULL for DuckDB
                // ANY_VALUE(x) -> ANY_VALUE(x) IGNORE NULLS for Spark
                Expression::AnyValue(ref agg) => {
                    if matches!(source, DialectType::BigQuery)
                        && matches!(target, DialectType::DuckDB)
                        && agg.having_max.is_some()
                    {
                        Action::Aggregates(aggregates::Action::BigQueryAnyValueHaving)
                    } else if matches!(target, DialectType::Spark | DialectType::Databricks)
                        && !matches!(source, DialectType::Spark | DialectType::Databricks)
                        && agg.ignore_nulls.is_none()
                    {
                        Action::Aggregates(aggregates::Action::AnyValueIgnoreNulls)
                    } else {
                        Action::None
                    }
                }
                Expression::Any(ref q) => {
                    if matches!(source, DialectType::PostgreSQL)
                        && matches!(
                            target,
                            DialectType::Spark | DialectType::Databricks | DialectType::Hive
                        )
                        && q.op.is_some()
                        && !matches!(
                            q.subquery,
                            Expression::Select(_) | Expression::Subquery(_)
                        )
                    {
                        Action::Operators(operators::Action::AnyToExists)
                    } else {
                        Action::None
                    }
                }
                // BigQuery APPROX_QUANTILES(x, n) -> APPROX_QUANTILE(x, [quantiles]) for DuckDB
                // Snowflake RLIKE does full-string match; DuckDB REGEXP_FULL_MATCH also does full-string match
                Expression::RegexpLike(_)
                    if matches!(source, DialectType::Snowflake)
                        && matches!(target, DialectType::DuckDB) =>
                {
                    Action::Operators(operators::Action::RlikeSnowflakeToDuckDB)
                }
                // PostgreSQL regex predicates have no native T-SQL/Fabric equivalent.
                // Default mode emits a best-effort PATINDEX predicate; strict mode rejects
                // before this rewrite runs.
                Expression::RegexpLike(_) | Expression::RegexpILike(_)
                    if matches!(source, DialectType::PostgreSQL | DialectType::CockroachDB)
                        && matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                {
                    Action::Operators(operators::Action::RegexpLikeToTsqlPatindex)
                }
                Expression::SimilarTo(s)
                    if matches!(source, DialectType::PostgreSQL | DialectType::CockroachDB)
                        && matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && operators::similar_to_can_lower_to_tsql_like(s) =>
                {
                    Action::Operators(operators::Action::SimilarToToTsqlLike)
                }
                // RegexpLike from non-DuckDB/non-Snowflake sources -> REGEXP_MATCHES for DuckDB target
                Expression::RegexpLike(_)
                    if !matches!(source, DialectType::DuckDB)
                        && matches!(target, DialectType::DuckDB) =>
                {
                    Action::Operators(operators::Action::RegexpLikeToDuckDB)
                }
                // RegexpLike -> Exasol: anchor pattern with .*...*
                Expression::RegexpLike(_)
                    if matches!(target, DialectType::Exasol) =>
                {
                    Action::Operators(operators::Action::RegexpLikeExasolAnchor)
                }
                // Safe-division source -> non-safe target: NULLIF wrapping and/or CAST
                // Safe-division dialects: MySQL, DuckDB, SingleStore, TiDB, ClickHouse, Doris
                Expression::Div(ref op)
                    if matches!(
                        source,
                        DialectType::MySQL
                            | DialectType::DuckDB
                            | DialectType::SingleStore
                            | DialectType::TiDB
                            | DialectType::ClickHouse
                            | DialectType::Doris
                    ) && matches!(
                        target,
                        DialectType::PostgreSQL
                            | DialectType::Redshift
                            | DialectType::Drill
                            | DialectType::Trino
                            | DialectType::Presto
                            | DialectType::Athena
                            | DialectType::TSQL
                            | DialectType::Teradata
                            | DialectType::SQLite
                            | DialectType::BigQuery
                            | DialectType::Snowflake
                            | DialectType::Databricks
                            | DialectType::Oracle
                            | DialectType::Materialize
                            | DialectType::RisingWave
                    ) =>
                {
                    // Only wrap if RHS is not already NULLIF
                    if !matches!(&op.right, Expression::Function(f) if f.name.eq_ignore_ascii_case("NULLIF"))
                    {
                        Action::Operators(operators::Action::MySQLSafeDivide)
                    } else {
                        Action::None
                    }
                }
                // ALTER TABLE ... RENAME TO <schema>.<table> -> strip schema for most targets
                // For TSQL/Fabric, convert to sp_rename instead
                Expression::AlterTable(ref at) if !at.actions.is_empty() => {
                    if let Some(crate::expressions::AlterTableAction::RenameTable(
                        ref new_tbl,
                    )) = at.actions.first()
                    {
                        if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                            // TSQL: ALTER TABLE RENAME -> EXEC sp_rename
                            Action::Statements(statements::Action::AlterTableToSpRename)
                        } else if new_tbl.schema.is_some()
                            && matches!(
                                target,
                                DialectType::BigQuery
                                    | DialectType::Doris
                                    | DialectType::StarRocks
                                    | DialectType::DuckDB
                                    | DialectType::PostgreSQL
                                    | DialectType::Redshift
                            )
                        {
                            Action::Statements(statements::Action::AlterTableRenameStripSchema)
                        } else {
                            Action::None
                        }
                    } else {
                        Action::None
                    }
                }
                // EPOCH(x) expression -> target-specific epoch conversion
                Expression::Epoch(_) if !matches!(target, DialectType::DuckDB) => {
                    Action::Temporal(temporal::Action::EpochConvert)
                }
                // EPOCH_MS(x) expression -> target-specific epoch ms conversion
                Expression::EpochMs(_) if !matches!(target, DialectType::DuckDB) => {
                    Action::Temporal(temporal::Action::EpochMsConvert)
                }
                // STRING_AGG -> GROUP_CONCAT for MySQL/SQLite
                Expression::StringAgg(_) => {
                    if matches!(
                        target,
                        DialectType::MySQL
                            | DialectType::SingleStore
                            | DialectType::Doris
                            | DialectType::StarRocks
                            | DialectType::SQLite
                    ) {
                        Action::Aggregates(aggregates::Action::StringAggConvert)
                    } else if matches!(target, DialectType::Spark | DialectType::Databricks) {
                        Action::Aggregates(aggregates::Action::StringAggConvert)
                    } else {
                        Action::None
                    }
                }
                Expression::CombinedParameterizedAgg(_) => Action::Scalar(scalar::Action::GenericFunctionNormalize),
                // GROUP_CONCAT -> STRING_AGG for PostgreSQL/Presto/etc.
                // Also handles GROUP_CONCAT normalization for MySQL/SQLite targets
                Expression::GroupConcat(_) => Action::Aggregates(aggregates::Action::GroupConcatConvert),
                // CARDINALITY/ARRAY_LENGTH/ARRAY_SIZE -> target-specific array length
                // DuckDB CARDINALITY -> keep as CARDINALITY for DuckDB target (used for maps)
                Expression::Cardinality(_)
                    if matches!(source, DialectType::DuckDB)
                        && matches!(target, DialectType::DuckDB) =>
                {
                    Action::None
                }
                Expression::Cardinality(_) | Expression::ArrayLength(_) => {
                    Action::Collections(collections::Action::ArrayLengthConvert)
                }
                Expression::ArraySize(_) => {
                    if matches!(target, DialectType::Drill) {
                        Action::Collections(collections::Action::ArraySizeDrill)
                    } else {
                        Action::Collections(collections::Action::ArrayLengthConvert)
                    }
                }
                // ARRAY_REMOVE(arr, target) -> LIST_FILTER/arrayFilter/ARRAY subquery
                Expression::ArrayRemove(_) => match target {
                    DialectType::DuckDB | DialectType::ClickHouse | DialectType::BigQuery => {
                        Action::Collections(collections::Action::ArrayRemoveConvert)
                    }
                    _ => Action::None,
                },
                // ARRAY_REVERSE(x) -> arrayReverse for ClickHouse
                Expression::ArrayReverse(_) => match target {
                    DialectType::ClickHouse => Action::Collections(collections::Action::ArrayReverseConvert),
                    _ => Action::None,
                },
                // JSON_KEYS(x) -> JSON_OBJECT_KEYS/OBJECT_KEYS for Spark/Databricks/Snowflake
                Expression::JsonKeys(_) => match target {
                    DialectType::Spark | DialectType::Databricks | DialectType::Snowflake => {
                        Action::Json(json::Action::JsonKeysConvert)
                    }
                    _ => Action::None,
                },
                // PARSE_JSON(x) -> strip for SQLite/Doris/MySQL/StarRocks
                Expression::ParseJson(_) => match target {
                    DialectType::SQLite
                    | DialectType::Doris
                    | DialectType::MySQL
                    | DialectType::StarRocks => Action::Json(json::Action::ParseJsonStrip),
                    _ => Action::None,
                },
                // WeekOfYear -> WEEKISO for Snowflake (cross-dialect only)
                Expression::WeekOfYear(_)
                    if matches!(target, DialectType::Snowflake)
                        && !matches!(source, DialectType::Snowflake) =>
                {
                    Action::Temporal(temporal::Action::WeekOfYearToWeekIso)
                }
                // NVL: clear original_name so generator uses dialect-specific function names
                Expression::Nvl(f) if f.original_name.is_some() => Action::Scalar(scalar::Action::NvlClearOriginal),
                // XOR: expand for dialects that don't support the XOR keyword
                Expression::Xor(_) => {
                    let target_supports_xor = matches!(
                        target,
                        DialectType::MySQL
                            | DialectType::SingleStore
                            | DialectType::Doris
                            | DialectType::StarRocks
                    );
                    if !target_supports_xor {
                        Action::Operators(operators::Action::XorExpand)
                    } else {
                        Action::None
                    }
                }
                // TSQL #table -> temp table normalization (CREATE TABLE)
                Expression::CreateTable(ct)
                    if matches!(source, DialectType::TSQL | DialectType::Fabric)
                        && !matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && ct.name.name.name.starts_with('#') =>
                {
                    Action::Statements(statements::Action::TempTableHash)
                }
                // TSQL #table -> strip # from table references in SELECT/etc.
                Expression::Table(tr)
                    if matches!(source, DialectType::TSQL | DialectType::Fabric)
                        && !matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && tr.name.name.starts_with('#') =>
                {
                    Action::Statements(statements::Action::TempTableHash)
                }
                // TSQL #table -> strip # from DROP TABLE names
                Expression::DropTable(ref dt)
                    if matches!(source, DialectType::TSQL | DialectType::Fabric)
                        && !matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && dt.names.iter().any(|n| n.name.name.starts_with('#')) =>
                {
                    Action::Statements(statements::Action::TempTableHash)
                }
                // JSON_EXTRACT / PostgreSQL `->` -> T-SQL JSON functions
                Expression::JsonExtract(_)
                    if matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                {
                    Action::Json(json::Action::JsonExtractToTsql)
                }
                // JSON_EXTRACT_SCALAR / PostgreSQL `->>`/`#>>` -> T-SQL JSON functions
                Expression::JsonExtractScalar(_)
                    if matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                {
                    Action::Json(json::Action::JsonExtractToTsql)
                }
                // PostgreSQL `#>` -> T-SQL/Fabric JSON_QUERY
                Expression::JsonExtractPath(_)
                    if matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                {
                    Action::Json(json::Action::JsonExtractToTsql)
                }
                // JSON_EXTRACT -> JSONExtractString for ClickHouse
                Expression::JsonExtract(_) if matches!(target, DialectType::ClickHouse) => {
                    Action::Json(json::Action::JsonExtractToClickHouse)
                }
                // JSON_EXTRACT_SCALAR -> JSONExtractString for ClickHouse
                Expression::JsonExtractScalar(_)
                    if matches!(target, DialectType::ClickHouse) =>
                {
                    Action::Json(json::Action::JsonExtractToClickHouse)
                }
                // JSON_EXTRACT -> arrow syntax for SQLite/DuckDB
                Expression::JsonExtract(ref f)
                    if !f.arrow_syntax
                        && matches!(target, DialectType::SQLite | DialectType::DuckDB) =>
                {
                    Action::Json(json::Action::JsonExtractToArrow)
                }
                // JSON_EXTRACT with JSONPath -> JSON_EXTRACT_PATH for PostgreSQL (non-PG sources only)
                Expression::JsonExtract(ref f)
                    if matches!(target, DialectType::PostgreSQL | DialectType::Redshift)
                        && !matches!(
                            source,
                            DialectType::PostgreSQL
                                | DialectType::Redshift
                                | DialectType::Materialize
                        )
                        && matches!(&f.path, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(s) if s.starts_with('$'))) =>
                {
                    Action::Json(json::Action::JsonExtractToGetJsonObject)
                }
                // JSON_EXTRACT -> GET_JSON_OBJECT for Hive/Spark
                Expression::JsonExtract(_)
                    if matches!(
                        target,
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks
                    ) =>
                {
                    Action::Json(json::Action::JsonExtractToGetJsonObject)
                }
                // JSON_EXTRACT_SCALAR -> target-specific for PostgreSQL, Snowflake, SQLite
                // Skip if already in arrow/hash_arrow syntax (same-dialect identity case)
                Expression::JsonExtractScalar(ref f)
                    if !f.arrow_syntax
                        && !f.hash_arrow_syntax
                        && matches!(
                            target,
                            DialectType::PostgreSQL
                                | DialectType::Redshift
                                | DialectType::Snowflake
                                | DialectType::SQLite
                                | DialectType::DuckDB
                        ) =>
                {
                    Action::Json(json::Action::JsonExtractScalarConvert)
                }
                // JSON_EXTRACT_SCALAR -> GET_JSON_OBJECT for Hive/Spark
                Expression::JsonExtractScalar(_)
                    if matches!(
                        target,
                        DialectType::Hive | DialectType::Spark | DialectType::Databricks
                    ) =>
                {
                    Action::Json(json::Action::JsonExtractScalarToGetJsonObject)
                }
                // JSON_EXTRACT path normalization for BigQuery, MySQL (bracket/wildcard handling)
                Expression::JsonExtract(ref f)
                    if !f.arrow_syntax
                        && matches!(target, DialectType::BigQuery | DialectType::MySQL) =>
                {
                    Action::Json(json::Action::JsonPathNormalize)
                }
                // JsonQuery (parsed JSON_QUERY) -> target-specific
                Expression::JsonQuery(_) => Action::Json(json::Action::JsonQueryValueConvert),
                // JsonValue (parsed JSON_VALUE) -> target-specific
                Expression::JsonValue(_) => Action::Json(json::Action::JsonQueryValueConvert),
                // AT TIME ZONE -> AT_TIMEZONE for Presto, FROM_UTC_TIMESTAMP for Spark,
                // TIMESTAMP(DATETIME(...)) for BigQuery, CONVERT_TIMEZONE for Snowflake
                Expression::AtTimeZone(_)
                    if matches!(
                        target,
                        DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Athena
                            | DialectType::Spark
                            | DialectType::Databricks
                            | DialectType::BigQuery
                            | DialectType::Snowflake
                    ) =>
                {
                    Action::Temporal(temporal::Action::AtTimeZoneConvert)
                }
                // DAY_OF_WEEK -> dialect-specific
                Expression::DayOfWeek(_)
                    if matches!(
                        target,
                        DialectType::DuckDB | DialectType::Spark | DialectType::Databricks
                    ) =>
                {
                    Action::Temporal(temporal::Action::DayOfWeekConvert)
                }
                // CURRENT_USER -> CURRENT_USER() for Snowflake
                Expression::CurrentUser(_) if matches!(target, DialectType::Snowflake) => {
                    Action::Scalar(scalar::Action::CurrentUserParens)
                }
                // ELEMENT_AT(arr, idx) -> arr[idx] for PostgreSQL, arr[SAFE_ORDINAL(idx)] for BigQuery
                Expression::ElementAt(_)
                    if matches!(target, DialectType::PostgreSQL | DialectType::BigQuery) =>
                {
                    Action::Collections(collections::Action::ElementAtConvert)
                }
                // ARRAY[...] (ArrayFunc bracket_notation=false) -> convert for target dialect
                Expression::ArrayFunc(ref arr)
                    if !arr.bracket_notation
                        && matches!(
                            target,
                            DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive
                                | DialectType::BigQuery
                                | DialectType::DuckDB
                                | DialectType::Snowflake
                                | DialectType::Presto
                                | DialectType::Trino
                                | DialectType::Athena
                                | DialectType::ClickHouse
                                | DialectType::StarRocks
                        ) =>
                {
                    Action::Collections(collections::Action::ArraySyntaxConvert)
                }
                // VARIANCE expression -> varSamp for ClickHouse
                Expression::Variance(_) if matches!(target, DialectType::ClickHouse) => {
                    Action::Aggregates(aggregates::Action::VarianceToClickHouse)
                }
                // STDDEV expression -> stddevSamp for ClickHouse
                Expression::Stddev(_) if matches!(target, DialectType::ClickHouse) => {
                    Action::Aggregates(aggregates::Action::StddevToClickHouse)
                }
                // ApproxQuantile -> APPROX_PERCENTILE for Snowflake
                Expression::ApproxQuantile(_) if matches!(target, DialectType::Snowflake) => {
                    Action::Aggregates(aggregates::Action::ApproxQuantileConvert)
                }
                // MonthsBetween -> target-specific
                Expression::MonthsBetween(_)
                    if !matches!(
                        target,
                        DialectType::Spark | DialectType::Databricks | DialectType::Hive
                    ) =>
                {
                    Action::Temporal(temporal::Action::MonthsBetweenConvert)
                }
                // AddMonths -> target-specific DATEADD/DATE_ADD
                Expression::AddMonths(_) => Action::Temporal(temporal::Action::AddMonthsConvert),
                // MapFromArrays -> target-specific (MAP, OBJECT_CONSTRUCT, MAP_FROM_ARRAYS)
                Expression::MapFromArrays(_)
                    if !matches!(target, DialectType::Spark | DialectType::Databricks) =>
                {
                    Action::Collections(collections::Action::MapFromArraysConvert)
                }
                // CURRENT_USER -> CURRENT_USER() for Spark
                Expression::CurrentUser(_)
                    if matches!(target, DialectType::Spark | DialectType::Databricks) =>
                {
                    Action::Scalar(scalar::Action::CurrentUserSparkParens)
                }
                // MONTH/YEAR/DAY('string') from Spark -> cast string to DATE for DuckDB/Presto
                Expression::Month(ref f) | Expression::Year(ref f) | Expression::Day(ref f)
                    if matches!(
                        source,
                        DialectType::Spark | DialectType::Databricks | DialectType::Hive
                    ) && matches!(&f.this, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))
                        && matches!(
                            target,
                            DialectType::DuckDB
                                | DialectType::Presto
                                | DialectType::Trino
                                | DialectType::Athena
                                | DialectType::PostgreSQL
                                | DialectType::Redshift
                        ) =>
                {
                    Action::Types(types::Action::SparkDateFuncCast)
                }
                // $parameter -> @parameter for BigQuery
                Expression::Parameter(ref p)
                    if matches!(target, DialectType::BigQuery)
                        && matches!(source, DialectType::DuckDB)
                        && (p.style == crate::expressions::ParameterStyle::Dollar
                            || p.style == crate::expressions::ParameterStyle::DoubleDollar) =>
                {
                    Action::Operators(operators::Action::DollarParamConvert)
                }
                // EscapeString literal: normalize literal newlines to \n
                Expression::Literal(lit) if matches!(lit.as_ref(), Literal::EscapeString(ref s) if s.contains('\n') || s.contains('\r') || s.contains('\t'))
                    =>
                {
                    Action::Scalar(scalar::Action::EscapeStringNormalize)
                }
                // straight_join: keep lowercase for DuckDB, quote for MySQL
                Expression::Column(ref col)
                    if col.name.name == "STRAIGHT_JOIN"
                        && col.table.is_none()
                        && matches!(source, DialectType::DuckDB)
                        && matches!(target, DialectType::DuckDB | DialectType::MySQL) =>
                {
                    Action::Statements(statements::Action::StraightJoinCase)
                }
                // DATE and TIMESTAMP literal type conversions are now handled in the generator directly
                // Snowflake INTERVAL format: INTERVAL '2' HOUR -> INTERVAL '2 HOUR'
                Expression::Interval(ref iv)
                    if matches!(
                        target,
                        DialectType::Snowflake
                            | DialectType::PostgreSQL
                            | DialectType::Redshift
                    ) && iv.unit.is_some()
                        && iv.this.as_ref().map_or(false, |t| matches!(t, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::String(_)))) =>
                {
                    Action::Scalar(scalar::Action::SnowflakeIntervalFormat)
                }
                // TABLESAMPLE -> TABLESAMPLE RESERVOIR for DuckDB target
                Expression::TableSample(ref ts) if matches!(target, DialectType::DuckDB) => {
                    if let Some(ref sample) = ts.sample {
                        if !sample.explicit_method {
                            Action::Statements(statements::Action::TablesampleReservoir)
                        } else {
                            Action::None
                        }
                    } else {
                        Action::None
                    }
                }
                // TABLESAMPLE from non-Snowflake source to Snowflake: strip method and PERCENT
                // Handles both Expression::TableSample wrapper and Expression::Table with table_sample
                Expression::TableSample(ref ts)
                    if matches!(target, DialectType::Snowflake)
                        && !matches!(source, DialectType::Snowflake)
                        && ts.sample.is_some() =>
                {
                    if let Some(ref sample) = ts.sample {
                        if !sample.explicit_method {
                            Action::Statements(statements::Action::TablesampleSnowflakeStrip)
                        } else {
                            Action::None
                        }
                    } else {
                        Action::None
                    }
                }
                Expression::Table(ref t)
                    if matches!(target, DialectType::Snowflake)
                        && !matches!(source, DialectType::Snowflake)
                        && t.table_sample.is_some() =>
                {
                    if let Some(ref sample) = t.table_sample {
                        if !sample.explicit_method {
                            Action::Statements(statements::Action::TablesampleSnowflakeStrip)
                        } else {
                            Action::None
                        }
                    } else {
                        Action::None
                    }
                }
                // ALTER TABLE RENAME -> EXEC sp_rename for TSQL
                Expression::AlterTable(ref at)
                    if matches!(target, DialectType::TSQL | DialectType::Fabric)
                        && !at.actions.is_empty()
                        && matches!(
                            at.actions.first(),
                            Some(crate::expressions::AlterTableAction::RenameTable(_))
                        ) =>
                {
                    Action::Statements(statements::Action::AlterTableToSpRename)
                }
                // Subscript index: 1-based to 0-based for BigQuery/Hive/Spark
                Expression::Subscript(ref sub)
                    if matches!(
                        target,
                        DialectType::BigQuery
                            | DialectType::Hive
                            | DialectType::Spark
                            | DialectType::Databricks
                    ) && matches!(
                        source,
                        DialectType::DuckDB
                            | DialectType::PostgreSQL
                            | DialectType::Presto
                            | DialectType::Trino
                            | DialectType::Redshift
                            | DialectType::ClickHouse
                    ) && matches!(&sub.index, Expression::Literal(lit) if matches!(lit.as_ref(), Literal::Number(ref n) if n.parse::<i64>().unwrap_or(0) > 0)) =>
                {
                    Action::Collections(collections::Action::ArrayIndexConvert)
                }
                // ANY_VALUE IGNORE NULLS detection moved to the AnyValue arm above
                // MysqlNullsOrdering for Ordered is now handled in the Ordered arm above
                // RESPECT NULLS handling for SQLite (strip it, add NULLS LAST to ORDER BY)
                // and for MySQL (rewrite ORDER BY with CASE WHEN for null ordering)
                Expression::WindowFunction(ref wf) => {
                    // BigQuery doesn't support NULLS FIRST/LAST in window function ORDER BY
                    // EXCEPT for ROW_NUMBER which keeps NULLS LAST
                    let is_row_number = matches!(wf.this, Expression::RowNumber(_));
                    if matches!(target, DialectType::BigQuery)
                        && !is_row_number
                        && !wf.over.order_by.is_empty()
                        && wf.over.order_by.iter().any(|o| o.nulls_first.is_some())
                    {
                        Action::Operators(operators::Action::BigQueryNullsOrdering)
                    // DuckDB -> MySQL: Add CASE WHEN for NULLS LAST simulation in window ORDER BY
                    // But NOT when frame is RANGE/GROUPS, since adding CASE WHEN would break value-based frames
                    } else {
                        let source_nulls_last = matches!(source, DialectType::DuckDB);
                        let has_range_frame = wf.over.frame.as_ref().map_or(false, |f| {
                            matches!(
                                f.kind,
                                crate::expressions::WindowFrameKind::Range
                                    | crate::expressions::WindowFrameKind::Groups
                            )
                        });
                        if source_nulls_last
                            && matches!(target, DialectType::MySQL)
                            && !wf.over.order_by.is_empty()
                            && wf.over.order_by.iter().any(|o| !o.desc)
                            && !has_range_frame
                        {
                            Action::Scalar(scalar::Action::MysqlNullsLastRewrite)
                        } else {
                            // Check for Snowflake window frame handling for FIRST_VALUE/LAST_VALUE/NTH_VALUE
                            let is_ranking_window_func = matches!(
                                &wf.this,
                                Expression::FirstValue(_)
                                    | Expression::LastValue(_)
                                    | Expression::NthValue(_)
                            );
                            let has_full_unbounded_frame = wf.over.frame.as_ref().map_or(false, |f| {
                                matches!(f.kind, crate::expressions::WindowFrameKind::Rows)
                                    && matches!(f.start, crate::expressions::WindowFrameBound::UnboundedPreceding)
                                    && matches!(f.end, Some(crate::expressions::WindowFrameBound::UnboundedFollowing))
                                    && f.exclude.is_none()
                            });
                            if is_ranking_window_func && matches!(source, DialectType::Snowflake) {
                                if has_full_unbounded_frame && matches!(target, DialectType::Snowflake) {
                                    // Strip the default frame for Snowflake target
                                    Action::Operators(operators::Action::SnowflakeWindowFrameStrip)
                                } else if !has_full_unbounded_frame && wf.over.frame.is_none() && !matches!(target, DialectType::Snowflake) {
                                    // Add default frame for non-Snowflake target
                                    Action::Operators(operators::Action::SnowflakeWindowFrameAdd)
                                } else {
                                    match &wf.this {
                                        Expression::FirstValue(ref vf)
                                        | Expression::LastValue(ref vf)
                                            if vf.ignore_nulls == Some(false) =>
                                        {
                                            match target {
                                                DialectType::SQLite => Action::Operators(operators::Action::RespectNullsConvert),
                                                _ => Action::None,
                                            }
                                        }
                                        _ => Action::None,
                                    }
                                }
                            } else {
                                match &wf.this {
                                    Expression::FirstValue(ref vf)
                                    | Expression::LastValue(ref vf)
                                        if vf.ignore_nulls == Some(false) =>
                                    {
                                        // RESPECT NULLS
                                        match target {
                                            DialectType::SQLite | DialectType::PostgreSQL => {
                                                Action::Operators(operators::Action::RespectNullsConvert)
                                            }
                                            _ => Action::None,
                                        }
                                    }
                                    _ => Action::None,
                                }
                            }
                        }
                    }
                }
                // CREATE TABLE a LIKE b -> dialect-specific transformations
                Expression::CreateTable(ref ct)
                    if ct.columns.is_empty()
                        && ct.constraints.iter().any(|c| {
                            matches!(c, crate::expressions::TableConstraint::Like { .. })
                        })
                        && matches!(
                            target,
                            DialectType::DuckDB | DialectType::SQLite | DialectType::Drill
                        ) =>
                {
                    Action::Statements(statements::Action::CreateTableLikeToCtas)
                }
                Expression::CreateTable(ref ct)
                    if ct.columns.is_empty()
                        && ct.constraints.iter().any(|c| {
                            matches!(c, crate::expressions::TableConstraint::Like { .. })
                        })
                        && matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                {
                    Action::Statements(statements::Action::CreateTableLikeToSelectInto)
                }
                Expression::CreateTable(ref ct)
                    if ct.columns.is_empty()
                        && ct.constraints.iter().any(|c| {
                            matches!(c, crate::expressions::TableConstraint::Like { .. })
                        })
                        && matches!(target, DialectType::ClickHouse) =>
                {
                    Action::Statements(statements::Action::CreateTableLikeToAs)
                }
                // CREATE TABLE: strip COMMENT column constraint, USING, PARTITIONED BY for DuckDB
                Expression::CreateTable(ref ct)
                    if matches!(target, DialectType::DuckDB)
                        && matches!(
                            source,
                            DialectType::DuckDB
                                | DialectType::Spark
                                | DialectType::Databricks
                                | DialectType::Hive
                        ) =>
                {
                    let has_comment = ct.columns.iter().any(|c| {
                        c.comment.is_some()
                            || c.constraints.iter().any(|con| {
                                matches!(con, crate::expressions::ColumnConstraint::Comment(_))
                            })
                    });
                    let has_props = !ct.properties.is_empty();
                    if has_comment || has_props {
                        Action::Statements(statements::Action::CreateTableStripComment)
                    } else {
                        Action::None
                    }
                }
                // Array conversion: Expression::Array -> Expression::ArrayFunc for PostgreSQL
                Expression::Array(_)
                    if matches!(target, DialectType::PostgreSQL | DialectType::Redshift) =>
                {
                    Action::Collections(collections::Action::ArrayConcatBracketConvert)
                }
                // ArrayFunc (bracket notation) -> Function("ARRAY") for Redshift (from BigQuery source)
                Expression::ArrayFunc(ref arr)
                    if arr.bracket_notation
                        && matches!(source, DialectType::BigQuery)
                        && matches!(target, DialectType::Redshift) =>
                {
                    Action::Collections(collections::Action::ArrayConcatBracketConvert)
                }
                // BIT_OR/BIT_AND/BIT_XOR: float/decimal arg cast for DuckDB, or rename for Snowflake
                Expression::BitwiseOrAgg(ref f)
                | Expression::BitwiseAndAgg(ref f)
                | Expression::BitwiseXorAgg(ref f) => {
                    if matches!(target, DialectType::DuckDB) {
                        // Check if the arg is CAST(val AS FLOAT/DOUBLE/DECIMAL/REAL)
                        if let Expression::Cast(ref c) = f.this {
                            match &c.to {
                                DataType::Float { .. }
                                | DataType::Double { .. }
                                | DataType::Decimal { .. } => Action::Types(types::Action::BitAggFloatCast),
                                DataType::Custom { ref name }
                                    if name.eq_ignore_ascii_case("REAL") =>
                                {
                                    Action::Types(types::Action::BitAggFloatCast)
                                }
                                _ => Action::None,
                            }
                        } else {
                            Action::None
                        }
                    } else if matches!(target, DialectType::Snowflake) {
                        Action::Aggregates(aggregates::Action::BitAggSnowflakeRename)
                    } else {
                        Action::None
                    }
                }
                // FILTER -> IFF for Snowflake (aggregate functions with FILTER clause)
                Expression::Filter(ref _f) if matches!(target, DialectType::Snowflake) => {
                    Action::Scalar(scalar::Action::FilterToIff)
                }
                // AggFunc.filter -> IFF wrapping for Snowflake (e.g., AVG(x) FILTER(WHERE cond))
                Expression::Avg(ref f)
                | Expression::Sum(ref f)
                | Expression::Min(ref f)
                | Expression::Max(ref f)
                | Expression::CountIf(ref f)
                | Expression::Stddev(ref f)
                | Expression::StddevPop(ref f)
                | Expression::StddevSamp(ref f)
                | Expression::Variance(ref f)
                | Expression::VarPop(ref f)
                | Expression::VarSamp(ref f)
                | Expression::Median(ref f)
                | Expression::Mode(ref f)
                | Expression::First(ref f)
                | Expression::Last(ref f)
                | Expression::ApproxDistinct(ref f)
                    if f.filter.is_some() && matches!(target, DialectType::Snowflake) =>
                {
                    Action::Aggregates(aggregates::Action::AggFilterToIff)
                }
                Expression::Count(ref c)
                    if c.filter.is_some() && matches!(target, DialectType::Snowflake) =>
                {
                    Action::Aggregates(aggregates::Action::AggFilterToIff)
                }
                // COUNT(DISTINCT a, b) -> COUNT(DISTINCT CASE WHEN ... END) for dialects that don't support multi-arg DISTINCT
                Expression::Count(ref c)
                    if c.distinct
                        && matches!(&c.this, Some(Expression::Tuple(_)))
                        && matches!(
                            target,
                            DialectType::Presto
                                | DialectType::Trino
                                | DialectType::DuckDB
                                | DialectType::PostgreSQL
                        ) =>
                {
                    Action::Aggregates(aggregates::Action::CountDistinctMultiArg)
                }
                // JSON arrow -> GET_PATH/PARSE_JSON for Snowflake
                Expression::JsonExtract(_) if matches!(target, DialectType::Snowflake) => {
                    Action::Json(json::Action::JsonToGetPath)
                }
                // DuckDB struct/dict -> BigQuery STRUCT / Presto ROW
                Expression::Struct(_)
                    if matches!(
                        target,
                        DialectType::BigQuery | DialectType::Presto | DialectType::Trino
                    ) && matches!(source, DialectType::DuckDB) =>
                {
                    Action::Collections(collections::Action::StructToRow)
                }
                // DuckDB curly-brace dict {'key': value} -> BigQuery STRUCT / Presto ROW
                Expression::MapFunc(ref m)
                    if m.curly_brace_syntax
                        && matches!(
                            target,
                            DialectType::BigQuery | DialectType::Presto | DialectType::Trino
                        )
                        && matches!(source, DialectType::DuckDB) =>
                {
                    Action::Collections(collections::Action::StructToRow)
                }
                // APPROX_COUNT_DISTINCT -> APPROX_DISTINCT for Presto/Trino
                Expression::ApproxCountDistinct(_)
                    if matches!(
                        target,
                        DialectType::Presto | DialectType::Trino | DialectType::Athena
                    ) =>
                {
                    Action::Aggregates(aggregates::Action::ApproxCountDistinctToApproxDistinct)
                }
                // ARRAY_CONTAINS(arr, val) -> CONTAINS(arr, val) for Presto, ARRAY_CONTAINS(CAST(val AS VARIANT), arr) for Snowflake
                Expression::ArrayContains(_)
                    if matches!(
                        target,
                        DialectType::Presto | DialectType::Trino | DialectType::Snowflake
                    ) && !(matches!(source, DialectType::Snowflake) && matches!(target, DialectType::Snowflake)) =>
                {
                    Action::Collections(collections::Action::ArrayContainsConvert)
                }
                // ARRAY_CONTAINS -> DuckDB NULL-aware CASE (from Snowflake source with check_null semantics)
                Expression::ArrayContains(_)
                    if matches!(target, DialectType::DuckDB)
                        && matches!(source, DialectType::Snowflake) =>
                {
                    Action::Collections(collections::Action::ArrayContainsDuckDBConvert)
                }
                // ARRAY_EXCEPT -> target-specific conversion
                Expression::ArrayExcept(_)
                    if matches!(
                        target,
                        DialectType::DuckDB | DialectType::Snowflake | DialectType::Presto | DialectType::Trino | DialectType::Athena
                    ) =>
                {
                    Action::Collections(collections::Action::ArrayExceptConvert)
                }
                // ARRAY_POSITION -> swap args for Snowflake target (only when source is not Snowflake)
                Expression::ArrayPosition(_)
                    if matches!(target, DialectType::Snowflake)
                        && !matches!(source, DialectType::Snowflake) =>
                {
                    Action::Collections(collections::Action::ArrayPositionSnowflakeSwap)
                }
                // ARRAY_POSITION(val, arr) -> ARRAY_POSITION(arr, val) - 1 for DuckDB from Snowflake source
                Expression::ArrayPosition(_)
                    if matches!(target, DialectType::DuckDB)
                        && matches!(source, DialectType::Snowflake) =>
                {
                    Action::Collections(collections::Action::SnowflakeArrayPositionToDuckDB)
                }
                // ARRAY_DISTINCT -> arrayDistinct for ClickHouse
                Expression::ArrayDistinct(_)
                    if matches!(target, DialectType::ClickHouse) =>
                {
                    Action::Collections(collections::Action::ArrayDistinctClickHouse)
                }
                // ARRAY_DISTINCT -> DuckDB LIST_DISTINCT with NULL-aware CASE
                Expression::ArrayDistinct(_)
                    if matches!(target, DialectType::DuckDB)
                        && matches!(source, DialectType::Snowflake) =>
                {
                    Action::Collections(collections::Action::ArrayDistinctConvert)
                }
                // StrPosition with position -> complex expansion for Presto/DuckDB
                // STRPOS doesn't support a position arg in these dialects
                Expression::StrPosition(ref sp)
                    if sp.position.is_some()
                        && matches!(
                            target,
                            DialectType::Presto
                                | DialectType::Trino
                                | DialectType::Athena
                                | DialectType::DuckDB
                        ) =>
                {
                    Action::Scalar(scalar::Action::StrPositionExpand)
                }
                // FIRST(col) IGNORE NULLS -> ANY_VALUE(col) for DuckDB
                Expression::First(ref f)
                    if f.ignore_nulls == Some(true)
                        && matches!(target, DialectType::DuckDB) =>
                {
                    Action::Aggregates(aggregates::Action::FirstToAnyValue)
                }
                // BEGIN -> START TRANSACTION for Presto/Trino
                Expression::Command(ref cmd)
                    if cmd.this.eq_ignore_ascii_case("BEGIN")
                        && matches!(
                            target,
                            DialectType::Presto | DialectType::Trino | DialectType::Athena
                        ) =>
                {
                    // Handled inline below
                    Action::None // We'll handle it directly
                }
                // Note: PostgreSQL ^ is now parsed as Power directly (not BitwiseXor).
                // PostgreSQL # is parsed as BitwiseXor (which is correct).
                Expression::Cbrt(_)
                    if Dialect::is_postgres_family_source(source)
                        && matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                {
                    Action::Scalar(scalar::Action::CbrtToPower)
                }
                // PostgreSQL string concatenation coerces non-string operands to text.
                // T-SQL's overloaded + needs explicit string operands to avoid numeric addition.
                Expression::Concat(ref _op)
                    if matches!(source, DialectType::PostgreSQL)
                        && matches!(target, DialectType::TSQL | DialectType::Fabric) =>
                {
                    Action::Operators(operators::Action::PostgresPipeConcatToTsql)
                }
                // a || b (Concat operator) -> CONCAT function for Presto/Trino
                Expression::Concat(ref _op)
                    if matches!(source, DialectType::PostgreSQL | DialectType::Redshift)
                        && matches!(target, DialectType::Presto | DialectType::Trino) =>
                {
                    Action::Operators(operators::Action::PipeConcatToConcat)
                }
                _ => Action::None,
            }
        };

        // Dispatch exactly one action. Rewritten nodes are returned to the
        // traversal without being run through another normalization domain.
        let outcome = match action {
            Action::None => {
                let expression = (|| -> Result<Expression> {
                    // Handle inline transforms that don't need a dedicated action
                    if matches!(target, DialectType::TSQL | DialectType::Fabric) {
                        if let Some(rewritten) =
                            temporal::rewrite_tsql_interval_arithmetic(&e, &context)?
                        {
                            return Ok(rewritten);
                        }
                    }

                    // BETWEEN SYMMETRIC/ASYMMETRIC expansion for non-PostgreSQL/Dremio targets
                    if let Expression::Between(ref b) = e {
                        if let Some(sym) = b.symmetric {
                            let keeps_symmetric =
                                matches!(target, DialectType::PostgreSQL | DialectType::Dremio);
                            if !keeps_symmetric {
                                if sym {
                                    // SYMMETRIC: expand to (x BETWEEN a AND b OR x BETWEEN b AND a)
                                    let b = if let Expression::Between(b) = e {
                                        *b
                                    } else {
                                        unreachable!()
                                    };
                                    let between1 = Expression::Between(Box::new(
                                        crate::expressions::Between {
                                            this: b.this.clone(),
                                            low: b.low.clone(),
                                            high: b.high.clone(),
                                            not: b.not,
                                            symmetric: None,
                                        },
                                    ));
                                    let between2 = Expression::Between(Box::new(
                                        crate::expressions::Between {
                                            this: b.this,
                                            low: b.high,
                                            high: b.low,
                                            not: b.not,
                                            symmetric: None,
                                        },
                                    ));
                                    return Ok(Expression::Paren(Box::new(
                                        crate::expressions::Paren {
                                            this: Expression::Or(Box::new(
                                                crate::expressions::BinaryOp::new(
                                                    between1, between2,
                                                ),
                                            )),
                                            trailing_comments: vec![],
                                        },
                                    )));
                                } else {
                                    // ASYMMETRIC: strip qualifier, keep as regular BETWEEN
                                    let b = if let Expression::Between(b) = e {
                                        *b
                                    } else {
                                        unreachable!()
                                    };
                                    return Ok(Expression::Between(Box::new(
                                        crate::expressions::Between {
                                            this: b.this,
                                            low: b.low,
                                            high: b.high,
                                            not: b.not,
                                            symmetric: None,
                                        },
                                    )));
                                }
                            }
                        }
                    }

                    // ILIKE -> LOWER(x) LIKE LOWER(y) for StarRocks/Doris
                    if let Expression::ILike(ref _like) = e {
                        if matches!(target, DialectType::StarRocks | DialectType::Doris) {
                            let like = if let Expression::ILike(l) = e {
                                *l
                            } else {
                                unreachable!()
                            };
                            let lower_left = Expression::Function(Box::new(Function::new(
                                "LOWER".to_string(),
                                vec![like.left],
                            )));
                            let lower_right = Expression::Function(Box::new(Function::new(
                                "LOWER".to_string(),
                                vec![like.right],
                            )));
                            return Ok(Expression::Like(Box::new(crate::expressions::LikeOp {
                                left: lower_left,
                                right: lower_right,
                                escape: like.escape,
                                quantifier: like.quantifier,
                                inferred_type: None,
                            })));
                        }
                    }

                    // Oracle DBMS_RANDOM.VALUE() -> RANDOM() for PostgreSQL, RAND() for others
                    if let Expression::MethodCall(ref mc) = e {
                        if matches!(source, DialectType::Oracle)
                            && mc.method.name.eq_ignore_ascii_case("VALUE")
                            && mc.args.is_empty()
                        {
                            let is_dbms_random = match &mc.this {
                                Expression::Identifier(id) => {
                                    id.name.eq_ignore_ascii_case("DBMS_RANDOM")
                                }
                                Expression::Column(col) => {
                                    col.table.is_none()
                                        && col.name.name.eq_ignore_ascii_case("DBMS_RANDOM")
                                }
                                _ => false,
                            };
                            if is_dbms_random {
                                let func_name = match target {
                                    DialectType::PostgreSQL
                                    | DialectType::Redshift
                                    | DialectType::DuckDB
                                    | DialectType::SQLite => "RANDOM",
                                    DialectType::Oracle => "DBMS_RANDOM.VALUE",
                                    _ => "RAND",
                                };
                                return Ok(Expression::Function(Box::new(Function::new(
                                    func_name.to_string(),
                                    vec![],
                                ))));
                            }
                        }
                    }
                    // TRIM without explicit position -> add BOTH for ClickHouse
                    if let Expression::Trim(ref trim) = e {
                        if matches!(target, DialectType::ClickHouse)
                            && trim.sql_standard_syntax
                            && trim.characters.is_some()
                            && !trim.position_explicit
                        {
                            let mut new_trim = (**trim).clone();
                            new_trim.position_explicit = true;
                            return Ok(Expression::Trim(Box::new(new_trim)));
                        }
                    }
                    // BEGIN -> START TRANSACTION for Presto/Trino
                    if let Expression::Transaction(ref txn) = e {
                        if matches!(
                            target,
                            DialectType::Presto | DialectType::Trino | DialectType::Athena
                        ) {
                            // Convert BEGIN to START TRANSACTION by setting mark to "START"
                            let mut txn = txn.clone();
                            txn.mark = Some(Box::new(Expression::Identifier(Identifier::new(
                                "START".to_string(),
                            ))));
                            return Ok(Expression::Transaction(Box::new(*txn)));
                        }
                    }
                    // IS TRUE/FALSE -> simplified forms for Presto/Trino
                    if matches!(
                        target,
                        DialectType::Presto | DialectType::Trino | DialectType::Athena
                    ) {
                        match &e {
                            Expression::IsTrue(itf) if !itf.not => {
                                // x IS TRUE -> x
                                return Ok(itf.this.clone());
                            }
                            Expression::IsTrue(itf) if itf.not => {
                                // x IS NOT TRUE -> NOT x
                                return Ok(Expression::Not(Box::new(
                                    crate::expressions::UnaryOp {
                                        this: itf.this.clone(),
                                        inferred_type: None,
                                    },
                                )));
                            }
                            Expression::IsFalse(itf) if !itf.not => {
                                // x IS FALSE -> NOT x
                                return Ok(Expression::Not(Box::new(
                                    crate::expressions::UnaryOp {
                                        this: itf.this.clone(),
                                        inferred_type: None,
                                    },
                                )));
                            }
                            Expression::IsFalse(itf) if itf.not => {
                                // x IS NOT FALSE -> NOT NOT x
                                let not_x =
                                    Expression::Not(Box::new(crate::expressions::UnaryOp {
                                        this: itf.this.clone(),
                                        inferred_type: None,
                                    }));
                                return Ok(Expression::Not(Box::new(
                                    crate::expressions::UnaryOp {
                                        this: not_x,
                                        inferred_type: None,
                                    },
                                )));
                            }
                            _ => {}
                        }
                    }
                    // x IS NOT FALSE -> NOT x IS FALSE for Redshift
                    if matches!(target, DialectType::Redshift) {
                        if let Expression::IsFalse(ref itf) = e {
                            if itf.not {
                                return Ok(Expression::Not(Box::new(
                                    crate::expressions::UnaryOp {
                                        this: Expression::IsFalse(Box::new(
                                            crate::expressions::IsTrueFalse {
                                                this: itf.this.clone(),
                                                not: false,
                                            },
                                        )),
                                        inferred_type: None,
                                    },
                                )));
                            }
                        }
                    }
                    // REGEXP_REPLACE: add 'g' flag when source defaults to global replacement
                    // Snowflake default is global, PostgreSQL/DuckDB default is first-match-only
                    if let Expression::Function(ref f) = e {
                        if f.name.eq_ignore_ascii_case("REGEXP_REPLACE")
                            && matches!(source, DialectType::Snowflake)
                            && matches!(target, DialectType::PostgreSQL | DialectType::DuckDB)
                        {
                            if f.args.len() == 3 {
                                let mut args = f.args.clone();
                                args.push(Expression::string("g"));
                                return Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_REPLACE".to_string(),
                                    args,
                                ))));
                            } else if f.args.len() == 4 {
                                // 4th arg might be position, add 'g' as 5th
                                let mut args = f.args.clone();
                                args.push(Expression::string("g"));
                                return Ok(Expression::Function(Box::new(Function::new(
                                    "REGEXP_REPLACE".to_string(),
                                    args,
                                ))));
                            }
                        }
                    }
                    Ok(e)
                })()?;
                RewriteOutcome::NoMatch(expression)
            }
            Action::Statements(action) => statements::rewrite(action, e, &context)?,
            Action::Types(action) => types::rewrite(action, e, &context)?,
            Action::Temporal(action) => temporal::rewrite(action, e, &context)?,
            Action::Collections(action) => collections::rewrite(action, e, &context)?,
            Action::Json(action) => json::rewrite(action, e, &context)?,
            Action::Aggregates(action) => aggregates::rewrite(action, e, &context)?,
            Action::Operators(action) => operators::rewrite(action, e, &context)?,
            Action::Scalar(action) => scalar::rewrite(action, e, &context)?,
        };

        Ok(outcome.into_expression())
    })
}
