//! Lower HANA system relations while respecting lexical CTE scope.

use crate::dialects::DialectType;
use crate::error::{Error, Result};
use crate::expressions::*;

fn is_name(identifier: &Identifier, name: &str) -> bool {
    if identifier.quoted {
        identifier.name == name
    } else {
        identifier.name.eq_ignore_ascii_case(name)
    }
}

fn with_clause(expression: &mut Expression) -> Option<&mut Option<With>> {
    match expression {
        Expression::Select(s) => Some(&mut s.with),
        Expression::Union(s) => Some(&mut s.with),
        Expression::Intersect(s) => Some(&mut s.with),
        Expression::Except(s) => Some(&mut s.with),
        Expression::Insert(s) => Some(&mut s.with),
        Expression::Update(s) => Some(&mut s.with),
        Expression::Delete(s) => Some(&mut s.with),
        Expression::CreateTable(s) => Some(&mut s.with_cte),
        Expression::Pivot(s) => Some(&mut s.with),
        _ => None,
    }
}

pub(super) fn lower_dummy(mut expression: Expression, target: DialectType) -> Result<Expression> {
    visit(&mut expression, false, target)?;
    Ok(expression)
}

fn visit(expression: &mut Expression, shadowed: bool, target: DialectType) -> Result<()> {
    #[cfg(feature = "stacker")]
    {
        stacker::maybe_grow(1024 * 1024, 8 * 1024 * 1024, || {
            visit_inner(expression, shadowed, target)
        })
    }
    #[cfg(not(feature = "stacker"))]
    {
        visit_inner(expression, shadowed, target)
    }
}

fn visit_inner(expression: &mut Expression, mut shadowed: bool, target: DialectType) -> Result<()> {
    if let Expression::Dot(dot) = expression {
        if matches!(&dot.this, Expression::Column(column)
            if is_name(&column.name, "DUMMY") && column.table.as_ref().is_some_and(|name| is_name(name, "SYS")))
        {
            // Replacing a schema-qualified relation with an alias can change
            // binding in correlated queries. Require an explicit source alias.
            return Err(Error::unsupported(
                "Schema-qualified HANA DUMMY columns require an explicit table alias",
                target.to_string(),
            ));
        }
    }
    // A non-recursive CTE sees preceding CTEs, but not its own alias. Temporarily
    // detach WITH so the generic child walk does not visit its bodies twice.
    let mut with = with_clause(expression).and_then(Option::take);
    if let Some(with) = &mut with {
        if with.recursive && with.ctes.iter().any(|cte| is_name(&cte.alias, "DUMMY")) {
            shadowed = true;
        }
        for cte in &mut with.ctes {
            visit(&mut cte.this, shadowed, target)?;
            shadowed |= is_name(&cte.alias, "DUMMY");
        }
        if let Some(search) = &mut with.search {
            visit(search, shadowed, target)?;
        }
    }

    if let Expression::Table(table) = expression {
        let system_schema = table.schema.as_ref().is_some_and(|s| is_name(s, "SYS"));
        if is_name(&table.name, "DUMMY")
            && table.catalog.is_none()
            && (system_schema || table.schema.is_none() && !shadowed)
        {
            if !matches!(
                target,
                DialectType::DuckDB
                    | DialectType::PostgreSQL
                    | DialectType::Trino
                    | DialectType::Presto
                    | DialectType::Athena
                    | DialectType::Dune
                    | DialectType::CockroachDB
                    | DialectType::Materialize
                    | DialectType::RisingWave
            ) || table.when.is_some()
                || table.only
                || table.final_
                || table.table_sample.is_some()
                || !table.hints.is_empty()
                || table.system_time.is_some()
                || !table.partitions.is_empty()
                || table.identifier_func.is_some()
                || table.changes.is_some()
                || table.version.is_some()
            {
                return Err(Error::unsupported(
                    "HANA DUMMY relation with no verified target mapping",
                    target.to_string(),
                ));
            }
            // SAP's system table is one row, one column named DUMMY, value 'X'.
            // Keeping that row as a derived table preserves *, aliases, joins,
            // predicates and aggregate cardinality, unlike dropping FROM blindly.
            let mut row = Select::new();
            row.expressions.push(Expression::Alias(Box::new(Alias::new(
                Expression::string("X"),
                Identifier::new("DUMMY"),
            ))));
            row.leading_comments = std::mem::take(&mut table.leading_comments);
            row.leading_comments.append(&mut table.trailing_comments);
            let subquery = Subquery {
                this: Expression::Select(Box::new(row)),
                alias: Some(table.alias.clone().unwrap_or_else(|| table.name.clone())),
                alias_explicit_as: true,
                column_aliases: table.column_aliases.clone(),
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
            };
            *expression = Expression::Subquery(Box::new(subquery));
        }
    }

    let mut result = Ok(());
    crate::ast_children::for_each_child_mut(expression, |child| {
        if result.is_ok() {
            result = visit(child, shadowed, target);
        }
    });
    if let Some(slot) = with_clause(expression) {
        *slot = with;
    }
    result
}
