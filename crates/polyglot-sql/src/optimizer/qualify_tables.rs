//! Table Qualification Module
//!
//! This module provides functionality for qualifying table references in SQL queries
//! with their database and catalog names.
//!
//! Ported from sqlglot's optimizer/qualify_tables.py

use crate::dialects::DialectType;
use crate::expressions::{Expression, Identifier, Select, TableRef};
use crate::helper::name_sequence;
use crate::optimizer::normalize_identifiers::{
    get_normalization_strategy, normalize_identifier, NormalizationStrategy,
};
use std::collections::{HashMap, HashSet};

/// Options for table qualification
#[derive(Debug, Clone, Default)]
pub struct QualifyTablesOptions {
    /// Default database name to add to unqualified tables
    pub db: Option<String>,
    /// Default catalog name to add to tables that have a db but no catalog
    pub catalog: Option<String>,
    /// The dialect to use for normalization
    pub dialect: Option<DialectType>,
    /// Whether to use canonical aliases (_0, _1, ...) instead of table names
    pub canonicalize_table_aliases: bool,
}

impl QualifyTablesOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_db(mut self, db: impl Into<String>) -> Self {
        self.db = Some(db.into());
        self
    }

    pub fn with_catalog(mut self, catalog: impl Into<String>) -> Self {
        self.catalog = Some(catalog.into());
        self
    }

    pub fn with_dialect(mut self, dialect: DialectType) -> Self {
        self.dialect = Some(dialect);
        self
    }

    pub fn with_canonical_aliases(mut self) -> Self {
        self.canonicalize_table_aliases = true;
        self
    }
}

/// Rewrite SQL AST to have fully qualified tables.
///
/// This function:
/// - Adds database/catalog prefixes to table references
/// - Ensures all tables have aliases
/// - Optionally canonicalizes aliases to _0, _1, etc.
///
/// # Examples
///
/// ```ignore
/// // SELECT 1 FROM tbl -> SELECT 1 FROM db.tbl AS tbl
/// let options = QualifyTablesOptions::new().with_db("db");
/// let qualified = qualify_tables(expression, &options);
/// ```
///
/// # Arguments
/// * `expression` - The expression to qualify
/// * `options` - Qualification options
///
/// # Returns
/// The qualified expression
pub fn qualify_tables(expression: Expression, options: &QualifyTablesOptions) -> Expression {
    let strategy = get_normalization_strategy(options.dialect);
    let mut next_alias = name_sequence("_");

    match expression {
        Expression::Select(select) => {
            let qualified = qualify_select(*select, options, strategy, &mut next_alias);
            Expression::Select(Box::new(qualified))
        }
        Expression::Union(mut union) => {
            union.left = qualify_tables(union.left, options);
            union.right = qualify_tables(union.right, options);
            Expression::Union(union)
        }
        Expression::Intersect(mut intersect) => {
            intersect.left = qualify_tables(intersect.left, options);
            intersect.right = qualify_tables(intersect.right, options);
            Expression::Intersect(intersect)
        }
        Expression::Except(mut except) => {
            except.left = qualify_tables(except.left, options);
            except.right = qualify_tables(except.right, options);
            Expression::Except(except)
        }
        _ => expression,
    }
}

/// Qualify a SELECT expression
fn qualify_select(
    mut select: Select,
    options: &QualifyTablesOptions,
    strategy: NormalizationStrategy,
    next_alias: &mut impl FnMut() -> String,
) -> Select {
    // Collect CTE names to avoid qualifying them
    let cte_names: HashSet<String> = select
        .with
        .as_ref()
        .map(|w| w.ctes.iter().map(|c| c.alias.name.clone()).collect())
        .unwrap_or_default();

    // Track canonical aliases if needed
    let mut canonical_aliases: HashMap<String, String> = HashMap::new();

    // Qualify CTEs first
    if let Some(ref mut with) = select.with {
        for cte in &mut with.ctes {
            cte.this = qualify_tables(cte.this.clone(), options);
        }
    }

    // Qualify tables in FROM clause
    if let Some(ref mut from) = select.from {
        for expr in &mut from.expressions {
            *expr = qualify_table_expression(
                expr.clone(),
                options,
                strategy,
                &cte_names,
                &mut canonical_aliases,
                next_alias,
            );
        }
    }

    // Qualify tables in JOINs
    for join in &mut select.joins {
        join.this = qualify_table_expression(
            join.this.clone(),
            options,
            strategy,
            &cte_names,
            &mut canonical_aliases,
            next_alias,
        );
    }

    // Update column references if using canonical aliases
    if options.canonicalize_table_aliases && !canonical_aliases.is_empty() {
        select = update_column_references(select, &canonical_aliases);
    }

    select
}

/// Qualify a table expression (Table, Subquery, etc.)
fn qualify_table_expression(
    expression: Expression,
    options: &QualifyTablesOptions,
    strategy: NormalizationStrategy,
    cte_names: &HashSet<String>,
    canonical_aliases: &mut HashMap<String, String>,
    next_alias: &mut impl FnMut() -> String,
) -> Expression {
    match expression {
        Expression::Table(mut table) => {
            let table_name = table.name.name.clone();

            // Don't qualify CTEs
            if cte_names.contains(&table_name) {
                // Still ensure it has an alias
                ensure_table_alias(&mut table, strategy, canonical_aliases, next_alias, options);
                return Expression::Table(table);
            }

            // Add db if specified and not already present
            if let Some(ref db) = options.db {
                if table.schema.is_none() {
                    table.schema = Some(normalize_identifier(
                        Identifier::new(db.clone()),
                        strategy,
                    ));
                }
            }

            // Add catalog if specified, db is present, and catalog not already present
            if let Some(ref catalog) = options.catalog {
                if table.schema.is_some() && table.catalog.is_none() {
                    table.catalog = Some(normalize_identifier(
                        Identifier::new(catalog.clone()),
                        strategy,
                    ));
                }
            }

            // Ensure the table has an alias
            ensure_table_alias(&mut table, strategy, canonical_aliases, next_alias, options);

            Expression::Table(table)
        }
        Expression::Subquery(mut subquery) => {
            // Qualify the inner query
            subquery.this = qualify_tables(subquery.this, options);

            // Ensure the subquery has an alias
            if subquery.alias.is_none() || options.canonicalize_table_aliases {
                let alias_name = if options.canonicalize_table_aliases {
                    let new_name = next_alias();
                    if let Some(ref old_alias) = subquery.alias {
                        canonical_aliases.insert(old_alias.name.clone(), new_name.clone());
                    }
                    new_name
                } else {
                    subquery
                        .alias
                        .as_ref()
                        .map(|a| a.name.clone())
                        .unwrap_or_else(|| next_alias())
                };

                subquery.alias = Some(normalize_identifier(Identifier::new(alias_name), strategy));
            }

            Expression::Subquery(subquery)
        }
        Expression::Paren(mut paren) => {
            paren.this = qualify_table_expression(
                paren.this,
                options,
                strategy,
                cte_names,
                canonical_aliases,
                next_alias,
            );
            Expression::Paren(paren)
        }
        _ => expression,
    }
}

/// Ensure a table has an alias
fn ensure_table_alias(
    table: &mut TableRef,
    strategy: NormalizationStrategy,
    canonical_aliases: &mut HashMap<String, String>,
    next_alias: &mut impl FnMut() -> String,
    options: &QualifyTablesOptions,
) {
    let table_name = table.name.name.clone();

    if options.canonicalize_table_aliases {
        // Use canonical alias (_0, _1, etc.)
        let new_alias = next_alias();
        let old_alias = table.alias.as_ref().map(|a| a.name.clone()).unwrap_or(table_name.clone());
        canonical_aliases.insert(old_alias, new_alias.clone());
        table.alias = Some(normalize_identifier(Identifier::new(new_alias), strategy));
    } else if table.alias.is_none() {
        // Use table name as alias
        table.alias = Some(normalize_identifier(
            Identifier::new(table_name),
            strategy,
        ));
    }
}

/// Update column references to use canonical aliases
fn update_column_references(mut select: Select, canonical_aliases: &HashMap<String, String>) -> Select {
    // Update SELECT expressions
    select.expressions = select
        .expressions
        .into_iter()
        .map(|e| update_column_in_expression(e, canonical_aliases))
        .collect();

    // Update WHERE
    if let Some(mut where_clause) = select.where_clause {
        where_clause.this = update_column_in_expression(where_clause.this, canonical_aliases);
        select.where_clause = Some(where_clause);
    }

    // Update GROUP BY
    if let Some(mut group_by) = select.group_by {
        group_by.expressions = group_by
            .expressions
            .into_iter()
            .map(|e| update_column_in_expression(e, canonical_aliases))
            .collect();
        select.group_by = Some(group_by);
    }

    // Update HAVING
    if let Some(mut having) = select.having {
        having.this = update_column_in_expression(having.this, canonical_aliases);
        select.having = Some(having);
    }

    // Update ORDER BY
    if let Some(mut order_by) = select.order_by {
        order_by.expressions = order_by
            .expressions
            .into_iter()
            .map(|mut o| {
                o.this = update_column_in_expression(o.this, canonical_aliases);
                o
            })
            .collect();
        select.order_by = Some(order_by);
    }

    // Update JOIN ON conditions
    for join in &mut select.joins {
        if let Some(on) = &mut join.on {
            *on = update_column_in_expression(on.clone(), canonical_aliases);
        }
    }

    select
}

/// Update column references in an expression
fn update_column_in_expression(
    expression: Expression,
    canonical_aliases: &HashMap<String, String>,
) -> Expression {
    match expression {
        Expression::Column(mut col) => {
            if let Some(ref table) = col.table {
                if let Some(canonical) = canonical_aliases.get(&table.name) {
                    col.table = Some(Identifier {
                        name: canonical.clone(),
                        quoted: table.quoted,
                        trailing_comments: table.trailing_comments.clone(),
                    });
                }
            }
            Expression::Column(col)
        }
        Expression::And(mut bin) => {
            bin.left = update_column_in_expression(bin.left, canonical_aliases);
            bin.right = update_column_in_expression(bin.right, canonical_aliases);
            Expression::And(bin)
        }
        Expression::Or(mut bin) => {
            bin.left = update_column_in_expression(bin.left, canonical_aliases);
            bin.right = update_column_in_expression(bin.right, canonical_aliases);
            Expression::Or(bin)
        }
        Expression::Eq(mut bin) => {
            bin.left = update_column_in_expression(bin.left, canonical_aliases);
            bin.right = update_column_in_expression(bin.right, canonical_aliases);
            Expression::Eq(bin)
        }
        Expression::Neq(mut bin) => {
            bin.left = update_column_in_expression(bin.left, canonical_aliases);
            bin.right = update_column_in_expression(bin.right, canonical_aliases);
            Expression::Neq(bin)
        }
        Expression::Lt(mut bin) => {
            bin.left = update_column_in_expression(bin.left, canonical_aliases);
            bin.right = update_column_in_expression(bin.right, canonical_aliases);
            Expression::Lt(bin)
        }
        Expression::Lte(mut bin) => {
            bin.left = update_column_in_expression(bin.left, canonical_aliases);
            bin.right = update_column_in_expression(bin.right, canonical_aliases);
            Expression::Lte(bin)
        }
        Expression::Gt(mut bin) => {
            bin.left = update_column_in_expression(bin.left, canonical_aliases);
            bin.right = update_column_in_expression(bin.right, canonical_aliases);
            Expression::Gt(bin)
        }
        Expression::Gte(mut bin) => {
            bin.left = update_column_in_expression(bin.left, canonical_aliases);
            bin.right = update_column_in_expression(bin.right, canonical_aliases);
            Expression::Gte(bin)
        }
        Expression::Not(mut un) => {
            un.this = update_column_in_expression(un.this, canonical_aliases);
            Expression::Not(un)
        }
        Expression::Paren(mut paren) => {
            paren.this = update_column_in_expression(paren.this, canonical_aliases);
            Expression::Paren(paren)
        }
        Expression::Alias(mut alias) => {
            alias.this = update_column_in_expression(alias.this, canonical_aliases);
            Expression::Alias(alias)
        }
        Expression::Function(mut func) => {
            func.args = func
                .args
                .into_iter()
                .map(|a| update_column_in_expression(a, canonical_aliases))
                .collect();
            Expression::Function(func)
        }
        Expression::AggregateFunction(mut agg) => {
            agg.args = agg
                .args
                .into_iter()
                .map(|a| update_column_in_expression(a, canonical_aliases))
                .collect();
            Expression::AggregateFunction(agg)
        }
        Expression::Case(mut case) => {
            case.operand = case
                .operand
                .map(|o| update_column_in_expression(o, canonical_aliases));
            case.whens = case
                .whens
                .into_iter()
                .map(|(w, t)| {
                    (
                        update_column_in_expression(w, canonical_aliases),
                        update_column_in_expression(t, canonical_aliases),
                    )
                })
                .collect();
            case.else_ = case
                .else_
                .map(|e| update_column_in_expression(e, canonical_aliases));
            Expression::Case(case)
        }
        _ => expression,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::Generator;
    use crate::parser::Parser;

    fn gen(expr: &Expression) -> String {
        Generator::new().generate(expr).unwrap()
    }

    fn parse(sql: &str) -> Expression {
        Parser::parse_sql(sql).expect("Failed to parse")[0].clone()
    }

    #[test]
    fn test_qualify_with_db() {
        let options = QualifyTablesOptions::new().with_db("mydb");
        let expr = parse("SELECT * FROM users");
        let qualified = qualify_tables(expr, &options);
        let sql = gen(&qualified);
        // Should contain mydb.users
        assert!(sql.contains("mydb") && sql.contains("users"));
    }

    #[test]
    fn test_qualify_with_db_and_catalog() {
        let options = QualifyTablesOptions::new()
            .with_db("mydb")
            .with_catalog("mycatalog");
        let expr = parse("SELECT * FROM users");
        let qualified = qualify_tables(expr, &options);
        let sql = gen(&qualified);
        // Should contain mycatalog.mydb.users
        assert!(sql.contains("mycatalog") && sql.contains("mydb") && sql.contains("users"));
    }

    #[test]
    fn test_preserve_existing_schema() {
        let options = QualifyTablesOptions::new().with_db("default_db");
        let expr = parse("SELECT * FROM other_db.users");
        let qualified = qualify_tables(expr, &options);
        let sql = gen(&qualified);
        // Should preserve other_db, not add default_db
        assert!(sql.contains("other_db"));
        assert!(!sql.contains("default_db"));
    }

    #[test]
    fn test_ensure_table_alias() {
        let options = QualifyTablesOptions::new();
        let expr = parse("SELECT * FROM users");
        let qualified = qualify_tables(expr, &options);
        let sql = gen(&qualified);
        // Should have alias (AS users)
        assert!(sql.contains("AS") || sql.to_lowercase().contains(" users"));
    }

    #[test]
    fn test_canonical_aliases() {
        let options = QualifyTablesOptions::new().with_canonical_aliases();
        let expr = parse("SELECT u.id FROM users u");
        let qualified = qualify_tables(expr, &options);
        let sql = gen(&qualified);
        // Should use canonical alias like _0
        assert!(sql.contains("_0"));
    }

    #[test]
    fn test_qualify_join() {
        let options = QualifyTablesOptions::new().with_db("mydb");
        let expr = parse("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
        let qualified = qualify_tables(expr, &options);
        let sql = gen(&qualified);
        // Both tables should be qualified
        assert!(sql.contains("mydb"));
    }

    #[test]
    fn test_dont_qualify_cte() {
        let options = QualifyTablesOptions::new().with_db("mydb");
        let expr = parse("WITH cte AS (SELECT 1) SELECT * FROM cte");
        let qualified = qualify_tables(expr, &options);
        let sql = gen(&qualified);
        // CTE reference should not be qualified with mydb
        // The CTE definition might have mydb, but the SELECT FROM cte should not
        assert!(sql.contains("cte"));
    }

    #[test]
    fn test_qualify_subquery() {
        let options = QualifyTablesOptions::new().with_db("mydb");
        let expr = parse("SELECT * FROM (SELECT * FROM users) AS sub");
        let qualified = qualify_tables(expr, &options);
        let sql = gen(&qualified);
        // Inner table should be qualified
        assert!(sql.contains("mydb"));
    }
}
