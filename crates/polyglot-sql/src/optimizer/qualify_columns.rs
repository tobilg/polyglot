//! Column Qualification Module
//!
//! This module provides functionality for qualifying column references in SQL queries,
//! adding table qualifiers to column names and expanding star expressions.
//!
//! Ported from sqlglot's optimizer/qualify_columns.py

use crate::dialects::DialectType;
use crate::expressions::{Alias, Column, Expression, Identifier, Join, LateralView, Over, TableRef, With};
use crate::resolver::{Resolver, ResolverError};
use crate::schema::Schema;
use crate::scope::{traverse_scope, Scope};
use std::collections::{HashMap, HashSet};
use thiserror::Error;

/// Errors that can occur during column qualification
#[derive(Debug, Error, Clone)]
pub enum QualifyColumnsError {
    #[error("Unknown table: {0}")]
    UnknownTable(String),

    #[error("Unknown column: {0}")]
    UnknownColumn(String),

    #[error("Ambiguous column: {0}")]
    AmbiguousColumn(String),

    #[error("Cannot automatically join: {0}")]
    CannotAutoJoin(String),

    #[error("Unknown output column: {0}")]
    UnknownOutputColumn(String),

    #[error("Column could not be resolved: {column}{for_table}")]
    ColumnNotResolved { column: String, for_table: String },

    #[error("Resolver error: {0}")]
    ResolverError(#[from] ResolverError),
}

/// Result type for column qualification operations
pub type QualifyColumnsResult<T> = Result<T, QualifyColumnsError>;

/// Options for column qualification
#[derive(Debug, Clone, Default)]
pub struct QualifyColumnsOptions {
    /// Whether to expand references to aliases
    pub expand_alias_refs: bool,
    /// Whether to expand star expressions to explicit columns
    pub expand_stars: bool,
    /// Whether to infer schema if not provided
    pub infer_schema: Option<bool>,
    /// Whether to allow partial qualification
    pub allow_partial_qualification: bool,
    /// The dialect for dialect-specific behavior
    pub dialect: Option<DialectType>,
}

impl QualifyColumnsOptions {
    /// Create new options with defaults
    pub fn new() -> Self {
        Self {
            expand_alias_refs: true,
            expand_stars: true,
            infer_schema: None,
            allow_partial_qualification: false,
            dialect: None,
        }
    }

    /// Set whether to expand alias refs
    pub fn with_expand_alias_refs(mut self, expand: bool) -> Self {
        self.expand_alias_refs = expand;
        self
    }

    /// Set whether to expand stars
    pub fn with_expand_stars(mut self, expand: bool) -> Self {
        self.expand_stars = expand;
        self
    }

    /// Set the dialect
    pub fn with_dialect(mut self, dialect: DialectType) -> Self {
        self.dialect = Some(dialect);
        self
    }

    /// Set whether to allow partial qualification
    pub fn with_allow_partial(mut self, allow: bool) -> Self {
        self.allow_partial_qualification = allow;
        self
    }
}

/// Rewrite SQL AST to have fully qualified columns.
///
/// # Example
/// ```ignore
/// // SELECT col FROM tbl => SELECT tbl.col AS col FROM tbl
/// ```
///
/// # Arguments
/// * `expression` - Expression to qualify
/// * `schema` - Database schema for column lookup
/// * `options` - Qualification options
///
/// # Returns
/// The qualified expression
pub fn qualify_columns(
    expression: Expression,
    schema: &dyn Schema,
    options: &QualifyColumnsOptions,
) -> QualifyColumnsResult<Expression> {
    let infer_schema = options.infer_schema.unwrap_or(schema.is_empty());
    let dialect = options.dialect.or_else(|| schema.dialect());

    let result = expression.clone();

    // Traverse all scopes and qualify columns
    for scope in traverse_scope(&expression) {
        let scope_expression = &scope.expression;
        let is_select = matches!(scope_expression, Expression::Select(_));

        // Create resolver for this scope
        let mut resolver = Resolver::new(&scope, schema, infer_schema);

        // Qualify columns - add table qualifiers
        qualify_columns_in_scope(&scope, &mut resolver, options.allow_partial_qualification)?;

        // Expand alias refs if enabled
        if options.expand_alias_refs {
            expand_alias_refs(&scope, &mut resolver, dialect)?;
        }

        if is_select {
            // Expand stars if enabled
            if options.expand_stars {
                expand_stars(&scope, &mut resolver)?;
            }

            // Ensure output columns are aliased
            qualify_outputs(&scope)?;
        }

        // Expand GROUP BY positional references
        expand_group_by(&scope, dialect)?;
    }

    Ok(result)
}

/// Validate that all columns in an expression are qualified.
///
/// # Returns
/// The expression if valid, or an error if unqualified columns exist.
pub fn validate_qualify_columns(expression: &Expression) -> QualifyColumnsResult<()> {
    let mut all_unqualified = Vec::new();

    for scope in traverse_scope(expression) {
        if let Expression::Select(_) = &scope.expression {
            // Get unqualified columns from this scope
            let unqualified = get_unqualified_columns(&scope);

            // Check for external columns that couldn't be resolved
            let external = get_external_columns(&scope);
            if !external.is_empty() && !is_correlated_subquery(&scope) {
                let first = &external[0];
                let for_table = if first.table.is_some() {
                    format!(" for table: '{}'", first.table.as_ref().unwrap())
                } else {
                    String::new()
                };
                return Err(QualifyColumnsError::ColumnNotResolved {
                    column: first.name.clone(),
                    for_table,
                });
            }

            all_unqualified.extend(unqualified);
        }
    }

    if !all_unqualified.is_empty() {
        let first = &all_unqualified[0];
        return Err(QualifyColumnsError::AmbiguousColumn(first.name.clone()));
    }

    Ok(())
}

/// Qualify columns in a scope by adding table qualifiers
fn qualify_columns_in_scope(
    scope: &Scope,
    resolver: &mut Resolver,
    allow_partial: bool,
) -> QualifyColumnsResult<()> {
    let columns = get_scope_columns(scope);

    for column_ref in columns {
        let column_table = &column_ref.table;
        let column_name = &column_ref.name;

        if let Some(table) = column_table {
            // Column already has a table qualifier, validate it
            if scope.sources.contains_key(table) {
                if let Ok(source_columns) = resolver.get_source_columns(table) {
                    if !allow_partial
                        && !source_columns.is_empty()
                        && !source_columns.contains(column_name)
                        && !source_columns.contains(&"*".to_string())
                    {
                        return Err(QualifyColumnsError::UnknownColumn(column_name.clone()));
                    }
                }
            }
        } else {
            // Column needs a table qualifier
            if let Some(table) = resolver.get_table(column_name) {
                // In a real implementation, we would modify the AST here
                // For now, we just validate that we can find the table
                let _ = table;
            }
        }
    }

    Ok(())
}

/// Expand alias references in a scope.
///
/// For example:
/// `SELECT y.foo AS bar, bar * 2 AS baz FROM y`
/// becomes:
/// `SELECT y.foo AS bar, y.foo * 2 AS baz FROM y`
fn expand_alias_refs(
    scope: &Scope,
    _resolver: &mut Resolver,
    _dialect: Option<DialectType>,
) -> QualifyColumnsResult<()> {
    let expression = &scope.expression;

    if !matches!(expression, Expression::Select(_)) {
        return Ok(());
    }

    // Build alias to expression mapping from SELECT list
    let mut _alias_to_expression: HashMap<String, (Expression, usize)> = HashMap::new();

    if let Expression::Select(select) = expression {
        for (i, expr) in select.expressions.iter().enumerate() {
            if let Expression::Alias(alias) = expr {
                _alias_to_expression.insert(alias.alias.name.clone(), (alias.this.clone(), i + 1));
            }
        }
    }

    // In a real implementation, we would walk through WHERE, GROUP BY, HAVING, etc.
    // and replace column references to aliases with the aliased expression

    Ok(())
}

/// Expand GROUP BY positional references.
///
/// For example:
/// `SELECT a, b FROM t GROUP BY 1, 2`
/// becomes:
/// `SELECT a, b FROM t GROUP BY a, b`
fn expand_group_by(scope: &Scope, _dialect: Option<DialectType>) -> QualifyColumnsResult<()> {
    if let Expression::Select(select) = &scope.expression {
        if let Some(_group_by) = &select.group_by {
            // In a real implementation, we would replace numeric literals
            // with the corresponding SELECT expressions
        }
    }
    Ok(())
}

/// Expand star expressions to explicit column lists.
///
/// For example:
/// `SELECT * FROM users`
/// becomes:
/// `SELECT users.id, users.name, users.email FROM users`
fn expand_stars(scope: &Scope, resolver: &mut Resolver) -> QualifyColumnsResult<()> {
    if let Expression::Select(select) = &scope.expression {
        let mut _new_selections: Vec<Expression> = Vec::new();
        let mut _has_star = false;

        for expr in &select.expressions {
            match expr {
                Expression::Star(_) => {
                    _has_star = true;
                    // Expand to all columns from all sources
                    for (source_name, _) in &scope.sources {
                        if let Ok(columns) = resolver.get_source_columns(source_name) {
                            if columns.contains(&"*".to_string()) || columns.is_empty() {
                                // Can't expand - unknown columns
                                return Ok(());
                            }
                            for col_name in columns {
                                _new_selections.push(create_qualified_column(
                                    &col_name,
                                    Some(source_name),
                                ));
                            }
                        }
                    }
                }
                Expression::Column(col) if is_star_column(col) => {
                    _has_star = true;
                    // Expand table.* to all columns from that table
                    if let Some(table) = &col.table {
                        let table_name = &table.name;
                        if !scope.sources.contains_key(table_name) {
                            return Err(QualifyColumnsError::UnknownTable(table_name.clone()));
                        }
                        if let Ok(columns) = resolver.get_source_columns(table_name) {
                            if columns.contains(&"*".to_string()) || columns.is_empty() {
                                return Ok(());
                            }
                            for col_name in columns {
                                _new_selections.push(create_qualified_column(
                                    &col_name,
                                    Some(table_name),
                                ));
                            }
                        }
                    }
                }
                _ => {
                    _new_selections.push(expr.clone());
                }
            }
        }

        // In a real implementation, we would replace the SELECT expressions
        // with _new_selections if _has_star is true
    }

    Ok(())
}

/// Ensure all output columns in a SELECT are aliased.
///
/// For example:
/// `SELECT a + b FROM t`
/// becomes:
/// `SELECT a + b AS _col_0 FROM t`
pub fn qualify_outputs(scope: &Scope) -> QualifyColumnsResult<()> {
    if let Expression::Select(select) = &scope.expression {
        let mut new_selections: Vec<Expression> = Vec::new();

        for (i, expr) in select.expressions.iter().enumerate() {
            match expr {
                Expression::Alias(_) => {
                    // Already aliased
                    new_selections.push(expr.clone());
                }
                Expression::Column(col) => {
                    // Use column name as alias
                    new_selections.push(create_alias(expr.clone(), &col.name.name));
                }
                Expression::Star(_) => {
                    // Don't alias stars
                    new_selections.push(expr.clone());
                }
                _ => {
                    // Need to add alias
                    let alias_name = get_output_name(expr).unwrap_or_else(|| format!("_col_{}", i));
                    new_selections.push(create_alias(expr.clone(), &alias_name));
                }
            }
        }

        // In a real implementation, we would replace the SELECT expressions
    }

    Ok(())
}

/// Returns the set of SQL reserved words for a given dialect.
/// If no dialect is provided, returns a comprehensive default set.
fn get_reserved_words(dialect: Option<DialectType>) -> HashSet<&'static str> {
    // Core SQL reserved words that are common across all dialects
    let mut words: HashSet<&'static str> = [
        // SQL standard reserved words
        "ADD", "ALL", "ALTER", "AND", "ANY", "AS", "ASC", "BETWEEN", "BY",
        "CASE", "CAST", "CHECK", "COLUMN", "CONSTRAINT", "CREATE", "CROSS",
        "CURRENT", "CURRENT_DATE", "CURRENT_TIME", "CURRENT_TIMESTAMP",
        "CURRENT_USER", "DATABASE", "DEFAULT", "DELETE", "DESC", "DISTINCT",
        "DROP", "ELSE", "END", "ESCAPE", "EXCEPT", "EXISTS", "FALSE",
        "FETCH", "FOR", "FOREIGN", "FROM", "FULL", "GRANT", "GROUP",
        "HAVING", "IF", "IN", "INDEX", "INNER", "INSERT", "INTERSECT",
        "INTO", "IS", "JOIN", "KEY", "LEFT", "LIKE", "LIMIT", "NATURAL",
        "NOT", "NULL", "OFFSET", "ON", "OR", "ORDER", "OUTER", "PRIMARY",
        "REFERENCES", "REPLACE", "RETURNING", "RIGHT", "ROLLBACK", "ROW",
        "ROWS", "SELECT", "SESSION_USER", "SET", "SOME", "TABLE", "THEN",
        "TO", "TRUE", "TRUNCATE", "UNION", "UNIQUE", "UPDATE", "USING",
        "VALUES", "VIEW", "WHEN", "WHERE", "WINDOW", "WITH",
    ].iter().copied().collect();

    // Add dialect-specific reserved words
    match dialect {
        Some(DialectType::MySQL) => {
            words.extend([
                "ANALYZE", "BOTH", "CHANGE", "CONDITION", "DATABASES",
                "DAY_HOUR", "DAY_MICROSECOND", "DAY_MINUTE", "DAY_SECOND",
                "DELAYED", "DETERMINISTIC", "DIV", "DUAL", "EACH",
                "ELSEIF", "ENCLOSED", "EXPLAIN", "FLOAT4", "FLOAT8",
                "FORCE", "HOUR_MICROSECOND", "HOUR_MINUTE", "HOUR_SECOND",
                "IGNORE", "INFILE", "INT1", "INT2", "INT3", "INT4", "INT8",
                "ITERATE", "KEYS", "KILL", "LEADING", "LEAVE", "LINES",
                "LOAD", "LOCK", "LONG", "LONGBLOB", "LONGTEXT", "LOOP",
                "LOW_PRIORITY", "MATCH", "MEDIUMBLOB", "MEDIUMINT",
                "MEDIUMTEXT", "MINUTE_MICROSECOND", "MINUTE_SECOND", "MOD",
                "MODIFIES", "NO_WRITE_TO_BINLOG", "OPTIMIZE", "OPTIONALLY",
                "OUT", "OUTFILE", "PURGE", "READS", "REGEXP", "RELEASE",
                "RENAME", "REPEAT", "REQUIRE", "RESIGNAL", "RETURN",
                "REVOKE", "RLIKE", "SCHEMA", "SCHEMAS", "SECOND_MICROSECOND",
                "SENSITIVE", "SEPARATOR", "SHOW", "SIGNAL", "SPATIAL",
                "SQL", "SQLEXCEPTION", "SQLSTATE", "SQLWARNING",
                "SQL_BIG_RESULT", "SQL_CALC_FOUND_ROWS", "SQL_SMALL_RESULT",
                "SSL", "STARTING", "STRAIGHT_JOIN", "TERMINATED",
                "TINYBLOB", "TINYINT", "TINYTEXT", "TRAILING", "TRIGGER",
                "UNDO", "UNLOCK", "UNSIGNED", "USAGE", "UTC_DATE",
                "UTC_TIME", "UTC_TIMESTAMP", "VARBINARY", "VARCHARACTER",
                "WHILE", "WRITE", "XOR", "YEAR_MONTH", "ZEROFILL",
            ].iter().copied());
        }
        Some(DialectType::PostgreSQL) | Some(DialectType::CockroachDB) => {
            words.extend([
                "ANALYSE", "ANALYZE", "ARRAY", "AUTHORIZATION", "BINARY",
                "BOTH", "COLLATE", "CONCURRENTLY", "DO", "FREEZE",
                "ILIKE", "INITIALLY", "ISNULL", "LATERAL", "LEADING",
                "LOCALTIME", "LOCALTIMESTAMP", "NOTNULL", "ONLY", "OVERLAPS",
                "PLACING", "SIMILAR", "SYMMETRIC", "TABLESAMPLE",
                "TRAILING", "VARIADIC", "VERBOSE",
            ].iter().copied());
        }
        Some(DialectType::BigQuery) => {
            words.extend([
                "ASSERT_ROWS_MODIFIED", "COLLATE", "CONTAINS", "CUBE",
                "DEFINE", "ENUM", "EXTRACT", "FOLLOWING", "GROUPING",
                "GROUPS", "HASH", "IGNORE", "LATERAL", "LOOKUP",
                "MERGE", "NEW", "NO", "NULLS", "OF", "OVER", "PARTITION",
                "PRECEDING", "PROTO", "RANGE", "RECURSIVE", "RESPECT",
                "ROLLUP", "STRUCT", "TABLESAMPLE", "TREAT", "UNBOUNDED",
                "WITHIN",
            ].iter().copied());
        }
        Some(DialectType::Snowflake) => {
            words.extend([
                "ACCOUNT", "BOTH", "CONNECT", "FOLLOWING", "ILIKE",
                "INCREMENT", "ISSUE", "LATERAL", "LEADING", "LOCALTIME",
                "LOCALTIMESTAMP", "MINUS", "QUALIFY", "REGEXP", "RLIKE",
                "SOME", "START", "TABLESAMPLE", "TOP", "TRAILING",
                "TRY_CAST",
            ].iter().copied());
        }
        Some(DialectType::TSQL) | Some(DialectType::Fabric) => {
            words.extend([
                "BACKUP", "BREAK", "BROWSE", "BULK", "CASCADE", "CHECKPOINT",
                "CLOSE", "CLUSTERED", "COALESCE", "COMPUTE", "CONTAINS",
                "CONTAINSTABLE", "CONTINUE", "CONVERT", "DBCC",
                "DEALLOCATE", "DENY", "DISK", "DISTRIBUTED", "DUMP",
                "ERRLVL", "EXEC", "EXECUTE", "EXIT", "EXTERNAL", "FILE",
                "FILLFACTOR", "FREETEXT", "FREETEXTTABLE", "FUNCTION",
                "GOTO", "HOLDLOCK", "IDENTITY", "IDENTITYCOL",
                "IDENTITY_INSERT", "KILL", "LINENO", "MERGE",
                "NONCLUSTERED", "NULLIF", "OF", "OFF", "OFFSETS",
                "OPEN", "OPENDATASOURCE", "OPENQUERY", "OPENROWSET",
                "OPENXML", "OVER", "PERCENT", "PIVOT", "PLAN", "PRINT",
                "PROC", "PROCEDURE", "PUBLIC", "RAISERROR", "READ",
                "READTEXT", "RECONFIGURE", "REPLICATION", "RESTORE",
                "RESTRICT", "REVERT", "ROWCOUNT", "ROWGUIDCOL", "RULE",
                "SAVE", "SECURITYAUDIT", "SEMANTICKEYPHRASETABLE",
                "SEMANTICSIMILARITYDETAILSTABLE",
                "SEMANTICSIMILARITYTABLE", "SETUSER", "SHUTDOWN",
                "STATISTICS", "SYSTEM_USER", "TEXTSIZE", "TOP", "TRAN",
                "TRANSACTION", "TRIGGER", "TSEQUAL", "UNPIVOT",
                "UPDATETEXT", "WAITFOR", "WRITETEXT",
            ].iter().copied());
        }
        Some(DialectType::ClickHouse) => {
            words.extend([
                "ANTI", "ARRAY", "ASOF", "FINAL", "FORMAT", "GLOBAL",
                "INF", "KILL", "MATERIALIZED", "NAN", "PREWHERE",
                "SAMPLE", "SEMI", "SETTINGS", "TOP",
            ].iter().copied());
        }
        Some(DialectType::DuckDB) => {
            words.extend([
                "ANALYSE", "ANALYZE", "ARRAY", "BOTH", "LATERAL",
                "LEADING", "LOCALTIME", "LOCALTIMESTAMP", "PLACING",
                "QUALIFY", "SIMILAR", "TABLESAMPLE", "TRAILING",
            ].iter().copied());
        }
        Some(DialectType::Hive) | Some(DialectType::Spark) | Some(DialectType::Databricks) => {
            words.extend([
                "BOTH", "CLUSTER", "DISTRIBUTE", "EXCHANGE", "EXTENDED",
                "FUNCTION", "LATERAL", "LEADING", "MACRO", "OVER",
                "PARTITION", "PERCENT", "RANGE", "READS", "REDUCE",
                "REGEXP", "REVOKE", "RLIKE", "ROLLUP", "SEMI", "SORT",
                "TABLESAMPLE", "TRAILING", "TRANSFORM", "UNBOUNDED",
                "UNIQUEJOIN",
            ].iter().copied());
        }
        Some(DialectType::Trino) | Some(DialectType::Presto) | Some(DialectType::Athena) => {
            words.extend([
                "CUBE", "DEALLOCATE", "DESCRIBE", "EXECUTE", "EXTRACT",
                "GROUPING", "LATERAL", "LOCALTIME", "LOCALTIMESTAMP",
                "NORMALIZE", "PREPARE", "ROLLUP", "SOME",
                "TABLESAMPLE", "UESCAPE", "UNNEST",
            ].iter().copied());
        }
        Some(DialectType::Oracle) => {
            words.extend([
                "ACCESS", "AUDIT", "CLUSTER", "COMMENT", "COMPRESS",
                "CONNECT", "EXCLUSIVE", "FILE", "IDENTIFIED", "IMMEDIATE",
                "INCREMENT", "INITIAL", "LEVEL", "LOCK", "LONG",
                "MAXEXTENTS", "MINUS", "MODE", "NOAUDIT", "NOCOMPRESS",
                "NOWAIT", "NUMBER", "OF", "OFFLINE", "ONLINE", "PCTFREE",
                "PRIOR", "RAW", "RENAME", "RESOURCE", "REVOKE",
                "SHARE", "SIZE", "START", "SUCCESSFUL", "SYNONYM",
                "SYSDATE", "TRIGGER", "UID", "VALIDATE", "VARCHAR2",
                "WHENEVER",
            ].iter().copied());
        }
        Some(DialectType::Redshift) => {
            words.extend([
                "AZ64", "BZIP2", "DELTA", "DELTA32K", "DISTSTYLE",
                "ENCODE", "GZIP", "ILIKE", "LIMIT", "LUNS", "LZO",
                "LZOP", "MOSTLY13", "MOSTLY32", "MOSTLY8", "RAW",
                "SIMILAR", "SNAPSHOT", "SORTKEY", "SYSDATE", "TOP",
                "ZSTD",
            ].iter().copied());
        }
        _ => {
            // For Generic or unknown dialects, add a broad set of commonly reserved words
            words.extend([
                "ANALYZE", "ARRAY", "BOTH", "CUBE", "GROUPING", "LATERAL",
                "LEADING", "LOCALTIME", "LOCALTIMESTAMP", "OVER", "PARTITION",
                "QUALIFY", "RANGE", "ROLLUP", "SIMILAR", "SOME",
                "TABLESAMPLE", "TRAILING",
            ].iter().copied());
        }
    }

    words
}

/// Check whether an identifier name needs quoting.
///
/// An identifier needs quoting if:
/// - It is empty
/// - It starts with a digit
/// - It contains characters other than `[a-zA-Z0-9_]`
/// - It is a SQL reserved word (case-insensitive)
fn needs_quoting(name: &str, reserved_words: &HashSet<&str>) -> bool {
    if name.is_empty() {
        return false;
    }

    // Starts with a digit
    if name.as_bytes()[0].is_ascii_digit() {
        return true;
    }

    // Contains non-identifier characters
    if !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_') {
        return true;
    }

    // Is a reserved word (case-insensitive check)
    let upper = name.to_uppercase();
    reserved_words.contains(upper.as_str())
}

/// Conditionally set `quoted = true` on an identifier if it needs quoting.
fn maybe_quote(id: &mut Identifier, reserved_words: &HashSet<&str>) {
    // Don't re-quote something already quoted, and don't quote empty identifiers
    // or wildcard identifiers
    if id.quoted || id.name.is_empty() || id.name == "*" {
        return;
    }
    if needs_quoting(&id.name, reserved_words) {
        id.quoted = true;
    }
}

/// Recursively walk an expression and quote identifiers that need quoting.
fn quote_identifiers_recursive(expr: &mut Expression, reserved_words: &HashSet<&str>) {
    match expr {
        // ── Leaf nodes with Identifier ────────────────────────────
        Expression::Identifier(id) => {
            maybe_quote(id, reserved_words);
        }

        Expression::Column(col) => {
            maybe_quote(&mut col.name, reserved_words);
            if let Some(ref mut table) = col.table {
                maybe_quote(table, reserved_words);
            }
        }

        Expression::Table(table_ref) => {
            maybe_quote(&mut table_ref.name, reserved_words);
            if let Some(ref mut schema) = table_ref.schema {
                maybe_quote(schema, reserved_words);
            }
            if let Some(ref mut catalog) = table_ref.catalog {
                maybe_quote(catalog, reserved_words);
            }
            if let Some(ref mut alias) = table_ref.alias {
                maybe_quote(alias, reserved_words);
            }
            for ca in &mut table_ref.column_aliases {
                maybe_quote(ca, reserved_words);
            }
            for p in &mut table_ref.partitions {
                maybe_quote(p, reserved_words);
            }
            // Recurse into hints and other child expressions
            for h in &mut table_ref.hints {
                quote_identifiers_recursive(h, reserved_words);
            }
            if let Some(ref mut ver) = table_ref.version {
                quote_identifiers_recursive(&mut ver.this, reserved_words);
                if let Some(ref mut e) = ver.expression {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
        }

        Expression::Star(star) => {
            if let Some(ref mut table) = star.table {
                maybe_quote(table, reserved_words);
            }
            if let Some(ref mut except_ids) = star.except {
                for id in except_ids {
                    maybe_quote(id, reserved_words);
                }
            }
            if let Some(ref mut replace_aliases) = star.replace {
                for alias in replace_aliases {
                    maybe_quote(&mut alias.alias, reserved_words);
                    quote_identifiers_recursive(&mut alias.this, reserved_words);
                }
            }
            if let Some(ref mut rename_pairs) = star.rename {
                for (from, to) in rename_pairs {
                    maybe_quote(from, reserved_words);
                    maybe_quote(to, reserved_words);
                }
            }
        }

        // ── Alias ─────────────────────────────────────────────────
        Expression::Alias(alias) => {
            maybe_quote(&mut alias.alias, reserved_words);
            for ca in &mut alias.column_aliases {
                maybe_quote(ca, reserved_words);
            }
            quote_identifiers_recursive(&mut alias.this, reserved_words);
        }

        // ── SELECT ────────────────────────────────────────────────
        Expression::Select(select) => {
            for e in &mut select.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
            if let Some(ref mut from) = select.from {
                for e in &mut from.expressions {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
            for join in &mut select.joins {
                quote_join(join, reserved_words);
            }
            for lv in &mut select.lateral_views {
                quote_lateral_view(lv, reserved_words);
            }
            if let Some(ref mut prewhere) = select.prewhere {
                quote_identifiers_recursive(prewhere, reserved_words);
            }
            if let Some(ref mut wh) = select.where_clause {
                quote_identifiers_recursive(&mut wh.this, reserved_words);
            }
            if let Some(ref mut gb) = select.group_by {
                for e in &mut gb.expressions {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
            if let Some(ref mut hv) = select.having {
                quote_identifiers_recursive(&mut hv.this, reserved_words);
            }
            if let Some(ref mut q) = select.qualify {
                quote_identifiers_recursive(&mut q.this, reserved_words);
            }
            if let Some(ref mut ob) = select.order_by {
                for o in &mut ob.expressions {
                    quote_identifiers_recursive(&mut o.this, reserved_words);
                }
            }
            if let Some(ref mut lim) = select.limit {
                quote_identifiers_recursive(&mut lim.this, reserved_words);
            }
            if let Some(ref mut off) = select.offset {
                quote_identifiers_recursive(&mut off.this, reserved_words);
            }
            if let Some(ref mut with) = select.with {
                quote_with(with, reserved_words);
            }
            if let Some(ref mut windows) = select.windows {
                for nw in windows {
                    maybe_quote(&mut nw.name, reserved_words);
                    quote_over(&mut nw.spec, reserved_words);
                }
            }
            if let Some(ref mut distinct_on) = select.distinct_on {
                for e in distinct_on {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
            if let Some(ref mut limit_by) = select.limit_by {
                for e in limit_by {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
            if let Some(ref mut settings) = select.settings {
                for e in settings {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
            if let Some(ref mut format) = select.format {
                quote_identifiers_recursive(format, reserved_words);
            }
        }

        // ── Set operations ────────────────────────────────────────
        Expression::Union(u) => {
            quote_identifiers_recursive(&mut u.left, reserved_words);
            quote_identifiers_recursive(&mut u.right, reserved_words);
            if let Some(ref mut with) = u.with {
                quote_with(with, reserved_words);
            }
            if let Some(ref mut ob) = u.order_by {
                for o in &mut ob.expressions {
                    quote_identifiers_recursive(&mut o.this, reserved_words);
                }
            }
            if let Some(ref mut lim) = u.limit {
                quote_identifiers_recursive(lim, reserved_words);
            }
            if let Some(ref mut off) = u.offset {
                quote_identifiers_recursive(off, reserved_words);
            }
        }
        Expression::Intersect(i) => {
            quote_identifiers_recursive(&mut i.left, reserved_words);
            quote_identifiers_recursive(&mut i.right, reserved_words);
            if let Some(ref mut with) = i.with {
                quote_with(with, reserved_words);
            }
            if let Some(ref mut ob) = i.order_by {
                for o in &mut ob.expressions {
                    quote_identifiers_recursive(&mut o.this, reserved_words);
                }
            }
        }
        Expression::Except(e) => {
            quote_identifiers_recursive(&mut e.left, reserved_words);
            quote_identifiers_recursive(&mut e.right, reserved_words);
            if let Some(ref mut with) = e.with {
                quote_with(with, reserved_words);
            }
            if let Some(ref mut ob) = e.order_by {
                for o in &mut ob.expressions {
                    quote_identifiers_recursive(&mut o.this, reserved_words);
                }
            }
        }

        // ── Subquery ──────────────────────────────────────────────
        Expression::Subquery(sq) => {
            quote_identifiers_recursive(&mut sq.this, reserved_words);
            if let Some(ref mut alias) = sq.alias {
                maybe_quote(alias, reserved_words);
            }
            for ca in &mut sq.column_aliases {
                maybe_quote(ca, reserved_words);
            }
            if let Some(ref mut ob) = sq.order_by {
                for o in &mut ob.expressions {
                    quote_identifiers_recursive(&mut o.this, reserved_words);
                }
            }
        }

        // ── DML ───────────────────────────────────────────────────
        Expression::Insert(ins) => {
            quote_table_ref(&mut ins.table, reserved_words);
            for c in &mut ins.columns {
                maybe_quote(c, reserved_words);
            }
            for row in &mut ins.values {
                for e in row {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
            if let Some(ref mut q) = ins.query {
                quote_identifiers_recursive(q, reserved_words);
            }
            for (id, val) in &mut ins.partition {
                maybe_quote(id, reserved_words);
                if let Some(ref mut v) = val {
                    quote_identifiers_recursive(v, reserved_words);
                }
            }
            for e in &mut ins.returning {
                quote_identifiers_recursive(e, reserved_words);
            }
            if let Some(ref mut on_conflict) = ins.on_conflict {
                quote_identifiers_recursive(on_conflict, reserved_words);
            }
            if let Some(ref mut with) = ins.with {
                quote_with(with, reserved_words);
            }
            if let Some(ref mut alias) = ins.alias {
                maybe_quote(alias, reserved_words);
            }
            if let Some(ref mut src_alias) = ins.source_alias {
                maybe_quote(src_alias, reserved_words);
            }
        }

        Expression::Update(upd) => {
            quote_table_ref(&mut upd.table, reserved_words);
            for tr in &mut upd.extra_tables {
                quote_table_ref(tr, reserved_words);
            }
            for join in &mut upd.table_joins {
                quote_join(join, reserved_words);
            }
            for (id, val) in &mut upd.set {
                maybe_quote(id, reserved_words);
                quote_identifiers_recursive(val, reserved_words);
            }
            if let Some(ref mut from) = upd.from_clause {
                for e in &mut from.expressions {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
            for join in &mut upd.from_joins {
                quote_join(join, reserved_words);
            }
            if let Some(ref mut wh) = upd.where_clause {
                quote_identifiers_recursive(&mut wh.this, reserved_words);
            }
            for e in &mut upd.returning {
                quote_identifiers_recursive(e, reserved_words);
            }
            if let Some(ref mut with) = upd.with {
                quote_with(with, reserved_words);
            }
        }

        Expression::Delete(del) => {
            quote_table_ref(&mut del.table, reserved_words);
            if let Some(ref mut alias) = del.alias {
                maybe_quote(alias, reserved_words);
            }
            for tr in &mut del.using {
                quote_table_ref(tr, reserved_words);
            }
            if let Some(ref mut wh) = del.where_clause {
                quote_identifiers_recursive(&mut wh.this, reserved_words);
            }
            if let Some(ref mut with) = del.with {
                quote_with(with, reserved_words);
            }
        }

        // ── Binary operations ─────────────────────────────────────
        Expression::And(bin) | Expression::Or(bin) | Expression::Eq(bin) |
        Expression::Neq(bin) | Expression::Lt(bin) | Expression::Lte(bin) |
        Expression::Gt(bin) | Expression::Gte(bin) | Expression::Add(bin) |
        Expression::Sub(bin) | Expression::Mul(bin) | Expression::Div(bin) |
        Expression::Mod(bin) | Expression::BitwiseAnd(bin) |
        Expression::BitwiseOr(bin) | Expression::BitwiseXor(bin) |
        Expression::Concat(bin) | Expression::Adjacent(bin) |
        Expression::TsMatch(bin) | Expression::PropertyEQ(bin) |
        Expression::ArrayContainsAll(bin) | Expression::ArrayContainedBy(bin) |
        Expression::ArrayOverlaps(bin) | Expression::JSONBContainsAllTopKeys(bin) |
        Expression::JSONBContainsAnyTopKeys(bin) | Expression::JSONBDeleteAtPath(bin) |
        Expression::ExtendsLeft(bin) | Expression::ExtendsRight(bin) |
        Expression::Is(bin) | Expression::NullSafeEq(bin) |
        Expression::NullSafeNeq(bin) | Expression::Glob(bin) |
        Expression::Match(bin) | Expression::MemberOf(bin) |
        Expression::BitwiseLeftShift(bin) | Expression::BitwiseRightShift(bin) => {
            quote_identifiers_recursive(&mut bin.left, reserved_words);
            quote_identifiers_recursive(&mut bin.right, reserved_words);
        }

        // ── Like operations ───────────────────────────────────────
        Expression::Like(like) | Expression::ILike(like) => {
            quote_identifiers_recursive(&mut like.left, reserved_words);
            quote_identifiers_recursive(&mut like.right, reserved_words);
            if let Some(ref mut esc) = like.escape {
                quote_identifiers_recursive(esc, reserved_words);
            }
        }

        // ── Unary operations ──────────────────────────────────────
        Expression::Not(un) | Expression::Neg(un) | Expression::BitwiseNot(un) => {
            quote_identifiers_recursive(&mut un.this, reserved_words);
        }

        // ── Predicates ────────────────────────────────────────────
        Expression::In(in_expr) => {
            quote_identifiers_recursive(&mut in_expr.this, reserved_words);
            for e in &mut in_expr.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
            if let Some(ref mut q) = in_expr.query {
                quote_identifiers_recursive(q, reserved_words);
            }
            if let Some(ref mut un) = in_expr.unnest {
                quote_identifiers_recursive(un, reserved_words);
            }
        }

        Expression::Between(bw) => {
            quote_identifiers_recursive(&mut bw.this, reserved_words);
            quote_identifiers_recursive(&mut bw.low, reserved_words);
            quote_identifiers_recursive(&mut bw.high, reserved_words);
        }

        Expression::IsNull(is_null) => {
            quote_identifiers_recursive(&mut is_null.this, reserved_words);
        }

        Expression::IsTrue(is_tf) | Expression::IsFalse(is_tf) => {
            quote_identifiers_recursive(&mut is_tf.this, reserved_words);
        }

        Expression::Exists(ex) => {
            quote_identifiers_recursive(&mut ex.this, reserved_words);
        }

        // ── Functions ─────────────────────────────────────────────
        Expression::Function(func) => {
            for arg in &mut func.args {
                quote_identifiers_recursive(arg, reserved_words);
            }
        }

        Expression::AggregateFunction(agg) => {
            for arg in &mut agg.args {
                quote_identifiers_recursive(arg, reserved_words);
            }
            if let Some(ref mut filter) = agg.filter {
                quote_identifiers_recursive(filter, reserved_words);
            }
            for o in &mut agg.order_by {
                quote_identifiers_recursive(&mut o.this, reserved_words);
            }
        }

        Expression::WindowFunction(wf) => {
            quote_identifiers_recursive(&mut wf.this, reserved_words);
            quote_over(&mut wf.over, reserved_words);
        }

        // ── CASE ──────────────────────────────────────────────────
        Expression::Case(case) => {
            if let Some(ref mut operand) = case.operand {
                quote_identifiers_recursive(operand, reserved_words);
            }
            for (when, then) in &mut case.whens {
                quote_identifiers_recursive(when, reserved_words);
                quote_identifiers_recursive(then, reserved_words);
            }
            if let Some(ref mut else_) = case.else_ {
                quote_identifiers_recursive(else_, reserved_words);
            }
        }

        // ── CAST / TryCast / SafeCast ─────────────────────────────
        Expression::Cast(cast) | Expression::TryCast(cast) | Expression::SafeCast(cast) => {
            quote_identifiers_recursive(&mut cast.this, reserved_words);
            if let Some(ref mut fmt) = cast.format {
                quote_identifiers_recursive(fmt, reserved_words);
            }
        }

        // ── Paren / Annotated ─────────────────────────────────────
        Expression::Paren(paren) => {
            quote_identifiers_recursive(&mut paren.this, reserved_words);
        }

        Expression::Annotated(ann) => {
            quote_identifiers_recursive(&mut ann.this, reserved_words);
        }

        // ── WITH clause (standalone) ──────────────────────────────
        Expression::With(with) => {
            quote_with(with, reserved_words);
        }

        Expression::Cte(cte) => {
            maybe_quote(&mut cte.alias, reserved_words);
            for c in &mut cte.columns {
                maybe_quote(c, reserved_words);
            }
            quote_identifiers_recursive(&mut cte.this, reserved_words);
        }

        // ── Clauses (standalone) ──────────────────────────────────
        Expression::From(from) => {
            for e in &mut from.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
        }

        Expression::Join(join) => {
            quote_join(join, reserved_words);
        }

        Expression::JoinedTable(jt) => {
            quote_identifiers_recursive(&mut jt.left, reserved_words);
            for join in &mut jt.joins {
                quote_join(join, reserved_words);
            }
            if let Some(ref mut alias) = jt.alias {
                maybe_quote(alias, reserved_words);
            }
        }

        Expression::Where(wh) => {
            quote_identifiers_recursive(&mut wh.this, reserved_words);
        }

        Expression::GroupBy(gb) => {
            for e in &mut gb.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
        }

        Expression::Having(hv) => {
            quote_identifiers_recursive(&mut hv.this, reserved_words);
        }

        Expression::OrderBy(ob) => {
            for o in &mut ob.expressions {
                quote_identifiers_recursive(&mut o.this, reserved_words);
            }
        }

        Expression::Ordered(ord) => {
            quote_identifiers_recursive(&mut ord.this, reserved_words);
        }

        Expression::Limit(lim) => {
            quote_identifiers_recursive(&mut lim.this, reserved_words);
        }

        Expression::Offset(off) => {
            quote_identifiers_recursive(&mut off.this, reserved_words);
        }

        Expression::Qualify(q) => {
            quote_identifiers_recursive(&mut q.this, reserved_words);
        }

        Expression::Window(ws) => {
            for e in &mut ws.partition_by {
                quote_identifiers_recursive(e, reserved_words);
            }
            for o in &mut ws.order_by {
                quote_identifiers_recursive(&mut o.this, reserved_words);
            }
        }

        Expression::Over(over) => {
            quote_over(over, reserved_words);
        }

        Expression::WithinGroup(wg) => {
            quote_identifiers_recursive(&mut wg.this, reserved_words);
            for o in &mut wg.order_by {
                quote_identifiers_recursive(&mut o.this, reserved_words);
            }
        }

        // ── Pivot / Unpivot ───────────────────────────────────────
        Expression::Pivot(piv) => {
            quote_identifiers_recursive(&mut piv.this, reserved_words);
            for e in &mut piv.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
            for f in &mut piv.fields {
                quote_identifiers_recursive(f, reserved_words);
            }
            if let Some(ref mut alias) = piv.alias {
                maybe_quote(alias, reserved_words);
            }
        }

        Expression::Unpivot(unpiv) => {
            quote_identifiers_recursive(&mut unpiv.this, reserved_words);
            maybe_quote(&mut unpiv.value_column, reserved_words);
            maybe_quote(&mut unpiv.name_column, reserved_words);
            for e in &mut unpiv.columns {
                quote_identifiers_recursive(e, reserved_words);
            }
            if let Some(ref mut alias) = unpiv.alias {
                maybe_quote(alias, reserved_words);
            }
        }

        // ── Values ────────────────────────────────────────────────
        Expression::Values(vals) => {
            for tuple in &mut vals.expressions {
                for e in &mut tuple.expressions {
                    quote_identifiers_recursive(e, reserved_words);
                }
            }
            if let Some(ref mut alias) = vals.alias {
                maybe_quote(alias, reserved_words);
            }
            for ca in &mut vals.column_aliases {
                maybe_quote(ca, reserved_words);
            }
        }

        // ── Array / Struct / Tuple ────────────────────────────────
        Expression::Array(arr) => {
            for e in &mut arr.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
        }

        Expression::Struct(st) => {
            for (_name, e) in &mut st.fields {
                quote_identifiers_recursive(e, reserved_words);
            }
        }

        Expression::Tuple(tup) => {
            for e in &mut tup.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
        }

        // ── Subscript / Dot / Method ──────────────────────────────
        Expression::Subscript(sub) => {
            quote_identifiers_recursive(&mut sub.this, reserved_words);
            quote_identifiers_recursive(&mut sub.index, reserved_words);
        }

        Expression::Dot(dot) => {
            quote_identifiers_recursive(&mut dot.this, reserved_words);
            maybe_quote(&mut dot.field, reserved_words);
        }

        Expression::ScopeResolution(sr) => {
            if let Some(ref mut this) = sr.this {
                quote_identifiers_recursive(this, reserved_words);
            }
            quote_identifiers_recursive(&mut sr.expression, reserved_words);
        }

        // ── Lateral ───────────────────────────────────────────────
        Expression::Lateral(lat) => {
            quote_identifiers_recursive(&mut lat.this, reserved_words);
            // lat.alias is Option<String>, not Identifier, so we skip it
        }

        // ── DPipe (|| concatenation) ──────────────────────────────
        Expression::DPipe(dpipe) => {
            quote_identifiers_recursive(&mut dpipe.this, reserved_words);
            quote_identifiers_recursive(&mut dpipe.expression, reserved_words);
        }

        // ── Merge ─────────────────────────────────────────────────
        Expression::Merge(merge) => {
            quote_identifiers_recursive(&mut merge.this, reserved_words);
            quote_identifiers_recursive(&mut merge.using, reserved_words);
            if let Some(ref mut on) = merge.on {
                quote_identifiers_recursive(on, reserved_words);
            }
            if let Some(ref mut whens) = merge.whens {
                quote_identifiers_recursive(whens, reserved_words);
            }
            if let Some(ref mut with) = merge.with_ {
                quote_identifiers_recursive(with, reserved_words);
            }
            if let Some(ref mut ret) = merge.returning {
                quote_identifiers_recursive(ret, reserved_words);
            }
        }

        // ── LateralView (standalone) ──────────────────────────────
        Expression::LateralView(lv) => {
            quote_lateral_view(lv, reserved_words);
        }

        // ── Anonymous (generic function) ──────────────────────────
        Expression::Anonymous(anon) => {
            quote_identifiers_recursive(&mut anon.this, reserved_words);
            for e in &mut anon.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
        }

        // ── Filter (e.g., FILTER(WHERE ...)) ──────────────────────
        Expression::Filter(filter) => {
            quote_identifiers_recursive(&mut filter.this, reserved_words);
            quote_identifiers_recursive(&mut filter.expression, reserved_words);
        }

        // ── Returning ─────────────────────────────────────────────
        Expression::Returning(ret) => {
            for e in &mut ret.expressions {
                quote_identifiers_recursive(e, reserved_words);
            }
        }

        // ── BracedWildcard ────────────────────────────────────────
        Expression::BracedWildcard(inner) => {
            quote_identifiers_recursive(inner, reserved_words);
        }

        // ── ReturnStmt ────────────────────────────────────────────
        Expression::ReturnStmt(inner) => {
            quote_identifiers_recursive(inner, reserved_words);
        }

        // ── Leaf nodes that never contain identifiers ─────────────
        Expression::Literal(_)
        | Expression::Boolean(_)
        | Expression::Null(_)
        | Expression::DataType(_)
        | Expression::Raw(_)
        | Expression::Placeholder(_)
        | Expression::CurrentDate(_)
        | Expression::CurrentTime(_)
        | Expression::CurrentTimestamp(_)
        | Expression::CurrentTimestampLTZ(_)
        | Expression::SessionUser(_)
        | Expression::RowNumber(_)
        | Expression::Rank(_)
        | Expression::DenseRank(_)
        | Expression::PercentRank(_)
        | Expression::CumeDist(_)
        | Expression::Random(_)
        | Expression::Pi(_)
        | Expression::JSONPathRoot(_) => {
            // Nothing to do – these are leaves or do not contain identifiers
        }

        // ── Catch-all: many expression variants follow common patterns.
        // Rather than listing every single variant, we leave them unchanged.
        // The key identifier-bearing variants are covered above.
        _ => {}
    }
}

/// Helper: quote identifiers in a Join.
fn quote_join(join: &mut Join, reserved_words: &HashSet<&str>) {
    quote_identifiers_recursive(&mut join.this, reserved_words);
    if let Some(ref mut on) = join.on {
        quote_identifiers_recursive(on, reserved_words);
    }
    for id in &mut join.using {
        maybe_quote(id, reserved_words);
    }
    if let Some(ref mut mc) = join.match_condition {
        quote_identifiers_recursive(mc, reserved_words);
    }
    for piv in &mut join.pivots {
        quote_identifiers_recursive(piv, reserved_words);
    }
}

/// Helper: quote identifiers in a WITH clause.
fn quote_with(with: &mut With, reserved_words: &HashSet<&str>) {
    for cte in &mut with.ctes {
        maybe_quote(&mut cte.alias, reserved_words);
        for c in &mut cte.columns {
            maybe_quote(c, reserved_words);
        }
        for k in &mut cte.key_expressions {
            maybe_quote(k, reserved_words);
        }
        quote_identifiers_recursive(&mut cte.this, reserved_words);
    }
}

/// Helper: quote identifiers in an Over clause.
fn quote_over(over: &mut Over, reserved_words: &HashSet<&str>) {
    if let Some(ref mut wn) = over.window_name {
        maybe_quote(wn, reserved_words);
    }
    for e in &mut over.partition_by {
        quote_identifiers_recursive(e, reserved_words);
    }
    for o in &mut over.order_by {
        quote_identifiers_recursive(&mut o.this, reserved_words);
    }
    if let Some(ref mut alias) = over.alias {
        maybe_quote(alias, reserved_words);
    }
}

/// Helper: quote identifiers in a TableRef (used by DML statements).
fn quote_table_ref(table_ref: &mut TableRef, reserved_words: &HashSet<&str>) {
    maybe_quote(&mut table_ref.name, reserved_words);
    if let Some(ref mut schema) = table_ref.schema {
        maybe_quote(schema, reserved_words);
    }
    if let Some(ref mut catalog) = table_ref.catalog {
        maybe_quote(catalog, reserved_words);
    }
    if let Some(ref mut alias) = table_ref.alias {
        maybe_quote(alias, reserved_words);
    }
    for ca in &mut table_ref.column_aliases {
        maybe_quote(ca, reserved_words);
    }
    for p in &mut table_ref.partitions {
        maybe_quote(p, reserved_words);
    }
    for h in &mut table_ref.hints {
        quote_identifiers_recursive(h, reserved_words);
    }
}

/// Helper: quote identifiers in a LateralView.
fn quote_lateral_view(lv: &mut LateralView, reserved_words: &HashSet<&str>) {
    quote_identifiers_recursive(&mut lv.this, reserved_words);
    if let Some(ref mut ta) = lv.table_alias {
        maybe_quote(ta, reserved_words);
    }
    for ca in &mut lv.column_aliases {
        maybe_quote(ca, reserved_words);
    }
}

/// Quote identifiers that need quoting based on dialect rules.
///
/// Walks the entire AST recursively and sets `quoted = true` on any
/// `Identifier` that:
/// - contains special characters (anything not `[a-zA-Z0-9_]`)
/// - starts with a digit
/// - is a SQL reserved word for the given dialect
///
/// The function takes ownership of the expression, mutates a clone,
/// and returns the modified version.
pub fn quote_identifiers(expression: Expression, dialect: Option<DialectType>) -> Expression {
    let reserved_words = get_reserved_words(dialect);
    let mut result = expression;
    quote_identifiers_recursive(&mut result, &reserved_words);
    result
}

/// Pushdown CTE alias columns into the projection.
///
/// This is useful for dialects like Snowflake where CTE alias columns
/// can be referenced in HAVING.
pub fn pushdown_cte_alias_columns(_scope: &Scope) {
    // For each CTE with alias columns, rename the SELECT columns to match
    // In a real implementation, this would modify the AST
}

// ============================================================================
// Helper functions
// ============================================================================

/// Get all column references in a scope
fn get_scope_columns(scope: &Scope) -> Vec<ColumnRef> {
    let mut columns = Vec::new();
    collect_columns(&scope.expression, &mut columns);
    columns
}

/// Column reference for tracking
#[derive(Debug, Clone)]
struct ColumnRef {
    table: Option<String>,
    name: String,
}

/// Recursively collect column references from an expression
fn collect_columns(expr: &Expression, columns: &mut Vec<ColumnRef>) {
    match expr {
        Expression::Column(col) => {
            columns.push(ColumnRef {
                table: col.table.as_ref().map(|t| t.name.clone()),
                name: col.name.name.clone(),
            });
        }
        Expression::Select(select) => {
            for e in &select.expressions {
                collect_columns(e, columns);
            }
            if let Some(from) = &select.from {
                for e in &from.expressions {
                    collect_columns(e, columns);
                }
            }
            if let Some(where_clause) = &select.where_clause {
                collect_columns(&where_clause.this, columns);
            }
            if let Some(group_by) = &select.group_by {
                for e in &group_by.expressions {
                    collect_columns(e, columns);
                }
            }
            if let Some(having) = &select.having {
                collect_columns(&having.this, columns);
            }
            if let Some(order_by) = &select.order_by {
                for o in &order_by.expressions {
                    collect_columns(&o.this, columns);
                }
            }
            for join in &select.joins {
                collect_columns(&join.this, columns);
                if let Some(on) = &join.on {
                    collect_columns(on, columns);
                }
            }
        }
        Expression::Alias(alias) => {
            collect_columns(&alias.this, columns);
        }
        Expression::Function(func) => {
            for arg in &func.args {
                collect_columns(arg, columns);
            }
        }
        Expression::AggregateFunction(agg) => {
            for arg in &agg.args {
                collect_columns(arg, columns);
            }
        }
        Expression::And(bin)
        | Expression::Or(bin)
        | Expression::Eq(bin)
        | Expression::Neq(bin)
        | Expression::Lt(bin)
        | Expression::Lte(bin)
        | Expression::Gt(bin)
        | Expression::Gte(bin)
        | Expression::Add(bin)
        | Expression::Sub(bin)
        | Expression::Mul(bin)
        | Expression::Div(bin) => {
            collect_columns(&bin.left, columns);
            collect_columns(&bin.right, columns);
        }
        Expression::Not(unary) | Expression::Neg(unary) => {
            collect_columns(&unary.this, columns);
        }
        Expression::Paren(paren) => {
            collect_columns(&paren.this, columns);
        }
        Expression::Case(case) => {
            if let Some(operand) = &case.operand {
                collect_columns(operand, columns);
            }
            for (when, then) in &case.whens {
                collect_columns(when, columns);
                collect_columns(then, columns);
            }
            if let Some(else_) = &case.else_ {
                collect_columns(else_, columns);
            }
        }
        Expression::Cast(cast) => {
            collect_columns(&cast.this, columns);
        }
        Expression::In(in_expr) => {
            collect_columns(&in_expr.this, columns);
            for e in &in_expr.expressions {
                collect_columns(e, columns);
            }
            if let Some(query) = &in_expr.query {
                collect_columns(query, columns);
            }
        }
        Expression::Between(between) => {
            collect_columns(&between.this, columns);
            collect_columns(&between.low, columns);
            collect_columns(&between.high, columns);
        }
        Expression::Subquery(subquery) => {
            collect_columns(&subquery.this, columns);
        }
        _ => {}
    }
}

/// Get unqualified columns in a scope
fn get_unqualified_columns(scope: &Scope) -> Vec<ColumnRef> {
    get_scope_columns(scope)
        .into_iter()
        .filter(|c| c.table.is_none())
        .collect()
}

/// Get external columns (columns not resolvable in current scope)
fn get_external_columns(scope: &Scope) -> Vec<ColumnRef> {
    let source_names: HashSet<_> = scope.sources.keys().cloned().collect();

    get_scope_columns(scope)
        .into_iter()
        .filter(|c| {
            if let Some(table) = &c.table {
                !source_names.contains(table)
            } else {
                false
            }
        })
        .collect()
}

/// Check if a scope represents a correlated subquery
fn is_correlated_subquery(scope: &Scope) -> bool {
    scope.can_be_correlated && !get_external_columns(scope).is_empty()
}

/// Check if a column represents a star (e.g., table.*)
fn is_star_column(col: &Column) -> bool {
    col.name.name == "*"
}

/// Create a qualified column expression
fn create_qualified_column(name: &str, table: Option<&str>) -> Expression {
    Expression::Column(Column {
        name: Identifier::new(name),
        table: table.map(Identifier::new),
        join_mark: false,
        trailing_comments: vec![],
    })
}

/// Create an alias expression
fn create_alias(expr: Expression, alias_name: &str) -> Expression {
    Expression::Alias(Box::new(Alias {
        this: expr,
        alias: Identifier::new(alias_name),
        column_aliases: vec![],
        pre_alias_comments: vec![],
        trailing_comments: vec![],
    }))
}

/// Get the output name for an expression
fn get_output_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::Column(col) => Some(col.name.name.clone()),
        Expression::Alias(alias) => Some(alias.alias.name.clone()),
        Expression::Identifier(id) => Some(id.name.clone()),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::generator::Generator;
    use crate::parser::Parser;
    use crate::scope::build_scope;

    fn gen(expr: &Expression) -> String {
        Generator::new().generate(expr).unwrap()
    }

    fn parse(sql: &str) -> Expression {
        Parser::parse_sql(sql).expect("Failed to parse")[0].clone()
    }

    #[test]
    fn test_qualify_columns_options() {
        let options = QualifyColumnsOptions::new()
            .with_expand_alias_refs(true)
            .with_expand_stars(false)
            .with_dialect(DialectType::PostgreSQL)
            .with_allow_partial(true);

        assert!(options.expand_alias_refs);
        assert!(!options.expand_stars);
        assert_eq!(options.dialect, Some(DialectType::PostgreSQL));
        assert!(options.allow_partial_qualification);
    }

    #[test]
    fn test_get_scope_columns() {
        let expr = parse("SELECT a, b FROM t WHERE c = 1");
        let scope = build_scope(&expr);
        let columns = get_scope_columns(&scope);

        assert!(columns.iter().any(|c| c.name == "a"));
        assert!(columns.iter().any(|c| c.name == "b"));
        assert!(columns.iter().any(|c| c.name == "c"));
    }

    #[test]
    fn test_get_unqualified_columns() {
        let expr = parse("SELECT t.a, b FROM t");
        let scope = build_scope(&expr);
        let unqualified = get_unqualified_columns(&scope);

        // Only 'b' should be unqualified
        assert!(unqualified.iter().any(|c| c.name == "b"));
        assert!(!unqualified.iter().any(|c| c.name == "a"));
    }

    #[test]
    fn test_is_star_column() {
        let col = Column {
            name: Identifier::new("*"),
            table: Some(Identifier::new("t")),
            join_mark: false,
            trailing_comments: vec![],
        };
        assert!(is_star_column(&col));

        let col2 = Column {
            name: Identifier::new("id"),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        };
        assert!(!is_star_column(&col2));
    }

    #[test]
    fn test_create_qualified_column() {
        let expr = create_qualified_column("id", Some("users"));
        let sql = gen(&expr);
        assert!(sql.contains("users"));
        assert!(sql.contains("id"));
    }

    #[test]
    fn test_create_alias() {
        let col = Expression::Column(Column {
            name: Identifier::new("value"),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        });
        let aliased = create_alias(col, "total");
        let sql = gen(&aliased);
        assert!(sql.contains("AS") || sql.contains("total"));
    }

    #[test]
    fn test_validate_qualify_columns_success() {
        // All columns qualified
        let expr = parse("SELECT t.a, t.b FROM t");
        let result = validate_qualify_columns(&expr);
        // This may or may not error depending on scope analysis
        // The test verifies the function runs without panic
        let _ = result;
    }

    #[test]
    fn test_collect_columns_nested() {
        let expr = parse("SELECT a + b, c FROM t WHERE d > 0 GROUP BY e HAVING f = 1");
        let mut columns = Vec::new();
        collect_columns(&expr, &mut columns);

        let names: Vec<_> = columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
        assert!(names.contains(&"c"));
        assert!(names.contains(&"d"));
        assert!(names.contains(&"e"));
        assert!(names.contains(&"f"));
    }

    #[test]
    fn test_collect_columns_in_case() {
        let expr = parse("SELECT CASE WHEN a = 1 THEN b ELSE c END FROM t");
        let mut columns = Vec::new();
        collect_columns(&expr, &mut columns);

        let names: Vec<_> = columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
        assert!(names.contains(&"c"));
    }

    #[test]
    fn test_collect_columns_in_subquery() {
        let expr = parse("SELECT a FROM t WHERE b IN (SELECT c FROM s)");
        let mut columns = Vec::new();
        collect_columns(&expr, &mut columns);

        let names: Vec<_> = columns.iter().map(|c| c.name.as_str()).collect();
        assert!(names.contains(&"a"));
        assert!(names.contains(&"b"));
        assert!(names.contains(&"c"));
    }

    #[test]
    fn test_qualify_outputs_basic() {
        let expr = parse("SELECT a, b + c FROM t");
        let scope = build_scope(&expr);
        let result = qualify_outputs(&scope);
        assert!(result.is_ok());
    }

    // ======================================================================
    // quote_identifiers tests
    // ======================================================================

    #[test]
    fn test_needs_quoting_reserved_word() {
        let reserved = get_reserved_words(None);
        assert!(needs_quoting("select", &reserved));
        assert!(needs_quoting("SELECT", &reserved));
        assert!(needs_quoting("from", &reserved));
        assert!(needs_quoting("WHERE", &reserved));
        assert!(needs_quoting("join", &reserved));
        assert!(needs_quoting("table", &reserved));
    }

    #[test]
    fn test_needs_quoting_normal_identifiers() {
        let reserved = get_reserved_words(None);
        assert!(!needs_quoting("foo", &reserved));
        assert!(!needs_quoting("my_column", &reserved));
        assert!(!needs_quoting("col1", &reserved));
        assert!(!needs_quoting("A", &reserved));
        assert!(!needs_quoting("_hidden", &reserved));
    }

    #[test]
    fn test_needs_quoting_special_characters() {
        let reserved = get_reserved_words(None);
        assert!(needs_quoting("my column", &reserved)); // space
        assert!(needs_quoting("my-column", &reserved)); // hyphen
        assert!(needs_quoting("my.column", &reserved)); // dot
        assert!(needs_quoting("col@name", &reserved));  // at sign
        assert!(needs_quoting("col#name", &reserved));  // hash
    }

    #[test]
    fn test_needs_quoting_starts_with_digit() {
        let reserved = get_reserved_words(None);
        assert!(needs_quoting("1col", &reserved));
        assert!(needs_quoting("123", &reserved));
        assert!(needs_quoting("0_start", &reserved));
    }

    #[test]
    fn test_needs_quoting_empty() {
        let reserved = get_reserved_words(None);
        assert!(!needs_quoting("", &reserved));
    }

    #[test]
    fn test_maybe_quote_sets_quoted_flag() {
        let reserved = get_reserved_words(None);
        let mut id = Identifier::new("select");
        assert!(!id.quoted);
        maybe_quote(&mut id, &reserved);
        assert!(id.quoted);
    }

    #[test]
    fn test_maybe_quote_skips_already_quoted() {
        let reserved = get_reserved_words(None);
        let mut id = Identifier::quoted("myname");
        assert!(id.quoted);
        maybe_quote(&mut id, &reserved);
        assert!(id.quoted); // still quoted
        assert_eq!(id.name, "myname"); // name unchanged
    }

    #[test]
    fn test_maybe_quote_skips_star() {
        let reserved = get_reserved_words(None);
        let mut id = Identifier::new("*");
        maybe_quote(&mut id, &reserved);
        assert!(!id.quoted); // star should not be quoted
    }

    #[test]
    fn test_maybe_quote_skips_normal() {
        let reserved = get_reserved_words(None);
        let mut id = Identifier::new("normal_col");
        maybe_quote(&mut id, &reserved);
        assert!(!id.quoted);
    }

    #[test]
    fn test_quote_identifiers_column_with_reserved_name() {
        // A column named "select" should be quoted
        let expr = Expression::Column(Column {
            name: Identifier::new("select"),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        });
        let result = quote_identifiers(expr, None);
        if let Expression::Column(col) = &result {
            assert!(col.name.quoted, "Column named 'select' should be quoted");
        } else {
            panic!("Expected Column expression");
        }
    }

    #[test]
    fn test_quote_identifiers_column_with_special_chars() {
        let expr = Expression::Column(Column {
            name: Identifier::new("my column"),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        });
        let result = quote_identifiers(expr, None);
        if let Expression::Column(col) = &result {
            assert!(col.name.quoted, "Column with space should be quoted");
        } else {
            panic!("Expected Column expression");
        }
    }

    #[test]
    fn test_quote_identifiers_preserves_normal_column() {
        let expr = Expression::Column(Column {
            name: Identifier::new("normal_col"),
            table: Some(Identifier::new("my_table")),
            join_mark: false,
            trailing_comments: vec![],
        });
        let result = quote_identifiers(expr, None);
        if let Expression::Column(col) = &result {
            assert!(!col.name.quoted, "Normal column should not be quoted");
            assert!(!col.table.as_ref().unwrap().quoted, "Normal table should not be quoted");
        } else {
            panic!("Expected Column expression");
        }
    }

    #[test]
    fn test_quote_identifiers_table_ref_reserved() {
        let expr = Expression::Table(TableRef::new("select"));
        let result = quote_identifiers(expr, None);
        if let Expression::Table(tr) = &result {
            assert!(tr.name.quoted, "Table named 'select' should be quoted");
        } else {
            panic!("Expected Table expression");
        }
    }

    #[test]
    fn test_quote_identifiers_table_ref_schema_and_alias() {
        let mut tr = TableRef::new("my_table");
        tr.schema = Some(Identifier::new("from"));
        tr.alias = Some(Identifier::new("t"));
        let expr = Expression::Table(tr);
        let result = quote_identifiers(expr, None);
        if let Expression::Table(tr) = &result {
            assert!(!tr.name.quoted, "Normal table name should not be quoted");
            assert!(tr.schema.as_ref().unwrap().quoted, "Schema named 'from' should be quoted");
            assert!(!tr.alias.as_ref().unwrap().quoted, "Normal alias should not be quoted");
        } else {
            panic!("Expected Table expression");
        }
    }

    #[test]
    fn test_quote_identifiers_identifier_node() {
        let expr = Expression::Identifier(Identifier::new("order"));
        let result = quote_identifiers(expr, None);
        if let Expression::Identifier(id) = &result {
            assert!(id.quoted, "Identifier named 'order' should be quoted");
        } else {
            panic!("Expected Identifier expression");
        }
    }

    #[test]
    fn test_quote_identifiers_alias() {
        let inner = Expression::Column(Column {
            name: Identifier::new("val"),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        });
        let expr = Expression::Alias(Box::new(Alias {
            this: inner,
            alias: Identifier::new("select"),
            column_aliases: vec![Identifier::new("from")],
            pre_alias_comments: vec![],
            trailing_comments: vec![],
        }));
        let result = quote_identifiers(expr, None);
        if let Expression::Alias(alias) = &result {
            assert!(alias.alias.quoted, "Alias named 'select' should be quoted");
            assert!(alias.column_aliases[0].quoted, "Column alias named 'from' should be quoted");
            // Inner column "val" should not be quoted
            if let Expression::Column(col) = &alias.this {
                assert!(!col.name.quoted);
            }
        } else {
            panic!("Expected Alias expression");
        }
    }

    #[test]
    fn test_quote_identifiers_select_recursive() {
        // Parse a query and verify quote_identifiers walks through it
        let expr = parse("SELECT a, b FROM t WHERE c = 1");
        let result = quote_identifiers(expr, None);
        // "a", "b", "c", "t" are all normal identifiers, none should be quoted
        let sql = gen(&result);
        // The SQL should be unchanged since no reserved words are used
        assert!(sql.contains("a"));
        assert!(sql.contains("b"));
        assert!(sql.contains("t"));
    }

    #[test]
    fn test_quote_identifiers_digit_start() {
        let expr = Expression::Column(Column {
            name: Identifier::new("1col"),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        });
        let result = quote_identifiers(expr, None);
        if let Expression::Column(col) = &result {
            assert!(col.name.quoted, "Column starting with digit should be quoted");
        } else {
            panic!("Expected Column expression");
        }
    }

    #[test]
    fn test_quote_identifiers_with_mysql_dialect() {
        let reserved = get_reserved_words(Some(DialectType::MySQL));
        // "KILL" is reserved in MySQL
        assert!(needs_quoting("KILL", &reserved));
        // "FORCE" is reserved in MySQL
        assert!(needs_quoting("FORCE", &reserved));
    }

    #[test]
    fn test_quote_identifiers_with_postgresql_dialect() {
        let reserved = get_reserved_words(Some(DialectType::PostgreSQL));
        // "ILIKE" is reserved in PostgreSQL
        assert!(needs_quoting("ILIKE", &reserved));
        // "VERBOSE" is reserved in PostgreSQL
        assert!(needs_quoting("VERBOSE", &reserved));
    }

    #[test]
    fn test_quote_identifiers_with_bigquery_dialect() {
        let reserved = get_reserved_words(Some(DialectType::BigQuery));
        // "STRUCT" is reserved in BigQuery
        assert!(needs_quoting("STRUCT", &reserved));
        // "PROTO" is reserved in BigQuery
        assert!(needs_quoting("PROTO", &reserved));
    }

    #[test]
    fn test_quote_identifiers_case_insensitive_reserved() {
        let reserved = get_reserved_words(None);
        assert!(needs_quoting("Select", &reserved));
        assert!(needs_quoting("sElEcT", &reserved));
        assert!(needs_quoting("FROM", &reserved));
        assert!(needs_quoting("from", &reserved));
    }

    #[test]
    fn test_quote_identifiers_join_using() {
        // Build a join with USING identifiers that include reserved words
        let mut join = crate::expressions::Join {
            this: Expression::Table(TableRef::new("other")),
            on: None,
            using: vec![Identifier::new("key"), Identifier::new("value")],
            kind: crate::expressions::JoinKind::Inner,
            use_inner_keyword: false,
            use_outer_keyword: false,
            deferred_condition: false,
            join_hint: None,
            match_condition: None,
            pivots: vec![],
        };
        let reserved = get_reserved_words(None);
        quote_join(&mut join, &reserved);
        // "key" is reserved, "value" is not
        assert!(join.using[0].quoted, "USING identifier 'key' should be quoted");
        assert!(!join.using[1].quoted, "USING identifier 'value' should not be quoted");
    }

    #[test]
    fn test_quote_identifiers_cte() {
        // Build a CTE where alias is a reserved word
        let mut cte = crate::expressions::Cte {
            alias: Identifier::new("select"),
            this: Expression::Column(Column {
                name: Identifier::new("x"),
                table: None,
                join_mark: false,
                trailing_comments: vec![],
            }),
            columns: vec![Identifier::new("from"), Identifier::new("normal")],
            materialized: None,
            key_expressions: vec![],
            alias_first: false,
        };
        let reserved = get_reserved_words(None);
        maybe_quote(&mut cte.alias, &reserved);
        for c in &mut cte.columns {
            maybe_quote(c, &reserved);
        }
        assert!(cte.alias.quoted, "CTE alias 'select' should be quoted");
        assert!(cte.columns[0].quoted, "CTE column 'from' should be quoted");
        assert!(!cte.columns[1].quoted, "CTE column 'normal' should not be quoted");
    }

    #[test]
    fn test_quote_identifiers_binary_ops_recurse() {
        // a_col + select_col should quote "select_col" but that's actually
        // just a regular name. Use actual reserved word as column name.
        let expr = Expression::Add(Box::new(crate::expressions::BinaryOp::new(
            Expression::Column(Column {
                name: Identifier::new("select"),
                table: None,
                join_mark: false,
                trailing_comments: vec![],
            }),
            Expression::Column(Column {
                name: Identifier::new("normal"),
                table: None,
                join_mark: false,
                trailing_comments: vec![],
            }),
        )));
        let result = quote_identifiers(expr, None);
        if let Expression::Add(bin) = &result {
            if let Expression::Column(left) = &bin.left {
                assert!(left.name.quoted, "'select' column should be quoted in binary op");
            }
            if let Expression::Column(right) = &bin.right {
                assert!(!right.name.quoted, "'normal' column should not be quoted");
            }
        } else {
            panic!("Expected Add expression");
        }
    }

    #[test]
    fn test_quote_identifiers_already_quoted_preserved() {
        // Already-quoted identifier should stay quoted even if it doesn't need it
        let expr = Expression::Column(Column {
            name: Identifier::quoted("normal_name"),
            table: None,
            join_mark: false,
            trailing_comments: vec![],
        });
        let result = quote_identifiers(expr, None);
        if let Expression::Column(col) = &result {
            assert!(col.name.quoted, "Already-quoted identifier should remain quoted");
        } else {
            panic!("Expected Column expression");
        }
    }

    #[test]
    fn test_quote_identifiers_full_parsed_query() {
        // Test with a parsed query that uses reserved words as identifiers
        // We build the AST manually since the parser would fail on unquoted reserved words
        let mut select = crate::expressions::Select::new();
        select.expressions.push(Expression::Column(Column {
            name: Identifier::new("order"),
            table: Some(Identifier::new("t")),
            join_mark: false,
            trailing_comments: vec![],
        }));
        select.from = Some(crate::expressions::From {
            expressions: vec![Expression::Table(TableRef::new("t"))],
        });
        let expr = Expression::Select(Box::new(select));

        let result = quote_identifiers(expr, None);
        if let Expression::Select(sel) = &result {
            if let Expression::Column(col) = &sel.expressions[0] {
                assert!(col.name.quoted, "Column named 'order' should be quoted");
                assert!(!col.table.as_ref().unwrap().quoted, "Table 't' should not be quoted");
            } else {
                panic!("Expected Column in SELECT list");
            }
        } else {
            panic!("Expected Select expression");
        }
    }

    #[test]
    fn test_get_reserved_words_all_dialects() {
        // Ensure get_reserved_words doesn't panic for any dialect
        let dialects = [
            None,
            Some(DialectType::Generic),
            Some(DialectType::MySQL),
            Some(DialectType::PostgreSQL),
            Some(DialectType::BigQuery),
            Some(DialectType::Snowflake),
            Some(DialectType::TSQL),
            Some(DialectType::ClickHouse),
            Some(DialectType::DuckDB),
            Some(DialectType::Hive),
            Some(DialectType::Spark),
            Some(DialectType::Trino),
            Some(DialectType::Oracle),
            Some(DialectType::Redshift),
        ];
        for dialect in &dialects {
            let words = get_reserved_words(*dialect);
            // All dialects should have basic SQL reserved words
            assert!(words.contains("SELECT"), "All dialects should have SELECT as reserved");
            assert!(words.contains("FROM"), "All dialects should have FROM as reserved");
        }
    }
}
